#include "l2flow/ingress/raw_emergency_reserve_reprovision_posix.h"

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <dirent.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace l2flow::ingress {
namespace {

inline constexpr std::uint64_t kStatBlockBytes = 512U;
inline constexpr mode_t kPrivateFileMode = 0600U;
inline constexpr mode_t kPrivateDirectoryMode = 0700U;
inline constexpr std::string_view kDataCandidatePrefix =
    ".reserve.data.";
inline constexpr std::string_view kDataCandidateSuffix =
    ".reserve-file-v1.tmp";
inline constexpr std::string_view kStateCandidatePrefix =
    ".reserve.state.";
inline constexpr std::string_view kStateCandidateSuffix =
    ".reserve-state-v1.tmp";
inline constexpr std::string_view kInodeCandidatePrefix = ".inode-";
inline constexpr std::string_view kInodeCandidateSuffix =
    ".reserve-inode-v1.tmp";

class ScopedFd final {
public:
    explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}

    ~ScopedFd() {
        Reset();
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
    RawEmergencyReservePosixErrorV1* failure,
    RawEmergencyReservePosixErrorV1 value) noexcept {
    if (failure != nullptr) {
        *failure = value;
    }
}

void SetCodecError(
    ReserveStateV1Error* codec_error,
    ReserveStateV1Error value) noexcept {
    if (codec_error != nullptr) {
        *codec_error = value;
    }
}

[[nodiscard]] bool AddChecked(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        right >
            std::numeric_limits<std::uint64_t>::max() -
                left) {
        return false;
    }
    *result = left + right;
    return true;
}

[[nodiscard]] bool MultiplyChecked(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        (left != 0U &&
         right >
             std::numeric_limits<std::uint64_t>::max() /
                 left)) {
        return false;
    }
    *result = left * right;
    return true;
}

[[nodiscard]] bool RequiredBlocks(
    std::uint64_t bytes,
    std::uint64_t* blocks) noexcept {
    std::uint64_t rounded = 0U;
    if (!AddChecked(
            bytes,
            kStatBlockBytes - 1U,
            &rounded)) {
        return false;
    }
    *blocks = rounded / kStatBlockBytes;
    return true;
}

[[nodiscard]] bool FitsOffT(std::uint64_t value) noexcept {
    return value <=
           static_cast<std::uint64_t>(
               std::numeric_limits<off_t>::max());
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
    std::span<std::byte> output,
    std::uint64_t offset = 0U) noexcept {
    std::size_t completed = 0U;
    while (completed < output.size()) {
        const std::size_t request = std::min(
            output.size() - completed,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const std::uint64_t absolute =
            offset + static_cast<std::uint64_t>(completed);
        if (!FitsOffT(absolute)) {
            errno = EOVERFLOW;
            return false;
        }
        const ssize_t read_count = ::pread(
            fd,
            output.data() + completed,
            request,
            static_cast<off_t>(absolute));
        if (read_count < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (read_count == 0) {
            errno = EIO;
            return false;
        }
        completed += static_cast<std::size_t>(read_count);
    }
    return true;
}

[[nodiscard]] bool PwriteAll(
    int fd,
    std::span<const std::byte> input,
    std::uint64_t offset = 0U) noexcept {
    std::size_t completed = 0U;
    while (completed < input.size()) {
        const std::size_t request = std::min(
            input.size() - completed,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const std::uint64_t absolute =
            offset + static_cast<std::uint64_t>(completed);
        if (!FitsOffT(absolute)) {
            errno = EOVERFLOW;
            return false;
        }
        const ssize_t write_count = ::pwrite(
            fd,
            input.data() + completed,
            request,
            static_cast<off_t>(absolute));
        if (write_count < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (write_count == 0) {
            errno = EIO;
            return false;
        }
        completed += static_cast<std::size_t>(write_count);
    }
    return true;
}

[[nodiscard]] int OpenAtLoop(
    int directory_fd,
    const char* name,
    int flags,
    mode_t mode = 0U) noexcept {
    for (;;) {
        const int fd = ::openat(
            directory_fd, name, flags, mode);
        if (fd >= 0 || errno != EINTR) {
            return fd;
        }
    }
}

[[nodiscard]] bool UnlinkAtLoop(
    int directory_fd,
    const char* name) noexcept {
    for (;;) {
        if (::unlinkat(directory_fd, name, 0) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] int RenameNoReplace(
    int old_directory_fd,
    const char* old_name,
    int new_directory_fd,
    const char* new_name) noexcept {
    return static_cast<int>(
        ::syscall(
            SYS_renameat2,
            old_directory_fd,
            old_name,
            new_directory_fd,
            new_name,
            RENAME_NOREPLACE));
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

[[nodiscard]] bool NameExists(
    int directory_fd,
    const char* name,
    bool* exists) noexcept {
    if (exists == nullptr) {
        errno = EINVAL;
        return false;
    }
    struct stat status {};
    if (::fstatat(
            directory_fd,
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
    return false;
}

[[nodiscard]] bool ValidateRootFd(
    int fd,
    std::string* error) noexcept {
    struct stat status {};
    if (fd < 0 || ::fstat(fd, &status) != 0) {
        SetError(
            error,
            std::string("cannot inspect retained Raw root: ") +
                std::strerror(errno));
        return false;
    }
    const int flags = ::fcntl(fd, F_GETFL);
    if (!S_ISDIR(status.st_mode) ||
        status.st_uid != ::geteuid() ||
        (status.st_mode & (S_IWGRP | S_IWOTH)) != 0 ||
        flags < 0) {
        SetError(
            error,
            "Raw root is not a private retained directory owned by the service UID");
        return false;
    }
    return true;
}

[[nodiscard]] int RetainRootFd(
    int supplied_fd,
    std::string* error) noexcept {
    if (!ValidateRootFd(supplied_fd, error)) {
        return -1;
    }
    const int retained = OpenAtLoop(
        supplied_fd,
        ".",
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_NONBLOCK |
            O_CLOEXEC);
    if (retained < 0 || !ValidateRootFd(retained, error)) {
        if (retained < 0) {
            SetError(
                error,
                std::string("cannot retain Raw root: ") +
                    std::strerror(errno));
        } else {
            static_cast<void>(::close(retained));
        }
        return -1;
    }
    struct stat supplied_status {};
    struct stat retained_status {};
    if (::fstat(supplied_fd, &supplied_status) != 0 ||
        ::fstat(retained, &retained_status) != 0 ||
        supplied_status.st_dev != retained_status.st_dev ||
        supplied_status.st_ino != retained_status.st_ino) {
        SetError(error, "retained Raw root changed inode");
        static_cast<void>(::close(retained));
        return -1;
    }
    return retained;
}

[[nodiscard]] ReserveAllocationPoolIdentityV1 PoolFromHeader(
    const ReserveCoordinatorHeaderV1& header) noexcept {
    ReserveAllocationPoolIdentityV1 pool{};
    pool.reserve_state_uuid = header.reserve_state_uuid;
    pool.device_id = header.device_id;
    pool.quota_identity_sha256 =
        header.quota_identity_sha256;
    pool.mount_identity_sha256 =
        header.mount_identity_sha256;
    return pool;
}

[[nodiscard]] bool IsExactPrivateRegular(
    const struct stat& status,
    std::uint64_t expected_device) noexcept {
    return S_ISREG(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           (status.st_mode & 0777U) == kPrivateFileMode &&
           status.st_nlink == 1 &&
           static_cast<std::uint64_t>(status.st_dev) ==
               expected_device;
}

[[nodiscard]] bool ValidateOpenFlags(int fd) noexcept {
    const int flags = ::fcntl(fd, F_GETFL);
    return flags >= 0 &&
           (flags & O_ACCMODE) == O_RDWR &&
           (flags & O_APPEND) == 0 &&
           (flags & O_NONBLOCK) != 0;
}

[[nodiscard]] bool InodeAllocationSize(
    const ReserveCoordinatorHeaderV1& header,
    std::uint64_t* output) noexcept;

// declared_releasable_bytes is the exact aggregate allocation budget for the
// data reserve plus every inode-reserve file. ReserveFileHeaderV1, by
// contrast, declares only reserve.data's own file size.  The inventory
// commitment freezes each inode reserve's expected 512-byte st_blocks count,
// so the data-file size is the checked remainder after those expected
// allocated bytes, not after the inode files' logical st_size.
[[nodiscard]] bool ReserveDataDeclaredBytes(
    const ReserveCoordinatorHeaderV1& header,
    std::uint64_t* output) noexcept;

[[nodiscard]] bool ValidateDataFd(
    int root_fd,
    const char* name,
    int fd,
    const ReserveCoordinatorHeaderV1& header,
    std::string* error) noexcept {
    struct stat status {};
    std::uint64_t data_declared_bytes = 0U;
    if (!ReserveDataDeclaredBytes(
            header, &data_declared_bytes) ||
        ::fstat(fd, &status) != 0) {
        SetError(
            error,
            std::string("cannot inspect reserve data inode: ") +
                std::strerror(errno));
        return false;
    }
    if (!IsExactPrivateRegular(status, header.device_id) ||
        !ValidateOpenFlags(fd) ||
        status.st_size < 0 ||
        static_cast<std::uint64_t>(status.st_size) !=
            data_declared_bytes ||
        !SameNamedInode(root_fd, name, fd)) {
        SetError(
            error,
            "reserve data has an unsafe type, owner, mode, link count, device, size, flags, or name-to-inode binding");
        return false;
    }
    if (status.st_blocks < 0) {
        SetError(error, "reserve data has a negative st_blocks value");
        return false;
    }
    std::uint64_t minimum_blocks = 0U;
    if (!RequiredBlocks(
            data_declared_bytes,
            &minimum_blocks) ||
        static_cast<std::uint64_t>(status.st_blocks) <
            minimum_blocks) {
        SetError(
            error,
            "reserve data allocation does not cover declared bytes");
        return false;
    }

    ReserveHeaderWireV1 wire{};
    if (!PreadAll(fd, wire)) {
        SetError(
            error,
            std::string("cannot read reserve data header: ") +
                std::strerror(errno));
        return false;
    }
    ReserveFileHeaderV1 decoded{};
    const ReserveHeaderV1Error decode_error =
        DecodeReserveFileHeaderV1(wire, &decoded);
    if (decode_error != ReserveHeaderV1Error::kNone ||
        ValidateReserveFileHeaderBindingV1(
            decoded,
            PoolFromHeader(header),
            static_cast<std::uint64_t>(status.st_size)) !=
            ReserveHeaderV1Error::kNone) {
        SetError(
            error,
            "reserve data header does not match the immutable state header");
        return false;
    }
    return true;
}

[[nodiscard]] bool InodeAllocationSize(
    const ReserveCoordinatorHeaderV1& header,
    std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    *output = std::max(
        static_cast<std::uint64_t>(kReserveHeaderV1Bytes),
        header.allocation_quantum_bytes);
    return FitsOffT(*output);
}

[[nodiscard]] bool ReserveDataDeclaredBytes(
    const ReserveCoordinatorHeaderV1& header,
    std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    std::uint64_t inode_size = 0U;
    std::uint64_t inode_blocks = 0U;
    std::uint64_t inode_allocated_bytes = 0U;
    std::uint64_t inode_bytes = 0U;
    if (!InodeAllocationSize(header, &inode_size) ||
        !RequiredBlocks(inode_size, &inode_blocks) ||
        !MultiplyChecked(
            inode_blocks, 512U, &inode_allocated_bytes) ||
        !MultiplyChecked(
            inode_allocated_bytes,
            header.declared_inode_reserve_count,
            &inode_bytes) ||
        header.declared_releasable_bytes < inode_bytes ||
        header.declared_releasable_bytes - inode_bytes <
            kReserveHeaderV1Bytes) {
        return false;
    }
    const std::uint64_t data_bytes =
        header.declared_releasable_bytes - inode_bytes;
    if (!FitsOffT(data_bytes)) {
        return false;
    }
    *output = data_bytes;
    return true;
}

[[nodiscard]] bool AddRetainedFileCharge(
    int fd,
    std::uint64_t* bytes,
    std::uint64_t* inodes,
    std::string* error) noexcept {
    if (fd < 0 || bytes == nullptr || inodes == nullptr) {
        SetError(error, "retained reserve charge input is invalid");
        return false;
    }
    struct stat status {};
    std::uint64_t allocated_bytes = 0U;
    if (::fstat(fd, &status) != 0 || status.st_size < 0 ||
        status.st_blocks < 0 ||
        !MultiplyChecked(
            static_cast<std::uint64_t>(status.st_blocks),
            512U,
            &allocated_bytes) ||
        allocated_bytes <
            static_cast<std::uint64_t>(status.st_size) ||
        !AddChecked(*bytes, allocated_bytes, bytes) ||
        !AddChecked(*inodes, 1U, inodes)) {
        SetError(
            error,
            "retained reserve st_blocks charge is invalid or overflows");
        return false;
    }
    return true;
}

[[nodiscard]] bool ComputeRetainedReserveCharge(
    int data_fd,
    std::span<const int> inode_fds,
    std::uint64_t* bytes,
    std::uint64_t* inodes,
    std::string* error) noexcept {
    if (bytes == nullptr || inodes == nullptr) {
        SetError(error, "retained reserve charge output is null");
        return false;
    }
    *bytes = 0U;
    *inodes = 0U;
    if (data_fd >= 0 &&
        !AddRetainedFileCharge(
            data_fd, bytes, inodes, error)) {
        return false;
    }
    for (const int fd : inode_fds) {
        if (!AddRetainedFileCharge(
                fd, bytes, inodes, error)) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool ValidateCompleteRetainedReserveCharge(
    const ReserveCoordinatorHeaderV1& header,
    int data_fd,
    std::span<const int> inode_fds,
    std::string* error) noexcept {
    std::uint64_t actual_bytes = 0U;
    std::uint64_t actual_inodes = 0U;
    std::uint64_t expected_inodes = 0U;
    if (data_fd < 0 ||
        inode_fds.size() !=
            static_cast<std::size_t>(
                header.declared_inode_reserve_count) ||
        !AddChecked(
            header.declared_inode_reserve_count,
            1U,
            &expected_inodes) ||
        !ComputeRetainedReserveCharge(
            data_fd,
            inode_fds,
            &actual_bytes,
            &actual_inodes,
            error) ||
        actual_bytes != header.declared_releasable_bytes ||
        actual_inodes != expected_inodes) {
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "data plus inode-reserve st_blocks/inode charge does not exactly match immutable state");
        }
        return false;
    }
    return true;
}

[[nodiscard]] bool ValidateInodeFdWithBinding(
    int inode_directory_fd,
    const char* pathname,
    std::string_view binding_name,
    int fd,
    const ReserveCoordinatorHeaderV1& header,
    std::uint32_t expected_index,
    std::string* error) noexcept {
    struct stat status {};
    std::uint64_t expected_size = 0U;
    if (!InodeAllocationSize(header, &expected_size) ||
        ::fstat(fd, &status) != 0) {
        SetError(
            error,
            std::string("cannot inspect reserve inode file: ") +
                std::strerror(errno));
        return false;
    }
    if (!IsExactPrivateRegular(status, header.device_id) ||
        !ValidateOpenFlags(fd) ||
        status.st_size < 0 ||
        static_cast<std::uint64_t>(status.st_size) !=
            expected_size ||
        !SameNamedInode(inode_directory_fd, pathname, fd)) {
        SetError(
            error,
            "reserve inode file has an unsafe type, owner, mode, link count, device, size, flags, or name-to-inode binding");
        return false;
    }
    if (status.st_blocks < 0) {
        SetError(error, "reserve inode file has negative st_blocks");
        return false;
    }
    std::uint64_t minimum_blocks = 0U;
    if (!RequiredBlocks(expected_size, &minimum_blocks) ||
        static_cast<std::uint64_t>(status.st_blocks) <
            minimum_blocks) {
        SetError(
            error,
            "reserve inode file allocation is smaller than one declared quantum");
        return false;
    }

    ReserveHeaderWireV1 wire{};
    if (!PreadAll(fd, wire)) {
        SetError(
            error,
            std::string("cannot read reserve inode header: ") +
                std::strerror(errno));
        return false;
    }
    ReserveInodeHeaderV1 decoded{};
    const ReserveHeaderV1Error decode_error =
        DecodeReserveInodeHeaderV1(wire, &decoded);
    if (decode_error != ReserveHeaderV1Error::kNone ||
        decoded.inode_index != expected_index ||
        ValidateReserveInodeHeaderBindingV1(
            decoded,
            PoolFromHeader(header),
            header.declared_inode_reserve_count,
            binding_name) != ReserveHeaderV1Error::kNone) {
        SetError(
            error,
            "reserve inode header/name/index does not match immutable state");
        return false;
    }
    return true;
}

[[nodiscard]] bool ValidateInodeFd(
    int inode_directory_fd,
    const char* name,
    int fd,
    const ReserveCoordinatorHeaderV1& header,
    std::uint32_t expected_index,
    std::string* error) noexcept {
    return ValidateInodeFdWithBinding(
        inode_directory_fd,
        name,
        name,
        fd,
        header,
        expected_index,
        error);
}

[[nodiscard]] bool ValidateInodeDirectoryFd(
    int root_fd,
    int inode_directory_fd,
    std::uint64_t expected_device,
    std::string* error) noexcept {
    struct stat status {};
    const int flags = ::fcntl(inode_directory_fd, F_GETFL);
    if (::fstat(inode_directory_fd, &status) != 0 ||
        !S_ISDIR(status.st_mode) ||
        status.st_uid != ::geteuid() ||
        (status.st_mode & 0777U) != kPrivateDirectoryMode ||
        static_cast<std::uint64_t>(status.st_dev) !=
            expected_device ||
        flags < 0 ||
        !SameNamedInode(
            root_fd,
            kRawEmergencyReserveInodesDirectory,
            inode_directory_fd)) {
        SetError(
            error,
            "reserve-inodes has an unsafe type, owner, mode, device, flags, or name-to-inode binding");
        return false;
    }
    return true;
}

[[nodiscard]] int OpenInodeDirectory(
    int root_fd,
    std::uint64_t expected_device,
    std::string* error) noexcept {
    const int fd = OpenAtLoop(
        root_fd,
        kRawEmergencyReserveInodesDirectory,
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_NONBLOCK |
            O_CLOEXEC);
    if (fd < 0) {
        SetError(
            error,
            std::string("cannot securely open reserve-inodes: ") +
                std::strerror(errno));
        return -1;
    }
    if (!ValidateInodeDirectoryFd(
            root_fd, fd, expected_device, error)) {
        static_cast<void>(::close(fd));
        return -1;
    }
    return fd;
}

[[nodiscard]] int CreateOrOpenInodeDirectory(
    int root_fd,
    std::uint64_t expected_device,
    std::string* error) noexcept {
    if (::mkdirat(
            root_fd,
            kRawEmergencyReserveInodesDirectory,
            kPrivateDirectoryMode) != 0 &&
        errno != EEXIST) {
        SetError(
            error,
            std::string("cannot create reserve-inodes: ") +
                std::strerror(errno));
        return -1;
    }
    const int fd =
        OpenInodeDirectory(root_fd, expected_device, error);
    if (fd < 0) {
        return -1;
    }
    if (!FsyncLoop(root_fd)) {
        SetError(
            error,
            std::string(
                "cannot fsync Raw root after reserve-inodes gate: ") +
                std::strerror(errno));
        static_cast<void>(::close(fd));
        return -1;
    }
    return fd;
}

[[nodiscard]] bool ListDirectoryNames(
    int directory_fd,
    std::vector<std::string>* names,
    std::string* error) {
    if (names == nullptr) {
        SetError(error, "directory enumeration output is null");
        return false;
    }
    const int scan_fd = OpenAtLoop(
        directory_fd,
        ".",
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_NONBLOCK |
            O_CLOEXEC);
    if (scan_fd < 0) {
        SetError(
            error,
            std::string("cannot retain directory for enumeration: ") +
                std::strerror(errno));
        return false;
    }
    DIR* directory = ::fdopendir(scan_fd);
    if (directory == nullptr) {
        const int saved = errno;
        static_cast<void>(::close(scan_fd));
        errno = saved;
        SetError(
            error,
            std::string("cannot enumerate reserve directory: ") +
                std::strerror(errno));
        return false;
    }

    names->clear();
    errno = 0;
    for (;;) {
        struct dirent* entry = ::readdir(directory);
        if (entry == nullptr) {
            if (errno != 0) {
                const int saved = errno;
                static_cast<void>(::closedir(directory));
                errno = saved;
                SetError(
                    error,
                    std::string(
                        "reserve directory enumeration failed: ") +
                        std::strerror(errno));
                return false;
            }
            break;
        }
        const std::string_view name(entry->d_name);
        if (name == "." || name == "..") {
            continue;
        }
        names->emplace_back(name);
        errno = 0;
    }
    if (::closedir(directory) != 0) {
        SetError(
            error,
            std::string("cannot close reserve directory scan: ") +
                std::strerror(errno));
        return false;
    }
    std::sort(names->begin(), names->end());
    return true;
}

[[nodiscard]] std::string DataCandidateName(
    const ReserveStateV1Identity& uuid) {
    return std::string(kDataCandidatePrefix) +
           l2flow::common::Identity128Hex(uuid) +
           std::string(kDataCandidateSuffix);
}

[[nodiscard]] std::string StateCandidateName(
    const ReserveStateV1Identity& uuid) {
    return std::string(kStateCandidatePrefix) +
           l2flow::common::Identity128Hex(uuid) +
           std::string(kStateCandidateSuffix);
}

[[nodiscard]] std::string InodeCandidateName(
    std::string_view final_name) {
    return "." + std::string(final_name) +
           std::string(kInodeCandidateSuffix);
}

[[nodiscard]] bool StartsWith(
    std::string_view value,
    std::string_view prefix) noexcept {
    return value.size() >= prefix.size() &&
           value.substr(0U, prefix.size()) == prefix;
}

[[nodiscard]] bool EndsWith(
    std::string_view value,
    std::string_view suffix) noexcept {
    return value.size() >= suffix.size() &&
           value.substr(value.size() - suffix.size()) ==
               suffix;
}

[[nodiscard]] bool ParseInodeCandidateName(
    std::string_view name,
    ReserveStateV1Identity* uuid,
    std::uint32_t* index) noexcept {
    if (name.empty() || name.front() != '.' ||
        !EndsWith(name, kInodeCandidateSuffix)) {
        return false;
    }
    const std::size_t final_size =
        name.size() - 1U - kInodeCandidateSuffix.size();
    const std::string_view final_name =
        name.substr(1U, final_size);
    return ParseReserveInodeFilenameV1(
               final_name, uuid, index) ==
           ReserveHeaderV1Error::kNone;
}

struct EnumeratedInventory final {
    std::uint32_t prefix_count = 0U;
    std::vector<std::string> names{};
};

[[nodiscard]] bool EnumerateFinalInventory(
    int inode_directory_fd,
    const ReserveCoordinatorHeaderV1& header,
    bool allow_prefix,
    EnumeratedInventory* inventory,
    std::string* error) {
    if (inventory == nullptr) {
        SetError(error, "inventory enumeration output is null");
        return false;
    }
    std::vector<std::string> names;
    if (!ListDirectoryNames(
            inode_directory_fd, &names, error)) {
        return false;
    }

    std::uint64_t index_sum = 0U;
    std::uint32_t maximum_index = 0U;
    bool have_index = false;
    for (const std::string& name : names) {
        ReserveStateV1Identity uuid{};
        std::uint32_t index = 0U;
        if (ParseReserveInodeFilenameV1(
                name, &uuid, &index) !=
                ReserveHeaderV1Error::kNone ||
            uuid != header.reserve_state_uuid ||
            index >= header.declared_inode_reserve_count) {
            SetError(
                error,
                "reserve-inodes contains an unknown, temporary, cross-UUID, or out-of-range entry");
            return false;
        }
        if (!AddChecked(index_sum, index, &index_sum)) {
            SetError(error, "reserve inode index sum overflow");
            return false;
        }
        maximum_index =
            have_index ? std::max(maximum_index, index) : index;
        have_index = true;
    }

    if (names.size() >
        static_cast<std::size_t>(
            header.declared_inode_reserve_count)) {
        SetError(error, "reserve inode inventory exceeds declared count");
        return false;
    }
    const std::uint64_t entry_count =
        static_cast<std::uint64_t>(names.size());
    const std::uint64_t expected_sum =
        entry_count == 0U
            ? 0U
            : (entry_count * (entry_count - 1U)) / 2U;
    const bool exact_prefix =
        names.empty() ||
        (maximum_index ==
             static_cast<std::uint32_t>(names.size() - 1U) &&
         index_sum == expected_sum);
    if (!exact_prefix ||
        (!allow_prefix &&
         names.size() !=
             static_cast<std::size_t>(
                 header.declared_inode_reserve_count))) {
        SetError(
            error,
            allow_prefix
                ? "reserve inode inventory is not an exact remaining index prefix"
                : "reserve inode inventory is not exact and complete");
        return false;
    }
    inventory->prefix_count =
        static_cast<std::uint32_t>(names.size());
    inventory->names = std::move(names);
    return true;
}

[[nodiscard]] bool ScanRootCandidateGrammar(
    int root_fd,
    std::string_view expected_data_candidate,
    std::string_view expected_state_candidate,
    std::string* error) {
    std::vector<std::string> names;
    if (!ListDirectoryNames(root_fd, &names, error)) {
        return false;
    }
    for (const std::string& name : names) {
        if (StartsWith(name, kDataCandidatePrefix) &&
            name != expected_data_candidate) {
            SetError(
                error,
                "Raw root contains another or malformed reserve-data candidate set");
            return false;
        }
        if (StartsWith(name, kStateCandidatePrefix) &&
            name != expected_state_candidate) {
            SetError(
                error,
                "Raw root contains another, malformed, or legacy reserve-state candidate set");
            return false;
        }
    }
    return true;
}

void StoreU32Le(
    std::uint32_t value,
    std::array<std::byte, 4U>* output) noexcept {
    for (std::size_t index = 0U; index < output->size(); ++index) {
        (*output)[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
}

void StoreU64Le(
    std::uint64_t value,
    std::array<std::byte, 8U>* output) noexcept {
    for (std::size_t index = 0U; index < output->size(); ++index) {
        (*output)[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
}

[[nodiscard]] bool HashUpdate(
    l2flow::common::Sha256Hasher* hasher,
    std::span<const std::byte> bytes) noexcept {
    return hasher != nullptr && hasher->Update(bytes);
}

[[nodiscard]] bool IsZeroDigest(
    const ReserveStateV1Digest& digest) noexcept {
    return std::all_of(
        digest.begin(),
        digest.end(),
        [](std::byte value) {
            return value == std::byte{0};
        });
}

[[nodiscard]] bool ExpectedReserveCharge(
    const ReserveCoordinatorHeaderV1& header,
    std::uint64_t* bytes,
    std::uint64_t* inodes) noexcept {
    std::uint64_t data_bytes = 0U;
    if (bytes == nullptr || inodes == nullptr ||
        !ReserveDataDeclaredBytes(header, &data_bytes) ||
        !AddChecked(
            header.declared_inode_reserve_count,
            1U,
            inodes)) {
        return false;
    }
    *bytes = header.declared_releasable_bytes;
    return true;
}

[[nodiscard]] bool ProbeCapacity(
    int root_fd,
    const ReserveCoordinatorHeaderV1& header,
    RawEmergencyReserveProbeStageV1 stage,
    std::uint64_t required_bytes,
    std::uint64_t required_inodes,
    RawEmergencyReserveCapacityProbeV1* probe,
    RawEmergencyReserveCapacityObservationV1* observation,
    std::string* error) noexcept {
    if (probe == nullptr || observation == nullptr) {
        SetError(
            error,
            "a filesystem+quota byte/inode capacity probe is mandatory");
        return false;
    }
    RawEmergencyReserveCapacityProbeRequestV1 request{};
    request.stage = stage;
    request.pool = PoolFromHeader(header);
    request.byte_probe_version = header.byte_probe_version;
    request.inode_probe_version = header.inode_probe_version;
    request.required_bytes = required_bytes;
    request.required_inodes = required_inodes;

    RawEmergencyReserveCapacityObservationV1 observed{};
    std::string probe_error;
    if (!probe->Observe(
            root_fd, request, &observed, &probe_error)) {
        SetError(
            error,
            probe_error.empty()
                ? "capacity probe rejected the reserve pool"
                : "capacity probe rejected the reserve pool: " +
                      probe_error);
        return false;
    }
    if (observed.pool != request.pool ||
        observed.byte_probe_version !=
            request.byte_probe_version ||
        observed.inode_probe_version !=
            request.inode_probe_version ||
        !observed.filesystem_bytes_proven ||
        !observed.quota_bytes_proven ||
        !observed.filesystem_inodes_proven ||
        !observed.quota_inodes_proven) {
        SetError(
            error,
            "capacity probe did not prove the exact pool and all filesystem/quota byte/inode dimensions");
        return false;
    }
    if (stage ==
            RawEmergencyReserveProbeStageV1::kBeforeProvision &&
        (observed.filesystem_free_bytes < required_bytes ||
         observed.quota_free_bytes < required_bytes ||
         observed.filesystem_free_inodes < required_inodes ||
         observed.quota_free_inodes < required_inodes)) {
        SetError(
            error,
            "capacity probe reports insufficient byte or inode capacity for provision");
        return false;
    }
    if ((stage ==
             RawEmergencyReserveProbeStageV1::
                 kProvisionedCandidate ||
         stage ==
             RawEmergencyReserveProbeStageV1::kAttached) &&
        (!observed.reserve_byte_charge_proven ||
         !observed.reserve_inode_charge_proven ||
         observed.proven_reserve_byte_charge !=
             required_bytes ||
         observed.proven_reserve_inode_charge !=
             required_inodes)) {
        SetError(
            error,
            "capacity probe did not prove the exact reserve byte and inode charge");
        return false;
    }
    if (stage == RawEmergencyReserveProbeStageV1::kReleased &&
        (!observed.reserve_byte_charge_proven ||
         !observed.reserve_inode_charge_proven ||
         observed.proven_reserve_byte_charge != 0U ||
         observed.proven_reserve_inode_charge != 0U)) {
        SetError(
            error,
            "capacity probe still observes reserve byte or inode charge after release");
        return false;
    }
    *observation = observed;
    return true;
}

[[nodiscard]] bool InvokeHook(
    const RawEmergencyReserveMutationHooksV1* hooks,
    RawEmergencyReserveMutationPointV1 point,
    std::uint32_t index) noexcept {
    return hooks == nullptr || hooks->after == nullptr ||
           hooks->after(point, index, hooks->context);
}

void CloseFdVector(std::vector<int>* fds) noexcept;

struct AttachedInventoryParts final {
    ~AttachedInventoryParts() {
        CloseFdVector(&inode_fds);
    }

    AttachedInventoryParts() = default;
    AttachedInventoryParts(const AttachedInventoryParts&) = delete;
    AttachedInventoryParts& operator=(
        const AttachedInventoryParts&) = delete;

    ScopedFd root{};
    ScopedFd data{};
    ScopedFd inode_directory{};
    std::vector<int> inode_fds{};
    std::uint32_t prefix_count = 0U;
};

void CloseFdVector(std::vector<int>* fds) noexcept {
    if (fds == nullptr) {
        return;
    }
    for (int fd : *fds) {
        if (fd >= 0) {
            static_cast<void>(::close(fd));
        }
    }
    fds->clear();
}

[[nodiscard]] bool OpenInventoryPrefix(
    int inode_directory_fd,
    const ReserveCoordinatorHeaderV1& header,
    std::uint32_t prefix_count,
    std::vector<int>* fds,
    std::string* error) {
    if (fds == nullptr) {
        SetError(error, "reserve inode fd output is null");
        return false;
    }
    fds->clear();
    fds->reserve(prefix_count);
    for (std::uint32_t index = 0U;
         index < prefix_count;
         ++index) {
        std::string name;
        if (FormatReserveInodeFilenameV1(
                header.reserve_state_uuid,
                index,
                &name) != ReserveHeaderV1Error::kNone) {
            SetError(
                error,
                "cannot format canonical reserve inode filename");
            CloseFdVector(fds);
            return false;
        }
        const int fd = OpenAtLoop(
            inode_directory_fd,
            name.c_str(),
            O_RDWR | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
        if (fd < 0 ||
            !ValidateInodeFd(
                inode_directory_fd,
                name.c_str(),
                fd,
                header,
                index,
                error)) {
            if (fd < 0) {
                SetError(
                    error,
                    std::string(
                        "cannot securely open reserve inode file: ") +
                        std::strerror(errno));
            } else {
                static_cast<void>(::close(fd));
            }
            CloseFdVector(fds);
            return false;
        }
        fds->push_back(fd);
    }
    return true;
}

[[nodiscard]] bool RevalidateInventoryPrefix(
    int inode_directory_fd,
    const ReserveCoordinatorHeaderV1& header,
    std::span<const int> fds,
    std::string* error) {
    if (fds.size() >
        static_cast<std::size_t>(
            std::numeric_limits<std::uint32_t>::max())) {
        SetError(error, "reserve inode descriptor count overflow");
        return false;
    }
    for (std::size_t offset = 0U;
         offset < fds.size();
         ++offset) {
        std::string name;
        const std::uint32_t index =
            static_cast<std::uint32_t>(offset);
        if (FormatReserveInodeFilenameV1(
                header.reserve_state_uuid,
                index,
                &name) != ReserveHeaderV1Error::kNone ||
            !ValidateInodeFd(
                inode_directory_fd,
                name.c_str(),
                fds[offset],
                header,
                index,
                error)) {
            if (error != nullptr && error->empty()) {
                SetError(
                    error,
                    "remaining reserve inode descriptor no longer matches its name");
            }
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool LoadAttachedInventoryParts(
    int supplied_root_fd,
    int state_fd,
    const ReserveCoordinatorStateV1& state,
    RawEmergencyReserveCapacityProbeV1* capacity_probe,
    AttachedInventoryParts* parts,
    RawEmergencyReservePosixErrorV1* failure,
    std::string* error) {
    if (parts == nullptr ||
        state.selected_slot >= state.slots.size()) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kInvalidArgument);
        SetError(error, "reserve attach state selection is invalid");
        return false;
    }
    ReserveStateV1Digest expected_inventory{};
    const RawEmergencyReservePosixErrorV1 commitment_error =
        ComputeRawEmergencyReserveInventorySha256V1(
            state.header, &expected_inventory, error);
    if (commitment_error !=
            RawEmergencyReservePosixErrorV1::kNone ||
        expected_inventory !=
            state.header.inode_inventory_sha256) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kInventoryMismatch);
        if (commitment_error ==
            RawEmergencyReservePosixErrorV1::kNone) {
            SetError(
                error,
                "reserve state inventory commitment does not match its immutable count/quantum/pool facts");
        }
        return false;
    }

    parts->root.Reset(RetainRootFd(supplied_root_fd, error));
    if (parts->root.get() < 0) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kUnsafeRoot);
        return false;
    }
    struct stat root_status {};
    if (::fstat(parts->root.get(), &root_status) != 0 ||
        static_cast<std::uint64_t>(root_status.st_dev) !=
            state.header.device_id) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kUnsafeRoot);
        SetError(
            error,
            "Raw root device does not match immutable reserve state");
        return false;
    }
    const std::string data_candidate =
        DataCandidateName(state.header.reserve_state_uuid);
    const std::string state_candidate =
        StateCandidateName(state.header.reserve_state_uuid);
    bool data_candidate_exists = false;
    bool state_candidate_exists = false;
    if (!ScanRootCandidateGrammar(
            parts->root.get(),
            data_candidate,
            state_candidate,
            error) ||
        !NameExists(
            parts->root.get(),
            data_candidate.c_str(),
            &data_candidate_exists) ||
        !NameExists(
            parts->root.get(),
            state_candidate.c_str(),
            &state_candidate_exists) ||
        data_candidate_exists ||
        state_candidate_exists) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kCandidateConflict);
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "published reserve state coexists with a typed provision candidate");
        }
        return false;
    }
    parts->inode_directory.Reset(
        OpenInodeDirectory(
            parts->root.get(),
            state.header.device_id,
            error));
    if (parts->inode_directory.get() < 0) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::
                kUnsafeInodeDirectory);
        return false;
    }

    bool data_exists = false;
    if (!NameExists(
            parts->root.get(),
            kRawEmergencyReserveDataFilename,
            &data_exists)) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kUnsafeDataFile);
        SetError(
            error,
            std::string("cannot inspect fixed reserve data name: ") +
                std::strerror(errno));
        return false;
    }
    if (data_exists) {
        parts->data.Reset(
            OpenAtLoop(
                parts->root.get(),
                kRawEmergencyReserveDataFilename,
                O_RDWR | O_NOFOLLOW | O_NONBLOCK |
                    O_CLOEXEC));
        if (parts->data.get() < 0 ||
            !ValidateDataFd(
                parts->root.get(),
                kRawEmergencyReserveDataFilename,
                parts->data.get(),
                state.header,
                error)) {
            SetFailure(
                failure,
                RawEmergencyReservePosixErrorV1::
                    kUnsafeDataFile);
            if (parts->data.get() < 0) {
                SetError(
                    error,
                    std::string(
                        "cannot securely open fixed reserve data: ") +
                        std::strerror(errno));
            }
            return false;
        }
    }

    const ReserveStateSlotV1& selected =
        state.slots[state.selected_slot];
    const bool allow_prefix =
        selected.coordinator_state ==
            ReserveCoordinatorPhaseV1::kReleasingPrepared ||
        selected.coordinator_state ==
            ReserveCoordinatorPhaseV1::kConsumed;
    if (selected.coordinator_state ==
            ReserveCoordinatorPhaseV1::kReleasingPrepared &&
        !data_exists) {
        // Re-establish both name barriers before accepting an already
        // removed data file or inventory suffix.
        if (!FsyncLoop(parts->root.get()) ||
            !FsyncLoop(parts->inode_directory.get())) {
            SetFailure(
                failure,
                RawEmergencyReservePosixErrorV1::kSyncFailure);
            SetError(
                error,
                std::string(
                    "cannot sync release-restart parent directories: ") +
                    std::strerror(errno));
            return false;
        }
    }

    EnumeratedInventory enumerated{};
    if (!EnumerateFinalInventory(
            parts->inode_directory.get(),
            state.header,
            allow_prefix,
            &enumerated,
            error)) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kInventoryMismatch);
        return false;
    }
    parts->prefix_count = enumerated.prefix_count;

    switch (selected.coordinator_state) {
        case ReserveCoordinatorPhaseV1::kProvisioned:
        case ReserveCoordinatorPhaseV1::kReleasingIntent:
            if (!data_exists ||
                parts->prefix_count !=
                    state.header.declared_inode_reserve_count) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kInventoryMismatch);
                SetError(
                    error,
                    "PROVISIONED/RELEASING_INTENT requires fixed data and complete inventory");
                return false;
            }
            break;
        case ReserveCoordinatorPhaseV1::kReleasingPrepared:
            if (data_exists &&
                parts->prefix_count !=
                    state.header.declared_inode_reserve_count) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kReleaseOrderViolation);
                SetError(
                    error,
                    "data reserve is present while inventory is already partial");
                return false;
            }
            break;
        case ReserveCoordinatorPhaseV1::kConsumed:
            if (data_exists || parts->prefix_count != 0U) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kReleaseOrderViolation);
                SetError(
                    error,
                    "CONSUMED state still has reserve data or inventory");
                return false;
            }
            break;
    }

    if (!OpenInventoryPrefix(
            parts->inode_directory.get(),
            state.header,
            parts->prefix_count,
            &parts->inode_fds,
            error)) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kUnsafeInodeFile);
        return false;
    }

    if (parts->data.get() >= 0 &&
        !FsyncLoop(parts->data.get())) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kSyncFailure);
        SetError(
            error,
            std::string("cannot fsync reserve data on attach: ") +
                std::strerror(errno));
        return false;
    }
    for (int fd : parts->inode_fds) {
        if (!FsyncLoop(fd)) {
            SetFailure(
                failure,
                RawEmergencyReservePosixErrorV1::kSyncFailure);
            SetError(
                error,
                std::string(
                    "cannot fsync reserve inode on attach: ") +
                    std::strerror(errno));
            return false;
        }
    }
    if (!FsyncLoop(parts->inode_directory.get()) ||
        (state_fd >= 0 && !FsyncLoop(state_fd))) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kSyncFailure);
        SetError(
            error,
            std::string(
                "cannot fsync reserve inventory/state on attach: ") +
                std::strerror(errno));
        return false;
    }
    if (state_fd >= 0 &&
        !SameNamedInode(
            parts->root.get(),
            kRawReserveStateFilename,
            state_fd)) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kStateConflict);
        SetError(
            error,
            "retained reserve state no longer matches its fixed name");
        return false;
    }
    if (!ValidateInodeDirectoryFd(
            parts->root.get(),
            parts->inode_directory.get(),
            state.header.device_id,
            error) ||
        (parts->data.get() >= 0 &&
         !ValidateDataFd(
             parts->root.get(),
             kRawEmergencyReserveDataFilename,
             parts->data.get(),
             state.header,
             error)) ||
        !RevalidateInventoryPrefix(
            parts->inode_directory.get(),
            state.header,
            parts->inode_fds,
            error)) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kInventoryMismatch);
        return false;
    }
    if (!FsyncLoop(parts->root.get())) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kSyncFailure);
        SetError(
            error,
            std::string("cannot fsync Raw root on reserve attach: ") +
                std::strerror(errno));
        return false;
    }

    std::uint64_t remaining_bytes = 0U;
    std::uint64_t remaining_inodes = 0U;
    if (!ComputeRetainedReserveCharge(
            parts->data.get(),
            parts->inode_fds,
            &remaining_bytes,
            &remaining_inodes,
            error) ||
        (data_exists &&
         parts->prefix_count ==
             state.header.declared_inode_reserve_count &&
         (remaining_bytes !=
              state.header.declared_releasable_bytes ||
          remaining_inodes !=
              static_cast<std::uint64_t>(
                  state.header
                      .declared_inode_reserve_count) +
                  1U))) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::
                kAllocationProofFailure);
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "retained reserve st_blocks/inode charge does not match immutable state");
        }
        return false;
    }
    RawEmergencyReserveCapacityObservationV1 observation{};
    const RawEmergencyReserveProbeStageV1 probe_stage =
        selected.coordinator_state ==
                ReserveCoordinatorPhaseV1::kConsumed
            ? RawEmergencyReserveProbeStageV1::kReleased
            : RawEmergencyReserveProbeStageV1::kAttached;
    if (!ProbeCapacity(
            parts->root.get(),
            state.header,
            probe_stage,
            remaining_bytes,
            remaining_inodes,
            capacity_probe,
            &observation,
            error)) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::
                kCapacityProbeFailure);
        return false;
    }
    return true;
}

[[nodiscard]] bool IsFreshBootstrap(
    const ReserveCoordinatorStateV1& state) noexcept {
    return state.selected_slot == 0U &&
           state.slots[0U].generation == 1U &&
           state.slots[1U].generation == 1U &&
           state.slots[0U].coordinator_state ==
               ReserveCoordinatorPhaseV1::kProvisioned &&
           state.slots[1U].coordinator_state ==
               ReserveCoordinatorPhaseV1::kProvisioned &&
           state.slots[0U] == state.slots[1U] &&
           state.slots[0U].reserve_state_uuid ==
               state.header.reserve_state_uuid;
}

[[nodiscard]] bool ValidateCleanupCandidate(
    int directory_fd,
    const char* name,
    std::uint64_t expected_device,
    std::string* error) noexcept {
    const int fd = OpenAtLoop(
        directory_fd,
        name,
        O_RDWR | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
    if (fd < 0) {
        SetError(
            error,
            std::string(
                "cannot securely open recognized reserve candidate: ") +
                std::strerror(errno));
        return false;
    }
    ScopedFd retained(fd);
    struct stat status {};
    if (::fstat(fd, &status) != 0 ||
        !IsExactPrivateRegular(status, expected_device) ||
        !ValidateOpenFlags(fd) ||
        !SameNamedInode(directory_fd, name, fd)) {
        SetError(
            error,
            "recognized reserve candidate has unsafe metadata or name-to-inode binding");
        return false;
    }
    return true;
}

[[nodiscard]] bool RemoveCandidateAndSync(
    int directory_fd,
    const char* name,
    std::uint64_t expected_device,
    std::string* error) noexcept {
    bool exists = false;
    if (!NameExists(directory_fd, name, &exists)) {
        SetError(
            error,
            std::string("cannot inspect reserve candidate: ") +
                std::strerror(errno));
        return false;
    }
    if (!exists) {
        return true;
    }
    if (!ValidateCleanupCandidate(
            directory_fd, name, expected_device, error) ||
        !UnlinkAtLoop(directory_fd, name) ||
        !FsyncLoop(directory_fd)) {
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                std::string(
                    "cannot remove and sync reserve candidate: ") +
                    std::strerror(errno));
        }
        return false;
    }
    return true;
}

[[nodiscard]] bool CleanupUnpublishedCandidateSet(
    int root_fd,
    int inode_directory_fd,
    const ReserveCoordinatorHeaderV1& header,
    std::string_view data_candidate,
    std::string_view state_candidate,
    std::string* error) {
    std::vector<std::string> names;
    if (!ListDirectoryNames(
            inode_directory_fd, &names, error)) {
        return false;
    }
    for (const std::string& name : names) {
        ReserveStateV1Identity uuid{};
        std::uint32_t index = 0U;
        const bool final =
            ParseReserveInodeFilenameV1(
                name, &uuid, &index) ==
            ReserveHeaderV1Error::kNone;
        const bool candidate =
            ParseInodeCandidateName(name, &uuid, &index);
        if ((!final && !candidate) ||
            uuid != header.reserve_state_uuid ||
            index >= header.declared_inode_reserve_count) {
            SetError(
                error,
                "reserve-inodes contains an unknown or different-UUID candidate set");
            return false;
        }
    }
    // Reverse lexical order is reverse index order for the frozen fixed-width
    // grammar and removes an entire unpublished set without leaving a mate.
    for (auto iterator = names.rbegin();
         iterator != names.rend();
         ++iterator) {
        if (!RemoveCandidateAndSync(
                inode_directory_fd,
                iterator->c_str(),
                header.device_id,
                error)) {
            return false;
        }
    }
    if (!RemoveCandidateAndSync(
            root_fd,
            std::string(data_candidate).c_str(),
            header.device_id,
            error) ||
        !RemoveCandidateAndSync(
            root_fd,
            std::string(state_candidate).c_str(),
            header.device_id,
            error)) {
        return false;
    }
    return true;
}

[[nodiscard]] int CreatePrivateCandidate(
    int directory_fd,
    const char* name,
    std::string* error) noexcept {
    const int fd = OpenAtLoop(
        directory_fd,
        name,
        O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
            O_NONBLOCK | O_CLOEXEC,
        kPrivateFileMode);
    if (fd < 0) {
        SetError(
            error,
            std::string("cannot create typed reserve candidate: ") +
                std::strerror(errno));
        return -1;
    }
    if (::fchmod(fd, kPrivateFileMode) != 0) {
        const int saved = errno;
        static_cast<void>(::close(fd));
        errno = saved;
        SetError(
            error,
            std::string("cannot set reserve candidate mode: ") +
                std::strerror(errno));
        return -1;
    }
    return fd;
}

[[nodiscard]] bool AllocateFile(
    int fd,
    std::uint64_t bytes,
    std::string* error) noexcept {
    if (!FitsOffT(bytes)) {
        errno = EOVERFLOW;
        SetError(error, "reserve allocation does not fit off_t");
        return false;
    }
    const int allocation_error =
        ::posix_fallocate(fd, 0, static_cast<off_t>(bytes));
    if (allocation_error != 0) {
        errno = allocation_error;
        SetError(
            error,
            std::string("posix_fallocate could not prove allocation: ") +
                std::strerror(allocation_error));
        return false;
    }
    return true;
}

[[nodiscard]] bool ValidateStateCandidateFd(
    int root_fd,
    const char* name,
    int fd,
    const ReserveCoordinatorHeaderV1& header,
    std::span<const std::byte> expected_wire,
    std::string* error) noexcept {
    struct stat status {};
    if (::fstat(fd, &status) != 0 ||
        !IsExactPrivateRegular(status, header.device_id) ||
        !ValidateOpenFlags(fd) ||
        status.st_size !=
            static_cast<off_t>(kReserveStateV1FileBytes) ||
        status.st_blocks < 0 ||
        !SameNamedInode(root_fd, name, fd)) {
        SetError(
            error,
            "reserve state candidate has unsafe metadata, flags, size, or name-to-inode binding");
        return false;
    }
    std::uint64_t minimum_blocks = 0U;
    if (!RequiredBlocks(
            kReserveStateV1FileBytes,
            &minimum_blocks) ||
        static_cast<std::uint64_t>(status.st_blocks) <
            minimum_blocks) {
        SetError(
            error,
            "reserve state candidate is not fully block allocated");
        return false;
    }
    ReserveStateV1FileWire actual{};
    if (!PreadAll(fd, actual) ||
        !std::equal(
            actual.begin(),
            actual.end(),
            expected_wire.begin(),
            expected_wire.end())) {
        SetError(
            error,
            "reserve state candidate bytes do not match bootstrap");
        return false;
    }
    ReserveCoordinatorStateV1 decoded{};
    if (DecodeAndSelectReserveCoordinatorStateV1(
            actual, &decoded) != ReserveStateV1Error::kNone) {
        SetError(
            error,
            "reserve state candidate is not a valid complete two-slot image");
        return false;
    }
    return true;
}

[[nodiscard]] bool PublishNoReplaceAndSync(
    int directory_fd,
    const char* candidate,
    const char* final_name,
    std::string* error) noexcept {
    if (RenameNoReplace(
            directory_fd,
            candidate,
            directory_fd,
            final_name) != 0) {
        SetError(
            error,
            std::string("cannot publish reserve candidate: ") +
                std::strerror(errno));
        return false;
    }
    if (!FsyncLoop(directory_fd)) {
        SetError(
            error,
            std::string(
                "cannot fsync reserve candidate parent: ") +
                std::strerror(errno));
        return false;
    }
    return true;
}

}  // namespace

std::string_view RawEmergencyReservePosixErrorV1Name(
    RawEmergencyReservePosixErrorV1 error) noexcept {
    switch (error) {
        case RawEmergencyReservePosixErrorV1::kNone:
            return "none";
        case RawEmergencyReservePosixErrorV1::kInvalidArgument:
            return "invalid_argument";
        case RawEmergencyReservePosixErrorV1::kUnsafeRoot:
            return "unsafe_root";
        case RawEmergencyReservePosixErrorV1::kUnsafeInodeDirectory:
            return "unsafe_inode_directory";
        case RawEmergencyReservePosixErrorV1::kUnsafeDataFile:
            return "unsafe_data_file";
        case RawEmergencyReservePosixErrorV1::kUnsafeInodeFile:
            return "unsafe_inode_file";
        case RawEmergencyReservePosixErrorV1::kInventoryMismatch:
            return "inventory_mismatch";
        case RawEmergencyReservePosixErrorV1::kHeaderMismatch:
            return "header_mismatch";
        case RawEmergencyReservePosixErrorV1::kAllocationProofFailure:
            return "allocation_proof_failure";
        case RawEmergencyReservePosixErrorV1::kCapacityProbeFailure:
            return "capacity_probe_failure";
        case RawEmergencyReservePosixErrorV1::kCandidateConflict:
            return "candidate_conflict";
        case RawEmergencyReservePosixErrorV1::kStateConflict:
            return "state_conflict";
        case RawEmergencyReservePosixErrorV1::kCodecFailure:
            return "codec_failure";
        case RawEmergencyReservePosixErrorV1::kIoFailure:
            return "io_failure";
        case RawEmergencyReservePosixErrorV1::kSyncFailure:
            return "sync_failure";
        case RawEmergencyReservePosixErrorV1::kPublishConflict:
            return "publish_conflict";
        case RawEmergencyReservePosixErrorV1::kReleaseOrderViolation:
            return "release_order_violation";
        case RawEmergencyReservePosixErrorV1::kInjectedInterruption:
            return "injected_interruption";
        case RawEmergencyReservePosixErrorV1::kAllocationFailure:
            return "allocation_failure";
    }
    return "unknown";
}

RawEmergencyReservePosixErrorV1
ComputeRawEmergencyReserveInventorySha256V1(
    const ReserveCoordinatorHeaderV1& header,
    ReserveStateV1Digest* digest,
    std::string* error) noexcept {
    SetError(error, {});
    if (digest == nullptr ||
        l2flow::common::IsZeroIdentity(
            header.reserve_state_uuid) ||
        header.device_id == 0U ||
        IsZeroDigest(header.quota_identity_sha256) ||
        IsZeroDigest(header.mount_identity_sha256) ||
        header.allocation_quantum_bytes == 0U ||
        header.declared_inode_reserve_count == 0U ||
        header.declared_inode_reserve_count >
            kReserveInodeV1MaxCount) {
        SetError(
            error,
            "reserve inventory commitment input is invalid");
        return RawEmergencyReservePosixErrorV1::kInvalidArgument;
    }
    std::uint64_t expected_size = 0U;
    std::uint64_t expected_blocks = 0U;
    if (!InodeAllocationSize(header, &expected_size) ||
        !RequiredBlocks(expected_size, &expected_blocks)) {
        SetError(
            error,
            "reserve inventory expected allocation overflows");
        return RawEmergencyReservePosixErrorV1::kInvalidArgument;
    }

    try {
        l2flow::common::Sha256Hasher hasher;
        const auto domain_bytes = std::as_bytes(
            std::span{
                kRawEmergencyReserveInventoryCommitmentDomainV1
                    .data(),
                kRawEmergencyReserveInventoryCommitmentDomainV1
                    .size()});
        const std::array<std::byte, 1U> terminator{
            std::byte{0}};
        std::array<std::byte, 4U> count_le{};
        std::array<std::byte, 8U> quantum_le{};
        StoreU32Le(
            header.declared_inode_reserve_count, &count_le);
        StoreU64Le(
            header.allocation_quantum_bytes, &quantum_le);
        if (!HashUpdate(&hasher, domain_bytes) ||
            !HashUpdate(&hasher, terminator) ||
            !HashUpdate(
                &hasher, header.reserve_state_uuid) ||
            !HashUpdate(&hasher, count_le) ||
            !HashUpdate(&hasher, quantum_le)) {
            SetError(
                error,
                "reserve inventory commitment length overflow");
            return RawEmergencyReservePosixErrorV1::
                kInvalidArgument;
        }

        for (std::uint32_t index = 0U;
             index <
             header.declared_inode_reserve_count;
             ++index) {
            std::string name;
            if (FormatReserveInodeFilenameV1(
                    header.reserve_state_uuid,
                    index,
                    &name) != ReserveHeaderV1Error::kNone) {
                SetError(
                    error,
                    "cannot format inventory commitment filename");
                return RawEmergencyReservePosixErrorV1::
                    kAllocationFailure;
            }
            ReserveInodeHeaderV1 inode_header{};
            inode_header.pool = PoolFromHeader(header);
            inode_header.inode_index = index;
            inode_header.declared_inode_reserve_count =
                header.declared_inode_reserve_count;
            ReserveHeaderWireV1 header_wire{};
            if (EncodeReserveInodeHeaderV1(
                    inode_header, &header_wire) !=
                ReserveHeaderV1Error::kNone) {
                SetError(
                    error,
                    "cannot encode deterministic reserve inode header");
                return RawEmergencyReservePosixErrorV1::
                    kHeaderMismatch;
            }
            const ReserveStateV1Digest header_sha =
                l2flow::common::ComputeSha256(header_wire);
            std::array<std::byte, 8U> size_le{};
            std::array<std::byte, 8U> blocks_le{};
            StoreU64Le(expected_size, &size_le);
            StoreU64Le(expected_blocks, &blocks_le);
            if (!HashUpdate(
                    &hasher,
                    std::as_bytes(
                        std::span{name.data(), name.size()})) ||
                !HashUpdate(&hasher, header_sha) ||
                !HashUpdate(&hasher, size_le) ||
                !HashUpdate(&hasher, blocks_le)) {
                SetError(
                    error,
                    "reserve inventory commitment length overflow");
                return RawEmergencyReservePosixErrorV1::
                    kInvalidArgument;
            }
        }
        ReserveStateV1Digest computed{};
        if (!hasher.Finalize(&computed)) {
            SetError(
                error,
                "cannot finalize reserve inventory commitment");
            return RawEmergencyReservePosixErrorV1::
                kInvalidArgument;
        }
        *digest = computed;
        return RawEmergencyReservePosixErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        SetError(
            error,
            "cannot allocate inventory commitment filename");
        return RawEmergencyReservePosixErrorV1::kAllocationFailure;
    } catch (...) {
        SetError(
            error,
            "unexpected inventory commitment failure");
        return RawEmergencyReservePosixErrorV1::kAllocationFailure;
    }
}

RawEmergencyReserveInventoryV1::
    RawEmergencyReserveInventoryV1(
        int root_fd,
        int data_fd,
        int inode_directory_fd,
        std::vector<int> inode_fds,
        ReserveCoordinatorHeaderV1 header,
        ReserveStateSlotV1 selected_slot,
        std::uint32_t remaining_inode_prefix_count) noexcept
    : root_fd_(root_fd),
      data_fd_(data_fd),
      inode_directory_fd_(inode_directory_fd),
      inode_fds_(std::move(inode_fds)),
      header_(std::move(header)),
      selected_slot_(std::move(selected_slot)),
      phase_(selected_slot_.coordinator_state),
      remaining_inode_prefix_count_(
          remaining_inode_prefix_count) {}

RawEmergencyReserveInventoryV1::
    ~RawEmergencyReserveInventoryV1() {
    if (data_fd_ >= 0) {
        static_cast<void>(::close(data_fd_));
    }
    CloseFdVector(&inode_fds_);
    if (inode_directory_fd_ >= 0) {
        static_cast<void>(::close(inode_directory_fd_));
    }
    if (root_fd_ >= 0) {
        static_cast<void>(::close(root_fd_));
    }
}

std::unique_ptr<RawEmergencyReserveInventoryV1>
AttachRawEmergencyReserveInventoryV1(
    const RawReserveStateFileV1& state_file,
    RawEmergencyReserveCapacityProbeV1* capacity_probe,
    RawEmergencyReservePosixErrorV1* failure,
    std::string* error) noexcept {
    SetFailure(
        failure,
        RawEmergencyReservePosixErrorV1::kNone);
    SetError(error, {});
    if (state_file.poisoned() ||
        state_file.descriptor() < 0 ||
        state_file.directory_descriptor() < 0) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kInvalidArgument);
        SetError(
            error,
            "reserve state handle is invalid or poisoned");
        return nullptr;
    }

    try {
        AttachedInventoryParts parts{};
        if (!LoadAttachedInventoryParts(
                state_file.directory_descriptor(),
                state_file.descriptor(),
                state_file.state(),
                capacity_probe,
                &parts,
                failure,
                error)) {
            return nullptr;
        }
        const ReserveCoordinatorStateV1& state =
            state_file.state();
        std::vector<int> inode_fds;
        inode_fds.swap(parts.inode_fds);
        std::unique_ptr<RawEmergencyReserveInventoryV1>
            attached(
                new (std::nothrow)
                    RawEmergencyReserveInventoryV1(
                        parts.root.Release(),
                        parts.data.Release(),
                        parts.inode_directory.Release(),
                        std::move(inode_fds),
                        state.header,
                        state.slots[state.selected_slot],
                        parts.prefix_count));
        if (attached == nullptr) {
            CloseFdVector(&inode_fds);
            SetFailure(
                failure,
                RawEmergencyReservePosixErrorV1::
                    kAllocationFailure);
            SetError(
                error,
                "cannot allocate retained reserve inventory capability");
            return nullptr;
        }
        return attached;
    } catch (const std::bad_alloc&) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kAllocationFailure);
        SetError(
            error,
            "cannot allocate retained reserve inventory");
        return nullptr;
    } catch (...) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kAllocationFailure);
        SetError(
            error,
            "unexpected reserve inventory attach failure");
        return nullptr;
    }
}

namespace {

[[nodiscard]] bool FinishProvisionAttach(
    int root_fd,
    const ReserveCoordinatorStateV1& bootstrap,
    RawEmergencyReserveCapacityProbeV1* capacity_probe,
    RawEmergencyReserveProvisionResultV1* result,
    RawEmergencyReservePosixErrorV1* failure,
    ReserveStateV1Error* codec_error,
    std::string* error) noexcept {
    RawReserveStatePosixError state_failure =
        RawReserveStatePosixError::kNone;
    std::unique_ptr<RawReserveStateFileV1> state_file =
        AttachRawReserveStateAtV1(
            root_fd,
            &state_failure,
            codec_error,
            error);
    if (state_file == nullptr) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kStateConflict);
        return false;
    }
    if (state_file->state() != bootstrap) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kStateConflict);
        SetError(
            error,
            "published reserve state is not the exact requested bootstrap");
        return false;
    }
    std::unique_ptr<RawEmergencyReserveInventoryV1> inventory =
        AttachRawEmergencyReserveInventoryV1(
            *state_file, capacity_probe, failure, error);
    if (inventory == nullptr) {
        return false;
    }
    result->state_file = std::move(state_file);
    result->inventory = std::move(inventory);
    return true;
}

[[nodiscard]] bool OpenCompleteFinalInventory(
    AttachedInventoryParts* parts,
    const ReserveCoordinatorHeaderV1& header,
    std::string* error) {
    EnumeratedInventory enumerated{};
    if (!EnumerateFinalInventory(
            parts->inode_directory.get(),
            header,
            false,
            &enumerated,
            error) ||
        enumerated.prefix_count !=
            header.declared_inode_reserve_count ||
        !OpenInventoryPrefix(
            parts->inode_directory.get(),
            header,
            enumerated.prefix_count,
            &parts->inode_fds,
            error)) {
        return false;
    }
    parts->prefix_count = enumerated.prefix_count;
    return true;
}

[[nodiscard]] bool SyncAndValidateProvisionCandidates(
    AttachedInventoryParts* parts,
    int state_candidate_fd,
    const char* state_candidate_name,
    const ReserveCoordinatorStateV1& bootstrap,
    std::span<const std::byte> state_wire,
    std::string* error) {
    if (parts->data.get() < 0 ||
        parts->inode_directory.get() < 0 ||
        parts->root.get() < 0 ||
        !FsyncLoop(parts->data.get())) {
        SetError(
            error,
            std::string("cannot fsync reserve data candidate: ") +
                std::strerror(errno));
        return false;
    }
    for (int fd : parts->inode_fds) {
        if (!FsyncLoop(fd)) {
            SetError(
                error,
                std::string(
                    "cannot fsync reserve inode candidate: ") +
                    std::strerror(errno));
            return false;
        }
    }
    if (!FsyncLoop(parts->inode_directory.get()) ||
        !FsyncLoop(state_candidate_fd) ||
        !ValidateStateCandidateFd(
            parts->root.get(),
            state_candidate_name,
            state_candidate_fd,
            bootstrap.header,
            state_wire,
            error) ||
        !RevalidateInventoryPrefix(
            parts->inode_directory.get(),
            bootstrap.header,
            parts->inode_fds,
            error) ||
        !ValidateCompleteRetainedReserveCharge(
            bootstrap.header,
            parts->data.get(),
            parts->inode_fds,
            error) ||
        !FsyncLoop(parts->root.get())) {
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                std::string(
                    "cannot validate/sync complete reserve candidate set: ") +
                    std::strerror(errno));
        }
        return false;
    }
    return true;
}

}  // namespace

bool ProvisionFreshRawEmergencyReserveV1(
    int retained_raw_root_fd,
    const ReserveCoordinatorStateV1& bootstrap,
    RawEmergencyReserveCapacityProbeV1* capacity_probe,
    const RawEmergencyReserveMutationHooksV1* hooks,
    RawEmergencyReserveProvisionResultV1* result,
    RawEmergencyReservePosixErrorV1* failure,
    ReserveStateV1Error* codec_error,
    std::string* error) noexcept {
    SetFailure(
        failure,
        RawEmergencyReservePosixErrorV1::kNone);
    SetCodecError(codec_error, ReserveStateV1Error::kNone);
    SetError(error, {});
    if (result == nullptr || capacity_probe == nullptr) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kInvalidArgument);
        SetError(
            error,
            result == nullptr
                ? "provision result output is null"
                : "provision requires an injected filesystem+quota byte/inode probe");
        return false;
    }
    result->inventory.reset();
    result->state_file.reset();

    try {
        ReserveStateV1Digest expected_inventory{};
        const RawEmergencyReservePosixErrorV1 commitment_error =
            ComputeRawEmergencyReserveInventorySha256V1(
                bootstrap.header,
                &expected_inventory,
                error);
        if (commitment_error !=
                RawEmergencyReservePosixErrorV1::kNone ||
            expected_inventory !=
                bootstrap.header.inode_inventory_sha256 ||
            !IsFreshBootstrap(bootstrap)) {
            SetFailure(
                failure,
                RawEmergencyReservePosixErrorV1::kInvalidArgument);
            if (commitment_error ==
                    RawEmergencyReservePosixErrorV1::kNone &&
                error != nullptr && error->empty()) {
                SetError(
                    error,
                    "provision input is not the exact duplicate generation=1 PROVISIONED bootstrap or has the wrong inventory commitment");
            }
            return false;
        }
        ReserveStateV1FileWire state_wire{};
        const ReserveStateV1Error encoded =
            EncodeReserveCoordinatorStateV1(
                bootstrap, &state_wire);
        SetCodecError(codec_error, encoded);
        if (encoded != ReserveStateV1Error::kNone) {
            SetFailure(
                failure,
                RawEmergencyReservePosixErrorV1::kCodecFailure);
            SetError(
                error,
                std::string("bootstrap codec rejected provision: ") +
                    std::string(ReserveStateV1ErrorName(encoded)));
            return false;
        }

        AttachedInventoryParts parts{};
        parts.root.Reset(
            RetainRootFd(retained_raw_root_fd, error));
        if (parts.root.get() < 0) {
            SetFailure(
                failure,
                RawEmergencyReservePosixErrorV1::kUnsafeRoot);
            return false;
        }
        struct stat root_status {};
        if (::fstat(parts.root.get(), &root_status) != 0 ||
            static_cast<std::uint64_t>(root_status.st_dev) !=
                bootstrap.header.device_id) {
            SetFailure(
                failure,
                RawEmergencyReservePosixErrorV1::kUnsafeRoot);
            SetError(
                error,
                "provision Raw root device does not match bootstrap");
            return false;
        }

        const std::string data_candidate =
            DataCandidateName(
                bootstrap.header.reserve_state_uuid);
        const std::string state_candidate =
            StateCandidateName(
                bootstrap.header.reserve_state_uuid);
        if (!ScanRootCandidateGrammar(
                parts.root.get(),
                data_candidate,
                state_candidate,
                error)) {
            SetFailure(
                failure,
                RawEmergencyReservePosixErrorV1::
                    kCandidateConflict);
            return false;
        }

        bool fixed_state_exists = false;
        bool data_candidate_exists = false;
        bool state_candidate_exists = false;
        if (!NameExists(
                parts.root.get(),
                kRawReserveStateFilename,
                &fixed_state_exists) ||
            !NameExists(
                parts.root.get(),
                data_candidate.c_str(),
                &data_candidate_exists) ||
            !NameExists(
                parts.root.get(),
                state_candidate.c_str(),
                &state_candidate_exists)) {
            SetFailure(
                failure,
                RawEmergencyReservePosixErrorV1::kIoFailure);
            SetError(
                error,
                std::string(
                    "cannot enumerate reserve publication names: ") +
                    std::strerror(errno));
            return false;
        }
        if (fixed_state_exists) {
            if (data_candidate_exists ||
                state_candidate_exists) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kCandidateConflict);
                SetError(
                    error,
                    "published reserve state coexists with a typed root candidate");
                return false;
            }
            return FinishProvisionAttach(
                parts.root.get(),
                bootstrap,
                capacity_probe,
                result,
                failure,
                codec_error,
                error);
        }

        parts.inode_directory.Reset(
            CreateOrOpenInodeDirectory(
                parts.root.get(),
                bootstrap.header.device_id,
                error));
        if (parts.inode_directory.get() < 0) {
            SetFailure(
                failure,
                RawEmergencyReservePosixErrorV1::
                    kUnsafeInodeDirectory);
            return false;
        }

        bool fixed_data_exists = false;
        if (!NameExists(
                parts.root.get(),
                kRawEmergencyReserveDataFilename,
                &fixed_data_exists)) {
            SetFailure(
                failure,
                RawEmergencyReservePosixErrorV1::kIoFailure);
            SetError(
                error,
                std::string(
                    "cannot inspect fixed reserve data: ") +
                    std::strerror(errno));
            return false;
        }

        if (!fixed_data_exists) {
            if (!CleanupUnpublishedCandidateSet(
                    parts.root.get(),
                    parts.inode_directory.get(),
                    bootstrap.header,
                    data_candidate,
                    state_candidate,
                    error)) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kCandidateConflict);
                return false;
            }

            std::uint64_t reserve_bytes = 0U;
            std::uint64_t reserve_inodes = 0U;
            std::uint64_t provision_bytes = 0U;
            std::uint64_t provision_inodes = 0U;
            if (!ExpectedReserveCharge(
                    bootstrap.header,
                    &reserve_bytes,
                    &reserve_inodes) ||
                !AddChecked(
                    reserve_bytes,
                    kReserveStateV1FileBytes,
                    &provision_bytes) ||
                !AddChecked(
                    reserve_inodes,
                    1U,
                    &provision_inodes)) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kInvalidArgument);
                SetError(
                    error,
                    "provision byte/inode requirement overflows");
                return false;
            }
            RawEmergencyReserveCapacityObservationV1
                before_observation{};
            if (!ProbeCapacity(
                    parts.root.get(),
                    bootstrap.header,
                    RawEmergencyReserveProbeStageV1::
                        kBeforeProvision,
                    provision_bytes,
                    provision_inodes,
                    capacity_probe,
                    &before_observation,
                    error)) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kCapacityProbeFailure);
                return false;
            }

            parts.data.Reset(
                CreatePrivateCandidate(
                    parts.root.get(),
                    data_candidate.c_str(),
                    error));
            if (parts.data.get() < 0) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::kIoFailure);
                return false;
            }
            ReserveFileHeaderV1 data_header{};
            data_header.pool =
                PoolFromHeader(bootstrap.header);
            ReserveHeaderWireV1 data_wire{};
            if (!ReserveDataDeclaredBytes(
                    bootstrap.header,
                    &data_header.declared_bytes) ||
                EncodeReserveFileHeaderV1(
                    data_header, &data_wire) !=
                    ReserveHeaderV1Error::kNone ||
                !PwriteAll(parts.data.get(), data_wire) ||
                !AllocateFile(
                    parts.data.get(),
                    data_header.declared_bytes,
                    error) ||
                !ValidateDataFd(
                    parts.root.get(),
                    data_candidate.c_str(),
                    parts.data.get(),
                    bootstrap.header,
                    error) ||
                !FsyncLoop(parts.data.get())) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kAllocationProofFailure);
                if (error != nullptr && error->empty()) {
                    SetError(
                        error,
                        std::string(
                            "cannot build/sync reserve data candidate: ") +
                            std::strerror(errno));
                }
                return false;
            }
            if (!InvokeHook(
                    hooks,
                    RawEmergencyReserveMutationPointV1::
                        kDataCandidateSynced,
                    0U)) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kInjectedInterruption);
                SetError(
                    error,
                    "interrupted after reserve data candidate sync");
                return false;
            }

            parts.inode_fds.reserve(
                bootstrap.header
                    .declared_inode_reserve_count);
            std::uint64_t inode_size = 0U;
            if (!InodeAllocationSize(
                    bootstrap.header, &inode_size)) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kInvalidArgument);
                SetError(
                    error,
                    "reserve inode allocation size is invalid");
                return false;
            }
            for (std::uint32_t index = 0U;
                 index <
                 bootstrap.header
                     .declared_inode_reserve_count;
                 ++index) {
                std::string final_name;
                if (FormatReserveInodeFilenameV1(
                        bootstrap.header.reserve_state_uuid,
                        index,
                        &final_name) !=
                    ReserveHeaderV1Error::kNone) {
                    SetFailure(
                        failure,
                        RawEmergencyReservePosixErrorV1::
                            kAllocationFailure);
                    SetError(
                        error,
                        "cannot format reserve inode candidate");
                    return false;
                }
                const std::string candidate_name =
                    InodeCandidateName(final_name);
                ScopedFd inode_fd(
                    CreatePrivateCandidate(
                        parts.inode_directory.get(),
                        candidate_name.c_str(),
                        error));
                if (inode_fd.get() < 0) {
                    SetFailure(
                        failure,
                        RawEmergencyReservePosixErrorV1::
                            kIoFailure);
                    return false;
                }
                ReserveInodeHeaderV1 inode_header{};
                inode_header.pool =
                    PoolFromHeader(bootstrap.header);
                inode_header.inode_index = index;
                inode_header.declared_inode_reserve_count =
                    bootstrap.header
                        .declared_inode_reserve_count;
                ReserveHeaderWireV1 inode_wire{};
                if (EncodeReserveInodeHeaderV1(
                        inode_header, &inode_wire) !=
                        ReserveHeaderV1Error::kNone ||
                    !PwriteAll(inode_fd.get(), inode_wire) ||
                    !AllocateFile(
                        inode_fd.get(), inode_size, error) ||
                    !ValidateInodeFdWithBinding(
                        parts.inode_directory.get(),
                        candidate_name.c_str(),
                        final_name,
                        inode_fd.get(),
                        bootstrap.header,
                        index,
                        error) ||
                    !FsyncLoop(inode_fd.get())) {
                    SetFailure(
                        failure,
                        RawEmergencyReservePosixErrorV1::
                            kAllocationProofFailure);
                    if (error != nullptr && error->empty()) {
                        SetError(
                            error,
                            std::string(
                                "cannot build/sync reserve inode candidate: ") +
                                std::strerror(errno));
                    }
                    return false;
                }
                if (RenameNoReplace(
                        parts.inode_directory.get(),
                        candidate_name.c_str(),
                        parts.inode_directory.get(),
                        final_name.c_str()) != 0 ||
                    !FsyncLoop(
                        parts.inode_directory.get()) ||
                    !ValidateInodeFd(
                        parts.inode_directory.get(),
                        final_name.c_str(),
                        inode_fd.get(),
                        bootstrap.header,
                        index,
                        error)) {
                    SetFailure(
                        failure,
                        RawEmergencyReservePosixErrorV1::
                            kPublishConflict);
                    if (error != nullptr && error->empty()) {
                        SetError(
                            error,
                            std::string(
                                "cannot publish reserve inode candidate: ") +
                                std::strerror(errno));
                    }
                    return false;
                }
                parts.inode_fds.push_back(
                    inode_fd.Release());
                parts.prefix_count = index + 1U;
                if (!InvokeHook(
                        hooks,
                        RawEmergencyReserveMutationPointV1::
                            kInodePublished,
                        index)) {
                    SetFailure(
                        failure,
                        RawEmergencyReservePosixErrorV1::
                            kInjectedInterruption);
                    SetError(
                        error,
                        "interrupted after reserve inode publication");
                    return false;
                }
            }

            EnumeratedInventory complete{};
            if (!EnumerateFinalInventory(
                    parts.inode_directory.get(),
                    bootstrap.header,
                    false,
                    &complete,
                    error) ||
                complete.prefix_count !=
                    bootstrap.header
                        .declared_inode_reserve_count ||
                !FsyncLoop(
                    parts.inode_directory.get())) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kInventoryMismatch);
                return false;
            }
            if (!InvokeHook(
                    hooks,
                    RawEmergencyReserveMutationPointV1::
                        kInventorySynced,
                    complete.prefix_count)) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kInjectedInterruption);
                SetError(
                    error,
                    "interrupted after complete inventory sync");
                return false;
            }

            ScopedFd state_candidate_fd(
                CreatePrivateCandidate(
                    parts.root.get(),
                    state_candidate.c_str(),
                    error));
            if (state_candidate_fd.get() < 0 ||
                !PwriteAll(
                    state_candidate_fd.get(), state_wire) ||
                !AllocateFile(
                    state_candidate_fd.get(),
                    kReserveStateV1FileBytes,
                    error) ||
                !FsyncLoop(state_candidate_fd.get()) ||
                !ValidateStateCandidateFd(
                    parts.root.get(),
                    state_candidate.c_str(),
                    state_candidate_fd.get(),
                    bootstrap.header,
                    state_wire,
                    error)) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::kIoFailure);
                if (error != nullptr && error->empty()) {
                    SetError(
                        error,
                        std::string(
                            "cannot build/sync reserve state candidate: ") +
                            std::strerror(errno));
                }
                return false;
            }
            if (!InvokeHook(
                    hooks,
                    RawEmergencyReserveMutationPointV1::
                        kStateCandidateSynced,
                    0U)) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kInjectedInterruption);
                SetError(
                    error,
                    "interrupted after reserve state candidate sync");
                return false;
            }

            if (!SyncAndValidateProvisionCandidates(
                    &parts,
                    state_candidate_fd.get(),
                    state_candidate.c_str(),
                    bootstrap,
                    state_wire,
                    error)) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kAllocationProofFailure);
                return false;
            }
            RawEmergencyReserveCapacityObservationV1
                provisioned_observation{};
            if (!ProbeCapacity(
                    parts.root.get(),
                    bootstrap.header,
                    RawEmergencyReserveProbeStageV1::
                        kProvisionedCandidate,
                    reserve_bytes,
                    reserve_inodes,
                    capacity_probe,
                    &provisioned_observation,
                    error)) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kCapacityProbeFailure);
                return false;
            }

            if (!PublishNoReplaceAndSync(
                    parts.root.get(),
                    data_candidate.c_str(),
                    kRawEmergencyReserveDataFilename,
                    error) ||
                !ValidateDataFd(
                    parts.root.get(),
                    kRawEmergencyReserveDataFilename,
                    parts.data.get(),
                    bootstrap.header,
                    error)) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kPublishConflict);
                return false;
            }
            if (!InvokeHook(
                    hooks,
                    RawEmergencyReserveMutationPointV1::
                        kDataPublished,
                    0U)) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kInjectedInterruption);
                SetError(
                    error,
                    "interrupted after fixed reserve data publication");
                return false;
            }
            if (!PublishNoReplaceAndSync(
                    parts.root.get(),
                    state_candidate.c_str(),
                    kRawReserveStateFilename,
                    error) ||
                !SameNamedInode(
                    parts.root.get(),
                    kRawReserveStateFilename,
                    state_candidate_fd.get())) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kPublishConflict);
                if (error != nullptr && error->empty()) {
                    SetError(
                        error,
                        "published reserve state changed inode");
                }
                return false;
            }
            if (!InvokeHook(
                    hooks,
                    RawEmergencyReserveMutationPointV1::
                        kStatePublished,
                    0U)) {
                SetFailure(
                    failure,
                    RawEmergencyReservePosixErrorV1::
                        kInjectedInterruption);
                SetError(
                    error,
                    "interrupted after fixed reserve state publication");
                return false;
            }
            return FinishProvisionAttach(
                parts.root.get(),
                bootstrap,
                capacity_probe,
                result,
                failure,
                codec_error,
                error);
        }

        // A fixed data final is the irrevocable publication boundary. It may
        // only be completed by adopting one exact complete matching state
        // candidate after rebuilding all durability barriers.
        if (data_candidate_exists ||
            !state_candidate_exists) {
            SetFailure(
                failure,
                RawEmergencyReservePosixErrorV1::
                    kCandidateConflict);
            SetError(
                error,
                "fixed reserve data lacks the unique matching state candidate or coexists with a data candidate");
            return false;
        }
        parts.data.Reset(
            OpenAtLoop(
                parts.root.get(),
                kRawEmergencyReserveDataFilename,
                O_RDWR | O_NOFOLLOW | O_NONBLOCK |
                    O_CLOEXEC));
        if (parts.data.get() < 0 ||
            !ValidateDataFd(
                parts.root.get(),
                kRawEmergencyReserveDataFilename,
                parts.data.get(),
                bootstrap.header,
                error) ||
            !OpenCompleteFinalInventory(
                &parts, bootstrap.header, error)) {
            SetFailure(
                failure,
                RawEmergencyReservePosixErrorV1::
                    kInventoryMismatch);
            if (parts.data.get() < 0) {
                SetError(
                    error,
                    std::string(
                        "cannot securely reopen fixed reserve data: ") +
                        std::strerror(errno));
            }
            return false;
        }
        ScopedFd state_candidate_fd(
            OpenAtLoop(
                parts.root.get(),
                state_candidate.c_str(),
                O_RDWR | O_NOFOLLOW | O_NONBLOCK |
                    O_CLOEXEC));
        if (state_candidate_fd.get() < 0 ||
            !ValidateStateCandidateFd(
                parts.root.get(),
                state_candidate.c_str(),
                state_candidate_fd.get(),
                bootstrap.header,
                state_wire,
                error) ||
            !SyncAndValidateProvisionCandidates(
                &parts,
                state_candidate_fd.get(),
                state_candidate.c_str(),
                bootstrap,
                state_wire,
                error)) {
            SetFailure(
                failure,
                RawEmergencyReservePosixErrorV1::
                    kStateConflict);
            if (state_candidate_fd.get() < 0) {
                SetError(
                    error,
                    std::string(
                        "cannot securely reopen reserve state candidate: ") +
                        std::strerror(errno));
            }
            return false;
        }
        std::uint64_t reserve_bytes = 0U;
        std::uint64_t reserve_inodes = 0U;
        RawEmergencyReserveCapacityObservationV1
            provisioned_observation{};
        if (!ExpectedReserveCharge(
                bootstrap.header,
                &reserve_bytes,
                &reserve_inodes) ||
            !ProbeCapacity(
                parts.root.get(),
                bootstrap.header,
                RawEmergencyReserveProbeStageV1::
                    kProvisionedCandidate,
                reserve_bytes,
                reserve_inodes,
                capacity_probe,
                &provisioned_observation,
                error)) {
            SetFailure(
                failure,
                RawEmergencyReservePosixErrorV1::
                    kCapacityProbeFailure);
            return false;
        }
        if (!PublishNoReplaceAndSync(
                parts.root.get(),
                state_candidate.c_str(),
                kRawReserveStateFilename,
                error) ||
            !SameNamedInode(
                parts.root.get(),
                kRawReserveStateFilename,
                state_candidate_fd.get())) {
            SetFailure(
                failure,
                RawEmergencyReservePosixErrorV1::
                    kPublishConflict);
            return false;
        }
        return FinishProvisionAttach(
            parts.root.get(),
            bootstrap,
            capacity_probe,
            result,
            failure,
            codec_error,
            error);
    } catch (const std::bad_alloc&) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kAllocationFailure);
        SetError(error, "cannot allocate reserve provision state");
        return false;
    } catch (...) {
        SetFailure(
            failure,
            RawEmergencyReservePosixErrorV1::kAllocationFailure);
        SetError(error, "unexpected reserve provision failure");
        return false;
    }
}

}  // namespace l2flow::ingress

namespace l2flow::ingress {

RawEmergencyReservePosixErrorV1
ReleaseRawEmergencyReserveV1(
    RawEmergencyReserveInventoryV1& inventory,
    RawEmergencyReserveCapacityProbeV1* capacity_probe,
    const RawEmergencyReserveMutationHooksV1* hooks,
    std::string* error) noexcept {
    SetError(error, {});
    if (capacity_probe == nullptr ||
        inventory.phase_ !=
            ReserveCoordinatorPhaseV1::kReleasingPrepared ||
        inventory.root_fd_ < 0 ||
        inventory.inode_directory_fd_ < 0) {
        SetError(
            error,
            capacity_probe == nullptr
                ? "release requires an injected filesystem+quota byte/inode probe"
                : "physical reserve release requires a valid RELEASING_PREPARED capability");
        return RawEmergencyReservePosixErrorV1::kInvalidArgument;
    }
    try {
        if (!ValidateRootFd(inventory.root_fd_, error) ||
            !ValidateInodeDirectoryFd(
                inventory.root_fd_,
                inventory.inode_directory_fd_,
                inventory.header_.device_id,
                error)) {
            return RawEmergencyReservePosixErrorV1::
                kUnsafeInodeDirectory;
        }
        if (inventory.data_fd_ >= 0) {
            if (inventory.remaining_inode_prefix_count_ !=
                    inventory.header_
                        .declared_inode_reserve_count ||
                inventory.inode_fds_.size() !=
                    static_cast<std::size_t>(
                        inventory.remaining_inode_prefix_count_) ||
                !ValidateDataFd(
                    inventory.root_fd_,
                    kRawEmergencyReserveDataFilename,
                    inventory.data_fd_,
                    inventory.header_,
                    error) ||
                !RevalidateInventoryPrefix(
                    inventory.inode_directory_fd_,
                    inventory.header_,
                    inventory.inode_fds_,
                    error)) {
                SetError(
                    error,
                    "data-present PREPARED release lacks complete retained inventory");
                return RawEmergencyReservePosixErrorV1::
                    kReleaseOrderViolation;
            }
            if (!UnlinkAtLoop(
                    inventory.root_fd_,
                    kRawEmergencyReserveDataFilename)) {
                SetError(
                    error,
                    std::string(
                        "cannot unlink fixed reserve data: ") +
                        std::strerror(errno));
                return RawEmergencyReservePosixErrorV1::kIoFailure;
            }
            const int released_fd = inventory.data_fd_;
            inventory.data_fd_ = -1;
            const int close_result = ::close(released_fd);
            const int close_error =
                close_result == 0 ? 0 : errno;
            if (!FsyncLoop(inventory.root_fd_)) {
                SetError(
                    error,
                    std::string(
                        "cannot fsync Raw root after data release: ") +
                        std::strerror(errno));
                return RawEmergencyReservePosixErrorV1::
                    kSyncFailure;
            }
            if (close_result != 0) {
                SetError(
                    error,
                    std::string(
                        "reserve data close did not prove block release: ") +
                        std::strerror(close_error));
                return RawEmergencyReservePosixErrorV1::kIoFailure;
            }
            if (!InvokeHook(
                    hooks,
                    RawEmergencyReserveMutationPointV1::
                        kDataReleased,
                    0U)) {
                SetError(
                    error,
                    "interrupted after reserve data release");
                return RawEmergencyReservePosixErrorV1::
                    kInjectedInterruption;
            }
        } else if (!FsyncLoop(inventory.root_fd_)) {
            SetError(
                error,
                std::string(
                    "cannot rebuild absent-data root barrier: ") +
                    std::strerror(errno));
            return RawEmergencyReservePosixErrorV1::kSyncFailure;
        }

        // Before adopting any absent suffix, rebuild the actual parent
        // barrier and re-enumerate an exact [0,k) prefix.
        if (!FsyncLoop(inventory.inode_directory_fd_)) {
            SetError(
                error,
                std::string(
                    "cannot rebuild inventory suffix barrier: ") +
                    std::strerror(errno));
            return RawEmergencyReservePosixErrorV1::kSyncFailure;
        }
        EnumeratedInventory current{};
        if (!EnumerateFinalInventory(
                inventory.inode_directory_fd_,
                inventory.header_,
                true,
                &current,
                error) ||
            current.prefix_count !=
                inventory.remaining_inode_prefix_count_ ||
            inventory.inode_fds_.size() !=
                static_cast<std::size_t>(
                    current.prefix_count) ||
            !RevalidateInventoryPrefix(
                inventory.inode_directory_fd_,
                inventory.header_,
                inventory.inode_fds_,
                error)) {
            if (error != nullptr && error->empty()) {
                SetError(
                    error,
                    "release restart inventory is not the retained exact prefix");
            }
            return RawEmergencyReservePosixErrorV1::
                kInventoryMismatch;
        }
        while (inventory.remaining_inode_prefix_count_ > 0U) {
            const std::uint32_t index =
                inventory.remaining_inode_prefix_count_ - 1U;
            if (inventory.inode_fds_.size() !=
                static_cast<std::size_t>(index) + 1U) {
                SetError(
                    error,
                    "retained inventory descriptors are not an exact prefix");
                return RawEmergencyReservePosixErrorV1::
                    kInventoryMismatch;
            }
            std::string name;
            if (FormatReserveInodeFilenameV1(
                    inventory.header_.reserve_state_uuid,
                    index,
                    &name) != ReserveHeaderV1Error::kNone ||
                !ValidateInodeFd(
                    inventory.inode_directory_fd_,
                    name.c_str(),
                    inventory.inode_fds_.back(),
                    inventory.header_,
                    index,
                    error)) {
                return RawEmergencyReservePosixErrorV1::
                    kUnsafeInodeFile;
            }
            if (!UnlinkAtLoop(
                    inventory.inode_directory_fd_,
                    name.c_str())) {
                SetError(
                    error,
                    std::string(
                        "cannot unlink descending reserve inode: ") +
                        std::strerror(errno));
                return RawEmergencyReservePosixErrorV1::kIoFailure;
            }
            const int released_fd =
                inventory.inode_fds_.back();
            inventory.inode_fds_.pop_back();
            --inventory.remaining_inode_prefix_count_;
            const int close_result = ::close(released_fd);
            const int close_error =
                close_result == 0 ? 0 : errno;
            if (!FsyncLoop(inventory.inode_directory_fd_)) {
                SetError(
                    error,
                    std::string(
                        "cannot fsync reserve-inodes after descending unlink: ") +
                        std::strerror(errno));
                return RawEmergencyReservePosixErrorV1::
                    kSyncFailure;
            }
            if (close_result != 0) {
                SetError(
                    error,
                    std::string(
                        "reserve inode close did not prove inode release: ") +
                        std::strerror(close_error));
                return RawEmergencyReservePosixErrorV1::kIoFailure;
            }
            if (!InvokeHook(
                    hooks,
                    RawEmergencyReserveMutationPointV1::
                        kInodeReleased,
                    index)) {
                SetError(
                    error,
                    "interrupted after descending reserve inode release");
                return RawEmergencyReservePosixErrorV1::
                    kInjectedInterruption;
            }
        }
        if (!FsyncLoop(inventory.inode_directory_fd_)) {
            SetError(
                error,
                std::string(
                    "cannot fsync empty reserve-inodes directory: ") +
                    std::strerror(errno));
            return RawEmergencyReservePosixErrorV1::kSyncFailure;
        }
        EnumeratedInventory empty{};
        if (!EnumerateFinalInventory(
                inventory.inode_directory_fd_,
                inventory.header_,
                true,
                &empty,
                error) ||
            empty.prefix_count != 0U) {
            SetError(
                error,
                "reserve-inodes is not empty after descending release");
            return RawEmergencyReservePosixErrorV1::
                kInventoryMismatch;
        }

        RawEmergencyReserveCapacityObservationV1 observation{};
        if (!ProbeCapacity(
                inventory.root_fd_,
                inventory.header_,
                RawEmergencyReserveProbeStageV1::kReleased,
                0U,
                0U,
                capacity_probe,
                &observation,
                error)) {
            return RawEmergencyReservePosixErrorV1::
                kCapacityProbeFailure;
        }
        std::uint64_t minimum_fs_bytes = 0U;
        std::uint64_t minimum_quota_bytes = 0U;
        std::uint64_t minimum_fs_inodes = 0U;
        std::uint64_t minimum_quota_inodes = 0U;
        if (!AddChecked(
                inventory.selected_slot_
                    .pre_release_fs_free_bytes,
                inventory.selected_slot_
                    .expected_release_fs_bytes,
                &minimum_fs_bytes) ||
            !AddChecked(
                inventory.selected_slot_
                    .pre_release_quota_free_bytes,
                inventory.selected_slot_
                    .expected_release_quota_bytes,
                &minimum_quota_bytes) ||
            !AddChecked(
                inventory.selected_slot_
                    .pre_release_fs_free_inodes,
                inventory.selected_slot_
                    .expected_release_fs_inodes,
                &minimum_fs_inodes) ||
            !AddChecked(
                inventory.selected_slot_
                    .pre_release_quota_free_inodes,
                inventory.selected_slot_
                    .expected_release_quota_inodes,
                &minimum_quota_inodes)) {
            SetError(
                error,
                "PREPARED release baseline/expected charge overflows");
            return RawEmergencyReservePosixErrorV1::
                kInvalidArgument;
        }
        if (observation.filesystem_free_bytes <
                minimum_fs_bytes ||
            observation.quota_free_bytes <
                minimum_quota_bytes ||
            observation.filesystem_free_inodes <
                minimum_fs_inodes ||
            observation.quota_free_inodes <
                minimum_quota_inodes) {
            SetError(
                error,
                "released reserve did not restore all frozen filesystem/quota byte/inode expectations");
            return RawEmergencyReservePosixErrorV1::
                kCapacityProbeFailure;
        }
        return RawEmergencyReservePosixErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        SetError(error, "cannot allocate reserve release filename");
        return RawEmergencyReservePosixErrorV1::
            kAllocationFailure;
    } catch (...) {
        SetError(error, "unexpected reserve release failure");
        return RawEmergencyReservePosixErrorV1::
            kAllocationFailure;
    }
}

struct RawEmergencyReserveReprovisionAccessV1 final {
    [[nodiscard]] static int RootFd(
        const FinalizationArchiveSourceCleanupReceiptV1&
            receipt) noexcept {
        return receipt.raw_root_fd_;
    }

    [[nodiscard]] static std::uint64_t RootDevice(
        const FinalizationArchiveSourceCleanupReceiptV1&
            receipt) noexcept {
        return receipt.raw_root_device_;
    }

    [[nodiscard]] static std::uint64_t RootInode(
        const FinalizationArchiveSourceCleanupReceiptV1&
            receipt) noexcept {
        return receipt.raw_root_inode_;
    }

    [[nodiscard]] static int LeaseFd(
        const FinalizationArchiveSourceCleanupReceiptV1&
            receipt) noexcept {
        return receipt.maintenance_lease_fd_;
    }

    [[nodiscard]] static std::uint64_t LeaseDevice(
        const FinalizationArchiveSourceCleanupReceiptV1&
            receipt) noexcept {
        return receipt.maintenance_lease_device_;
    }

    [[nodiscard]] static std::uint64_t LeaseInode(
        const FinalizationArchiveSourceCleanupReceiptV1&
            receipt) noexcept {
        return receipt.maintenance_lease_inode_;
    }

    [[nodiscard]] static int OldStateFd(
        const FinalizationArchiveSourceCleanupReceiptV1&
            receipt) noexcept {
        return receipt.state_file_ == nullptr
                   ? -1
                   : receipt.state_file_->descriptor();
    }

    [[nodiscard]] static const ReserveStateV1HeaderWire&
    OldStateHeader(
        const FinalizationArchiveSourceCleanupReceiptV1&
            receipt) noexcept {
        return receipt.frozen_state_header_;
    }

    [[nodiscard]] static const ReserveStateV1SlotWire&
    OldAllDoneSlot(
        const FinalizationArchiveSourceCleanupReceiptV1&
            receipt) noexcept {
        return receipt.frozen_all_done_slot_;
    }

    [[nodiscard]] static bool ValidateArchive(
        const FinalizationArchiveSourceCleanupReceiptV1&
            receipt,
        std::string* diagnostic) noexcept {
        return receipt.archive_receipt_ != nullptr &&
               receipt.archive_receipt_->Validate(diagnostic);
    }

    [[nodiscard]] static const std::vector<
        FinalizationArchiveSourceCleanupReceiptV1::
            RetainedSourceV1>&
    Sources(
        const FinalizationArchiveSourceCleanupReceiptV1&
            receipt) noexcept {
        return receipt.sources_;
    }
};

namespace {

[[nodiscard]] bool SameReprovisionDomain(
    const ReserveCoordinatorHeaderV1& old_header,
    const ReserveCoordinatorHeaderV1& new_header) noexcept {
    return old_header.schema_sha256 ==
               new_header.schema_sha256 &&
           old_header.quota_identity_sha256 ==
               new_header.quota_identity_sha256 &&
           old_header.mount_identity_sha256 ==
               new_header.mount_identity_sha256 &&
           old_header.device_id == new_header.device_id &&
           old_header.declared_releasable_bytes ==
               new_header.declared_releasable_bytes &&
           old_header.allocation_quantum_bytes ==
               new_header.allocation_quantum_bytes &&
           old_header.declared_inode_reserve_count ==
               new_header.declared_inode_reserve_count &&
           old_header.byte_probe_method ==
               new_header.byte_probe_method &&
           old_header.byte_probe_version ==
               new_header.byte_probe_version &&
           old_header.inode_probe_method ==
               new_header.inode_probe_method &&
           old_header.inode_probe_version ==
               new_header.inode_probe_version &&
           old_header.safe_stop_catalog_sha256 ==
               new_header.safe_stop_catalog_sha256;
}

[[nodiscard]] bool InvokeReprovisionHook(
    const RawEmergencyReserveReprovisionHooksV1* hooks,
    RawEmergencyReserveReprovisionMutationPointV1 point,
    std::uint32_t inode_index) noexcept {
    return hooks == nullptr || hooks->after == nullptr ||
           hooks->after(point, inode_index, hooks->context);
}

void ReturnCleanupCapabilityIfStillValid(
    std::unique_ptr<
        FinalizationArchiveSourceCleanupReceiptV1>*
        cleanup_receipt,
    RawEmergencyReserveReprovisionResultV1*
        result) noexcept {
    if (cleanup_receipt == nullptr || result == nullptr ||
        *cleanup_receipt == nullptr ||
        result->state_replace_may_have_occurred) {
        return;
    }
    if ((*cleanup_receipt)->Validate(nullptr)) {
        result->retry_cleanup_receipt =
            std::move(*cleanup_receipt);
    }
}

[[nodiscard]] bool ReadAndDecodeStateFd(
    int fd,
    ReserveStateV1FileWire* wire,
    ReserveCoordinatorStateV1* state) noexcept {
    return fd >= 0 && wire != nullptr && state != nullptr &&
           PreadAll(fd, *wire) &&
           DecodeAndSelectReserveCoordinatorStateV1(
               *wire, state) ==
               ReserveStateV1Error::kNone;
}

[[nodiscard]] bool OldStateEvidenceMatches(
    int old_state_fd,
    const ReserveStateV1HeaderWire& expected_header,
    const ReserveStateV1SlotWire& expected_all_done,
    const ReserveStateV1Identity& expected_uuid,
    std::string* diagnostic) noexcept {
    ReserveStateV1FileWire wire{};
    ReserveCoordinatorStateV1 decoded{};
    if (!ReadAndDecodeStateFd(
            old_state_fd, &wire, &decoded) ||
        decoded.selected_slot >= decoded.slots.size() ||
        decoded.header.reserve_state_uuid != expected_uuid ||
        !std::equal(
            expected_header.begin(),
            expected_header.end(),
            wire.begin(),
            wire.begin() +
                static_cast<std::ptrdiff_t>(
                    kReserveStateV1HeaderBytes))) {
        SetError(
            diagnostic,
            "retained old reserve state no longer contains the archived header");
        return false;
    }
    const std::size_t slot_offset =
        kReserveStateV1HeaderBytes +
        (decoded.selected_slot * kReserveStateV1SlotBytes);
    if (!std::equal(
            expected_all_done.begin(),
            expected_all_done.end(),
            wire.begin() +
                static_cast<std::ptrdiff_t>(slot_offset),
            wire.begin() +
                static_cast<std::ptrdiff_t>(
                    slot_offset +
                    kReserveStateV1SlotBytes))) {
        SetError(
            diagnostic,
            "retained old reserve state no longer contains the archived all-DONE slot");
        return false;
    }
    return true;
}

[[nodiscard]] bool ValidateRetainedRootIdentity(
    const FinalizationArchiveSourceCleanupReceiptV1&
        receipt,
    std::string* diagnostic) noexcept {
    const int root_fd =
        RawEmergencyReserveReprovisionAccessV1::RootFd(
            receipt);
    struct stat status {};
    if (!ValidateRootFd(root_fd, diagnostic) ||
        ::fstat(root_fd, &status) != 0 ||
        static_cast<std::uint64_t>(status.st_dev) !=
            RawEmergencyReserveReprovisionAccessV1::
                RootDevice(receipt) ||
        static_cast<std::uint64_t>(status.st_ino) !=
            RawEmergencyReserveReprovisionAccessV1::
                RootInode(receipt)) {
        SetError(
            diagnostic,
            "cleanup capability Raw-root identity changed");
        return false;
    }
    return true;
}

[[nodiscard]] bool ValidateRetainedLeaseUnchanged(
    const FinalizationArchiveSourceCleanupReceiptV1&
        receipt,
    const RawReserveCoordinatorLeaseMarkerWireV1&
        expected_wire,
    std::string* diagnostic) noexcept {
    const int root_fd =
        RawEmergencyReserveReprovisionAccessV1::RootFd(
            receipt);
    const int lease_fd =
        RawEmergencyReserveReprovisionAccessV1::LeaseFd(
            receipt);
    struct stat lease {};
    RawReserveCoordinatorLeaseMarkerWireV1 wire{};
    RawReserveCoordinatorLeaseMarkerV1 decoded{};
    if (lease_fd < 0 || ::fstat(lease_fd, &lease) != 0 ||
        !IsExactPrivateRegular(
            lease,
            RawEmergencyReserveReprovisionAccessV1::
                RootDevice(receipt)) ||
        static_cast<std::uint64_t>(lease.st_dev) !=
            RawEmergencyReserveReprovisionAccessV1::
                LeaseDevice(receipt) ||
        static_cast<std::uint64_t>(lease.st_ino) !=
            RawEmergencyReserveReprovisionAccessV1::
                LeaseInode(receipt) ||
        lease.st_size !=
            static_cast<off_t>(
                kRawReserveCoordinatorLeaseMarkerBytes) ||
        !SameNamedInode(
            root_fd,
            kRawReserveCoordinatorLeaseFilename,
            lease_fd) ||
        !PreadAll(lease_fd, wire) ||
        wire != expected_wire ||
        !DecodeRawReserveCoordinatorLeaseMarkerV1(
            wire, &decoded)) {
        SetError(
            diagnostic,
            "persistent coordinator lease inode or marker changed during reprovision");
        return false;
    }
    return true;
}

[[nodiscard]] bool ValidateRetainedSourceAbsence(
    const FinalizationArchiveSourceCleanupReceiptV1&
        receipt,
    std::string* diagnostic) noexcept {
    for (const auto& source :
         RawEmergencyReserveReprovisionAccessV1::Sources(
             receipt)) {
        struct stat parent {};
        struct stat named {};
        if (source.parent_directory_fd < 0 ||
            ::fstat(
                source.parent_directory_fd, &parent) != 0 ||
            !S_ISDIR(parent.st_mode) ||
            parent.st_uid != ::geteuid() ||
            (parent.st_mode & 07777U) != 0700U ||
            static_cast<std::uint64_t>(parent.st_dev) !=
                source.parent_device ||
            static_cast<std::uint64_t>(parent.st_ino) !=
                source.parent_inode) {
            SetError(
                diagnostic,
                "retained source-report parent changed after cleanup");
            return false;
        }
        if (::fstatat(
                source.parent_directory_fd,
                source.filename.c_str(),
                &named,
                AT_SYMLINK_NOFOLLOW) == 0 ||
            errno != ENOENT) {
            SetError(
                diagnostic,
                "a cleaned source-report name is present again");
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool ReadLeaseMarkerWire(
    const FinalizationArchiveSourceCleanupReceiptV1&
        receipt,
    RawReserveCoordinatorLeaseMarkerWireV1* wire,
    std::string* diagnostic) noexcept {
    if (wire == nullptr ||
        !PreadAll(
            RawEmergencyReserveReprovisionAccessV1::
                LeaseFd(receipt),
            *wire)) {
        SetError(
            diagnostic,
            std::string(
                "cannot read persistent coordinator lease marker: ") +
                std::strerror(errno));
        return false;
    }
    RawReserveCoordinatorLeaseMarkerV1 decoded{};
    if (!DecodeRawReserveCoordinatorLeaseMarkerV1(
            *wire, &decoded)) {
        SetError(
            diagnostic,
            "persistent coordinator lease marker is invalid");
        return false;
    }
    return true;
}

[[nodiscard]] bool ValidateFixedStateWire(
    int root_fd,
    int retained_state_fd,
    const ReserveStateV1FileWire& expected_wire,
    const ReserveCoordinatorStateV1& expected_state,
    std::string* diagnostic) noexcept {
    struct stat status {};
    ReserveStateV1FileWire actual{};
    ReserveCoordinatorStateV1 decoded{};
    const bool valid =
        retained_state_fd >= 0 &&
        ::fstat(retained_state_fd, &status) == 0 &&
        IsExactPrivateRegular(
            status, expected_state.header.device_id) &&
        status.st_size ==
            static_cast<off_t>(
                kReserveStateV1FileBytes) &&
        SameNamedInode(
            root_fd,
            kRawReserveStateFilename,
            retained_state_fd) &&
        PreadAll(retained_state_fd, actual) &&
        actual == expected_wire &&
        DecodeAndSelectReserveCoordinatorStateV1(
            actual, &decoded) ==
            ReserveStateV1Error::kNone &&
        decoded == expected_state;
    if (!valid) {
        SetError(
            diagnostic,
            "fixed reserve.state is not the exact new bootstrap inode and wire image");
    }
    return valid;
}

[[nodiscard]] bool AtomicReplaceState(
    int root_fd,
    const char* state_candidate,
    std::string* diagnostic) noexcept {
    if (::renameat(
            root_fd,
            state_candidate,
            root_fd,
            kRawReserveStateFilename) != 0) {
        SetError(
            diagnostic,
            std::string(
                "atomic fixed-state replacement failed: ") +
                std::strerror(errno));
        return false;
    }
    return true;
}

[[nodiscard]] bool ParseUuidBoundRootCandidate(
    std::string_view name,
    std::string_view prefix,
    std::string_view suffix,
    ReserveStateV1Identity* uuid) noexcept {
    if (uuid == nullptr || !StartsWith(name, prefix) ||
        !EndsWith(name, suffix) ||
        name.size() !=
            prefix.size() + 32U + suffix.size()) {
        return false;
    }
    const std::string_view uuid_text =
        name.substr(prefix.size(), 32U);
    ReserveStateV1Identity parsed{};
    if (!l2flow::common::ParseIdentity128Hex(
            uuid_text, &parsed) ||
        l2flow::common::Identity128Hex(parsed) !=
            uuid_text) {
        return false;
    }
    *uuid = parsed;
    return true;
}

[[nodiscard]] bool MergeDiscoveredUuid(
    const ReserveStateV1Identity& old_uuid,
    const ReserveStateV1Identity& observed,
    bool* have_uuid,
    ReserveStateV1Identity* candidate_uuid,
    std::string* diagnostic) noexcept {
    if (have_uuid == nullptr || candidate_uuid == nullptr ||
        l2flow::common::IsZeroIdentity(observed) ||
        observed == old_uuid) {
        SetError(
            diagnostic,
            "reprovision candidate uses the old, zero, or invalid reserve UUID");
        return false;
    }
    if (!*have_uuid) {
        *have_uuid = true;
        *candidate_uuid = observed;
        return true;
    }
    if (*candidate_uuid != observed) {
        SetError(
            diagnostic,
            "durable reprovision artifacts contain multiple reserve UUIDs");
        return false;
    }
    return true;
}

struct DiscoveredInodeArtifact final {
    std::string name;
};

[[nodiscard]] bool ReadReserveDataCandidateUuid(
    int root_fd,
    const char* name,
    const ReserveCoordinatorHeaderV1& old_header,
    ReserveStateV1Identity* uuid,
    std::string* diagnostic) noexcept {
    ScopedFd fd(
        OpenAtLoop(
            root_fd,
            name,
            O_RDWR | O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC));
    struct stat status {};
    ReserveHeaderWireV1 wire{};
    ReserveFileHeaderV1 decoded{};
    std::uint64_t expected_data_bytes = 0U;
    if (!ReserveDataDeclaredBytes(
            old_header, &expected_data_bytes) ||
        fd.get() < 0 ||
        ::fstat(fd.get(), &status) != 0 ||
        !IsExactPrivateRegular(
            status, old_header.device_id) ||
        !ValidateOpenFlags(fd.get()) ||
        status.st_size < 0 ||
        static_cast<std::uint64_t>(status.st_size) !=
            expected_data_bytes ||
        !SameNamedInode(root_fd, name, fd.get()) ||
        !PreadAll(fd.get(), wire) ||
        DecodeReserveFileHeaderV1(wire, &decoded) !=
            ReserveHeaderV1Error::kNone ||
        decoded.declared_bytes !=
            expected_data_bytes ||
        decoded.pool.device_id != old_header.device_id ||
        decoded.pool.quota_identity_sha256 !=
            old_header.quota_identity_sha256 ||
        decoded.pool.mount_identity_sha256 !=
            old_header.mount_identity_sha256) {
        SetError(
            diagnostic,
            "durable reprovision data artifact is unsafe or outside the old allocation domain");
        return false;
    }
    *uuid = decoded.pool.reserve_state_uuid;
    return true;
}

}  // namespace

std::string_view
RawEmergencyReserveReprovisionErrorV1Name(
    RawEmergencyReserveReprovisionErrorV1 error) noexcept {
    switch (error) {
        case RawEmergencyReserveReprovisionErrorV1::kNone:
            return "none";
        case RawEmergencyReserveReprovisionErrorV1::
            kInvalidArgument:
            return "invalid_argument";
        case RawEmergencyReserveReprovisionErrorV1::
            kCleanupBarrierInvalid:
            return "cleanup_barrier_invalid";
        case RawEmergencyReserveReprovisionErrorV1::
            kBootstrapInvalid:
            return "bootstrap_invalid";
        case RawEmergencyReserveReprovisionErrorV1::
            kOldStateChanged:
            return "old_state_changed";
        case RawEmergencyReserveReprovisionErrorV1::kUnsafeRoot:
            return "unsafe_root";
        case RawEmergencyReserveReprovisionErrorV1::
            kNoDurableCandidate:
            return "no_durable_candidate";
        case RawEmergencyReserveReprovisionErrorV1::
            kCandidateConflict:
            return "candidate_conflict";
        case RawEmergencyReserveReprovisionErrorV1::
            kInventoryConflict:
            return "inventory_conflict";
        case RawEmergencyReserveReprovisionErrorV1::
            kAllocationProofFailure:
            return "allocation_proof_failure";
        case RawEmergencyReserveReprovisionErrorV1::
            kCapacityProbeFailure:
            return "capacity_probe_failure";
        case RawEmergencyReserveReprovisionErrorV1::
            kIoFailure:
            return "io_failure";
        case RawEmergencyReserveReprovisionErrorV1::
            kSyncFailure:
            return "sync_failure";
        case RawEmergencyReserveReprovisionErrorV1::
            kPublishConflict:
            return "publish_conflict";
        case RawEmergencyReserveReprovisionErrorV1::
            kInjectedInterruption:
            return "injected_interruption";
        case RawEmergencyReserveReprovisionErrorV1::
            kPostReplaceFailStop:
            return "post_replace_fail_stop";
        case RawEmergencyReserveReprovisionErrorV1::
            kAllocationFailure:
            return "allocation_failure";
    }
    return "unknown";
}

RawEmergencyReservePosixErrorV1
BuildRawEmergencyReserveReprovisionBootstrapV1(
    const ReserveCoordinatorHeaderV1& old_header,
    const ReserveStateV1Identity& new_reserve_state_uuid,
    ReserveCoordinatorStateV1* bootstrap,
    ReserveStateV1Error* codec_error,
    std::string* diagnostic) noexcept {
    SetCodecError(codec_error, ReserveStateV1Error::kNone);
    SetError(diagnostic, {});
    if (bootstrap == nullptr ||
        l2flow::common::IsZeroIdentity(
            new_reserve_state_uuid) ||
        new_reserve_state_uuid ==
            old_header.reserve_state_uuid) {
        SetError(
            diagnostic,
            "reprovision bootstrap requires a nonzero new reserve UUID");
        return RawEmergencyReservePosixErrorV1::
            kInvalidArgument;
    }
    ReserveCoordinatorStateV1 candidate{};
    candidate.header = old_header;
    candidate.header.reserve_state_uuid =
        new_reserve_state_uuid;
    candidate.header.inode_inventory_sha256 = {};
    const RawEmergencyReservePosixErrorV1
        inventory_error =
            ComputeRawEmergencyReserveInventorySha256V1(
                candidate.header,
                &candidate.header
                     .inode_inventory_sha256,
                diagnostic);
    if (inventory_error !=
        RawEmergencyReservePosixErrorV1::kNone) {
        return inventory_error;
    }
    ReserveStateSlotV1 slot{};
    slot.coordinator_state =
        ReserveCoordinatorPhaseV1::kProvisioned;
    slot.generation = 1U;
    slot.reserve_state_uuid = new_reserve_state_uuid;
    candidate.slots[0U] = slot;
    candidate.slots[1U] = slot;
    candidate.selected_slot = 0U;
    ReserveStateV1FileWire wire{};
    const ReserveStateV1Error encoded =
        EncodeReserveCoordinatorStateV1(
            candidate, &wire);
    SetCodecError(codec_error, encoded);
    if (encoded != ReserveStateV1Error::kNone) {
        SetError(
            diagnostic,
            std::string(
                "generated reprovision bootstrap is invalid: ") +
                std::string(
                    ReserveStateV1ErrorName(encoded)));
        return RawEmergencyReservePosixErrorV1::
            kCodecFailure;
    }
    *bootstrap = candidate;
    return RawEmergencyReservePosixErrorV1::kNone;
}

RawEmergencyReserveReprovisionDiscoveryResultV1
DiscoverRawEmergencyReserveReprovisionCandidateV1(
    const FinalizationArchiveSourceCleanupReceiptV1&
        cleanup_receipt,
    std::uint64_t metadata_margin_bytes,
    std::uint64_t metadata_margin_inodes,
    std::string* diagnostic) noexcept {
    RawEmergencyReserveReprovisionDiscoveryResultV1
        result{};
    SetError(diagnostic, {});
    if (metadata_margin_bytes == 0U ||
        metadata_margin_inodes == 0U) {
        result.error =
            RawEmergencyReserveReprovisionErrorV1::
                kInvalidArgument;
        SetError(
            diagnostic,
            "candidate discovery requires nonzero reprovision metadata margins");
        return result;
    }
    try {
        if (!cleanup_receipt.Validate(diagnostic)) {
            result.error =
                RawEmergencyReserveReprovisionErrorV1::
                    kCleanupBarrierInvalid;
            return result;
        }
        ReserveCoordinatorHeaderV1 old_header{};
        if (DecodeReserveCoordinatorHeaderV1(
                RawEmergencyReserveReprovisionAccessV1::
                    OldStateHeader(cleanup_receipt),
                &old_header) !=
            ReserveStateV1Error::kNone) {
            result.error =
                RawEmergencyReserveReprovisionErrorV1::
                    kCleanupBarrierInvalid;
            SetError(
                diagnostic,
                "cleanup capability old-state header cannot be decoded");
            return result;
        }
        const int root_fd =
            RawEmergencyReserveReprovisionAccessV1::
                RootFd(cleanup_receipt);
        if (!ValidateRetainedRootIdentity(
                cleanup_receipt, diagnostic)) {
            result.error =
                RawEmergencyReserveReprovisionErrorV1::
                    kUnsafeRoot;
            return result;
        }

        std::vector<std::string> root_names;
        if (!ListDirectoryNames(
                root_fd, &root_names, diagnostic)) {
            result.error =
                RawEmergencyReserveReprovisionErrorV1::
                    kIoFailure;
            return result;
        }
        bool have_uuid = false;
        ReserveStateV1Identity candidate_uuid{};
        bool have_data_candidate = false;
        bool have_state_candidate = false;
        std::string data_candidate_name;
        std::string state_candidate_name;
        for (const std::string& name : root_names) {
            if (StartsWith(name, kDataCandidatePrefix)) {
                ReserveStateV1Identity parsed{};
                if (have_data_candidate ||
                    !ParseUuidBoundRootCandidate(
                        name,
                        kDataCandidatePrefix,
                        kDataCandidateSuffix,
                        &parsed) ||
                    !MergeDiscoveredUuid(
                        old_header.reserve_state_uuid,
                        parsed,
                        &have_uuid,
                        &candidate_uuid,
                        diagnostic)) {
                    result.error =
                        RawEmergencyReserveReprovisionErrorV1::
                            kCandidateConflict;
                    return result;
                }
                have_data_candidate = true;
                data_candidate_name = name;
            } else if (
                StartsWith(name, kStateCandidatePrefix)) {
                ReserveStateV1Identity parsed{};
                if (have_state_candidate ||
                    !ParseUuidBoundRootCandidate(
                        name,
                        kStateCandidatePrefix,
                        kStateCandidateSuffix,
                        &parsed) ||
                    !MergeDiscoveredUuid(
                        old_header.reserve_state_uuid,
                        parsed,
                        &have_uuid,
                        &candidate_uuid,
                        diagnostic)) {
                    result.error =
                        RawEmergencyReserveReprovisionErrorV1::
                            kCandidateConflict;
                    return result;
                }
                have_state_candidate = true;
                state_candidate_name = name;
            }
        }

        ScopedFd inode_directory(
            OpenInodeDirectory(
                root_fd,
                old_header.device_id,
                diagnostic));
        if (inode_directory.get() < 0) {
            result.error =
                RawEmergencyReserveReprovisionErrorV1::
                    kInventoryConflict;
            return result;
        }
        std::vector<std::string> inode_names;
        if (!ListDirectoryNames(
                inode_directory.get(),
                &inode_names,
                diagnostic)) {
            result.error =
                RawEmergencyReserveReprovisionErrorV1::
                    kIoFailure;
            return result;
        }
        std::vector<DiscoveredInodeArtifact>
            inode_artifacts;
        inode_artifacts.reserve(inode_names.size());
        for (const std::string& name : inode_names) {
            ReserveStateV1Identity parsed{};
            std::uint32_t index = 0U;
            bool candidate = false;
            if (ParseReserveInodeFilenameV1(
                    name, &parsed, &index) !=
                ReserveHeaderV1Error::kNone) {
                if (!ParseInodeCandidateName(
                        name, &parsed, &index)) {
                    result.error =
                        RawEmergencyReserveReprovisionErrorV1::
                            kCandidateConflict;
                    SetError(
                        diagnostic,
                        "reserve-inodes contains an unknown or malformed reprovision artifact");
                    return result;
                }
                candidate = true;
            }
            if (!MergeDiscoveredUuid(
                    old_header.reserve_state_uuid,
                    parsed,
                    &have_uuid,
                    &candidate_uuid,
                    diagnostic)) {
                result.error =
                    RawEmergencyReserveReprovisionErrorV1::
                        kCandidateConflict;
                return result;
            }
            if (index >=
                old_header.declared_inode_reserve_count) {
                result.error =
                    RawEmergencyReserveReprovisionErrorV1::
                        kCandidateConflict;
                SetError(
                    diagnostic,
                    "reprovision inode candidate index exceeds the immutable inventory count");
                return result;
            }
            std::string binding_name = name;
            if (candidate &&
                FormatReserveInodeFilenameV1(
                    parsed, index, &binding_name) !=
                    ReserveHeaderV1Error::kNone) {
                result.error =
                    RawEmergencyReserveReprovisionErrorV1::
                        kCandidateConflict;
                return result;
            }
            inode_artifacts.push_back(
                DiscoveredInodeArtifact{name});
        }

        bool fixed_data_exists = false;
        if (!NameExists(
                root_fd,
                kRawEmergencyReserveDataFilename,
                &fixed_data_exists)) {
            result.error =
                RawEmergencyReserveReprovisionErrorV1::
                    kIoFailure;
            return result;
        }
        if (fixed_data_exists) {
            ReserveStateV1Identity data_uuid{};
            if (!ReadReserveDataCandidateUuid(
                    root_fd,
                    kRawEmergencyReserveDataFilename,
                    old_header,
                    &data_uuid,
                    diagnostic) ||
                !MergeDiscoveredUuid(
                    old_header.reserve_state_uuid,
                    data_uuid,
                    &have_uuid,
                    &candidate_uuid,
                    diagnostic)) {
                result.error =
                    RawEmergencyReserveReprovisionErrorV1::
                        kCandidateConflict;
                return result;
            }
        }
        if (!have_uuid) {
            result.error =
                RawEmergencyReserveReprovisionErrorV1::
                    kNoDurableCandidate;
            SetError(
                diagnostic,
                "old all-DONE state has no durable reprovision candidate UUID to resume");
            return result;
        }

        result.request.metadata_margin_bytes =
            metadata_margin_bytes;
        result.request.metadata_margin_inodes =
            metadata_margin_inodes;
        ReserveStateV1Error codec_error{};
        if (BuildRawEmergencyReserveReprovisionBootstrapV1(
                old_header,
                candidate_uuid,
                &result.request.bootstrap,
                &codec_error,
                diagnostic) !=
            RawEmergencyReservePosixErrorV1::kNone) {
            result.error =
                RawEmergencyReserveReprovisionErrorV1::
                    kBootstrapInvalid;
            return result;
        }

        if (fixed_data_exists) {
            if (have_data_candidate ||
                !have_state_candidate) {
                result.error =
                    RawEmergencyReserveReprovisionErrorV1::
                        kCandidateConflict;
                SetError(
                    diagnostic,
                    "fixed reprovision data lacks its sole matching state candidate");
                return result;
            }
            ScopedFd data_fd(
                OpenAtLoop(
                    root_fd,
                    kRawEmergencyReserveDataFilename,
                    O_RDWR | O_NOFOLLOW | O_NONBLOCK |
                        O_CLOEXEC));
            ScopedFd state_fd(
                OpenAtLoop(
                    root_fd,
                    state_candidate_name.c_str(),
                    O_RDWR | O_NOFOLLOW | O_NONBLOCK |
                        O_CLOEXEC));
            ReserveStateV1FileWire state_wire{};
            if (EncodeReserveCoordinatorStateV1(
                    result.request.bootstrap,
                    &state_wire) !=
                    ReserveStateV1Error::kNone ||
                data_fd.get() < 0 ||
                !ValidateDataFd(
                    root_fd,
                    kRawEmergencyReserveDataFilename,
                    data_fd.get(),
                    result.request.bootstrap.header,
                    diagnostic) ||
                state_fd.get() < 0 ||
                !ValidateStateCandidateFd(
                    root_fd,
                    state_candidate_name.c_str(),
                    state_fd.get(),
                    result.request.bootstrap.header,
                    state_wire,
                    diagnostic)) {
                result.error =
                    RawEmergencyReserveReprovisionErrorV1::
                        kCandidateConflict;
                return result;
            }
            EnumeratedInventory complete{};
            std::vector<int> inode_fds;
            if (!EnumerateFinalInventory(
                    inode_directory.get(),
                    result.request.bootstrap.header,
                    false,
                    &complete,
                    diagnostic) ||
                complete.prefix_count !=
                    result.request.bootstrap.header
                        .declared_inode_reserve_count ||
                !OpenInventoryPrefix(
                    inode_directory.get(),
                    result.request.bootstrap.header,
                    complete.prefix_count,
                    &inode_fds,
                    diagnostic) ||
                !RevalidateInventoryPrefix(
                    inode_directory.get(),
                    result.request.bootstrap.header,
                    inode_fds,
                    diagnostic)) {
                CloseFdVector(&inode_fds);
                result.error =
                    RawEmergencyReserveReprovisionErrorV1::
                        kInventoryConflict;
                return result;
            }
            CloseFdVector(&inode_fds);
        } else {
            if (have_data_candidate &&
                !ValidateCleanupCandidate(
                    root_fd,
                    data_candidate_name.c_str(),
                    old_header.device_id,
                    diagnostic)) {
                result.error =
                    RawEmergencyReserveReprovisionErrorV1::
                        kCandidateConflict;
                return result;
            }
            if (have_state_candidate &&
                !ValidateCleanupCandidate(
                    root_fd,
                    state_candidate_name.c_str(),
                    old_header.device_id,
                    diagnostic)) {
                result.error =
                    RawEmergencyReserveReprovisionErrorV1::
                        kCandidateConflict;
                return result;
            }
            for (const DiscoveredInodeArtifact& artifact :
                 inode_artifacts) {
                if (!ValidateCleanupCandidate(
                        inode_directory.get(),
                        artifact.name.c_str(),
                        old_header.device_id,
                        diagnostic)) {
                    result.error =
                        RawEmergencyReserveReprovisionErrorV1::
                            kCandidateConflict;
                    return result;
                }
            }
        }
        if (!cleanup_receipt.Validate(diagnostic)) {
            result.error =
                RawEmergencyReserveReprovisionErrorV1::
                    kOldStateChanged;
            return result;
        }
        result.candidate_reserve_state_uuid =
            candidate_uuid;
        result.error =
            RawEmergencyReserveReprovisionErrorV1::kNone;
        return result;
    } catch (const std::bad_alloc&) {
        result.error =
            RawEmergencyReserveReprovisionErrorV1::
                kAllocationFailure;
        SetError(
            diagnostic,
            "cannot allocate reprovision candidate discovery state");
        return result;
    } catch (...) {
        result.error =
            RawEmergencyReserveReprovisionErrorV1::
                kAllocationFailure;
        SetError(
            diagnostic,
            "unexpected reprovision candidate discovery failure");
        return result;
    }
}

RawEmergencyReserveReprovisionReceiptV1::
    RawEmergencyReserveReprovisionReceiptV1(
        std::unique_ptr<
            FinalizationArchiveSourceCleanupReceiptV1>
            cleanup_receipt,
        ReserveStateV1Identity old_reserve_state_uuid,
        ReserveStateV1Identity new_reserve_state_uuid,
        ReserveStateV1HeaderWire old_state_header,
        ReserveStateV1SlotWire old_all_done_slot,
        RawReserveCoordinatorLeaseMarkerWireV1
            maintenance_lease_marker,
        ReserveStateV1FileWire new_state_wire,
        ReserveCoordinatorStateV1 bootstrap,
        std::unique_ptr<RawReserveStateFileV1>
            new_state_file,
        std::unique_ptr<RawEmergencyReserveInventoryV1>
            new_inventory) noexcept
    : cleanup_receipt_(std::move(cleanup_receipt)),
      old_reserve_state_uuid_(
          old_reserve_state_uuid),
      new_reserve_state_uuid_(
          new_reserve_state_uuid),
      old_state_header_(old_state_header),
      old_all_done_slot_(old_all_done_slot),
      maintenance_lease_marker_(
          maintenance_lease_marker),
      new_state_wire_(new_state_wire),
      bootstrap_(std::move(bootstrap)),
      new_state_file_(std::move(new_state_file)),
      new_inventory_(std::move(new_inventory)) {}

RawEmergencyReserveReprovisionReceiptV1::
    ~RawEmergencyReserveReprovisionReceiptV1() = default;

struct RawEmergencyReserveReprovisionEngineV1 final {
    [[nodiscard]] static std::unique_ptr<
        RawEmergencyReserveReprovisionReceiptV1>
    MakeReceipt(
        std::unique_ptr<
            FinalizationArchiveSourceCleanupReceiptV1>
            cleanup_receipt,
        ReserveStateV1Identity old_reserve_state_uuid,
        ReserveStateV1Identity new_reserve_state_uuid,
        ReserveStateV1HeaderWire old_state_header,
        ReserveStateV1SlotWire old_all_done_slot,
        RawReserveCoordinatorLeaseMarkerWireV1
            maintenance_lease_marker,
        ReserveStateV1FileWire new_state_wire,
        ReserveCoordinatorStateV1 bootstrap,
        std::unique_ptr<RawReserveStateFileV1>
            new_state_file,
        std::unique_ptr<RawEmergencyReserveInventoryV1>
            new_inventory) noexcept {
        return std::unique_ptr<
            RawEmergencyReserveReprovisionReceiptV1>(
            new (std::nothrow)
                RawEmergencyReserveReprovisionReceiptV1(
                    std::move(cleanup_receipt),
                    old_reserve_state_uuid,
                    new_reserve_state_uuid,
                    old_state_header,
                    old_all_done_slot,
                    maintenance_lease_marker,
                    new_state_wire,
                    std::move(bootstrap),
                    std::move(new_state_file),
                    std::move(new_inventory)));
    }
};

int RawEmergencyReserveReprovisionReceiptV1::
    retained_old_state_descriptor() const noexcept {
    return cleanup_receipt_ == nullptr
               ? -1
               : RawEmergencyReserveReprovisionAccessV1::
                     OldStateFd(*cleanup_receipt_);
}

bool RawEmergencyReserveReprovisionReceiptV1::Validate(
    std::string* diagnostic) const noexcept {
    SetError(diagnostic, {});
    try {
        if (cleanup_receipt_ == nullptr ||
            new_state_file_ == nullptr ||
            new_inventory_ == nullptr ||
            old_reserve_state_uuid_ ==
                new_reserve_state_uuid_ ||
            l2flow::common::IsZeroIdentity(
                new_reserve_state_uuid_) ||
            bootstrap_.header.reserve_state_uuid !=
                new_reserve_state_uuid_ ||
            !IsFreshBootstrap(bootstrap_) ||
            new_state_file_->state() != bootstrap_ ||
            new_inventory_->header() !=
                bootstrap_.header ||
            new_inventory_->phase() !=
                ReserveCoordinatorPhaseV1::kProvisioned ||
            !new_inventory_->data_present() ||
            new_inventory_
                    ->remaining_inode_prefix_count() !=
                bootstrap_.header
                    .declared_inode_reserve_count ||
            !ValidateRetainedRootIdentity(
                *cleanup_receipt_, diagnostic) ||
            !RawEmergencyReserveReprovisionAccessV1::
                ValidateArchive(
                    *cleanup_receipt_, diagnostic) ||
            !ValidateRetainedLeaseUnchanged(
                *cleanup_receipt_,
                maintenance_lease_marker_,
                diagnostic) ||
            !ValidateRetainedSourceAbsence(
                *cleanup_receipt_, diagnostic) ||
            !OldStateEvidenceMatches(
                retained_old_state_descriptor(),
                old_state_header_,
                old_all_done_slot_,
                old_reserve_state_uuid_,
                diagnostic)) {
            if (diagnostic != nullptr &&
                diagnostic->empty()) {
                SetError(
                    diagnostic,
                    "reprovision receipt capability or immutable binding changed");
            }
            return false;
        }
        const int root_fd =
            RawEmergencyReserveReprovisionAccessV1::
                RootFd(*cleanup_receipt_);
        struct stat old_state {};
        struct stat fixed_state {};
        if (::fstat(
                retained_old_state_descriptor(),
                &old_state) != 0 ||
            ::fstat(
                new_state_file_->descriptor(),
                &fixed_state) != 0 ||
            old_state.st_dev != fixed_state.st_dev ||
            old_state.st_ino == fixed_state.st_ino ||
            !S_ISREG(old_state.st_mode) ||
            old_state.st_uid != ::geteuid() ||
            (old_state.st_mode & 07777U) != 0600U ||
            old_state.st_nlink != static_cast<nlink_t>(0) ||
            old_state.st_size !=
                static_cast<off_t>(
                    kReserveStateV1FileBytes) ||
            !ValidateFixedStateWire(
                root_fd,
                new_state_file_->descriptor(),
                new_state_wire_,
                bootstrap_,
                diagnostic) ||
            !ValidateInodeDirectoryFd(
                root_fd,
                new_inventory_
                    ->inode_directory_descriptor(),
                bootstrap_.header.device_id,
                diagnostic) ||
            !ValidateDataFd(
                root_fd,
                kRawEmergencyReserveDataFilename,
                new_inventory_->data_descriptor(),
                bootstrap_.header,
                diagnostic) ||
            !RevalidateInventoryPrefix(
                new_inventory_
                    ->inode_directory_descriptor(),
                bootstrap_.header,
                new_inventory_->inode_descriptors(),
                diagnostic)) {
            if (diagnostic != nullptr &&
                diagnostic->empty()) {
                SetError(
                    diagnostic,
                    "new reserve state/data/inventory readback changed");
            }
            return false;
        }
        const std::string data_candidate =
            DataCandidateName(new_reserve_state_uuid_);
        const std::string state_candidate =
            StateCandidateName(new_reserve_state_uuid_);
        bool data_candidate_exists = false;
        bool state_candidate_exists = false;
        EnumeratedInventory complete{};
        if (!ScanRootCandidateGrammar(
                root_fd,
                data_candidate,
                state_candidate,
                diagnostic) ||
            !NameExists(
                root_fd,
                data_candidate.c_str(),
                &data_candidate_exists) ||
            !NameExists(
                root_fd,
                state_candidate.c_str(),
                &state_candidate_exists) ||
            data_candidate_exists ||
            state_candidate_exists ||
            !EnumerateFinalInventory(
                new_inventory_
                    ->inode_directory_descriptor(),
                bootstrap_.header,
                false,
                &complete,
                diagnostic) ||
            complete.prefix_count !=
                bootstrap_.header
                    .declared_inode_reserve_count) {
            if (diagnostic != nullptr &&
                diagnostic->empty()) {
                SetError(
                    diagnostic,
                    "published reprovision set has a candidate, hole, or conflict");
            }
            return false;
        }
        return true;
    } catch (...) {
        SetError(
            diagnostic,
            "unexpected reprovision receipt validation failure");
        return false;
    }
}

RawEmergencyReserveReprovisionResultV1
ReprovisionRawEmergencyReserveV1(
    std::unique_ptr<
        FinalizationArchiveSourceCleanupReceiptV1>
        cleanup_receipt,
    const RawEmergencyReserveReprovisionRequestV1& request,
    RawEmergencyReserveCapacityProbeV1* capacity_probe,
    const RawEmergencyReserveReprovisionHooksV1* hooks,
    std::string* diagnostic) noexcept {
    RawEmergencyReserveReprovisionResultV1 result{};
    SetError(diagnostic, {});
    auto fail_before_replace =
        [&](RawEmergencyReserveReprovisionErrorV1 error_value)
        -> RawEmergencyReserveReprovisionResultV1 {
        result.error = error_value;
        ReturnCleanupCapabilityIfStillValid(
            &cleanup_receipt, &result);
        return std::move(result);
    };
    auto fail_after_replace =
        [&]() -> RawEmergencyReserveReprovisionResultV1 {
        result.error =
            RawEmergencyReserveReprovisionErrorV1::
                kPostReplaceFailStop;
        result.state_replace_may_have_occurred = true;
        result.fail_stop_required = true;
        cleanup_receipt.reset();
        result.retry_cleanup_receipt.reset();
        result.receipt.reset();
        return std::move(result);
    };

    if (cleanup_receipt == nullptr ||
        capacity_probe == nullptr ||
        request.metadata_margin_bytes == 0U ||
        request.metadata_margin_inodes == 0U) {
        SetError(
            diagnostic,
            "reprovision requires cleanup capability, four-dimensional probe, and nonzero metadata margins");
        return fail_before_replace(
            RawEmergencyReserveReprovisionErrorV1::
                kInvalidArgument);
    }

    try {
        if (!cleanup_receipt->Validate(diagnostic)) {
            return fail_before_replace(
                RawEmergencyReserveReprovisionErrorV1::
                    kCleanupBarrierInvalid);
        }
        const int root_fd =
            RawEmergencyReserveReprovisionAccessV1::
                RootFd(*cleanup_receipt);
        const int old_state_fd =
            RawEmergencyReserveReprovisionAccessV1::
                OldStateFd(*cleanup_receipt);
        ReserveCoordinatorHeaderV1 old_header{};
        if (DecodeReserveCoordinatorHeaderV1(
                RawEmergencyReserveReprovisionAccessV1::
                    OldStateHeader(*cleanup_receipt),
                &old_header) !=
                ReserveStateV1Error::kNone ||
            old_header.reserve_state_uuid !=
                cleanup_receipt->reserve_state_uuid()) {
            SetError(
                diagnostic,
                "cleanup capability contains invalid old-state header evidence");
            return fail_before_replace(
                RawEmergencyReserveReprovisionErrorV1::
                    kCleanupBarrierInvalid);
        }

        ReserveStateV1Digest expected_inventory{};
        ReserveStateV1FileWire state_wire{};
        const ReserveStateV1Error encoded =
            EncodeReserveCoordinatorStateV1(
                request.bootstrap, &state_wire);
        if (encoded != ReserveStateV1Error::kNone ||
            l2flow::common::IsZeroIdentity(
                request.bootstrap.header
                    .reserve_state_uuid) ||
            request.bootstrap.header.reserve_state_uuid ==
                old_header.reserve_state_uuid ||
            !SameReprovisionDomain(
                old_header, request.bootstrap.header) ||
            !IsFreshBootstrap(request.bootstrap) ||
            ComputeRawEmergencyReserveInventorySha256V1(
                request.bootstrap.header,
                &expected_inventory,
                diagnostic) !=
                RawEmergencyReservePosixErrorV1::kNone ||
            expected_inventory !=
                request.bootstrap.header
                    .inode_inventory_sha256) {
            if (diagnostic != nullptr &&
                diagnostic->empty()) {
                SetError(
                    diagnostic,
                    "requested bootstrap is not the exact new-UUID duplicate generation=1 PROVISIONED state for the old allocation domain");
            }
            return fail_before_replace(
                RawEmergencyReserveReprovisionErrorV1::
                    kBootstrapInvalid);
        }
        if (!ValidateRetainedRootIdentity(
                *cleanup_receipt, diagnostic)) {
            return fail_before_replace(
                RawEmergencyReserveReprovisionErrorV1::
                    kUnsafeRoot);
        }
        struct stat root_status {};
        if (::fstat(root_fd, &root_status) != 0 ||
            static_cast<std::uint64_t>(
                root_status.st_dev) !=
                request.bootstrap.header.device_id) {
            SetError(
                diagnostic,
                "Raw root device differs from reprovision bootstrap");
            return fail_before_replace(
                RawEmergencyReserveReprovisionErrorV1::
                    kUnsafeRoot);
        }

        RawReserveCoordinatorLeaseMarkerWireV1
            lease_marker_wire{};
        if (!ReadLeaseMarkerWire(
                *cleanup_receipt,
                &lease_marker_wire,
                diagnostic)) {
            return fail_before_replace(
                RawEmergencyReserveReprovisionErrorV1::
                    kCleanupBarrierInvalid);
        }

        const std::string data_candidate =
            DataCandidateName(
                request.bootstrap.header
                    .reserve_state_uuid);
        const std::string state_candidate =
            StateCandidateName(
                request.bootstrap.header
                    .reserve_state_uuid);
        if (!ScanRootCandidateGrammar(
                root_fd,
                data_candidate,
                state_candidate,
                diagnostic)) {
            return fail_before_replace(
                RawEmergencyReserveReprovisionErrorV1::
                    kCandidateConflict);
        }

        AttachedInventoryParts parts{};
        parts.root.Reset(RetainRootFd(root_fd, diagnostic));
        parts.inode_directory.Reset(
            OpenInodeDirectory(
                parts.root.get(),
                request.bootstrap.header.device_id,
                diagnostic));
        if (parts.root.get() < 0 ||
            parts.inode_directory.get() < 0) {
            return fail_before_replace(
                RawEmergencyReserveReprovisionErrorV1::
                    kInventoryConflict);
        }

        bool fixed_data_exists = false;
        bool data_candidate_exists = false;
        bool state_candidate_exists = false;
        if (!NameExists(
                parts.root.get(),
                kRawEmergencyReserveDataFilename,
                &fixed_data_exists) ||
            !NameExists(
                parts.root.get(),
                data_candidate.c_str(),
                &data_candidate_exists) ||
            !NameExists(
                parts.root.get(),
                state_candidate.c_str(),
                &state_candidate_exists)) {
            SetError(
                diagnostic,
                "cannot enumerate reprovision publication names");
            return fail_before_replace(
                RawEmergencyReserveReprovisionErrorV1::
                    kIoFailure);
        }

        ScopedFd state_candidate_fd{};
        std::uint64_t reserve_bytes = 0U;
        std::uint64_t reserve_inodes = 0U;
        if (!ExpectedReserveCharge(
                request.bootstrap.header,
                &reserve_bytes,
                &reserve_inodes)) {
            SetError(
                diagnostic,
                "reprovision reserve charge overflows");
            return fail_before_replace(
                RawEmergencyReserveReprovisionErrorV1::
                    kInvalidArgument);
        }

        if (!fixed_data_exists) {
            if (!CleanupUnpublishedCandidateSet(
                    parts.root.get(),
                    parts.inode_directory.get(),
                    request.bootstrap.header,
                    data_candidate,
                    state_candidate,
                    diagnostic)) {
                return fail_before_replace(
                    RawEmergencyReserveReprovisionErrorV1::
                        kCandidateConflict);
            }
            if (!InvokeReprovisionHook(
                    hooks,
                    RawEmergencyReserveReprovisionMutationPointV1::
                        kCandidateSetReset,
                    0U)) {
                if (diagnostic != nullptr &&
                    diagnostic->empty()) {
                    SetError(
                        diagnostic,
                        "interrupted after reprovision candidate-set reset");
                }
                return fail_before_replace(
                    RawEmergencyReserveReprovisionErrorV1::
                        kInjectedInterruption);
            }
            if (!cleanup_receipt->Validate(diagnostic)) {
                return fail_before_replace(
                    RawEmergencyReserveReprovisionErrorV1::
                        kOldStateChanged);
            }
            EnumeratedInventory empty{};
            bool fixed_data_after_cleanup = false;
            if (!EnumerateFinalInventory(
                    parts.inode_directory.get(),
                    old_header,
                    true,
                    &empty,
                    diagnostic) ||
                empty.prefix_count != 0U ||
                !NameExists(
                    parts.root.get(),
                    kRawEmergencyReserveDataFilename,
                    &fixed_data_after_cleanup) ||
                fixed_data_after_cleanup) {
                if (diagnostic != nullptr &&
                    diagnostic->empty()) {
                    SetError(
                        diagnostic,
                        "old reserve data or inode inventory is not empty before reprovision allocation");
                }
                return fail_before_replace(
                    RawEmergencyReserveReprovisionErrorV1::
                        kInventoryConflict);
            }
            std::uint64_t provision_bytes = 0U;
            std::uint64_t provision_inodes = 0U;
            if (!AddChecked(
                    reserve_bytes,
                    kReserveStateV1FileBytes,
                    &provision_bytes) ||
                !AddChecked(
                    provision_bytes,
                    request.metadata_margin_bytes,
                    &provision_bytes) ||
                !AddChecked(
                    reserve_inodes,
                    1U,
                    &provision_inodes) ||
                !AddChecked(
                    provision_inodes,
                    request.metadata_margin_inodes,
                    &provision_inodes)) {
                SetError(
                    diagnostic,
                    "reprovision capacity requirement overflows");
                return fail_before_replace(
                    RawEmergencyReserveReprovisionErrorV1::
                        kInvalidArgument);
            }
            RawEmergencyReserveCapacityObservationV1
                before_observation{};
            if (!ProbeCapacity(
                    parts.root.get(),
                    request.bootstrap.header,
                    RawEmergencyReserveProbeStageV1::
                        kBeforeProvision,
                    provision_bytes,
                    provision_inodes,
                    capacity_probe,
                    &before_observation,
                    diagnostic)) {
                return fail_before_replace(
                    RawEmergencyReserveReprovisionErrorV1::
                        kCapacityProbeFailure);
            }

            parts.data.Reset(
                CreatePrivateCandidate(
                    parts.root.get(),
                    data_candidate.c_str(),
                    diagnostic));
            ReserveFileHeaderV1 data_header{};
            data_header.pool =
                PoolFromHeader(request.bootstrap.header);
            ReserveHeaderWireV1 data_wire{};
            if (parts.data.get() < 0 ||
                !ReserveDataDeclaredBytes(
                    request.bootstrap.header,
                    &data_header.declared_bytes) ||
                EncodeReserveFileHeaderV1(
                    data_header, &data_wire) !=
                    ReserveHeaderV1Error::kNone ||
                !PwriteAll(parts.data.get(), data_wire) ||
                !AllocateFile(
                    parts.data.get(),
                    data_header.declared_bytes,
                    diagnostic) ||
                !ValidateDataFd(
                    parts.root.get(),
                    data_candidate.c_str(),
                    parts.data.get(),
                    request.bootstrap.header,
                    diagnostic) ||
                !FsyncLoop(parts.data.get())) {
                return fail_before_replace(
                    RawEmergencyReserveReprovisionErrorV1::
                        kAllocationProofFailure);
            }
            if (!InvokeReprovisionHook(
                    hooks,
                    RawEmergencyReserveReprovisionMutationPointV1::
                        kDataCandidateSynced,
                    0U)) {
                SetError(
                    diagnostic,
                    "interrupted after reprovision data candidate sync");
                return fail_before_replace(
                    RawEmergencyReserveReprovisionErrorV1::
                        kInjectedInterruption);
            }

            std::uint64_t inode_size = 0U;
            if (!InodeAllocationSize(
                    request.bootstrap.header,
                    &inode_size)) {
                SetError(
                    diagnostic,
                    "reprovision inode allocation size is invalid");
                return fail_before_replace(
                    RawEmergencyReserveReprovisionErrorV1::
                        kInvalidArgument);
            }
            parts.inode_fds.reserve(
                request.bootstrap.header
                    .declared_inode_reserve_count);
            for (std::uint32_t index = 0U;
                 index <
                 request.bootstrap.header
                     .declared_inode_reserve_count;
                 ++index) {
                std::string final_name;
                if (FormatReserveInodeFilenameV1(
                        request.bootstrap.header
                            .reserve_state_uuid,
                        index,
                        &final_name) !=
                    ReserveHeaderV1Error::kNone) {
                    SetError(
                        diagnostic,
                        "cannot format reprovision inode filename");
                    return fail_before_replace(
                        RawEmergencyReserveReprovisionErrorV1::
                            kAllocationFailure);
                }
                const std::string candidate_name =
                    InodeCandidateName(final_name);
                ScopedFd inode_fd(
                    CreatePrivateCandidate(
                        parts.inode_directory.get(),
                        candidate_name.c_str(),
                        diagnostic));
                ReserveInodeHeaderV1 inode_header{};
                inode_header.pool =
                    PoolFromHeader(
                        request.bootstrap.header);
                inode_header.inode_index = index;
                inode_header
                    .declared_inode_reserve_count =
                    request.bootstrap.header
                        .declared_inode_reserve_count;
                ReserveHeaderWireV1 inode_wire{};
                if (inode_fd.get() < 0 ||
                    EncodeReserveInodeHeaderV1(
                        inode_header, &inode_wire) !=
                        ReserveHeaderV1Error::kNone ||
                    !PwriteAll(
                        inode_fd.get(), inode_wire) ||
                    !AllocateFile(
                        inode_fd.get(),
                        inode_size,
                        diagnostic) ||
                    !ValidateInodeFdWithBinding(
                        parts.inode_directory.get(),
                        candidate_name.c_str(),
                        final_name,
                        inode_fd.get(),
                        request.bootstrap.header,
                        index,
                        diagnostic) ||
                    !FsyncLoop(inode_fd.get()) ||
                    RenameNoReplace(
                        parts.inode_directory.get(),
                        candidate_name.c_str(),
                        parts.inode_directory.get(),
                        final_name.c_str()) != 0 ||
                    !FsyncLoop(
                        parts.inode_directory.get()) ||
                    !ValidateInodeFd(
                        parts.inode_directory.get(),
                        final_name.c_str(),
                        inode_fd.get(),
                        request.bootstrap.header,
                        index,
                        diagnostic)) {
                    return fail_before_replace(
                        RawEmergencyReserveReprovisionErrorV1::
                            kAllocationProofFailure);
                }
                parts.inode_fds.push_back(
                    inode_fd.Release());
                parts.prefix_count = index + 1U;
                if (!InvokeReprovisionHook(
                        hooks,
                        RawEmergencyReserveReprovisionMutationPointV1::
                            kInodePublished,
                        index)) {
                    SetError(
                        diagnostic,
                        "interrupted after reprovision inode publication");
                    return fail_before_replace(
                        RawEmergencyReserveReprovisionErrorV1::
                            kInjectedInterruption);
                }
            }
            EnumeratedInventory complete{};
            if (!EnumerateFinalInventory(
                    parts.inode_directory.get(),
                    request.bootstrap.header,
                    false,
                    &complete,
                    diagnostic) ||
                complete.prefix_count !=
                    request.bootstrap.header
                        .declared_inode_reserve_count ||
                !FsyncLoop(
                    parts.inode_directory.get()) ||
                !FsyncLoop(parts.root.get())) {
                return fail_before_replace(
                    RawEmergencyReserveReprovisionErrorV1::
                        kInventoryConflict);
            }
            if (!InvokeReprovisionHook(
                    hooks,
                    RawEmergencyReserveReprovisionMutationPointV1::
                        kInventorySynced,
                    complete.prefix_count)) {
                SetError(
                    diagnostic,
                    "interrupted after reprovision inventory barrier");
                return fail_before_replace(
                    RawEmergencyReserveReprovisionErrorV1::
                        kInjectedInterruption);
            }

            state_candidate_fd.Reset(
                CreatePrivateCandidate(
                    parts.root.get(),
                    state_candidate.c_str(),
                    diagnostic));
            if (state_candidate_fd.get() < 0 ||
                !PwriteAll(
                    state_candidate_fd.get(),
                    state_wire) ||
                !AllocateFile(
                    state_candidate_fd.get(),
                    kReserveStateV1FileBytes,
                    diagnostic) ||
                !FsyncLoop(state_candidate_fd.get()) ||
                !ValidateStateCandidateFd(
                    parts.root.get(),
                    state_candidate.c_str(),
                    state_candidate_fd.get(),
                    request.bootstrap.header,
                    state_wire,
                    diagnostic) ||
                !FsyncLoop(parts.root.get())) {
                return fail_before_replace(
                    RawEmergencyReserveReprovisionErrorV1::
                        kAllocationProofFailure);
            }
            if (!InvokeReprovisionHook(
                    hooks,
                    RawEmergencyReserveReprovisionMutationPointV1::
                        kStateCandidateSynced,
                    0U)) {
                SetError(
                    diagnostic,
                    "interrupted after reprovision state-candidate barrier");
                return fail_before_replace(
                    RawEmergencyReserveReprovisionErrorV1::
                        kInjectedInterruption);
            }
        } else {
            if (data_candidate_exists ||
                !state_candidate_exists) {
                SetError(
                    diagnostic,
                    "fixed new reserve data lacks its unique matching state candidate");
                return fail_before_replace(
                    RawEmergencyReserveReprovisionErrorV1::
                        kCandidateConflict);
            }
            parts.data.Reset(
                OpenAtLoop(
                    parts.root.get(),
                    kRawEmergencyReserveDataFilename,
                    O_RDWR | O_NOFOLLOW | O_NONBLOCK |
                        O_CLOEXEC));
            if (parts.data.get() < 0 ||
                !ValidateDataFd(
                    parts.root.get(),
                    kRawEmergencyReserveDataFilename,
                    parts.data.get(),
                    request.bootstrap.header,
                    diagnostic) ||
                !OpenCompleteFinalInventory(
                    &parts,
                    request.bootstrap.header,
                    diagnostic)) {
                return fail_before_replace(
                    RawEmergencyReserveReprovisionErrorV1::
                        kInventoryConflict);
            }
            state_candidate_fd.Reset(
                OpenAtLoop(
                    parts.root.get(),
                    state_candidate.c_str(),
                    O_RDWR | O_NOFOLLOW | O_NONBLOCK |
                        O_CLOEXEC));
            if (state_candidate_fd.get() < 0 ||
                !ValidateStateCandidateFd(
                    parts.root.get(),
                    state_candidate.c_str(),
                    state_candidate_fd.get(),
                    request.bootstrap.header,
                    state_wire,
                    diagnostic)) {
                return fail_before_replace(
                    RawEmergencyReserveReprovisionErrorV1::
                        kCandidateConflict);
            }
        }

        if (!SyncAndValidateProvisionCandidates(
                &parts,
                state_candidate_fd.get(),
                state_candidate.c_str(),
                request.bootstrap,
                state_wire,
                diagnostic) ||
            !cleanup_receipt->Validate(diagnostic) ||
            !OldStateEvidenceMatches(
                old_state_fd,
                RawEmergencyReserveReprovisionAccessV1::
                    OldStateHeader(*cleanup_receipt),
                RawEmergencyReserveReprovisionAccessV1::
                    OldAllDoneSlot(*cleanup_receipt),
                old_header.reserve_state_uuid,
                diagnostic) ||
            !ValidateRetainedLeaseUnchanged(
                *cleanup_receipt,
                lease_marker_wire,
                diagnostic)) {
            return fail_before_replace(
                RawEmergencyReserveReprovisionErrorV1::
                    kOldStateChanged);
        }
        RawEmergencyReserveCapacityObservationV1
            provisioned_observation{};
        if (!ProbeCapacity(
                parts.root.get(),
                request.bootstrap.header,
                RawEmergencyReserveProbeStageV1::
                    kProvisionedCandidate,
                reserve_bytes,
                reserve_inodes,
                capacity_probe,
                &provisioned_observation,
                diagnostic)) {
            return fail_before_replace(
                RawEmergencyReserveReprovisionErrorV1::
                    kCapacityProbeFailure);
        }

        if (!fixed_data_exists) {
            if (!PublishNoReplaceAndSync(
                    parts.root.get(),
                    data_candidate.c_str(),
                    kRawEmergencyReserveDataFilename,
                    diagnostic) ||
                !ValidateDataFd(
                    parts.root.get(),
                    kRawEmergencyReserveDataFilename,
                    parts.data.get(),
                    request.bootstrap.header,
                    diagnostic)) {
                return fail_before_replace(
                    RawEmergencyReserveReprovisionErrorV1::
                        kPublishConflict);
            }
            if (!InvokeReprovisionHook(
                    hooks,
                    RawEmergencyReserveReprovisionMutationPointV1::
                        kDataPublished,
                    0U)) {
                SetError(
                    diagnostic,
                    "interrupted after fixed reprovision data publication");
                return fail_before_replace(
                    RawEmergencyReserveReprovisionErrorV1::
                        kInjectedInterruption);
            }
        }

        if (!cleanup_receipt->Validate(diagnostic)) {
            return fail_before_replace(
                RawEmergencyReserveReprovisionErrorV1::
                    kOldStateChanged);
        }
        if (!SyncAndValidateProvisionCandidates(
                &parts,
                state_candidate_fd.get(),
                state_candidate.c_str(),
                request.bootstrap,
                state_wire,
                diagnostic)) {
            return fail_before_replace(
                RawEmergencyReserveReprovisionErrorV1::
                    kAllocationProofFailure);
        }
        if (!InvokeReprovisionHook(
                hooks,
                RawEmergencyReserveReprovisionMutationPointV1::
                    kBeforeStateReplace,
                0U)) {
            if (diagnostic != nullptr &&
                diagnostic->empty()) {
                SetError(
                    diagnostic,
                    "interrupted before atomic reserve-state replacement");
            }
            return fail_before_replace(
                RawEmergencyReserveReprovisionErrorV1::
                    kInjectedInterruption);
        }

        result.state_replace_may_have_occurred = true;
        if (!AtomicReplaceState(
                parts.root.get(),
                state_candidate.c_str(),
                diagnostic) ||
            !FsyncLoop(parts.root.get()) ||
            !InvokeReprovisionHook(
                hooks,
                RawEmergencyReserveReprovisionMutationPointV1::
                    kStateReplaced,
                0U)) {
            if (diagnostic != nullptr &&
                diagnostic->empty()) {
                SetError(
                    diagnostic,
                    "post-replacement barrier failed; state-first restart is required");
            }
            return fail_after_replace();
        }

        RawReserveStatePosixError state_failure =
            RawReserveStatePosixError::kNone;
        ReserveStateV1Error codec_error =
            ReserveStateV1Error::kNone;
        std::unique_ptr<RawReserveStateFileV1>
            new_state_file =
                AttachRawReserveStateAtV1(
                    parts.root.get(),
                    &state_failure,
                    &codec_error,
                    diagnostic);
        if (new_state_file == nullptr ||
            new_state_file->state() !=
                request.bootstrap) {
            return fail_after_replace();
        }
        RawEmergencyReservePosixErrorV1
            inventory_failure =
                RawEmergencyReservePosixErrorV1::kNone;
        std::unique_ptr<RawEmergencyReserveInventoryV1>
            new_inventory =
                AttachRawEmergencyReserveInventoryV1(
                    *new_state_file,
                    capacity_probe,
                    &inventory_failure,
                    diagnostic);
        if (new_inventory == nullptr ||
            !ValidateRetainedLeaseUnchanged(
                *cleanup_receipt,
                lease_marker_wire,
                diagnostic) ||
            !InvokeReprovisionHook(
                hooks,
                RawEmergencyReserveReprovisionMutationPointV1::
                    kReadbackComplete,
                0U)) {
            return fail_after_replace();
        }
        const ReserveStateV1HeaderWire old_state_header =
            RawEmergencyReserveReprovisionAccessV1::
                OldStateHeader(*cleanup_receipt);
        const ReserveStateV1SlotWire old_all_done_slot =
            RawEmergencyReserveReprovisionAccessV1::
                OldAllDoneSlot(*cleanup_receipt);

        std::unique_ptr<
            RawEmergencyReserveReprovisionReceiptV1>
            receipt =
                RawEmergencyReserveReprovisionEngineV1::
                    MakeReceipt(
                        std::move(cleanup_receipt),
                        old_header.reserve_state_uuid,
                        request.bootstrap.header
                            .reserve_state_uuid,
                        old_state_header,
                        old_all_done_slot,
                        lease_marker_wire,
                        state_wire,
                        request.bootstrap,
                        std::move(new_state_file),
                        std::move(new_inventory));
        if (receipt == nullptr ||
            !receipt->Validate(diagnostic)) {
            receipt.reset();
            return fail_after_replace();
        }
        result.error =
            RawEmergencyReserveReprovisionErrorV1::kNone;
        result.fail_stop_required = false;
        result.receipt = std::move(receipt);
        return result;
    } catch (const std::bad_alloc&) {
        SetError(
            diagnostic,
            "cannot allocate offline reprovision transaction state");
        if (result.state_replace_may_have_occurred) {
            return fail_after_replace();
        }
        return fail_before_replace(
            RawEmergencyReserveReprovisionErrorV1::
                kAllocationFailure);
    } catch (...) {
        SetError(
            diagnostic,
            "unexpected offline reprovision failure");
        if (result.state_replace_may_have_occurred) {
            return fail_after_replace();
        }
        return fail_before_replace(
            RawEmergencyReserveReprovisionErrorV1::
                kAllocationFailure);
    }
}

}  // namespace l2flow::ingress
