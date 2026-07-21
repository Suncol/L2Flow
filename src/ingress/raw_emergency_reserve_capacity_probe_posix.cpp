#include "l2flow/ingress/raw_emergency_reserve_capacity_probe_posix.h"

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
#include <type_traits>
#include <utility>
#include <vector>

#include <dirent.h>
#include <fcntl.h>
#include <linux/dqblk_xfs.h>
#include <linux/fs.h>
#include <linux/magic.h>
#include <sys/ioctl.h>
#include <sys/quota.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace l2flow::ingress {
namespace {

inline constexpr mode_t kPrivateFileMode = 0600U;
inline constexpr mode_t kPrivateDirectoryMode = 0700U;
inline constexpr std::uint64_t kStatBlockBytes = 512U;
inline constexpr std::uint64_t kGenericQuotaBlockBytes = 1024U;
inline constexpr std::string_view kDataCandidatePrefix =
    ".reserve.data.";
inline constexpr std::string_view kDataCandidateSuffix =
    ".reserve-file-v1.tmp";

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
            if (fd_ >= 0) {
                static_cast<void>(::close(fd_));
            }
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }

    [[nodiscard]] int get() const noexcept {
        return fd_;
    }

private:
    int fd_ = -1;
};

void SetError(std::string* error, std::string message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        *error = std::move(message);
    } catch (...) {
        try {
            *error = "capacity probe diagnostic allocation failed";
        } catch (...) {
        }
    }
}

[[nodiscard]] bool IsZeroDigest(
    const ReserveHeaderDigestV1& digest) noexcept {
    return std::all_of(
        digest.begin(),
        digest.end(),
        [](std::byte value) noexcept {
            return value == std::byte{0};
        });
}

[[nodiscard]] bool AddChecked(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        right >
            std::numeric_limits<std::uint64_t>::max() -
                left) {
        return false;
    }
    *output = left + right;
    return true;
}

[[nodiscard]] bool MultiplyChecked(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        (left != 0U &&
         right >
             std::numeric_limits<std::uint64_t>::max() /
                 left)) {
        return false;
    }
    *output = left * right;
    return true;
}

template <typename Value>
[[nodiscard]] bool UnsignedToU64(
    Value value,
    std::uint64_t* output) noexcept {
    static_assert(std::is_integral_v<Value>);
    static_assert(std::is_unsigned_v<Value>);
    if (output == nullptr) {
        return false;
    }
    if constexpr (sizeof(Value) > sizeof(std::uint64_t)) {
        if (value >
            static_cast<Value>(
                std::numeric_limits<std::uint64_t>::max())) {
            return false;
        }
    }
    *output = static_cast<std::uint64_t>(value);
    return true;
}

[[nodiscard]] bool ValidSubjects(
    std::span<const RawEmergencyReserveQuotaSubjectV1>
        subjects) noexcept {
    if (subjects.empty() || subjects.size() > 3U) {
        return false;
    }
    std::uint8_t previous = 0U;
    for (const auto& subject : subjects) {
        const std::uint8_t kind =
            static_cast<std::uint8_t>(subject.kind);
        if (kind <
                static_cast<std::uint8_t>(
                    RawEmergencyReserveQuotaKindV1::kUser) ||
            kind >
                static_cast<std::uint8_t>(
                    RawEmergencyReserveQuotaKindV1::kProject) ||
            kind <= previous) {
            return false;
        }
        previous = kind;
    }
    return true;
}

[[nodiscard]] std::uint64_t ExpectedFilesystemType(
    RawEmergencyReserveFilesystemModelV1 model) noexcept {
    switch (model) {
        case RawEmergencyReserveFilesystemModelV1::kExt4:
            return static_cast<std::uint64_t>(
                EXT4_SUPER_MAGIC);
        case RawEmergencyReserveFilesystemModelV1::kXfs:
            return static_cast<std::uint64_t>(
                XFS_SUPER_MAGIC);
    }
    return 0U;
}

[[nodiscard]] int GenericQuotaType(
    RawEmergencyReserveQuotaKindV1 kind) noexcept {
    switch (kind) {
        case RawEmergencyReserveQuotaKindV1::kUser:
            return USRQUOTA;
        case RawEmergencyReserveQuotaKindV1::kGroup:
            return GRPQUOTA;
        case RawEmergencyReserveQuotaKindV1::kProject:
            return PRJQUOTA;
    }
    return -1;
}

[[nodiscard]] std::uint16_t XfsQuotaStatusBits(
    RawEmergencyReserveQuotaKindV1 kind) noexcept {
    switch (kind) {
        case RawEmergencyReserveQuotaKindV1::kUser:
            return static_cast<std::uint16_t>(
                FS_QUOTA_UDQ_ACCT |
                FS_QUOTA_UDQ_ENFD);
        case RawEmergencyReserveQuotaKindV1::kGroup:
            return static_cast<std::uint16_t>(
                FS_QUOTA_GDQ_ACCT |
                FS_QUOTA_GDQ_ENFD);
        case RawEmergencyReserveQuotaKindV1::kProject:
            return static_cast<std::uint16_t>(
                FS_QUOTA_PDQ_ACCT |
                FS_QUOTA_PDQ_ENFD);
    }
    return 0U;
}

[[nodiscard]] std::int8_t XfsQuotaSubjectFlag(
    RawEmergencyReserveQuotaKindV1 kind) noexcept {
    switch (kind) {
        case RawEmergencyReserveQuotaKindV1::kUser:
            return static_cast<std::int8_t>(
                FS_USER_QUOTA);
        case RawEmergencyReserveQuotaKindV1::kGroup:
            return static_cast<std::int8_t>(
                FS_GROUP_QUOTA);
        case RawEmergencyReserveQuotaKindV1::kProject:
            return static_cast<std::int8_t>(
                FS_PROJ_QUOTA);
    }
    return 0;
}

class SystemPosixOps final
    : public RawEmergencyReserveCapacityProbePosixOpsV1 {
public:
    [[nodiscard]] bool ObserveFilesystem(
        int fd,
        RawEmergencyReserveFilesystemObservationV1*
            observation,
        int* system_error) noexcept override {
        if (observation == nullptr || system_error == nullptr) {
            return false;
        }
        *system_error = 0;
        struct statvfs vfs {};
        struct statfs fs {};
        if (::fstatvfs(fd, &vfs) != 0 ||
            ::fstatfs(fd, &fs) != 0) {
            *system_error = errno;
            return false;
        }
        if (fs.f_type < 0) {
            *system_error = EINVAL;
            return false;
        }
        RawEmergencyReserveFilesystemObservationV1 result{};
        if (!UnsignedToU64(vfs.f_fsid, &result.filesystem_id) ||
            !UnsignedToU64(vfs.f_frsize, &result.fragment_size) ||
            !UnsignedToU64(
                vfs.f_bavail, &result.blocks_available) ||
            !UnsignedToU64(vfs.f_bfree, &result.blocks_free) ||
            !UnsignedToU64(
                vfs.f_favail, &result.inodes_available) ||
            !UnsignedToU64(vfs.f_ffree, &result.inodes_free)) {
            *system_error = EOVERFLOW;
            return false;
        }
        result.filesystem_type =
            static_cast<std::uint64_t>(fs.f_type);
        result.read_only =
            (vfs.f_flag &
             static_cast<unsigned long>(ST_RDONLY)) != 0U;
        *observation = result;
        return true;
    }

    [[nodiscard]] bool ObserveQuota(
        int fd,
        RawEmergencyReserveFilesystemModelV1 filesystem_model,
        const RawEmergencyReserveQuotaSubjectV1& subject,
        RawEmergencyReserveQuotaObservationV1* observation,
        int* system_error) noexcept override {
        if (observation == nullptr || system_error == nullptr) {
            return false;
        }
        *system_error = 0;
        const int type = GenericQuotaType(subject.kind);
        if (type < 0) {
            *system_error = EINVAL;
            return false;
        }

        RawEmergencyReserveQuotaObservationV1 result{};
        if (filesystem_model ==
            RawEmergencyReserveFilesystemModelV1::kExt4) {
            struct dqblk quota {};
            if (::syscall(
                    SYS_quotactl_fd,
                    fd,
                    QCMD(Q_GETQUOTA, type),
                    subject.id,
                    &quota) != 0) {
                *system_error = errno;
                return false;
            }
            if (!MultiplyChecked(
                    quota.dqb_bhardlimit,
                    kGenericQuotaBlockBytes,
                    &result.byte_hard_limit) ||
                !MultiplyChecked(
                    quota.dqb_bsoftlimit,
                    kGenericQuotaBlockBytes,
                    &result.byte_soft_limit)) {
                *system_error = EOVERFLOW;
                return false;
            }
            result.bytes_used = quota.dqb_curspace;
            result.inode_hard_limit =
                quota.dqb_ihardlimit;
            result.inode_soft_limit =
                quota.dqb_isoftlimit;
            result.inodes_used =
                quota.dqb_curinodes;
            result.byte_limits_valid =
                (quota.dqb_valid & QIF_BLIMITS) != 0U;
            result.byte_usage_valid =
                (quota.dqb_valid & QIF_SPACE) != 0U;
            result.inode_limits_valid =
                (quota.dqb_valid & QIF_ILIMITS) != 0U;
            result.inode_usage_valid =
                (quota.dqb_valid & QIF_INODES) != 0U;
            // Q_GETQUOTA proves an active accounting domain but the
            // generic VFS API exposes no enforcement-on status bit.
            // Claiming EDQUOT protection from this query alone would be
            // unsound, so default ext4 operation remains fail closed.
            result.accounting_enabled = true;
            result.enforcement_enabled = false;
        } else if (
            filesystem_model ==
            RawEmergencyReserveFilesystemModelV1::kXfs) {
            struct fs_quota_statv status {};
            status.qs_version = FS_QSTATV_VERSION1;
            if (::syscall(
                    SYS_quotactl_fd,
                    fd,
                    QCMD(Q_XGETQSTATV, type),
                    0U,
                    &status) != 0) {
                *system_error = errno;
                return false;
            }
            const std::uint16_t required_status =
                XfsQuotaStatusBits(subject.kind);
            result.accounting_enabled =
                (status.qs_flags &
                 static_cast<std::uint16_t>(
                     required_status &
                     static_cast<std::uint16_t>(
                         FS_QUOTA_UDQ_ACCT |
                         FS_QUOTA_GDQ_ACCT |
                         FS_QUOTA_PDQ_ACCT))) != 0U;
            result.enforcement_enabled =
                (status.qs_flags & required_status) ==
                required_status;

            struct fs_disk_quota quota {};
            if (::syscall(
                    SYS_quotactl_fd,
                    fd,
                    QCMD(Q_XGETQUOTA, type),
                    subject.id,
                    &quota) != 0) {
                *system_error = errno;
                return false;
            }
            if (quota.d_version != FS_DQUOT_VERSION ||
                quota.d_id != subject.id ||
                quota.d_flags !=
                    XfsQuotaSubjectFlag(subject.kind) ||
                !MultiplyChecked(
                    quota.d_blk_hardlimit,
                    kStatBlockBytes,
                    &result.byte_hard_limit) ||
                !MultiplyChecked(
                    quota.d_blk_softlimit,
                    kStatBlockBytes,
                    &result.byte_soft_limit) ||
                !MultiplyChecked(
                    quota.d_bcount,
                    kStatBlockBytes,
                    &result.bytes_used)) {
                *system_error = EPROTO;
                return false;
            }
            result.inode_hard_limit =
                quota.d_ino_hardlimit;
            result.inode_soft_limit =
                quota.d_ino_softlimit;
            result.inodes_used = quota.d_icount;
            result.byte_limits_valid = true;
            result.byte_usage_valid = true;
            result.inode_limits_valid = true;
            result.inode_usage_valid = true;
        } else {
            *system_error = EINVAL;
            return false;
        }
        *observation = result;
        return true;
    }

    [[nodiscard]] bool ObserveProjectId(
        int fd,
        std::uint32_t* project_id,
        bool* project_inherit,
        int* system_error) noexcept override {
        if (project_id == nullptr ||
            project_inherit == nullptr ||
            system_error == nullptr) {
            return false;
        }
        *system_error = 0;
        struct fsxattr attributes {};
        if (::ioctl(
                fd,
                FS_IOC_FSGETXATTR,
                &attributes) != 0) {
            *system_error = errno;
            return false;
        }
        *project_id = attributes.fsx_projid;
        *project_inherit =
            (attributes.fsx_xflags &
             FS_XFLAG_PROJINHERIT) != 0U;
        return true;
    }
};

[[nodiscard]] SystemPosixOps& SystemOperations() noexcept {
    static SystemPosixOps operations;
    return operations;
}

[[nodiscard]] bool SameRootStatus(
    const struct stat& left,
    const struct stat& right) noexcept {
    return left.st_dev == right.st_dev &&
           left.st_ino == right.st_ino &&
           left.st_uid == right.st_uid &&
           left.st_gid == right.st_gid &&
           left.st_mode == right.st_mode &&
           left.st_nlink == right.st_nlink;
}

[[nodiscard]] bool ValidRootStatus(
    const struct stat& status) noexcept {
    return S_ISDIR(status.st_mode) &&
           (status.st_mode & 07777U) ==
               kPrivateDirectoryMode &&
           status.st_uid == ::geteuid() &&
           status.st_nlink >= 2;
}

[[nodiscard]] bool ValidFileStatus(
    const struct stat& status,
    std::uint64_t device,
    std::uint32_t uid,
    std::uint32_t gid) noexcept {
    return S_ISREG(status.st_mode) &&
           status.st_nlink == 1 &&
           (status.st_mode & 07777U) == kPrivateFileMode &&
           static_cast<std::uint64_t>(status.st_dev) ==
               device &&
           static_cast<std::uint32_t>(status.st_uid) == uid &&
           static_cast<std::uint32_t>(status.st_gid) == gid &&
           status.st_size >= 0 &&
           status.st_blocks >= 0;
}

[[nodiscard]] bool SameNamedInode(
    int parent_fd,
    const char* name,
    int fd) noexcept {
    struct stat named {};
    struct stat opened {};
    return name != nullptr &&
           ::fstat(fd, &opened) == 0 &&
           ::fstatat(
               parent_fd,
               name,
               &named,
               AT_SYMLINK_NOFOLLOW) == 0 &&
           named.st_dev == opened.st_dev &&
           named.st_ino == opened.st_ino &&
           named.st_mode == opened.st_mode &&
           named.st_nlink == opened.st_nlink;
}

[[nodiscard]] bool SameFileSnapshot(
    int fd,
    const struct stat& before) noexcept {
    struct stat after {};
    return ::fstat(fd, &after) == 0 &&
           after.st_dev == before.st_dev &&
           after.st_ino == before.st_ino &&
           after.st_uid == before.st_uid &&
           after.st_gid == before.st_gid &&
           after.st_mode == before.st_mode &&
           after.st_nlink == before.st_nlink &&
           after.st_size == before.st_size &&
           after.st_blocks == before.st_blocks &&
           after.st_mtim.tv_sec == before.st_mtim.tv_sec &&
           after.st_mtim.tv_nsec == before.st_mtim.tv_nsec &&
           after.st_ctim.tv_sec == before.st_ctim.tv_sec &&
           after.st_ctim.tv_nsec == before.st_ctim.tv_nsec;
}

[[nodiscard]] bool PreadAllHeader(
    int fd,
    ReserveHeaderWireV1* wire) noexcept {
    if (wire == nullptr) {
        return false;
    }
    std::size_t offset = 0U;
    while (offset < wire->size()) {
        const ssize_t read_bytes = ::pread(
            fd,
            wire->data() + offset,
            wire->size() - offset,
            static_cast<off_t>(offset));
        if (read_bytes > 0) {
            offset += static_cast<std::size_t>(read_bytes);
            continue;
        }
        if (read_bytes < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

[[nodiscard]] bool ListDirectoryNames(
    int directory_fd,
    std::vector<std::string>* names,
    std::string* error) {
    if (names == nullptr) {
        SetError(error, "directory scan output is null");
        return false;
    }
    names->clear();
    struct stat retained_status {};
    struct stat scan_status {};
    const int duplicate = ::openat(
        directory_fd,
        ".",
        O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (duplicate < 0 ||
        ::fstat(directory_fd, &retained_status) != 0 ||
        ::fstat(duplicate, &scan_status) != 0 ||
        retained_status.st_dev != scan_status.st_dev ||
        retained_status.st_ino != scan_status.st_ino) {
        const int saved = errno == 0 ? EIO : errno;
        if (duplicate >= 0) {
            static_cast<void>(::close(duplicate));
        }
        errno = saved;
        SetError(
            error,
            std::string(
                "cannot open an identity-bound directory scan: ") +
                std::strerror(errno));
        return false;
    }
    DIR* directory = ::fdopendir(duplicate);
    if (directory == nullptr) {
        const int saved = errno;
        static_cast<void>(::close(duplicate));
        errno = saved;
        SetError(
            error,
            std::string("cannot scan retained directory: ") +
                std::strerror(errno));
        return false;
    }
    errno = 0;
    while (true) {
        struct dirent* entry = ::readdir(directory);
        if (entry == nullptr) {
            if (errno != 0) {
                const int saved = errno;
                static_cast<void>(::closedir(directory));
                errno = saved;
                SetError(
                    error,
                    std::string("retained directory scan failed: ") +
                        std::strerror(errno));
                return false;
            }
            break;
        }
        const std::string_view name(entry->d_name);
        if (name != "." && name != "..") {
            names->emplace_back(name);
        }
        errno = 0;
    }
    if (::closedir(directory) != 0) {
        SetError(
            error,
            std::string("cannot close retained directory scan: ") +
                std::strerror(errno));
        return false;
    }
    std::sort(names->begin(), names->end());
    return true;
}

[[nodiscard]] bool StartsWith(
    std::string_view value,
    std::string_view prefix) noexcept {
    return value.size() >= prefix.size() &&
           value.substr(0U, prefix.size()) == prefix;
}

[[nodiscard]] std::string DataCandidateName(
    const l2flow::common::Identity128& uuid) {
    return std::string(kDataCandidatePrefix) +
           l2flow::common::Identity128Hex(uuid) +
           std::string(kDataCandidateSuffix);
}

[[nodiscard]] bool SubjectMatchesStatus(
    const struct stat& status,
    std::span<const RawEmergencyReserveQuotaSubjectV1>
        subjects) noexcept {
    for (const auto& subject : subjects) {
        if (subject.kind ==
                RawEmergencyReserveQuotaKindV1::kUser &&
            static_cast<std::uint32_t>(status.st_uid) !=
                subject.id) {
            return false;
        }
        if (subject.kind ==
                RawEmergencyReserveQuotaKindV1::kGroup &&
            static_cast<std::uint32_t>(status.st_gid) !=
                subject.id) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool ProjectSubject(
    std::span<const RawEmergencyReserveQuotaSubjectV1>
        subjects,
    std::uint32_t* project_id) noexcept {
    if (project_id == nullptr) {
        return false;
    }
    for (const auto& subject : subjects) {
        if (subject.kind ==
            RawEmergencyReserveQuotaKindV1::kProject) {
            *project_id = subject.id;
            return true;
        }
    }
    return false;
}

[[nodiscard]] bool ValidateProjectBinding(
    RawEmergencyReserveCapacityProbePosixOpsV1* operations,
    int fd,
    std::span<const RawEmergencyReserveQuotaSubjectV1>
        subjects,
    bool require_inherit,
    std::string* error) noexcept {
    std::uint32_t expected = 0U;
    if (!ProjectSubject(subjects, &expected)) {
        return true;
    }
    std::uint32_t observed = 0U;
    bool inherit = false;
    int system_error = 0;
    if (operations == nullptr ||
        !operations->ObserveProjectId(
            fd, &observed, &inherit, &system_error)) {
        SetError(
            error,
            std::string("cannot prove project quota binding: ") +
                std::strerror(
                    system_error == 0
                        ? EIO
                        : system_error));
        return false;
    }
    if (observed != expected ||
        (require_inherit && !inherit)) {
        SetError(
            error,
            "project quota id/inheritance does not match the configured domain");
        return false;
    }
    return true;
}

[[nodiscard]] bool EffectiveQuotaFree(
    const RawEmergencyReserveQuotaObservationV1& quota,
    std::uint64_t* free_bytes,
    std::uint64_t* free_inodes) noexcept {
    if (free_bytes == nullptr || free_inodes == nullptr ||
        !quota.byte_limits_valid ||
        !quota.byte_usage_valid ||
        !quota.inode_limits_valid ||
        !quota.inode_usage_valid ||
        !quota.accounting_enabled ||
        !quota.enforcement_enabled) {
        return false;
    }

    const auto limiting_value = [](
                                    std::uint64_t hard,
                                    std::uint64_t soft) noexcept {
        if (hard == 0U) {
            return soft;
        }
        if (soft == 0U) {
            return hard;
        }
        return std::min(hard, soft);
    };
    const std::uint64_t byte_limit = limiting_value(
        quota.byte_hard_limit,
        quota.byte_soft_limit);
    const std::uint64_t inode_limit = limiting_value(
        quota.inode_hard_limit,
        quota.inode_soft_limit);
    *free_bytes =
        byte_limit == 0U
            ? std::numeric_limits<std::uint64_t>::max()
            : (quota.bytes_used >= byte_limit
                   ? 0U
                   : byte_limit - quota.bytes_used);
    *free_inodes =
        inode_limit == 0U
            ? std::numeric_limits<std::uint64_t>::max()
            : (quota.inodes_used >= inode_limit
                   ? 0U
                   : inode_limit - quota.inodes_used);
    return true;
}

[[nodiscard]] bool ObserveCapacityDimensions(
    RawEmergencyReserveCapacityProbePosixOpsV1* operations,
    int root_fd,
    const RawEmergencyReserveCapacityProbePosixConfigV1&
        config,
    std::uint64_t expected_filesystem_id,
    std::uint64_t expected_filesystem_type,
    std::uint64_t* filesystem_free_bytes,
    std::uint64_t* filesystem_free_inodes,
    std::uint64_t* quota_free_bytes,
    std::uint64_t* quota_free_inodes,
    std::string* error) noexcept {
    if (operations == nullptr ||
        filesystem_free_bytes == nullptr ||
        filesystem_free_inodes == nullptr ||
        quota_free_bytes == nullptr ||
        quota_free_inodes == nullptr) {
        SetError(error, "capacity observation input is invalid");
        return false;
    }
    RawEmergencyReserveFilesystemObservationV1 filesystem{};
    int system_error = 0;
    if (!operations->ObserveFilesystem(
            root_fd, &filesystem, &system_error)) {
        SetError(
            error,
            std::string("filesystem capacity query failed: ") +
                std::strerror(
                    system_error == 0
                        ? EIO
                        : system_error));
        return false;
    }
    if (filesystem.filesystem_id !=
            expected_filesystem_id ||
        filesystem.filesystem_type !=
            expected_filesystem_type ||
        filesystem.fragment_size == 0U ||
        filesystem.blocks_available ==
            std::numeric_limits<std::uint64_t>::max() ||
        filesystem.blocks_free ==
            std::numeric_limits<std::uint64_t>::max() ||
        filesystem.inodes_available ==
            std::numeric_limits<std::uint64_t>::max() ||
        filesystem.inodes_free ==
            std::numeric_limits<std::uint64_t>::max() ||
        filesystem.read_only) {
        SetError(
            error,
            "filesystem identity, allocation unit, or writable state changed");
        return false;
    }
    const std::uint64_t available_blocks =
        std::min(
            filesystem.blocks_available,
            filesystem.blocks_free);
    if (!MultiplyChecked(
            available_blocks,
            filesystem.fragment_size,
            filesystem_free_bytes)) {
        SetError(
            error,
            "filesystem free-byte observation overflows");
        return false;
    }
    *filesystem_free_inodes = std::min(
        filesystem.inodes_available,
        filesystem.inodes_free);

    std::uint64_t effective_quota_bytes =
        std::numeric_limits<std::uint64_t>::max();
    std::uint64_t effective_quota_inodes =
        std::numeric_limits<std::uint64_t>::max();
    for (const auto& subject : config.quota_subjects) {
        RawEmergencyReserveQuotaObservationV1 quota{};
        system_error = 0;
        if (!operations->ObserveQuota(
                root_fd,
                config.filesystem_model,
                subject,
                &quota,
                &system_error)) {
            SetError(
                error,
                std::string("quota capacity query failed: ") +
                    std::strerror(
                        system_error == 0
                            ? EIO
                            : system_error));
            return false;
        }
        std::uint64_t subject_free_bytes = 0U;
        std::uint64_t subject_free_inodes = 0U;
        if (!EffectiveQuotaFree(
                quota,
                &subject_free_bytes,
                &subject_free_inodes)) {
            SetError(
                error,
                "quota query did not prove enabled byte and inode accounting/enforcement");
            return false;
        }
        effective_quota_bytes = std::min(
            effective_quota_bytes,
            subject_free_bytes);
        effective_quota_inodes = std::min(
            effective_quota_inodes,
            subject_free_inodes);
    }
    *quota_free_bytes = effective_quota_bytes;
    *quota_free_inodes = effective_quota_inodes;
    return true;
}

[[nodiscard]] bool AddAllocatedFileCharge(
    const struct stat& status,
    std::uint64_t* bytes,
    std::uint64_t* inodes) noexcept {
    if (status.st_blocks < 0 || status.st_size < 0 ||
        bytes == nullptr || inodes == nullptr) {
        return false;
    }
    std::uint64_t allocated = 0U;
    if (!MultiplyChecked(
            static_cast<std::uint64_t>(
                status.st_blocks),
            kStatBlockBytes,
            &allocated) ||
        allocated <
            static_cast<std::uint64_t>(
                status.st_size) ||
        !AddChecked(*bytes, allocated, bytes) ||
        !AddChecked(*inodes, 1U, inodes)) {
        return false;
    }
    return true;
}

[[nodiscard]] int OpenOptionalRegularAt(
    int parent_fd,
    const char* name,
    bool* exists,
    std::string* error) noexcept {
    if (exists == nullptr || name == nullptr) {
        SetError(error, "optional reserve open input is invalid");
        return -1;
    }
    *exists = false;
    const int fd = ::openat(
        parent_fd,
        name,
        O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (fd >= 0) {
        *exists = true;
        return fd;
    }
    if (errno == ENOENT) {
        return -1;
    }
    // Treat an occupied but unopenable name as present so the caller cannot
    // mistake a symlink, directory, permission failure, or I/O error for
    // durable absence.
    *exists = true;
    SetError(
        error,
        std::string("cannot securely open reserve artifact: ") +
            std::strerror(errno));
    return -1;
}

struct ReserveCharge final {
    std::uint64_t bytes = 0U;
    std::uint64_t inodes = 0U;
};

[[nodiscard]] bool ValidateChargeFileCommon(
    RawEmergencyReserveCapacityProbePosixOpsV1* operations,
    int parent_fd,
    const char* name,
    int fd,
    std::uint64_t device,
    std::uint32_t uid,
    std::uint32_t gid,
    std::span<const RawEmergencyReserveQuotaSubjectV1>
        subjects,
    struct stat* status,
    std::string* error) noexcept {
    if (status == nullptr ||
        ::fstat(fd, status) != 0 ||
        !ValidFileStatus(*status, device, uid, gid) ||
        !SubjectMatchesStatus(*status, subjects) ||
        !ValidateProjectBinding(
            operations,
            fd,
            subjects,
            false,
            error) ||
        !SameNamedInode(parent_fd, name, fd)) {
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "reserve artifact type, owner, mode, device, quota binding, or name-to-inode identity is unsafe");
        }
        return false;
    }
    return true;
}

[[nodiscard]] bool ObserveDataCharge(
    RawEmergencyReserveCapacityProbePosixOpsV1* operations,
    int root_fd,
    const ReserveAllocationPoolIdentityV1& pool,
    std::uint32_t root_uid,
    std::uint32_t root_gid,
    std::span<const RawEmergencyReserveQuotaSubjectV1>
        subjects,
    bool* data_exists,
    ReserveCharge* charge,
    std::string* error) {
    if (data_exists == nullptr || charge == nullptr) {
        SetError(error, "reserve data charge output is invalid");
        return false;
    }
    *data_exists = false;
    const std::string candidate =
        DataCandidateName(pool.reserve_state_uuid);
    bool fixed_exists = false;
    bool candidate_exists = false;
    ScopedFd fixed(OpenOptionalRegularAt(
        root_fd,
        kRawEmergencyReserveDataFilename,
        &fixed_exists,
        error));
    if (fixed.get() < 0 && fixed_exists) {
        return false;
    }
    ScopedFd pending(OpenOptionalRegularAt(
        root_fd,
        candidate.c_str(),
        &candidate_exists,
        error));
    if (pending.get() < 0 && candidate_exists) {
        return false;
    }
    if (fixed_exists && candidate_exists) {
        SetError(
            error,
            "fixed and candidate reserve data coexist");
        return false;
    }

    std::vector<std::string> root_names;
    if (!ListDirectoryNames(
            root_fd, &root_names, error)) {
        return false;
    }
    for (const std::string& name : root_names) {
        if (StartsWith(name, kDataCandidatePrefix) &&
            name != candidate) {
            SetError(
                error,
                "another or malformed reserve data candidate exists");
            return false;
        }
    }
    if (!fixed_exists && !candidate_exists) {
        std::vector<std::string> names_after;
        if (!ListDirectoryNames(
                root_fd, &names_after, error) ||
            names_after != root_names) {
            SetError(
                error,
                "Raw-root namespace changed during reserve absence observation");
            return false;
        }
        return true;
    }

    const int fd =
        fixed_exists ? fixed.get() : pending.get();
    const char* name =
        fixed_exists
            ? kRawEmergencyReserveDataFilename
            : candidate.c_str();
    struct stat status {};
    if (!ValidateChargeFileCommon(
            operations,
            root_fd,
            name,
            fd,
            pool.device_id,
            root_uid,
            root_gid,
            subjects,
            &status,
            error)) {
        return false;
    }
    ReserveHeaderWireV1 wire{};
    ReserveHeaderWireV1 confirmed_wire{};
    ReserveFileHeaderV1 header{};
    if (!PreadAllHeader(fd, &wire) ||
        DecodeReserveFileHeaderV1(
            wire, &header) !=
            ReserveHeaderV1Error::kNone ||
        ValidateReserveFileHeaderBindingV1(
            header,
            pool,
            static_cast<std::uint64_t>(
                status.st_size)) !=
            ReserveHeaderV1Error::kNone ||
        !AddAllocatedFileCharge(
            status, &charge->bytes, &charge->inodes) ||
        !PreadAllHeader(fd, &confirmed_wire) ||
        confirmed_wire != wire ||
        !SameFileSnapshot(fd, status) ||
        !SameNamedInode(root_fd, name, fd)) {
        SetError(
            error,
            "reserve data header, allocation, or retained name binding is invalid");
        return false;
    }
    *data_exists = true;
    std::vector<std::string> names_after;
    if (!ListDirectoryNames(
            root_fd, &names_after, error) ||
        names_after != root_names) {
        SetError(
            error,
            "Raw-root namespace changed during reserve data observation");
        return false;
    }
    return true;
}

struct ParsedInventoryName final {
    std::string name{};
    std::uint32_t index = 0U;
};

[[nodiscard]] bool ObserveInodeCharge(
    RawEmergencyReserveCapacityProbePosixOpsV1* operations,
    int root_fd,
    const ReserveAllocationPoolIdentityV1& pool,
    std::uint32_t root_uid,
    std::uint32_t root_gid,
    std::span<const RawEmergencyReserveQuotaSubjectV1>
        subjects,
    RawEmergencyReserveProbeStageV1 stage,
    std::uint64_t* observed_file_count,
    std::uint32_t* declared_count,
    ReserveCharge* charge,
    std::string* error) {
    if (observed_file_count == nullptr ||
        declared_count == nullptr || charge == nullptr) {
        SetError(error, "reserve inode charge output is invalid");
        return false;
    }
    *observed_file_count = 0U;
    *declared_count = 0U;

    ScopedFd directory(::openat(
        root_fd,
        kRawEmergencyReserveInodesDirectory,
        O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    if (directory.get() < 0) {
        SetError(
            error,
            std::string(
                "cannot securely open reserve-inodes directory: ") +
                std::strerror(errno));
        return false;
    }
    struct stat directory_status {};
    if (::fstat(directory.get(), &directory_status) != 0 ||
        !S_ISDIR(directory_status.st_mode) ||
        (directory_status.st_mode & 07777U) !=
            kPrivateDirectoryMode ||
        directory_status.st_uid != root_uid ||
        directory_status.st_gid != root_gid ||
        static_cast<std::uint64_t>(
            directory_status.st_dev) != pool.device_id ||
        !SubjectMatchesStatus(directory_status, subjects) ||
        !ValidateProjectBinding(
            operations,
            directory.get(),
            subjects,
            true,
            error) ||
        !SameNamedInode(
            root_fd,
            kRawEmergencyReserveInodesDirectory,
            directory.get())) {
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "reserve-inodes directory identity or quota inheritance is unsafe");
        }
        return false;
    }

    std::vector<std::string> names;
    if (!ListDirectoryNames(
            directory.get(), &names, error)) {
        return false;
    }
    std::vector<ParsedInventoryName> inventory;
    inventory.reserve(names.size());
    for (const std::string& name : names) {
        l2flow::common::Identity128 uuid{};
        std::uint32_t index = 0U;
        if (ParseReserveInodeFilenameV1(
                name, &uuid, &index) !=
                ReserveHeaderV1Error::kNone ||
            uuid != pool.reserve_state_uuid) {
            SetError(
                error,
                "reserve-inodes contains an unknown, malformed, or foreign artifact");
            return false;
        }
        inventory.push_back(
            ParsedInventoryName{name, index});
    }
    std::sort(
        inventory.begin(),
        inventory.end(),
        [](const ParsedInventoryName& left,
           const ParsedInventoryName& right) noexcept {
            return left.index < right.index;
        });
    for (std::size_t offset = 0U;
         offset < inventory.size();
         ++offset) {
        if (offset >
                std::numeric_limits<std::uint32_t>::max() ||
            inventory[offset].index !=
                static_cast<std::uint32_t>(offset)) {
            SetError(
                error,
                "reserve inode inventory is not a contiguous prefix");
            return false;
        }
    }

    bool have_declared_count = false;
    for (const ParsedInventoryName& item : inventory) {
        ScopedFd fd(::openat(
            directory.get(),
            item.name.c_str(),
            O_RDONLY | O_CLOEXEC | O_NOFOLLOW |
                O_NONBLOCK));
        struct stat status {};
        if (fd.get() < 0 ||
            !ValidateChargeFileCommon(
                operations,
                directory.get(),
                item.name.c_str(),
                fd.get(),
                pool.device_id,
                root_uid,
                root_gid,
                subjects,
                &status,
                error)) {
            if (error != nullptr && error->empty()) {
                SetError(
                    error,
                    "cannot securely validate reserve inode artifact");
            }
            return false;
        }
        ReserveHeaderWireV1 wire{};
        ReserveHeaderWireV1 confirmed_wire{};
        ReserveInodeHeaderV1 header{};
        if (!PreadAllHeader(fd.get(), &wire) ||
            DecodeReserveInodeHeaderV1(
                wire, &header) !=
                ReserveHeaderV1Error::kNone) {
            SetError(
                error,
                "reserve inode header is invalid");
            return false;
        }
        if (!have_declared_count) {
            *declared_count =
                header.declared_inode_reserve_count;
            have_declared_count = true;
        }
        if (ValidateReserveInodeHeaderBindingV1(
                header,
                pool,
                *declared_count,
                item.name) !=
                ReserveHeaderV1Error::kNone ||
            !AddAllocatedFileCharge(
                status,
                &charge->bytes,
                &charge->inodes) ||
            !PreadAllHeader(
                fd.get(), &confirmed_wire) ||
            confirmed_wire != wire ||
            !SameFileSnapshot(fd.get(), status) ||
            !SameNamedInode(
                directory.get(),
                item.name.c_str(),
                fd.get())) {
            SetError(
                error,
                "reserve inode header, allocation, count, or retained name binding is invalid");
            return false;
        }
    }
    if (!UnsignedToU64(
            inventory.size(), observed_file_count)) {
        SetError(
            error,
            "reserve inode inventory count overflows");
        return false;
    }
    if (stage ==
            RawEmergencyReserveProbeStageV1::
                kProvisionedCandidate &&
        (!have_declared_count ||
         *observed_file_count !=
             static_cast<std::uint64_t>(
                 *declared_count))) {
        SetError(
            error,
            "provisioned reserve inode inventory is incomplete");
        return false;
    }
    if (!SameNamedInode(
            root_fd,
            kRawEmergencyReserveInodesDirectory,
            directory.get())) {
        SetError(
            error,
            "reserve-inodes directory was replaced during observation");
        return false;
    }
    std::vector<std::string> names_after;
    if (!ListDirectoryNames(
            directory.get(), &names_after, error) ||
        names_after != names) {
        SetError(
            error,
            "reserve inode inventory changed during observation");
        return false;
    }
    return true;
}

[[nodiscard]] bool ObserveReserveCharge(
    RawEmergencyReserveCapacityProbePosixOpsV1* operations,
    int root_fd,
    const RawEmergencyReserveCapacityProbePosixConfigV1&
        config,
    const ReserveAllocationPoolIdentityV1& pool,
    std::uint32_t root_uid,
    std::uint32_t root_gid,
    RawEmergencyReserveProbeStageV1 stage,
    std::uint64_t required_bytes,
    std::uint64_t required_inodes,
    ReserveCharge* charge,
    std::string* error) {
    if (charge == nullptr) {
        SetError(error, "reserve charge output is null");
        return false;
    }
    *charge = {};
    bool data_exists = false;
    if (!ObserveDataCharge(
            operations,
            root_fd,
            pool,
            root_uid,
            root_gid,
            config.quota_subjects,
            &data_exists,
            charge,
            error)) {
        return false;
    }
    std::uint64_t inventory_count = 0U;
    std::uint32_t declared_count = 0U;
    if (!ObserveInodeCharge(
            operations,
            root_fd,
            pool,
            root_uid,
            root_gid,
            config.quota_subjects,
            stage,
            &inventory_count,
            &declared_count,
            charge,
            error)) {
        return false;
    }
    static_cast<void>(inventory_count);
    static_cast<void>(declared_count);

    if (stage ==
            RawEmergencyReserveProbeStageV1::
                kProvisionedCandidate &&
        !data_exists) {
        SetError(
            error,
            "provisioned reserve data is absent");
        return false;
    }
    if (stage ==
            RawEmergencyReserveProbeStageV1::kReleased) {
        if (data_exists || charge->bytes != 0U ||
            charge->inodes != 0U ||
            required_bytes != 0U ||
            required_inodes != 0U) {
            SetError(
                error,
                "released reserve still has data or inode charge");
            return false;
        }
        return true;
    }
    if (charge->inodes == 0U ||
        charge->inodes != required_inodes ||
        charge->bytes != required_bytes) {
        SetError(
            error,
            "observed reserve byte/inode charge does not exactly match the request");
        return false;
    }
    return true;
}

[[nodiscard]] bool ValidStage(
    RawEmergencyReserveProbeStageV1 stage) noexcept {
    switch (stage) {
        case RawEmergencyReserveProbeStageV1::kBeforeProvision:
        case RawEmergencyReserveProbeStageV1::
            kProvisionedCandidate:
        case RawEmergencyReserveProbeStageV1::kAttached:
        case RawEmergencyReserveProbeStageV1::kReleased:
        case RawEmergencyReserveProbeStageV1::
            kFinalizationPostPublish:
            return true;
    }
    return false;
}

}  // namespace

bool ComputeRawEmergencyReserveQuotaIdentitySha256V1(
    std::span<const RawEmergencyReserveQuotaSubjectV1> subjects,
    ReserveHeaderDigestV1* digest,
    std::string* error) noexcept {
    SetError(error, {});
    if (digest == nullptr || !ValidSubjects(subjects)) {
        SetError(
            error,
            "quota identity requires one strictly kind-sorted user/group/project vector");
        return false;
    }
    std::array<std::byte, 64U> canonical{};
    const std::span<const std::byte> domain = std::as_bytes(
        std::span{
            kRawEmergencyReserveQuotaIdentityDomainV1.data(),
            kRawEmergencyReserveQuotaIdentityDomainV1.size()});
    std::copy(
        domain.begin(), domain.end(), canonical.begin());
    std::size_t offset = domain.size();
    canonical[offset++] = std::byte{0};
    canonical[offset++] =
        static_cast<std::byte>(subjects.size());
    for (const auto& subject : subjects) {
        const std::array<std::byte, 5U> encoded{
            static_cast<std::byte>(subject.kind),
            static_cast<std::byte>(
                subject.id & 0xffU),
            static_cast<std::byte>(
                (subject.id >> 8U) & 0xffU),
            static_cast<std::byte>(
                (subject.id >> 16U) & 0xffU),
            static_cast<std::byte>(
                (subject.id >> 24U) & 0xffU)};
        std::copy(
            encoded.begin(),
            encoded.end(),
            canonical.begin() +
                static_cast<std::ptrdiff_t>(offset));
        offset += encoded.size();
    }
    *digest = l2flow::common::ComputeSha256(
        std::span<const std::byte>(
            canonical.data(), offset));
    return true;
}

RawEmergencyReserveCapacityProbePosixV1::
    RawEmergencyReserveCapacityProbePosixV1(
        RawEmergencyReserveCapacityProbePosixConfigV1 config,
        RawEmergencyReserveCapacityProbePosixOpsV1* operations,
        std::uint64_t root_device,
        std::uint64_t root_inode,
        std::uint32_t root_uid,
        std::uint32_t root_gid,
        std::uint64_t filesystem_id,
        std::uint64_t filesystem_type,
        ReserveHeaderDigestV1 quota_identity_sha256) noexcept
    : config_(std::move(config)),
      operations_(operations),
      root_device_(root_device),
      root_inode_(root_inode),
      root_uid_(root_uid),
      root_gid_(root_gid),
      filesystem_id_(filesystem_id),
      filesystem_type_(filesystem_type),
      quota_identity_sha256_(
          std::move(quota_identity_sha256)) {}

RawEmergencyReserveCapacityProbePosixV1::
    ~RawEmergencyReserveCapacityProbePosixV1() = default;

std::unique_ptr<RawEmergencyReserveCapacityProbePosixV1>
BindRawEmergencyReserveCapacityProbePosixV1At(
    int retained_raw_root_fd,
    const RawEmergencyReserveCapacityProbePosixConfigV1& config,
    RawEmergencyReserveCapacityProbePosixOpsV1* operations,
    std::string* error) noexcept {
    SetError(error, {});
    try {
        if (retained_raw_root_fd < 0 ||
            config.byte_probe_version == 0U ||
            config.inode_probe_version == 0U ||
            IsZeroDigest(config.mount_identity_sha256) ||
            !ValidSubjects(config.quota_subjects)) {
            SetError(
                error,
                "capacity probe binding configuration is invalid");
            return nullptr;
        }
        const std::uint64_t expected_type =
            ExpectedFilesystemType(
                config.filesystem_model);
        if (expected_type == 0U) {
            SetError(
                error,
                "capacity probe filesystem model is unsupported");
            return nullptr;
        }
        struct stat root_status {};
        if (::fstat(
                retained_raw_root_fd,
                &root_status) != 0 ||
            !ValidRootStatus(root_status) ||
            !SubjectMatchesStatus(
                root_status,
                config.quota_subjects) ||
            root_status.st_ino == 0 ||
            root_status.st_dev == 0) {
            SetError(
                error,
                "capacity probe root descriptor has unsafe identity, ownership, or mode");
            return nullptr;
        }

        RawEmergencyReserveCapacityProbePosixOpsV1*
            selected_operations =
                operations == nullptr
                    ? &SystemOperations()
                    : operations;
        if (!ValidateProjectBinding(
                selected_operations,
                retained_raw_root_fd,
                config.quota_subjects,
                true,
                error)) {
            return nullptr;
        }
        RawEmergencyReserveFilesystemObservationV1 filesystem{};
        int system_error = 0;
        if (!selected_operations->ObserveFilesystem(
                retained_raw_root_fd,
                &filesystem,
                &system_error)) {
            SetError(
                error,
                std::string(
                    "cannot bind filesystem observation: ") +
                    std::strerror(
                        system_error == 0
                            ? EIO
                            : system_error));
            return nullptr;
        }
        if (filesystem.filesystem_id == 0U ||
            filesystem.filesystem_type != expected_type ||
            filesystem.fragment_size == 0U ||
            filesystem.read_only) {
            SetError(
                error,
                "capacity probe binding does not match the configured writable ext4/XFS allocation model");
            return nullptr;
        }
        for (const auto& subject : config.quota_subjects) {
            RawEmergencyReserveQuotaObservationV1 quota{};
            system_error = 0;
            if (!selected_operations->ObserveQuota(
                    retained_raw_root_fd,
                    config.filesystem_model,
                    subject,
                    &quota,
                    &system_error)) {
                SetError(
                    error,
                    std::string(
                        "cannot bind quota observation: ") +
                        std::strerror(
                            system_error == 0
                                ? EIO
                                : system_error));
                return nullptr;
            }
            std::uint64_t unused_bytes = 0U;
            std::uint64_t unused_inodes = 0U;
            if (!EffectiveQuotaFree(
                    quota,
                    &unused_bytes,
                    &unused_inodes)) {
                SetError(
                    error,
                    "configured quota domain does not prove byte/inode accounting and enforcement");
                return nullptr;
            }
        }
        ReserveHeaderDigestV1 quota_identity{};
        if (!ComputeRawEmergencyReserveQuotaIdentitySha256V1(
                config.quota_subjects,
                &quota_identity,
                error)) {
            return nullptr;
        }
        return std::unique_ptr<
            RawEmergencyReserveCapacityProbePosixV1>(
            new RawEmergencyReserveCapacityProbePosixV1(
                config,
                selected_operations,
                static_cast<std::uint64_t>(
                    root_status.st_dev),
                static_cast<std::uint64_t>(
                    root_status.st_ino),
                static_cast<std::uint32_t>(
                    root_status.st_uid),
                static_cast<std::uint32_t>(
                    root_status.st_gid),
                filesystem.filesystem_id,
                filesystem.filesystem_type,
                quota_identity));
    } catch (const std::bad_alloc&) {
        SetError(
            error,
            "cannot allocate capacity probe binding");
        return nullptr;
    } catch (...) {
        SetError(
            error,
            "unexpected capacity probe binding failure");
        return nullptr;
    }
}

bool RawEmergencyReserveCapacityProbePosixV1::Observe(
    int retained_raw_root_fd,
    const RawEmergencyReserveCapacityProbeRequestV1& request,
    RawEmergencyReserveCapacityObservationV1* observation,
    std::string* error) noexcept {
    SetError(error, {});
    if (observation != nullptr) {
        *observation = {};
    }
    try {
        if (retained_raw_root_fd < 0 ||
            observation == nullptr ||
            operations_ == nullptr ||
            !ValidStage(request.stage) ||
            l2flow::common::IsZeroIdentity(
                request.pool.reserve_state_uuid) ||
            request.pool.device_id != root_device_ ||
            request.pool.quota_identity_sha256 !=
                quota_identity_sha256_ ||
            request.pool.mount_identity_sha256 !=
                config_.mount_identity_sha256 ||
            request.byte_probe_version !=
                config_.byte_probe_version ||
            request.inode_probe_version !=
                config_.inode_probe_version) {
            SetError(
                error,
                "capacity request does not match the bound pool, quota vector, mount, or probe versions");
            return false;
        }
        struct stat before {};
        if (::fstat(retained_raw_root_fd, &before) != 0 ||
            !ValidRootStatus(before) ||
            static_cast<std::uint64_t>(before.st_dev) !=
                root_device_ ||
            static_cast<std::uint64_t>(before.st_ino) !=
                root_inode_ ||
            static_cast<std::uint32_t>(before.st_uid) !=
                root_uid_ ||
            static_cast<std::uint32_t>(before.st_gid) !=
                root_gid_ ||
            !SubjectMatchesStatus(
                before, config_.quota_subjects) ||
            !ValidateProjectBinding(
                operations_,
                retained_raw_root_fd,
                config_.quota_subjects,
                true,
                error)) {
            if (error != nullptr && error->empty()) {
                SetError(
                    error,
                    "retained Raw-root descriptor identity changed");
            }
            return false;
        }

        RawEmergencyReserveCapacityObservationV1 result{};
        result.pool = request.pool;
        result.byte_probe_version =
            request.byte_probe_version;
        result.inode_probe_version =
            request.inode_probe_version;
        if (!ObserveCapacityDimensions(
                operations_,
                retained_raw_root_fd,
                config_,
                filesystem_id_,
                filesystem_type_,
                &result.filesystem_free_bytes,
                &result.filesystem_free_inodes,
                &result.quota_free_bytes,
                &result.quota_free_inodes,
                error)) {
            return false;
        }
        result.filesystem_bytes_proven = true;
        result.filesystem_inodes_proven = true;
        result.quota_bytes_proven = true;
        result.quota_inodes_proven = true;

        if (request.stage ==
                RawEmergencyReserveProbeStageV1::
                    kBeforeProvision &&
            (result.filesystem_free_bytes <
                 request.required_bytes ||
             result.quota_free_bytes <
                 request.required_bytes ||
             result.filesystem_free_inodes <
                 request.required_inodes ||
             result.quota_free_inodes <
                 request.required_inodes)) {
            SetError(
                error,
                "filesystem or quota byte/inode capacity is below the provision requirement");
            return false;
        }

        if (request.stage ==
                RawEmergencyReserveProbeStageV1::
                    kProvisionedCandidate ||
            request.stage ==
                RawEmergencyReserveProbeStageV1::kAttached ||
            request.stage ==
                RawEmergencyReserveProbeStageV1::kReleased) {
            ReserveCharge charge{};
            if (!ObserveReserveCharge(
                    operations_,
                    retained_raw_root_fd,
                    config_,
                    request.pool,
                    root_uid_,
                    root_gid_,
                    request.stage,
                    request.required_bytes,
                    request.required_inodes,
                    &charge,
                    error)) {
                return false;
            }
            result.reserve_byte_charge_proven = true;
            result.reserve_inode_charge_proven = true;
            result.proven_reserve_byte_charge =
                charge.bytes;
            result.proven_reserve_inode_charge =
                charge.inodes;
        }

        struct stat after {};
        if (::fstat(retained_raw_root_fd, &after) != 0 ||
            !SameRootStatus(before, after) ||
            static_cast<std::uint64_t>(after.st_dev) !=
                root_device_ ||
            static_cast<std::uint64_t>(after.st_ino) !=
                root_inode_) {
            SetError(
                error,
                "retained Raw-root descriptor changed during capacity observation");
            return false;
        }
        *observation = result;
        return true;
    } catch (const std::bad_alloc&) {
        SetError(
            error,
            "cannot allocate capacity probe observation");
        return false;
    } catch (...) {
        SetError(
            error,
            "unexpected capacity probe failure");
        return false;
    }
}

}  // namespace l2flow::ingress
