#include "l2flow/canonical/source_frontier_posix_v1.h"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <utility>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace l2flow::canonical {
namespace {

struct ValidatedFile final {
    std::uint64_t device = 0U;
    std::uint64_t inode = 0U;
};

[[nodiscard]] SourceFrontierPosixResultV1 Failure(
    SourceFrontierPosixErrorV1 error,
    int system_errno = 0,
    SourceFrontierErrorV1 frontier_error =
        SourceFrontierErrorV1::kNone) noexcept {
    return {error, system_errno, frontier_error};
}

[[nodiscard]] bool ValidName(std::string_view name) noexcept {
    return !name.empty() && name != "." && name != ".." &&
           name.size() <= 255U &&
           name.find('/') == std::string_view::npos &&
           name.find('\0') == std::string_view::npos;
}

[[nodiscard]] SourceFrontierPosixResultV1 ValidateDirectory(
    const SourceFrontierPosixFileOptionsV1& options) noexcept {
    if (options.directory_fd < 0) {
        return Failure(SourceFrontierPosixErrorV1::kInvalidArgument);
    }
    if (!ValidName(options.file_name)) {
        return Failure(SourceFrontierPosixErrorV1::kInvalidName);
    }
    struct stat status {};
    if (::fstat(options.directory_fd, &status) != 0) {
        return Failure(
            SourceFrontierPosixErrorV1::kInvalidDirectory, errno);
    }
    if (!S_ISDIR(status.st_mode)) {
        return Failure(SourceFrontierPosixErrorV1::kInvalidDirectory);
    }
    if (static_cast<std::uint64_t>(status.st_uid) !=
        static_cast<std::uint64_t>(options.expected_owner_uid)) {
        return Failure(
            SourceFrontierPosixErrorV1::kDirectoryOwnerMismatch);
    }
    if ((status.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
        return Failure(SourceFrontierPosixErrorV1::kUnsafeDirectoryMode);
    }
    return {};
}

[[nodiscard]] SourceFrontierPosixResultV1 ValidateFile(
    int descriptor,
    std::uint32_t expected_owner_uid,
    ValidatedFile* output) noexcept {
    if (descriptor < 0 || output == nullptr) {
        return Failure(SourceFrontierPosixErrorV1::kInvalidArgument);
    }
    struct stat status {};
    if (::fstat(descriptor, &status) != 0) {
        return Failure(SourceFrontierPosixErrorV1::kOpenFailed, errno);
    }
    if (!S_ISREG(status.st_mode)) {
        return Failure(SourceFrontierPosixErrorV1::kWrongFileType);
    }
    if (static_cast<std::uint64_t>(status.st_uid) !=
        static_cast<std::uint64_t>(expected_owner_uid)) {
        return Failure(SourceFrontierPosixErrorV1::kFileOwnerMismatch);
    }
    if ((status.st_mode & 07777U) !=
        static_cast<mode_t>(kSourceFrontierPosixFileModeV1)) {
        return Failure(SourceFrontierPosixErrorV1::kWrongFileMode);
    }
    if (status.st_nlink != 1) {
        return Failure(SourceFrontierPosixErrorV1::kWrongLinkCount);
    }
    if (status.st_size !=
        static_cast<off_t>(kSourceFrontierPageBytesV1)) {
        return Failure(SourceFrontierPosixErrorV1::kWrongFileSize);
    }
    const int flags = ::fcntl(descriptor, F_GETFL);
    if (flags < 0) {
        return Failure(SourceFrontierPosixErrorV1::kOpenFailed, errno);
    }
    if ((flags & O_ACCMODE) != O_RDWR || (flags & O_APPEND) != 0) {
        return Failure(SourceFrontierPosixErrorV1::kOpenFailed, EBADF);
    }
    output->device = static_cast<std::uint64_t>(status.st_dev);
    output->inode = static_cast<std::uint64_t>(status.st_ino);
    return {};
}

[[nodiscard]] SourceFrontierPosixResultV1 OpenFailure(
    int error_number,
    bool creating) noexcept {
    if (error_number == ELOOP) {
        return Failure(
            SourceFrontierPosixErrorV1::kSymlinkRejected,
            error_number);
    }
    if (creating && error_number == EEXIST) {
        return Failure(
            SourceFrontierPosixErrorV1::kAlreadyExists,
            error_number);
    }
    return Failure(SourceFrontierPosixErrorV1::kOpenFailed, error_number);
}

[[nodiscard]] int OpenAtLoop(
    int directory_descriptor,
    const char* name,
    int flags,
    mode_t mode,
    bool creating) noexcept {
    for (;;) {
        const int descriptor = creating
            ? ::openat(directory_descriptor, name, flags, mode)
            : ::openat(directory_descriptor, name, flags);
        if (descriptor >= 0 || errno != EINTR) {
            return descriptor;
        }
    }
}

[[nodiscard]] int DuplicateCloexec(int descriptor) noexcept {
    for (;;) {
        const int duplicate = ::fcntl(descriptor, F_DUPFD_CLOEXEC, 0);
        if (duplicate >= 0 || errno != EINTR) {
            return duplicate;
        }
    }
}

void CloseDescriptor(int descriptor) noexcept {
    if (descriptor >= 0) {
        // Retrying close after EINTR can close an unrelated reused descriptor
        // on Linux.  One close is the correct ownership release operation.
        static_cast<void>(::close(descriptor));
    }
}

[[nodiscard]] bool SyncMemory(void* mapping) noexcept {
    for (;;) {
        if (::msync(mapping, kSourceFrontierPageBytesV1, MS_SYNC) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool SyncDescriptor(int descriptor) noexcept {
    for (;;) {
        if (::fsync(descriptor) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool NamedInodeMatches(
    int directory_descriptor,
    const char* name,
    const ValidatedFile& expected,
    int* error_number) noexcept {
    struct stat named {};
    if (::fstatat(
            directory_descriptor,
            name,
            &named,
            AT_SYMLINK_NOFOLLOW) != 0) {
        if (error_number != nullptr) {
            *error_number = errno;
        }
        return false;
    }
    if (error_number != nullptr) {
        *error_number = 0;
    }
    return S_ISREG(named.st_mode) &&
           static_cast<std::uint64_t>(named.st_dev) == expected.device &&
           static_cast<std::uint64_t>(named.st_ino) == expected.inode;
}

[[nodiscard]] SourceFrontierPosixResultV1 ValidateIdentityShape(
    const SourceFrontierIdentityV1& identity) noexcept {
    SourceFrontierConfigV1 config{};
    config.source_stream_id = identity.source_stream_id;
    config.capture_date = identity.capture_date;
    config.stream_day_id = identity.stream_day_id;
    config.clock_epoch = identity.clock_epoch;
    config.writer_instance = identity.writer_instance;
    config.generation = identity.generation;
    SourceFrontierPageV1 page{};
    const SourceFrontierErrorV1 error =
        InitializeSourceFrontierPageV1(config, &page);
    return error == SourceFrontierErrorV1::kNone
        ? SourceFrontierPosixResultV1{}
        : Failure(
              SourceFrontierPosixErrorV1::kSourceFrontierFailure,
              0,
              error);
}

[[nodiscard]] SourceFrontierIdentityV1 IdentityFromSnapshot(
    const SourceFrontierV1& value) noexcept {
    SourceFrontierIdentityV1 identity{};
    identity.source_stream_id = value.source_stream_id;
    identity.capture_date = value.capture_date;
    identity.stream_day_id = value.stream_day_id;
    identity.clock_epoch = value.clock_epoch;
    identity.writer_instance = value.writer_instance;
    identity.generation = value.generation;
    return identity;
}

[[nodiscard]] SourceFrontierErrorV1 ReadStableIdentity(
    const SourceFrontierPageV1& page,
    SourceFrontierIdentityV1* output) noexcept {
    SourceFrontierErrorV1 error = SourceFrontierErrorV1::kIdentityChanged;
    for (std::size_t attempt = 0U; attempt < 16U; ++attempt) {
        SourceFrontierV1 snapshot{};
        error = ReadSourceFrontierV1(page, &snapshot);
        if (error == SourceFrontierErrorV1::kNone) {
            *output = IdentityFromSnapshot(snapshot);
            return error;
        }
        if (error != SourceFrontierErrorV1::kIdentityChanged &&
            error != SourceFrontierErrorV1::kBusy) {
            return error;
        }
    }
    return error;
}

[[nodiscard]] bool ValidRole(SourceFrontierWriterRoleV1 role) noexcept {
    switch (role) {
        case SourceFrontierWriterRoleV1::kProducer:
        case SourceFrontierWriterRoleV1::kProcessor:
            return true;
    }
    return false;
}

[[nodiscard]] off_t RoleOffset(SourceFrontierWriterRoleV1 role) noexcept {
    return role == SourceFrontierWriterRoleV1::kProducer
        ? static_cast<off_t>(0)
        : static_cast<off_t>(1);
}

void UnlockRole(
    int descriptor,
    SourceFrontierWriterRoleV1 role) noexcept {
#if defined(__linux__) && defined(F_OFD_SETLK)
    if (descriptor < 0 || !ValidRole(role)) {
        return;
    }
    struct flock lock {};
    lock.l_type = F_UNLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = RoleOffset(role);
    lock.l_len = 1;
    while (::fcntl(descriptor, F_OFD_SETLK, &lock) != 0 &&
           errno == EINTR) {
    }
#else
    static_cast<void>(descriptor);
    static_cast<void>(role);
#endif
}

}  // namespace

SourceFrontierIdentityV1 SourceFrontierIdentityFromConfigV1(
    const SourceFrontierConfigV1& config) noexcept {
    SourceFrontierIdentityV1 result{};
    result.source_stream_id = config.source_stream_id;
    result.capture_date = config.capture_date;
    result.stream_day_id = config.stream_day_id;
    result.clock_epoch = config.clock_epoch;
    result.writer_instance = config.writer_instance;
    result.generation = config.generation;
    return result;
}

const char* SourceFrontierPosixErrorNameV1(
    SourceFrontierPosixErrorV1 error) noexcept {
    switch (error) {
        case SourceFrontierPosixErrorV1::kNone:
            return "NONE";
        case SourceFrontierPosixErrorV1::kUnsupportedPlatform:
            return "UNSUPPORTED_PLATFORM";
        case SourceFrontierPosixErrorV1::kInvalidArgument:
            return "INVALID_ARGUMENT";
        case SourceFrontierPosixErrorV1::kInvalidName:
            return "INVALID_NAME";
        case SourceFrontierPosixErrorV1::kInvalidDirectory:
            return "INVALID_DIRECTORY";
        case SourceFrontierPosixErrorV1::kDirectoryOwnerMismatch:
            return "DIRECTORY_OWNER_MISMATCH";
        case SourceFrontierPosixErrorV1::kUnsafeDirectoryMode:
            return "UNSAFE_DIRECTORY_MODE";
        case SourceFrontierPosixErrorV1::kOpenFailed:
            return "OPEN_FAILED";
        case SourceFrontierPosixErrorV1::kAlreadyExists:
            return "ALREADY_EXISTS";
        case SourceFrontierPosixErrorV1::kSymlinkRejected:
            return "SYMLINK_REJECTED";
        case SourceFrontierPosixErrorV1::kWrongFileType:
            return "WRONG_FILE_TYPE";
        case SourceFrontierPosixErrorV1::kFileOwnerMismatch:
            return "FILE_OWNER_MISMATCH";
        case SourceFrontierPosixErrorV1::kWrongFileMode:
            return "WRONG_FILE_MODE";
        case SourceFrontierPosixErrorV1::kWrongLinkCount:
            return "WRONG_LINK_COUNT";
        case SourceFrontierPosixErrorV1::kWrongFileSize:
            return "WRONG_FILE_SIZE";
        case SourceFrontierPosixErrorV1::kAllocationFailed:
            return "ALLOCATION_FAILED";
        case SourceFrontierPosixErrorV1::kMapFailed:
            return "MAP_FAILED";
        case SourceFrontierPosixErrorV1::kSyncFailed:
            return "SYNC_FAILED";
        case SourceFrontierPosixErrorV1::kSourceFrontierFailure:
            return "SOURCE_FRONTIER_FAILURE";
        case SourceFrontierPosixErrorV1::kIdentityMismatch:
            return "IDENTITY_MISMATCH";
        case SourceFrontierPosixErrorV1::kPathReplaced:
            return "PATH_REPLACED";
        case SourceFrontierPosixErrorV1::kRoleConflict:
            return "ROLE_CONFLICT";
        case SourceFrontierPosixErrorV1::kOfdLocksUnsupported:
            return "OFD_LOCKS_UNSUPPORTED";
        case SourceFrontierPosixErrorV1::kLockFailed:
            return "LOCK_FAILED";
        case SourceFrontierPosixErrorV1::kResourceExhausted:
            return "RESOURCE_EXHAUSTED";
    }
    return "UNKNOWN";
}

SourceFrontierPosixRoleLeaseV1::SourceFrontierPosixRoleLeaseV1(
    int descriptor,
    SourceFrontierWriterRoleV1 role) noexcept
    : descriptor_(descriptor), role_(role) {}

SourceFrontierPosixRoleLeaseV1::~SourceFrontierPosixRoleLeaseV1() {
    UnlockRole(descriptor_, role_);
    CloseDescriptor(descriptor_);
}

SourceFrontierPosixMappingV1::SourceFrontierPosixMappingV1(
    int directory_descriptor,
    int file_descriptor,
    SourceFrontierPageV1* page,
    std::string_view file_name,
    std::uint32_t expected_owner_uid,
    std::uint64_t device,
    std::uint64_t inode,
    SourceFrontierIdentityV1 identity)
    : directory_descriptor_(directory_descriptor),
      file_descriptor_(file_descriptor),
      page_(page),
      file_name_(file_name),
      expected_owner_uid_(expected_owner_uid),
      device_(device),
      inode_(inode),
      identity_(std::move(identity)) {}

SourceFrontierPosixMappingV1::~SourceFrontierPosixMappingV1() {
    if (page_ != nullptr) {
        static_cast<void>(
            ::munmap(page_, kSourceFrontierPageBytesV1));
    }
    CloseDescriptor(file_descriptor_);
    CloseDescriptor(directory_descriptor_);
}

SourceFrontierPosixResultV1 SourceFrontierPosixMappingV1::FinishOpen(
    const SourceFrontierPosixFileOptionsV1& options,
    const std::string& file_name,
    int file_descriptor,
    SourceFrontierPageV1* page,
    std::uint64_t device,
    std::uint64_t inode,
    SourceFrontierIdentityV1 identity,
    std::unique_ptr<SourceFrontierPosixMappingV1>* output) noexcept {
    const ValidatedFile file{device, inode};
    int path_error = 0;
    if (!NamedInodeMatches(
            options.directory_fd,
            file_name.c_str(),
            file,
            &path_error)) {
        static_cast<void>(
            ::munmap(page, kSourceFrontierPageBytesV1));
        CloseDescriptor(file_descriptor);
        return Failure(
            SourceFrontierPosixErrorV1::kPathReplaced,
            path_error);
    }
    const int directory_duplicate = DuplicateCloexec(options.directory_fd);
    if (directory_duplicate < 0) {
        const int saved_errno = errno;
        static_cast<void>(
            ::munmap(page, kSourceFrontierPageBytesV1));
        CloseDescriptor(file_descriptor);
        return Failure(
            SourceFrontierPosixErrorV1::kResourceExhausted,
            saved_errno);
    }
    try {
        output->reset(new SourceFrontierPosixMappingV1(
            directory_duplicate,
            file_descriptor,
            page,
            file_name,
            options.expected_owner_uid,
            device,
            inode,
            std::move(identity)));
    } catch (...) {
        CloseDescriptor(directory_duplicate);
        static_cast<void>(
            ::munmap(page, kSourceFrontierPageBytesV1));
        CloseDescriptor(file_descriptor);
        return Failure(
            SourceFrontierPosixErrorV1::kResourceExhausted,
            ENOMEM);
    }
    return {};
}

SourceFrontierPosixResultV1 SourceFrontierPosixMappingV1::Create(
    const SourceFrontierPosixFileOptionsV1& options,
    const SourceFrontierConfigV1& config,
    std::unique_ptr<SourceFrontierPosixMappingV1>* output) noexcept {
    if (output == nullptr) {
        return Failure(SourceFrontierPosixErrorV1::kInvalidArgument);
    }
    output->reset();
#if !defined(__linux__)
    static_cast<void>(options);
    static_cast<void>(config);
    return Failure(SourceFrontierPosixErrorV1::kUnsupportedPlatform);
#else
    const SourceFrontierPosixResultV1 directory_error =
        ValidateDirectory(options);
    if (!directory_error) {
        return directory_error;
    }
    if (static_cast<std::uint64_t>(::geteuid()) !=
        static_cast<std::uint64_t>(options.expected_owner_uid)) {
        return Failure(
            SourceFrontierPosixErrorV1::kFileOwnerMismatch);
    }
    SourceFrontierPageV1 validation_page{};
    const SourceFrontierErrorV1 validation_error =
        InitializeSourceFrontierPageV1(config, &validation_page);
    if (validation_error != SourceFrontierErrorV1::kNone) {
        return Failure(
            SourceFrontierPosixErrorV1::kSourceFrontierFailure,
            0,
            validation_error);
    }
    std::string name;
    try {
        name.assign(options.file_name);
    } catch (...) {
        return Failure(
            SourceFrontierPosixErrorV1::kResourceExhausted,
            ENOMEM);
    }
    const int flags = O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC |
                      O_NOFOLLOW | O_NONBLOCK | O_NOCTTY;
    const int descriptor = OpenAtLoop(
        options.directory_fd,
        name.c_str(),
        flags,
        static_cast<mode_t>(kSourceFrontierPosixFileModeV1),
        true);
    if (descriptor < 0) {
        return OpenFailure(errno, true);
    }
    if (::fchmod(
            descriptor,
            static_cast<mode_t>(kSourceFrontierPosixFileModeV1)) != 0) {
        const int saved_errno = errno;
        CloseDescriptor(descriptor);
        return Failure(
            SourceFrontierPosixErrorV1::kWrongFileMode,
            saved_errno);
    }
    int allocation_error = 0;
    do {
        allocation_error = ::posix_fallocate(
            descriptor,
            static_cast<off_t>(0),
            static_cast<off_t>(kSourceFrontierPageBytesV1));
    } while (allocation_error == EINTR);
    if (allocation_error != 0) {
        CloseDescriptor(descriptor);
        return Failure(
            SourceFrontierPosixErrorV1::kAllocationFailed,
            allocation_error);
    }
    ValidatedFile file{};
    const SourceFrontierPosixResultV1 file_error = ValidateFile(
        descriptor, options.expected_owner_uid, &file);
    if (!file_error) {
        CloseDescriptor(descriptor);
        return file_error;
    }
    void* const raw_mapping = ::mmap(
        nullptr,
        kSourceFrontierPageBytesV1,
        PROT_READ | PROT_WRITE,
        MAP_SHARED,
        descriptor,
        0);
    if (raw_mapping == MAP_FAILED) {
        const int saved_errno = errno;
        CloseDescriptor(descriptor);
        return Failure(
            SourceFrontierPosixErrorV1::kMapFailed,
            saved_errno);
    }
    auto* const page = static_cast<SourceFrontierPageV1*>(raw_mapping);
    const SourceFrontierErrorV1 initialize_error =
        InitializeSourceFrontierPageV1(config, page);
    if (initialize_error != SourceFrontierErrorV1::kNone) {
        static_cast<void>(
            ::munmap(page, kSourceFrontierPageBytesV1));
        CloseDescriptor(descriptor);
        return Failure(
            SourceFrontierPosixErrorV1::kSourceFrontierFailure,
            0,
            initialize_error);
    }
    if (!SyncMemory(page) || !SyncDescriptor(descriptor) ||
        !SyncDescriptor(options.directory_fd)) {
        const int saved_errno = errno;
        static_cast<void>(
            ::munmap(page, kSourceFrontierPageBytesV1));
        CloseDescriptor(descriptor);
        return Failure(
            SourceFrontierPosixErrorV1::kSyncFailed,
            saved_errno);
    }
    return FinishOpen(
        options,
        name,
        descriptor,
        page,
        file.device,
        file.inode,
        SourceFrontierIdentityFromConfigV1(config),
        output);
#endif
}

SourceFrontierPosixResultV1 SourceFrontierPosixMappingV1::Attach(
    const SourceFrontierPosixFileOptionsV1& options,
    const SourceFrontierIdentityV1& expected_identity,
    std::unique_ptr<SourceFrontierPosixMappingV1>* output) noexcept {
    if (output == nullptr) {
        return Failure(SourceFrontierPosixErrorV1::kInvalidArgument);
    }
    output->reset();
#if !defined(__linux__)
    static_cast<void>(options);
    static_cast<void>(expected_identity);
    return Failure(SourceFrontierPosixErrorV1::kUnsupportedPlatform);
#else
    const SourceFrontierPosixResultV1 directory_error =
        ValidateDirectory(options);
    if (!directory_error) {
        return directory_error;
    }
    const SourceFrontierPosixResultV1 identity_shape =
        ValidateIdentityShape(expected_identity);
    if (!identity_shape) {
        return identity_shape;
    }
    std::string name;
    try {
        name.assign(options.file_name);
    } catch (...) {
        return Failure(
            SourceFrontierPosixErrorV1::kResourceExhausted,
            ENOMEM);
    }
    const int descriptor = OpenAtLoop(
        options.directory_fd,
        name.c_str(),
        O_RDWR | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK | O_NOCTTY,
        static_cast<mode_t>(0),
        false);
    if (descriptor < 0) {
        return OpenFailure(errno, false);
    }
    ValidatedFile file{};
    const SourceFrontierPosixResultV1 file_error = ValidateFile(
        descriptor, options.expected_owner_uid, &file);
    if (!file_error) {
        CloseDescriptor(descriptor);
        return file_error;
    }
    void* const raw_mapping = ::mmap(
        nullptr,
        kSourceFrontierPageBytesV1,
        PROT_READ | PROT_WRITE,
        MAP_SHARED,
        descriptor,
        0);
    if (raw_mapping == MAP_FAILED) {
        const int saved_errno = errno;
        CloseDescriptor(descriptor);
        return Failure(
            SourceFrontierPosixErrorV1::kMapFailed,
            saved_errno);
    }
    auto* const page = static_cast<SourceFrontierPageV1*>(raw_mapping);
    SourceFrontierIdentityV1 observed_identity{};
    const SourceFrontierErrorV1 read_error =
        ReadStableIdentity(*page, &observed_identity);
    if (read_error != SourceFrontierErrorV1::kNone) {
        static_cast<void>(
            ::munmap(page, kSourceFrontierPageBytesV1));
        CloseDescriptor(descriptor);
        return Failure(
            SourceFrontierPosixErrorV1::kSourceFrontierFailure,
            0,
            read_error);
    }
    if (observed_identity != expected_identity) {
        static_cast<void>(
            ::munmap(page, kSourceFrontierPageBytesV1));
        CloseDescriptor(descriptor);
        return Failure(SourceFrontierPosixErrorV1::kIdentityMismatch);
    }
    return FinishOpen(
        options,
        name,
        descriptor,
        page,
        file.device,
        file.inode,
        std::move(observed_identity),
        output);
#endif
}

SourceFrontierPosixResultV1 SourceFrontierPosixMappingV1::AcquireRole(
    SourceFrontierWriterRoleV1 role,
    std::unique_ptr<SourceFrontierPosixRoleLeaseV1>* output) const
    noexcept {
    if (output == nullptr || !ValidRole(role) ||
        directory_descriptor_ < 0 || page_ == nullptr) {
        return Failure(SourceFrontierPosixErrorV1::kInvalidArgument);
    }
    output->reset();
#if !defined(__linux__) || !defined(F_OFD_SETLK)
    return Failure(SourceFrontierPosixErrorV1::kOfdLocksUnsupported);
#else
    const int descriptor = OpenAtLoop(
        directory_descriptor_,
        file_name_.c_str(),
        O_RDWR | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK | O_NOCTTY,
        static_cast<mode_t>(0),
        false);
    if (descriptor < 0) {
        return OpenFailure(errno, false);
    }
    ValidatedFile reopened{};
    const SourceFrontierPosixResultV1 file_error = ValidateFile(
        descriptor, expected_owner_uid_, &reopened);
    if (!file_error) {
        CloseDescriptor(descriptor);
        return file_error;
    }
    if (reopened.device != device_ || reopened.inode != inode_) {
        CloseDescriptor(descriptor);
        return Failure(SourceFrontierPosixErrorV1::kPathReplaced);
    }
    struct flock lock {};
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = RoleOffset(role);
    lock.l_len = 1;
    int lock_result = -1;
    do {
        lock_result = ::fcntl(descriptor, F_OFD_SETLK, &lock);
    } while (lock_result != 0 && errno == EINTR);
    if (lock_result != 0) {
        const int saved_errno = errno;
        CloseDescriptor(descriptor);
        if (saved_errno == EACCES || saved_errno == EAGAIN) {
            return Failure(
                SourceFrontierPosixErrorV1::kRoleConflict,
                saved_errno);
        }
        if (saved_errno == EINVAL || saved_errno == ENOSYS) {
            return Failure(
                SourceFrontierPosixErrorV1::kOfdLocksUnsupported,
                saved_errno);
        }
        return Failure(
            SourceFrontierPosixErrorV1::kLockFailed,
            saved_errno);
    }
    try {
        output->reset(new SourceFrontierPosixRoleLeaseV1(
            descriptor, role));
    } catch (...) {
        UnlockRole(descriptor, role);
        CloseDescriptor(descriptor);
        return Failure(
            SourceFrontierPosixErrorV1::kResourceExhausted,
            ENOMEM);
    }
    return {};
#endif
}

}  // namespace l2flow::canonical
