#include "l2flow/ingress/run_manifest_posix_store.h"

#include "l2flow/common/sha256.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dirent.h>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include <fcntl.h>
#include <linux/fs.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace l2flow::ingress {

namespace {

class ScopedFd final {
public:
    ScopedFd() noexcept = default;
    explicit ScopedFd(int fd) noexcept : fd_(fd) {}
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
    void Reset(int replacement = -1) noexcept {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
        fd_ = replacement;
    }

private:
    int fd_ = -1;
};

class ScopedDir final {
public:
    explicit ScopedDir(DIR* directory) noexcept
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

struct CandidateInventory final {
    bool final_present = false;
    bool temporary_present = false;
    std::uint32_t count = 0U;
};

struct LoadedCandidate final {
    ScopedFd fd;
    struct stat status {};
    std::string bytes;
};

void SetDiagnostic(
    std::string* diagnostic,
    std::string_view message) noexcept {
    if (diagnostic == nullptr) {
        return;
    }
    try {
        diagnostic->assign(
            message.data(), message.size());
    } catch (...) {
    }
}

template <std::size_t Size>
[[nodiscard]] bool IsZero(
    const std::array<std::byte, Size>& value) noexcept {
    return std::all_of(
        value.begin(),
        value.end(),
        [](std::byte byte) noexcept {
            return byte == std::byte{0};
        });
}

[[nodiscard]] int OpenAtNoIntr(
    int directory_fd,
    const char* name,
    int flags,
    mode_t mode = 0) noexcept {
    for (;;) {
        const int result =
            (flags & O_CREAT) != 0
                ? ::openat(
                      directory_fd,
                      name,
                      flags,
                      mode)
                : ::openat(
                      directory_fd,
                      name,
                      flags);
        if (result >= 0 || errno != EINTR) {
            return result;
        }
    }
}

[[nodiscard]] bool FsyncNoIntr(int fd) noexcept {
    for (;;) {
        if (::fsync(fd) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool LockExclusiveNoIntr(int fd) noexcept {
    for (;;) {
        if (::flock(fd, LOCK_EX) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool UnlockNoIntr(int fd) noexcept {
    for (;;) {
        if (::flock(fd, LOCK_UN) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool RenameNoReplace(
    int directory_fd,
    const char* old_name,
    const char* new_name) noexcept {
    for (;;) {
        const long result = ::syscall(
            SYS_renameat2,
            directory_fd,
            old_name,
            directory_fd,
            new_name,
            RENAME_NOREPLACE);
        if (result == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool SameInode(
    const struct stat& left,
    const struct stat& right) noexcept {
    return left.st_dev == right.st_dev &&
           left.st_ino == right.st_ino;
}

[[nodiscard]] bool IsSafeDirectory(
    const struct stat& status) noexcept {
    return S_ISDIR(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           (status.st_mode & 07777U) == 0700U;
}

[[nodiscard]] bool IsSafeFile(
    const struct stat& status,
    const struct stat& directory) noexcept {
    return S_ISREG(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           (status.st_mode & 07777U) == 0600U &&
           status.st_nlink == static_cast<nlink_t>(1) &&
           status.st_size >= 0 &&
           static_cast<std::uint64_t>(status.st_size) <=
               kRunManifestV1MaximumBytes &&
           status.st_dev == directory.st_dev;
}

[[nodiscard]] bool NameMatchesDescriptor(
    int directory_fd,
    const char* name,
    int fd,
    const struct stat* expected = nullptr) noexcept {
    struct stat opened {};
    struct stat named {};
    return ::fstat(fd, &opened) == 0 &&
           ::fstatat(
               directory_fd,
               name,
               &named,
               AT_SYMLINK_NOFOLLOW) == 0 &&
           SameInode(opened, named) &&
           (expected == nullptr ||
            (SameInode(opened, *expected) &&
             opened.st_size == expected->st_size));
}

[[nodiscard]] bool NameAbsent(
    int directory_fd,
    const char* name) noexcept {
    struct stat status {};
    return ::fstatat(
               directory_fd,
               name,
               &status,
               AT_SYMLINK_NOFOLLOW) != 0 &&
           errno == ENOENT;
}

[[nodiscard]] bool DirectoryMatches(
    int directory_fd,
    const struct stat& expected) noexcept {
    struct stat actual {};
    return ::fstat(directory_fd, &actual) == 0 &&
           IsSafeDirectory(actual) &&
           SameInode(actual, expected);
}

[[nodiscard]] RunManifestPosixStoreErrorV1
OpenRetainedDirectory(
    int retained_directory_fd,
    ScopedFd* output,
    struct stat* status) noexcept {
    if (retained_directory_fd < 0 ||
        output == nullptr || status == nullptr) {
        return RunManifestPosixStoreErrorV1::
            kInvalidArgument;
    }
    struct stat retained_status {};
    if (::fstat(
            retained_directory_fd,
            &retained_status) != 0 ||
        !IsSafeDirectory(retained_status)) {
        return RunManifestPosixStoreErrorV1::
            kUnsafeDirectory;
    }
    ScopedFd candidate(
        OpenAtNoIntr(
            retained_directory_fd,
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC));
    struct stat candidate_status {};
    if (candidate.get() < 0 ||
        ::fstat(
            candidate.get(),
            &candidate_status) != 0 ||
        !IsSafeDirectory(candidate_status) ||
        !SameInode(
            retained_status,
            candidate_status)) {
        return RunManifestPosixStoreErrorV1::
            kUnsafeDirectory;
    }
    *status = candidate_status;
    *output = std::move(candidate);
    return RunManifestPosixStoreErrorV1::kNone;
}

[[nodiscard]] bool IsLowerHex(
    std::string_view text) noexcept {
    return std::all_of(
        text.begin(),
        text.end(),
        [](char character) noexcept {
            return (character >= '0' &&
                    character <= '9') ||
                   (character >= 'a' &&
                    character <= 'f');
        });
}

[[nodiscard]] bool IsFinalName(
    std::string_view name) noexcept {
    const std::size_t expected =
        kRunManifestV1FilenamePrefix.size() +
        32U +
        kRunManifestV1FilenameSuffix.size();
    return name.size() == expected &&
           name.starts_with(
               kRunManifestV1FilenamePrefix) &&
           name.ends_with(
               kRunManifestV1FilenameSuffix) &&
           IsLowerHex(
               name.substr(
                   kRunManifestV1FilenamePrefix.size(),
                   32U));
}

[[nodiscard]] bool IsTemporaryName(
    std::string_view name) noexcept {
    if (name.size() <=
            1U +
                kRunManifestV1TemporarySuffix.size() ||
        name.front() != '.' ||
        !name.ends_with(
            kRunManifestV1TemporarySuffix)) {
        return false;
    }
    const std::size_t final_size =
        name.size() - 1U -
        kRunManifestV1TemporarySuffix.size();
    return IsFinalName(
        name.substr(1U, final_size));
}

[[nodiscard]] bool StartsLikeCandidate(
    std::string_view name) noexcept {
    return name.starts_with(
               kRunManifestV1FilenamePrefix) ||
           name.starts_with(
               std::string_view{
                   ".run-manifest-"});
}

[[nodiscard]] RunManifestPosixStoreErrorV1
InventoryCandidates(
    int directory_fd,
    const struct stat& directory_status,
    std::string_view expected_final,
    std::string_view expected_temporary,
    CandidateInventory* output) {
    if (output == nullptr) {
        return RunManifestPosixStoreErrorV1::
            kInvalidArgument;
    }
    ScopedFd scan(
        OpenAtNoIntr(
            directory_fd,
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC));
    struct stat scan_status {};
    if (scan.get() < 0 ||
        ::fstat(scan.get(), &scan_status) != 0 ||
        !SameInode(
            directory_status, scan_status)) {
        return RunManifestPosixStoreErrorV1::
            kUnsafeDirectory;
    }
    DIR* const native = ::fdopendir(scan.Release());
    if (native == nullptr) {
        return RunManifestPosixStoreErrorV1::
            kUnsafeDirectory;
    }
    ScopedDir entries(native);
    CandidateInventory inventory{};
    for (;;) {
        errno = 0;
        dirent* const entry =
            ::readdir(entries.get());
        if (entry == nullptr) {
            if (errno != 0) {
                return RunManifestPosixStoreErrorV1::
                    kUnsafeDirectory;
            }
            break;
        }
        const std::string_view name(entry->d_name);
        if (name == "." || name == "..") {
            continue;
        }
        const bool final = IsFinalName(name);
        const bool temporary =
            IsTemporaryName(name);
        if (StartsLikeCandidate(name) &&
            !final && !temporary) {
            return RunManifestPosixStoreErrorV1::
                kMalformedCandidateName;
        }
        if (!final && !temporary) {
            continue;
        }
        ++inventory.count;
        if (inventory.count > 1U ||
            (name != expected_final &&
             name != expected_temporary)) {
            *output = inventory;
            return RunManifestPosixStoreErrorV1::
                kAmbiguousCandidates;
        }
        inventory.final_present = final;
        inventory.temporary_present = temporary;
    }
    if (!DirectoryMatches(
            directory_fd, directory_status)) {
        return RunManifestPosixStoreErrorV1::
            kUnsafeDirectory;
    }
    *output = inventory;
    return RunManifestPosixStoreErrorV1::kNone;
}

[[nodiscard]] RunManifestPosixStoreErrorV1
RequireSoleCandidate(
    int directory_fd,
    const struct stat& directory_status,
    const std::string& final_name,
    const std::string& temporary_name,
    bool expect_final) {
    CandidateInventory inventory{};
    const RunManifestPosixStoreErrorV1 error =
        InventoryCandidates(
            directory_fd,
            directory_status,
            final_name,
            temporary_name,
            &inventory);
    if (error !=
        RunManifestPosixStoreErrorV1::kNone) {
        return error;
    }
    if (inventory.count != 1U ||
        inventory.final_present != expect_final ||
        inventory.temporary_present == expect_final) {
        return RunManifestPosixStoreErrorV1::
            kPublishConflict;
    }
    return RunManifestPosixStoreErrorV1::kNone;
}

[[nodiscard]] bool ReadExact(
    int fd,
    std::size_t size,
    std::string* output) {
    if (output == nullptr ||
        size >
            static_cast<std::size_t>(
                std::numeric_limits<off_t>::max())) {
        return false;
    }
    std::string candidate(size, '\0');
    std::size_t completed = 0U;
    while (completed < candidate.size()) {
        const ssize_t result = ::pread(
            fd,
            candidate.data() + completed,
            candidate.size() - completed,
            static_cast<off_t>(completed));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            return false;
        }
        completed +=
            static_cast<std::size_t>(result);
    }
    output->swap(candidate);
    return true;
}

[[nodiscard]] RunManifestPosixStoreErrorV1
LoadExactCandidate(
    int directory_fd,
    const struct stat& directory_status,
    const std::string& name,
    std::string_view expected_bytes,
    const RawV1Digest& expected_sha256,
    LoadedCandidate* output) {
    if (output == nullptr) {
        return RunManifestPosixStoreErrorV1::
            kInvalidArgument;
    }
    ScopedFd fd(
        OpenAtNoIntr(
            directory_fd,
            name.c_str(),
            O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC));
    if (fd.get() < 0) {
        return errno == ENOENT
                   ? RunManifestPosixStoreErrorV1::
                         kPublishConflict
                   : RunManifestPosixStoreErrorV1::
                         kUnsafeCandidate;
    }
    struct stat before {};
    if (::fstat(fd.get(), &before) != 0 ||
        !IsSafeFile(before, directory_status) ||
        !NameMatchesDescriptor(
            directory_fd,
            name.c_str(),
            fd.get(),
            &before)) {
        return RunManifestPosixStoreErrorV1::
            kUnsafeCandidate;
    }
    if (static_cast<std::uint64_t>(before.st_size) !=
        expected_bytes.size()) {
        return RunManifestPosixStoreErrorV1::
            kCandidateConflict;
    }
    std::string bytes;
    if (!ReadExact(
            fd.get(),
            expected_bytes.size(),
            &bytes)) {
        return RunManifestPosixStoreErrorV1::
            kReadbackFailure;
    }
    struct stat after {};
    if (::fstat(fd.get(), &after) != 0 ||
        !IsSafeFile(after, directory_status) ||
        !SameInode(before, after) ||
        before.st_size != after.st_size ||
        !NameMatchesDescriptor(
            directory_fd,
            name.c_str(),
            fd.get(),
            &after)) {
        return RunManifestPosixStoreErrorV1::
            kUnsafeCandidate;
    }
    if (bytes != expected_bytes ||
        l2flow::common::ComputeSha256(
            std::string_view{bytes}) !=
            expected_sha256) {
        return RunManifestPosixStoreErrorV1::
            kCandidateConflict;
    }
    LoadedCandidate candidate{};
    candidate.fd = std::move(fd);
    candidate.status = after;
    candidate.bytes = std::move(bytes);
    *output = std::move(candidate);
    return RunManifestPosixStoreErrorV1::kNone;
}

[[nodiscard]] bool WriteExact(
    int directory_fd,
    const struct stat& directory_status,
    const std::string& name,
    int fd,
    std::string_view bytes) noexcept {
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        struct stat file_status {};
        if (!DirectoryMatches(
                directory_fd, directory_status) ||
            ::fstat(fd, &file_status) != 0 ||
            !IsSafeFile(
                file_status, directory_status) ||
            !NameMatchesDescriptor(
                directory_fd,
                name.c_str(),
                fd,
                &file_status)) {
            return false;
        }
        const ssize_t result = ::pwrite(
            fd,
            bytes.data() + completed,
            bytes.size() - completed,
            static_cast<off_t>(completed));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            return false;
        }
        completed +=
            static_cast<std::size_t>(result);
    }
    return DirectoryMatches(
               directory_fd, directory_status) &&
           NameMatchesDescriptor(
               directory_fd,
               name.c_str(),
               fd);
}

[[nodiscard]] bool ValidateCapability(
    const BuiltRunManifestV1& manifest) noexcept {
    const std::string_view bytes =
        manifest.canonical_jcs();
    return ValidateRunManifestV1(
               manifest.model()) ==
               RunManifestV1Error::kNone &&
           !bytes.empty() &&
           bytes.size() <=
               kRunManifestV1MaximumBytes &&
           bytes.front() == '{' &&
           bytes.back() == '}' &&
           bytes.find('\n') ==
               std::string_view::npos &&
           l2flow::common::ComputeSha256(bytes) ==
               manifest.sha256();
}

[[nodiscard]] RunManifestPosixStoreErrorV1
SyncActualDirectory(
    int directory_fd,
    const struct stat& expected) noexcept {
    if (!DirectoryMatches(
            directory_fd, expected)) {
        return RunManifestPosixStoreErrorV1::
            kUnsafeDirectory;
    }
    if (!FsyncNoIntr(directory_fd)) {
        return RunManifestPosixStoreErrorV1::
            kSyncFailure;
    }
    if (!DirectoryMatches(
            directory_fd, expected)) {
        return RunManifestPosixStoreErrorV1::
            kUnsafeDirectory;
    }
    return RunManifestPosixStoreErrorV1::kNone;
}

[[nodiscard]] std::uint64_t Device(
    const struct stat& status) noexcept {
    return static_cast<std::uint64_t>(
        status.st_dev);
}

[[nodiscard]] std::uint64_t Inode(
    const struct stat& status) noexcept {
    return static_cast<std::uint64_t>(
        status.st_ino);
}

}  // namespace

struct RunManifestPosixPublishAccessV1 final {
    [[nodiscard]] static std::unique_ptr<
        PublishedRunManifestReceiptV1>
    CreateReceipt(
        RawV1Identity run_id,
        RawV1Digest sha256,
        std::size_t byte_count,
        std::string filename,
        int directory_fd,
        int file_fd,
        const struct stat& directory_status,
        const struct stat& file_status) {
        try {
            return std::unique_ptr<
                PublishedRunManifestReceiptV1>(
                new PublishedRunManifestReceiptV1(
                    run_id,
                    sha256,
                    byte_count,
                    std::move(filename),
                    directory_fd,
                    file_fd,
                    Device(directory_status),
                    Inode(directory_status),
                    Device(file_status),
                    Inode(file_status)));
        } catch (...) {
            if (file_fd >= 0) {
                static_cast<void>(::close(file_fd));
            }
            if (directory_fd >= 0) {
                static_cast<void>(
                    ::close(directory_fd));
            }
            throw;
        }
    }
};

std::string_view RunManifestPosixStoreErrorV1Name(
    RunManifestPosixStoreErrorV1 error) noexcept {
    switch (error) {
        case RunManifestPosixStoreErrorV1::kNone:
            return "none";
        case RunManifestPosixStoreErrorV1::
            kInvalidArgument:
            return "invalid_argument";
        case RunManifestPosixStoreErrorV1::
            kUnsafeDirectory:
            return "unsafe_directory";
        case RunManifestPosixStoreErrorV1::
            kMalformedCandidateName:
            return "malformed_candidate_name";
        case RunManifestPosixStoreErrorV1::
            kAmbiguousCandidates:
            return "ambiguous_candidates";
        case RunManifestPosixStoreErrorV1::
            kUnsafeCandidate:
            return "unsafe_candidate";
        case RunManifestPosixStoreErrorV1::
            kCandidateConflict:
            return "candidate_conflict";
        case RunManifestPosixStoreErrorV1::
            kTemporaryCreate:
            return "temporary_create";
        case RunManifestPosixStoreErrorV1::
            kWriteFailure:
            return "write_failure";
        case RunManifestPosixStoreErrorV1::
            kSyncFailure:
            return "sync_failure";
        case RunManifestPosixStoreErrorV1::
            kPublishConflict:
            return "publish_conflict";
        case RunManifestPosixStoreErrorV1::
            kReadbackFailure:
            return "readback_failure";
        case RunManifestPosixStoreErrorV1::
            kAllocationFailure:
            return "allocation_failure";
    }
    return "unknown";
}

RunManifestPosixStoreErrorV1
RunManifestV1Filename(
    const BuiltRunManifestV1& manifest,
    std::string* filename) noexcept {
    if (filename == nullptr ||
        IsZero(manifest.model().run_id)) {
        return RunManifestPosixStoreErrorV1::
            kInvalidArgument;
    }
    try {
        constexpr std::string_view hex =
            "0123456789abcdef";
        std::string candidate;
        candidate.reserve(
            kRunManifestV1FilenamePrefix.size() +
            32U +
            kRunManifestV1FilenameSuffix.size());
        candidate.append(
            kRunManifestV1FilenamePrefix);
        for (const std::byte byte :
             manifest.model().run_id) {
            const std::uint8_t octet =
                std::to_integer<std::uint8_t>(
                    byte);
            candidate.push_back(
                hex[(octet >> 4U) & 0x0fU]);
            candidate.push_back(
                hex[octet & 0x0fU]);
        }
        candidate.append(
            kRunManifestV1FilenameSuffix);
        filename->swap(candidate);
        return RunManifestPosixStoreErrorV1::kNone;
    } catch (...) {
        return RunManifestPosixStoreErrorV1::
            kAllocationFailure;
    }
}

RunManifestPosixStoreErrorV1
RunManifestV1TemporaryFilename(
    const BuiltRunManifestV1& manifest,
    std::string* filename) noexcept {
    if (filename == nullptr) {
        return RunManifestPosixStoreErrorV1::
            kInvalidArgument;
    }
    try {
        std::string final_name;
        const RunManifestPosixStoreErrorV1 error =
            RunManifestV1Filename(
                manifest, &final_name);
        if (error !=
            RunManifestPosixStoreErrorV1::kNone) {
            return error;
        }
        std::string candidate;
        candidate.reserve(
            1U + final_name.size() +
            kRunManifestV1TemporarySuffix.size());
        candidate.push_back('.');
        candidate.append(final_name);
        candidate.append(
            kRunManifestV1TemporarySuffix);
        filename->swap(candidate);
        return RunManifestPosixStoreErrorV1::kNone;
    } catch (...) {
        return RunManifestPosixStoreErrorV1::
            kAllocationFailure;
    }
}

PublishedRunManifestReceiptV1::
PublishedRunManifestReceiptV1(
    RawV1Identity run_id,
    RawV1Digest sha256,
    std::size_t byte_count,
    std::string filename,
    int directory_fd,
    int file_fd,
    std::uint64_t directory_device,
    std::uint64_t directory_inode,
    std::uint64_t file_device,
    std::uint64_t file_inode) noexcept
    : run_id_(run_id),
      sha256_(sha256),
      byte_count_(byte_count),
      filename_(std::move(filename)),
      directory_fd_(directory_fd),
      file_fd_(file_fd),
      directory_device_(directory_device),
      directory_inode_(directory_inode),
      file_device_(file_device),
      file_inode_(file_inode) {}

PublishedRunManifestReceiptV1::
~PublishedRunManifestReceiptV1() {
    if (file_fd_ >= 0) {
        static_cast<void>(::close(file_fd_));
    }
    if (directory_fd_ >= 0) {
        static_cast<void>(::close(directory_fd_));
    }
}

bool PublishedRunManifestReceiptV1::Validate(
    std::string* diagnostic) const noexcept {
    try {
        struct stat directory {};
        struct stat before {};
        struct stat after {};
        const std::string temporary_name =
            "." + filename_ +
            std::string(
                kRunManifestV1TemporarySuffix);
        if (directory_fd_ < 0 || file_fd_ < 0 ||
            filename_.empty() ||
            byte_count_ == 0U ||
            byte_count_ >
                kRunManifestV1MaximumBytes ||
            ::fstat(
                directory_fd_, &directory) != 0 ||
            !IsSafeDirectory(directory) ||
            Device(directory) !=
                directory_device_ ||
            Inode(directory) !=
                directory_inode_ ||
            ::fstat(file_fd_, &before) != 0 ||
            !IsSafeFile(before, directory) ||
            Device(before) != file_device_ ||
            Inode(before) != file_inode_ ||
            static_cast<std::uint64_t>(
                before.st_size) != byte_count_ ||
            !NameMatchesDescriptor(
                directory_fd_,
                filename_.c_str(),
                file_fd_,
                &before)) {
            SetDiagnostic(
                diagnostic,
                "published run manifest identity is stale");
            return false;
        }
        std::string bytes;
        if (!ReadExact(
                file_fd_, byte_count_, &bytes) ||
            l2flow::common::ComputeSha256(
                std::string_view{bytes}) !=
                sha256_ ||
            ::fstat(file_fd_, &after) != 0 ||
            !IsSafeFile(after, directory) ||
            !SameInode(before, after) ||
            before.st_size != after.st_size ||
            !NameMatchesDescriptor(
                directory_fd_,
                filename_.c_str(),
                file_fd_,
                &after) ||
            !DirectoryMatches(
                directory_fd_, directory) ||
            RequireSoleCandidate(
                directory_fd_,
                directory,
                filename_,
                temporary_name,
                true) !=
                RunManifestPosixStoreErrorV1::
                    kNone) {
            SetDiagnostic(
                diagnostic,
                "published run manifest readback changed");
            return false;
        }
        SetDiagnostic(diagnostic, "");
        return true;
    } catch (...) {
        SetDiagnostic(
            diagnostic,
            "published run manifest validation allocation failed");
        return false;
    }
}

RunManifestPosixPublishResultV1
PublishRunManifestV1At(
    int retained_directory_fd,
    const BuiltRunManifestV1& manifest,
    std::string* diagnostic) noexcept {
    RunManifestPosixPublishResultV1 result{};
    try {
        if (!ValidateCapability(manifest)) {
            result.error =
                RunManifestPosixStoreErrorV1::
                    kInvalidArgument;
            SetDiagnostic(
                diagnostic,
                "built run manifest capability is invalid");
            return result;
        }
        result.manifest_sha256 = manifest.sha256();
        result.error = RunManifestV1Filename(
            manifest, &result.filename);
        if (result.error !=
            RunManifestPosixStoreErrorV1::kNone) {
            return result;
        }
        std::string temporary;
        result.error =
            RunManifestV1TemporaryFilename(
                manifest, &temporary);
        if (result.error !=
            RunManifestPosixStoreErrorV1::kNone) {
            return result;
        }

        ScopedFd directory;
        struct stat directory_status {};
        result.error = OpenRetainedDirectory(
            retained_directory_fd,
            &directory,
            &directory_status);
        if (result.error !=
            RunManifestPosixStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "run manifest directory is not retained owner-only storage");
            return result;
        }
        if (!LockExclusiveNoIntr(directory.get()) ||
            !DirectoryMatches(
                directory.get(),
                directory_status)) {
            result.error =
                RunManifestPosixStoreErrorV1::
                    kUnsafeDirectory;
            SetDiagnostic(
                diagnostic,
                "run manifest directory cannot be serialized");
            return result;
        }

        CandidateInventory inventory{};
        result.error = InventoryCandidates(
            directory.get(),
            directory_status,
            result.filename,
            temporary,
            &inventory);
        result.observed_candidate_count =
            inventory.count;
        if (result.error !=
            RunManifestPosixStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "run manifest candidate inventory is ambiguous");
            return result;
        }

        const std::string_view desired =
            manifest.canonical_jcs();
        LoadedCandidate retained_candidate{};
        bool adopted = false;
        if (inventory.final_present) {
            result.error = LoadExactCandidate(
                directory.get(),
                directory_status,
                result.filename,
                desired,
                manifest.sha256(),
                &retained_candidate);
            if (result.error !=
                RunManifestPosixStoreErrorV1::kNone) {
                SetDiagnostic(
                    diagnostic,
                    "existing run manifest final is not exact");
                return result;
            }
            if (!FsyncNoIntr(
                    retained_candidate.fd.get())) {
                result.error =
                    RunManifestPosixStoreErrorV1::
                        kSyncFailure;
                return result;
            }
            result.file_synced = true;
            result.disposition =
                RunManifestPosixDispositionV1::
                    kAcceptedExistingFinal;
        } else {
            if (inventory.temporary_present) {
                result.error = LoadExactCandidate(
                    directory.get(),
                    directory_status,
                    temporary,
                    desired,
                    manifest.sha256(),
                    &retained_candidate);
                if (result.error !=
                    RunManifestPosixStoreErrorV1::
                        kNone) {
                    SetDiagnostic(
                        diagnostic,
                        "existing run manifest temporary is partial or conflicting");
                    return result;
                }
                adopted = true;
            } else {
                if (!DirectoryMatches(
                        directory.get(),
                        directory_status) ||
                    !NameAbsent(
                        directory.get(),
                        result.filename.c_str()) ||
                    !NameAbsent(
                        directory.get(),
                        temporary.c_str())) {
                    result.error =
                        RunManifestPosixStoreErrorV1::
                            kPublishConflict;
                    return result;
                }
                ScopedFd created(
                    OpenAtNoIntr(
                        directory.get(),
                        temporary.c_str(),
                        O_RDWR | O_CREAT | O_EXCL |
                            O_NOFOLLOW | O_NONBLOCK |
                            O_CLOEXEC,
                        0600));
                if (created.get() < 0) {
                    result.error =
                        RunManifestPosixStoreErrorV1::
                            kTemporaryCreate;
                    return result;
                }
                struct stat created_status {};
                if (::fchmod(
                        created.get(), 0600) != 0 ||
                    ::fstat(
                        created.get(),
                        &created_status) != 0 ||
                    !IsSafeFile(
                        created_status,
                        directory_status) ||
                    created_status.st_size != 0 ||
                    !NameMatchesDescriptor(
                        directory.get(),
                        temporary.c_str(),
                        created.get(),
                        &created_status) ||
                    !WriteExact(
                        directory.get(),
                        directory_status,
                        temporary,
                        created.get(),
                        desired)) {
                    result.error =
                        RunManifestPosixStoreErrorV1::
                            kWriteFailure;
                    return result;
                }
                retained_candidate.fd =
                    std::move(created);
            }

            struct stat temporary_status {};
            std::string temporary_readback;
            if (::fstat(
                    retained_candidate.fd.get(),
                    &temporary_status) != 0 ||
                !IsSafeFile(
                    temporary_status,
                    directory_status) ||
                static_cast<std::uint64_t>(
                    temporary_status.st_size) !=
                    desired.size() ||
                !NameMatchesDescriptor(
                    directory.get(),
                    temporary.c_str(),
                    retained_candidate.fd.get(),
                    &temporary_status) ||
                !ReadExact(
                    retained_candidate.fd.get(),
                    desired.size(),
                    &temporary_readback) ||
                temporary_readback != desired ||
                l2flow::common::ComputeSha256(
                    std::string_view{
                        temporary_readback}) !=
                    manifest.sha256()) {
                result.error =
                    RunManifestPosixStoreErrorV1::
                        kReadbackFailure;
                return result;
            }
            if (!FsyncNoIntr(
                    retained_candidate.fd.get())) {
                result.error =
                    RunManifestPosixStoreErrorV1::
                        kSyncFailure;
                return result;
            }
            result.file_synced = true;
            result.error = RequireSoleCandidate(
                directory.get(),
                directory_status,
                result.filename,
                temporary,
                false);
            if (result.error !=
                RunManifestPosixStoreErrorV1::kNone) {
                return result;
            }
            if (!DirectoryMatches(
                    directory.get(),
                    directory_status) ||
                !NameAbsent(
                    directory.get(),
                    result.filename.c_str()) ||
                !NameMatchesDescriptor(
                    directory.get(),
                    temporary.c_str(),
                    retained_candidate.fd.get(),
                    &temporary_status) ||
                !RenameNoReplace(
                    directory.get(),
                    temporary.c_str(),
                    result.filename.c_str())) {
                result.error =
                    RunManifestPosixStoreErrorV1::
                        kPublishConflict;
                return result;
            }
            if (!NameMatchesDescriptor(
                    directory.get(),
                    result.filename.c_str(),
                    retained_candidate.fd.get()) ||
                !NameAbsent(
                    directory.get(),
                    temporary.c_str())) {
                result.error =
                    RunManifestPosixStoreErrorV1::
                        kPublishConflict;
                return result;
            }
            result.disposition =
                adopted
                    ? RunManifestPosixDispositionV1::
                          kAdoptedCompleteTemporary
                    : RunManifestPosixDispositionV1::
                          kPublishedNew;
        }

        result.error = SyncActualDirectory(
            directory.get(),
            directory_status);
        if (result.error !=
            RunManifestPosixStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                result.error ==
                        RunManifestPosixStoreErrorV1::
                            kSyncFailure
                    ? "actual run manifest parent directory did not sync"
                    : "actual run manifest parent directory changed");
            return result;
        }
        result.directory_synced = true;

        LoadedCandidate final_readback{};
        result.error = LoadExactCandidate(
            directory.get(),
            directory_status,
            result.filename,
            desired,
            manifest.sha256(),
            &final_readback);
        if (result.error !=
            RunManifestPosixStoreErrorV1::kNone) {
            result.error =
                RunManifestPosixStoreErrorV1::
                    kReadbackFailure;
            SetDiagnostic(
                diagnostic,
                "published run manifest failed stable exact readback");
            return result;
        }
        struct stat final_status {};
        if (::fstat(
                final_readback.fd.get(),
                &final_status) != 0 ||
            !IsSafeFile(
                final_status, directory_status) ||
            !NameMatchesDescriptor(
                directory.get(),
                result.filename.c_str(),
                final_readback.fd.get(),
                &final_status) ||
            !DirectoryMatches(
                directory.get(),
                directory_status)) {
            result.error =
                RunManifestPosixStoreErrorV1::
                    kReadbackFailure;
            return result;
        }
        if (retained_candidate.fd.get() >= 0) {
            struct stat retained_status {};
            if (::fstat(
                    retained_candidate.fd.get(),
                    &retained_status) != 0 ||
                !SameInode(
                    retained_status,
                    final_status)) {
                result.error =
                    RunManifestPosixStoreErrorV1::
                        kReadbackFailure;
                return result;
            }
        }
        result.error = RequireSoleCandidate(
            directory.get(),
            directory_status,
            result.filename,
            temporary,
            true);
        if (result.error !=
            RunManifestPosixStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "published run manifest no longer has a sole final candidate");
            return result;
        }

        ScopedFd receipt_directory(
            OpenAtNoIntr(
                directory.get(),
                ".",
                O_RDONLY | O_DIRECTORY |
                    O_NOFOLLOW | O_NONBLOCK |
                    O_CLOEXEC));
        struct stat receipt_directory_status {};
        if (receipt_directory.get() < 0 ||
            ::fstat(
                receipt_directory.get(),
                &receipt_directory_status) != 0 ||
            !IsSafeDirectory(
                receipt_directory_status) ||
            !SameInode(
                directory_status,
                receipt_directory_status) ||
            !UnlockNoIntr(directory.get())) {
            result.error =
                RunManifestPosixStoreErrorV1::
                    kUnsafeDirectory;
            return result;
        }

        result.receipt =
            RunManifestPosixPublishAccessV1::
                CreateReceipt(
                    manifest.model().run_id,
                    manifest.sha256(),
                    desired.size(),
                    result.filename,
                    receipt_directory.Release(),
                    final_readback.fd.Release(),
                    directory_status,
                    final_status);
        if (result.receipt == nullptr ||
            !result.receipt->Validate()) {
            result.receipt.reset();
            result.error =
                RunManifestPosixStoreErrorV1::
                    kReadbackFailure;
            return result;
        }
        result.error =
            RunManifestPosixStoreErrorV1::kNone;
        SetDiagnostic(diagnostic, "");
        return result;
    } catch (...) {
        result.error =
            RunManifestPosixStoreErrorV1::
                kAllocationFailure;
        SetDiagnostic(
            diagnostic,
            "run manifest publication allocation failed");
        return result;
    }
}

}  // namespace l2flow::ingress
