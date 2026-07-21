#include "l2flow/ingress/finalization_archive_posix_store.h"

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_namespace.h"

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
#include <vector>

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
    explicit ScopedDir(DIR* value = nullptr) noexcept
        : value_(value) {}
    ~ScopedDir() {
        if (value_ != nullptr) {
            static_cast<void>(::closedir(value_));
        }
    }
    ScopedDir(const ScopedDir&) = delete;
    ScopedDir& operator=(const ScopedDir&) = delete;

    [[nodiscard]] DIR* get() const noexcept {
        return value_;
    }

private:
    DIR* value_ = nullptr;
};

class ScopedFlock final {
public:
    explicit ScopedFlock(int fd) noexcept : fd_(fd) {}
    ~ScopedFlock() {
        if (locked_) {
            for (;;) {
                if (::flock(fd_, LOCK_UN) == 0 ||
                    errno != EINTR) {
                    break;
                }
            }
        }
    }
    ScopedFlock(const ScopedFlock&) = delete;
    ScopedFlock& operator=(const ScopedFlock&) = delete;

    [[nodiscard]] bool Lock() noexcept {
        for (;;) {
            if (::flock(fd_, LOCK_EX) == 0) {
                locked_ = true;
                return true;
            }
            if (errno != EINTR) {
                return false;
            }
        }
    }

private:
    int fd_ = -1;
    bool locked_ = false;
};

struct ExpectedFile final {
    bool evidence_parent = false;
    std::string name;
    std::span<const std::byte> bytes;
    RawV1Digest sha256{};
};

struct LoadedFile final {
    ScopedFd descriptor;
    bool evidence_parent = false;
    std::string name;
    RawV1Digest sha256{};
    std::uint64_t byte_count = 0U;
    std::uint64_t device = 0U;
    std::uint64_t inode = 0U;
};

struct LoadedTree final {
    ScopedFd archive_directory;
    ScopedFd evidence_directory;
    struct stat archive_status {};
    struct stat evidence_status {};
    std::vector<LoadedFile> files;
};

struct CandidateInventory final {
    bool final_present = false;
    bool temporary_present = false;
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

[[nodiscard]] int OpenAtNoIntr(
    int directory_fd,
    const char* name,
    int flags,
    mode_t mode = 0U) noexcept {
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

[[nodiscard]] int DuplicateFd(int fd) noexcept {
    for (;;) {
        const int result =
            ::fcntl(fd, F_DUPFD_CLOEXEC, 0);
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

[[nodiscard]] bool UnlinkAtNoIntr(
    int directory_fd,
    const char* name,
    int flags = 0) noexcept {
    for (;;) {
        if (::unlinkat(directory_fd, name, flags) ==
            0) {
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
    const struct stat& parent) noexcept {
    return S_ISREG(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           (status.st_mode & 07777U) == 0600U &&
           status.st_nlink == static_cast<nlink_t>(1) &&
           status.st_size >= 0 &&
           status.st_dev == parent.st_dev;
}

[[nodiscard]] bool NameMatchesDescriptor(
    int directory_fd,
    const char* name,
    int descriptor,
    const struct stat* expected = nullptr) noexcept {
    struct stat opened {};
    struct stat named {};
    return descriptor >= 0 &&
           ::fstat(descriptor, &opened) == 0 &&
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
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            return false;
        }
        completed +=
            static_cast<std::size_t>(result);
    }
    return true;
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
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            return false;
        }
        completed +=
            static_cast<std::size_t>(result);
    }
    return true;
}

[[nodiscard]] bool ExactFileBytes(
    int descriptor,
    std::span<const std::byte> expected,
    const RawV1Digest& expected_sha256) {
    std::vector<std::byte> bytes(expected.size());
    struct stat before {};
    struct stat after {};
    if (::fstat(descriptor, &before) != 0 ||
        before.st_size !=
            static_cast<off_t>(expected.size()) ||
        !PreadAll(descriptor, bytes) ||
        ::fstat(descriptor, &after) != 0 ||
        !SameInode(before, after) ||
        before.st_size != after.st_size ||
        bytes.size() != expected.size() ||
        !std::equal(
            bytes.begin(),
            bytes.end(),
            expected.begin(),
            expected.end()) ||
        l2flow::common::ComputeSha256(
            std::span<const std::byte>(bytes)) !=
            expected_sha256) {
        return false;
    }
    return true;
}

[[nodiscard]] bool IsLowerHex(
    std::string_view value) noexcept {
    return std::all_of(
        value.begin(),
        value.end(),
        [](char character) noexcept {
            return (character >= '0' &&
                    character <= '9') ||
                   (character >= 'a' &&
                    character <= 'f');
        });
}

[[nodiscard]] bool IsFinalArchiveName(
    std::string_view name) noexcept {
    constexpr std::string_view prefix =
        "finalization-";
    constexpr std::size_t expected_size =
        prefix.size() + 32U + 1U + 32U;
    return name.size() == expected_size &&
           name.starts_with(prefix) &&
           name[prefix.size() + 32U] == '-' &&
           IsLowerHex(
               name.substr(prefix.size(), 32U)) &&
           IsLowerHex(
               name.substr(
                   prefix.size() + 33U, 32U));
}

[[nodiscard]] bool IsTemporaryArchiveName(
    std::string_view name) noexcept {
    constexpr std::string_view suffix =
        ".finalization-archive-v1.tmp";
    if (name.size() <= 1U + suffix.size() ||
        name.front() != '.' ||
        !name.ends_with(suffix)) {
        return false;
    }
    return IsFinalArchiveName(
        name.substr(
            1U,
            name.size() - 1U - suffix.size()));
}

[[nodiscard]] bool StartsLikeArchive(
    std::string_view name) noexcept {
    return name.starts_with("finalization-") ||
           name.starts_with(".finalization-");
}

[[nodiscard]] FinalizationArchivePosixStoreErrorV1
OpenRetainedAuditDirectory(
    int supplied,
    ScopedFd* output,
    struct stat* output_status) noexcept {
    if (supplied < 0 || output == nullptr ||
        output_status == nullptr) {
        return FinalizationArchivePosixStoreErrorV1::
            kInvalidArgument;
    }
    struct stat supplied_status {};
    if (::fstat(supplied, &supplied_status) != 0 ||
        !IsSafeDirectory(supplied_status)) {
        return FinalizationArchivePosixStoreErrorV1::
            kUnsafeAuditDirectory;
    }
    ScopedFd retained(
        OpenAtNoIntr(
            supplied,
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC | O_NOATIME));
    struct stat retained_status {};
    if (retained.get() < 0 ||
        ::fstat(
            retained.get(), &retained_status) != 0 ||
        !IsSafeDirectory(retained_status) ||
        !SameInode(
            supplied_status, retained_status)) {
        return FinalizationArchivePosixStoreErrorV1::
            kUnsafeAuditDirectory;
    }
    *output_status = retained_status;
    *output = std::move(retained);
    return FinalizationArchivePosixStoreErrorV1::kNone;
}

[[nodiscard]] FinalizationArchivePosixStoreErrorV1
InventoryCandidates(
    int audit_fd,
    const struct stat& audit_status,
    std::string_view expected_final,
    std::string_view expected_temporary,
    CandidateInventory* output) {
    if (output == nullptr) {
        return FinalizationArchivePosixStoreErrorV1::
            kInvalidArgument;
    }
    ScopedFd scan(
        OpenAtNoIntr(
            audit_fd,
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC));
    struct stat scan_status {};
    if (scan.get() < 0 ||
        ::fstat(scan.get(), &scan_status) != 0 ||
        !SameInode(scan_status, audit_status)) {
        return FinalizationArchivePosixStoreErrorV1::
            kUnsafeAuditDirectory;
    }
    DIR* native = ::fdopendir(scan.Release());
    if (native == nullptr) {
        return FinalizationArchivePosixStoreErrorV1::
            kUnsafeAuditDirectory;
    }
    ScopedDir directory(native);
    CandidateInventory inventory{};
    for (;;) {
        errno = 0;
        dirent* entry = ::readdir(directory.get());
        if (entry == nullptr) {
            if (errno != 0) {
                return FinalizationArchivePosixStoreErrorV1::
                    kUnsafeAuditDirectory;
            }
            break;
        }
        const std::string_view name(entry->d_name);
        if (name == "." || name == "..") {
            continue;
        }
        const bool final_name =
            IsFinalArchiveName(name);
        const bool temporary_name =
            IsTemporaryArchiveName(name);
        if (StartsLikeArchive(name) &&
            !final_name && !temporary_name) {
            return FinalizationArchivePosixStoreErrorV1::
                kMalformedCandidateName;
        }
        if (name == expected_final) {
            inventory.final_present = true;
        } else if (name == expected_temporary) {
            inventory.temporary_present = true;
        }
    }
    struct stat after {};
    if (::fstat(audit_fd, &after) != 0 ||
        !SameInode(after, audit_status) ||
        !IsSafeDirectory(after)) {
        return FinalizationArchivePosixStoreErrorV1::
            kUnsafeAuditDirectory;
    }
    *output = inventory;
    return FinalizationArchivePosixStoreErrorV1::kNone;
}

[[nodiscard]] std::string_view ArtifactBasename(
    const FinalizationArchiveArtifactV1& artifact)
    noexcept {
    const std::size_t slash =
        artifact.archive_path.rfind('/');
    return slash == std::string::npos
               ? std::string_view(artifact.archive_path)
               : std::string_view(artifact.archive_path)
                     .substr(slash + 1U);
}

[[nodiscard]] std::vector<ExpectedFile>
BuildExpectedFiles(
    const BuiltFinalizationArchiveV1& archive) {
    std::vector<ExpectedFile> files;
    files.reserve(archive.artifacts().size() + 3U);
    files.push_back(ExpectedFile{
        .evidence_parent = false,
        .name = std::string(
            kFinalizationArchiveV1StateHeaderFilename),
        .bytes = archive.state_header_bytes(),
        .sha256 =
            archive.model()
                .immutable_state_header_sha256});
    files.push_back(ExpectedFile{
        .evidence_parent = false,
        .name = std::string(
            kFinalizationArchiveV1AllDoneSlotFilename),
        .bytes = archive.all_done_slot_bytes(),
        .sha256 =
            archive.model().all_done_slot_sha256});
    files.push_back(ExpectedFile{
        .evidence_parent = false,
        .name = std::string(
            kFinalizationArchiveV1ManifestFilename),
        .bytes = std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(
                archive.canonical_jcs().data()),
            archive.canonical_jcs().size()),
        .sha256 = archive.manifest_sha256()});
    for (const auto& artifact : archive.artifacts()) {
        files.push_back(ExpectedFile{
            .evidence_parent = true,
            .name = std::string(
                ArtifactBasename(artifact.model())),
            .bytes = std::span<const std::byte>(
                reinterpret_cast<const std::byte*>(
                    artifact.exact_bytes().data()),
                artifact.exact_bytes().size()),
            .sha256 = artifact.model().sha256});
    }
    return files;
}

[[nodiscard]] bool NameInExpected(
    const std::vector<ExpectedFile>& expected,
    bool evidence_parent,
    std::string_view name,
    const ExpectedFile** output = nullptr) noexcept {
    const auto iterator = std::find_if(
        expected.begin(),
        expected.end(),
        [&](const ExpectedFile& value) noexcept {
            return value.evidence_parent ==
                       evidence_parent &&
                   value.name == name;
        });
    if (iterator == expected.end()) {
        return false;
    }
    if (output != nullptr) {
        *output = &*iterator;
    }
    return true;
}

[[nodiscard]] FinalizationArchivePosixStoreErrorV1
InventoryExactDirectory(
    int directory_fd,
    const struct stat& directory_status,
    const std::vector<ExpectedFile>& expected,
    bool evidence_directory) {
    ScopedFd scan(
        OpenAtNoIntr(
            directory_fd,
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC));
    struct stat scan_status {};
    if (scan.get() < 0 ||
        ::fstat(scan.get(), &scan_status) != 0 ||
        !SameInode(scan_status, directory_status)) {
        return FinalizationArchivePosixStoreErrorV1::
            kUnsafeCandidate;
    }
    DIR* native = ::fdopendir(scan.Release());
    if (native == nullptr) {
        return FinalizationArchivePosixStoreErrorV1::
            kUnsafeCandidate;
    }
    ScopedDir entries(native);
    std::size_t observed = 0U;
    for (;;) {
        errno = 0;
        dirent* entry = ::readdir(entries.get());
        if (entry == nullptr) {
            if (errno != 0) {
                return FinalizationArchivePosixStoreErrorV1::
                    kUnsafeCandidate;
            }
            break;
        }
        const std::string_view name(entry->d_name);
        if (name == "." || name == "..") {
            continue;
        }
        if (!evidence_directory &&
            name ==
                kFinalizationArchiveV1EvidenceDirectory) {
            ++observed;
            continue;
        }
        if (!NameInExpected(
                expected,
                evidence_directory,
                name)) {
            return FinalizationArchivePosixStoreErrorV1::
                kCandidateConflict;
        }
        ++observed;
    }
    const std::size_t expected_count =
        static_cast<std::size_t>(std::count_if(
            expected.begin(),
            expected.end(),
            [evidence_directory](
                const ExpectedFile& value) noexcept {
                return value.evidence_parent ==
                       evidence_directory;
            })) +
        (evidence_directory ? 0U : 1U);
    if (observed != expected_count) {
        return FinalizationArchivePosixStoreErrorV1::
            kCandidateConflict;
    }
    struct stat after {};
    if (::fstat(directory_fd, &after) != 0 ||
        !SameInode(after, directory_status) ||
        !IsSafeDirectory(after)) {
        return FinalizationArchivePosixStoreErrorV1::
            kUnsafeCandidate;
    }
    return FinalizationArchivePosixStoreErrorV1::kNone;
}

[[nodiscard]] FinalizationArchivePosixStoreErrorV1
LoadExactTree(
    int audit_fd,
    const struct stat& audit_status,
    const std::string& tree_name,
    const std::vector<ExpectedFile>& expected,
    bool sync_tree,
    LoadedTree* output) {
    if (output == nullptr) {
        return FinalizationArchivePosixStoreErrorV1::
            kInvalidArgument;
    }
    LoadedTree tree{};
    tree.archive_directory.Reset(
        OpenAtNoIntr(
            audit_fd,
            tree_name.c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC | O_NOATIME));
    if (tree.archive_directory.get() < 0 ||
        ::fstat(
            tree.archive_directory.get(),
            &tree.archive_status) != 0 ||
        !IsSafeDirectory(tree.archive_status) ||
        tree.archive_status.st_dev !=
            audit_status.st_dev ||
        !NameMatchesDescriptor(
            audit_fd,
            tree_name.c_str(),
            tree.archive_directory.get(),
            &tree.archive_status)) {
        return FinalizationArchivePosixStoreErrorV1::
            kUnsafeCandidate;
    }
    tree.evidence_directory.Reset(
        OpenAtNoIntr(
            tree.archive_directory.get(),
            std::string(
                kFinalizationArchiveV1EvidenceDirectory)
                .c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC | O_NOATIME));
    if (tree.evidence_directory.get() < 0 ||
        ::fstat(
            tree.evidence_directory.get(),
            &tree.evidence_status) != 0 ||
        !IsSafeDirectory(tree.evidence_status) ||
        tree.evidence_status.st_dev !=
            tree.archive_status.st_dev ||
        !NameMatchesDescriptor(
            tree.archive_directory.get(),
            std::string(
                kFinalizationArchiveV1EvidenceDirectory)
                .c_str(),
            tree.evidence_directory.get(),
            &tree.evidence_status)) {
        return FinalizationArchivePosixStoreErrorV1::
            kUnsafeCandidate;
    }

    FinalizationArchivePosixStoreErrorV1 error =
        InventoryExactDirectory(
            tree.archive_directory.get(),
            tree.archive_status,
            expected,
            false);
    if (error !=
        FinalizationArchivePosixStoreErrorV1::kNone) {
        return error;
    }
    error = InventoryExactDirectory(
        tree.evidence_directory.get(),
        tree.evidence_status,
        expected,
        true);
    if (error !=
        FinalizationArchivePosixStoreErrorV1::kNone) {
        return error;
    }

    tree.files.reserve(expected.size());
    for (const ExpectedFile& file : expected) {
        const int parent =
            file.evidence_parent
                ? tree.evidence_directory.get()
                : tree.archive_directory.get();
        const struct stat& parent_status =
            file.evidence_parent
                ? tree.evidence_status
                : tree.archive_status;
        LoadedFile loaded{};
        loaded.evidence_parent =
            file.evidence_parent;
        loaded.name = file.name;
        loaded.sha256 = file.sha256;
        loaded.byte_count = file.bytes.size();
        loaded.descriptor.Reset(
            OpenAtNoIntr(
                parent,
                file.name.c_str(),
                O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
                    O_CLOEXEC | O_NOATIME));
        struct stat status {};
        if (loaded.descriptor.get() < 0 ||
            ::fstat(
                loaded.descriptor.get(),
                &status) != 0 ||
            !IsSafeFile(status, parent_status) ||
            status.st_size !=
                static_cast<off_t>(
                    file.bytes.size()) ||
            !NameMatchesDescriptor(
                parent,
                file.name.c_str(),
                loaded.descriptor.get(),
                &status)) {
            return FinalizationArchivePosixStoreErrorV1::
                kUnsafeCandidate;
        }
        if (sync_tree &&
            !FsyncNoIntr(loaded.descriptor.get())) {
            return FinalizationArchivePosixStoreErrorV1::
                kSyncFailure;
        }
        if (!ExactFileBytes(
                loaded.descriptor.get(),
                file.bytes,
                file.sha256) ||
            !NameMatchesDescriptor(
                parent,
                file.name.c_str(),
                loaded.descriptor.get(),
                &status)) {
            return FinalizationArchivePosixStoreErrorV1::
                kReadbackFailure;
        }
        loaded.device =
            static_cast<std::uint64_t>(
                status.st_dev);
        loaded.inode =
            static_cast<std::uint64_t>(
                status.st_ino);
        tree.files.push_back(std::move(loaded));
    }
    if (sync_tree &&
        (!FsyncNoIntr(tree.evidence_directory.get()) ||
         !FsyncNoIntr(tree.archive_directory.get()))) {
        return FinalizationArchivePosixStoreErrorV1::
            kSyncFailure;
    }
    if (!NameMatchesDescriptor(
            audit_fd,
            tree_name.c_str(),
            tree.archive_directory.get(),
            &tree.archive_status) ||
        !NameMatchesDescriptor(
            tree.archive_directory.get(),
            std::string(
                kFinalizationArchiveV1EvidenceDirectory)
                .c_str(),
            tree.evidence_directory.get(),
            &tree.evidence_status)) {
        return FinalizationArchivePosixStoreErrorV1::
            kReadbackFailure;
    }
    *output = std::move(tree);
    return FinalizationArchivePosixStoreErrorV1::kNone;
}

[[nodiscard]] FinalizationArchivePosixStoreErrorV1
CreateFile(
    int parent_fd,
    const struct stat& parent_status,
    const ExpectedFile& expected) noexcept {
    ScopedFd file(
        OpenAtNoIntr(
            parent_fd,
            expected.name.c_str(),
            O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC,
            0600U));
    if (file.get() < 0) {
        return FinalizationArchivePosixStoreErrorV1::
            kFileCreate;
    }
    if (::fchmod(file.get(), 0600U) != 0) {
        return FinalizationArchivePosixStoreErrorV1::
            kFileCreate;
    }
    struct stat status {};
    if (::fstat(file.get(), &status) != 0 ||
        !IsSafeFile(status, parent_status) ||
        status.st_size != 0 ||
        !NameMatchesDescriptor(
            parent_fd,
            expected.name.c_str(),
            file.get(),
            &status)) {
        return FinalizationArchivePosixStoreErrorV1::
            kFileCreate;
    }
    if (!PwriteAll(file.get(), expected.bytes)) {
        return FinalizationArchivePosixStoreErrorV1::
            kWriteFailure;
    }
    if (!FsyncNoIntr(file.get())) {
        return FinalizationArchivePosixStoreErrorV1::
            kSyncFailure;
    }
    if (!ExactFileBytes(
            file.get(),
            expected.bytes,
            expected.sha256) ||
        !NameMatchesDescriptor(
            parent_fd,
            expected.name.c_str(),
            file.get())) {
        return FinalizationArchivePosixStoreErrorV1::
            kReadbackFailure;
    }
    return FinalizationArchivePosixStoreErrorV1::kNone;
}

[[nodiscard]] FinalizationArchivePosixStoreErrorV1
CreateTemporaryTree(
    int audit_fd,
    const struct stat& audit_status,
    const std::string& temporary_name,
    const std::vector<ExpectedFile>& expected) noexcept {
    if (::mkdirat(
            audit_fd,
            temporary_name.c_str(),
            0700U) != 0) {
        return FinalizationArchivePosixStoreErrorV1::
            kTemporaryCreate;
    }
    ScopedFd archive_directory(
        OpenAtNoIntr(
            audit_fd,
            temporary_name.c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC | O_NOATIME));
    struct stat archive_status {};
    if (archive_directory.get() < 0 ||
        ::fchmod(archive_directory.get(), 0700U) != 0 ||
        ::fstat(
            archive_directory.get(),
            &archive_status) != 0 ||
        !IsSafeDirectory(archive_status) ||
        archive_status.st_dev != audit_status.st_dev ||
        !NameMatchesDescriptor(
            audit_fd,
            temporary_name.c_str(),
            archive_directory.get(),
            &archive_status)) {
        return FinalizationArchivePosixStoreErrorV1::
            kTemporaryCreate;
    }
    const std::string evidence_name(
        kFinalizationArchiveV1EvidenceDirectory);
    if (::mkdirat(
            archive_directory.get(),
            evidence_name.c_str(),
            0700U) != 0) {
        return FinalizationArchivePosixStoreErrorV1::
            kDirectoryCreate;
    }
    ScopedFd evidence_directory(
        OpenAtNoIntr(
            archive_directory.get(),
            evidence_name.c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC | O_NOATIME));
    struct stat evidence_status {};
    if (evidence_directory.get() < 0 ||
        ::fchmod(evidence_directory.get(), 0700U) != 0 ||
        ::fstat(
            evidence_directory.get(),
            &evidence_status) != 0 ||
        !IsSafeDirectory(evidence_status) ||
        evidence_status.st_dev !=
            archive_status.st_dev ||
        !NameMatchesDescriptor(
            archive_directory.get(),
            evidence_name.c_str(),
            evidence_directory.get(),
            &evidence_status)) {
        return FinalizationArchivePosixStoreErrorV1::
            kDirectoryCreate;
    }

    // State first, evidence second, manifest last.  archive.json therefore
    // never exists in a temporary tree whose payload set was not attempted.
    for (const ExpectedFile& file : expected) {
        if (file.name ==
            kFinalizationArchiveV1ManifestFilename) {
            continue;
        }
        const int parent =
            file.evidence_parent
                ? evidence_directory.get()
                : archive_directory.get();
        const struct stat& parent_status =
            file.evidence_parent
                ? evidence_status
                : archive_status;
        const auto error =
            CreateFile(parent, parent_status, file);
        if (error !=
            FinalizationArchivePosixStoreErrorV1::kNone) {
            return error;
        }
    }
    if (!FsyncNoIntr(evidence_directory.get()) ||
        !FsyncNoIntr(archive_directory.get())) {
        return FinalizationArchivePosixStoreErrorV1::
            kSyncFailure;
    }
    const auto manifest = std::find_if(
        expected.begin(),
        expected.end(),
        [](const ExpectedFile& file) noexcept {
            return !file.evidence_parent &&
                   file.name ==
                       kFinalizationArchiveV1ManifestFilename;
        });
    if (manifest == expected.end()) {
        return FinalizationArchivePosixStoreErrorV1::
            kInvalidArgument;
    }
    const auto manifest_error =
        CreateFile(
            archive_directory.get(),
            archive_status,
            *manifest);
    if (manifest_error !=
        FinalizationArchivePosixStoreErrorV1::kNone) {
        return manifest_error;
    }
    if (!FsyncNoIntr(archive_directory.get())) {
        return FinalizationArchivePosixStoreErrorV1::
            kSyncFailure;
    }
    return FinalizationArchivePosixStoreErrorV1::kNone;
}

[[nodiscard]] FinalizationArchivePosixStoreErrorV1
ScanRecognizedPartialDirectory(
    int directory_fd,
    const struct stat& directory_status,
    const std::vector<ExpectedFile>& expected,
    bool evidence,
    std::vector<std::string>* observed,
    bool* incomplete) {
    if (observed == nullptr || incomplete == nullptr) {
        return FinalizationArchivePosixStoreErrorV1::
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
        !SameInode(scan_status, directory_status)) {
        return FinalizationArchivePosixStoreErrorV1::
            kUnsafeCandidate;
    }
    DIR* native = ::fdopendir(scan.Release());
    if (native == nullptr) {
        return FinalizationArchivePosixStoreErrorV1::
            kUnsafeCandidate;
    }
    ScopedDir entries(native);
    for (;;) {
        errno = 0;
        dirent* entry = ::readdir(entries.get());
        if (entry == nullptr) {
            if (errno != 0) {
                return FinalizationArchivePosixStoreErrorV1::
                    kUnsafeCandidate;
            }
            break;
        }
        const std::string_view name(entry->d_name);
        if (name == "." || name == "..") {
            continue;
        }
        if (!evidence &&
            name ==
                kFinalizationArchiveV1EvidenceDirectory) {
            continue;
        }
        const ExpectedFile* expected_file = nullptr;
        if (!NameInExpected(
                expected,
                evidence,
                name,
                &expected_file) ||
            expected_file == nullptr) {
            return FinalizationArchivePosixStoreErrorV1::
                kCandidateConflict;
        }
        struct stat status {};
        if (::fstatat(
                directory_fd,
                entry->d_name,
                &status,
                AT_SYMLINK_NOFOLLOW) != 0 ||
            !IsSafeFile(status, directory_status) ||
            static_cast<std::uint64_t>(
                status.st_size) >
                expected_file->bytes.size()) {
            return FinalizationArchivePosixStoreErrorV1::
                kUnsafeCandidate;
        }
        if (static_cast<std::uint64_t>(
                status.st_size) <
            expected_file->bytes.size()) {
            *incomplete = true;
        } else {
            ScopedFd complete(
                OpenAtNoIntr(
                    directory_fd,
                    entry->d_name,
                    O_RDONLY | O_NOFOLLOW |
                        O_NONBLOCK | O_CLOEXEC |
                        O_NOATIME));
            if (complete.get() < 0 ||
                !NameMatchesDescriptor(
                    directory_fd,
                    entry->d_name,
                    complete.get(),
                    &status) ||
                !ExactFileBytes(
                    complete.get(),
                    expected_file->bytes,
                    expected_file->sha256)) {
                return FinalizationArchivePosixStoreErrorV1::
                    kCandidateConflict;
            }
        }
        observed->emplace_back(name);
        if (observed->size() > expected.size()) {
            return FinalizationArchivePosixStoreErrorV1::
                kCandidateConflict;
        }
    }
    return FinalizationArchivePosixStoreErrorV1::kNone;
}

[[nodiscard]] FinalizationArchivePosixStoreErrorV1
CleanupRecognizedPartialTree(
    int audit_fd,
    const struct stat& audit_status,
    const std::string& temporary_name,
    const std::vector<ExpectedFile>& expected) {
    ScopedFd root(
        OpenAtNoIntr(
            audit_fd,
            temporary_name.c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC | O_NOATIME));
    struct stat root_status {};
    if (root.get() < 0 ||
        ::fstat(root.get(), &root_status) != 0 ||
        !IsSafeDirectory(root_status) ||
        root_status.st_dev != audit_status.st_dev ||
        !NameMatchesDescriptor(
            audit_fd,
            temporary_name.c_str(),
            root.get(),
            &root_status)) {
        return FinalizationArchivePosixStoreErrorV1::
            kUnsafeCandidate;
    }

    std::vector<std::string> root_files;
    bool incomplete = false;
    auto error = ScanRecognizedPartialDirectory(
        root.get(),
        root_status,
        expected,
        false,
        &root_files,
        &incomplete);
    if (error !=
        FinalizationArchivePosixStoreErrorV1::kNone) {
        return error;
    }

    const std::string evidence_name(
        kFinalizationArchiveV1EvidenceDirectory);
    struct stat evidence_named {};
    const bool evidence_present =
        ::fstatat(
            root.get(),
            evidence_name.c_str(),
            &evidence_named,
            AT_SYMLINK_NOFOLLOW) == 0;
    if (!evidence_present && errno != ENOENT) {
        return FinalizationArchivePosixStoreErrorV1::
            kUnsafeCandidate;
    }
    if (!evidence_present) {
        incomplete = true;
    }
    ScopedFd evidence;
    struct stat evidence_status {};
    std::vector<std::string> evidence_files;
    if (evidence_present) {
        evidence.Reset(
            OpenAtNoIntr(
                root.get(),
                evidence_name.c_str(),
                O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                    O_NONBLOCK | O_CLOEXEC |
                    O_NOATIME));
        if (evidence.get() < 0 ||
            ::fstat(
                evidence.get(),
                &evidence_status) != 0 ||
            !IsSafeDirectory(evidence_status) ||
            evidence_status.st_dev !=
                root_status.st_dev ||
            !NameMatchesDescriptor(
                root.get(),
                evidence_name.c_str(),
                evidence.get(),
                &evidence_status)) {
            return FinalizationArchivePosixStoreErrorV1::
                kUnsafeCandidate;
        }
        error = ScanRecognizedPartialDirectory(
            evidence.get(),
            evidence_status,
            expected,
            true,
            &evidence_files,
            &incomplete);
        if (error !=
            FinalizationArchivePosixStoreErrorV1::kNone) {
            return error;
        }
        const std::size_t expected_evidence =
            static_cast<std::size_t>(std::count_if(
                expected.begin(),
                expected.end(),
                [](const ExpectedFile&
                       file) noexcept {
                    return file.evidence_parent;
                }));
        if (evidence_files.size() !=
            expected_evidence) {
            incomplete = true;
        }
    }
    const std::size_t expected_root =
        static_cast<std::size_t>(std::count_if(
            expected.begin(),
            expected.end(),
            [](const ExpectedFile& file) noexcept {
                return !file.evidence_parent;
            }));
    if (root_files.size() != expected_root) {
        incomplete = true;
    }
    if (!incomplete) {
        return FinalizationArchivePosixStoreErrorV1::
            kCandidateConflict;
    }
    if (evidence_present) {
        for (const std::string& name : evidence_files) {
            if (!UnlinkAtNoIntr(
                    evidence.get(), name.c_str())) {
                return FinalizationArchivePosixStoreErrorV1::
                    kCleanupFailure;
            }
        }
        if (!FsyncNoIntr(evidence.get())) {
            return FinalizationArchivePosixStoreErrorV1::
                kCleanupFailure;
        }
        evidence.Reset();
        if (!UnlinkAtNoIntr(
                root.get(),
                evidence_name.c_str(),
                AT_REMOVEDIR) ||
            !FsyncNoIntr(root.get())) {
            return FinalizationArchivePosixStoreErrorV1::
                kCleanupFailure;
        }
    }
    for (const std::string& name : root_files) {
        if (!UnlinkAtNoIntr(
                root.get(), name.c_str())) {
            return FinalizationArchivePosixStoreErrorV1::
                kCleanupFailure;
        }
    }
    if (!FsyncNoIntr(root.get())) {
        return FinalizationArchivePosixStoreErrorV1::
            kCleanupFailure;
    }
    root.Reset();
    if (!UnlinkAtNoIntr(
            audit_fd,
            temporary_name.c_str(),
            AT_REMOVEDIR) ||
        !FsyncNoIntr(audit_fd)) {
        return FinalizationArchivePosixStoreErrorV1::
            kCleanupFailure;
    }
    return FinalizationArchivePosixStoreErrorV1::kNone;
}

[[nodiscard]] FinalizationArchivePosixStoreErrorV1
RemoveExactTree(
    int audit_fd,
    const struct stat& audit_status,
    const std::string& temporary_name,
    const std::vector<ExpectedFile>& expected) {
    LoadedTree exact{};
    const auto load = LoadExactTree(
        audit_fd,
        audit_status,
        temporary_name,
        expected,
        true,
        &exact);
    if (load !=
        FinalizationArchivePosixStoreErrorV1::kNone) {
        return load;
    }
    for (const ExpectedFile& file : expected) {
        if (file.evidence_parent &&
            !UnlinkAtNoIntr(
                exact.evidence_directory.get(),
                file.name.c_str())) {
            return FinalizationArchivePosixStoreErrorV1::
                kCleanupFailure;
        }
    }
    if (!FsyncNoIntr(
            exact.evidence_directory.get())) {
        return FinalizationArchivePosixStoreErrorV1::
            kCleanupFailure;
    }
    exact.files.clear();
    exact.evidence_directory.Reset();
    const std::string evidence_name(
        kFinalizationArchiveV1EvidenceDirectory);
    if (!UnlinkAtNoIntr(
            exact.archive_directory.get(),
            evidence_name.c_str(),
            AT_REMOVEDIR) ||
        !FsyncNoIntr(
            exact.archive_directory.get())) {
        return FinalizationArchivePosixStoreErrorV1::
            kCleanupFailure;
    }
    for (const ExpectedFile& file : expected) {
        if (!file.evidence_parent &&
            !UnlinkAtNoIntr(
                exact.archive_directory.get(),
                file.name.c_str())) {
            return FinalizationArchivePosixStoreErrorV1::
                kCleanupFailure;
        }
    }
    if (!FsyncNoIntr(
            exact.archive_directory.get())) {
        return FinalizationArchivePosixStoreErrorV1::
            kCleanupFailure;
    }
    exact.archive_directory.Reset();
    if (!UnlinkAtNoIntr(
            audit_fd,
            temporary_name.c_str(),
            AT_REMOVEDIR) ||
        !FsyncNoIntr(audit_fd)) {
        return FinalizationArchivePosixStoreErrorV1::
            kCleanupFailure;
    }
    return FinalizationArchivePosixStoreErrorV1::kNone;
}

[[nodiscard]] bool ReadExactString(
    int fd,
    std::size_t size,
    std::string* output) {
    if (output == nullptr) {
        return false;
    }
    std::string value(size, '\0');
    if (!PreadAll(
            fd,
            std::span<std::byte>(
                reinterpret_cast<std::byte*>(
                    value.data()),
                value.size()))) {
        return false;
    }
    output->swap(value);
    return true;
}

[[nodiscard]] bool ReadBoundedArchiveFile(
    int parent_fd,
    const struct stat& parent_status,
    std::string_view name,
    std::size_t maximum_bytes,
    std::string* output) {
    if (parent_fd < 0 || name.empty() ||
        name.find('/') != std::string_view::npos ||
        maximum_bytes == 0U || output == nullptr) {
        return false;
    }
    const std::string owned_name(name);
    ScopedFd file(
        OpenAtNoIntr(
            parent_fd,
            owned_name.c_str(),
            O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME));
    struct stat before {};
    if (file.get() < 0 ||
        ::fstat(file.get(), &before) != 0 ||
        !IsSafeFile(before, parent_status) ||
        before.st_size <= 0 ||
        static_cast<std::uint64_t>(before.st_size) >
            static_cast<std::uint64_t>(
                maximum_bytes) ||
        !NameMatchesDescriptor(
            parent_fd,
            owned_name.c_str(),
            file.get(),
            &before)) {
        return false;
    }
    std::string bytes;
    if (!ReadExactString(
            file.get(),
            static_cast<std::size_t>(before.st_size),
            &bytes)) {
        return false;
    }
    struct stat after {};
    if (::fstat(file.get(), &after) != 0 ||
        !SameInode(before, after) ||
        before.st_size != after.st_size ||
        !NameMatchesDescriptor(
            parent_fd,
            owned_name.c_str(),
            file.get(),
            &after)) {
        return false;
    }
    output->swap(bytes);
    return true;
}

[[nodiscard]] bool InventoryReceiptDirectory(
    int directory_fd,
    bool evidence,
    const std::vector<
        PublishedFinalizationArchiveReceiptV1::
            RetainedFileV1>& files) {
    ScopedFd scan(
        OpenAtNoIntr(
            directory_fd,
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC));
    if (scan.get() < 0) {
        return false;
    }
    DIR* native = ::fdopendir(scan.Release());
    if (native == nullptr) {
        return false;
    }
    ScopedDir entries(native);
    std::size_t observed = 0U;
    for (;;) {
        errno = 0;
        dirent* entry = ::readdir(entries.get());
        if (entry == nullptr) {
            if (errno != 0) {
                return false;
            }
            break;
        }
        const std::string_view name(entry->d_name);
        if (name == "." || name == "..") {
            continue;
        }
        if (!evidence &&
            name ==
                kFinalizationArchiveV1EvidenceDirectory) {
            ++observed;
            continue;
        }
        const bool found = std::any_of(
            files.begin(),
            files.end(),
            [&](const auto& file) noexcept {
                return file.evidence_parent ==
                           evidence &&
                       file.name == name;
            });
        if (!found) {
            return false;
        }
        ++observed;
    }
    const std::size_t expected =
        static_cast<std::size_t>(std::count_if(
            files.begin(),
            files.end(),
            [evidence](const auto& file) noexcept {
                return file.evidence_parent ==
                       evidence;
            })) +
        (evidence ? 0U : 1U);
    return observed == expected;
}

}  // namespace

struct FinalizationArchivePosixPublishAccessV1 final {
    [[nodiscard]] static std::unique_ptr<
        PublishedFinalizationArchiveReceiptV1>
    MakeReceipt(
        const BuiltFinalizationArchiveV1& archive,
        int audit_fd,
        const struct stat& audit_status,
        LoadedTree* loaded) {
        if (loaded == nullptr) {
            return nullptr;
        }
        std::vector<
            PublishedFinalizationArchiveReceiptV1::
                RetainedFileV1>
            files;
        files.reserve(loaded->files.size());
        for (LoadedFile& file : loaded->files) {
            PublishedFinalizationArchiveReceiptV1::
                RetainedFileV1 retained{};
            retained.descriptor =
                file.descriptor.Release();
            retained.evidence_parent =
                file.evidence_parent;
            retained.name = std::move(file.name);
            retained.sha256 = file.sha256;
            retained.byte_count = file.byte_count;
            retained.device = file.device;
            retained.inode = file.inode;
            files.push_back(std::move(retained));
        }
        return std::unique_ptr<
            PublishedFinalizationArchiveReceiptV1>(
            new PublishedFinalizationArchiveReceiptV1(
                archive.model().reserve_state_uuid,
                archive.model().finalization_cycle_id,
                archive.manifest_sha256(),
                archive.model().state_sha256,
                std::string(archive.directory_name()),
                archive.artifacts().size(),
                DuplicateFd(audit_fd),
                loaded->archive_directory.Release(),
                loaded->evidence_directory.Release(),
                static_cast<std::uint64_t>(
                    audit_status.st_dev),
                static_cast<std::uint64_t>(
                    audit_status.st_ino),
                static_cast<std::uint64_t>(
                    loaded->archive_status.st_dev),
                static_cast<std::uint64_t>(
                    loaded->archive_status.st_ino),
                static_cast<std::uint64_t>(
                    loaded->evidence_status.st_dev),
                static_cast<std::uint64_t>(
                    loaded->evidence_status.st_ino),
                std::move(files)));
    }
};

PublishedFinalizationArchiveReceiptV1::RetainedFileV1::
    ~RetainedFileV1() {
    if (descriptor >= 0) {
        static_cast<void>(::close(descriptor));
    }
}

PublishedFinalizationArchiveReceiptV1::RetainedFileV1::
    RetainedFileV1(RetainedFileV1&& other) noexcept
    : descriptor(std::exchange(other.descriptor, -1)),
      evidence_parent(other.evidence_parent),
      name(std::move(other.name)),
      sha256(other.sha256),
      byte_count(other.byte_count),
      device(other.device),
      inode(other.inode) {}

PublishedFinalizationArchiveReceiptV1::RetainedFileV1&
PublishedFinalizationArchiveReceiptV1::RetainedFileV1::
operator=(RetainedFileV1&& other) noexcept {
    if (this != &other) {
        if (descriptor >= 0) {
            static_cast<void>(::close(descriptor));
        }
        descriptor =
            std::exchange(other.descriptor, -1);
        evidence_parent = other.evidence_parent;
        name = std::move(other.name);
        sha256 = other.sha256;
        byte_count = other.byte_count;
        device = other.device;
        inode = other.inode;
    }
    return *this;
}

PublishedFinalizationArchiveReceiptV1::
    PublishedFinalizationArchiveReceiptV1(
        RawV1Identity reserve_state_uuid,
        RawV1Identity finalization_cycle_id,
        RawV1Digest manifest_sha256,
        RawV1Digest state_sha256,
        std::string directory_name,
        std::size_t artifact_count,
        int audit_directory_fd,
        int archive_directory_fd,
        int evidence_directory_fd,
        std::uint64_t audit_device,
        std::uint64_t audit_inode,
        std::uint64_t archive_device,
        std::uint64_t archive_inode,
        std::uint64_t evidence_device,
        std::uint64_t evidence_inode,
        std::vector<RetainedFileV1> files) noexcept
    : reserve_state_uuid_(reserve_state_uuid),
      finalization_cycle_id_(finalization_cycle_id),
      manifest_sha256_(manifest_sha256),
      state_sha256_(state_sha256),
      directory_name_(std::move(directory_name)),
      artifact_count_(artifact_count),
      audit_directory_fd_(audit_directory_fd),
      archive_directory_fd_(archive_directory_fd),
      evidence_directory_fd_(evidence_directory_fd),
      audit_device_(audit_device),
      audit_inode_(audit_inode),
      archive_device_(archive_device),
      archive_inode_(archive_inode),
      evidence_device_(evidence_device),
      evidence_inode_(evidence_inode),
      files_(std::move(files)) {}

PublishedFinalizationArchiveReceiptV1::
    ~PublishedFinalizationArchiveReceiptV1() {
    if (evidence_directory_fd_ >= 0) {
        static_cast<void>(
            ::close(evidence_directory_fd_));
    }
    if (archive_directory_fd_ >= 0) {
        static_cast<void>(
            ::close(archive_directory_fd_));
    }
    if (audit_directory_fd_ >= 0) {
        static_cast<void>(
            ::close(audit_directory_fd_));
    }
}

bool PublishedFinalizationArchiveReceiptV1::Validate(
    std::string* diagnostic) const noexcept {
    try {
        struct stat audit {};
        struct stat archive {};
        struct stat evidence {};
        if (audit_directory_fd_ < 0 ||
            archive_directory_fd_ < 0 ||
            evidence_directory_fd_ < 0 ||
            ::fstat(
                audit_directory_fd_, &audit) != 0 ||
            ::fstat(
                archive_directory_fd_,
                &archive) != 0 ||
            ::fstat(
                evidence_directory_fd_,
                &evidence) != 0 ||
            !IsSafeDirectory(audit) ||
            !IsSafeDirectory(archive) ||
            !IsSafeDirectory(evidence) ||
            static_cast<std::uint64_t>(
                audit.st_dev) != audit_device_ ||
            static_cast<std::uint64_t>(
                audit.st_ino) != audit_inode_ ||
            static_cast<std::uint64_t>(
                archive.st_dev) != archive_device_ ||
            static_cast<std::uint64_t>(
                archive.st_ino) != archive_inode_ ||
            static_cast<std::uint64_t>(
                evidence.st_dev) != evidence_device_ ||
            static_cast<std::uint64_t>(
                evidence.st_ino) != evidence_inode_ ||
            archive.st_dev != audit.st_dev ||
            evidence.st_dev != archive.st_dev ||
            !NameMatchesDescriptor(
                audit_directory_fd_,
                directory_name_.c_str(),
                archive_directory_fd_,
                &archive) ||
            !NameMatchesDescriptor(
                archive_directory_fd_,
                std::string(
                    kFinalizationArchiveV1EvidenceDirectory)
                    .c_str(),
                evidence_directory_fd_,
                &evidence) ||
            files_.size() != artifact_count_ + 3U ||
            !InventoryReceiptDirectory(
                archive_directory_fd_,
                false,
                files_) ||
            !InventoryReceiptDirectory(
                evidence_directory_fd_,
                true,
                files_)) {
            SetDiagnostic(
                diagnostic,
                "archive receipt directory identity or inventory changed");
            return false;
        }

        const RetainedFileV1* manifest_file = nullptr;
        const RetainedFileV1* header_file = nullptr;
        const RetainedFileV1* slot_file = nullptr;
        for (const RetainedFileV1& file : files_) {
            const int parent =
                file.evidence_parent
                    ? evidence_directory_fd_
                    : archive_directory_fd_;
            const struct stat& parent_status =
                file.evidence_parent
                    ? evidence
                    : archive;
            struct stat status {};
            RawV1Digest digest{};
            if (file.descriptor < 0 ||
                ::fstat(file.descriptor, &status) != 0 ||
                !IsSafeFile(status, parent_status) ||
                status.st_size !=
                    static_cast<off_t>(
                        file.byte_count) ||
                static_cast<std::uint64_t>(
                    status.st_dev) != file.device ||
                static_cast<std::uint64_t>(
                    status.st_ino) != file.inode ||
                !NameMatchesDescriptor(
                    parent,
                    file.name.c_str(),
                    file.descriptor,
                    &status) ||
                !l2flow::common::
                    ComputeFileSha256ForOpenFd(
                        file.descriptor,
                        &digest,
                        nullptr,
                        file.byte_count) ||
                digest != file.sha256) {
                SetDiagnostic(
                    diagnostic,
                    "archive receipt file identity, size, or hash changed");
                return false;
            }
            if (!file.evidence_parent &&
                file.name ==
                    kFinalizationArchiveV1ManifestFilename) {
                manifest_file = &file;
            } else if (
                !file.evidence_parent &&
                file.name ==
                    kFinalizationArchiveV1StateHeaderFilename) {
                header_file = &file;
            } else if (
                !file.evidence_parent &&
                file.name ==
                    kFinalizationArchiveV1AllDoneSlotFilename) {
                slot_file = &file;
            }
        }
        if (manifest_file == nullptr ||
            header_file == nullptr ||
            slot_file == nullptr ||
            manifest_file->sha256 != manifest_sha256_ ||
            header_file->byte_count !=
                kReserveStateV1HeaderBytes ||
            slot_file->byte_count !=
                kReserveStateV1SlotBytes) {
            SetDiagnostic(
                diagnostic,
                "archive receipt is missing its manifest or state wires");
            return false;
        }

        std::string manifest_bytes;
        if (!ReadExactString(
                manifest_file->descriptor,
                static_cast<std::size_t>(
                    manifest_file->byte_count),
                &manifest_bytes)) {
            SetDiagnostic(
                diagnostic,
                "archive manifest readback failed");
            return false;
        }
        FinalizationArchiveV1 manifest{};
        if (ParseFinalizationArchiveV1Jcs(
                manifest_bytes,
                &manifest) !=
                FinalizationArchiveV1Error::kNone ||
            manifest.reserve_state_uuid !=
                reserve_state_uuid_ ||
            manifest.finalization_cycle_id !=
                finalization_cycle_id_ ||
            manifest.state_sha256 != state_sha256_ ||
            manifest.artifact_count != artifact_count_ ||
            manifest.immutable_state_header_sha256 !=
                header_file->sha256 ||
            manifest.all_done_slot_sha256 !=
                slot_file->sha256) {
            SetDiagnostic(
                diagnostic,
                "archive manifest no longer matches the receipt");
            return false;
        }
        for (const auto& artifact : manifest.artifacts) {
            const std::string_view name =
                ArtifactBasename(artifact);
            const bool found = std::any_of(
                files_.begin(),
                files_.end(),
                [&](const RetainedFileV1&
                        file) noexcept {
                    return file.evidence_parent &&
                           file.name == name &&
                           file.byte_count ==
                               artifact.byte_count &&
                           file.sha256 ==
                               artifact.sha256;
                });
            if (!found) {
                SetDiagnostic(
                    diagnostic,
                    "archive evidence no longer matches its manifest");
                return false;
            }
        }

        std::array<std::byte, kReserveStateV1HeaderBytes>
            header_bytes{};
        std::array<std::byte, kReserveStateV1SlotBytes>
            slot_bytes{};
        l2flow::common::Sha256Hasher hasher;
        RawV1Digest state_digest{};
        if (!PreadAll(
                header_file->descriptor,
                std::span<std::byte>(header_bytes)) ||
            !PreadAll(
                slot_file->descriptor,
                std::span<std::byte>(slot_bytes)) ||
            !hasher.Update(
                std::span<const std::byte>(
                    header_bytes)) ||
            !hasher.Update(
                std::span<const std::byte>(
                    slot_bytes)) ||
            !hasher.Finalize(&state_digest) ||
            state_digest != state_sha256_) {
            SetDiagnostic(
                diagnostic,
                "archive state hash chain changed");
            return false;
        }
        return true;
    } catch (...) {
        SetDiagnostic(
            diagnostic,
            "archive receipt validation allocation failed");
        return false;
    }
}

std::string_view
FinalizationArchivePosixStoreErrorV1Name(
    FinalizationArchivePosixStoreErrorV1 error) noexcept {
    switch (error) {
    case FinalizationArchivePosixStoreErrorV1::kNone:
        return "NONE";
    case FinalizationArchivePosixStoreErrorV1::
        kInvalidArgument:
        return "INVALID_ARGUMENT";
    case FinalizationArchivePosixStoreErrorV1::
        kUnsafeAuditDirectory:
        return "UNSAFE_AUDIT_DIRECTORY";
    case FinalizationArchivePosixStoreErrorV1::
        kMalformedCandidateName:
        return "MALFORMED_CANDIDATE_NAME";
    case FinalizationArchivePosixStoreErrorV1::
        kUnsafeCandidate:
        return "UNSAFE_CANDIDATE";
    case FinalizationArchivePosixStoreErrorV1::
        kCandidateConflict:
        return "CANDIDATE_CONFLICT";
    case FinalizationArchivePosixStoreErrorV1::
        kTemporaryCreate:
        return "TEMPORARY_CREATE";
    case FinalizationArchivePosixStoreErrorV1::
        kDirectoryCreate:
        return "DIRECTORY_CREATE";
    case FinalizationArchivePosixStoreErrorV1::
        kFileCreate:
        return "FILE_CREATE";
    case FinalizationArchivePosixStoreErrorV1::
        kWriteFailure:
        return "WRITE_FAILURE";
    case FinalizationArchivePosixStoreErrorV1::
        kSyncFailure:
        return "SYNC_FAILURE";
    case FinalizationArchivePosixStoreErrorV1::
        kPublishConflict:
        return "PUBLISH_CONFLICT";
    case FinalizationArchivePosixStoreErrorV1::
        kReadbackFailure:
        return "READBACK_FAILURE";
    case FinalizationArchivePosixStoreErrorV1::
        kCleanupFailure:
        return "CLEANUP_FAILURE";
    case FinalizationArchivePosixStoreErrorV1::
        kAllocationFailure:
        return "ALLOCATION_FAILURE";
    }
    return "UNKNOWN";
}

FinalizationArchivePosixLoadResultV1
LoadPublishedFinalizationArchiveCapabilityV1At(
    int retained_audit_directory_fd,
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& all_done_slot,
    std::string* diagnostic) noexcept {
    FinalizationArchivePosixLoadResultV1 result{};
    SetDiagnostic(diagnostic, {});
    try {
        ReserveStateV1HeaderWire header_wire{};
        ReserveStateV1SlotWire slot_wire{};
        if (EncodeReserveCoordinatorHeaderV1(
                header, &header_wire) !=
                ReserveStateV1Error::kNone ||
            EncodeReserveStateSlotV1(
                header, all_done_slot, &slot_wire) !=
                ReserveStateV1Error::kNone ||
            all_done_slot.coordinator_state !=
                ReserveCoordinatorPhaseV1::kConsumed ||
            all_done_slot.reserve_state_uuid !=
                header.reserve_state_uuid ||
            l2flow::common::IsZeroIdentity(
                header.reserve_state_uuid) ||
            l2flow::common::IsZeroIdentity(
                all_done_slot.finalization_cycle_id)) {
            result.error =
                FinalizationArchivePosixStoreErrorV1::
                    kInvalidArgument;
            SetDiagnostic(
                diagnostic,
                "archive restart load requires one valid matching CONSUMED all-DONE state");
            return result;
        }

        result.directory_name =
            "finalization-" +
            l2flow::common::Identity128Hex(
                header.reserve_state_uuid) +
            "-" +
            l2flow::common::Identity128Hex(
                all_done_slot.finalization_cycle_id);

        ScopedFd audit;
        struct stat audit_status {};
        result.error = OpenRetainedAuditDirectory(
            retained_audit_directory_fd,
            &audit,
            &audit_status);
        if (result.error !=
            FinalizationArchivePosixStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "reserve-audit descriptor is not a retained owner-only directory");
            return result;
        }
        ScopedFlock lock(audit.get());
        if (!lock.Lock()) {
            result.error =
                FinalizationArchivePosixStoreErrorV1::
                    kUnsafeAuditDirectory;
            SetDiagnostic(
                diagnostic,
                "cannot serialize finalization archive restart load");
            return result;
        }

        ScopedFd archive_directory(
            OpenAtNoIntr(
                audit.get(),
                result.directory_name.c_str(),
                O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                    O_NONBLOCK | O_CLOEXEC | O_NOATIME));
        struct stat archive_status {};
        if (archive_directory.get() < 0 ||
            ::fstat(
                archive_directory.get(),
                &archive_status) != 0 ||
            !IsSafeDirectory(archive_status) ||
            archive_status.st_dev !=
                audit_status.st_dev ||
            !NameMatchesDescriptor(
                audit.get(),
                result.directory_name.c_str(),
                archive_directory.get(),
                &archive_status)) {
            result.error =
                FinalizationArchivePosixStoreErrorV1::
                    kUnsafeCandidate;
            SetDiagnostic(
                diagnostic,
                "published finalization archive directory is missing or unsafe");
            return result;
        }
        const std::string evidence_name(
            kFinalizationArchiveV1EvidenceDirectory);
        ScopedFd evidence_directory(
            OpenAtNoIntr(
                archive_directory.get(),
                evidence_name.c_str(),
                O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                    O_NONBLOCK | O_CLOEXEC | O_NOATIME));
        struct stat evidence_status {};
        if (evidence_directory.get() < 0 ||
            ::fstat(
                evidence_directory.get(),
                &evidence_status) != 0 ||
            !IsSafeDirectory(evidence_status) ||
            evidence_status.st_dev !=
                archive_status.st_dev ||
            !NameMatchesDescriptor(
                archive_directory.get(),
                evidence_name.c_str(),
                evidence_directory.get(),
                &evidence_status)) {
            result.error =
                FinalizationArchivePosixStoreErrorV1::
                    kUnsafeCandidate;
            SetDiagnostic(
                diagnostic,
                "published finalization archive evidence directory is missing or unsafe");
            return result;
        }

        std::string manifest_bytes;
        if (!ReadBoundedArchiveFile(
                archive_directory.get(),
                archive_status,
                kFinalizationArchiveV1ManifestFilename,
                kFinalizationArchiveV1MaximumManifestBytes,
                &manifest_bytes)) {
            result.error =
                FinalizationArchivePosixStoreErrorV1::
                    kReadbackFailure;
            SetDiagnostic(
                diagnostic,
                "cannot read the bounded canonical archive manifest");
            return result;
        }
        FinalizationArchiveV1 parsed{};
        std::string canonical_manifest;
        if (ParseFinalizationArchiveV1Jcs(
                manifest_bytes, &parsed) !=
                FinalizationArchiveV1Error::kNone ||
            EncodeFinalizationArchiveV1Jcs(
                parsed, &canonical_manifest) !=
                FinalizationArchiveV1Error::kNone ||
            canonical_manifest != manifest_bytes ||
            parsed.reserve_state_uuid !=
                header.reserve_state_uuid ||
            parsed.finalization_cycle_id !=
                all_done_slot.finalization_cycle_id ||
            parsed.all_done_generation !=
                all_done_slot.generation) {
            result.error =
                FinalizationArchivePosixStoreErrorV1::
                    kReadbackFailure;
            SetDiagnostic(
                diagnostic,
                "archive manifest is noncanonical or does not bind the selected all-DONE state");
            return result;
        }

        std::vector<
            FinalizationArchiveArtifactInputV1>
            inputs;
        inputs.reserve(parsed.artifacts.size());
        for (const FinalizationArchiveArtifactV1&
                 artifact : parsed.artifacts) {
            if (artifact.byte_count == 0U ||
                artifact.byte_count >
                    static_cast<std::uint64_t>(
                        std::numeric_limits<
                            std::size_t>::max())) {
                result.error =
                    FinalizationArchivePosixStoreErrorV1::
                        kReadbackFailure;
                SetDiagnostic(
                    diagnostic,
                    "archive evidence size is not representable");
                return result;
            }
            std::string exact_bytes;
            if (!ReadBoundedArchiveFile(
                    evidence_directory.get(),
                    evidence_status,
                    ArtifactBasename(artifact),
                    static_cast<std::size_t>(
                        artifact.byte_count),
                    &exact_bytes) ||
                exact_bytes.size() !=
                    static_cast<std::size_t>(
                        artifact.byte_count) ||
                l2flow::common::ComputeSha256(
                    std::string_view(exact_bytes)) !=
                    artifact.sha256) {
                result.error =
                    FinalizationArchivePosixStoreErrorV1::
                        kReadbackFailure;
                SetDiagnostic(
                    diagnostic,
                    "archive evidence bytes do not match the canonical manifest");
                return result;
            }

            FinalizationArchiveArtifactInputV1 input{};
            input.artifact_type =
                artifact.artifact_type;
            input.exact_bytes = std::move(exact_bytes);
            switch (artifact.artifact_type) {
                case FinalizationArchiveArtifactTypeV1::
                    kFinalizationReport:
                case FinalizationArchiveArtifactTypeV1::
                    kPreexistingRecoveryReport: {
                    const auto slug =
                        CanonicalRawStreamSlugV1(
                            artifact
                                .namespace_identity
                                .source_stream_id);
                    if (!slug.has_value()) {
                        result.error =
                            FinalizationArchivePosixStoreErrorV1::
                                kReadbackFailure;
                        SetDiagnostic(
                            diagnostic,
                            "archive report namespace has no frozen Raw route");
                        return result;
                    }
                    input.source_stream_slug =
                        std::string(*slug);
                    break;
                }
                case FinalizationArchiveArtifactTypeV1::
                    kScaffoldingFinalizationReport:
                    break;
                case FinalizationArchiveArtifactTypeV1::
                    kRawManifest:
                case FinalizationArchiveArtifactTypeV1::
                    kSealedRawCertificate:
                case FinalizationArchiveArtifactTypeV1::
                    kEmptyAnchorTombstone:
                    input.source_locator =
                        artifact.source_locator;
                    break;
            }
            inputs.push_back(std::move(input));
        }

        std::unique_ptr<BuiltFinalizationArchiveV1>
            rebuilt;
        if (BuildFinalizationArchiveCapabilityV1(
                header,
                all_done_slot,
                inputs,
                &rebuilt) !=
                FinalizationArchiveV1Error::kNone ||
            rebuilt == nullptr ||
            rebuilt->model() != parsed ||
            rebuilt->canonical_jcs() !=
                manifest_bytes ||
            rebuilt->directory_name() !=
                result.directory_name) {
            result.error =
                FinalizationArchivePosixStoreErrorV1::
                    kReadbackFailure;
            SetDiagnostic(
                diagnostic,
                "published archive cannot be reconstructed through the causal archive validator");
            return result;
        }

        const std::vector<ExpectedFile> expected =
            BuildExpectedFiles(*rebuilt);
        LoadedTree verified{};
        result.error = LoadExactTree(
            audit.get(),
            audit_status,
            result.directory_name,
            expected,
            false,
            &verified);
        if (result.error !=
            FinalizationArchivePosixStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "published archive tree is not the exact reconstructed immutable tree");
            return result;
        }
        result.manifest_sha256 =
            rebuilt->manifest_sha256();
        result.state_sha256 =
            rebuilt->model().state_sha256;
        result.archive = std::move(rebuilt);
        result.error =
            FinalizationArchivePosixStoreErrorV1::kNone;
        return result;
    } catch (...) {
        result.archive.reset();
        result.error =
            FinalizationArchivePosixStoreErrorV1::
                kAllocationFailure;
        SetDiagnostic(
            diagnostic,
            "cannot allocate finalization archive restart capability");
        return result;
    }
}

FinalizationArchivePosixPublishResultV1
PublishFinalizationArchiveV1At(
    int retained_audit_directory_fd,
    const BuiltFinalizationArchiveV1& archive,
    std::string* diagnostic) noexcept {
    FinalizationArchivePosixPublishResultV1 result{};
    try {
        result.directory_name =
            std::string(archive.directory_name());
        result.manifest_sha256 =
            archive.manifest_sha256();
        result.state_sha256 =
            archive.model().state_sha256;

        ScopedFd audit;
        struct stat audit_status {};
        result.error = OpenRetainedAuditDirectory(
            retained_audit_directory_fd,
            &audit,
            &audit_status);
        if (result.error !=
            FinalizationArchivePosixStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "reserve-audit descriptor is not a retained owner-only directory");
            return result;
        }
        ScopedFlock lock(audit.get());
        if (!lock.Lock()) {
            result.error =
                FinalizationArchivePosixStoreErrorV1::
                    kUnsafeAuditDirectory;
            SetDiagnostic(
                diagnostic,
                "cannot serialize finalization archive publication");
            return result;
        }

        const std::vector<ExpectedFile> expected =
            BuildExpectedFiles(archive);
        CandidateInventory inventory{};
        result.error = InventoryCandidates(
            audit.get(),
            audit_status,
            archive.directory_name(),
            archive.temporary_directory_name(),
            &inventory);
        if (result.error !=
            FinalizationArchivePosixStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "finalization archive candidate inventory is malformed or unsafe");
            return result;
        }

        LoadedTree final_tree{};
        if (inventory.final_present) {
            result.error = LoadExactTree(
                audit.get(),
                audit_status,
                std::string(archive.directory_name()),
                expected,
                true,
                &final_tree);
            if (result.error !=
                FinalizationArchivePosixStoreErrorV1::
                    kNone) {
                SetDiagnostic(
                    diagnostic,
                    "existing finalization archive is not an exact durable tree");
                return result;
            }
            result.files_synced = true;
            result.subdirectories_synced = true;
            if (!FsyncNoIntr(audit.get())) {
                result.error =
                    FinalizationArchivePosixStoreErrorV1::
                        kSyncFailure;
                SetDiagnostic(
                    diagnostic,
                    "cannot synchronize reserve-audit after final archive revalidation");
                return result;
            }
            result.audit_parent_synced = true;
            if (inventory.temporary_present) {
                result.error = RemoveExactTree(
                    audit.get(),
                    audit_status,
                    std::string(
                        archive
                            .temporary_directory_name()),
                    expected);
                if (result.error !=
                    FinalizationArchivePosixStoreErrorV1::
                        kNone) {
                    SetDiagnostic(
                        diagnostic,
                        "final archive coexists with a non-identical or unremovable temporary tree");
                    return result;
                }
            }
            result.disposition =
                FinalizationArchivePosixDispositionV1::
                    kAcceptedExistingFinal;
        } else if (inventory.temporary_present) {
            LoadedTree temporary{};
            result.error = LoadExactTree(
                audit.get(),
                audit_status,
                std::string(
                    archive.temporary_directory_name()),
                expected,
                true,
                &temporary);
            if (result.error ==
                FinalizationArchivePosixStoreErrorV1::
                    kNone) {
                result.files_synced = true;
                result.subdirectories_synced = true;
                if (!RenameNoReplace(
                        audit.get(),
                        std::string(
                            archive
                                .temporary_directory_name())
                            .c_str(),
                        std::string(
                            archive.directory_name())
                            .c_str())) {
                    result.error =
                        FinalizationArchivePosixStoreErrorV1::
                            kPublishConflict;
                    SetDiagnostic(
                        diagnostic,
                        "cannot publish complete finalization archive temporary without replacement");
                    return result;
                }
                if (!FsyncNoIntr(audit.get())) {
                    result.error =
                        FinalizationArchivePosixStoreErrorV1::
                            kSyncFailure;
                    SetDiagnostic(
                        diagnostic,
                        "cannot synchronize reserve-audit after adopting archive temporary");
                    return result;
                }
                result.audit_parent_synced = true;
                if (!NameMatchesDescriptor(
                        audit.get(),
                        std::string(
                            archive.directory_name())
                            .c_str(),
                        temporary.archive_directory.get(),
                        &temporary.archive_status)) {
                    result.error =
                        FinalizationArchivePosixStoreErrorV1::
                            kReadbackFailure;
                    return result;
                }
                final_tree = std::move(temporary);
                result.disposition =
                    FinalizationArchivePosixDispositionV1::
                        kAdoptedCompleteTemporary;
            } else {
                if (result.error ==
                    FinalizationArchivePosixStoreErrorV1::
                        kSyncFailure) {
                    SetDiagnostic(
                        diagnostic,
                        "complete archive temporary could not be synchronized");
                    return result;
                }
                // A sole path-bound safe subset is the only partial tree
                // grammar this publisher is authorized to remove.
                result.error =
                    CleanupRecognizedPartialTree(
                        audit.get(),
                        audit_status,
                        std::string(
                            archive
                                .temporary_directory_name()),
                        expected);
                if (result.error !=
                    FinalizationArchivePosixStoreErrorV1::
                        kNone) {
                    SetDiagnostic(
                        diagnostic,
                        "temporary archive is neither complete nor a recognized safe subset");
                    return result;
                }
                result.error = CreateTemporaryTree(
                    audit.get(),
                    audit_status,
                    std::string(
                        archive
                            .temporary_directory_name()),
                    expected);
                if (result.error !=
                    FinalizationArchivePosixStoreErrorV1::
                        kNone) {
                    SetDiagnostic(
                        diagnostic,
                        "cannot rebuild recognized partial archive temporary");
                    return result;
                }
                LoadedTree rebuilt{};
                result.error = LoadExactTree(
                    audit.get(),
                    audit_status,
                    std::string(
                        archive
                            .temporary_directory_name()),
                    expected,
                    true,
                    &rebuilt);
                if (result.error !=
                    FinalizationArchivePosixStoreErrorV1::
                        kNone ||
                    !RenameNoReplace(
                        audit.get(),
                        std::string(
                            archive
                                .temporary_directory_name())
                            .c_str(),
                        std::string(
                            archive.directory_name())
                            .c_str()) ||
                    !FsyncNoIntr(audit.get())) {
                    if (result.error ==
                        FinalizationArchivePosixStoreErrorV1::
                            kNone) {
                        result.error =
                            FinalizationArchivePosixStoreErrorV1::
                                kPublishConflict;
                    }
                    SetDiagnostic(
                        diagnostic,
                        "rebuilt archive temporary could not cross its publish barrier");
                    return result;
                }
                result.files_synced = true;
                result.subdirectories_synced = true;
                result.audit_parent_synced = true;
                final_tree = std::move(rebuilt);
                result.disposition =
                    FinalizationArchivePosixDispositionV1::
                        kRebuiltRecognizedPartialTemporary;
            }
        } else {
            result.error = CreateTemporaryTree(
                audit.get(),
                audit_status,
                std::string(
                    archive.temporary_directory_name()),
                expected);
            if (result.error !=
                FinalizationArchivePosixStoreErrorV1::
                    kNone) {
                SetDiagnostic(
                    diagnostic,
                    "cannot build finalization archive temporary tree");
                return result;
            }
            LoadedTree temporary{};
            result.error = LoadExactTree(
                audit.get(),
                audit_status,
                std::string(
                    archive.temporary_directory_name()),
                expected,
                true,
                &temporary);
            if (result.error !=
                FinalizationArchivePosixStoreErrorV1::
                    kNone) {
                SetDiagnostic(
                    diagnostic,
                    "new finalization archive temporary failed exact readback");
                return result;
            }
            result.files_synced = true;
            result.subdirectories_synced = true;
            if (!RenameNoReplace(
                    audit.get(),
                    std::string(
                        archive
                            .temporary_directory_name())
                        .c_str(),
                    std::string(
                        archive.directory_name())
                        .c_str())) {
                result.error =
                    FinalizationArchivePosixStoreErrorV1::
                        kPublishConflict;
                return result;
            }
            if (!FsyncNoIntr(audit.get())) {
                result.error =
                    FinalizationArchivePosixStoreErrorV1::
                        kSyncFailure;
                return result;
            }
            result.audit_parent_synced = true;
            if (!NameMatchesDescriptor(
                    audit.get(),
                    std::string(
                        archive.directory_name())
                        .c_str(),
                    temporary.archive_directory.get(),
                    &temporary.archive_status)) {
                result.error =
                    FinalizationArchivePosixStoreErrorV1::
                        kReadbackFailure;
                return result;
            }
            final_tree = std::move(temporary);
            result.disposition =
                FinalizationArchivePosixDispositionV1::
                    kPublishedNew;
        }

        // Every branch returns only after one more complete readback through
        // the retained final-name tree.
        LoadedTree verified{};
        result.error = LoadExactTree(
            audit.get(),
            audit_status,
            std::string(archive.directory_name()),
            expected,
            false,
            &verified);
        if (result.error !=
            FinalizationArchivePosixStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "published finalization archive failed final-name readback");
            return result;
        }
        final_tree = std::move(verified);
        result.receipt =
            FinalizationArchivePosixPublishAccessV1::
                MakeReceipt(
                    archive,
                    audit.get(),
                    audit_status,
                    &final_tree);
        if (result.receipt == nullptr ||
            !result.receipt->Validate(diagnostic)) {
            result.receipt.reset();
            result.error =
                FinalizationArchivePosixStoreErrorV1::
                    kReadbackFailure;
            return result;
        }
        result.error =
            FinalizationArchivePosixStoreErrorV1::kNone;
        return result;
    } catch (...) {
        result.receipt.reset();
        result.error =
            FinalizationArchivePosixStoreErrorV1::
                kAllocationFailure;
        SetDiagnostic(
            diagnostic,
            "finalization archive publication allocation failed");
        return result;
    }
}

}  // namespace l2flow::ingress
