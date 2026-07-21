#include "l2flow/ingress/empty_anchor_tombstone_store.h"

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

class LeaseTargetProvider final
    : public RawReserveMutationTargetProviderV1 {
public:
    explicit LeaseTargetProvider(
        const RawWriterLease& lease) noexcept
        : lease_(lease) {}

    [[nodiscard]] int
    RawReserveMutationTargetDirectoryDescriptorV1()
        const noexcept override {
        return lease_.directory_descriptor();
    }

private:
    const RawWriterLease& lease_;
};

struct LoadedCandidate final {
    ScopedFd descriptor;
    struct stat status {};
    std::string bytes;
};

struct CandidateInventory final {
    std::uint32_t count = 0U;
    bool final_present = false;
    bool temporary_present = false;
    LoadedCandidate final;
    LoadedCandidate temporary;
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

[[nodiscard]] bool LockExclusiveNoIntr(
    int fd) noexcept {
    for (;;) {
        if (::flock(fd, LOCK_EX) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] int DuplicateFd(int fd) noexcept {
    for (;;) {
        const int duplicate =
            ::fcntl(fd, F_DUPFD_CLOEXEC, 0);
        if (duplicate >= 0 || errno != EINTR) {
            return duplicate;
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

[[nodiscard]] bool IsSafeTombstoneFile(
    const struct stat& status) noexcept {
    return S_ISREG(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           (status.st_mode & 07777U) == 0600U &&
           status.st_nlink == static_cast<nlink_t>(1) &&
           status.st_size >= 0 &&
           static_cast<std::uint64_t>(status.st_size) <=
               kEmptyAnchorTombstoneV1MaximumBytes;
}

[[nodiscard]] bool IsSafeJournalFile(
    const struct stat& status) noexcept {
    return S_ISREG(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           (status.st_mode & 07777U) == 0600U &&
           status.st_nlink == static_cast<nlink_t>(1) &&
           status.st_size ==
               static_cast<off_t>(
                   kRawV1JournalHeaderBytes);
}

[[nodiscard]] bool NameMatchesDescriptor(
    int directory_fd,
    const char* name,
    int fd,
    const struct stat* expected = nullptr) noexcept {
    struct stat opened {};
    struct stat named {};
    return fd >= 0 &&
           ::fstat(fd, &opened) == 0 &&
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

[[nodiscard]] bool ReadExactBytes(
    int fd,
    std::span<std::byte> output) noexcept {
    std::size_t completed = 0U;
    while (completed < output.size()) {
        const ssize_t result = ::pread(
            fd,
            output.data() + completed,
            output.size() - completed,
            static_cast<off_t>(completed));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            return false;
        }
        completed += static_cast<std::size_t>(result);
    }
    return true;
}

[[nodiscard]] bool ReadExactString(
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
    if (!ReadExactBytes(
            fd,
            std::span<std::byte>(
                reinterpret_cast<std::byte*>(
                    candidate.data()),
                candidate.size()))) {
        return false;
    }
    output->swap(candidate);
    return true;
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

[[nodiscard]] bool IsFinalCandidateName(
    std::string_view name) noexcept {
    constexpr std::string_view prefix =
        "empty-anchor-";
    constexpr std::string_view suffix = ".json";
    constexpr std::size_t expected_size =
        prefix.size() + 32U + suffix.size();
    return name.size() == expected_size &&
           name.substr(0U, prefix.size()) == prefix &&
           name.substr(
               name.size() - suffix.size()) == suffix &&
           IsLowerHex(
               name.substr(prefix.size(), 32U));
}

[[nodiscard]] bool IsTemporaryCandidateName(
    std::string_view name) noexcept {
    if (name.size() <=
            1U +
                kEmptyAnchorTombstoneV1TemporarySuffix
                    .size() ||
        name.front() != '.' ||
        !name.ends_with(
            kEmptyAnchorTombstoneV1TemporarySuffix)) {
        return false;
    }
    const std::size_t final_size =
        name.size() - 1U -
        kEmptyAnchorTombstoneV1TemporarySuffix.size();
    return IsFinalCandidateName(
        name.substr(1U, final_size));
}

[[nodiscard]] bool StartsLikeCandidate(
    std::string_view name) noexcept {
    return name.starts_with("empty-anchor-") ||
           name.starts_with(".empty-anchor-");
}

[[nodiscard]] bool Authorize(
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider) noexcept {
    const RawReserveMutationTargetAnchorV1* const target =
        action.target();
    return target != nullptr &&
           action.required_status() ==
               ReserveRegistryStatusV1::kRecovering &&
           action.recovery_intent() ==
               ReserveRecoveryIntentV1::
                   kRecoverSealOnly &&
           action.token().recovery_attempt_id ==
               action.key().recovery_attempt_id &&
           !l2flow::common::IsZeroIdentity(
               action.token().writer_instance_id) &&
           ValidateRawReserveMutationTargetProviderV1(
               provider, *target) &&
           action.ValidateLatest();
}

[[nodiscard]] bool MaintenanceStillNamed(
    const RawWriterLease& lease,
    int maintenance_fd,
    const struct stat* expected = nullptr) noexcept {
    struct stat maintenance {};
    struct stat route {};
    return maintenance_fd >= 0 &&
           ::fstat(maintenance_fd, &maintenance) == 0 &&
           ::fstat(
               lease.directory_descriptor(),
               &route) == 0 &&
           IsSafeDirectory(maintenance) &&
           maintenance.st_dev == route.st_dev &&
           NameMatchesDescriptor(
               lease.directory_descriptor(),
               "maintenance",
               maintenance_fd,
               expected);
}

[[nodiscard]] bool JournalStillNamed(
    const RawWriterLease& lease,
    int journal_fd,
    const struct stat& expected) noexcept {
    struct stat current {};
    return journal_fd >= 0 &&
           ::fstat(journal_fd, &current) == 0 &&
           IsSafeJournalFile(current) &&
           SameInode(current, expected) &&
           current.st_size == expected.st_size &&
           NameMatchesDescriptor(
               lease.directory_descriptor(),
               kRawJournalFilename,
               journal_fd,
               &expected);
}

[[nodiscard]] EmptyAnchorTombstoneStoreErrorV1
ValidateContext(
    const RawWriterLease& lease,
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider,
    int maintenance_fd,
    const struct stat& maintenance_status,
    int journal_fd,
    const struct stat& journal_status) noexcept {
    if (!Authorize(action, provider)) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kAuthorizationRejected;
    }
    if (!MaintenanceStillNamed(
            lease,
            maintenance_fd,
            &maintenance_status)) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kUnsafeMaintenanceDirectory;
    }
    if (!JournalStillNamed(
            lease,
            journal_fd,
            journal_status)) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kUnsafeJournal;
    }
    return EmptyAnchorTombstoneStoreErrorV1::kNone;
}

[[nodiscard]] EmptyAnchorTombstoneStoreErrorV1
OpenMaintenance(
    const RawWriterLease& lease,
    ScopedFd* output,
    struct stat* output_status) noexcept {
    if (output == nullptr || output_status == nullptr) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kInvalidArgument;
    }
    ScopedFd candidate(
        OpenAtNoIntr(
            lease.directory_descriptor(),
            "maintenance",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC | O_NOATIME));
    if (candidate.get() < 0) {
        return errno == ENOENT
                   ? EmptyAnchorTombstoneStoreErrorV1::
                         kMaintenanceDirectoryMissing
                   : EmptyAnchorTombstoneStoreErrorV1::
                         kUnsafeMaintenanceDirectory;
    }
    struct stat status {};
    struct stat route {};
    if (::fstat(candidate.get(), &status) != 0 ||
        ::fstat(
            lease.directory_descriptor(),
            &route) != 0 ||
        !IsSafeDirectory(status) ||
        status.st_dev != route.st_dev ||
        !NameMatchesDescriptor(
            lease.directory_descriptor(),
            "maintenance",
            candidate.get(),
            &status)) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kUnsafeMaintenanceDirectory;
    }
    *output_status = status;
    *output = std::move(candidate);
    return EmptyAnchorTombstoneStoreErrorV1::kNone;
}

[[nodiscard]] EmptyAnchorTombstoneStoreErrorV1
OpenAndValidateJournal(
    const RawWriterLease& lease,
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider,
    const BuiltEmptyAnchorTombstoneV1& tombstone,
    int maintenance_fd,
    const struct stat& maintenance_status,
    ScopedFd* output,
    struct stat* output_status,
    RawV1JournalHeaderWire* output_wire) noexcept {
    if (output == nullptr ||
        output_status == nullptr ||
        output_wire == nullptr) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kInvalidArgument;
    }
    ScopedFd journal(
        OpenAtNoIntr(
            lease.directory_descriptor(),
            kRawJournalFilename,
            O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME));
    if (journal.get() < 0) {
        return errno == ENOENT
                   ? EmptyAnchorTombstoneStoreErrorV1::
                         kJournalMissing
                   : EmptyAnchorTombstoneStoreErrorV1::
                         kUnsafeJournal;
    }
    struct stat status {};
    const int flags = ::fcntl(journal.get(), F_GETFL);
    if (::fstat(journal.get(), &status) != 0 ||
        !IsSafeJournalFile(status) ||
        flags < 0 ||
        (flags & O_ACCMODE) != O_RDONLY ||
        (flags & O_APPEND) != 0 ||
        !NameMatchesDescriptor(
            lease.directory_descriptor(),
            kRawJournalFilename,
            journal.get(),
            &status)) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kUnsafeJournal;
    }
    RawV1JournalHeaderWire wire{};
    DurableJournalHeaderV1 decoded{};
    RawV1JournalHeaderWire canonical{};
    const EmptyAnchorTombstoneV1& model =
        tombstone.model();
    if (!ReadExactBytes(journal.get(), wire) ||
        DecodeDurableJournalHeaderV1(
            wire, &decoded) != RawV1Error::kNone ||
        EncodeDurableJournalHeaderV1(
            decoded, &canonical) != RawV1Error::kNone ||
        canonical != wire ||
        decoded.source_stream_id !=
            model.namespace_identity.source_stream_id ||
        decoded.capture_date !=
            model.namespace_identity.capture_date ||
        decoded.stream_day_id !=
            model.namespace_identity.stream_day_id ||
        l2flow::common::ComputeSha256(wire) !=
            model.journal_header_sha256) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kJournalMismatch;
    }
    const EmptyAnchorTombstoneStoreErrorV1 context =
        ValidateContext(
            lease,
            action,
            provider,
            maintenance_fd,
            maintenance_status,
            journal.get(),
            status);
    if (context !=
        EmptyAnchorTombstoneStoreErrorV1::kNone) {
        return context;
    }
    *output_status = status;
    *output_wire = wire;
    *output = std::move(journal);
    return EmptyAnchorTombstoneStoreErrorV1::kNone;
}

[[nodiscard]] bool ValidateRetainedWriterLease(
    const RawWriterLease& lease) noexcept {
    struct stat lease_status {};
    struct stat route_status {};
    const int flags =
        ::fcntl(lease.descriptor(), F_GETFL);
    std::array<std::byte, kRawWriterLeaseMarkerBytes>
        marker_wire{};
    RawWriterLeaseMarkerV1 marker{};
    return lease.descriptor() >= 0 &&
           lease.directory_descriptor() >= 0 &&
           ::fstat(
               lease.directory_descriptor(),
               &route_status) == 0 &&
           IsSafeDirectory(route_status) &&
           ::fstat(
               lease.descriptor(),
               &lease_status) == 0 &&
           S_ISREG(lease_status.st_mode) &&
           lease_status.st_uid == ::geteuid() &&
           (lease_status.st_mode & 07777U) == 0600U &&
           lease_status.st_nlink ==
               static_cast<nlink_t>(1) &&
           lease_status.st_size ==
               static_cast<off_t>(
                   kRawWriterLeaseMarkerBytes) &&
           flags >= 0 &&
           (flags & O_ACCMODE) == O_RDWR &&
           (flags & O_APPEND) == 0 &&
           NameMatchesDescriptor(
               lease.directory_descriptor(),
               kRawWriterLeaseFilename,
               lease.descriptor(),
               &lease_status) &&
           ReadExactBytes(
               lease.descriptor(), marker_wire) &&
           DecodeRawWriterLeaseMarkerV1(
               marker_wire, &marker) &&
           marker.source_stream_id ==
               lease.source_stream_id() &&
           marker.capture_date ==
               lease.capture_date();
}

[[nodiscard]] EmptyAnchorTombstoneStoreErrorV1
InventoryEmptyStreamDay(
    const RawWriterLease& lease,
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider,
    int maintenance_fd,
    const struct stat& maintenance_status,
    int journal_fd,
    const struct stat& journal_status) noexcept {
    EmptyAnchorTombstoneStoreErrorV1 context =
        ValidateContext(
            lease,
            action,
            provider,
            maintenance_fd,
            maintenance_status,
            journal_fd,
            journal_status);
    if (context !=
        EmptyAnchorTombstoneStoreErrorV1::kNone) {
        return context;
    }
    ScopedFd scan_fd(
        OpenAtNoIntr(
            lease.directory_descriptor(),
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC | O_NOATIME));
    struct stat route {};
    struct stat scan {};
    if (scan_fd.get() < 0 ||
        ::fstat(
            lease.directory_descriptor(),
            &route) != 0 ||
        ::fstat(scan_fd.get(), &scan) != 0 ||
        !IsSafeDirectory(route) ||
        !SameInode(route, scan)) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kNamespaceInventory;
    }
    DIR* const native =
        ::fdopendir(scan_fd.Release());
    if (native == nullptr) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kNamespaceInventory;
    }
    ScopedDir entries(native);
    bool saw_lease = false;
    bool saw_maintenance = false;
    bool saw_journal = false;
    for (;;) {
        errno = 0;
        dirent* const entry = ::readdir(entries.get());
        if (entry == nullptr) {
            if (errno != 0) {
                return EmptyAnchorTombstoneStoreErrorV1::
                    kNamespaceInventory;
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
            !saw_lease) {
            saw_lease = true;
            continue;
        }
        if (std::strcmp(name, "maintenance") == 0 &&
            !saw_maintenance) {
            saw_maintenance = true;
            continue;
        }
        if (std::strcmp(
                name,
                kRawJournalFilename) == 0 &&
            !saw_journal) {
            saw_journal = true;
            continue;
        }
        return EmptyAnchorTombstoneStoreErrorV1::
            kNamespaceInventory;
    }
    context = ValidateContext(
        lease,
        action,
        provider,
        maintenance_fd,
        maintenance_status,
        journal_fd,
        journal_status);
    if (!saw_lease ||
        !saw_maintenance ||
        !saw_journal ||
        !ValidateRetainedWriterLease(lease) ||
        context !=
            EmptyAnchorTombstoneStoreErrorV1::kNone) {
        return context !=
                       EmptyAnchorTombstoneStoreErrorV1::
                           kNone
                   ? context
                   : EmptyAnchorTombstoneStoreErrorV1::
                         kNamespaceInventory;
    }
    return EmptyAnchorTombstoneStoreErrorV1::kNone;
}

[[nodiscard]] EmptyAnchorTombstoneStoreErrorV1
LoadCandidate(
    const RawWriterLease& lease,
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider,
    int maintenance_fd,
    const struct stat& maintenance_status,
    int journal_fd,
    const struct stat& journal_status,
    const std::string& name,
    const std::string& expected_final_name,
    std::string_view desired,
    LoadedCandidate* output) {
    if (output == nullptr) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kInvalidArgument;
    }
    EmptyAnchorTombstoneStoreErrorV1 context =
        ValidateContext(
            lease,
            action,
            provider,
            maintenance_fd,
            maintenance_status,
            journal_fd,
            journal_status);
    if (context !=
        EmptyAnchorTombstoneStoreErrorV1::kNone) {
        return context;
    }
    ScopedFd fd(
        OpenAtNoIntr(
            maintenance_fd,
            name.c_str(),
            O_RDWR | O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME));
    if (fd.get() < 0) {
        return errno == ENOENT
                   ? EmptyAnchorTombstoneStoreErrorV1::
                         kPublishConflict
                   : EmptyAnchorTombstoneStoreErrorV1::
                         kUnsafeCandidate;
    }
    struct stat before {};
    if (::fstat(fd.get(), &before) != 0 ||
        !IsSafeTombstoneFile(before) ||
        !NameMatchesDescriptor(
            maintenance_fd,
            name.c_str(),
            fd.get(),
            &before)) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kUnsafeCandidate;
    }
    std::string bytes;
    if (!ReadExactString(
            fd.get(),
            static_cast<std::size_t>(before.st_size),
            &bytes)) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kReadbackFailure;
    }
    struct stat after {};
    EmptyAnchorTombstoneV1 parsed{};
    std::string parsed_filename;
    if (::fstat(fd.get(), &after) != 0 ||
        !IsSafeTombstoneFile(after) ||
        !SameInode(before, after) ||
        before.st_size != after.st_size ||
        !NameMatchesDescriptor(
            maintenance_fd,
            name.c_str(),
            fd.get(),
            &after) ||
        bytes != desired ||
        ParseEmptyAnchorTombstoneV1Jcs(
            bytes, &parsed) !=
            EmptyAnchorTombstoneV1Error::kNone ||
        EmptyAnchorTombstoneV1Filename(
            parsed, &parsed_filename) !=
            EmptyAnchorTombstoneV1Error::kNone ||
        parsed_filename != expected_final_name ||
        ValidateContext(
            lease,
            action,
            provider,
            maintenance_fd,
            maintenance_status,
            journal_fd,
            journal_status) !=
            EmptyAnchorTombstoneStoreErrorV1::kNone) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kCandidateConflict;
    }
    LoadedCandidate candidate{};
    candidate.descriptor = std::move(fd);
    candidate.status = after;
    candidate.bytes = std::move(bytes);
    *output = std::move(candidate);
    return EmptyAnchorTombstoneStoreErrorV1::kNone;
}

[[nodiscard]] EmptyAnchorTombstoneStoreErrorV1
InventoryCandidates(
    const RawWriterLease& lease,
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider,
    int maintenance_fd,
    const struct stat& maintenance_status,
    int journal_fd,
    const struct stat& journal_status,
    const std::string& final_name,
    const std::string& temporary_name,
    std::string_view desired,
    CandidateInventory* output) {
    if (output == nullptr) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kInvalidArgument;
    }
    EmptyAnchorTombstoneStoreErrorV1 context =
        ValidateContext(
            lease,
            action,
            provider,
            maintenance_fd,
            maintenance_status,
            journal_fd,
            journal_status);
    if (context !=
        EmptyAnchorTombstoneStoreErrorV1::kNone) {
        return context;
    }
    ScopedFd scan_fd(
        OpenAtNoIntr(
            maintenance_fd,
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC | O_NOATIME));
    struct stat expected {};
    struct stat actual {};
    if (scan_fd.get() < 0 ||
        ::fstat(maintenance_fd, &expected) != 0 ||
        ::fstat(scan_fd.get(), &actual) != 0 ||
        !SameInode(expected, actual) ||
        !SameInode(expected, maintenance_status)) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kUnsafeMaintenanceDirectory;
    }
    DIR* const native =
        ::fdopendir(scan_fd.Release());
    if (native == nullptr) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kUnsafeMaintenanceDirectory;
    }
    ScopedDir entries(native);
    CandidateInventory inventory{};
    bool malformed = false;
    bool foreign_candidate = false;
    for (;;) {
        errno = 0;
        dirent* const entry = ::readdir(entries.get());
        if (entry == nullptr) {
            if (errno != 0) {
                return EmptyAnchorTombstoneStoreErrorV1::
                    kUnsafeMaintenanceDirectory;
            }
            break;
        }
        const std::string_view name(entry->d_name);
        if (name == "." || name == "..") {
            continue;
        }
        const bool is_final =
            IsFinalCandidateName(name);
        const bool is_temporary =
            IsTemporaryCandidateName(name);
        if (StartsLikeCandidate(name) &&
            !is_final && !is_temporary) {
            malformed = true;
        }
        if (!is_final && !is_temporary) {
            continue;
        }
        if (inventory.count ==
            kEmptyAnchorTombstoneV1MaximumCandidates) {
            return EmptyAnchorTombstoneStoreErrorV1::
                kCandidateLimitExceeded;
        }
        ++inventory.count;
        if (name == final_name) {
            inventory.final_present = true;
        } else if (name == temporary_name) {
            inventory.temporary_present = true;
        } else {
            foreign_candidate = true;
        }
    }
    context = ValidateContext(
        lease,
        action,
        provider,
        maintenance_fd,
        maintenance_status,
        journal_fd,
        journal_status);
    if (context !=
        EmptyAnchorTombstoneStoreErrorV1::kNone) {
        return context;
    }
    if (malformed) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kMalformedCandidateName;
    }
    if (foreign_candidate ||
        (inventory.final_present &&
         inventory.temporary_present)) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kCandidateConflict;
    }
    if (inventory.final_present) {
        context = LoadCandidate(
            lease,
            action,
            provider,
            maintenance_fd,
            maintenance_status,
            journal_fd,
            journal_status,
            final_name,
            final_name,
            desired,
            &inventory.final);
    } else if (inventory.temporary_present) {
        context = LoadCandidate(
            lease,
            action,
            provider,
            maintenance_fd,
            maintenance_status,
            journal_fd,
            journal_status,
            temporary_name,
            final_name,
            desired,
            &inventory.temporary);
    }
    if (context !=
        EmptyAnchorTombstoneStoreErrorV1::kNone) {
        return context;
    }
    *output = std::move(inventory);
    return EmptyAnchorTombstoneStoreErrorV1::kNone;
}

[[nodiscard]] EmptyAnchorTombstoneStoreErrorV1
RevalidateCandidate(
    const RawWriterLease& lease,
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider,
    int maintenance_fd,
    const struct stat& maintenance_status,
    int journal_fd,
    const struct stat& journal_status,
    const std::string& name,
    LoadedCandidate* candidate) {
    if (candidate == nullptr ||
        candidate->descriptor.get() < 0) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kInvalidArgument;
    }
    EmptyAnchorTombstoneStoreErrorV1 context =
        ValidateContext(
            lease,
            action,
            provider,
            maintenance_fd,
            maintenance_status,
            journal_fd,
            journal_status);
    if (context !=
        EmptyAnchorTombstoneStoreErrorV1::kNone) {
        return context;
    }
    struct stat before {};
    std::string bytes;
    struct stat after {};
    if (::fstat(
            candidate->descriptor.get(),
            &before) != 0 ||
        !IsSafeTombstoneFile(before) ||
        !SameInode(before, candidate->status) ||
        before.st_size != candidate->status.st_size ||
        !NameMatchesDescriptor(
            maintenance_fd,
            name.c_str(),
            candidate->descriptor.get(),
            &before) ||
        !ReadExactString(
            candidate->descriptor.get(),
            static_cast<std::size_t>(before.st_size),
            &bytes) ||
        bytes != candidate->bytes ||
        ::fstat(
            candidate->descriptor.get(),
            &after) != 0 ||
        !IsSafeTombstoneFile(after) ||
        !SameInode(before, after) ||
        before.st_size != after.st_size ||
        !NameMatchesDescriptor(
            maintenance_fd,
            name.c_str(),
            candidate->descriptor.get(),
            &after)) {
        return EmptyAnchorTombstoneStoreErrorV1::
            kCandidateConflict;
    }
    context = ValidateContext(
        lease,
        action,
        provider,
        maintenance_fd,
        maintenance_status,
        journal_fd,
        journal_status);
    if (context !=
        EmptyAnchorTombstoneStoreErrorV1::kNone) {
        return context;
    }
    candidate->status = after;
    return EmptyAnchorTombstoneStoreErrorV1::kNone;
}

[[nodiscard]] bool WriteExactAuthorized(
    const RawWriterLease& lease,
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider,
    int maintenance_fd,
    const struct stat& maintenance_status,
    int journal_fd,
    const struct stat& journal_status,
    const char* name,
    int fd,
    std::string_view bytes) noexcept {
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        struct stat status {};
        if (ValidateContext(
                lease,
                action,
                provider,
                maintenance_fd,
                maintenance_status,
                journal_fd,
                journal_status) !=
                EmptyAnchorTombstoneStoreErrorV1::
                    kNone ||
            ::fstat(fd, &status) != 0 ||
            !IsSafeTombstoneFile(status) ||
            !NameMatchesDescriptor(
                maintenance_fd, name, fd)) {
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
        completed += static_cast<std::size_t>(result);
    }
    return ValidateContext(
               lease,
               action,
               provider,
               maintenance_fd,
               maintenance_status,
               journal_fd,
               journal_status) ==
               EmptyAnchorTombstoneStoreErrorV1::kNone &&
           NameMatchesDescriptor(
               maintenance_fd, name, fd);
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

[[nodiscard]] bool NameAbsent(
    int directory_fd,
    const char* name) noexcept {
    struct stat status {};
    errno = 0;
    return ::fstatat(
               directory_fd,
               name,
               &status,
               AT_SYMLINK_NOFOLLOW) != 0 &&
           errno == ENOENT;
}

}  // namespace

std::string_view
EmptyAnchorTombstoneStoreErrorV1Name(
    EmptyAnchorTombstoneStoreErrorV1 error) noexcept {
    switch (error) {
    case EmptyAnchorTombstoneStoreErrorV1::kNone:
        return "none";
    case EmptyAnchorTombstoneStoreErrorV1::
        kInvalidArgument:
        return "invalid_argument";
    case EmptyAnchorTombstoneStoreErrorV1::
        kAuthorizationRejected:
        return "authorization_rejected";
    case EmptyAnchorTombstoneStoreErrorV1::
        kTargetMismatch:
        return "target_mismatch";
    case EmptyAnchorTombstoneStoreErrorV1::
        kNamespaceInventory:
        return "namespace_inventory";
    case EmptyAnchorTombstoneStoreErrorV1::
        kJournalMissing:
        return "journal_missing";
    case EmptyAnchorTombstoneStoreErrorV1::
        kUnsafeJournal:
        return "unsafe_journal";
    case EmptyAnchorTombstoneStoreErrorV1::
        kJournalMismatch:
        return "journal_mismatch";
    case EmptyAnchorTombstoneStoreErrorV1::
        kMaintenanceDirectoryMissing:
        return "maintenance_directory_missing";
    case EmptyAnchorTombstoneStoreErrorV1::
        kUnsafeMaintenanceDirectory:
        return "unsafe_maintenance_directory";
    case EmptyAnchorTombstoneStoreErrorV1::
        kCandidateLimitExceeded:
        return "candidate_limit_exceeded";
    case EmptyAnchorTombstoneStoreErrorV1::
        kMalformedCandidateName:
        return "malformed_candidate_name";
    case EmptyAnchorTombstoneStoreErrorV1::
        kUnsafeCandidate:
        return "unsafe_candidate";
    case EmptyAnchorTombstoneStoreErrorV1::
        kCandidateConflict:
        return "candidate_conflict";
    case EmptyAnchorTombstoneStoreErrorV1::
        kTemporaryCreate:
        return "temporary_create";
    case EmptyAnchorTombstoneStoreErrorV1::
        kWriteFailure:
        return "write_failure";
    case EmptyAnchorTombstoneStoreErrorV1::
        kSyncFailure:
        return "sync_failure";
    case EmptyAnchorTombstoneStoreErrorV1::
        kPublishConflict:
        return "publish_conflict";
    case EmptyAnchorTombstoneStoreErrorV1::
        kReadbackFailure:
        return "readback_failure";
    case EmptyAnchorTombstoneStoreErrorV1::
        kAllocationFailure:
        return "allocation_failure";
    }
    return "unknown";
}

EmptyAnchorTombstonePublishResultV1
PublishEmptyAnchorTombstoneV1(
    const RawWriterLease& lease,
    std::unique_ptr<RawReserveAuthorizedActionV1>
        recovering_action,
    const BuiltEmptyAnchorTombstoneV1& tombstone,
    std::string* diagnostic) noexcept {
    EmptyAnchorTombstonePublishResultV1 result{};
    SetDiagnostic(diagnostic, {});
    try {
        if (recovering_action == nullptr) {
            result.error =
                EmptyAnchorTombstoneStoreErrorV1::
                    kInvalidArgument;
            SetDiagnostic(
                diagnostic,
                "RECOVER_SEAL_ONLY tombstone action is missing");
            return result;
        }
        RawReserveAuthorizedActionV1& action =
            *recovering_action;
        const EmptyAnchorTombstoneV1& model =
            tombstone.model();
        const std::string desired(
            tombstone.canonical_jcs());
        std::string filename(tombstone.filename());
        if (lease.descriptor() < 0 ||
            lease.directory_descriptor() < 0 ||
            lease.source_stream_id() !=
                model.namespace_identity
                    .source_stream_id ||
            lease.capture_date() !=
                model.namespace_identity.capture_date ||
            desired.empty() ||
            desired.size() >
                kEmptyAnchorTombstoneV1MaximumBytes ||
            !IsFinalCandidateName(filename) ||
            ValidateEmptyAnchorTombstoneV1(model) !=
                EmptyAnchorTombstoneV1Error::kNone ||
            l2flow::common::ComputeSha256(desired) !=
                tombstone.tombstone_sha256()) {
            result.error =
                EmptyAnchorTombstoneStoreErrorV1::
                    kInvalidArgument;
            SetDiagnostic(
                diagnostic,
                "empty-anchor tombstone input is invalid");
            return result;
        }
        const RawReserveRegistryEntryKeyV1& key =
            action.key();
        if (key.route.source_stream_id !=
                model.namespace_identity
                    .source_stream_id ||
            key.route.capture_date !=
                model.namespace_identity.capture_date ||
            key.stream_day_id !=
                model.namespace_identity.stream_day_id ||
            action.token().recovery_attempt_id !=
                key.recovery_attempt_id) {
            result.error =
                EmptyAnchorTombstoneStoreErrorV1::
                    kAuthorizationRejected;
            SetDiagnostic(
                diagnostic,
                "RECOVERING action namespace does not match the empty anchor");
            return result;
        }
        LeaseTargetProvider provider(lease);
        const RawReserveMutationTargetAnchorV1* const
            target = action.target();
        if (target == nullptr ||
            !ValidateRawReserveMutationTargetProviderV1(
                provider, *target)) {
            result.error =
                EmptyAnchorTombstoneStoreErrorV1::
                    kTargetMismatch;
            SetDiagnostic(
                diagnostic,
                "empty-anchor route target does not match the capability");
            return result;
        }
        if (!Authorize(action, provider)) {
            result.error =
                EmptyAnchorTombstoneStoreErrorV1::
                    kAuthorizationRejected;
            SetDiagnostic(
                diagnostic,
                "RECOVER_SEAL_ONLY capability is stale");
            return result;
        }

        ScopedFd maintenance;
        struct stat maintenance_status {};
        result.error =
            OpenMaintenance(
                lease,
                &maintenance,
                &maintenance_status);
        if (result.error !=
            EmptyAnchorTombstoneStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "cannot open the pre-existing maintenance directory");
            return result;
        }
        if (!LockExclusiveNoIntr(maintenance.get())) {
            result.error =
                EmptyAnchorTombstoneStoreErrorV1::
                    kUnsafeMaintenanceDirectory;
            SetDiagnostic(
                diagnostic,
                "cannot serialize empty-anchor maintenance publication");
            return result;
        }

        ScopedFd journal;
        struct stat journal_status {};
        RawV1JournalHeaderWire journal_wire{};
        result.error = OpenAndValidateJournal(
            lease,
            action,
            provider,
            tombstone,
            maintenance.get(),
            maintenance_status,
            &journal,
            &journal_status,
            &journal_wire);
        if (result.error !=
            EmptyAnchorTombstoneStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "header-only Raw journal evidence is invalid");
            return result;
        }
        result.error = InventoryEmptyStreamDay(
            lease,
            action,
            provider,
            maintenance.get(),
            maintenance_status,
            journal.get(),
            journal_status);
        if (result.error !=
            EmptyAnchorTombstoneStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "stream-day is not an exact empty anchor");
            return result;
        }

        const std::string temporary =
            "." + filename +
            std::string(
                kEmptyAnchorTombstoneV1TemporarySuffix);
        CandidateInventory inventory{};
        result.error = InventoryCandidates(
            lease,
            action,
            provider,
            maintenance.get(),
            maintenance_status,
            journal.get(),
            journal_status,
            filename,
            temporary,
            desired,
            &inventory);
        result.observed_candidate_count =
            inventory.count;
        if (result.error !=
            EmptyAnchorTombstoneStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "empty-anchor tombstone candidate inventory is invalid");
            return result;
        }

        const auto ContextReady = [&]() noexcept {
            result.error = ValidateContext(
                lease,
                action,
                provider,
                maintenance.get(),
                maintenance_status,
                journal.get(),
                journal_status);
            return result.error ==
                   EmptyAnchorTombstoneStoreErrorV1::
                       kNone;
        };

        LoadedCandidate final =
            std::move(inventory.final);
        bool adopted = false;
        if (inventory.final_present) {
            result.error = RevalidateCandidate(
                lease,
                action,
                provider,
                maintenance.get(),
                maintenance_status,
                journal.get(),
                journal_status,
                filename,
                &final);
            if (result.error !=
                EmptyAnchorTombstoneStoreErrorV1::
                    kNone) {
                return result;
            }
        } else {
            LoadedCandidate temporary_candidate =
                std::move(inventory.temporary);
            if (!inventory.temporary_present) {
                if (!ContextReady() ||
                    !NameAbsent(
                        maintenance.get(),
                        filename.c_str()) ||
                    !NameAbsent(
                        maintenance.get(),
                        temporary.c_str())) {
                    if (result.error ==
                        EmptyAnchorTombstoneStoreErrorV1::
                            kNone) {
                        result.error =
                            EmptyAnchorTombstoneStoreErrorV1::
                                kPublishConflict;
                    }
                    return result;
                }
                ScopedFd created(
                    OpenAtNoIntr(
                        maintenance.get(),
                        temporary.c_str(),
                        O_RDWR | O_CREAT | O_EXCL |
                            O_NOFOLLOW | O_NONBLOCK |
                            O_CLOEXEC,
                        0600));
                struct stat created_status {};
                if (created.get() < 0) {
                    const int create_error = errno;
                    if (create_error != EEXIST) {
                        result.error =
                            EmptyAnchorTombstoneStoreErrorV1::
                                kTemporaryCreate;
                        return result;
                    }
                    CandidateInventory raced{};
                    result.error = InventoryCandidates(
                        lease,
                        action,
                        provider,
                        maintenance.get(),
                        maintenance_status,
                        journal.get(),
                        journal_status,
                        filename,
                        temporary,
                        desired,
                        &raced);
                    result.observed_candidate_count =
                        raced.count;
                    if (result.error !=
                            EmptyAnchorTombstoneStoreErrorV1::
                                kNone ||
                        raced.final_present ||
                        !raced.temporary_present) {
                        if (result.error ==
                            EmptyAnchorTombstoneStoreErrorV1::
                                kNone) {
                            result.error =
                                EmptyAnchorTombstoneStoreErrorV1::
                                    kPublishConflict;
                        }
                        return result;
                    }
                    temporary_candidate =
                        std::move(raced.temporary);
                    inventory.temporary_present = true;
                    adopted = true;
                } else {
                    if (!ContextReady() ||
                        ::fchmod(
                            created.get(), 0600) != 0 ||
                        ::fstat(
                            created.get(),
                            &created_status) != 0 ||
                        !IsSafeTombstoneFile(
                            created_status) ||
                        created_status.st_size != 0 ||
                        !NameMatchesDescriptor(
                            maintenance.get(),
                            temporary.c_str(),
                            created.get(),
                            &created_status) ||
                        !WriteExactAuthorized(
                            lease,
                            action,
                            provider,
                            maintenance.get(),
                            maintenance_status,
                            journal.get(),
                            journal_status,
                            temporary.c_str(),
                            created.get(),
                            desired) ||
                        ::fstat(
                            created.get(),
                            &created_status) != 0 ||
                        !IsSafeTombstoneFile(
                            created_status) ||
                        created_status.st_size !=
                            static_cast<off_t>(
                                desired.size()) ||
                        !NameMatchesDescriptor(
                            maintenance.get(),
                            temporary.c_str(),
                            created.get(),
                            &created_status)) {
                        if (result.error ==
                            EmptyAnchorTombstoneStoreErrorV1::
                                kNone) {
                            result.error =
                                EmptyAnchorTombstoneStoreErrorV1::
                                    kWriteFailure;
                        }
                        return result;
                    }
                    temporary_candidate.descriptor =
                        std::move(created);
                    temporary_candidate.status =
                        created_status;
                    temporary_candidate.bytes = desired;
                }
            } else {
                adopted = true;
            }

            result.error = RevalidateCandidate(
                lease,
                action,
                provider,
                maintenance.get(),
                maintenance_status,
                journal.get(),
                journal_status,
                temporary,
                &temporary_candidate);
            if (result.error !=
                EmptyAnchorTombstoneStoreErrorV1::
                    kNone) {
                return result;
            }
            if (!FsyncNoIntr(
                    temporary_candidate
                        .descriptor.get()) ||
                !ContextReady() ||
                !NameMatchesDescriptor(
                    maintenance.get(),
                    temporary.c_str(),
                    temporary_candidate
                        .descriptor.get())) {
                if (result.error ==
                    EmptyAnchorTombstoneStoreErrorV1::
                        kNone) {
                    result.error =
                        EmptyAnchorTombstoneStoreErrorV1::
                            kSyncFailure;
                }
                return result;
            }
            result.file_synced = true;
            if (!NameAbsent(
                    maintenance.get(),
                    filename.c_str()) ||
                !ContextReady() ||
                !RenameNoReplace(
                    maintenance.get(),
                    temporary.c_str(),
                    filename.c_str()) ||
                !ContextReady() ||
                !NameMatchesDescriptor(
                    maintenance.get(),
                    filename.c_str(),
                    temporary_candidate
                        .descriptor.get()) ||
                !NameAbsent(
                    maintenance.get(),
                    temporary.c_str())) {
                if (result.error ==
                    EmptyAnchorTombstoneStoreErrorV1::
                        kNone) {
                    result.error =
                        EmptyAnchorTombstoneStoreErrorV1::
                            kPublishConflict;
                }
                return result;
            }
            final = std::move(temporary_candidate);
        }

        if (!result.file_synced) {
            if (!FsyncNoIntr(final.descriptor.get()) ||
                !ContextReady() ||
                !NameMatchesDescriptor(
                    maintenance.get(),
                    filename.c_str(),
                    final.descriptor.get())) {
                if (result.error ==
                    EmptyAnchorTombstoneStoreErrorV1::
                        kNone) {
                    result.error =
                        EmptyAnchorTombstoneStoreErrorV1::
                            kSyncFailure;
                }
                return result;
            }
            result.file_synced = true;
        }
        if (!ContextReady() ||
            !FsyncNoIntr(maintenance.get()) ||
            !ContextReady()) {
            if (result.error ==
                EmptyAnchorTombstoneStoreErrorV1::
                    kNone) {
                result.error =
                    EmptyAnchorTombstoneStoreErrorV1::
                        kSyncFailure;
            }
            return result;
        }
        result.directory_synced = true;

        std::string readback;
        struct stat final_status {};
        struct stat after_readback {};
        RawV1JournalHeaderWire journal_readback{};
        if (::fstat(
                final.descriptor.get(),
                &final_status) != 0 ||
            !IsSafeTombstoneFile(final_status) ||
            static_cast<std::uint64_t>(
                final_status.st_size) !=
                desired.size() ||
            !ReadExactString(
                final.descriptor.get(),
                desired.size(),
                &readback) ||
            readback != desired ||
            l2flow::common::ComputeSha256(readback) !=
                tombstone.tombstone_sha256() ||
            ::fstat(
                final.descriptor.get(),
                &after_readback) != 0 ||
            !IsSafeTombstoneFile(after_readback) ||
            !SameInode(
                final_status, after_readback) ||
            final_status.st_size !=
                after_readback.st_size ||
            !NameMatchesDescriptor(
                maintenance.get(),
                filename.c_str(),
                final.descriptor.get(),
                &after_readback) ||
            !ReadExactBytes(
                journal.get(), journal_readback) ||
            journal_readback != journal_wire ||
            l2flow::common::ComputeSha256(
                journal_readback) !=
                model.journal_header_sha256 ||
            !ContextReady()) {
            if (result.error ==
                EmptyAnchorTombstoneStoreErrorV1::
                    kNone) {
                result.error =
                    EmptyAnchorTombstoneStoreErrorV1::
                        kReadbackFailure;
            }
            return result;
        }
        result.error = InventoryEmptyStreamDay(
            lease,
            action,
            provider,
            maintenance.get(),
            maintenance_status,
            journal.get(),
            journal_status);
        if (result.error !=
            EmptyAnchorTombstoneStoreErrorV1::kNone) {
            return result;
        }

        ScopedFd receipt_maintenance(
            OpenAtNoIntr(
                maintenance.get(),
                ".",
                O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                    O_NONBLOCK | O_CLOEXEC | O_NOATIME));
        ScopedFd receipt_journal(
            DuplicateFd(journal.get()));
        ScopedFd receipt_tombstone(
            DuplicateFd(final.descriptor.get()));
        struct stat receipt_maintenance_status {};
        struct stat receipt_journal_status {};
        struct stat receipt_tombstone_status {};
        const RawReserveMutationTargetAnchorV1* const
            receipt_target = action.target();
        if (receipt_target == nullptr ||
            receipt_maintenance.get() < 0 ||
            receipt_journal.get() < 0 ||
            receipt_tombstone.get() < 0 ||
            ::fstat(
                receipt_maintenance.get(),
                &receipt_maintenance_status) != 0 ||
            ::fstat(
                receipt_journal.get(),
                &receipt_journal_status) != 0 ||
            ::fstat(
                receipt_tombstone.get(),
                &receipt_tombstone_status) != 0 ||
            !SameInode(
                receipt_maintenance_status,
                maintenance_status) ||
            !SameInode(
                receipt_journal_status,
                journal_status) ||
            !SameInode(
                receipt_tombstone_status,
                after_readback) ||
            !MaintenanceStillNamed(
                lease,
                receipt_maintenance.get(),
                &maintenance_status) ||
            !JournalStillNamed(
                lease,
                receipt_journal.get(),
                journal_status) ||
            !NameMatchesDescriptor(
                receipt_maintenance.get(),
                filename.c_str(),
                receipt_tombstone.get(),
                &after_readback) ||
            !ContextReady()) {
            result.error =
                EmptyAnchorTombstoneStoreErrorV1::
                    kReadbackFailure;
            SetDiagnostic(
                diagnostic,
                "cannot retain empty-anchor receipt descriptors");
            return result;
        }

        result.tombstone_sha256 =
            tombstone.tombstone_sha256();
        result.disposition =
            inventory.final_present
                ? EmptyAnchorTombstoneDispositionV1::
                      kAcceptedExistingFinal
                : (adopted
                       ? EmptyAnchorTombstoneDispositionV1::
                             kAdoptedCompleteTemporary
                       : EmptyAnchorTombstoneDispositionV1::
                             kPublishedNew);
        result.receipt =
            std::unique_ptr<
                EmptyAnchorTombstoneReceiptV1>(
                new EmptyAnchorTombstoneReceiptV1(
                    action.key(),
                    action.token(),
                    *receipt_target,
                    action.token()
                        .writer_instance_id,
                    result.tombstone_sha256,
                    model.journal_header_sha256,
                    filename,
                    static_cast<std::uint64_t>(
                        receipt_maintenance_status
                            .st_dev),
                    static_cast<std::uint64_t>(
                        receipt_maintenance_status
                            .st_ino),
                    static_cast<std::uint64_t>(
                        receipt_journal_status.st_dev),
                    static_cast<std::uint64_t>(
                        receipt_journal_status.st_ino),
                    static_cast<std::uint64_t>(
                        receipt_tombstone_status.st_dev),
                    static_cast<std::uint64_t>(
                        receipt_tombstone_status.st_ino),
                    receipt_maintenance.get(),
                    receipt_journal.get(),
                    receipt_tombstone.get()));
        static_cast<void>(
            receipt_maintenance.Release());
        static_cast<void>(receipt_journal.Release());
        static_cast<void>(
            receipt_tombstone.Release());
        result.filename = std::move(filename);
        result.error =
            EmptyAnchorTombstoneStoreErrorV1::kNone;
        SetDiagnostic(diagnostic, {});
        return result;
    } catch (const std::bad_alloc&) {
        result.error =
            EmptyAnchorTombstoneStoreErrorV1::
                kAllocationFailure;
        result.disposition =
            EmptyAnchorTombstoneDispositionV1::kNone;
        result.receipt.reset();
        SetDiagnostic(
            diagnostic,
            "empty-anchor tombstone publication allocation failed");
        return result;
    } catch (...) {
        result.error =
            EmptyAnchorTombstoneStoreErrorV1::
                kAllocationFailure;
        result.disposition =
            EmptyAnchorTombstoneDispositionV1::kNone;
        result.receipt.reset();
        SetDiagnostic(
            diagnostic,
            "empty-anchor tombstone publication failed");
        return result;
    }
}

}  // namespace l2flow::ingress
