#include "l2flow/ingress/raw_sealed_certificate_store.h"

#include "l2flow/common/sha256.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dirent.h>
#include <limits>
#include <map>
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

enum class CurrentTemporaryState : std::uint8_t {
    kAbsent = 0U,
    kComplete,
    kRecognizedPartial,
};

struct CandidateName final {
    std::string name;
    std::string final_name;
    bool temporary = false;
};

struct CandidateContent final {
    LoadedCandidate loaded;
    RawV1Digest sha256{};
    bool recognized_current_partial = false;
};

struct CandidateInventory final {
    std::uint32_t count = 0U;
    bool current_final_present = false;
    CurrentTemporaryState current_temporary =
        CurrentTemporaryState::kAbsent;
    LoadedCandidate current_final;
    LoadedCandidate current_temporary_candidate;
};

struct CandidatePairRecord final {
    bool final_present = false;
    std::uint64_t final_size = 0U;
    RawV1Digest final_sha256{};
    bool temporary_present = false;
    std::uint64_t temporary_size = 0U;
    RawV1Digest temporary_sha256{};
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

[[nodiscard]] bool IsSafeFile(
    const struct stat& status) noexcept {
    return S_ISREG(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           (status.st_mode & 07777U) == 0600U &&
           status.st_nlink == static_cast<nlink_t>(1) &&
           status.st_size >= 0 &&
           static_cast<std::uint64_t>(status.st_size) <=
               kSealedRawCertificateV1MaximumBytes;
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
        completed += static_cast<std::size_t>(result);
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
        "sealed-raw-";
    constexpr std::string_view suffix = ".json";
    constexpr std::size_t expected =
        prefix.size() + 32U + 1U + 64U +
        suffix.size();
    return name.size() == expected &&
           name.substr(0U, prefix.size()) == prefix &&
           name[prefix.size() + 32U] == '-' &&
           name.substr(
               name.size() - suffix.size()) == suffix &&
           IsLowerHex(
               name.substr(prefix.size(), 32U)) &&
           IsLowerHex(
               name.substr(
                   prefix.size() + 33U, 64U));
}

[[nodiscard]] bool IsTemporaryCandidateName(
    std::string_view name) noexcept {
    if (name.size() <=
            1U +
                kSealedRawCertificateV1TemporarySuffix
                    .size() ||
        name.front() != '.' ||
        !name.ends_with(
            kSealedRawCertificateV1TemporarySuffix)) {
        return false;
    }
    const std::size_t final_size =
        name.size() - 1U -
        kSealedRawCertificateV1TemporarySuffix.size();
    return IsFinalCandidateName(
        name.substr(1U, final_size));
}

[[nodiscard]] bool StartsLikeCandidate(
    std::string_view name) noexcept {
    return name.starts_with("sealed-raw-") ||
           name.starts_with(".sealed-raw-");
}

[[nodiscard]] bool Authorize(
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider) noexcept {
    const RawReserveMutationTargetAnchorV1* const target =
        action.target();
    const bool active =
        action.required_status() ==
        ReserveRegistryStatusV1::kActive;
    const bool recovering_terminal =
        action.required_status() ==
            ReserveRegistryStatusV1::kRecovering &&
        action.recovery_intent() ==
            ReserveRecoveryIntentV1::
                kRecoverSealOnly &&
        action.token().recovery_attempt_id ==
            action.key().recovery_attempt_id &&
        !l2flow::common::IsZeroIdentity(
            action.token().writer_instance_id);
    return target != nullptr &&
           (active || recovering_terminal) &&
           ValidateRawReserveMutationTargetProviderV1(
               provider, *target) &&
           action.ValidateLatest();
}

[[nodiscard]] bool MaintenanceStillNamed(
    const RawWriterLease& lease,
    int maintenance_fd) noexcept {
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
               &maintenance);
}

[[nodiscard]] SealedRawCertificateStoreErrorV1
ValidateContext(
    const RawWriterLease& lease,
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider,
    int maintenance_fd) noexcept {
    if (!Authorize(action, provider)) {
        return SealedRawCertificateStoreErrorV1::
            kAuthorizationRejected;
    }
    if (!MaintenanceStillNamed(lease, maintenance_fd)) {
        return SealedRawCertificateStoreErrorV1::
            kUnsafeMaintenanceDirectory;
    }
    return SealedRawCertificateStoreErrorV1::kNone;
}

[[nodiscard]] SealedRawCertificateStoreErrorV1
OpenMaintenance(
    const RawWriterLease& lease,
    ScopedFd* output) noexcept {
    if (output == nullptr) {
        return SealedRawCertificateStoreErrorV1::
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
                   ? SealedRawCertificateStoreErrorV1::
                         kMaintenanceDirectoryMissing
                   : SealedRawCertificateStoreErrorV1::
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
            candidate.get())) {
        return SealedRawCertificateStoreErrorV1::
            kUnsafeMaintenanceDirectory;
    }
    *output = std::move(candidate);
    return SealedRawCertificateStoreErrorV1::kNone;
}

[[nodiscard]] SealedRawCertificateStoreErrorV1
CollectCandidateNames(
    const RawWriterLease& lease,
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider,
    int maintenance_fd,
    std::vector<CandidateName>* output) {
    if (output == nullptr) {
        return SealedRawCertificateStoreErrorV1::
            kInvalidArgument;
    }
    SealedRawCertificateStoreErrorV1 context =
        ValidateContext(
            lease, action, provider, maintenance_fd);
    if (context !=
        SealedRawCertificateStoreErrorV1::kNone) {
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
        !SameInode(expected, actual)) {
        return SealedRawCertificateStoreErrorV1::
            kUnsafeMaintenanceDirectory;
    }
    DIR* const native =
        ::fdopendir(scan_fd.Release());
    if (native == nullptr) {
        return SealedRawCertificateStoreErrorV1::
            kUnsafeMaintenanceDirectory;
    }
    ScopedDir entries(native);
    std::vector<CandidateName> observed;
    observed.reserve(64U);
    bool malformed = false;
    for (;;) {
        errno = 0;
        dirent* const entry = ::readdir(entries.get());
        if (entry == nullptr) {
            if (errno != 0) {
                return SealedRawCertificateStoreErrorV1::
                    kUnsafeMaintenanceDirectory;
            }
            break;
        }
        const std::string_view name(entry->d_name);
        if (name == "." || name == "..") {
            continue;
        }
        const bool final =
            IsFinalCandidateName(name);
        const bool temporary =
            IsTemporaryCandidateName(name);
        if (StartsLikeCandidate(name) &&
            !final && !temporary) {
            malformed = true;
        }
        if (final || temporary) {
            if (observed.size() ==
                kSealedRawCertificateV1MaximumCandidates) {
                return SealedRawCertificateStoreErrorV1::
                    kCandidateLimitExceeded;
            }
            CandidateName candidate{};
            candidate.name.assign(
                name.data(), name.size());
            candidate.temporary = temporary;
            if (temporary) {
                const std::size_t final_size =
                    name.size() - 1U -
                    kSealedRawCertificateV1TemporarySuffix
                        .size();
                candidate.final_name.assign(
                    name.data() + 1U, final_size);
            } else {
                candidate.final_name =
                    candidate.name;
            }
            observed.push_back(
                std::move(candidate));
        }
    }
    context = ValidateContext(
        lease, action, provider, maintenance_fd);
    if (context !=
        SealedRawCertificateStoreErrorV1::kNone) {
        return context;
    }
    if (malformed) {
        return SealedRawCertificateStoreErrorV1::
            kMalformedCandidateName;
    }
    output->swap(observed);
    return SealedRawCertificateStoreErrorV1::kNone;
}

[[nodiscard]] SealedRawCertificateStoreErrorV1
ValidateCandidate(
    const RawWriterLease& lease,
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider,
    int maintenance_fd,
    const CandidateName& candidate_name,
    std::string_view current_final_name,
    std::string_view current_temporary_name,
    std::string_view desired,
    CandidateContent* output) {
    if (output == nullptr) {
        return SealedRawCertificateStoreErrorV1::
            kInvalidArgument;
    }
    SealedRawCertificateStoreErrorV1 context =
        ValidateContext(
            lease, action, provider, maintenance_fd);
    if (context !=
        SealedRawCertificateStoreErrorV1::kNone) {
        return context;
    }
    ScopedFd fd(
        OpenAtNoIntr(
            maintenance_fd,
            candidate_name.name.c_str(),
            O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME));
    if (fd.get() < 0) {
        return errno == ENOENT
                   ? SealedRawCertificateStoreErrorV1::
                         kPublishConflict
                   : SealedRawCertificateStoreErrorV1::
                         kUnsafeCandidate;
    }
    struct stat status {};
    if (::fstat(fd.get(), &status) != 0 ||
        !IsSafeFile(status) ||
        !NameMatchesDescriptor(
            maintenance_fd,
            candidate_name.name.c_str(),
            fd.get(),
            &status)) {
        return SealedRawCertificateStoreErrorV1::
            kUnsafeCandidate;
    }
    std::string bytes;
    if (!ReadExact(
            fd.get(),
            static_cast<std::size_t>(status.st_size),
            &bytes)) {
        return SealedRawCertificateStoreErrorV1::
            kReadbackFailure;
    }
    struct stat after {};
    if (::fstat(fd.get(), &after) != 0 ||
        !IsSafeFile(after) ||
        !SameInode(status, after) ||
        after.st_size != status.st_size ||
        !NameMatchesDescriptor(
            maintenance_fd,
            candidate_name.name.c_str(),
            fd.get(),
            &after)) {
        return SealedRawCertificateStoreErrorV1::
            kUnsafeCandidate;
    }
    context = ValidateContext(
        lease, action, provider, maintenance_fd);
    if (context !=
        SealedRawCertificateStoreErrorV1::kNone) {
        return context;
    }

    SealedRawCertificateV1 parsed{};
    const SealedRawCertificateV1Error parse_error =
        ParseSealedRawCertificateV1Jcs(
            bytes, &parsed);
    bool recognized_partial = false;
    if (parse_error ==
        SealedRawCertificateV1Error::kNone) {
        std::string parsed_filename;
        if (SealedRawCertificateV1Filename(
                parsed, &parsed_filename) !=
                SealedRawCertificateV1Error::kNone ||
            parsed_filename !=
                candidate_name.final_name ||
            ((candidate_name.name ==
                  current_final_name ||
              candidate_name.name ==
                  current_temporary_name) &&
             bytes != desired)) {
            return SealedRawCertificateStoreErrorV1::
                kCandidateConflict;
        }
    } else if (
        candidate_name.temporary &&
        candidate_name.name == current_temporary_name &&
        bytes.size() < desired.size() &&
        desired.substr(0U, bytes.size()) == bytes) {
        recognized_partial = true;
    } else {
        return SealedRawCertificateStoreErrorV1::
            kCandidateConflict;
    }

    CandidateContent candidate{};
    candidate.sha256 =
        l2flow::common::ComputeSha256(bytes);
    candidate.recognized_current_partial =
        recognized_partial;
    candidate.loaded.descriptor = std::move(fd);
    candidate.loaded.status = after;
    candidate.loaded.bytes = std::move(bytes);
    *output = std::move(candidate);
    return SealedRawCertificateStoreErrorV1::kNone;
}

[[nodiscard]] SealedRawCertificateStoreErrorV1
InventoryCandidates(
    const RawWriterLease& lease,
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider,
    int maintenance_fd,
    std::string_view current_final_name,
    std::string_view current_temporary_name,
    std::string_view desired,
    CandidateInventory* output) {
    if (output == nullptr) {
        return SealedRawCertificateStoreErrorV1::
            kInvalidArgument;
    }
    std::vector<CandidateName> names;
    SealedRawCertificateStoreErrorV1 error =
        CollectCandidateNames(
            lease,
            action,
            provider,
            maintenance_fd,
            &names);
    if (error !=
        SealedRawCertificateStoreErrorV1::kNone) {
        return error;
    }

    CandidateInventory inventory{};
    inventory.count =
        static_cast<std::uint32_t>(names.size());
    std::map<std::string, CandidatePairRecord>
        pairs;
    for (const CandidateName& name : names) {
        CandidateContent content{};
        error = ValidateCandidate(
            lease,
            action,
            provider,
            maintenance_fd,
            name,
            current_final_name,
            current_temporary_name,
            desired,
            &content);
        if (error !=
            SealedRawCertificateStoreErrorV1::kNone) {
            return error;
        }

        if (content.recognized_current_partial) {
            inventory.current_temporary =
                CurrentTemporaryState::
                    kRecognizedPartial;
            inventory.current_temporary_candidate =
                std::move(content.loaded);
            continue;
        }

        CandidatePairRecord& pair =
            pairs[name.final_name];
        const std::uint64_t size =
            static_cast<std::uint64_t>(
                content.loaded.bytes.size());
        if (name.temporary) {
            pair.temporary_present = true;
            pair.temporary_size = size;
            pair.temporary_sha256 = content.sha256;
        } else {
            pair.final_present = true;
            pair.final_size = size;
            pair.final_sha256 = content.sha256;
        }
        if (pair.final_present &&
            pair.temporary_present &&
            (pair.final_size != pair.temporary_size ||
             pair.final_sha256 !=
                 pair.temporary_sha256)) {
            return SealedRawCertificateStoreErrorV1::
                kCandidateConflict;
        }

        if (name.name == current_final_name) {
            inventory.current_final_present = true;
            inventory.current_final =
                std::move(content.loaded);
        } else if (
            name.name == current_temporary_name) {
            inventory.current_temporary =
                CurrentTemporaryState::kComplete;
            inventory.current_temporary_candidate =
                std::move(content.loaded);
        }
    }
    *output = std::move(inventory);
    return SealedRawCertificateStoreErrorV1::kNone;
}

[[nodiscard]] SealedRawCertificateStoreErrorV1
RevalidateRetainedCandidate(
    const RawWriterLease& lease,
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider,
    int maintenance_fd,
    const std::string& name,
    LoadedCandidate* candidate) {
    if (candidate == nullptr ||
        candidate->descriptor.get() < 0) {
        return SealedRawCertificateStoreErrorV1::
            kInvalidArgument;
    }
    SealedRawCertificateStoreErrorV1 context =
        ValidateContext(
            lease, action, provider, maintenance_fd);
    if (context !=
        SealedRawCertificateStoreErrorV1::kNone) {
        return context;
    }
    struct stat before {};
    if (::fstat(
            candidate->descriptor.get(),
            &before) != 0 ||
        !IsSafeFile(before) ||
        !SameInode(before, candidate->status) ||
        before.st_size != candidate->status.st_size ||
        !NameMatchesDescriptor(
            maintenance_fd,
            name.c_str(),
            candidate->descriptor.get(),
            &before)) {
        return SealedRawCertificateStoreErrorV1::
            kUnsafeCandidate;
    }
    std::string bytes;
    if (!ReadExact(
            candidate->descriptor.get(),
            static_cast<std::size_t>(before.st_size),
            &bytes)) {
        return SealedRawCertificateStoreErrorV1::
            kReadbackFailure;
    }
    struct stat after {};
    if (::fstat(
            candidate->descriptor.get(),
            &after) != 0 ||
        !IsSafeFile(after) ||
        !SameInode(before, after) ||
        after.st_size != before.st_size ||
        !NameMatchesDescriptor(
            maintenance_fd,
            name.c_str(),
            candidate->descriptor.get(),
            &after)) {
        return SealedRawCertificateStoreErrorV1::
            kUnsafeCandidate;
    }
    context = ValidateContext(
        lease, action, provider, maintenance_fd);
    if (context !=
        SealedRawCertificateStoreErrorV1::kNone) {
        return context;
    }
    if (bytes != candidate->bytes) {
        return SealedRawCertificateStoreErrorV1::
            kCandidateConflict;
    }
    candidate->status = after;
    return SealedRawCertificateStoreErrorV1::kNone;
}

[[nodiscard]] bool WriteExactAuthorized(
    const RawWriterLease& lease,
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider,
    int maintenance_fd,
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
                maintenance_fd) !=
                SealedRawCertificateStoreErrorV1::kNone ||
            ::fstat(fd, &status) != 0 ||
            !IsSafeFile(status) ||
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
               maintenance_fd) ==
               SealedRawCertificateStoreErrorV1::kNone &&
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
    return ::fstatat(
               directory_fd,
               name,
               &status,
               AT_SYMLINK_NOFOLLOW) != 0 &&
           errno == ENOENT;
}

}  // namespace

std::string_view
SealedRawCertificateStoreErrorV1Name(
    SealedRawCertificateStoreErrorV1 error) noexcept {
    switch (error) {
    case SealedRawCertificateStoreErrorV1::kNone:
        return "none";
    case SealedRawCertificateStoreErrorV1::
        kInvalidArgument:
        return "invalid_argument";
    case SealedRawCertificateStoreErrorV1::
        kAuthorizationRejected:
        return "authorization_rejected";
    case SealedRawCertificateStoreErrorV1::
        kTargetMismatch:
        return "target_mismatch";
    case SealedRawCertificateStoreErrorV1::
        kMaintenanceDirectoryMissing:
        return "maintenance_directory_missing";
    case SealedRawCertificateStoreErrorV1::
        kUnsafeMaintenanceDirectory:
        return "unsafe_maintenance_directory";
    case SealedRawCertificateStoreErrorV1::
        kCandidateLimitExceeded:
        return "candidate_limit_exceeded";
    case SealedRawCertificateStoreErrorV1::
        kMalformedCandidateName:
        return "malformed_candidate_name";
    case SealedRawCertificateStoreErrorV1::
        kUnsafeCandidate:
        return "unsafe_candidate";
    case SealedRawCertificateStoreErrorV1::
        kCandidateConflict:
        return "candidate_conflict";
    case SealedRawCertificateStoreErrorV1::
        kTemporaryCreate:
        return "temporary_create";
    case SealedRawCertificateStoreErrorV1::
        kWriteFailure:
        return "write_failure";
    case SealedRawCertificateStoreErrorV1::
        kSyncFailure:
        return "sync_failure";
    case SealedRawCertificateStoreErrorV1::
        kPublishConflict:
        return "publish_conflict";
    case SealedRawCertificateStoreErrorV1::
        kReadbackFailure:
        return "readback_failure";
    case SealedRawCertificateStoreErrorV1::
        kAllocationFailure:
        return "allocation_failure";
    }
    return "unknown";
}

SealedRawCertificatePublishResultV1
PublishSealedRawCertificateV1(
    const RawWriterLease& lease,
    std::unique_ptr<RawReserveAuthorizedActionV1>
        authorized_action,
    const BuiltSealedRawCertificateV1& certificate,
    std::string* diagnostic) noexcept {
    SealedRawCertificatePublishResultV1 result{};
    SetDiagnostic(diagnostic, {});
    try {
        if (authorized_action == nullptr) {
            result.error =
                SealedRawCertificateStoreErrorV1::
                    kInvalidArgument;
            SetDiagnostic(
                diagnostic,
                "sealed certificate action is missing");
            return result;
        }
        RawReserveAuthorizedActionV1& action =
            *authorized_action;
        const bool recovering_terminal =
            action.required_status() ==
            ReserveRegistryStatusV1::kRecovering;
        const SealedRawCertificateV1& model =
            certificate.model();
        const std::string desired(
            certificate.canonical_jcs());
        std::string filename(certificate.filename());
        if (lease.descriptor() < 0 ||
            lease.directory_descriptor() < 0 ||
            lease.source_stream_id() !=
                model.namespace_identity
                    .source_stream_id ||
            lease.capture_date() !=
                model.namespace_identity
                    .capture_date ||
            desired.empty() ||
            desired.size() >
                kSealedRawCertificateV1MaximumBytes ||
            !IsFinalCandidateName(filename) ||
            ValidateSealedRawCertificateV1(model) !=
                SealedRawCertificateV1Error::kNone ||
            l2flow::common::ComputeSha256(desired) !=
                certificate.certificate_sha256()) {
            result.error =
                SealedRawCertificateStoreErrorV1::
                    kInvalidArgument;
            SetDiagnostic(
                diagnostic,
                "sealed Raw certificate input is invalid");
            return result;
        }
        const RawReserveRegistryEntryKeyV1& key =
            action.key();
        if (key.route.source_stream_id !=
                model.namespace_identity
                    .source_stream_id ||
            key.route.capture_date !=
                model.namespace_identity
                    .capture_date ||
            key.stream_day_id !=
                model.namespace_identity
                    .stream_day_id ||
            (recovering_terminal &&
             (action.recovery_intent() !=
                      ReserveRecoveryIntentV1::
                          kRecoverSealOnly ||
              action.token().recovery_attempt_id !=
                  key.recovery_attempt_id ||
              l2flow::common::IsZeroIdentity(
                  action.token()
                      .writer_instance_id))) ||
            (!recovering_terminal &&
             action.required_status() !=
                 ReserveRegistryStatusV1::kActive)) {
            result.error =
                SealedRawCertificateStoreErrorV1::
                    kAuthorizationRejected;
            SetDiagnostic(
                diagnostic,
                "certificate action status or namespace is invalid");
            return result;
        }
        LeaseTargetProvider provider(lease);
        const RawReserveMutationTargetAnchorV1* const
            target = action.target();
        if (target == nullptr ||
            !ValidateRawReserveMutationTargetProviderV1(
                provider, *target)) {
            result.error =
                SealedRawCertificateStoreErrorV1::
                    kTargetMismatch;
            SetDiagnostic(
                diagnostic,
                "certificate route target does not match the capability");
            return result;
        }
        if (!Authorize(action, provider)) {
            result.error =
                SealedRawCertificateStoreErrorV1::
                    kAuthorizationRejected;
            SetDiagnostic(
                diagnostic,
                "sealed certificate capability is stale");
            return result;
        }

        ScopedFd maintenance;
        result.error =
            OpenMaintenance(lease, &maintenance);
        if (result.error !=
            SealedRawCertificateStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "cannot open the pre-existing maintenance directory");
            return result;
        }
        if (!LockExclusiveNoIntr(maintenance.get())) {
            result.error =
                SealedRawCertificateStoreErrorV1::
                    kUnsafeMaintenanceDirectory;
            SetDiagnostic(
                diagnostic,
                "cannot serialize sealed certificate maintenance");
            return result;
        }
        result.error = ValidateContext(
            lease,
            action,
            provider,
            maintenance.get());
        if (result.error !=
            SealedRawCertificateStoreErrorV1::kNone) {
            return result;
        }
        const std::string temporary =
            "." + filename +
            std::string(
                kSealedRawCertificateV1TemporarySuffix);
        CandidateInventory inventory{};
        result.error = InventoryCandidates(
            lease,
            action,
            provider,
            maintenance.get(),
            filename,
            temporary,
            desired,
            &inventory);
        result.observed_candidate_count =
            inventory.count;
        if (result.error !=
            SealedRawCertificateStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "sealed certificate candidate inventory is invalid");
            return result;
        }

        const bool final_present =
            inventory.current_final_present;
        const bool temporary_complete =
            inventory.current_temporary ==
            CurrentTemporaryState::kComplete;
        const bool temporary_partial =
            inventory.current_temporary ==
            CurrentTemporaryState::kRecognizedPartial;
        const std::uint32_t effective_count =
            inventory.count -
            (temporary_partial ? 1U : 0U);
        const std::uint32_t required_new_names =
            final_present
                ? 0U
                : (temporary_complete ? 1U : 2U);
        if (effective_count >
            kSealedRawCertificateV1MaximumCandidates -
                required_new_names) {
            result.error =
                SealedRawCertificateStoreErrorV1::
                    kCandidateLimitExceeded;
            SetDiagnostic(
                diagnostic,
                "sealed certificate candidate admission has no bounded crash space");
            return result;
        }

        LoadedCandidate final =
            std::move(inventory.current_final);
        LoadedCandidate temporary_candidate =
            std::move(
                inventory.current_temporary_candidate);

        const auto ContextReady = [&]() noexcept {
            result.error = ValidateContext(
                lease,
                action,
                provider,
                maintenance.get());
            return result.error ==
                   SealedRawCertificateStoreErrorV1::
                       kNone;
        };
        if (!ContextReady()) {
            return result;
        }
        if (final_present) {
            if (!NameMatchesDescriptor(
                    maintenance.get(),
                    filename.c_str(),
                    final.descriptor.get(),
                    &final.status)) {
                result.error =
                    SealedRawCertificateStoreErrorV1::
                        kPublishConflict;
                return result;
            }
        } else if (!NameAbsent(
                       maintenance.get(),
                       filename.c_str())) {
            result.error =
                SealedRawCertificateStoreErrorV1::
                    kPublishConflict;
            return result;
        }
        if (temporary_complete || temporary_partial) {
            if (!NameMatchesDescriptor(
                    maintenance.get(),
                    temporary.c_str(),
                    temporary_candidate.descriptor.get(),
                    &temporary_candidate.status)) {
                result.error =
                    SealedRawCertificateStoreErrorV1::
                        kPublishConflict;
                return result;
            }
        } else if (!NameAbsent(
                       maintenance.get(),
                       temporary.c_str())) {
            result.error =
                SealedRawCertificateStoreErrorV1::
                    kPublishConflict;
            return result;
        }

        bool adopted = false;
        bool cleaned = false;
        bool recovered_partial = false;
        if (temporary_partial) {
            result.error =
                RevalidateRetainedCandidate(
                    lease,
                    action,
                    provider,
                    maintenance.get(),
                    temporary,
                    &temporary_candidate);
            if (result.error !=
                SealedRawCertificateStoreErrorV1::
                    kNone) {
                return result;
            }
            if (::unlinkat(
                    maintenance.get(),
                    temporary.c_str(),
                    0) != 0) {
                result.error =
                    SealedRawCertificateStoreErrorV1::
                        kPublishConflict;
                return result;
            }
            if (!ContextReady()) {
                return result;
            }
            if (!NameAbsent(
                    maintenance.get(),
                    temporary.c_str())) {
                result.error =
                    SealedRawCertificateStoreErrorV1::
                        kPublishConflict;
                return result;
            }
            if (!FsyncNoIntr(maintenance.get())) {
                result.error =
                    SealedRawCertificateStoreErrorV1::
                        kSyncFailure;
                return result;
            }
            if (!ContextReady()) {
                return result;
            }
            recovered_partial = true;
            result.directory_synced = true;
            temporary_candidate = LoadedCandidate{};
        }

        if (final_present) {
            result.error =
                RevalidateRetainedCandidate(
                    lease,
                    action,
                    provider,
                    maintenance.get(),
                    filename,
                    &final);
            if (result.error !=
                SealedRawCertificateStoreErrorV1::
                    kNone) {
                return result;
            }
            if (!FsyncNoIntr(final.descriptor.get())) {
                result.error =
                    SealedRawCertificateStoreErrorV1::
                        kSyncFailure;
                return result;
            }
            if (!ContextReady() ||
                !NameMatchesDescriptor(
                    maintenance.get(),
                    filename.c_str(),
                    final.descriptor.get())) {
                if (result.error ==
                    SealedRawCertificateStoreErrorV1::
                        kNone) {
                    result.error =
                        SealedRawCertificateStoreErrorV1::
                            kPublishConflict;
                }
                return result;
            }
            result.file_synced = true;
            if (temporary_complete) {
                result.error =
                    RevalidateRetainedCandidate(
                        lease,
                        action,
                        provider,
                        maintenance.get(),
                        temporary,
                        &temporary_candidate);
                if (result.error !=
                    SealedRawCertificateStoreErrorV1::
                        kNone) {
                    return result;
                }
                if (::unlinkat(
                        maintenance.get(),
                        temporary.c_str(),
                        0) != 0) {
                    result.error =
                        SealedRawCertificateStoreErrorV1::
                            kPublishConflict;
                    return result;
                }
                if (!ContextReady()) {
                    return result;
                }
                if (!NameAbsent(
                        maintenance.get(),
                        temporary.c_str())) {
                    result.error =
                        SealedRawCertificateStoreErrorV1::
                            kPublishConflict;
                    return result;
                }
                if (!FsyncNoIntr(maintenance.get())) {
                    result.error =
                        SealedRawCertificateStoreErrorV1::
                            kSyncFailure;
                    return result;
                }
                if (!ContextReady()) {
                    return result;
                }
                cleaned = true;
                result.directory_synced = true;
            }
        } else {
            if (!temporary_complete) {
                if (!ContextReady()) {
                    return result;
                }
                if (!NameAbsent(
                        maintenance.get(),
                        filename.c_str()) ||
                    !NameAbsent(
                        maintenance.get(),
                        temporary.c_str())) {
                    result.error =
                        SealedRawCertificateStoreErrorV1::
                            kPublishConflict;
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
                struct stat status {};
                if (created.get() < 0) {
                    result.error =
                        SealedRawCertificateStoreErrorV1::
                            kTemporaryCreate;
                    return result;
                }
                if (!ContextReady()) {
                    return result;
                }
                if (::fstat(created.get(), &status) != 0 ||
                    !S_ISREG(status.st_mode) ||
                    status.st_uid != ::geteuid() ||
                    status.st_nlink !=
                        static_cast<nlink_t>(1) ||
                    status.st_size != 0 ||
                    !NameMatchesDescriptor(
                        maintenance.get(),
                        temporary.c_str(),
                        created.get(),
                        &status)) {
                    result.error =
                        SealedRawCertificateStoreErrorV1::
                            kWriteFailure;
                    return result;
                }
                if (!ContextReady() ||
                    ::fchmod(created.get(), 0600) != 0) {
                    if (result.error ==
                        SealedRawCertificateStoreErrorV1::
                            kNone) {
                        result.error =
                            SealedRawCertificateStoreErrorV1::
                                kWriteFailure;
                    }
                    return result;
                }
                if (!ContextReady() ||
                    !WriteExactAuthorized(
                        lease,
                        action,
                        provider,
                        maintenance.get(),
                        temporary.c_str(),
                        created.get(),
                        desired)) {
                    if (result.error ==
                        SealedRawCertificateStoreErrorV1::
                            kNone) {
                        result.error = ValidateContext(
                            lease,
                            action,
                            provider,
                            maintenance.get());
                        if (result.error ==
                            SealedRawCertificateStoreErrorV1::
                                kNone) {
                            result.error =
                                SealedRawCertificateStoreErrorV1::
                                    kWriteFailure;
                        }
                    }
                    return result;
                }
                if (::fstat(created.get(), &status) != 0 ||
                    !IsSafeFile(status) ||
                    status.st_size !=
                        static_cast<off_t>(
                            desired.size()) ||
                    !NameMatchesDescriptor(
                        maintenance.get(),
                        temporary.c_str(),
                        created.get(),
                        &status) ||
                    !ContextReady()) {
                    if (result.error ==
                        SealedRawCertificateStoreErrorV1::
                            kNone) {
                        result.error =
                            SealedRawCertificateStoreErrorV1::
                                kWriteFailure;
                    }
                    return result;
                }
                temporary_candidate.descriptor =
                    std::move(created);
                temporary_candidate.status = status;
                temporary_candidate.bytes = desired;
            } else {
                adopted = true;
            }
            result.error =
                RevalidateRetainedCandidate(
                    lease,
                    action,
                    provider,
                    maintenance.get(),
                    temporary,
                    &temporary_candidate);
            if (result.error !=
                SealedRawCertificateStoreErrorV1::
                    kNone) {
                return result;
            }
            if (!FsyncNoIntr(
                    temporary_candidate
                        .descriptor.get())) {
                result.error =
                    SealedRawCertificateStoreErrorV1::
                        kSyncFailure;
                return result;
            }
            if (!ContextReady() ||
                !NameMatchesDescriptor(
                    maintenance.get(),
                    temporary.c_str(),
                    temporary_candidate.descriptor.get())) {
                if (result.error ==
                    SealedRawCertificateStoreErrorV1::
                        kNone) {
                    result.error =
                        SealedRawCertificateStoreErrorV1::
                            kPublishConflict;
                }
                return result;
            }
            result.file_synced = true;
            if (!NameAbsent(
                    maintenance.get(),
                    filename.c_str())) {
                result.error =
                    SealedRawCertificateStoreErrorV1::
                        kPublishConflict;
                return result;
            }
            if (!ContextReady() ||
                !RenameNoReplace(
                    maintenance.get(),
                    temporary.c_str(),
                    filename.c_str())) {
                if (result.error ==
                    SealedRawCertificateStoreErrorV1::
                        kNone) {
                    result.error =
                        SealedRawCertificateStoreErrorV1::
                            kPublishConflict;
                }
                return result;
            }
            if (!ContextReady() ||
                !NameMatchesDescriptor(
                    maintenance.get(),
                    filename.c_str(),
                    temporary_candidate
                        .descriptor.get()) ||
                !NameAbsent(
                    maintenance.get(),
                    temporary.c_str())) {
                if (result.error ==
                    SealedRawCertificateStoreErrorV1::
                        kNone) {
                    result.error =
                        SealedRawCertificateStoreErrorV1::
                            kPublishConflict;
                }
                return result;
            }
            if (!FsyncNoIntr(maintenance.get())) {
                result.error =
                    SealedRawCertificateStoreErrorV1::
                        kSyncFailure;
                return result;
            }
            if (!ContextReady()) {
                return result;
            }
            result.directory_synced = true;
            final = std::move(temporary_candidate);
        }

        if (!result.directory_synced) {
            if (!ContextReady()) {
                return result;
            }
            if (!FsyncNoIntr(maintenance.get())) {
                result.error =
                    SealedRawCertificateStoreErrorV1::
                        kSyncFailure;
                return result;
            }
            if (!ContextReady()) {
                return result;
            }
            result.directory_synced = true;
        }
        if (!ContextReady() ||
            !NameMatchesDescriptor(
                maintenance.get(),
                filename.c_str(),
                final.descriptor.get()) ||
            final.bytes != desired) {
            if (result.error ==
                SealedRawCertificateStoreErrorV1::
                    kNone) {
                result.error =
                    SealedRawCertificateStoreErrorV1::
                        kReadbackFailure;
            }
            return result;
        }
        std::string readback;
        struct stat final_status {};
        struct stat after_readback {};
        if (::fstat(
                final.descriptor.get(),
                &final_status) != 0 ||
            !IsSafeFile(final_status) ||
            static_cast<std::uint64_t>(
                final_status.st_size) !=
                desired.size() ||
            !ReadExact(
                final.descriptor.get(),
                desired.size(),
                &readback) ||
            readback != desired ||
            ::fstat(
                final.descriptor.get(),
                &after_readback) != 0 ||
            !IsSafeFile(after_readback) ||
            !SameInode(final_status, after_readback) ||
            after_readback.st_size !=
                final_status.st_size ||
            !NameMatchesDescriptor(
                maintenance.get(),
                filename.c_str(),
                final.descriptor.get(),
                &after_readback) ||
            !ContextReady()) {
            if (result.error !=
                SealedRawCertificateStoreErrorV1::
                    kNone) {
                return result;
            }
            result.error =
                SealedRawCertificateStoreErrorV1::
                    kReadbackFailure;
            return result;
        }
        result.error =
            SealedRawCertificateStoreErrorV1::kNone;
        result.disposition =
            final_present
                ? (recovered_partial
                       ? SealedRawCertificateDispositionV1::
                             kAcceptedExistingAndCleanedRecognizedPartialTemporary
                       : (cleaned
                              ? SealedRawCertificateDispositionV1::
                                    kAcceptedExistingAndCleanedIdenticalTemporary
                              : SealedRawCertificateDispositionV1::
                                    kAcceptedExistingFinal))
                : (recovered_partial
                       ? SealedRawCertificateDispositionV1::
                             kRebuiltRecognizedPartialTemporary
                       : (adopted
                              ? SealedRawCertificateDispositionV1::
                                    kAdoptedCompleteTemporary
                              : SealedRawCertificateDispositionV1::
                                    kPublishedNew));
        result.certificate_sha256 =
            certificate.certificate_sha256();
        if (!ContextReady() ||
            !NameMatchesDescriptor(
                maintenance.get(),
                filename.c_str(),
                final.descriptor.get(),
                &after_readback)) {
            if (result.error ==
                SealedRawCertificateStoreErrorV1::
                    kNone) {
                result.error =
                    SealedRawCertificateStoreErrorV1::
                        kReadbackFailure;
            }
            result.disposition =
                SealedRawCertificateDispositionV1::kNone;
            return result;
        }
        const RawReserveMutationTargetAnchorV1* const
            receipt_target = action.target();
        if (receipt_target != nullptr) {
            ScopedFd receipt_maintenance(
                OpenAtNoIntr(
                    maintenance.get(),
                    ".",
                    O_RDONLY | O_DIRECTORY |
                        O_NOFOLLOW | O_NONBLOCK |
                        O_CLOEXEC | O_NOATIME));
            ScopedFd receipt_certificate(
                DuplicateFd(final.descriptor.get()));
            struct stat receipt_maintenance_status {};
            struct stat receipt_certificate_status {};
            if (receipt_maintenance.get() < 0 ||
                receipt_certificate.get() < 0 ||
                ::fstat(
                    receipt_maintenance.get(),
                    &receipt_maintenance_status) != 0 ||
                ::fstat(
                    receipt_certificate.get(),
                    &receipt_certificate_status) != 0 ||
                !IsSafeDirectory(
                    receipt_maintenance_status) ||
                !IsSafeFile(
                    receipt_certificate_status) ||
                !SameInode(
                    receipt_certificate_status,
                    after_readback) ||
                !MaintenanceStillNamed(
                    lease,
                    receipt_maintenance.get()) ||
                !ContextReady() ||
                !NameMatchesDescriptor(
                    receipt_maintenance.get(),
                    filename.c_str(),
                    receipt_certificate.get(),
                    &after_readback)) {
                result.error =
                    result.error ==
                            SealedRawCertificateStoreErrorV1::
                                kNone
                        ? SealedRawCertificateStoreErrorV1::
                              kReadbackFailure
                        : result.error;
                result.disposition =
                    SealedRawCertificateDispositionV1::
                        kNone;
                SetDiagnostic(
                    diagnostic,
                    "cannot retain sealed Raw certificate receipt descriptors");
                return result;
            }
            if (recovering_terminal) {
                result.recovery_terminal_receipt =
                    std::unique_ptr<
                        SealedRawRecoveryTerminalReceiptV1>(
                        new SealedRawRecoveryTerminalReceiptV1(
                            action.key(),
                            action.token(),
                            *receipt_target,
                            action.token()
                                .writer_instance_id,
                            result.certificate_sha256,
                            filename,
                            static_cast<std::uint64_t>(
                                receipt_maintenance_status
                                    .st_dev),
                            static_cast<std::uint64_t>(
                                receipt_maintenance_status
                                    .st_ino),
                            static_cast<std::uint64_t>(
                                receipt_certificate_status
                                    .st_dev),
                            static_cast<std::uint64_t>(
                                receipt_certificate_status
                                    .st_ino),
                            receipt_maintenance.get(),
                            receipt_certificate.get()));
            } else {
                result.unregister_receipt =
                    std::unique_ptr<
                        SealedRawCertificateReceiptV1>(
                        new SealedRawCertificateReceiptV1(
                            action.key(),
                            action.token(),
                            *receipt_target,
                            result.certificate_sha256,
                            filename,
                            receipt_maintenance.get(),
                            receipt_certificate.get()));
            }
            static_cast<void>(
                receipt_maintenance.Release());
            static_cast<void>(
                receipt_certificate.Release());
        }
        if ((recovering_terminal &&
             result.recovery_terminal_receipt ==
                 nullptr) ||
            (!recovering_terminal &&
             result.unregister_receipt == nullptr)) {
            result.error =
                SealedRawCertificateStoreErrorV1::
                    kAllocationFailure;
            result.disposition =
                SealedRawCertificateDispositionV1::kNone;
            SetDiagnostic(
                diagnostic,
                "cannot allocate sealed Raw certificate receipt");
            return result;
        }
        result.filename = std::move(filename);
        SetDiagnostic(diagnostic, {});
        return result;
    } catch (const std::bad_alloc&) {
        result.error =
            SealedRawCertificateStoreErrorV1::
                kAllocationFailure;
        SetDiagnostic(
            diagnostic,
            "sealed certificate publication allocation failed");
        return result;
    } catch (...) {
        result.error =
            SealedRawCertificateStoreErrorV1::
                kAllocationFailure;
        SetDiagnostic(
            diagnostic,
            "sealed certificate publication failed");
        return result;
    }
}

}  // namespace l2flow::ingress
