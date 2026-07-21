#include "l2flow/ingress/raw_recovery_maintenance_report_store.h"

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_namespace.h"
#include "l2flow/ingress/raw_schema.h"

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

class RecoveryMaintenanceReportStoreAccessV1 final {
public:
    struct SealedView final {
        const RawReserveRegistryEntryKeyV1* key = nullptr;
        const RawReserveGenerationActionTokenV1* token =
            nullptr;
        const RawReserveMutationTargetAnchorV1* target =
            nullptr;
        const RawV1Identity* writer_instance = nullptr;
        const RawV1Digest* sidecar_sha256 = nullptr;
        const std::string* filename = nullptr;
        std::uint64_t maintenance_device = 0U;
        std::uint64_t maintenance_inode = 0U;
        std::uint64_t sidecar_device = 0U;
        std::uint64_t sidecar_inode = 0U;
        int maintenance_fd = -1;
        int sidecar_fd = -1;
    };

    struct EmptyView final {
        const RawReserveRegistryEntryKeyV1* key = nullptr;
        const RawReserveGenerationActionTokenV1* token =
            nullptr;
        const RawReserveMutationTargetAnchorV1* target =
            nullptr;
        const RawV1Identity* writer_instance = nullptr;
        const RawV1Digest* sidecar_sha256 = nullptr;
        const RawV1Digest* journal_header_sha256 =
            nullptr;
        const std::string* filename = nullptr;
        std::uint64_t maintenance_device = 0U;
        std::uint64_t maintenance_inode = 0U;
        std::uint64_t journal_device = 0U;
        std::uint64_t journal_inode = 0U;
        std::uint64_t sidecar_device = 0U;
        std::uint64_t sidecar_inode = 0U;
        int maintenance_fd = -1;
        int journal_fd = -1;
        int sidecar_fd = -1;
    };

    [[nodiscard]] static bool Consume(
        SealedRawRecoveryTerminalReceiptV1* receipt,
        SealedView* output) noexcept {
        if (receipt == nullptr || output == nullptr ||
            receipt->consumed_ ||
            receipt->filename_.empty()) {
            return false;
        }
        receipt->consumed_ = true;
        output->key = &receipt->key_;
        output->token = &receipt->token_;
        output->target = &receipt->target_;
        output->writer_instance =
            &receipt->writer_instance_;
        output->sidecar_sha256 =
            &receipt->certificate_sha256_;
        output->filename = &receipt->filename_;
        output->maintenance_device =
            receipt->maintenance_device_;
        output->maintenance_inode =
            receipt->maintenance_inode_;
        output->sidecar_device =
            receipt->certificate_device_;
        output->sidecar_inode =
            receipt->certificate_inode_;
        output->maintenance_fd =
            receipt->maintenance_directory_fd_;
        output->sidecar_fd = receipt->certificate_fd_;
        return true;
    }

    [[nodiscard]] static bool Consume(
        EmptyAnchorTombstoneReceiptV1* receipt,
        EmptyView* output) noexcept {
        if (receipt == nullptr || output == nullptr ||
            receipt->consumed_ ||
            receipt->filename_.empty()) {
            return false;
        }
        receipt->consumed_ = true;
        output->key = &receipt->key_;
        output->token = &receipt->token_;
        output->target = &receipt->target_;
        output->writer_instance =
            &receipt->writer_instance_;
        output->sidecar_sha256 =
            &receipt->tombstone_sha256_;
        output->journal_header_sha256 =
            &receipt->journal_header_sha256_;
        output->filename = &receipt->filename_;
        output->maintenance_device =
            receipt->maintenance_device_;
        output->maintenance_inode =
            receipt->maintenance_inode_;
        output->journal_device =
            receipt->journal_device_;
        output->journal_inode =
            receipt->journal_inode_;
        output->sidecar_device =
            receipt->tombstone_device_;
        output->sidecar_inode =
            receipt->tombstone_inode_;
        output->maintenance_fd =
            receipt->maintenance_directory_fd_;
        output->journal_fd = receipt->journal_fd_;
        output->sidecar_fd = receipt->tombstone_fd_;
        return true;
    }

    [[nodiscard]] static std::unique_ptr<
        RecoveryMaintenanceReportReceiptV1>
    MakeActivationReceipt(
        RawReserveRegistryEntryKeyV1 key,
        RawReserveGenerationActionTokenV1 token,
        RawReserveMutationTargetAnchorV1 target,
        RawV1Identity writer_instance,
        RawV1Digest report_sha256,
        std::string filename,
        std::uint64_t closed_entry_count,
        RawV1Digest closed_prefix_sha256,
        std::uint64_t manifest_generation,
        RawV1Digest manifest_entry_commitment_sha256,
        std::uint32_t segment_sequence,
        RawV1Digest segment_header_sha256,
        RawV1DurableMarkerWire endpoint_marker_bytes,
        RawV1Digest endpoint_marker_sha256,
        RecoveryMaintenanceCursorV1 control_cursor,
        int maintenance_directory_fd,
        int report_fd) {
        return std::unique_ptr<
            RecoveryMaintenanceReportReceiptV1>(
            new RecoveryMaintenanceReportReceiptV1(
                std::move(key),
                std::move(token),
                std::move(target),
                writer_instance,
                report_sha256,
                std::move(filename),
                closed_entry_count,
                closed_prefix_sha256,
                manifest_generation,
                manifest_entry_commitment_sha256,
                segment_sequence,
                segment_header_sha256,
                endpoint_marker_bytes,
                endpoint_marker_sha256,
                control_cursor,
                maintenance_directory_fd,
                report_fd));
    }

    [[nodiscard]] static std::unique_ptr<
        RecoveryTerminalReportReceiptV1>
    MakeTerminalReceipt(
        RawReserveRegistryEntryKeyV1 key,
        RawReserveGenerationActionTokenV1 token,
        RawReserveMutationTargetAnchorV1 target,
        RawV1Identity writer_instance,
        RecoveryMaintenanceResultV1 result,
        RawV1Digest report_sha256,
        std::string report_filename,
        RawV1Digest sidecar_sha256,
        std::string sidecar_filename,
        RawV1Digest journal_header_sha256,
        RecoveryMaintenanceReportV1 report_model,
        RawV1JournalHeaderWire journal_header_bytes,
        const struct stat& maintenance,
        const struct stat& journal,
        const struct stat& sidecar,
        const struct stat& report,
        int maintenance_fd,
        int journal_fd,
        int sidecar_fd,
        int report_fd) {
        return std::unique_ptr<
            RecoveryTerminalReportReceiptV1>(
            new RecoveryTerminalReportReceiptV1(
                std::move(key),
                std::move(token),
                std::move(target),
                writer_instance,
                result,
                report_sha256,
                std::move(report_filename),
                sidecar_sha256,
                std::move(sidecar_filename),
                journal_header_sha256,
                std::move(report_model),
                journal_header_bytes,
                static_cast<std::uint64_t>(
                    maintenance.st_dev),
                static_cast<std::uint64_t>(
                    maintenance.st_ino),
                static_cast<std::uint64_t>(
                    journal.st_dev),
                static_cast<std::uint64_t>(
                    journal.st_ino),
                static_cast<std::uint64_t>(
                    sidecar.st_dev),
                static_cast<std::uint64_t>(
                    sidecar.st_ino),
                static_cast<std::uint64_t>(
                    report.st_dev),
                static_cast<std::uint64_t>(
                    report.st_ino),
                maintenance_fd,
                journal_fd,
                sidecar_fd,
                report_fd));
    }
};

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

struct TerminalSidecarEvidence final {
    ScopedFd maintenance;
    ScopedFd journal;
    ScopedFd sidecar;
    struct stat maintenance_status {};
    struct stat journal_status {};
    struct stat sidecar_status {};
    RawV1Digest sidecar_sha256{};
    std::string sidecar_filename;
    RawV1JournalHeaderWire journal_header_bytes{};
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
               kRecoveryMaintenanceReportV1MaximumBytes;
}

[[nodiscard]] bool IsSafeEvidenceFile(
    const struct stat& status,
    std::uint64_t maximum_bytes,
    bool require_nonempty = true) noexcept {
    return S_ISREG(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           (status.st_mode & 07777U) == 0600U &&
           status.st_nlink == static_cast<nlink_t>(1) &&
           status.st_size >=
               static_cast<off_t>(
                   require_nonempty ? 1U : 0U) &&
           static_cast<std::uint64_t>(status.st_size) <=
               maximum_bytes;
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
        "recovery-";
    constexpr std::string_view suffix = ".json";
    constexpr std::size_t expected =
        prefix.size() + 32U + suffix.size();
    return name.size() == expected &&
           name.substr(0U, prefix.size()) == prefix &&
           name.substr(
               name.size() - suffix.size()) == suffix &&
           IsLowerHex(
               name.substr(prefix.size(), 32U));
}

[[nodiscard]] bool IsSealedSidecarName(
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

[[nodiscard]] bool IsEmptySidecarName(
    std::string_view name) noexcept {
    constexpr std::string_view prefix =
        "empty-anchor-";
    constexpr std::string_view suffix = ".json";
    constexpr std::size_t expected =
        prefix.size() + 32U + suffix.size();
    return name.size() == expected &&
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
                kRecoveryMaintenanceReportV1TemporarySuffix
                    .size() ||
        name.front() != '.' ||
        !name.ends_with(
            kRecoveryMaintenanceReportV1TemporarySuffix)) {
        return false;
    }
    const std::size_t final_size =
        name.size() - 1U -
        kRecoveryMaintenanceReportV1TemporarySuffix.size();
    return IsFinalCandidateName(
        name.substr(1U, final_size));
}

[[nodiscard]] bool StartsLikeCandidate(
    std::string_view name) noexcept {
    return name.starts_with("recovery-") ||
           name.starts_with(".recovery-");
}

[[nodiscard]] bool Authorize(
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider) noexcept {
    const RawReserveMutationTargetAnchorV1* const target =
        action.target();
    const ReserveRecoveryIntentV1 intent =
        action.recovery_intent();
    return target != nullptr &&
           action.required_status() ==
               ReserveRegistryStatusV1::kRecovering &&
           (intent ==
                ReserveRecoveryIntentV1::kResumeConnect ||
            intent ==
                ReserveRecoveryIntentV1::
                    kRecoverSealOnly) &&
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

[[nodiscard]] RecoveryMaintenanceReportStoreErrorV1
ValidateContext(
    const RawWriterLease& lease,
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider,
    int maintenance_fd) noexcept {
    if (!Authorize(action, provider)) {
        return RecoveryMaintenanceReportStoreErrorV1::
            kAuthorizationRejected;
    }
    if (!MaintenanceStillNamed(lease, maintenance_fd)) {
        return RecoveryMaintenanceReportStoreErrorV1::
            kUnsafeMaintenanceDirectory;
    }
    return RecoveryMaintenanceReportStoreErrorV1::kNone;
}

[[nodiscard]] RecoveryMaintenanceReportStoreErrorV1
OpenMaintenance(
    const RawWriterLease& lease,
    ScopedFd* output) noexcept {
    if (output == nullptr) {
        return RecoveryMaintenanceReportStoreErrorV1::
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
                   ? RecoveryMaintenanceReportStoreErrorV1::
                         kMaintenanceDirectoryMissing
                   : RecoveryMaintenanceReportStoreErrorV1::
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
        return RecoveryMaintenanceReportStoreErrorV1::
            kUnsafeMaintenanceDirectory;
    }
    *output = std::move(candidate);
    return RecoveryMaintenanceReportStoreErrorV1::kNone;
}

[[nodiscard]] RecoveryMaintenanceReportStoreErrorV1
CollectCandidateNames(
    const RawWriterLease& lease,
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider,
    int maintenance_fd,
    std::vector<CandidateName>* output) {
    if (output == nullptr) {
        return RecoveryMaintenanceReportStoreErrorV1::
            kInvalidArgument;
    }
    RecoveryMaintenanceReportStoreErrorV1 context =
        ValidateContext(
            lease, action, provider, maintenance_fd);
    if (context !=
        RecoveryMaintenanceReportStoreErrorV1::kNone) {
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
        return RecoveryMaintenanceReportStoreErrorV1::
            kUnsafeMaintenanceDirectory;
    }
    DIR* const native =
        ::fdopendir(scan_fd.Release());
    if (native == nullptr) {
        return RecoveryMaintenanceReportStoreErrorV1::
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
                return RecoveryMaintenanceReportStoreErrorV1::
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
                kRecoveryMaintenanceReportV1MaximumCandidates) {
                return RecoveryMaintenanceReportStoreErrorV1::
                    kCandidateLimitExceeded;
            }
            CandidateName candidate{};
            candidate.name.assign(
                name.data(), name.size());
            candidate.temporary = temporary;
            if (temporary) {
                const std::size_t final_size =
                    name.size() - 1U -
                    kRecoveryMaintenanceReportV1TemporarySuffix
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
        RecoveryMaintenanceReportStoreErrorV1::kNone) {
        return context;
    }
    if (malformed) {
        return RecoveryMaintenanceReportStoreErrorV1::
            kMalformedCandidateName;
    }
    output->swap(observed);
    return RecoveryMaintenanceReportStoreErrorV1::kNone;
}

[[nodiscard]] RecoveryMaintenanceReportStoreErrorV1
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
        return RecoveryMaintenanceReportStoreErrorV1::
            kInvalidArgument;
    }
    RecoveryMaintenanceReportStoreErrorV1 context =
        ValidateContext(
            lease, action, provider, maintenance_fd);
    if (context !=
        RecoveryMaintenanceReportStoreErrorV1::kNone) {
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
                   ? RecoveryMaintenanceReportStoreErrorV1::
                         kPublishConflict
                   : RecoveryMaintenanceReportStoreErrorV1::
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
        return RecoveryMaintenanceReportStoreErrorV1::
            kUnsafeCandidate;
    }
    std::string bytes;
    if (!ReadExact(
            fd.get(),
            static_cast<std::size_t>(status.st_size),
            &bytes)) {
        return RecoveryMaintenanceReportStoreErrorV1::
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
        return RecoveryMaintenanceReportStoreErrorV1::
            kUnsafeCandidate;
    }
    context = ValidateContext(
        lease, action, provider, maintenance_fd);
    if (context !=
        RecoveryMaintenanceReportStoreErrorV1::kNone) {
        return context;
    }

    RecoveryMaintenanceReportV1 parsed{};
    const RecoveryMaintenanceReportV1Error parse_error =
        ParseRecoveryMaintenanceReportV1Jcs(
            bytes, &parsed);
    bool recognized_partial = false;
    if (parse_error ==
        RecoveryMaintenanceReportV1Error::kNone) {
        std::string parsed_filename;
        if (RecoveryMaintenanceReportV1Filename(
                parsed, &parsed_filename) !=
                RecoveryMaintenanceReportV1Error::kNone ||
            parsed_filename !=
                candidate_name.final_name ||
            ((candidate_name.name ==
                  current_final_name ||
              candidate_name.name ==
                  current_temporary_name) &&
             bytes != desired)) {
            return RecoveryMaintenanceReportStoreErrorV1::
                kCandidateConflict;
        }
    } else if (
        candidate_name.temporary &&
        candidate_name.name == current_temporary_name &&
        bytes.size() < desired.size() &&
        desired.substr(0U, bytes.size()) == bytes) {
        recognized_partial = true;
    } else {
        return RecoveryMaintenanceReportStoreErrorV1::
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
    return RecoveryMaintenanceReportStoreErrorV1::kNone;
}

[[nodiscard]] RecoveryMaintenanceReportStoreErrorV1
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
        return RecoveryMaintenanceReportStoreErrorV1::
            kInvalidArgument;
    }
    std::vector<CandidateName> names;
    RecoveryMaintenanceReportStoreErrorV1 error =
        CollectCandidateNames(
            lease,
            action,
            provider,
            maintenance_fd,
            &names);
    if (error !=
        RecoveryMaintenanceReportStoreErrorV1::kNone) {
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
            RecoveryMaintenanceReportStoreErrorV1::kNone) {
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
            return RecoveryMaintenanceReportStoreErrorV1::
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
    return RecoveryMaintenanceReportStoreErrorV1::kNone;
}

[[nodiscard]] RecoveryMaintenanceReportStoreErrorV1
RevalidateRetainedCandidate(
    const RawWriterLease& lease,
    RawReserveAuthorizedActionV1& action,
    const LeaseTargetProvider& provider,
    int maintenance_fd,
    const std::string& name,
    LoadedCandidate* candidate) {
    if (candidate == nullptr ||
        candidate->descriptor.get() < 0) {
        return RecoveryMaintenanceReportStoreErrorV1::
            kInvalidArgument;
    }
    RecoveryMaintenanceReportStoreErrorV1 context =
        ValidateContext(
            lease, action, provider, maintenance_fd);
    if (context !=
        RecoveryMaintenanceReportStoreErrorV1::kNone) {
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
        return RecoveryMaintenanceReportStoreErrorV1::
            kUnsafeCandidate;
    }
    std::string bytes;
    if (!ReadExact(
            candidate->descriptor.get(),
            static_cast<std::size_t>(before.st_size),
            &bytes)) {
        return RecoveryMaintenanceReportStoreErrorV1::
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
        return RecoveryMaintenanceReportStoreErrorV1::
            kUnsafeCandidate;
    }
    context = ValidateContext(
        lease, action, provider, maintenance_fd);
    if (context !=
        RecoveryMaintenanceReportStoreErrorV1::kNone) {
        return context;
    }
    if (bytes != candidate->bytes) {
        return RecoveryMaintenanceReportStoreErrorV1::
            kCandidateConflict;
    }
    candidate->status = after;
    return RecoveryMaintenanceReportStoreErrorV1::kNone;
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
                RecoveryMaintenanceReportStoreErrorV1::kNone ||
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
               RecoveryMaintenanceReportStoreErrorV1::kNone &&
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

[[nodiscard]] bool SameNamespace(
    const RawManifestNamespaceV1& left,
    const RawManifestNamespaceV1& right) noexcept {
    return left.capture_date == right.capture_date &&
           left.source_stream_id ==
               right.source_stream_id &&
           left.stream_day_id == right.stream_day_id;
}

[[nodiscard]] bool ValidateJournalEvidence(
    const RawWriterLease& lease,
    const RecoveryMaintenanceReportV1& report,
    int supplied_journal_fd,
    std::uint64_t expected_device,
    std::uint64_t expected_inode,
    TerminalSidecarEvidence* evidence) {
    if (evidence == nullptr) {
        return false;
    }
    ScopedFd journal(
        supplied_journal_fd >= 0
            ? DuplicateFd(supplied_journal_fd)
            : OpenAtNoIntr(
                  lease.directory_descriptor(),
                  kRawJournalFilename,
                  O_RDONLY | O_NOFOLLOW |
                      O_NONBLOCK | O_CLOEXEC |
                      O_NOATIME));
    struct stat before {};
    const std::uint64_t expected_size =
        report.result ==
                RecoveryMaintenanceResultV1::
                    kEmptyAnchorOnly
            ? static_cast<std::uint64_t>(
                  kRawV1JournalHeaderBytes)
            : report.recovery_range
                  .final_durable_journal_size;
    const int flags =
        journal.get() < 0
            ? -1
            : ::fcntl(journal.get(), F_GETFL);
    if (journal.get() < 0 ||
        expected_size < kRawV1JournalHeaderBytes ||
        ::fstat(journal.get(), &before) != 0 ||
        !IsSafeEvidenceFile(
            before, expected_size) ||
        static_cast<std::uint64_t>(before.st_size) !=
            expected_size ||
        before.st_dev !=
            evidence->maintenance_status.st_dev ||
        flags < 0 ||
        (flags & O_ACCMODE) != O_RDONLY ||
        (flags & O_APPEND) != 0 ||
        (supplied_journal_fd >= 0 &&
         (static_cast<std::uint64_t>(before.st_dev) !=
              expected_device ||
          static_cast<std::uint64_t>(before.st_ino) !=
              expected_inode)) ||
        !NameMatchesDescriptor(
            lease.directory_descriptor(),
            kRawJournalFilename,
            journal.get(),
            &before)) {
        return false;
    }

    std::string header_bytes;
    if (!ReadExact(
            journal.get(),
            kRawV1JournalHeaderBytes,
            &header_bytes)) {
        return false;
    }
    RawV1JournalHeaderWire wire{};
    std::memcpy(
        wire.data(),
        header_bytes.data(),
        header_bytes.size());
    DurableJournalHeaderV1 decoded{};
    RawV1JournalHeaderWire canonical{};
    struct stat after {};
    if (DecodeDurableJournalHeaderV1(
            wire, &decoded) != RawV1Error::kNone ||
        EncodeDurableJournalHeaderV1(
            decoded, &canonical) != RawV1Error::kNone ||
        canonical != wire ||
        decoded.raw_schema_sha256 !=
            kFrozenRawSchemaSha256 ||
        decoded.source_stream_id !=
            report.namespace_identity.source_stream_id ||
        decoded.capture_date !=
            report.namespace_identity.capture_date ||
        decoded.stream_day_id !=
            report.namespace_identity.stream_day_id ||
        l2flow::common::ComputeSha256(wire) !=
            report.journal_header_sha256 ||
        ::fstat(journal.get(), &after) != 0 ||
        !SameInode(before, after) ||
        before.st_mode != after.st_mode ||
        before.st_uid != after.st_uid ||
        before.st_nlink != after.st_nlink ||
        before.st_size != after.st_size ||
        !NameMatchesDescriptor(
            lease.directory_descriptor(),
            kRawJournalFilename,
            journal.get(),
            &after)) {
        return false;
    }
    evidence->journal_header_bytes = wire;
    evidence->journal_status = after;
    evidence->journal = std::move(journal);
    return true;
}

[[nodiscard]] bool ValidateTerminalSidecar(
    const RawWriterLease& lease,
    RawReserveAuthorizedActionV1& action,
    const RecoveryMaintenanceReportV1& report,
    int maintenance_fd,
    const RecoveryMaintenanceReportStoreAccessV1::
        SealedView* sealed,
    const RecoveryMaintenanceReportStoreAccessV1::
        EmptyView* empty,
    TerminalSidecarEvidence* evidence) {
    if (evidence == nullptr ||
        action.target() == nullptr ||
        ((sealed == nullptr) == (empty == nullptr))) {
        return false;
    }

    const RawReserveRegistryEntryKeyV1* key =
        sealed != nullptr ? sealed->key : empty->key;
    const RawReserveGenerationActionTokenV1* token =
        sealed != nullptr ? sealed->token : empty->token;
    const RawReserveMutationTargetAnchorV1* target =
        sealed != nullptr ? sealed->target : empty->target;
    const RawV1Identity* writer =
        sealed != nullptr
            ? sealed->writer_instance
            : empty->writer_instance;
    const RawV1Digest* sidecar_sha256 =
        sealed != nullptr
            ? sealed->sidecar_sha256
            : empty->sidecar_sha256;
    const std::string* sidecar_filename =
        sealed != nullptr
            ? sealed->filename
            : empty->filename;
    const std::uint64_t maintenance_device =
        sealed != nullptr
            ? sealed->maintenance_device
            : empty->maintenance_device;
    const std::uint64_t maintenance_inode =
        sealed != nullptr
            ? sealed->maintenance_inode
            : empty->maintenance_inode;
    const std::uint64_t sidecar_device =
        sealed != nullptr
            ? sealed->sidecar_device
            : empty->sidecar_device;
    const std::uint64_t sidecar_inode =
        sealed != nullptr
            ? sealed->sidecar_inode
            : empty->sidecar_inode;
    const int retained_maintenance_fd =
        sealed != nullptr
            ? sealed->maintenance_fd
            : empty->maintenance_fd;
    const int retained_sidecar_fd =
        sealed != nullptr
            ? sealed->sidecar_fd
            : empty->sidecar_fd;
    if (key == nullptr || token == nullptr ||
        target == nullptr || writer == nullptr ||
        sidecar_sha256 == nullptr ||
        sidecar_filename == nullptr ||
        *key != action.key() ||
        *token != action.token() ||
        *target != *action.target() ||
        *writer !=
            action.token().writer_instance_id ||
        report.recovery_attempt_id !=
            key->recovery_attempt_id ||
        key->recovery_attempt_id !=
            token->recovery_attempt_id ||
        l2flow::common::IsZeroIdentity(*writer)) {
        return false;
    }

    struct stat current_maintenance {};
    struct stat retained_maintenance {};
    if (::fstat(
            maintenance_fd,
            &current_maintenance) != 0 ||
        ::fstat(
            retained_maintenance_fd,
            &retained_maintenance) != 0 ||
        !IsSafeDirectory(current_maintenance) ||
        !SameInode(
            current_maintenance,
            retained_maintenance) ||
        static_cast<std::uint64_t>(
            retained_maintenance.st_dev) !=
            maintenance_device ||
        static_cast<std::uint64_t>(
            retained_maintenance.st_ino) !=
            maintenance_inode ||
        !NameMatchesDescriptor(
            lease.directory_descriptor(),
            "maintenance",
            retained_maintenance_fd,
            &retained_maintenance)) {
        return false;
    }
    evidence->maintenance.Reset(
        DuplicateFd(maintenance_fd));
    if (evidence->maintenance.get() < 0 ||
        ::fstat(
            evidence->maintenance.get(),
            &evidence->maintenance_status) != 0 ||
        !SameInode(
            evidence->maintenance_status,
            current_maintenance)) {
        return false;
    }

    ScopedFd sidecar(
        DuplicateFd(retained_sidecar_fd));
    struct stat sidecar_before {};
    const std::uint64_t sidecar_maximum =
        sealed != nullptr
            ? kSealedRawCertificateV1MaximumBytes
            : kEmptyAnchorTombstoneV1MaximumBytes;
    if (sidecar.get() < 0 ||
        ::fstat(sidecar.get(), &sidecar_before) != 0 ||
        !IsSafeEvidenceFile(
            sidecar_before, sidecar_maximum) ||
        sidecar_before.st_dev !=
            current_maintenance.st_dev ||
        static_cast<std::uint64_t>(
            sidecar_before.st_dev) !=
            sidecar_device ||
        static_cast<std::uint64_t>(
            sidecar_before.st_ino) !=
            sidecar_inode ||
        !(sealed != nullptr
              ? IsSealedSidecarName(
                    *sidecar_filename)
              : IsEmptySidecarName(
                    *sidecar_filename)) ||
        !NameMatchesDescriptor(
            maintenance_fd,
            sidecar_filename->c_str(),
            sidecar.get(),
            &sidecar_before)) {
        return false;
    }
    std::string sidecar_bytes;
    if (!ReadExact(
            sidecar.get(),
            static_cast<std::size_t>(
                sidecar_before.st_size),
            &sidecar_bytes) ||
        l2flow::common::ComputeSha256(
            sidecar_bytes) != *sidecar_sha256) {
        return false;
    }

    if (sealed != nullptr) {
        SealedRawCertificateV1 parsed{};
        std::string canonical;
        std::string canonical_filename;
        if (report.result !=
                RecoveryMaintenanceResultV1::
                    kSealedRaw ||
            report.current_sealed_raw_certificate_sha256 !=
                *sidecar_sha256 ||
            ParseSealedRawCertificateV1Jcs(
                sidecar_bytes, &parsed) !=
                SealedRawCertificateV1Error::kNone ||
            EncodeSealedRawCertificateV1Jcs(
                parsed, &canonical) !=
                SealedRawCertificateV1Error::kNone ||
            canonical != sidecar_bytes ||
            SealedRawCertificateV1Filename(
                parsed, &canonical_filename) !=
                SealedRawCertificateV1Error::kNone ||
            canonical_filename != *sidecar_filename ||
            !SameNamespace(
                parsed.namespace_identity,
                report.namespace_identity) ||
            parsed.journal_header_sha256 !=
                report.journal_header_sha256 ||
            parsed.closed_entry_count !=
                report.closed_frontier
                    .closed_entry_count ||
            parsed.closed_prefix_sha256 !=
                report.closed_frontier
                    .closed_prefix_sha256 ||
            parsed.accepted_sealed_marker_bytes !=
                report.sealed_boundary
                    .accepted_sealed_marker_bytes ||
            parsed.accepted_sealed_marker_sha256 !=
                report.sealed_boundary
                    .accepted_sealed_marker_sha256 ||
            parsed.last_segment_base_wal_pos !=
                report.sealed_boundary
                    .last_segment_base_wal_pos ||
            parsed.last_segment_flags !=
                report.sealed_boundary
                    .last_segment_flags ||
            parsed.last_segment_logical_length !=
                report.sealed_boundary
                    .last_segment_logical_length ||
            parsed.last_segment_sequence !=
                report.sealed_boundary
                    .last_segment_sequence ||
            parsed.last_segment_sha256 !=
                report.sealed_boundary
                    .last_segment_sha256 ||
            parsed.terminal_durable_cursor
                    .segment_sequence !=
                report.final_durable_cursor
                    .segment_sequence ||
            parsed.terminal_durable_cursor
                    .global_wal_pos !=
                report.final_durable_cursor
                    .global_wal_pos ||
            parsed.terminal_durable_cursor
                    .ingress_sequence !=
                report.final_durable_cursor
                    .ingress_sequence ||
            parsed.terminal_durable_cursor
                    .segment_offset !=
                report.final_durable_cursor
                    .segment_offset) {
            return false;
        }
    } else {
        EmptyAnchorTombstoneV1 parsed{};
        std::string canonical;
        std::string canonical_filename;
        if (report.result !=
                RecoveryMaintenanceResultV1::
                    kEmptyAnchorOnly ||
            report.current_empty_anchor_tombstone_sha256 !=
                *sidecar_sha256 ||
            empty->journal_header_sha256 == nullptr ||
            *empty->journal_header_sha256 !=
                report.journal_header_sha256 ||
            ParseEmptyAnchorTombstoneV1Jcs(
                sidecar_bytes, &parsed) !=
                EmptyAnchorTombstoneV1Error::kNone ||
            EncodeEmptyAnchorTombstoneV1Jcs(
                parsed, &canonical) !=
                EmptyAnchorTombstoneV1Error::kNone ||
            canonical != sidecar_bytes ||
            EmptyAnchorTombstoneV1Filename(
                parsed, &canonical_filename) !=
                EmptyAnchorTombstoneV1Error::kNone ||
            canonical_filename != *sidecar_filename ||
            parsed.namespace_identity.capture_date !=
                report.namespace_identity.capture_date ||
            parsed.namespace_identity.source_stream_id !=
                report.namespace_identity
                    .source_stream_id ||
            parsed.namespace_identity.stream_day_id !=
                report.namespace_identity.stream_day_id ||
            parsed.journal_header_sha256 !=
                report.journal_header_sha256 ||
            parsed.marker_count != 0U ||
            parsed.record_count != 0U ||
            parsed.segment_count != 0U) {
            return false;
        }
    }

    struct stat sidecar_after {};
    if (::fstat(sidecar.get(), &sidecar_after) != 0 ||
        !SameInode(sidecar_before, sidecar_after) ||
        sidecar_before.st_mode !=
            sidecar_after.st_mode ||
        sidecar_before.st_uid != sidecar_after.st_uid ||
        sidecar_before.st_nlink !=
            sidecar_after.st_nlink ||
        sidecar_before.st_size !=
            sidecar_after.st_size ||
        !NameMatchesDescriptor(
            maintenance_fd,
            sidecar_filename->c_str(),
            sidecar.get(),
            &sidecar_after)) {
        return false;
    }

    if (!ValidateJournalEvidence(
            lease,
            report,
            empty != nullptr ? empty->journal_fd : -1,
            empty != nullptr
                ? empty->journal_device
                : 0U,
            empty != nullptr
                ? empty->journal_inode
                : 0U,
            evidence)) {
        return false;
    }
    evidence->sidecar_sha256 = *sidecar_sha256;
    evidence->sidecar_filename = *sidecar_filename;
    evidence->sidecar_status = sidecar_after;
    evidence->sidecar = std::move(sidecar);
    return true;
}

}  // namespace

std::string_view
RecoveryMaintenanceReportStoreErrorV1Name(
    RecoveryMaintenanceReportStoreErrorV1 error) noexcept {
    switch (error) {
    case RecoveryMaintenanceReportStoreErrorV1::kNone:
        return "none";
    case RecoveryMaintenanceReportStoreErrorV1::
        kInvalidArgument:
        return "invalid_argument";
    case RecoveryMaintenanceReportStoreErrorV1::
        kAuthorizationRejected:
        return "authorization_rejected";
    case RecoveryMaintenanceReportStoreErrorV1::
        kTargetMismatch:
        return "target_mismatch";
    case RecoveryMaintenanceReportStoreErrorV1::
        kMaintenanceDirectoryMissing:
        return "maintenance_directory_missing";
    case RecoveryMaintenanceReportStoreErrorV1::
        kUnsafeMaintenanceDirectory:
        return "unsafe_maintenance_directory";
    case RecoveryMaintenanceReportStoreErrorV1::
        kCandidateLimitExceeded:
        return "candidate_limit_exceeded";
    case RecoveryMaintenanceReportStoreErrorV1::
        kMalformedCandidateName:
        return "malformed_candidate_name";
    case RecoveryMaintenanceReportStoreErrorV1::
        kUnsafeCandidate:
        return "unsafe_candidate";
    case RecoveryMaintenanceReportStoreErrorV1::
        kCandidateConflict:
        return "candidate_conflict";
    case RecoveryMaintenanceReportStoreErrorV1::
        kTemporaryCreate:
        return "temporary_create";
    case RecoveryMaintenanceReportStoreErrorV1::
        kWriteFailure:
        return "write_failure";
    case RecoveryMaintenanceReportStoreErrorV1::
        kSyncFailure:
        return "sync_failure";
    case RecoveryMaintenanceReportStoreErrorV1::
        kPublishConflict:
        return "publish_conflict";
    case RecoveryMaintenanceReportStoreErrorV1::
        kReadbackFailure:
        return "readback_failure";
    case RecoveryMaintenanceReportStoreErrorV1::
        kAllocationFailure:
        return "allocation_failure";
    }
    return "unknown";
}

static RecoveryMaintenanceReportPublishResultV1
PublishRecoveryMaintenanceReportImpl(
    const RawWriterLease& lease,
    std::unique_ptr<RawReserveAuthorizedActionV1>
        recovering_action,
    const BuiltRecoveryMaintenanceReportV1& report,
    std::unique_ptr<
        SealedRawRecoveryTerminalReceiptV1>
        sealed_sidecar_receipt,
    std::unique_ptr<EmptyAnchorTombstoneReceiptV1>
        empty_sidecar_receipt,
    std::string* diagnostic) noexcept {
    RecoveryMaintenanceReportPublishResultV1 result{};
    SetDiagnostic(diagnostic, {});
    try {
        RecoveryMaintenanceReportStoreAccessV1::
            SealedView sealed_view{};
        RecoveryMaintenanceReportStoreAccessV1::
            EmptyView empty_view{};
        const bool supplied_sealed_sidecar =
            sealed_sidecar_receipt != nullptr;
        const bool supplied_empty_sidecar =
            empty_sidecar_receipt != nullptr;
        if ((supplied_sealed_sidecar &&
             !RecoveryMaintenanceReportStoreAccessV1::
                  Consume(
                      sealed_sidecar_receipt.get(),
                      &sealed_view)) ||
            (supplied_empty_sidecar &&
             !RecoveryMaintenanceReportStoreAccessV1::
                  Consume(
                      empty_sidecar_receipt.get(),
                      &empty_view))) {
            result.error =
                RecoveryMaintenanceReportStoreErrorV1::
                    kInvalidArgument;
            SetDiagnostic(
                diagnostic,
                "terminal sidecar receipt is absent or consumed");
            return result;
        }
        if (recovering_action == nullptr) {
            result.error =
                RecoveryMaintenanceReportStoreErrorV1::
                    kInvalidArgument;
            SetDiagnostic(
                diagnostic,
                "RECOVERING report action is missing");
            return result;
        }
        RawReserveAuthorizedActionV1& action =
            *recovering_action;
        const RecoveryMaintenanceReportV1& model =
            report.model();
        const bool resumed =
            model.result ==
            RecoveryMaintenanceResultV1::kResumedOpen;
        const bool sealed_terminal =
            model.result ==
            RecoveryMaintenanceResultV1::kSealedRaw;
        const bool empty_terminal =
            model.result ==
            RecoveryMaintenanceResultV1::
                kEmptyAnchorOnly;
        const bool terminal =
            sealed_terminal || empty_terminal;
        const std::string desired(
            report.canonical_jcs());
        std::string filename(report.filename());
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
                kRecoveryMaintenanceReportV1MaximumBytes ||
            !IsFinalCandidateName(filename) ||
            ValidateRecoveryMaintenanceReportV1(model) !=
                RecoveryMaintenanceReportV1Error::kNone ||
            l2flow::common::ComputeSha256(desired) !=
                report.report_sha256() ||
            (!resumed && !terminal) ||
            (resumed &&
             (supplied_sealed_sidecar ||
              supplied_empty_sidecar)) ||
            (sealed_terminal &&
             (!supplied_sealed_sidecar ||
              supplied_empty_sidecar)) ||
            (empty_terminal &&
             (!supplied_empty_sidecar ||
              supplied_sealed_sidecar))) {
            result.error =
                RecoveryMaintenanceReportStoreErrorV1::
                    kInvalidArgument;
            SetDiagnostic(
                diagnostic,
                "recovery maintenance report input is invalid");
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
            key.recovery_attempt_id !=
                model.recovery_attempt_id ||
            action.token().recovery_attempt_id !=
                model.recovery_attempt_id ||
            action.token().recovery_attempt_id !=
                key.recovery_attempt_id ||
            action.required_status() !=
                ReserveRegistryStatusV1::kRecovering ||
            (resumed &&
             (action.recovery_intent() !=
                      ReserveRecoveryIntentV1::
                          kResumeConnect ||
              action.token().writer_instance_id !=
                  report.writer_instance() ||
              report.control_cursor() !=
                  model.final_durable_cursor)) ||
            (terminal &&
             (action.recovery_intent() !=
                      ReserveRecoveryIntentV1::
                          kRecoverSealOnly ||
              l2flow::common::IsZeroIdentity(
                  action.token()
                      .writer_instance_id) ||
              !l2flow::common::IsZeroIdentity(
                  report.writer_instance()) ||
              report.control_cursor() !=
                  RecoveryMaintenanceCursorV1{}))) {
            result.error =
                RecoveryMaintenanceReportStoreErrorV1::
                    kAuthorizationRejected;
            SetDiagnostic(
                diagnostic,
                "RECOVERING action identity does not match the report");
            return result;
        }
        LeaseTargetProvider provider(lease);
        const RawReserveMutationTargetAnchorV1* const
            target = action.target();
        if (target == nullptr ||
            !ValidateRawReserveMutationTargetProviderV1(
                provider, *target)) {
            result.error =
                RecoveryMaintenanceReportStoreErrorV1::
                    kTargetMismatch;
            SetDiagnostic(
                diagnostic,
                "report route target does not match RECOVERING capability");
            return result;
        }
        if (!Authorize(action, provider)) {
            result.error =
                RecoveryMaintenanceReportStoreErrorV1::
                    kAuthorizationRejected;
            SetDiagnostic(
                diagnostic,
                "RECOVERING report capability is stale");
            return result;
        }

        ScopedFd maintenance;
        result.error =
            OpenMaintenance(lease, &maintenance);
        if (result.error !=
            RecoveryMaintenanceReportStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "cannot open the pre-existing maintenance directory");
            return result;
        }
        if (!LockExclusiveNoIntr(maintenance.get())) {
            result.error =
                RecoveryMaintenanceReportStoreErrorV1::
                    kUnsafeMaintenanceDirectory;
            SetDiagnostic(
                diagnostic,
                "cannot serialize recovery maintenance report maintenance");
            return result;
        }
        TerminalSidecarEvidence terminal_evidence{};
        if (terminal &&
            !ValidateTerminalSidecar(
                lease,
                action,
                model,
                maintenance.get(),
                sealed_terminal ? &sealed_view : nullptr,
                empty_terminal ? &empty_view : nullptr,
                &terminal_evidence)) {
            result.error =
                RecoveryMaintenanceReportStoreErrorV1::
                    kAuthorizationRejected;
            SetDiagnostic(
                diagnostic,
                "terminal sidecar receipt or retained evidence does not match the report");
            return result;
        }
        result.error = ValidateContext(
            lease,
            action,
            provider,
            maintenance.get());
        if (result.error !=
            RecoveryMaintenanceReportStoreErrorV1::kNone) {
            return result;
        }
        const std::string temporary =
            "." + filename +
            std::string(
                kRecoveryMaintenanceReportV1TemporarySuffix);
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
            RecoveryMaintenanceReportStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "recovery maintenance report candidate inventory is invalid");
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
            kRecoveryMaintenanceReportV1MaximumCandidates -
                required_new_names) {
            result.error =
                RecoveryMaintenanceReportStoreErrorV1::
                    kCandidateLimitExceeded;
            SetDiagnostic(
                diagnostic,
                "recovery maintenance report candidate admission has no bounded crash space");
            return result;
        }

        LoadedCandidate final =
            std::move(inventory.current_final);
        LoadedCandidate temporary_candidate =
            std::move(
                inventory.current_temporary_candidate);

        const auto ContextReady = [&]() {
            result.error = ValidateContext(
                lease,
                action,
                provider,
                maintenance.get());
            if (result.error !=
                RecoveryMaintenanceReportStoreErrorV1::
                    kNone) {
                return false;
            }
            if (terminal) {
                TerminalSidecarEvidence current{};
                if (!ValidateTerminalSidecar(
                        lease,
                        action,
                        model,
                        maintenance.get(),
                        sealed_terminal
                            ? &sealed_view
                            : nullptr,
                        empty_terminal
                            ? &empty_view
                            : nullptr,
                        &current)) {
                    result.error =
                        RecoveryMaintenanceReportStoreErrorV1::
                            kAuthorizationRejected;
                    return false;
                }
            }
            return true;
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
                    RecoveryMaintenanceReportStoreErrorV1::
                        kPublishConflict;
                return result;
            }
        } else if (!NameAbsent(
                       maintenance.get(),
                       filename.c_str())) {
            result.error =
                RecoveryMaintenanceReportStoreErrorV1::
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
                    RecoveryMaintenanceReportStoreErrorV1::
                        kPublishConflict;
                return result;
            }
        } else if (!NameAbsent(
                       maintenance.get(),
                       temporary.c_str())) {
            result.error =
                RecoveryMaintenanceReportStoreErrorV1::
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
                RecoveryMaintenanceReportStoreErrorV1::
                    kNone) {
                return result;
            }
            if (::unlinkat(
                    maintenance.get(),
                    temporary.c_str(),
                    0) != 0) {
                result.error =
                    RecoveryMaintenanceReportStoreErrorV1::
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
                    RecoveryMaintenanceReportStoreErrorV1::
                        kPublishConflict;
                return result;
            }
            if (!FsyncNoIntr(maintenance.get())) {
                result.error =
                    RecoveryMaintenanceReportStoreErrorV1::
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
                RecoveryMaintenanceReportStoreErrorV1::
                    kNone) {
                return result;
            }
            if (!FsyncNoIntr(final.descriptor.get())) {
                result.error =
                    RecoveryMaintenanceReportStoreErrorV1::
                        kSyncFailure;
                return result;
            }
            if (!ContextReady() ||
                !NameMatchesDescriptor(
                    maintenance.get(),
                    filename.c_str(),
                    final.descriptor.get())) {
                if (result.error ==
                    RecoveryMaintenanceReportStoreErrorV1::
                        kNone) {
                    result.error =
                        RecoveryMaintenanceReportStoreErrorV1::
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
                    RecoveryMaintenanceReportStoreErrorV1::
                        kNone) {
                    return result;
                }
                if (::unlinkat(
                        maintenance.get(),
                        temporary.c_str(),
                        0) != 0) {
                    result.error =
                        RecoveryMaintenanceReportStoreErrorV1::
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
                        RecoveryMaintenanceReportStoreErrorV1::
                            kPublishConflict;
                    return result;
                }
                if (!FsyncNoIntr(maintenance.get())) {
                    result.error =
                        RecoveryMaintenanceReportStoreErrorV1::
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
                        RecoveryMaintenanceReportStoreErrorV1::
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
                        RecoveryMaintenanceReportStoreErrorV1::
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
                        RecoveryMaintenanceReportStoreErrorV1::
                            kWriteFailure;
                    return result;
                }
                if (!ContextReady() ||
                    ::fchmod(created.get(), 0600) != 0) {
                    if (result.error ==
                        RecoveryMaintenanceReportStoreErrorV1::
                            kNone) {
                        result.error =
                            RecoveryMaintenanceReportStoreErrorV1::
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
                        RecoveryMaintenanceReportStoreErrorV1::
                            kNone) {
                        result.error = ValidateContext(
                            lease,
                            action,
                            provider,
                            maintenance.get());
                        if (result.error ==
                            RecoveryMaintenanceReportStoreErrorV1::
                                kNone) {
                            result.error =
                                RecoveryMaintenanceReportStoreErrorV1::
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
                        RecoveryMaintenanceReportStoreErrorV1::
                            kNone) {
                        result.error =
                            RecoveryMaintenanceReportStoreErrorV1::
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
                RecoveryMaintenanceReportStoreErrorV1::
                    kNone) {
                return result;
            }
            if (!FsyncNoIntr(
                    temporary_candidate
                        .descriptor.get())) {
                result.error =
                    RecoveryMaintenanceReportStoreErrorV1::
                        kSyncFailure;
                return result;
            }
            if (!ContextReady() ||
                !NameMatchesDescriptor(
                    maintenance.get(),
                    temporary.c_str(),
                    temporary_candidate.descriptor.get())) {
                if (result.error ==
                    RecoveryMaintenanceReportStoreErrorV1::
                        kNone) {
                    result.error =
                        RecoveryMaintenanceReportStoreErrorV1::
                            kPublishConflict;
                }
                return result;
            }
            result.file_synced = true;
            if (!NameAbsent(
                    maintenance.get(),
                    filename.c_str())) {
                result.error =
                    RecoveryMaintenanceReportStoreErrorV1::
                        kPublishConflict;
                return result;
            }
            if (!ContextReady() ||
                !RenameNoReplace(
                    maintenance.get(),
                    temporary.c_str(),
                    filename.c_str())) {
                if (result.error ==
                    RecoveryMaintenanceReportStoreErrorV1::
                        kNone) {
                    result.error =
                        RecoveryMaintenanceReportStoreErrorV1::
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
                    RecoveryMaintenanceReportStoreErrorV1::
                        kNone) {
                    result.error =
                        RecoveryMaintenanceReportStoreErrorV1::
                            kPublishConflict;
                }
                return result;
            }
            if (!FsyncNoIntr(maintenance.get())) {
                result.error =
                    RecoveryMaintenanceReportStoreErrorV1::
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
                    RecoveryMaintenanceReportStoreErrorV1::
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
                RecoveryMaintenanceReportStoreErrorV1::
                    kNone) {
                result.error =
                    RecoveryMaintenanceReportStoreErrorV1::
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
                RecoveryMaintenanceReportStoreErrorV1::
                    kNone) {
                return result;
            }
            result.error =
                RecoveryMaintenanceReportStoreErrorV1::
                    kReadbackFailure;
            return result;
        }
        result.error =
            RecoveryMaintenanceReportStoreErrorV1::kNone;
        result.disposition =
            final_present
                ? (recovered_partial
                       ? RecoveryMaintenanceReportDispositionV1::
                             kAcceptedExistingAndCleanedRecognizedPartialTemporary
                       : (cleaned
                              ? RecoveryMaintenanceReportDispositionV1::
                                    kAcceptedExistingAndCleanedIdenticalTemporary
                              : RecoveryMaintenanceReportDispositionV1::
                                    kAcceptedExistingFinal))
                : (recovered_partial
                       ? RecoveryMaintenanceReportDispositionV1::
                             kRebuiltRecognizedPartialTemporary
                       : (adopted
                              ? RecoveryMaintenanceReportDispositionV1::
                                    kAdoptedCompleteTemporary
                              : RecoveryMaintenanceReportDispositionV1::
                                    kPublishedNew));
        result.report_sha256 =
            report.report_sha256();
        if (!ContextReady() ||
            !NameMatchesDescriptor(
                maintenance.get(),
                filename.c_str(),
                final.descriptor.get(),
                &after_readback)) {
            if (result.error ==
                RecoveryMaintenanceReportStoreErrorV1::
                    kNone) {
                result.error =
                    RecoveryMaintenanceReportStoreErrorV1::
                        kReadbackFailure;
            }
            result.disposition =
                RecoveryMaintenanceReportDispositionV1::kNone;
            return result;
        }
        const RawReserveMutationTargetAnchorV1* const
            receipt_target = action.target();
        if (receipt_target != nullptr && resumed) {
            ScopedFd receipt_maintenance(
                OpenAtNoIntr(
                    maintenance.get(),
                    ".",
                    O_RDONLY | O_DIRECTORY |
                        O_NOFOLLOW | O_NONBLOCK |
                        O_CLOEXEC | O_NOATIME));
            ScopedFd receipt_report(
                DuplicateFd(final.descriptor.get()));
            if (receipt_maintenance.get() < 0 ||
                receipt_report.get() < 0 ||
                !MaintenanceStillNamed(
                    lease,
                    receipt_maintenance.get()) ||
                !ContextReady() ||
                !NameMatchesDescriptor(
                    receipt_maintenance.get(),
                    filename.c_str(),
                    receipt_report.get(),
                    &after_readback)) {
                result.error =
                    result.error ==
                            RecoveryMaintenanceReportStoreErrorV1::
                                kNone
                        ? RecoveryMaintenanceReportStoreErrorV1::
                              kReadbackFailure
                        : result.error;
                result.disposition =
                    RecoveryMaintenanceReportDispositionV1::
                        kNone;
                SetDiagnostic(
                    diagnostic,
                    "cannot retain recovery maintenance report receipt descriptors");
                return result;
            }
            result.activation_receipt =
                RecoveryMaintenanceReportStoreAccessV1::
                    MakeActivationReceipt(
                        action.key(),
                        action.token(),
                        *receipt_target,
                        report.writer_instance(),
                        result.report_sha256,
                        filename,
                        model.closed_frontier
                            .closed_entry_count,
                        model.closed_frontier
                            .closed_prefix_sha256,
                        model.open_boundary
                            .manifest_generation,
                        model.open_boundary
                            .manifest_entry_commitment_sha256,
                        model.final_durable_cursor
                            .segment_sequence,
                        model.open_boundary.open_variant ==
                                RecoveryMaintenanceOpenVariantV1::
                                    kReuseOpen
                            ? model.open_boundary
                                  .reuse_segment
                                  .segment_header_sha256
                            : model.open_boundary
                                  .new_segment
                                  .segment_header_sha256,
                        model.open_boundary
                            .endpoint_marker.marker_bytes,
                        model.open_boundary
                            .endpoint_marker.marker_sha256,
                        report.control_cursor(),
                        receipt_maintenance.get(),
                        receipt_report.get());
            static_cast<void>(
                receipt_maintenance.Release());
            static_cast<void>(
                receipt_report.Release());
        } else if (receipt_target != nullptr &&
                   terminal) {
            // Revalidate the complete sidecar/journal evidence after the
            // report directory barrier. The terminal receipt therefore
            // captures the exact named inodes at both sides of the ordered
            // sidecar -> report durability sequence.
            TerminalSidecarEvidence refreshed_evidence{};
            ScopedFd receipt_report(
                DuplicateFd(final.descriptor.get()));
            struct stat receipt_report_status {};
            if (!ValidateTerminalSidecar(
                    lease,
                    action,
                    model,
                    maintenance.get(),
                    sealed_terminal
                        ? &sealed_view
                        : nullptr,
                    empty_terminal
                        ? &empty_view
                        : nullptr,
                    &refreshed_evidence) ||
                receipt_report.get() < 0 ||
                ::fstat(
                    receipt_report.get(),
                    &receipt_report_status) != 0 ||
                !IsSafeFile(receipt_report_status) ||
                !SameInode(
                    receipt_report_status,
                    after_readback) ||
                !NameMatchesDescriptor(
                    refreshed_evidence
                        .maintenance.get(),
                    filename.c_str(),
                    receipt_report.get(),
                    &after_readback) ||
                !ContextReady()) {
                result.error =
                    result.error ==
                            RecoveryMaintenanceReportStoreErrorV1::
                                kNone
                        ? RecoveryMaintenanceReportStoreErrorV1::
                              kReadbackFailure
                        : result.error;
                result.disposition =
                    RecoveryMaintenanceReportDispositionV1::
                        kNone;
                SetDiagnostic(
                    diagnostic,
                    "cannot retain terminal report, sidecar, and journal evidence");
                return result;
            }
            result.terminal_receipt =
                RecoveryMaintenanceReportStoreAccessV1::
                    MakeTerminalReceipt(
                        action.key(),
                        action.token(),
                        *receipt_target,
                        action.token()
                            .writer_instance_id,
                        model.result,
                        result.report_sha256,
                        filename,
                        refreshed_evidence
                            .sidecar_sha256,
                        refreshed_evidence
                            .sidecar_filename,
                        model.journal_header_sha256,
                        model,
                        refreshed_evidence
                            .journal_header_bytes,
                        refreshed_evidence
                            .maintenance_status,
                        refreshed_evidence
                            .journal_status,
                        refreshed_evidence
                            .sidecar_status,
                        receipt_report_status,
                        refreshed_evidence
                            .maintenance.get(),
                        refreshed_evidence
                            .journal.get(),
                        refreshed_evidence
                            .sidecar.get(),
                        receipt_report.get());
            static_cast<void>(
                refreshed_evidence
                    .maintenance.Release());
            static_cast<void>(
                refreshed_evidence.journal.Release());
            static_cast<void>(
                refreshed_evidence.sidecar.Release());
            static_cast<void>(
                receipt_report.Release());
        }
        if ((resumed &&
             result.activation_receipt == nullptr) ||
            (terminal &&
             result.terminal_receipt == nullptr)) {
            result.error =
                RecoveryMaintenanceReportStoreErrorV1::
                    kAllocationFailure;
            result.disposition =
                RecoveryMaintenanceReportDispositionV1::kNone;
            SetDiagnostic(
                diagnostic,
                "cannot allocate recovery maintenance report receipt");
            return result;
        }
        result.filename = std::move(filename);
        SetDiagnostic(diagnostic, {});
        return result;
    } catch (const std::bad_alloc&) {
        result.error =
            RecoveryMaintenanceReportStoreErrorV1::
                kAllocationFailure;
        SetDiagnostic(
            diagnostic,
            "recovery maintenance report publication allocation failed");
        return result;
    } catch (...) {
        result.error =
            RecoveryMaintenanceReportStoreErrorV1::
                kAllocationFailure;
        SetDiagnostic(
            diagnostic,
            "recovery maintenance report publication failed");
        return result;
    }
}

RecoveryMaintenanceReportPublishResultV1
PublishRecoveryMaintenanceReportV1(
    const RawWriterLease& lease,
    std::unique_ptr<RawReserveAuthorizedActionV1>
        recovering_action,
    const BuiltRecoveryMaintenanceReportV1& report,
    std::string* diagnostic) noexcept {
    return PublishRecoveryMaintenanceReportImpl(
        lease,
        std::move(recovering_action),
        report,
        nullptr,
        nullptr,
        diagnostic);
}

RecoveryMaintenanceReportPublishResultV1
PublishRecoveryMaintenanceReportV1(
    const RawWriterLease& lease,
    std::unique_ptr<RawReserveAuthorizedActionV1>
        recovering_action,
    const BuiltRecoveryMaintenanceReportV1& report,
    std::unique_ptr<
        SealedRawRecoveryTerminalReceiptV1>
        sidecar_receipt,
    std::string* diagnostic) noexcept {
    return PublishRecoveryMaintenanceReportImpl(
        lease,
        std::move(recovering_action),
        report,
        std::move(sidecar_receipt),
        nullptr,
        diagnostic);
}

RecoveryMaintenanceReportPublishResultV1
PublishRecoveryMaintenanceReportV1(
    const RawWriterLease& lease,
    std::unique_ptr<RawReserveAuthorizedActionV1>
        recovering_action,
    const BuiltRecoveryMaintenanceReportV1& report,
    std::unique_ptr<EmptyAnchorTombstoneReceiptV1>
        sidecar_receipt,
    std::string* diagnostic) noexcept {
    return PublishRecoveryMaintenanceReportImpl(
        lease,
        std::move(recovering_action),
        report,
        nullptr,
        std::move(sidecar_receipt),
        diagnostic);
}

}  // namespace l2flow::ingress
