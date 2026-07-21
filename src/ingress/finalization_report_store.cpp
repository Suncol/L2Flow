#include "l2flow/ingress/finalization_report_store.h"

#include "l2flow/common/sha256.h"
#include "l2flow/ingress/scaffolding_finalization_report_store.h"

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

enum class ReportDomain : std::uint8_t {
    kRouted = 1U,
    kScaffolding = 2U,
};

enum class PlanPrestate : std::uint8_t {
    kAbsent = 1U,
    kExistingFinal = 2U,
    kExistingFinalAndIdenticalTemporary = 3U,
    kCompleteTemporaryOnly = 4U,
};

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

struct ParentChain final {
    ReportDomain domain = ReportDomain::kRouted;
    ScopedFd root;
    ScopedFd first_parent;
    ScopedFd route_or_audit;
    ScopedFd report_parent;
    struct stat root_status {};
    struct stat first_parent_status {};
    struct stat route_or_audit_status {};
    struct stat report_parent_status {};
    std::string first_parent_name;
    std::string route_or_audit_name;
    std::string report_parent_name;
};

struct LoadedCandidate final {
    ScopedFd fd;
    struct stat status {};
    std::string bytes;
};

struct CandidateInventory final {
    std::uint32_t count = 0U;
    bool final_present = false;
    bool temporary_present = false;
    LoadedCandidate final;
    LoadedCandidate temporary;
    std::vector<std::string> names;
};

struct PublicationEvidence final {
    FinalizationReportStoreErrorV1 error =
        FinalizationReportStoreErrorV1::kNone;
    FinalizationReportStoreDispositionV1 disposition =
        FinalizationReportStoreDispositionV1::kNone;
    LoadedCandidate final;
    std::uint32_t observed_candidate_count = 0U;
    bool file_synced = false;
    bool directory_synced = false;
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

[[nodiscard]] bool UnlinkAtNoIntr(
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
    const struct stat& parent,
    std::size_t maximum_bytes) noexcept {
    return S_ISREG(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           (status.st_mode & 07777U) == 0600U &&
           status.st_nlink == static_cast<nlink_t>(1) &&
           status.st_size >= 0 &&
           static_cast<std::uint64_t>(status.st_size) <=
               maximum_bytes &&
           status.st_blocks >= 0 &&
           status.st_dev == parent.st_dev;
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

[[nodiscard]] bool IsRoutedFinalName(
    std::string_view name) noexcept {
    constexpr std::string_view prefix =
        "finalization-";
    constexpr std::string_view suffix = ".json";
    return name.size() ==
               prefix.size() + 32U + suffix.size() &&
           name.starts_with(prefix) &&
           name.ends_with(suffix) &&
           IsLowerHex(
               name.substr(prefix.size(), 32U));
}

[[nodiscard]] bool IsScaffoldingFinalName(
    std::string_view name) noexcept {
    constexpr std::string_view prefix =
        "scaffolding-";
    constexpr std::string_view suffix = ".json";
    constexpr std::size_t cycle_offset = prefix.size();
    constexpr std::size_t separator_offset =
        cycle_offset + 32U;
    constexpr std::size_t grant_offset =
        separator_offset + 1U;
    return name.size() ==
               prefix.size() + 32U + 1U + 64U +
                   suffix.size() &&
           name.starts_with(prefix) &&
           name.ends_with(suffix) &&
           name[separator_offset] == '-' &&
           IsLowerHex(
               name.substr(cycle_offset, 32U)) &&
           IsLowerHex(
               name.substr(grant_offset, 64U));
}

[[nodiscard]] bool IsFinalName(
    ReportDomain domain,
    std::string_view name) noexcept {
    return domain == ReportDomain::kRouted
               ? IsRoutedFinalName(name)
               : IsScaffoldingFinalName(name);
}

[[nodiscard]] std::string_view TemporarySuffix(
    ReportDomain domain) noexcept {
    return domain == ReportDomain::kRouted
               ? kFinalizationReportV1TemporarySuffix
               : kScaffoldingFinalizationReportV1TemporarySuffix;
}

[[nodiscard]] bool IsTemporaryName(
    ReportDomain domain,
    std::string_view name) noexcept {
    const std::string_view suffix =
        TemporarySuffix(domain);
    if (name.size() <= 1U + suffix.size() ||
        name.front() != '.' ||
        !name.ends_with(suffix)) {
        return false;
    }
    return IsFinalName(
        domain,
        name.substr(
            1U,
            name.size() - 1U - suffix.size()));
}

[[nodiscard]] bool StartsLikeCandidate(
    ReportDomain domain,
    std::string_view name) noexcept {
    return domain == ReportDomain::kRouted
               ? (name.starts_with("finalization-") ||
                  name.starts_with(".finalization-"))
               : (name.starts_with("scaffolding-") ||
                  name.starts_with(".scaffolding-"));
}

[[nodiscard]] std::size_t MaximumBytes(
    ReportDomain domain) noexcept {
    return domain == ReportDomain::kRouted
               ? kFinalizationReportV1MaximumBytes
               : kScaffoldingFinalizationReportV1MaximumBytes;
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

[[nodiscard]] std::uint64_t Device(
    const struct stat& status) noexcept {
    return static_cast<std::uint64_t>(status.st_dev);
}

[[nodiscard]] std::uint64_t Inode(
    const struct stat& status) noexcept {
    return static_cast<std::uint64_t>(status.st_ino);
}

[[nodiscard]] std::uint64_t Blocks(
    const struct stat& status) noexcept {
    return status.st_blocks < 0
               ? std::numeric_limits<std::uint64_t>::max()
               : static_cast<std::uint64_t>(
                     status.st_blocks);
}

[[nodiscard]] bool DirectoryStillNamed(
    int parent_fd,
    const std::string& name,
    int directory_fd,
    const struct stat& expected,
    const struct stat& parent_status) noexcept {
    struct stat current {};
    return ::fstat(directory_fd, &current) == 0 &&
           IsSafeDirectory(current) &&
           SameInode(current, expected) &&
           current.st_dev == parent_status.st_dev &&
           NameMatchesDescriptor(
               parent_fd,
               name.c_str(),
               directory_fd,
               nullptr);
}

[[nodiscard]] bool ValidateContext(
    RawReserveFinalizationActionV1& action,
    const ParentChain& chain) noexcept {
    if (!action.ValidateLatest()) {
        return false;
    }
    struct stat action_root {};
    struct stat retained_root {};
    if (action.raw_root_descriptor() < 0 ||
        ::fstat(
            action.raw_root_descriptor(),
            &action_root) != 0 ||
        ::fstat(chain.root.get(), &retained_root) != 0 ||
        !IsSafeDirectory(action_root) ||
        !IsSafeDirectory(retained_root) ||
        !SameInode(action_root, chain.root_status) ||
        !SameInode(retained_root, chain.root_status)) {
        return false;
    }

    if (!DirectoryStillNamed(
            chain.root.get(),
            chain.first_parent_name,
            chain.first_parent.get(),
            chain.first_parent_status,
            chain.root_status) ||
        !DirectoryStillNamed(
            chain.first_parent.get(),
            chain.route_or_audit_name,
            chain.route_or_audit.get(),
            chain.route_or_audit_status,
            chain.first_parent_status) ||
        !DirectoryStillNamed(
            chain.route_or_audit.get(),
            chain.report_parent_name,
            chain.report_parent.get(),
            chain.report_parent_status,
            chain.route_or_audit_status)) {
        return false;
    }

    if (chain.domain == ReportDomain::kRouted) {
        const auto* const target = action.target();
        struct stat action_route {};
        return target != nullptr &&
               action.route_directory_descriptor() >= 0 &&
               ::fstat(
                   action.route_directory_descriptor(),
                   &action_route) == 0 &&
               SameInode(
                   action_route,
                   chain.route_or_audit_status) &&
               Device(chain.root_status) ==
                   target->raw_root_device() &&
               Inode(chain.root_status) ==
                   target->raw_root_inode() &&
               Device(chain.route_or_audit_status) ==
                   target->route_device() &&
               Inode(chain.route_or_audit_status) ==
                   target->route_inode();
    }
    return action.target() == nullptr &&
           action.route_directory_descriptor() == -1;
}

[[nodiscard]] FinalizationReportStoreErrorV1
OpenDirectoryChild(
    int parent_fd,
    const struct stat& parent_status,
    std::string_view name,
    ScopedFd* output,
    struct stat* output_status) noexcept {
    if (output == nullptr || output_status == nullptr) {
        return FinalizationReportStoreErrorV1::
            kInvalidArgument;
    }
    const std::string owned_name(name);
    ScopedFd child(
        OpenAtNoIntr(
            parent_fd,
            owned_name.c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC | O_NOATIME));
    struct stat status {};
    if (child.get() < 0) {
        return errno == ENOENT
                   ? FinalizationReportStoreErrorV1::
                         kReportParentMissing
                   : FinalizationReportStoreErrorV1::
                         kUnsafeReportParent;
    }
    if (::fstat(child.get(), &status) != 0 ||
        !IsSafeDirectory(status) ||
        status.st_dev != parent_status.st_dev ||
        !NameMatchesDescriptor(
            parent_fd,
            owned_name.c_str(),
            child.get(),
            &status)) {
        return FinalizationReportStoreErrorV1::
            kUnsafeReportParent;
    }
    *output_status = status;
    *output = std::move(child);
    return FinalizationReportStoreErrorV1::kNone;
}

[[nodiscard]] FinalizationReportStoreErrorV1
OpenParentChain(
    RawReserveFinalizationActionV1& action,
    ReportDomain domain,
    ParentChain* output) noexcept {
    if (output == nullptr ||
        action.raw_root_descriptor() < 0 ||
        !action.ValidateLatest()) {
        return FinalizationReportStoreErrorV1::
            kAuthorizationRejected;
    }
    ParentChain chain{};
    chain.domain = domain;
    chain.root.Reset(
        OpenAtNoIntr(
            action.raw_root_descriptor(),
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC | O_NOATIME));
    struct stat action_root {};
    if (chain.root.get() < 0 ||
        ::fstat(
            action.raw_root_descriptor(),
            &action_root) != 0 ||
        ::fstat(
            chain.root.get(),
            &chain.root_status) != 0 ||
        !IsSafeDirectory(action_root) ||
        !IsSafeDirectory(chain.root_status) ||
        !SameInode(action_root, chain.root_status)) {
        return FinalizationReportStoreErrorV1::
            kTargetMismatch;
    }

    FinalizationReportStoreErrorV1 error{};
    if (domain == ReportDomain::kRouted) {
        const auto* const target = action.target();
        if (target == nullptr ||
            action.route_directory_descriptor() < 0) {
            return FinalizationReportStoreErrorV1::
                kTargetMismatch;
        }
        chain.first_parent_name =
            "capture_date=" +
            std::to_string(
                action.key().grant.capture_date);
        chain.route_or_audit_name =
            "stream=" +
            std::to_string(
                action.key().grant.source_stream_id) +
            "-" + std::string(target->stream_slug());
        chain.report_parent_name = "maintenance";
        error = OpenDirectoryChild(
            chain.root.get(),
            chain.root_status,
            chain.first_parent_name,
            &chain.first_parent,
            &chain.first_parent_status);
        if (error !=
            FinalizationReportStoreErrorV1::kNone) {
            return error;
        }
        error = OpenDirectoryChild(
            chain.first_parent.get(),
            chain.first_parent_status,
            chain.route_or_audit_name,
            &chain.route_or_audit,
            &chain.route_or_audit_status);
        if (error !=
            FinalizationReportStoreErrorV1::kNone) {
            return error;
        }
        struct stat action_route {};
        if (::fstat(
                action.route_directory_descriptor(),
                &action_route) != 0 ||
            !SameInode(
                action_route,
                chain.route_or_audit_status)) {
            return FinalizationReportStoreErrorV1::
                kTargetMismatch;
        }
    } else {
        if (action.target() != nullptr ||
            action.route_directory_descriptor() != -1) {
            return FinalizationReportStoreErrorV1::
                kTargetMismatch;
        }
        chain.first_parent_name = ".";
        chain.first_parent.Reset(
            OpenAtNoIntr(
                chain.root.get(),
                ".",
                O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                    O_NONBLOCK | O_CLOEXEC | O_NOATIME));
        if (chain.first_parent.get() < 0 ||
            ::fstat(
                chain.first_parent.get(),
                &chain.first_parent_status) != 0 ||
            !SameInode(
                chain.root_status,
                chain.first_parent_status)) {
            return FinalizationReportStoreErrorV1::
                kTargetMismatch;
        }
        chain.route_or_audit_name = "reserve-audit";
        chain.report_parent_name = "emergency-reports";
        error = OpenDirectoryChild(
            chain.first_parent.get(),
            chain.first_parent_status,
            chain.route_or_audit_name,
            &chain.route_or_audit,
            &chain.route_or_audit_status);
        if (error !=
            FinalizationReportStoreErrorV1::kNone) {
            return error;
        }
    }
    error = OpenDirectoryChild(
        chain.route_or_audit.get(),
        chain.route_or_audit_status,
        chain.report_parent_name,
        &chain.report_parent,
        &chain.report_parent_status);
    if (error !=
        FinalizationReportStoreErrorV1::kNone) {
        return error;
    }
    if (!ValidateContext(action, chain)) {
        return FinalizationReportStoreErrorV1::
            kTargetMismatch;
    }
    *output = std::move(chain);
    return FinalizationReportStoreErrorV1::kNone;
}

[[nodiscard]] bool RunHook(
    const FinalizationReportStoreHooksV1* hooks,
    FinalizationReportStoreMutationPointV1 point) noexcept {
    return hooks == nullptr ||
           hooks->allow == nullptr ||
           hooks->allow(point, hooks->context);
}

void AppendU16Le(
    std::vector<std::byte>* output,
    std::uint16_t value) {
    output->push_back(
        static_cast<std::byte>(value & 0xffU));
    output->push_back(
        static_cast<std::byte>((value >> 8U) & 0xffU));
}

void AppendU32Le(
    std::vector<std::byte>* output,
    std::uint32_t value) {
    for (unsigned shift = 0U; shift < 32U; shift += 8U) {
        output->push_back(
            static_cast<std::byte>(
                (value >> shift) & 0xffU));
    }
}

void AppendU64Le(
    std::vector<std::byte>* output,
    std::uint64_t value) {
    for (unsigned shift = 0U; shift < 64U; shift += 8U) {
        output->push_back(
            static_cast<std::byte>(
                (value >> shift) & 0xffU));
    }
}

template <std::size_t Size>
void AppendBytes(
    std::vector<std::byte>* output,
    const std::array<std::byte, Size>& value) {
    output->insert(
        output->end(), value.begin(), value.end());
}

void AppendPlanWire(
    std::vector<std::byte>* output,
    const FinalizationActionPlanV1& plan) {
    output->push_back(
        static_cast<std::byte>(plan.plan_version));
    output->push_back(
        static_cast<std::byte>(
            static_cast<std::uint8_t>(
                plan.object_type)));
    AppendU16Le(output, plan.plan_flags);
    AppendU32Le(output, plan.object_sequence);
    AppendU64Le(output, plan.range_start);
    AppendU64Le(output, plan.range_end_or_size);
    AppendBytes(output, plan.causal_id);
}

[[nodiscard]] bool ParsePlanPrestate(
    const FinalizationActionPlanV1& plan,
    PlanPrestate* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    constexpr std::uint16_t allowed =
        kFinalizationPlanExistingFinal |
        kFinalizationPlanIdenticalCompleteTmp |
        kFinalizationPlanDurabilityAlreadyProven |
        kFinalizationPlanCompleteTmpOnly;
    if ((plan.plan_flags & ~allowed) != 0U) {
        return false;
    }
    const bool existing =
        (plan.plan_flags &
         kFinalizationPlanExistingFinal) != 0U;
    const bool identical =
        (plan.plan_flags &
         kFinalizationPlanIdenticalCompleteTmp) != 0U;
    const bool durable =
        (plan.plan_flags &
         kFinalizationPlanDurabilityAlreadyProven) != 0U;
    const bool complete_tmp =
        (plan.plan_flags &
         kFinalizationPlanCompleteTmpOnly) != 0U;
    if ((identical && !existing) ||
        (durable && !existing) ||
        (complete_tmp &&
         (existing || identical || durable))) {
        return false;
    }
    *output =
        complete_tmp
            ? PlanPrestate::kCompleteTemporaryOnly
            : identical
                  ? PlanPrestate::
                        kExistingFinalAndIdenticalTemporary
                  : existing
                        ? PlanPrestate::kExistingFinal
                        : PlanPrestate::kAbsent;
    return true;
}

[[nodiscard]] bool ComputeExpectedPlanSha256(
    const RawReserveFinalizationActionV1& action,
    std::size_t exact_size,
    const RawV1Digest& exact_sha256,
    PlanPrestate prestate,
    ReserveStateV1Digest* output) {
    if (output == nullptr) {
        return false;
    }
    constexpr std::string_view domain =
        "L2FLOW_FINALIZATION_ACTION_PLAN_V1";
    std::vector<std::byte> encoded;
    encoded.reserve(256U);
    encoded.insert(
        encoded.end(),
        reinterpret_cast<const std::byte*>(
            domain.data()),
        reinterpret_cast<const std::byte*>(
            domain.data() + domain.size()));
    encoded.push_back(std::byte{0});
    const auto& key = action.key();
    AppendU32Le(
        &encoded, key.grant.source_stream_id);
    AppendU32Le(&encoded, key.grant.capture_date);
    AppendBytes(&encoded, key.grant.stream_day_id);
    AppendBytes(
        &encoded, key.grant.finalization_cycle_id);
    AppendU16Le(&encoded, key.action_id);
    AppendPlanWire(&encoded, action.plan());

    const auto AppendObject =
        [&](std::uint8_t role) {
            encoded.push_back(
                static_cast<std::byte>(role));
            AppendU64Le(
                &encoded,
                static_cast<std::uint64_t>(
                    exact_size));
            AppendBytes(&encoded, exact_sha256);
        };
    switch (prestate) {
    case PlanPrestate::kAbsent:
        encoded.push_back(std::byte{0});
        break;
    case PlanPrestate::kExistingFinal:
        encoded.push_back(std::byte{1});
        AppendObject(0x01U);
        break;
    case PlanPrestate::
        kExistingFinalAndIdenticalTemporary:
        encoded.push_back(std::byte{2});
        AppendObject(0x01U);
        AppendObject(0x02U);
        break;
    case PlanPrestate::kCompleteTemporaryOnly:
        encoded.push_back(std::byte{1});
        AppendObject(0x03U);
        break;
    }
    *output = l2flow::common::ComputeSha256(
        std::span<const std::byte>(
            encoded.data(), encoded.size()));
    return true;
}

[[nodiscard]] bool ValidateActionPlan(
    const RawReserveFinalizationActionV1& action,
    ReportDomain domain,
    std::size_t exact_size,
    const RawV1Digest& exact_sha256,
    PlanPrestate* prestate) {
    const auto& key = action.key();
    const auto& plan = action.plan();
    if (key.action_kind !=
            FinalizationActionKindV1::
                kFinalizationReport ||
        plan.plan_version != 1U ||
        plan.object_type !=
            FinalizationActionKindV1::
                kFinalizationReport ||
        plan.object_sequence != 0U ||
        plan.range_start != 0U ||
        plan.range_end_or_size !=
            MaximumBytes(domain) ||
        plan.causal_id !=
            key.grant.finalization_cycle_id ||
        key.grant.finalization_cycle_id !=
            action.generation_token()
                .finalization_cycle_id ||
        action.generation_token().reserve_state_uuid ==
            ReserveStateV1Identity{} ||
        action.generation_token().state_generation == 0U ||
        action.debit_generation() == 0U ||
        action.generation_token()
                .recovery_attempt_id !=
            action.executor_instance() ||
        !ParsePlanPrestate(plan, prestate)) {
        return false;
    }
    ReserveStateV1Digest expected{};
    return ComputeExpectedPlanSha256(
               action,
               exact_size,
               exact_sha256,
               *prestate,
               &expected) &&
           expected == key.object_plan_sha256;
}

[[nodiscard]] bool SameNamespace(
    const RawManifestNamespaceV1& left,
    const RawManifestNamespaceV1& right) noexcept {
    return left.source_stream_id ==
               right.source_stream_id &&
           left.capture_date == right.capture_date &&
           left.stream_day_id == right.stream_day_id;
}

[[nodiscard]] bool ValidateRoutedCapability(
    const RawReserveFinalizationActionV1& action,
    const BuiltFinalizationReportV1& built) noexcept {
    const auto& report = built.model();
    std::string encoded;
    std::string filename;
    const std::string_view bytes =
        built.canonical_jcs();
    const auto& key = action.key();
    const std::uint8_t expected_flags =
        report.result ==
                FinalizationReportResultV1::kSealedRaw
            ? kReserveGrantRawFinalization
            : kReserveGrantRawAnchorOnly;
    return ValidateFinalizationReportV1(report) ==
               FinalizationReportV1Error::kNone &&
           EncodeFinalizationReportV1Jcs(
               report, &encoded) ==
               FinalizationReportV1Error::kNone &&
           encoded == bytes &&
           FinalizationReportV1Filename(
               report, &filename) ==
               FinalizationReportV1Error::kNone &&
           filename == built.filename() &&
           !bytes.empty() &&
           bytes.size() <=
               kFinalizationReportV1MaximumBytes &&
           l2flow::common::ComputeSha256(bytes) ==
               built.report_sha256() &&
           action.grant_flags() == expected_flags &&
           report.reserve_state_uuid ==
               action.generation_token()
                   .reserve_state_uuid &&
           report.finalization_cycle_id ==
               key.grant.finalization_cycle_id &&
           report.immutable_grant_sha256 ==
               action.immutable_grant_sha256() &&
           report.namespace_identity.source_stream_id ==
               key.grant.source_stream_id &&
           report.namespace_identity.capture_date ==
               key.grant.capture_date &&
           report.namespace_identity.stream_day_id ==
               key.grant.stream_day_id &&
           report.ack_status == key.grant.ack_status &&
           report.ack_writer_instance ==
               key.grant.ack_writer_instance &&
           action.generation_token()
                   .writer_instance_id ==
               key.grant.ack_writer_instance;
}

[[nodiscard]] bool ValidateScaffoldingCapability(
    const RawReserveFinalizationActionV1& action,
    const BuiltScaffoldingFinalizationReportV1&
        built) noexcept {
    const auto& report = built.model();
    std::string encoded;
    std::string filename;
    const std::string_view bytes =
        built.canonical_jcs();
    const auto& key = action.key();
    return ValidateScaffoldingFinalizationReportV1(
               report) ==
               ScaffoldingFinalizationReportV1Error::
                   kNone &&
           EncodeScaffoldingFinalizationReportV1Jcs(
               report, &encoded) ==
               ScaffoldingFinalizationReportV1Error::
                   kNone &&
           encoded == bytes &&
           ScaffoldingFinalizationReportV1Filename(
               report, &filename) ==
               ScaffoldingFinalizationReportV1Error::
                   kNone &&
           filename == built.filename() &&
           !bytes.empty() &&
           bytes.size() <=
               kScaffoldingFinalizationReportV1MaximumBytes &&
           l2flow::common::ComputeSha256(bytes) ==
               built.report_sha256() &&
           action.grant_flags() ==
               kReserveGrantScaffoldingOnly &&
           report.reserve_state_uuid ==
               action.generation_token()
                   .reserve_state_uuid &&
           report.finalization_cycle_id ==
               key.grant.finalization_cycle_id &&
           report.immutable_grant_sha256 ==
               action.immutable_grant_sha256() &&
           report.planned_namespace.source_stream_id ==
               key.grant.source_stream_id &&
           report.planned_namespace.capture_date ==
               key.grant.capture_date &&
           report.planned_namespace.stream_day_id ==
               key.grant.stream_day_id &&
           action.generation_token()
                   .writer_instance_id ==
               key.grant.ack_writer_instance;
}

[[nodiscard]] FinalizationReportStoreErrorV1
LoadCandidate(
    RawReserveFinalizationActionV1& action,
    const ParentChain& chain,
    std::string_view name,
    LoadedCandidate* output) {
    if (output == nullptr ||
        !ValidateContext(action, chain)) {
        return FinalizationReportStoreErrorV1::
            kAuthorizationRejected;
    }
    const std::string owned_name(name);
    ScopedFd fd(
        OpenAtNoIntr(
            chain.report_parent.get(),
            owned_name.c_str(),
            O_RDWR | O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME));
    if (fd.get() < 0) {
        return errno == ENOENT
                   ? FinalizationReportStoreErrorV1::
                         kPublishConflict
                   : FinalizationReportStoreErrorV1::
                         kUnsafeCandidate;
    }
    struct stat before {};
    const int flags = ::fcntl(fd.get(), F_GETFL);
    if (::fstat(fd.get(), &before) != 0 ||
        !IsSafeFile(
            before,
            chain.report_parent_status,
            MaximumBytes(chain.domain)) ||
        flags < 0 ||
        (flags & O_ACCMODE) != O_RDWR ||
        (flags & O_APPEND) != 0 ||
        !NameMatchesDescriptor(
            chain.report_parent.get(),
            owned_name.c_str(),
            fd.get(),
            &before)) {
        return FinalizationReportStoreErrorV1::
            kUnsafeCandidate;
    }
    std::string bytes;
    if (!ReadExactString(
            fd.get(),
            static_cast<std::size_t>(
                before.st_size),
            &bytes)) {
        return FinalizationReportStoreErrorV1::
            kReadbackFailure;
    }
    struct stat after {};
    if (::fstat(fd.get(), &after) != 0 ||
        !IsSafeFile(
            after,
            chain.report_parent_status,
            MaximumBytes(chain.domain)) ||
        !SameInode(before, after) ||
        before.st_size != after.st_size ||
        before.st_blocks != after.st_blocks ||
        !NameMatchesDescriptor(
            chain.report_parent.get(),
            owned_name.c_str(),
            fd.get(),
            &after) ||
        !ValidateContext(action, chain)) {
        return FinalizationReportStoreErrorV1::
            kUnsafeCandidate;
    }
    LoadedCandidate loaded{};
    loaded.fd = std::move(fd);
    loaded.status = after;
    loaded.bytes = std::move(bytes);
    *output = std::move(loaded);
    return FinalizationReportStoreErrorV1::kNone;
}

[[nodiscard]] bool ValidateHistoricalFinal(
    ReportDomain domain,
    std::string_view name,
    std::string_view bytes,
    const FinalizationReportV1* routed_model) {
    std::string encoded;
    std::string derived;
    if (domain == ReportDomain::kRouted) {
        if (routed_model == nullptr) {
            return false;
        }
        FinalizationReportV1 parsed{};
        return ParseFinalizationReportV1Jcs(
                   bytes, &parsed) ==
                   FinalizationReportV1Error::kNone &&
               EncodeFinalizationReportV1Jcs(
                   parsed, &encoded) ==
                   FinalizationReportV1Error::kNone &&
               encoded == bytes &&
               FinalizationReportV1Filename(
                   parsed, &derived) ==
                   FinalizationReportV1Error::kNone &&
               derived == name &&
               SameNamespace(
                   parsed.namespace_identity,
                   routed_model->namespace_identity);
    }
    ScaffoldingFinalizationReportV1 parsed{};
    return ParseScaffoldingFinalizationReportV1Jcs(
               bytes, &parsed) ==
               ScaffoldingFinalizationReportV1Error::
                   kNone &&
           EncodeScaffoldingFinalizationReportV1Jcs(
               parsed, &encoded) ==
               ScaffoldingFinalizationReportV1Error::
                   kNone &&
           encoded == bytes &&
           ScaffoldingFinalizationReportV1Filename(
               parsed, &derived) ==
               ScaffoldingFinalizationReportV1Error::
                   kNone &&
           derived == name;
}

[[nodiscard]] FinalizationReportStoreErrorV1
InventoryCandidates(
    RawReserveFinalizationActionV1& action,
    const ParentChain& chain,
    const std::string& final_name,
    const std::string& temporary_name,
    std::string_view exact_bytes,
    const FinalizationReportV1* routed_model,
    CandidateInventory* output) {
    if (output == nullptr ||
        !ValidateContext(action, chain)) {
        return FinalizationReportStoreErrorV1::
            kAuthorizationRejected;
    }
    ScopedFd scan(
        OpenAtNoIntr(
            chain.report_parent.get(),
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC | O_NOATIME));
    struct stat scan_status {};
    if (scan.get() < 0 ||
        ::fstat(scan.get(), &scan_status) != 0 ||
        !SameInode(
            scan_status,
            chain.report_parent_status)) {
        return FinalizationReportStoreErrorV1::
            kUnsafeReportParent;
    }
    DIR* const native = ::fdopendir(scan.Release());
    if (native == nullptr) {
        return FinalizationReportStoreErrorV1::
            kUnsafeReportParent;
    }
    ScopedDir entries(native);
    CandidateInventory inventory{};
    for (;;) {
        errno = 0;
        dirent* const entry = ::readdir(entries.get());
        if (entry == nullptr) {
            if (errno != 0) {
                return FinalizationReportStoreErrorV1::
                    kUnsafeReportParent;
            }
            break;
        }
        const std::string_view name(entry->d_name);
        if (name == "." || name == "..") {
            continue;
        }
        const bool is_final =
            IsFinalName(chain.domain, name);
        const bool is_temporary =
            IsTemporaryName(chain.domain, name);
        if (StartsLikeCandidate(chain.domain, name) &&
            !is_final && !is_temporary) {
            return FinalizationReportStoreErrorV1::
                kMalformedCandidateName;
        }
        if (!is_final && !is_temporary) {
            continue;
        }
        if (inventory.count ==
            kFinalizationReportStoreMaximumCandidatesV1) {
            return FinalizationReportStoreErrorV1::
                kCandidateLimitExceeded;
        }
        ++inventory.count;
        inventory.names.emplace_back(name);
        inventory.final_present =
            inventory.final_present ||
            name == final_name;
        inventory.temporary_present =
            inventory.temporary_present ||
            name == temporary_name;
    }
    std::sort(
        inventory.names.begin(),
        inventory.names.end());
    if (!ValidateContext(action, chain)) {
        return FinalizationReportStoreErrorV1::
            kAuthorizationRejected;
    }

    const std::string_view scaffolding_cycle_prefix =
        chain.domain == ReportDomain::kScaffolding
            ? std::string_view(final_name).substr(
                  0U,
                  std::string_view("scaffolding-").size() +
                      32U + 1U)
            : std::string_view{};
    for (const std::string& name : inventory.names) {
        LoadedCandidate loaded{};
        const auto error = LoadCandidate(
            action, chain, name, &loaded);
        if (error !=
            FinalizationReportStoreErrorV1::kNone) {
            return error;
        }
        if (name == final_name) {
            if (loaded.bytes != exact_bytes) {
                return FinalizationReportStoreErrorV1::
                    kCandidateConflict;
            }
            inventory.final = std::move(loaded);
            continue;
        }
        if (name == temporary_name) {
            if (loaded.bytes != exact_bytes) {
                return FinalizationReportStoreErrorV1::
                    kCandidateConflict;
            }
            inventory.temporary =
                std::move(loaded);
            continue;
        }
        if (IsTemporaryName(chain.domain, name) ||
            (chain.domain ==
                 ReportDomain::kScaffolding &&
             std::string_view(name).starts_with(
                 scaffolding_cycle_prefix)) ||
            !ValidateHistoricalFinal(
                chain.domain,
                name,
                loaded.bytes,
                routed_model)) {
            return FinalizationReportStoreErrorV1::
                kCandidateConflict;
        }
    }
    *output = std::move(inventory);
    return FinalizationReportStoreErrorV1::kNone;
}

[[nodiscard]] bool CandidateStillExact(
    RawReserveFinalizationActionV1& action,
    const ParentChain& chain,
    std::string_view name,
    std::string_view exact_bytes,
    LoadedCandidate* candidate) {
    if (candidate == nullptr ||
        candidate->fd.get() < 0 ||
        !ValidateContext(action, chain)) {
        return false;
    }
    const std::string owned_name(name);
    struct stat before {};
    std::string bytes;
    struct stat after {};
    return ::fstat(candidate->fd.get(), &before) == 0 &&
           IsSafeFile(
               before,
               chain.report_parent_status,
               MaximumBytes(chain.domain)) &&
           SameInode(before, candidate->status) &&
           before.st_size == candidate->status.st_size &&
           NameMatchesDescriptor(
               chain.report_parent.get(),
               owned_name.c_str(),
               candidate->fd.get(),
               &before) &&
           ReadExactString(
               candidate->fd.get(),
               static_cast<std::size_t>(
                   before.st_size),
               &bytes) &&
           bytes == exact_bytes &&
           ::fstat(candidate->fd.get(), &after) == 0 &&
           IsSafeFile(
               after,
               chain.report_parent_status,
               MaximumBytes(chain.domain)) &&
           SameInode(before, after) &&
           before.st_size == after.st_size &&
           before.st_blocks == after.st_blocks &&
           NameMatchesDescriptor(
               chain.report_parent.get(),
               owned_name.c_str(),
               candidate->fd.get(),
               &after) &&
           ValidateContext(action, chain) &&
           ((candidate->status = after), true);
}

[[nodiscard]] bool AddPositiveBlockDelta(
    const struct stat& before,
    const struct stat& after,
    std::uint64_t* charge) noexcept {
    if (charge == nullptr ||
        before.st_blocks < 0 ||
        after.st_blocks < 0) {
        return false;
    }
    const std::uint64_t old_blocks =
        static_cast<std::uint64_t>(
            before.st_blocks);
    const std::uint64_t new_blocks =
        static_cast<std::uint64_t>(
            after.st_blocks);
    if (new_blocks <= old_blocks) {
        return true;
    }
    const std::uint64_t delta =
        new_blocks - old_blocks;
    if (delta >
            std::numeric_limits<std::uint64_t>::max() /
                512U ||
        *charge >
            std::numeric_limits<std::uint64_t>::max() -
                (delta * 512U)) {
        return false;
    }
    *charge += delta * 512U;
    return true;
}

[[nodiscard]] FinalizationReportStoreErrorV1
SyncCandidate(
    RawReserveFinalizationActionV1& action,
    const ParentChain& chain,
    std::string_view name,
    std::string_view exact_bytes,
    const FinalizationReportStoreHooksV1* hooks,
    LoadedCandidate* candidate,
    std::uint64_t* allocation_charge) {
    if (candidate == nullptr ||
        !CandidateStillExact(
            action,
            chain,
            name,
            exact_bytes,
            candidate) ||
        !ValidateContext(action, chain)) {
        return FinalizationReportStoreErrorV1::
            kCandidateConflict;
    }
    if (!RunHook(
            hooks,
            FinalizationReportStoreMutationPointV1::
                kBeforeFileSync)) {
        return FinalizationReportStoreErrorV1::
            kInjectedInterruption;
    }
    const struct stat before = candidate->status;
    if (!ValidateContext(action, chain) ||
        !FsyncNoIntr(candidate->fd.get())) {
        return FinalizationReportStoreErrorV1::
            kSyncFailure;
    }
    struct stat after {};
    const std::string owned_name(name);
    if (::fstat(candidate->fd.get(), &after) != 0 ||
        !IsSafeFile(
            after,
            chain.report_parent_status,
            MaximumBytes(chain.domain)) ||
        !SameInode(before, after) ||
        before.st_size != after.st_size ||
        !NameMatchesDescriptor(
            chain.report_parent.get(),
            owned_name.c_str(),
            candidate->fd.get(),
            &after) ||
        !ValidateContext(action, chain) ||
        !AddPositiveBlockDelta(
            before, after, allocation_charge)) {
        return FinalizationReportStoreErrorV1::
            kSyncFailure;
    }
    candidate->status = after;
    return FinalizationReportStoreErrorV1::kNone;
}

[[nodiscard]] bool WriteExactAuthorized(
    RawReserveFinalizationActionV1& action,
    const ParentChain& chain,
    const std::string& name,
    int fd,
    std::string_view bytes) noexcept {
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        struct stat status {};
        if (!ValidateContext(action, chain) ||
            ::fstat(fd, &status) != 0 ||
            !IsSafeFile(
                status,
                chain.report_parent_status,
                MaximumBytes(chain.domain)) ||
            !NameMatchesDescriptor(
                chain.report_parent.get(),
                name.c_str(),
                fd,
                &status)) {
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
    return ValidateContext(action, chain) &&
           NameMatchesDescriptor(
               chain.report_parent.get(),
               name.c_str(),
               fd);
}

[[nodiscard]] bool StateAllowedByPlan(
    PlanPrestate prestate,
    bool final_present,
    bool temporary_present) noexcept {
    switch (prestate) {
    case PlanPrestate::kAbsent:
        // A byte-identical final+tmp pair can be produced only inside the
        // still-DEBITED publication FSM (for example, a replacement
        // executor reaches an EEXIST window).  The exact current action may
        // remove that tmp and dirsync; no COMPLETE action receives this
        // capability.
        return true;
    case PlanPrestate::kExistingFinal:
        return final_present && !temporary_present;
    case PlanPrestate::
        kExistingFinalAndIdenticalTemporary:
        // final-only is the legal unlink-before-parent-fsync descendant.
        return final_present;
    case PlanPrestate::kCompleteTemporaryOnly:
        // final-only is the legal rename-before-parent-fsync descendant.
        return final_present != temporary_present;
    }
    return false;
}

[[nodiscard]] PublicationEvidence PublishAt(
    RawReserveFinalizationActionV1& action,
    ParentChain& chain,
    std::string_view exact_bytes,
    const RawV1Digest& exact_sha256,
    const std::string& final_name,
    const FinalizationReportV1* routed_model,
    PlanPrestate prestate,
    const FinalizationReportStoreHooksV1* hooks) {
    PublicationEvidence result{};
    const std::string temporary_name =
        "." + final_name +
        std::string(TemporarySuffix(chain.domain));
    CandidateInventory inventory{};
    result.error = InventoryCandidates(
        action,
        chain,
        final_name,
        temporary_name,
        exact_bytes,
        routed_model,
        &inventory);
    result.observed_candidate_count = inventory.count;
    if (result.error !=
        FinalizationReportStoreErrorV1::kNone) {
        return result;
    }
    if (!StateAllowedByPlan(
            prestate,
            inventory.final_present,
            inventory.temporary_present)) {
        result.error =
            FinalizationReportStoreErrorV1::
                kPlanMismatch;
        return result;
    }
    const bool initially_absent =
        !inventory.final_present &&
        !inventory.temporary_present;
    if (initially_absent &&
        inventory.count >
            kFinalizationReportStoreMaximumCandidatesV1 -
                2U) {
        result.error =
            FinalizationReportStoreErrorV1::
                kCandidateLimitExceeded;
        return result;
    }
    if (initially_absent &&
        (action.inode_cap() == 0U ||
         action.byte_cap() < exact_bytes.size())) {
        result.error =
            FinalizationReportStoreErrorV1::
                kPlanMismatch;
        return result;
    }

    LoadedCandidate final =
        std::move(inventory.final);
    LoadedCandidate temporary =
        std::move(inventory.temporary);
    std::uint64_t allocation_charge = 0U;
    bool created_new = false;
    bool adopted = false;

    if (!inventory.final_present) {
        if (!inventory.temporary_present) {
            if (!ValidateContext(action, chain) ||
                !NameAbsent(
                    chain.report_parent.get(),
                    final_name.c_str()) ||
                !NameAbsent(
                    chain.report_parent.get(),
                    temporary_name.c_str())) {
                result.error =
                    FinalizationReportStoreErrorV1::
                        kPublishConflict;
                return result;
            }
            if (!RunHook(
                    hooks,
                    FinalizationReportStoreMutationPointV1::
                        kBeforeTemporaryCreate)) {
                result.error =
                    FinalizationReportStoreErrorV1::
                        kInjectedInterruption;
                return result;
            }
            if (!ValidateContext(action, chain)) {
                result.error =
                    FinalizationReportStoreErrorV1::
                        kAuthorizationRejected;
                return result;
            }
            ScopedFd created(
                OpenAtNoIntr(
                    chain.report_parent.get(),
                    temporary_name.c_str(),
                    O_RDWR | O_CREAT | O_EXCL |
                        O_NOFOLLOW | O_NONBLOCK |
                        O_CLOEXEC,
                    0600));
            struct stat created_status {};
            if (created.get() < 0) {
                result.error =
                    errno == EEXIST
                        ? FinalizationReportStoreErrorV1::
                              kPublishConflict
                        : FinalizationReportStoreErrorV1::
                              kTemporaryCreate;
                return result;
            }
            if (::fstat(
                    created.get(),
                    &created_status) != 0 ||
                !IsSafeFile(
                    created_status,
                    chain.report_parent_status,
                    MaximumBytes(chain.domain)) ||
                created_status.st_size != 0 ||
                !NameMatchesDescriptor(
                    chain.report_parent.get(),
                    temporary_name.c_str(),
                    created.get(),
                    &created_status)) {
                result.error =
                    FinalizationReportStoreErrorV1::
                        kWriteFailure;
                return result;
            }
            const struct stat before_write =
                created_status;
            if (!RunHook(
                    hooks,
                    FinalizationReportStoreMutationPointV1::
                        kBeforeWrite)) {
                result.error =
                    FinalizationReportStoreErrorV1::
                        kInjectedInterruption;
                return result;
            }
            if (!WriteExactAuthorized(
                    action,
                    chain,
                    temporary_name,
                    created.get(),
                    exact_bytes) ||
                ::fstat(
                    created.get(),
                    &created_status) != 0 ||
                !IsSafeFile(
                    created_status,
                    chain.report_parent_status,
                    MaximumBytes(chain.domain)) ||
                created_status.st_size !=
                    static_cast<off_t>(
                        exact_bytes.size()) ||
                !NameMatchesDescriptor(
                    chain.report_parent.get(),
                    temporary_name.c_str(),
                    created.get(),
                    &created_status) ||
                !AddPositiveBlockDelta(
                    before_write,
                    created_status,
                    &allocation_charge)) {
                result.error =
                    FinalizationReportStoreErrorV1::
                        kWriteFailure;
                return result;
            }
            temporary.fd = std::move(created);
            temporary.status = created_status;
            temporary.bytes =
                std::string(exact_bytes);
            created_new = true;
        } else {
            adopted = true;
        }

        result.error = SyncCandidate(
            action,
            chain,
            temporary_name,
            exact_bytes,
            hooks,
            &temporary,
            &allocation_charge);
        if (result.error !=
            FinalizationReportStoreErrorV1::kNone) {
            return result;
        }
        result.file_synced = true;
        if (!RunHook(
                hooks,
                FinalizationReportStoreMutationPointV1::
                    kBeforePublishRename)) {
            result.error =
                FinalizationReportStoreErrorV1::
                    kInjectedInterruption;
            return result;
        }
        if (!ValidateContext(action, chain) ||
            !NameAbsent(
                chain.report_parent.get(),
                final_name.c_str()) ||
            !RenameNoReplace(
                chain.report_parent.get(),
                temporary_name.c_str(),
                final_name.c_str()) ||
            !ValidateContext(action, chain) ||
            !NameMatchesDescriptor(
                chain.report_parent.get(),
                final_name.c_str(),
                temporary.fd.get()) ||
            !NameAbsent(
                chain.report_parent.get(),
                temporary_name.c_str())) {
            result.error =
                FinalizationReportStoreErrorV1::
                    kPublishConflict;
            return result;
        }
        final = std::move(temporary);
        result.disposition =
            adopted
                ? FinalizationReportStoreDispositionV1::
                      kAdoptedCompleteTemporary
                : FinalizationReportStoreDispositionV1::
                      kPublishedNew;
    } else {
        result.disposition =
            FinalizationReportStoreDispositionV1::
                kAcceptedExistingFinal;
    }

    // A DEBITED report action always re-establishes the final-file barrier,
    // even when the same descriptor was already synced under its tmp name.
    result.error = SyncCandidate(
        action,
        chain,
        final_name,
        exact_bytes,
        hooks,
        &final,
        &allocation_charge);
    if (result.error !=
        FinalizationReportStoreErrorV1::kNone) {
        return result;
    }
    result.file_synced = true;

    if (inventory.final_present &&
        inventory.temporary_present) {
        result.error = SyncCandidate(
            action,
            chain,
            temporary_name,
            exact_bytes,
            hooks,
            &temporary,
            &allocation_charge);
        if (result.error !=
            FinalizationReportStoreErrorV1::kNone) {
            return result;
        }
        if (!RunHook(
                hooks,
                FinalizationReportStoreMutationPointV1::
                    kBeforeIdenticalTemporaryCleanup)) {
            result.error =
                FinalizationReportStoreErrorV1::
                    kInjectedInterruption;
            return result;
        }
        if (!ValidateContext(action, chain) ||
            !CandidateStillExact(
                action,
                chain,
                temporary_name,
                exact_bytes,
                &temporary) ||
            !UnlinkAtNoIntr(
                chain.report_parent.get(),
                temporary_name.c_str()) ||
            !NameAbsent(
                chain.report_parent.get(),
                temporary_name.c_str()) ||
            !CandidateStillExact(
                action,
                chain,
                final_name,
                exact_bytes,
                &final)) {
            result.error =
                FinalizationReportStoreErrorV1::
                    kCandidateConflict;
            return result;
        }
        result.disposition =
            FinalizationReportStoreDispositionV1::
                kAcceptedExistingFinalAndRemovedIdenticalTemporary;
    }

    if (!RunHook(
            hooks,
            FinalizationReportStoreMutationPointV1::
                kBeforeParentSync)) {
        result.error =
            FinalizationReportStoreErrorV1::
                kInjectedInterruption;
        return result;
    }
    const struct stat parent_before =
        chain.report_parent_status;
    if (!ValidateContext(action, chain) ||
        !FsyncNoIntr(chain.report_parent.get())) {
        result.error =
            FinalizationReportStoreErrorV1::
                kSyncFailure;
        return result;
    }
    struct stat parent_after {};
    if (::fstat(
            chain.report_parent.get(),
            &parent_after) != 0 ||
        !IsSafeDirectory(parent_after) ||
        !SameInode(
            parent_before, parent_after) ||
        !ValidateContext(action, chain) ||
        !AddPositiveBlockDelta(
            parent_before,
            parent_after,
            &allocation_charge)) {
        result.error =
            FinalizationReportStoreErrorV1::
                kSyncFailure;
        return result;
    }
    chain.report_parent_status = parent_after;
    result.directory_synced = true;

    if (allocation_charge > action.byte_cap()) {
        result.error =
            FinalizationReportStoreErrorV1::
                kPlanMismatch;
        return result;
    }
    if (created_new && action.inode_cap() < 1U) {
        result.error =
            FinalizationReportStoreErrorV1::
                kPlanMismatch;
        return result;
    }
    if (!RunHook(
            hooks,
            FinalizationReportStoreMutationPointV1::
                kBeforeReadback)) {
        result.error =
            FinalizationReportStoreErrorV1::
                kInjectedInterruption;
        return result;
    }
    if (!CandidateStillExact(
            action,
            chain,
            final_name,
            exact_bytes,
            &final) ||
        l2flow::common::ComputeSha256(
            std::string_view(final.bytes)) !=
            exact_sha256 ||
        !NameAbsent(
            chain.report_parent.get(),
            temporary_name.c_str())) {
        result.error =
            FinalizationReportStoreErrorV1::
                kReadbackFailure;
        return result;
    }

    CandidateInventory verified{};
    result.error = InventoryCandidates(
        action,
        chain,
        final_name,
        temporary_name,
        exact_bytes,
        routed_model,
        &verified);
    if (result.error !=
            FinalizationReportStoreErrorV1::kNone ||
        !verified.final_present ||
        verified.temporary_present) {
        if (result.error ==
            FinalizationReportStoreErrorV1::kNone) {
            result.error =
                FinalizationReportStoreErrorV1::
                    kReadbackFailure;
        }
        return result;
    }
    std::vector<std::string> expected_names =
        inventory.names;
    const auto temporary_position = std::find(
        expected_names.begin(),
        expected_names.end(),
        temporary_name);
    if (temporary_position != expected_names.end()) {
        expected_names.erase(temporary_position);
    }
    if (std::find(
            expected_names.begin(),
            expected_names.end(),
            final_name) == expected_names.end()) {
        expected_names.push_back(final_name);
        std::sort(
            expected_names.begin(),
            expected_names.end());
    }
    if (verified.names != expected_names) {
        result.error =
            FinalizationReportStoreErrorV1::
                kCandidateConflict;
        return result;
    }
    result.final = std::move(final);
    return result;
}

[[nodiscard]] bool ValidateRetainedReport(
    int parent_fd,
    const struct stat& parent_status,
    int report_fd,
    const struct stat& expected_report,
    std::string_view filename,
    std::string_view temporary_suffix,
    const RawV1Digest& expected_sha256,
    std::size_t expected_size,
    std::size_t maximum_size) {
    if (parent_fd < 0 || report_fd < 0 ||
        filename.empty() || expected_size == 0U ||
        expected_size > maximum_size) {
        return false;
    }
    struct stat before {};
    std::string bytes;
    struct stat after {};
    const std::string owned_name(filename);
    const std::string temporary_name =
        "." + owned_name +
        std::string(temporary_suffix);
    return ::fstat(report_fd, &before) == 0 &&
           IsSafeFile(
               before, parent_status, maximum_size) &&
           SameInode(before, expected_report) &&
           static_cast<std::uint64_t>(
               before.st_size) == expected_size &&
           NameMatchesDescriptor(
               parent_fd,
               owned_name.c_str(),
               report_fd,
               &before) &&
           NameAbsent(
               parent_fd, temporary_name.c_str()) &&
           ReadExactString(
               report_fd, expected_size, &bytes) &&
           l2flow::common::ComputeSha256(
               std::string_view(bytes)) ==
               expected_sha256 &&
           ::fstat(report_fd, &after) == 0 &&
           IsSafeFile(
               after, parent_status, maximum_size) &&
           SameInode(before, after) &&
           before.st_size == after.st_size &&
           before.st_blocks == after.st_blocks &&
           NameMatchesDescriptor(
               parent_fd,
               owned_name.c_str(),
               report_fd,
               &after);
}

}  // namespace

std::string_view
FinalizationReportStoreErrorV1Name(
    FinalizationReportStoreErrorV1 error) noexcept {
    switch (error) {
    case FinalizationReportStoreErrorV1::kNone:
        return "none";
    case FinalizationReportStoreErrorV1::kInvalidArgument:
        return "invalid_argument";
    case FinalizationReportStoreErrorV1::
        kAuthorizationRejected:
        return "authorization_rejected";
    case FinalizationReportStoreErrorV1::kPlanMismatch:
        return "plan_mismatch";
    case FinalizationReportStoreErrorV1::kTargetMismatch:
        return "target_mismatch";
    case FinalizationReportStoreErrorV1::
        kReportParentMissing:
        return "report_parent_missing";
    case FinalizationReportStoreErrorV1::
        kUnsafeReportParent:
        return "unsafe_report_parent";
    case FinalizationReportStoreErrorV1::
        kCandidateLimitExceeded:
        return "candidate_limit_exceeded";
    case FinalizationReportStoreErrorV1::
        kMalformedCandidateName:
        return "malformed_candidate_name";
    case FinalizationReportStoreErrorV1::kUnsafeCandidate:
        return "unsafe_candidate";
    case FinalizationReportStoreErrorV1::
        kCandidateConflict:
        return "candidate_conflict";
    case FinalizationReportStoreErrorV1::
        kTemporaryCreate:
        return "temporary_create";
    case FinalizationReportStoreErrorV1::kWriteFailure:
        return "write_failure";
    case FinalizationReportStoreErrorV1::kSyncFailure:
        return "sync_failure";
    case FinalizationReportStoreErrorV1::
        kPublishConflict:
        return "publish_conflict";
    case FinalizationReportStoreErrorV1::kReadbackFailure:
        return "readback_failure";
    case FinalizationReportStoreErrorV1::
        kInjectedInterruption:
        return "injected_interruption";
    case FinalizationReportStoreErrorV1::
        kAllocationFailure:
        return "allocation_failure";
    }
    return "unknown";
}

FinalizationReportReceiptV1::
FinalizationReportReceiptV1(
    ReserveFinalizationActionKeyV1 key,
    RawReserveGenerationActionTokenV1 token,
    ReserveStateV1Digest immutable_grant_sha256,
    FinalizationActionPlanV1 plan,
    std::uint8_t grant_flags,
    std::uint64_t byte_cap,
    std::uint32_t inode_cap,
    std::uint64_t debit_generation,
    ReserveStateV1Identity executor_instance,
    RawV1Digest report_sha256,
    std::size_t byte_count,
    std::string filename,
    std::string stream_slug,
    int raw_root_fd,
    int capture_date_fd,
    int route_fd,
    int report_parent_fd,
    int report_fd,
    std::uint64_t raw_root_device,
    std::uint64_t raw_root_inode,
    std::uint64_t capture_date_device,
    std::uint64_t capture_date_inode,
    std::uint64_t route_device,
    std::uint64_t route_inode,
    std::uint64_t report_parent_device,
    std::uint64_t report_parent_inode,
    std::uint64_t report_device,
    std::uint64_t report_inode,
    std::uint64_t report_blocks) noexcept
    : key_(std::move(key)),
      token_(std::move(token)),
      immutable_grant_sha256_(immutable_grant_sha256),
      plan_(plan),
      grant_flags_(grant_flags),
      byte_cap_(byte_cap),
      inode_cap_(inode_cap),
      debit_generation_(debit_generation),
      executor_instance_(executor_instance),
      report_sha256_(report_sha256),
      byte_count_(byte_count),
      filename_(std::move(filename)),
      stream_slug_(std::move(stream_slug)),
      raw_root_fd_(raw_root_fd),
      capture_date_fd_(capture_date_fd),
      route_fd_(route_fd),
      report_parent_fd_(report_parent_fd),
      report_fd_(report_fd),
      raw_root_device_(raw_root_device),
      raw_root_inode_(raw_root_inode),
      capture_date_device_(capture_date_device),
      capture_date_inode_(capture_date_inode),
      route_device_(route_device),
      route_inode_(route_inode),
      report_parent_device_(report_parent_device),
      report_parent_inode_(report_parent_inode),
      report_device_(report_device),
      report_inode_(report_inode),
      report_blocks_(report_blocks) {}

FinalizationReportReceiptV1::
~FinalizationReportReceiptV1() {
    if (report_fd_ >= 0) {
        static_cast<void>(::close(report_fd_));
    }
    if (report_parent_fd_ >= 0) {
        static_cast<void>(::close(report_parent_fd_));
    }
    if (route_fd_ >= 0) {
        static_cast<void>(::close(route_fd_));
    }
    if (capture_date_fd_ >= 0) {
        static_cast<void>(::close(capture_date_fd_));
    }
    if (raw_root_fd_ >= 0) {
        static_cast<void>(::close(raw_root_fd_));
    }
}

bool FinalizationReportReceiptV1::Validate(
    std::string* diagnostic) const noexcept {
    try {
        struct stat root {};
        struct stat capture {};
        struct stat route {};
        struct stat parent {};
        struct stat report {};
        const std::string capture_name =
            "capture_date=" +
            std::to_string(key_.grant.capture_date);
        const std::string route_name =
            "stream=" +
            std::to_string(
                key_.grant.source_stream_id) +
            "-" + stream_slug_;
        const bool valid =
            !consumed_ &&
            raw_root_fd_ >= 0 &&
            capture_date_fd_ >= 0 &&
            route_fd_ >= 0 &&
            report_parent_fd_ >= 0 &&
            report_fd_ >= 0 &&
            ::fstat(raw_root_fd_, &root) == 0 &&
            IsSafeDirectory(root) &&
            Device(root) == raw_root_device_ &&
            Inode(root) == raw_root_inode_ &&
            ::fstat(capture_date_fd_, &capture) == 0 &&
            IsSafeDirectory(capture) &&
            Device(capture) == capture_date_device_ &&
            Inode(capture) == capture_date_inode_ &&
            NameMatchesDescriptor(
                raw_root_fd_,
                capture_name.c_str(),
                capture_date_fd_,
                &capture) &&
            ::fstat(route_fd_, &route) == 0 &&
            IsSafeDirectory(route) &&
            Device(route) == route_device_ &&
            Inode(route) == route_inode_ &&
            NameMatchesDescriptor(
                capture_date_fd_,
                route_name.c_str(),
                route_fd_,
                &route) &&
            ::fstat(report_parent_fd_, &parent) == 0 &&
            IsSafeDirectory(parent) &&
            Device(parent) ==
                report_parent_device_ &&
            Inode(parent) ==
                report_parent_inode_ &&
            NameMatchesDescriptor(
                route_fd_,
                "maintenance",
                report_parent_fd_,
                &parent) &&
            ::fstat(report_fd_, &report) == 0 &&
            Device(report) == report_device_ &&
            Inode(report) == report_inode_ &&
            Blocks(report) == report_blocks_ &&
            ValidateRetainedReport(
                report_parent_fd_,
                parent,
                report_fd_,
                report,
                filename_,
                kFinalizationReportV1TemporarySuffix,
                report_sha256_,
                byte_count_,
                kFinalizationReportV1MaximumBytes);
        SetDiagnostic(
            diagnostic,
            valid ? std::string_view{}
                  : std::string_view{
                        "finalization report receipt evidence is stale"});
        return valid;
    } catch (...) {
        SetDiagnostic(
            diagnostic,
            "cannot validate finalization report receipt");
        return false;
    }
}

ScaffoldingFinalizationReportReceiptV1::
ScaffoldingFinalizationReportReceiptV1(
    ReserveFinalizationActionKeyV1 key,
    RawReserveGenerationActionTokenV1 token,
    ReserveStateV1Digest immutable_grant_sha256,
    FinalizationActionPlanV1 plan,
    std::uint8_t grant_flags,
    std::uint64_t byte_cap,
    std::uint32_t inode_cap,
    std::uint64_t debit_generation,
    ReserveStateV1Identity executor_instance,
    RawV1Digest report_sha256,
    std::size_t byte_count,
    std::string filename,
    int raw_root_fd,
    int reserve_audit_fd,
    int report_parent_fd,
    int report_fd,
    std::uint64_t raw_root_device,
    std::uint64_t raw_root_inode,
    std::uint64_t reserve_audit_device,
    std::uint64_t reserve_audit_inode,
    std::uint64_t report_parent_device,
    std::uint64_t report_parent_inode,
    std::uint64_t report_device,
    std::uint64_t report_inode,
    std::uint64_t report_blocks) noexcept
    : key_(std::move(key)),
      token_(std::move(token)),
      immutable_grant_sha256_(immutable_grant_sha256),
      plan_(plan),
      grant_flags_(grant_flags),
      byte_cap_(byte_cap),
      inode_cap_(inode_cap),
      debit_generation_(debit_generation),
      executor_instance_(executor_instance),
      report_sha256_(report_sha256),
      byte_count_(byte_count),
      filename_(std::move(filename)),
      raw_root_fd_(raw_root_fd),
      reserve_audit_fd_(reserve_audit_fd),
      report_parent_fd_(report_parent_fd),
      report_fd_(report_fd),
      raw_root_device_(raw_root_device),
      raw_root_inode_(raw_root_inode),
      reserve_audit_device_(reserve_audit_device),
      reserve_audit_inode_(reserve_audit_inode),
      report_parent_device_(report_parent_device),
      report_parent_inode_(report_parent_inode),
      report_device_(report_device),
      report_inode_(report_inode),
      report_blocks_(report_blocks) {}

ScaffoldingFinalizationReportReceiptV1::
~ScaffoldingFinalizationReportReceiptV1() {
    if (report_fd_ >= 0) {
        static_cast<void>(::close(report_fd_));
    }
    if (report_parent_fd_ >= 0) {
        static_cast<void>(::close(report_parent_fd_));
    }
    if (reserve_audit_fd_ >= 0) {
        static_cast<void>(::close(reserve_audit_fd_));
    }
    if (raw_root_fd_ >= 0) {
        static_cast<void>(::close(raw_root_fd_));
    }
}

bool ScaffoldingFinalizationReportReceiptV1::
Validate(std::string* diagnostic) const noexcept {
    try {
        struct stat root {};
        struct stat audit {};
        struct stat parent {};
        struct stat report {};
        const bool valid =
            !consumed_ &&
            raw_root_fd_ >= 0 &&
            reserve_audit_fd_ >= 0 &&
            report_parent_fd_ >= 0 &&
            report_fd_ >= 0 &&
            ::fstat(raw_root_fd_, &root) == 0 &&
            IsSafeDirectory(root) &&
            Device(root) == raw_root_device_ &&
            Inode(root) == raw_root_inode_ &&
            ::fstat(reserve_audit_fd_, &audit) == 0 &&
            IsSafeDirectory(audit) &&
            Device(audit) ==
                reserve_audit_device_ &&
            Inode(audit) ==
                reserve_audit_inode_ &&
            NameMatchesDescriptor(
                raw_root_fd_,
                "reserve-audit",
                reserve_audit_fd_,
                &audit) &&
            ::fstat(report_parent_fd_, &parent) == 0 &&
            IsSafeDirectory(parent) &&
            Device(parent) ==
                report_parent_device_ &&
            Inode(parent) ==
                report_parent_inode_ &&
            NameMatchesDescriptor(
                reserve_audit_fd_,
                "emergency-reports",
                report_parent_fd_,
                &parent) &&
            ::fstat(report_fd_, &report) == 0 &&
            Device(report) == report_device_ &&
            Inode(report) == report_inode_ &&
            Blocks(report) == report_blocks_ &&
            ValidateRetainedReport(
                report_parent_fd_,
                parent,
                report_fd_,
                report,
                filename_,
                kScaffoldingFinalizationReportV1TemporarySuffix,
                report_sha256_,
                byte_count_,
                kScaffoldingFinalizationReportV1MaximumBytes);
        SetDiagnostic(
            diagnostic,
            valid ? std::string_view{}
                  : std::string_view{
                        "scaffolding report receipt evidence is stale"});
        return valid;
    } catch (...) {
        SetDiagnostic(
            diagnostic,
            "cannot validate scaffolding report receipt");
        return false;
    }
}

FinalizationReportPublishResultV1
PublishFinalizationReportV1(
    std::unique_ptr<RawReserveFinalizationActionV1>
        action,
    const BuiltFinalizationReportV1& report,
    const FinalizationReportStoreHooksV1* hooks,
    std::string* diagnostic) noexcept {
    FinalizationReportPublishResultV1 result{};
    SetDiagnostic(diagnostic, {});
    try {
        if (action == nullptr) {
            result.error =
                FinalizationReportStoreErrorV1::
                    kInvalidArgument;
            SetDiagnostic(
                diagnostic,
                "finalization report action is missing");
            return result;
        }
        if (!ValidateRoutedCapability(*action, report)) {
            result.error =
                FinalizationReportStoreErrorV1::
                    kAuthorizationRejected;
            SetDiagnostic(
                diagnostic,
                "report capability does not match the routed grant");
            return result;
        }
        PlanPrestate prestate{};
        if (!ValidateActionPlan(
                *action,
                ReportDomain::kRouted,
                report.canonical_jcs().size(),
                report.report_sha256(),
                &prestate)) {
            result.error =
                FinalizationReportStoreErrorV1::
                    kPlanMismatch;
            SetDiagnostic(
                diagnostic,
                "terminal report action plan or object-plan hash is invalid");
            return result;
        }
        ParentChain chain{};
        result.error = OpenParentChain(
            *action,
            ReportDomain::kRouted,
            &chain);
        if (result.error !=
            FinalizationReportStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "pre-existing routed maintenance chain is unavailable");
            return result;
        }
        if (!ValidateContext(*action, chain) ||
            !LockExclusiveNoIntr(
                chain.report_parent.get()) ||
            !ValidateContext(*action, chain)) {
            result.error =
                FinalizationReportStoreErrorV1::
                    kAuthorizationRejected;
            SetDiagnostic(
                diagnostic,
                "cannot lock the exact authorized maintenance directory");
            return result;
        }
        PublicationEvidence evidence = PublishAt(
            *action,
            chain,
            report.canonical_jcs(),
            report.report_sha256(),
            std::string(report.filename()),
            &report.model(),
            prestate,
            hooks);
        result.error = evidence.error;
        result.disposition = evidence.disposition;
        result.observed_candidate_count =
            evidence.observed_candidate_count;
        result.file_synced = evidence.file_synced;
        result.directory_synced =
            evidence.directory_synced;
        if (result.error !=
            FinalizationReportStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "routed finalization report publication did not reach every durability barrier");
            return result;
        }
        const auto* const target = action->target();
        if (target == nullptr ||
            !ValidateContext(*action, chain) ||
            !UnlockNoIntr(chain.report_parent.get())) {
            result.error =
                FinalizationReportStoreErrorV1::
                    kAuthorizationRejected;
            return result;
        }
        const struct stat file_status =
            evidence.final.status;
        result.report_sha256 =
            report.report_sha256();
        result.filename =
            std::string(report.filename());
        result.receipt =
            std::unique_ptr<
                FinalizationReportReceiptV1>(
                new FinalizationReportReceiptV1(
                    action->key(),
                    action->generation_token(),
                    action->immutable_grant_sha256(),
                    action->plan(),
                    action->grant_flags(),
                    action->byte_cap(),
                    action->inode_cap(),
                    action->debit_generation(),
                    action->executor_instance(),
                    report.report_sha256(),
                    report.canonical_jcs().size(),
                    result.filename,
                    std::string(target->stream_slug()),
                    chain.root.Release(),
                    chain.first_parent.Release(),
                    chain.route_or_audit.Release(),
                    chain.report_parent.Release(),
                    evidence.final.fd.Release(),
                    Device(chain.root_status),
                    Inode(chain.root_status),
                    Device(chain.first_parent_status),
                    Inode(chain.first_parent_status),
                    Device(chain.route_or_audit_status),
                    Inode(chain.route_or_audit_status),
                    Device(chain.report_parent_status),
                    Inode(chain.report_parent_status),
                    Device(file_status),
                    Inode(file_status),
                    Blocks(file_status)));
        return result;
    } catch (...) {
        result.error =
            FinalizationReportStoreErrorV1::
                kAllocationFailure;
        result.receipt.reset();
        SetDiagnostic(
            diagnostic,
            "allocation failed while publishing finalization report");
        return result;
    }
}

ScaffoldingFinalizationReportPublishResultV1
PublishScaffoldingFinalizationReportV1(
    std::unique_ptr<RawReserveFinalizationActionV1>
        action,
    const BuiltScaffoldingFinalizationReportV1&
        report,
    const FinalizationReportStoreHooksV1* hooks,
    std::string* diagnostic) noexcept {
    ScaffoldingFinalizationReportPublishResultV1
        result{};
    SetDiagnostic(diagnostic, {});
    try {
        if (action == nullptr) {
            result.error =
                FinalizationReportStoreErrorV1::
                    kInvalidArgument;
            SetDiagnostic(
                diagnostic,
                "scaffolding report action is missing");
            return result;
        }
        if (!ValidateScaffoldingCapability(
                *action, report)) {
            result.error =
                FinalizationReportStoreErrorV1::
                    kAuthorizationRejected;
            SetDiagnostic(
                diagnostic,
                "report capability does not match the scaffolding grant");
            return result;
        }
        PlanPrestate prestate{};
        if (!ValidateActionPlan(
                *action,
                ReportDomain::kScaffolding,
                report.canonical_jcs().size(),
                report.report_sha256(),
                &prestate)) {
            result.error =
                FinalizationReportStoreErrorV1::
                    kPlanMismatch;
            SetDiagnostic(
                diagnostic,
                "scaffolding report action plan or object-plan hash is invalid");
            return result;
        }
        ParentChain chain{};
        result.error = OpenParentChain(
            *action,
            ReportDomain::kScaffolding,
            &chain);
        if (result.error !=
            FinalizationReportStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "pre-created reserve-audit/emergency-reports chain is unavailable");
            return result;
        }
        if (!ValidateContext(*action, chain) ||
            !LockExclusiveNoIntr(
                chain.report_parent.get()) ||
            !ValidateContext(*action, chain)) {
            result.error =
                FinalizationReportStoreErrorV1::
                    kAuthorizationRejected;
            SetDiagnostic(
                diagnostic,
                "cannot lock the exact authorized emergency report directory");
            return result;
        }
        PublicationEvidence evidence = PublishAt(
            *action,
            chain,
            report.canonical_jcs(),
            report.report_sha256(),
            std::string(report.filename()),
            nullptr,
            prestate,
            hooks);
        result.error = evidence.error;
        result.disposition = evidence.disposition;
        result.observed_candidate_count =
            evidence.observed_candidate_count;
        result.file_synced = evidence.file_synced;
        result.directory_synced =
            evidence.directory_synced;
        if (result.error !=
            FinalizationReportStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "scaffolding finalization report publication did not reach every durability barrier");
            return result;
        }
        if (!ValidateContext(*action, chain) ||
            !UnlockNoIntr(chain.report_parent.get())) {
            result.error =
                FinalizationReportStoreErrorV1::
                    kAuthorizationRejected;
            return result;
        }
        const struct stat file_status =
            evidence.final.status;
        result.report_sha256 =
            report.report_sha256();
        result.filename =
            std::string(report.filename());
        // first_parent is a second descriptor for Raw root and is not needed
        // after publication; route_or_audit is the retained reserve-audit.
        result.receipt =
            std::unique_ptr<
                ScaffoldingFinalizationReportReceiptV1>(
                new ScaffoldingFinalizationReportReceiptV1(
                    action->key(),
                    action->generation_token(),
                    action->immutable_grant_sha256(),
                    action->plan(),
                    action->grant_flags(),
                    action->byte_cap(),
                    action->inode_cap(),
                    action->debit_generation(),
                    action->executor_instance(),
                    report.report_sha256(),
                    report.canonical_jcs().size(),
                    result.filename,
                    chain.root.Release(),
                    chain.route_or_audit.Release(),
                    chain.report_parent.Release(),
                    evidence.final.fd.Release(),
                    Device(chain.root_status),
                    Inode(chain.root_status),
                    Device(chain.route_or_audit_status),
                    Inode(chain.route_or_audit_status),
                    Device(chain.report_parent_status),
                    Inode(chain.report_parent_status),
                    Device(file_status),
                    Inode(file_status),
                    Blocks(file_status)));
        return result;
    } catch (...) {
        result.error =
            FinalizationReportStoreErrorV1::
                kAllocationFailure;
        result.receipt.reset();
        SetDiagnostic(
            diagnostic,
            "allocation failed while publishing scaffolding report");
        return result;
    }
}

}  // namespace l2flow::ingress
