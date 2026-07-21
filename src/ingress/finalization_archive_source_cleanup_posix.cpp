#include "l2flow/ingress/finalization_archive_source_cleanup_posix.h"

#include "l2flow/common/sha256.h"
#include "l2flow/ingress/finalization_report_v1.h"
#include "l2flow/ingress/raw_namespace.h"
#include "l2flow/ingress/raw_recovery_maintenance_report_v1.h"
#include "l2flow/ingress/raw_reserve_coordinator_gate.h"
#include "l2flow/ingress/scaffolding_finalization_report_v1.h"

#include <algorithm>
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
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace l2flow::ingress {
namespace {

inline constexpr std::size_t
    kSourceParentMaximumCandidatesV1 = 4096U;

class ScopedFd final {
public:
    ScopedFd() noexcept = default;
    explicit ScopedFd(int value) noexcept
        : value_(value) {}
    ~ScopedFd() {
        Reset();
    }
    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
    ScopedFd(ScopedFd&& other) noexcept
        : value_(std::exchange(other.value_, -1)) {}
    ScopedFd& operator=(ScopedFd&& other) noexcept {
        if (this != &other) {
            Reset(std::exchange(other.value_, -1));
        }
        return *this;
    }
    [[nodiscard]] int get() const noexcept {
        return value_;
    }
    [[nodiscard]] int Release() noexcept {
        return std::exchange(value_, -1);
    }
    void Reset(int replacement = -1) noexcept {
        if (value_ >= 0) {
            static_cast<void>(::close(value_));
        }
        value_ = replacement;
    }

private:
    int value_ = -1;
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

struct CleanupTask final {
    FinalizationArchiveArtifactTypeV1 type =
        FinalizationArchiveArtifactTypeV1::
            kFinalizationReport;
    bool audit_rooted = false;
    std::string source_locator;
    std::string parent_locator;
    std::string filename;
    std::string expected_bytes;
    RawV1Digest expected_sha256{};
};

struct OpenedParent final {
    ScopedFd descriptor;
    struct stat status {};
};

struct ArchiveStateEvidence final {
    ReserveStateV1HeaderWire header_wire{};
    ReserveStateV1SlotWire all_done_slot_wire{};
    ReserveCoordinatorHeaderV1 header{};
    ReserveStateSlotV1 all_done_slot{};
    RawV1Digest state_sha256{};
};

struct MaintenanceSession final {
    ScopedFd lease;
    struct stat lease_status {};
    std::unique_ptr<RawReserveStateFileV1> state_file;
};

enum class NameObservation : std::uint8_t {
    kAbsent = 1U,
    kPresent = 2U,
    kFailure = 3U,
    kLimitExceeded = 4U,
};

void SetDiagnostic(
    std::string* diagnostic,
    std::string_view message) noexcept {
    if (diagnostic == nullptr) {
        return;
    }
    try {
        diagnostic->assign(message.data(), message.size());
    } catch (...) {
    }
}

[[nodiscard]] int OpenAtNoIntr(
    int directory_fd,
    const char* name,
    int flags) noexcept {
    for (;;) {
        const int result =
            ::openat(directory_fd, name, flags);
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

[[nodiscard]] bool SafeComponent(
    std::string_view component) noexcept {
    if (component.empty() || component == "." ||
        component == "..") {
        return false;
    }
    return std::all_of(
        component.begin(),
        component.end(),
        [](char character) noexcept {
            return (character >= 'a' &&
                    character <= 'z') ||
                   (character >= 'A' &&
                    character <= 'Z') ||
                   (character >= '0' &&
                    character <= '9') ||
                   character == '-' ||
                   character == '_' ||
                   character == '.' ||
                   character == '=';
        });
}

[[nodiscard]] bool SplitLocator(
    std::string_view locator,
    std::vector<std::string>* components) {
    if (components == nullptr || locator.empty() ||
        locator.size() >
            kFinalizationArchiveV1MaximumSourceLocatorBytes ||
        locator.front() == '/' || locator.back() == '/') {
        return false;
    }
    std::vector<std::string> values;
    std::size_t begin = 0U;
    for (std::size_t index = 0U;
         index <= locator.size();
         ++index) {
        if (index != locator.size() &&
            locator[index] != '/') {
            continue;
        }
        const std::string_view component =
            locator.substr(begin, index - begin);
        if (!SafeComponent(component)) {
            return false;
        }
        values.emplace_back(component);
        begin = index + 1U;
    }
    components->swap(values);
    return true;
}

[[nodiscard]] std::string JoinParent(
    const std::vector<std::string>& components) {
    std::string result;
    for (std::size_t index = 0U;
         index + 1U < components.size();
         ++index) {
        if (!result.empty()) {
            result.push_back('/');
        }
        result.append(components[index]);
    }
    return result;
}

[[nodiscard]] bool IsCleanable(
    FinalizationArchiveArtifactTypeV1 type) noexcept {
    return type ==
               FinalizationArchiveArtifactTypeV1::
                   kFinalizationReport ||
           type ==
               FinalizationArchiveArtifactTypeV1::
                   kScaffoldingFinalizationReport ||
           type ==
               FinalizationArchiveArtifactTypeV1::
                   kPreexistingRecoveryReport;
}

[[nodiscard]] bool ValidateCanonicalReport(
    FinalizationArchiveArtifactTypeV1 type,
    std::string_view exact_bytes,
    std::string_view filename) {
    std::string expected_filename;
    switch (type) {
    case FinalizationArchiveArtifactTypeV1::
        kFinalizationReport: {
        FinalizationReportV1 report{};
        return ParseFinalizationReportV1Jcs(
                   exact_bytes, &report) ==
                   FinalizationReportV1Error::kNone &&
               FinalizationReportV1Filename(
                   report, &expected_filename) ==
                   FinalizationReportV1Error::kNone &&
               expected_filename == filename;
    }
    case FinalizationArchiveArtifactTypeV1::
        kScaffoldingFinalizationReport: {
        ScaffoldingFinalizationReportV1 report{};
        return ParseScaffoldingFinalizationReportV1Jcs(
                   exact_bytes, &report) ==
                   ScaffoldingFinalizationReportV1Error::
                       kNone &&
               ScaffoldingFinalizationReportV1Filename(
                   report, &expected_filename) ==
                   ScaffoldingFinalizationReportV1Error::
                       kNone &&
               expected_filename == filename;
    }
    case FinalizationArchiveArtifactTypeV1::
        kPreexistingRecoveryReport: {
        RecoveryMaintenanceReportV1 report{};
        return ParseRecoveryMaintenanceReportV1Jcs(
                   exact_bytes, &report) ==
                   RecoveryMaintenanceReportV1Error::
                       kNone &&
               RecoveryMaintenanceReportV1Filename(
                   report, &expected_filename) ==
                   RecoveryMaintenanceReportV1Error::
                       kNone &&
               expected_filename == filename;
    }
    case FinalizationArchiveArtifactTypeV1::
        kRawManifest:
    case FinalizationArchiveArtifactTypeV1::
        kSealedRawCertificate:
    case FinalizationArchiveArtifactTypeV1::
        kEmptyAnchorTombstone:
        return false;
    }
    return false;
}

[[nodiscard]] bool Allow(
    const FinalizationArchiveSourceCleanupHooksV1* hooks,
    FinalizationArchiveSourceCleanupMutationPointV1 point,
    std::size_t index) noexcept {
    return hooks == nullptr || hooks->allow == nullptr ||
           hooks->allow(point, index, hooks->context);
}

[[nodiscard]] FinalizationArchiveSourceCleanupErrorV1
OpenRetainedRoot(
    int supplied_fd,
    ScopedFd* output,
    struct stat* output_status) noexcept {
    if (supplied_fd < 0 || output == nullptr ||
        output_status == nullptr) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kUnsafeRawRoot;
    }
    struct stat supplied {};
    if (::fstat(supplied_fd, &supplied) != 0 ||
        !IsSafeDirectory(supplied)) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kUnsafeRawRoot;
    }
    ScopedFd retained(
        OpenAtNoIntr(
            supplied_fd,
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NOATIME | O_NONBLOCK | O_CLOEXEC));
    struct stat retained_status {};
    if (retained.get() < 0 ||
        ::fstat(retained.get(), &retained_status) != 0 ||
        !IsSafeDirectory(retained_status) ||
        !SameInode(supplied, retained_status)) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kUnsafeRawRoot;
    }
    *output_status = retained_status;
    *output = std::move(retained);
    return FinalizationArchiveSourceCleanupErrorV1::kNone;
}

[[nodiscard]] bool AllDoneConsumed(
    const ReserveStateSlotV1& slot) noexcept {
    if (slot.coordinator_state !=
            ReserveCoordinatorPhaseV1::kConsumed ||
        slot.entry_count == 0U ||
        slot.entry_count >
            kReserveStateV1EntryCapacity ||
        slot.active_entry_index !=
            kReserveStateV1NoActiveEntry) {
        return false;
    }
    const std::uint16_t expected_bitmap =
        slot.entry_count ==
                kReserveStateV1EntryCapacity
            ? std::numeric_limits<std::uint16_t>::max()
            : static_cast<std::uint16_t>(
                  (1U << slot.entry_count) - 1U);
    if (slot.completed_bitmap != expected_bitmap) {
        return false;
    }
    for (std::size_t entry_index = 0U;
         entry_index < slot.entry_count;
         ++entry_index) {
        const ReserveStateEntryV1& entry =
            slot.entries[entry_index];
        if (entry.grant_status !=
            ReserveGrantStatusV1::kDone) {
            return false;
        }
        for (const auto& action : entry.actions) {
            if (action.action_kind ==
                    FinalizationActionKindV1::kUnused) {
                continue;
            }
            if (action.action_state !=
                FinalizationActionStateV1::kComplete) {
                return false;
            }
        }
    }
    return true;
}

[[nodiscard]] bool ComputeStateDigest(
    const ReserveStateV1HeaderWire& header,
    const ReserveStateV1SlotWire& slot,
    RawV1Digest* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    l2flow::common::Sha256Hasher hasher;
    return hasher.Update(
               std::span<const std::byte>(header)) &&
           hasher.Update(
               std::span<const std::byte>(slot)) &&
           hasher.Finalize(output);
}

[[nodiscard]] FinalizationArchiveSourceCleanupErrorV1
ValidateMaintenanceState(
    RawReserveStateFileV1* state_file,
    const ArchiveStateEvidence& frozen,
    bool reload,
    std::string* diagnostic) noexcept {
    if (state_file == nullptr) {
        SetDiagnostic(
            diagnostic,
            "maintenance state handle is absent");
        return FinalizationArchiveSourceCleanupErrorV1::
            kStateAttachFailure;
    }
    if (reload) {
        ReserveStateV1Error codec_error{};
        std::string state_error;
        if (state_file->Reload(
                &codec_error, &state_error) !=
            RawReserveStatePosixError::kNone) {
            SetDiagnostic(
                diagnostic,
                state_error.empty()
                    ? "fixed reserve state reload failed"
                    : state_error);
            return FinalizationArchiveSourceCleanupErrorV1::
                kStateMismatch;
        }
    }
    const ReserveCoordinatorStateV1& live =
        state_file->state();
    if (live.selected_slot >= live.slots.size()) {
        SetDiagnostic(
            diagnostic,
            "fixed reserve state has no selected slot");
        return FinalizationArchiveSourceCleanupErrorV1::
            kStateMismatch;
    }
    const ReserveStateSlotV1& selected =
        live.slots[live.selected_slot];
    if (!AllDoneConsumed(selected)) {
        SetDiagnostic(
            diagnostic,
            "fixed reserve state is not exact CONSUMED/all-DONE");
        return FinalizationArchiveSourceCleanupErrorV1::
            kStateNotAllDone;
    }
    ReserveStateV1HeaderWire live_header{};
    ReserveStateV1SlotWire live_slot{};
    RawV1Digest live_state_sha256{};
    if (EncodeReserveCoordinatorHeaderV1(
            live.header, &live_header) !=
            ReserveStateV1Error::kNone ||
        EncodeReserveStateSlotV1(
            live.header, selected, &live_slot) !=
            ReserveStateV1Error::kNone ||
        live_header != frozen.header_wire ||
        live_slot != frozen.all_done_slot_wire ||
        live.header != frozen.header ||
        selected != frozen.all_done_slot ||
        !ComputeStateDigest(
            live_header,
            live_slot,
            &live_state_sha256) ||
        live_state_sha256 != frozen.state_sha256) {
        SetDiagnostic(
            diagnostic,
            "fixed reserve state differs from the authoritative archive");
        return FinalizationArchiveSourceCleanupErrorV1::
            kStateMismatch;
    }
    return FinalizationArchiveSourceCleanupErrorV1::kNone;
}

[[nodiscard]] FinalizationArchiveSourceCleanupErrorV1
ValidateMaintenanceLeaseFd(
    int raw_root_fd,
    const struct stat& raw_root_status,
    int lease_fd,
    const ReserveCoordinatorHeaderV1& header,
    struct stat* output_status,
    std::string* diagnostic) noexcept {
    struct stat lease_status {};
    const int flags = ::fcntl(lease_fd, F_GETFL);
    RawReserveCoordinatorLeaseMarkerWireV1 wire{};
    RawReserveCoordinatorLeaseMarkerV1 marker{};
    if (lease_fd < 0 ||
        ::fstat(lease_fd, &lease_status) != 0 ||
        !S_ISREG(lease_status.st_mode) ||
        lease_status.st_uid != ::geteuid() ||
        (lease_status.st_mode & 07777U) != 0600U ||
        lease_status.st_nlink !=
            static_cast<nlink_t>(1) ||
        lease_status.st_dev != raw_root_status.st_dev ||
        lease_status.st_size !=
            static_cast<off_t>(
                kRawReserveCoordinatorLeaseMarkerBytes) ||
        flags < 0 ||
        (flags & O_ACCMODE) != O_RDWR ||
        (flags & O_APPEND) != 0 ||
        !NameMatchesDescriptor(
            raw_root_fd,
            kRawReserveCoordinatorLeaseFilename,
            lease_fd,
            &lease_status) ||
        !PreadAll(
            lease_fd,
            std::span<std::byte>(wire)) ||
        !DecodeRawReserveCoordinatorLeaseMarkerV1(
            wire, &marker)) {
        SetDiagnostic(
            diagnostic,
            "fixed coordinator lease is unsafe");
        return FinalizationArchiveSourceCleanupErrorV1::
            kMaintenanceLeaseUnsafe;
    }
    if (marker.device_id != header.device_id ||
        marker.quota_identity_sha256 !=
            header.quota_identity_sha256 ||
        marker.mount_identity_sha256 !=
            header.mount_identity_sha256) {
        SetDiagnostic(
            diagnostic,
            "fixed coordinator lease marker pool identity differs from archived state");
        return FinalizationArchiveSourceCleanupErrorV1::
            kMaintenanceMarkerMismatch;
    }
    if (output_status != nullptr) {
        *output_status = lease_status;
    }
    return FinalizationArchiveSourceCleanupErrorV1::kNone;
}

[[nodiscard]] FinalizationArchiveSourceCleanupErrorV1
AcquireMaintenanceSession(
    int raw_root_fd,
    const struct stat& raw_root_status,
    const ArchiveStateEvidence& frozen,
    MaintenanceSession* output,
    std::string* diagnostic) noexcept {
    if (output == nullptr) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kInvalidArgument;
    }
    struct stat temporary {};
    if (::fstatat(
            raw_root_fd,
            kRawReserveCoordinatorLeaseTemporaryFilename,
            &temporary,
            AT_SYMLINK_NOFOLLOW) == 0 ||
        errno != ENOENT) {
        SetDiagnostic(
            diagnostic,
            "fixed coordinator lease has an ambiguous temporary");
        return FinalizationArchiveSourceCleanupErrorV1::
            kMaintenanceLeaseUnsafe;
    }
    MaintenanceSession session{};
    session.lease.Reset(
        OpenAtNoIntr(
            raw_root_fd,
            kRawReserveCoordinatorLeaseFilename,
            O_RDWR | O_NOFOLLOW | O_NOATIME |
                O_NONBLOCK | O_CLOEXEC));
    if (session.lease.get() < 0) {
        SetDiagnostic(
            diagnostic,
            "fixed coordinator lease is missing");
        return errno == ENOENT
                   ? FinalizationArchiveSourceCleanupErrorV1::
                         kMaintenanceLeaseMissing
                   : FinalizationArchiveSourceCleanupErrorV1::
                         kMaintenanceLeaseUnsafe;
    }
    auto error = ValidateMaintenanceLeaseFd(
        raw_root_fd,
        raw_root_status,
        session.lease.get(),
        frozen.header,
        &session.lease_status,
        diagnostic);
    if (error !=
        FinalizationArchiveSourceCleanupErrorV1::kNone) {
        return error;
    }
    for (;;) {
        if (::flock(
                session.lease.get(),
                LOCK_EX | LOCK_NB) == 0) {
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        SetDiagnostic(
            diagnostic,
            errno == EWOULDBLOCK || errno == EAGAIN
                ? "a live coordinator holds the fixed lease"
                : "fixed coordinator maintenance flock failed");
        return errno == EWOULDBLOCK || errno == EAGAIN
                   ? FinalizationArchiveSourceCleanupErrorV1::
                         kMaintenanceLeaseBusy
                   : FinalizationArchiveSourceCleanupErrorV1::
                         kMaintenanceLeaseUnsafe;
    }
    error = ValidateMaintenanceLeaseFd(
        raw_root_fd,
        raw_root_status,
        session.lease.get(),
        frozen.header,
        &session.lease_status,
        diagnostic);
    if (error !=
        FinalizationArchiveSourceCleanupErrorV1::kNone) {
        return error;
    }
    RawReserveStatePosixError state_error{};
    ReserveStateV1Error codec_error{};
    std::string state_diagnostic;
    session.state_file = AttachRawReserveStateAtV1(
        raw_root_fd,
        &state_error,
        &codec_error,
        &state_diagnostic);
    if (session.state_file == nullptr) {
        SetDiagnostic(
            diagnostic,
            state_diagnostic.empty()
                ? "fixed reserve state attach failed"
                : state_diagnostic);
        return FinalizationArchiveSourceCleanupErrorV1::
            kStateAttachFailure;
    }
    error = ValidateMaintenanceState(
        session.state_file.get(),
        frozen,
        false,
        diagnostic);
    if (error !=
        FinalizationArchiveSourceCleanupErrorV1::kNone) {
        return error;
    }
    *output = std::move(session);
    return FinalizationArchiveSourceCleanupErrorV1::kNone;
}

[[nodiscard]] FinalizationArchiveSourceCleanupErrorV1
OpenParent(
    int root_fd,
    const struct stat& root_status,
    std::string_view parent_locator,
    OpenedParent* output) {
    if (root_fd < 0 || output == nullptr ||
        parent_locator.empty()) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kInvalidSourceLocator;
    }
    std::vector<std::string> components;
    if (!SplitLocator(
            std::string(parent_locator) + "/x",
            &components) ||
        components.size() < 2U) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kInvalidSourceLocator;
    }
    components.pop_back();

    ScopedFd current(DuplicateFd(root_fd));
    struct stat current_status {};
    if (current.get() < 0 ||
        ::fstat(current.get(), &current_status) != 0 ||
        !SameInode(current_status, root_status) ||
        !IsSafeDirectory(current_status)) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kUnsafeSourceParent;
    }
    for (const std::string& component : components) {
        ScopedFd next(
            OpenAtNoIntr(
                current.get(),
                component.c_str(),
                O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                    O_NOATIME | O_NONBLOCK | O_CLOEXEC));
        if (next.get() < 0) {
            return errno == ENOENT
                       ? FinalizationArchiveSourceCleanupErrorV1::
                             kSourceParentMissing
                       : FinalizationArchiveSourceCleanupErrorV1::
                             kUnsafeSourceParent;
        }
        struct stat next_status {};
        if (::fstat(next.get(), &next_status) != 0 ||
            !IsSafeDirectory(next_status) ||
            next_status.st_dev != root_status.st_dev ||
            !NameMatchesDescriptor(
                current.get(),
                component.c_str(),
                next.get(),
                &next_status)) {
            return FinalizationArchiveSourceCleanupErrorV1::
                kUnsafeSourceParent;
        }
        current = std::move(next);
        current_status = next_status;
    }
    output->descriptor = std::move(current);
    output->status = current_status;
    return FinalizationArchiveSourceCleanupErrorV1::kNone;
}

[[nodiscard]] NameObservation ObserveName(
    int parent_fd,
    const struct stat& parent_status,
    std::string_view filename) {
    ScopedFd scan(
        OpenAtNoIntr(
            parent_fd,
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NOATIME | O_NONBLOCK | O_CLOEXEC));
    struct stat scan_status {};
    if (scan.get() < 0 ||
        ::fstat(scan.get(), &scan_status) != 0 ||
        !SameInode(scan_status, parent_status)) {
        return NameObservation::kFailure;
    }
    DIR* native = ::fdopendir(scan.Release());
    if (native == nullptr) {
        return NameObservation::kFailure;
    }
    ScopedDir directory(native);
    std::size_t observed = 0U;
    bool present = false;
    for (;;) {
        errno = 0;
        dirent* entry = ::readdir(directory.get());
        if (entry == nullptr) {
            if (errno != 0) {
                return NameObservation::kFailure;
            }
            break;
        }
        const std::string_view name(entry->d_name);
        if (name == "." || name == "..") {
            continue;
        }
        ++observed;
        if (observed >
            kSourceParentMaximumCandidatesV1) {
            return NameObservation::kLimitExceeded;
        }
        if (name == filename) {
            present = true;
        }
    }
    struct stat after {};
    if (::fstat(parent_fd, &after) != 0 ||
        !SameInode(after, parent_status) ||
        !IsSafeDirectory(after)) {
        return NameObservation::kFailure;
    }
    return present ? NameObservation::kPresent
                   : NameObservation::kAbsent;
}

[[nodiscard]] FinalizationArchiveSourceCleanupErrorV1
OpenAndValidateSource(
    int parent_fd,
    const struct stat& parent_status,
    const CleanupTask& task,
    ScopedFd* output,
    struct stat* output_status) {
    if (output == nullptr || output_status == nullptr) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kInvalidArgument;
    }
    ScopedFd source(
        OpenAtNoIntr(
            parent_fd,
            task.filename.c_str(),
            O_RDONLY | O_NOFOLLOW | O_NOATIME |
                O_NONBLOCK | O_CLOEXEC));
    if (source.get() < 0) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kSourceConflict;
    }
    struct stat source_status {};
    if (::fstat(source.get(), &source_status) != 0 ||
        !IsSafeFile(source_status, parent_status) ||
        source_status.st_size !=
            static_cast<off_t>(
                task.expected_bytes.size()) ||
        !NameMatchesDescriptor(
            parent_fd,
            task.filename.c_str(),
            source.get(),
            &source_status)) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kSourceConflict;
    }
    std::string bytes;
    if (!ReadExactString(
            source.get(),
            task.expected_bytes.size(),
            &bytes) ||
        bytes != task.expected_bytes ||
        l2flow::common::ComputeSha256(
            std::string_view(bytes)) !=
            task.expected_sha256 ||
        !ValidateCanonicalReport(
            task.type, bytes, task.filename) ||
        !NameMatchesDescriptor(
            parent_fd,
            task.filename.c_str(),
            source.get(),
            &source_status)) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kSourceConflict;
    }
    *output_status = source_status;
    *output = std::move(source);
    return FinalizationArchiveSourceCleanupErrorV1::kNone;
}

[[nodiscard]] bool ConfirmAbsent(
    int parent_fd,
    const struct stat& parent_status,
    std::string_view filename,
    NameObservation* observation = nullptr) {
    const NameObservation scanned =
        ObserveName(parent_fd, parent_status, filename);
    if (observation != nullptr) {
        *observation = scanned;
    }
    if (scanned != NameObservation::kAbsent) {
        return false;
    }
    ScopedFd probe(
        OpenAtNoIntr(
            parent_fd,
            std::string(filename).c_str(),
            O_RDONLY | O_NOFOLLOW | O_NOATIME |
                O_NONBLOCK | O_CLOEXEC));
    return probe.get() < 0 && errno == ENOENT;
}

}  // namespace

struct FinalizationArchiveSourceCleanupAccessV1 final {
    [[nodiscard]] static int AuditDirectoryFd(
        const PublishedFinalizationArchiveReceiptV1&
            receipt) noexcept {
        return receipt.audit_directory_fd_;
    }

    [[nodiscard]] static const std::vector<
        PublishedFinalizationArchiveReceiptV1::
            RetainedFileV1>&
    Files(
        const PublishedFinalizationArchiveReceiptV1&
            receipt) noexcept {
        return receipt.files_;
    }

    [[nodiscard]] static std::unique_ptr<
        FinalizationArchiveSourceCleanupReceiptV1>
    MakeReceipt(
        int raw_root_fd,
        const struct stat& raw_root_status,
        MaintenanceSession session,
        ArchiveStateEvidence frozen_state,
        std::unique_ptr<
            PublishedFinalizationArchiveReceiptV1>
            archive_receipt,
        std::vector<
            FinalizationArchiveSourceCleanupReceiptV1::
                RetainedSourceV1>
            sources) {
        if (archive_receipt == nullptr) {
            return nullptr;
        }
        const RawV1Identity reserve_state_uuid =
            archive_receipt->reserve_state_uuid();
        const RawV1Identity finalization_cycle_id =
            archive_receipt->finalization_cycle_id();
        const RawV1Digest manifest_sha256 =
            archive_receipt->manifest_sha256();
        return std::unique_ptr<
            FinalizationArchiveSourceCleanupReceiptV1>(
            new FinalizationArchiveSourceCleanupReceiptV1(
                reserve_state_uuid,
                finalization_cycle_id,
                manifest_sha256,
                DuplicateFd(raw_root_fd),
                static_cast<std::uint64_t>(
                    raw_root_status.st_dev),
                static_cast<std::uint64_t>(
                    raw_root_status.st_ino),
                session.lease.Release(),
                static_cast<std::uint64_t>(
                    session.lease_status.st_dev),
                static_cast<std::uint64_t>(
                    session.lease_status.st_ino),
                frozen_state.header_wire,
                frozen_state.all_done_slot_wire,
                frozen_state.state_sha256,
                std::move(session.state_file),
                std::move(archive_receipt),
                std::move(sources)));
    }
};

namespace {

[[nodiscard]] FinalizationArchiveSourceCleanupErrorV1
LoadArchiveStateEvidence(
    const PublishedFinalizationArchiveReceiptV1& receipt,
    ArchiveStateEvidence* output) {
    if (output == nullptr) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kInvalidArgument;
    }
    const auto& files =
        FinalizationArchiveSourceCleanupAccessV1::
            Files(receipt);
    const auto header_file = std::find_if(
        files.begin(),
        files.end(),
        [](const auto& file) noexcept {
            return !file.evidence_parent &&
                   file.name ==
                       kFinalizationArchiveV1StateHeaderFilename;
        });
    const auto slot_file = std::find_if(
        files.begin(),
        files.end(),
        [](const auto& file) noexcept {
            return !file.evidence_parent &&
                   file.name ==
                       kFinalizationArchiveV1AllDoneSlotFilename;
        });
    if (header_file == files.end() ||
        slot_file == files.end() ||
        header_file->byte_count !=
            kReserveStateV1HeaderBytes ||
        slot_file->byte_count !=
            kReserveStateV1SlotBytes) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kArchiveBarrierInvalid;
    }
    ArchiveStateEvidence frozen{};
    if (!PreadAll(
            header_file->descriptor,
            std::span<std::byte>(
                frozen.header_wire)) ||
        !PreadAll(
            slot_file->descriptor,
            std::span<std::byte>(
                frozen.all_done_slot_wire)) ||
        DecodeReserveCoordinatorHeaderV1(
            frozen.header_wire,
            &frozen.header) !=
            ReserveStateV1Error::kNone ||
        DecodeReserveStateSlotV1(
            frozen.header,
            frozen.all_done_slot_wire,
            &frozen.all_done_slot) !=
            ReserveStateV1Error::kNone ||
        !AllDoneConsumed(frozen.all_done_slot) ||
        !ComputeStateDigest(
            frozen.header_wire,
            frozen.all_done_slot_wire,
            &frozen.state_sha256) ||
        frozen.state_sha256 != receipt.state_sha256() ||
        frozen.header.reserve_state_uuid !=
            receipt.reserve_state_uuid() ||
        frozen.all_done_slot.finalization_cycle_id !=
            receipt.finalization_cycle_id()) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kArchiveBarrierInvalid;
    }
    *output = std::move(frozen);
    return FinalizationArchiveSourceCleanupErrorV1::kNone;
}

[[nodiscard]] FinalizationArchiveSourceCleanupErrorV1
LoadCleanupTasks(
    const PublishedFinalizationArchiveReceiptV1& receipt,
    std::vector<CleanupTask>* output) {
    if (output == nullptr) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kInvalidArgument;
    }
    const auto& files =
        FinalizationArchiveSourceCleanupAccessV1::
            Files(receipt);
    const auto manifest_file = std::find_if(
        files.begin(),
        files.end(),
        [](const auto& file) noexcept {
            return !file.evidence_parent &&
                   file.name ==
                       kFinalizationArchiveV1ManifestFilename;
        });
    if (manifest_file == files.end() ||
        manifest_file->byte_count >
            kFinalizationArchiveV1MaximumManifestBytes) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kArchiveBarrierInvalid;
    }
    std::string manifest_bytes;
    if (!ReadExactString(
            manifest_file->descriptor,
            static_cast<std::size_t>(
                manifest_file->byte_count),
            &manifest_bytes)) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kArchiveBarrierInvalid;
    }
    FinalizationArchiveV1 manifest{};
    if (ParseFinalizationArchiveV1Jcs(
            manifest_bytes, &manifest) !=
        FinalizationArchiveV1Error::kNone) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kArchiveBarrierInvalid;
    }

    std::vector<CleanupTask> tasks;
    tasks.reserve(manifest.artifacts.size());
    for (const auto& artifact : manifest.artifacts) {
        if (!IsCleanable(artifact.artifact_type)) {
            continue;
        }
        if (tasks.size() >=
            kFinalizationArchiveV1MaximumArtifacts) {
            return FinalizationArchiveSourceCleanupErrorV1::
                kCandidateLimitExceeded;
        }
        std::vector<std::string> components;
        if (!SplitLocator(
                artifact.source_locator,
                &components) ||
            components.size() < 2U) {
            return FinalizationArchiveSourceCleanupErrorV1::
                kInvalidSourceLocator;
        }
        const bool scaffolding =
            artifact.artifact_type ==
            FinalizationArchiveArtifactTypeV1::
                kScaffoldingFinalizationReport;
        const std::string expected_capture =
            "capture_date=" +
            std::to_string(
                artifact.namespace_identity.capture_date);
        const std::string expected_stream_prefix =
            "stream=" +
            std::to_string(
                artifact.namespace_identity
                    .source_stream_id) +
            "-";
        const auto canonical_stream_slug =
            CanonicalRawStreamSlugV1(
                artifact.namespace_identity
                    .source_stream_id);
        const std::string expected_stream =
            canonical_stream_slug.has_value()
                ? expected_stream_prefix +
                      std::string(
                          *canonical_stream_slug)
                : std::string{};
        if ((scaffolding &&
             (components.size() != 2U ||
              components[0U] !=
                  "emergency-reports")) ||
            (!scaffolding &&
             (components.size() != 4U ||
              components[0U] != expected_capture ||
              !canonical_stream_slug.has_value() ||
              components[1U] != expected_stream ||
              components[2U] != "maintenance"))) {
            return FinalizationArchiveSourceCleanupErrorV1::
                kInvalidSourceLocator;
        }
        const std::string_view archive_path(
            artifact.archive_path);
        const std::string_view archive_name =
            archive_path.substr(
                archive_path.rfind('/') + 1U);
        const auto evidence_file = std::find_if(
            files.begin(),
            files.end(),
            [&](const auto& file) noexcept {
                return file.evidence_parent &&
                       file.name == archive_name &&
                       file.byte_count ==
                           artifact.byte_count &&
                       file.sha256 == artifact.sha256;
            });
        if (evidence_file == files.end() ||
            evidence_file->byte_count >
                kFinalizationReportV1MaximumBytes) {
            return FinalizationArchiveSourceCleanupErrorV1::
                kArchiveBarrierInvalid;
        }
        CleanupTask task{};
        task.type = artifact.artifact_type;
        task.audit_rooted = scaffolding;
        task.source_locator = artifact.source_locator;
        task.parent_locator = JoinParent(components);
        task.filename = components.back();
        task.expected_sha256 = artifact.sha256;
        if (!ReadExactString(
                evidence_file->descriptor,
                static_cast<std::size_t>(
                    evidence_file->byte_count),
                &task.expected_bytes) ||
            l2flow::common::ComputeSha256(
                std::string_view(
                    task.expected_bytes)) !=
                task.expected_sha256 ||
            !ValidateCanonicalReport(
                task.type,
                task.expected_bytes,
                task.filename)) {
            return FinalizationArchiveSourceCleanupErrorV1::
                kArchiveBarrierInvalid;
        }
        tasks.push_back(std::move(task));
    }
    std::vector<std::pair<bool, std::string>> mappings;
    mappings.reserve(tasks.size());
    for (const CleanupTask& task : tasks) {
        mappings.emplace_back(
            task.audit_rooted, task.source_locator);
    }
    std::sort(mappings.begin(), mappings.end());
    if (std::adjacent_find(
            mappings.begin(), mappings.end()) !=
        mappings.end()) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kDuplicateSourceMapping;
    }
    output->swap(tasks);
    return FinalizationArchiveSourceCleanupErrorV1::kNone;
}

[[nodiscard]] FinalizationArchiveSourceCleanupErrorV1
CleanupOne(
    std::size_t index,
    const CleanupTask& task,
    int root_fd,
    const struct stat& root_status,
    PublishedFinalizationArchiveReceiptV1*
        archive_receipt,
    const FinalizationArchiveSourceCleanupHooksV1*
        hooks,
    FinalizationArchiveSourceCleanupReceiptV1::
        RetainedSourceV1* retained,
    bool* removed,
    bool* adopted_absent,
    std::string* diagnostic) {
    if (archive_receipt == nullptr ||
        retained == nullptr || removed == nullptr ||
        adopted_absent == nullptr) {
        return FinalizationArchiveSourceCleanupErrorV1::
            kInvalidArgument;
    }
    *removed = false;
    *adopted_absent = false;
    OpenedParent parent{};
    auto error = OpenParent(
        root_fd,
        root_status,
        task.parent_locator,
        &parent);
    if (error !=
        FinalizationArchiveSourceCleanupErrorV1::kNone) {
        SetDiagnostic(
            diagnostic,
            "source report parent cannot be secure-opened");
        return error;
    }

    NameObservation observation = ObserveName(
        parent.descriptor.get(),
        parent.status,
        task.filename);
    if (observation ==
        NameObservation::kLimitExceeded) {
        SetDiagnostic(
            diagnostic,
            "source report parent candidate bound exceeded");
        return FinalizationArchiveSourceCleanupErrorV1::
            kCandidateLimitExceeded;
    }
    if (observation == NameObservation::kFailure) {
        SetDiagnostic(
            diagnostic,
            "source report parent enumeration failed");
        return FinalizationArchiveSourceCleanupErrorV1::
            kUnsafeSourceParent;
    }

    if (observation == NameObservation::kAbsent) {
        if (!Allow(
                hooks,
                FinalizationArchiveSourceCleanupMutationPointV1::
                    kBeforeParentSyncForAbsentSource,
                index)) {
            SetDiagnostic(
                diagnostic,
                "injected before absent-source parent sync");
            return FinalizationArchiveSourceCleanupErrorV1::
                kInjectedInterruption;
        }
        if (!FsyncNoIntr(parent.descriptor.get())) {
            SetDiagnostic(
                diagnostic,
                "absent-source parent fsync failed");
            return FinalizationArchiveSourceCleanupErrorV1::
                kSyncFailure;
        }
        if (!Allow(
                hooks,
                FinalizationArchiveSourceCleanupMutationPointV1::
                    kAfterParentSyncBeforeAbsentRecheck,
                index)) {
            SetDiagnostic(
                diagnostic,
                "injected after absent-source parent sync");
            return FinalizationArchiveSourceCleanupErrorV1::
                kInjectedInterruption;
        }
        observation = ObserveName(
            parent.descriptor.get(),
            parent.status,
            task.filename);
        if (observation ==
            NameObservation::kLimitExceeded) {
            return FinalizationArchiveSourceCleanupErrorV1::
                kCandidateLimitExceeded;
        }
        if (observation == NameObservation::kFailure) {
            return FinalizationArchiveSourceCleanupErrorV1::
                kUnsafeSourceParent;
        }
        if (observation == NameObservation::kAbsent) {
            if (!ConfirmAbsent(
                    parent.descriptor.get(),
                    parent.status,
                    task.filename)) {
                return FinalizationArchiveSourceCleanupErrorV1::
                    kReadbackFailure;
            }
            *adopted_absent = true;
        }
    }

    if (observation == NameObservation::kPresent) {
        ScopedFd source;
        struct stat source_status {};
        error = OpenAndValidateSource(
            parent.descriptor.get(),
            parent.status,
            task,
            &source,
            &source_status);
        if (error !=
            FinalizationArchiveSourceCleanupErrorV1::
                kNone) {
            SetDiagnostic(
                diagnostic,
                "source report differs from the authoritative archive");
            return error;
        }
        if (!archive_receipt->Validate(diagnostic)) {
            return FinalizationArchiveSourceCleanupErrorV1::
                kArchiveBarrierInvalid;
        }
        if (!Allow(
                hooks,
                FinalizationArchiveSourceCleanupMutationPointV1::
                    kBeforeSourceUnlink,
                index)) {
            SetDiagnostic(
                diagnostic,
                "injected before source report unlink");
            return FinalizationArchiveSourceCleanupErrorV1::
                kInjectedInterruption;
        }
        if (!NameMatchesDescriptor(
                parent.descriptor.get(),
                task.filename.c_str(),
                source.get(),
                &source_status) ||
            !UnlinkAtNoIntr(
                parent.descriptor.get(),
                task.filename.c_str())) {
            SetDiagnostic(
                diagnostic,
                "source report unlink failed");
            return FinalizationArchiveSourceCleanupErrorV1::
                kUnlinkFailure;
        }
        source.Reset();
        *removed = true;
        if (!Allow(
                hooks,
                FinalizationArchiveSourceCleanupMutationPointV1::
                    kAfterSourceUnlinkBeforeParentSync,
                index)) {
            SetDiagnostic(
                diagnostic,
                "injected after source report unlink");
            return FinalizationArchiveSourceCleanupErrorV1::
                kInjectedInterruption;
        }
        if (!FsyncNoIntr(parent.descriptor.get())) {
            SetDiagnostic(
                diagnostic,
                "source report parent fsync failed");
            return FinalizationArchiveSourceCleanupErrorV1::
                kSyncFailure;
        }
    }

    if (!Allow(
            hooks,
            FinalizationArchiveSourceCleanupMutationPointV1::
                kBeforeFinalReadback,
            index)) {
        SetDiagnostic(
            diagnostic,
            "injected before source cleanup readback");
        return FinalizationArchiveSourceCleanupErrorV1::
            kInjectedInterruption;
    }
    NameObservation final_observation{};
    if (!ConfirmAbsent(
            parent.descriptor.get(),
            parent.status,
            task.filename,
            &final_observation)) {
        if (final_observation ==
            NameObservation::kLimitExceeded) {
            return FinalizationArchiveSourceCleanupErrorV1::
                kCandidateLimitExceeded;
        }
        SetDiagnostic(
            diagnostic,
            "source report name is not durably absent");
        return FinalizationArchiveSourceCleanupErrorV1::
            kReadbackFailure;
    }

    retained->parent_directory_fd =
        parent.descriptor.Release();
    retained->audit_rooted = task.audit_rooted;
    retained->parent_locator = task.parent_locator;
    retained->filename = task.filename;
    retained->parent_device =
        static_cast<std::uint64_t>(
            parent.status.st_dev);
    retained->parent_inode =
        static_cast<std::uint64_t>(
            parent.status.st_ino);
    return FinalizationArchiveSourceCleanupErrorV1::kNone;
}

}  // namespace

FinalizationArchiveSourceCleanupReceiptV1::RetainedSourceV1::
    ~RetainedSourceV1() {
    if (parent_directory_fd >= 0) {
        static_cast<void>(::close(parent_directory_fd));
    }
}

FinalizationArchiveSourceCleanupReceiptV1::RetainedSourceV1::
    RetainedSourceV1(RetainedSourceV1&& other) noexcept
    : parent_directory_fd(
          std::exchange(other.parent_directory_fd, -1)),
      audit_rooted(other.audit_rooted),
      parent_locator(std::move(other.parent_locator)),
      filename(std::move(other.filename)),
      parent_device(other.parent_device),
      parent_inode(other.parent_inode) {}

FinalizationArchiveSourceCleanupReceiptV1::RetainedSourceV1&
FinalizationArchiveSourceCleanupReceiptV1::RetainedSourceV1::
operator=(RetainedSourceV1&& other) noexcept {
    if (this != &other) {
        if (parent_directory_fd >= 0) {
            static_cast<void>(::close(parent_directory_fd));
        }
        parent_directory_fd =
            std::exchange(other.parent_directory_fd, -1);
        audit_rooted = other.audit_rooted;
        parent_locator = std::move(other.parent_locator);
        filename = std::move(other.filename);
        parent_device = other.parent_device;
        parent_inode = other.parent_inode;
    }
    return *this;
}

FinalizationArchiveSourceCleanupReceiptV1::
    FinalizationArchiveSourceCleanupReceiptV1(
        RawV1Identity reserve_state_uuid,
        RawV1Identity finalization_cycle_id,
        RawV1Digest archive_manifest_sha256,
        int raw_root_fd,
        std::uint64_t raw_root_device,
        std::uint64_t raw_root_inode,
        int maintenance_lease_fd,
        std::uint64_t maintenance_lease_device,
        std::uint64_t maintenance_lease_inode,
        ReserveStateV1HeaderWire frozen_state_header,
        ReserveStateV1SlotWire frozen_all_done_slot,
        RawV1Digest frozen_state_sha256,
        std::unique_ptr<RawReserveStateFileV1>
            state_file,
        std::unique_ptr<
            PublishedFinalizationArchiveReceiptV1>
            archive_receipt,
        std::vector<RetainedSourceV1> sources) noexcept
    : reserve_state_uuid_(reserve_state_uuid),
      finalization_cycle_id_(finalization_cycle_id),
      archive_manifest_sha256_(
          archive_manifest_sha256),
      raw_root_fd_(raw_root_fd),
      raw_root_device_(raw_root_device),
      raw_root_inode_(raw_root_inode),
      maintenance_lease_fd_(maintenance_lease_fd),
      maintenance_lease_device_(
          maintenance_lease_device),
      maintenance_lease_inode_(
          maintenance_lease_inode),
      frozen_state_header_(frozen_state_header),
      frozen_all_done_slot_(frozen_all_done_slot),
      frozen_state_sha256_(frozen_state_sha256),
      state_file_(std::move(state_file)),
      archive_receipt_(std::move(archive_receipt)),
      sources_(std::move(sources)) {}

FinalizationArchiveSourceCleanupReceiptV1::
    ~FinalizationArchiveSourceCleanupReceiptV1() {
    if (raw_root_fd_ >= 0) {
        static_cast<void>(::close(raw_root_fd_));
    }
    if (maintenance_lease_fd_ >= 0) {
        static_cast<void>(
            ::close(maintenance_lease_fd_));
    }
}

bool FinalizationArchiveSourceCleanupReceiptV1::Validate(
    std::string* diagnostic) const noexcept {
    try {
        struct stat raw_root {};
        ArchiveStateEvidence frozen{};
        frozen.header_wire = frozen_state_header_;
        frozen.all_done_slot_wire =
            frozen_all_done_slot_;
        frozen.state_sha256 = frozen_state_sha256_;
        RawV1Digest recomputed_state_sha256{};
        if (archive_receipt_ == nullptr ||
            !archive_receipt_->Validate(diagnostic) ||
            raw_root_fd_ < 0 ||
            ::fstat(raw_root_fd_, &raw_root) != 0 ||
            !IsSafeDirectory(raw_root) ||
            static_cast<std::uint64_t>(
                raw_root.st_dev) !=
                raw_root_device_ ||
            static_cast<std::uint64_t>(
                raw_root.st_ino) !=
                raw_root_inode_ ||
            DecodeReserveCoordinatorHeaderV1(
                frozen.header_wire,
                &frozen.header) !=
                ReserveStateV1Error::kNone ||
            DecodeReserveStateSlotV1(
                frozen.header,
                frozen.all_done_slot_wire,
                &frozen.all_done_slot) !=
                ReserveStateV1Error::kNone ||
            !AllDoneConsumed(frozen.all_done_slot) ||
            !ComputeStateDigest(
                frozen.header_wire,
                frozen.all_done_slot_wire,
                &recomputed_state_sha256) ||
            recomputed_state_sha256 !=
                frozen.state_sha256 ||
            frozen.state_sha256 !=
                archive_receipt_->state_sha256() ||
            archive_receipt_->reserve_state_uuid() !=
                reserve_state_uuid_ ||
            archive_receipt_->finalization_cycle_id() !=
                finalization_cycle_id_ ||
            archive_receipt_->manifest_sha256() !=
                archive_manifest_sha256_ ||
            sources_.size() >
                kFinalizationArchiveV1MaximumArtifacts) {
            SetDiagnostic(
                diagnostic,
                "cleanup receipt archive or Raw-root identity changed");
            return false;
        }
        struct stat lease_status {};
        if (ValidateMaintenanceLeaseFd(
                raw_root_fd_,
                raw_root,
                maintenance_lease_fd_,
                frozen.header,
                &lease_status,
                diagnostic) !=
                FinalizationArchiveSourceCleanupErrorV1::
                    kNone ||
            static_cast<std::uint64_t>(
                lease_status.st_dev) !=
                maintenance_lease_device_ ||
            static_cast<std::uint64_t>(
                lease_status.st_ino) !=
                maintenance_lease_inode_ ||
            ::flock(
                maintenance_lease_fd_,
                LOCK_EX | LOCK_NB) != 0 ||
            ValidateMaintenanceState(
                state_file_.get(),
                frozen,
                true,
                diagnostic) !=
                FinalizationArchiveSourceCleanupErrorV1::
                    kNone) {
            SetDiagnostic(
                diagnostic,
                "cleanup receipt maintenance lock or state changed");
            return false;
        }
        const int audit_fd =
            FinalizationArchiveSourceCleanupAccessV1::
                AuditDirectoryFd(*archive_receipt_);
        struct stat audit {};
        if (audit_fd < 0 ||
            ::fstat(audit_fd, &audit) != 0 ||
            !IsSafeDirectory(audit)) {
            SetDiagnostic(
                diagnostic,
                "cleanup receipt audit root changed");
            return false;
        }
        for (const RetainedSourceV1& source :
             sources_) {
            const int root =
                source.audit_rooted ? audit_fd
                                    : raw_root_fd_;
            const struct stat& root_status =
                source.audit_rooted ? audit : raw_root;
            OpenedParent resolved{};
            struct stat retained {};
            if (source.parent_directory_fd < 0 ||
                ::fstat(
                    source.parent_directory_fd,
                    &retained) != 0 ||
                !IsSafeDirectory(retained) ||
                static_cast<std::uint64_t>(
                    retained.st_dev) !=
                    source.parent_device ||
                static_cast<std::uint64_t>(
                    retained.st_ino) !=
                    source.parent_inode ||
                OpenParent(
                    root,
                    root_status,
                    source.parent_locator,
                    &resolved) !=
                    FinalizationArchiveSourceCleanupErrorV1::
                        kNone ||
                !SameInode(resolved.status, retained) ||
                !ConfirmAbsent(
                    source.parent_directory_fd,
                    retained,
                    source.filename)) {
                SetDiagnostic(
                    diagnostic,
                    "cleanup receipt source parent or absence changed");
                return false;
            }
        }
        return true;
    } catch (...) {
        SetDiagnostic(
            diagnostic,
            "cleanup receipt validation allocation failed");
        return false;
    }
}

std::string_view
FinalizationArchiveSourceCleanupErrorV1Name(
    FinalizationArchiveSourceCleanupErrorV1 error) noexcept {
    switch (error) {
    case FinalizationArchiveSourceCleanupErrorV1::kNone:
        return "NONE";
    case FinalizationArchiveSourceCleanupErrorV1::
        kInvalidArgument:
        return "INVALID_ARGUMENT";
    case FinalizationArchiveSourceCleanupErrorV1::
        kArchiveBarrierInvalid:
        return "ARCHIVE_BARRIER_INVALID";
    case FinalizationArchiveSourceCleanupErrorV1::
        kUnsafeRawRoot:
        return "UNSAFE_RAW_ROOT";
    case FinalizationArchiveSourceCleanupErrorV1::
        kInvalidSourceLocator:
        return "INVALID_SOURCE_LOCATOR";
    case FinalizationArchiveSourceCleanupErrorV1::
        kDuplicateSourceMapping:
        return "DUPLICATE_SOURCE_MAPPING";
    case FinalizationArchiveSourceCleanupErrorV1::
        kMaintenanceLeaseMissing:
        return "MAINTENANCE_LEASE_MISSING";
    case FinalizationArchiveSourceCleanupErrorV1::
        kMaintenanceLeaseUnsafe:
        return "MAINTENANCE_LEASE_UNSAFE";
    case FinalizationArchiveSourceCleanupErrorV1::
        kMaintenanceLeaseBusy:
        return "MAINTENANCE_LEASE_BUSY";
    case FinalizationArchiveSourceCleanupErrorV1::
        kMaintenanceMarkerMismatch:
        return "MAINTENANCE_MARKER_MISMATCH";
    case FinalizationArchiveSourceCleanupErrorV1::
        kStateAttachFailure:
        return "STATE_ATTACH_FAILURE";
    case FinalizationArchiveSourceCleanupErrorV1::
        kStateMismatch:
        return "STATE_MISMATCH";
    case FinalizationArchiveSourceCleanupErrorV1::
        kStateNotAllDone:
        return "STATE_NOT_ALL_DONE";
    case FinalizationArchiveSourceCleanupErrorV1::
        kSourceParentMissing:
        return "SOURCE_PARENT_MISSING";
    case FinalizationArchiveSourceCleanupErrorV1::
        kUnsafeSourceParent:
        return "UNSAFE_SOURCE_PARENT";
    case FinalizationArchiveSourceCleanupErrorV1::
        kSourceConflict:
        return "SOURCE_CONFLICT";
    case FinalizationArchiveSourceCleanupErrorV1::
        kCandidateLimitExceeded:
        return "CANDIDATE_LIMIT_EXCEEDED";
    case FinalizationArchiveSourceCleanupErrorV1::
        kUnlinkFailure:
        return "UNLINK_FAILURE";
    case FinalizationArchiveSourceCleanupErrorV1::
        kSyncFailure:
        return "SYNC_FAILURE";
    case FinalizationArchiveSourceCleanupErrorV1::
        kInjectedInterruption:
        return "INJECTED_INTERRUPTION";
    case FinalizationArchiveSourceCleanupErrorV1::
        kReadbackFailure:
        return "READBACK_FAILURE";
    case FinalizationArchiveSourceCleanupErrorV1::
        kAllocationFailure:
        return "ALLOCATION_FAILURE";
    }
    return "UNKNOWN";
}

FinalizationArchiveSourceCleanupResultV1
CleanupFinalizationArchiveSourceReportsV1At(
    int retained_raw_root_fd,
    std::unique_ptr<
        PublishedFinalizationArchiveReceiptV1>
        archive_receipt,
    const FinalizationArchiveSourceCleanupHooksV1* hooks,
    std::string* diagnostic) noexcept {
    FinalizationArchiveSourceCleanupResultV1 result{};
    try {
        if (archive_receipt == nullptr) {
            result.error =
                FinalizationArchiveSourceCleanupErrorV1::
                    kInvalidArgument;
            SetDiagnostic(
                diagnostic,
                "archive receipt is required and is consumed");
            return result;
        }
        if (!archive_receipt->Validate(diagnostic)) {
            result.error =
                FinalizationArchiveSourceCleanupErrorV1::
                    kArchiveBarrierInvalid;
            return result;
        }
        ScopedFd raw_root;
        struct stat raw_root_status {};
        result.error = OpenRetainedRoot(
            retained_raw_root_fd,
            &raw_root,
            &raw_root_status);
        if (result.error !=
            FinalizationArchiveSourceCleanupErrorV1::
                kNone) {
            SetDiagnostic(
                diagnostic,
                "Raw root is not a retained owner-only directory");
            return result;
        }

        ArchiveStateEvidence frozen_state{};
        result.error = LoadArchiveStateEvidence(
            *archive_receipt, &frozen_state);
        if (result.error !=
            FinalizationArchiveSourceCleanupErrorV1::
                kNone) {
            SetDiagnostic(
                diagnostic,
                "archive state header/slot evidence is invalid");
            return result;
        }
        MaintenanceSession maintenance{};
        result.error = AcquireMaintenanceSession(
            raw_root.get(),
            raw_root_status,
            frozen_state,
            &maintenance,
            diagnostic);
        if (result.error !=
            FinalizationArchiveSourceCleanupErrorV1::
                kNone) {
            return result;
        }

        std::vector<CleanupTask> tasks;
        result.error =
            LoadCleanupTasks(*archive_receipt, &tasks);
        if (result.error !=
            FinalizationArchiveSourceCleanupErrorV1::
                kNone) {
            SetDiagnostic(
                diagnostic,
                "archive contains an unsafe source cleanup mapping");
            return result;
        }
        result.cleanable_source_count = tasks.size();
        std::vector<
            FinalizationArchiveSourceCleanupReceiptV1::
                RetainedSourceV1>
            retained_sources;
        retained_sources.reserve(tasks.size());
        const int audit_fd =
            FinalizationArchiveSourceCleanupAccessV1::
                AuditDirectoryFd(*archive_receipt);
        struct stat audit_status {};
        if (audit_fd < 0 ||
            ::fstat(audit_fd, &audit_status) != 0 ||
            !IsSafeDirectory(audit_status)) {
            result.error =
                FinalizationArchiveSourceCleanupErrorV1::
                    kArchiveBarrierInvalid;
            return result;
        }

        for (std::size_t index = 0U;
             index < tasks.size();
             ++index) {
            if (!archive_receipt->Validate(diagnostic)) {
                result.error =
                    FinalizationArchiveSourceCleanupErrorV1::
                        kArchiveBarrierInvalid;
                return result;
            }
            result.error = ValidateMaintenanceState(
                maintenance.state_file.get(),
                frozen_state,
                true,
                diagnostic);
            if (result.error !=
                FinalizationArchiveSourceCleanupErrorV1::
                    kNone) {
                return result;
            }
            const CleanupTask& task = tasks[index];
            const int root_fd =
                task.audit_rooted ? audit_fd
                                  : raw_root.get();
            const struct stat& root_status =
                task.audit_rooted ? audit_status
                                  : raw_root_status;
            FinalizationArchiveSourceCleanupReceiptV1::
                RetainedSourceV1 retained{};
            bool removed = false;
            bool adopted_absent = false;
            result.error = CleanupOne(
                index,
                task,
                root_fd,
                root_status,
                archive_receipt.get(),
                hooks,
                &retained,
                &removed,
                &adopted_absent,
                diagnostic);
            if (result.error !=
                FinalizationArchiveSourceCleanupErrorV1::
                    kNone) {
                return result;
            }
            if (removed) {
                ++result.removed_source_count;
            }
            if (adopted_absent) {
                ++result.adopted_absent_source_count;
            }
            retained_sources.push_back(
                std::move(retained));
        }
        if (!archive_receipt->Validate(diagnostic)) {
            result.error =
                FinalizationArchiveSourceCleanupErrorV1::
                    kArchiveBarrierInvalid;
            return result;
        }
        result.error = ValidateMaintenanceState(
            maintenance.state_file.get(),
            frozen_state,
            true,
            diagnostic);
        if (result.error !=
            FinalizationArchiveSourceCleanupErrorV1::
                kNone) {
            return result;
        }
        result.receipt =
            FinalizationArchiveSourceCleanupAccessV1::
                MakeReceipt(
                    raw_root.get(),
                    raw_root_status,
                    std::move(maintenance),
                    frozen_state,
                    std::move(archive_receipt),
                    std::move(retained_sources));
        if (result.receipt == nullptr ||
            !result.receipt->Validate(diagnostic)) {
            result.receipt.reset();
            result.error =
                FinalizationArchiveSourceCleanupErrorV1::
                    kReadbackFailure;
            return result;
        }
        return result;
    } catch (...) {
        result.receipt.reset();
        result.error =
            FinalizationArchiveSourceCleanupErrorV1::
                kAllocationFailure;
        SetDiagnostic(
            diagnostic,
            "source cleanup allocation failed");
        return result;
    }
}

}  // namespace l2flow::ingress
