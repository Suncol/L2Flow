#include "l2flow/ingress/raw_reserve_coordinator.h"

#include "l2flow/common/sha256.h"
#include "l2flow/ingress/finalization_report_receipt.h"
#include "l2flow/ingress/finalization_report_v1.h"
#include "l2flow/ingress/raw_recovery_maintenance_report_receipt.h"
#include "l2flow/ingress/raw_recovery_terminal_report_receipt.h"
#include "l2flow/ingress/raw_finalization_continuation_posix.h"
#include "l2flow/ingress/raw_recovery_maintenance_report_v1.h"
#include "l2flow/ingress/raw_reserve_active_activation_receipt.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/raw_sealed_certificate_receipt.h"
#include "l2flow/ingress/raw_sealed_certificate_v1.h"
#include "l2flow/ingress/scaffolding_finalization_report_receipt.h"
#include "l2flow/ingress/scaffolding_finalization_report_v1.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <new>
#include <span>
#include <utility>

#include <dirent.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace l2flow::ingress {
namespace {

void SetError(
    std::string* error,
    std::string message) noexcept;

class ScopedFd final {
public:
    ScopedFd() noexcept = default;
    explicit ScopedFd(int descriptor) noexcept
        : descriptor_(descriptor) {}
    ~ScopedFd() {
        if (descriptor_ >= 0) {
            static_cast<void>(::close(descriptor_));
        }
    }
    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
    ScopedFd(ScopedFd&& other) noexcept
        : descriptor_(
              std::exchange(other.descriptor_, -1)) {}
    ScopedFd& operator=(ScopedFd&& other) noexcept {
        if (this != &other) {
            if (descriptor_ >= 0) {
                static_cast<void>(::close(descriptor_));
            }
            descriptor_ =
                std::exchange(other.descriptor_, -1);
        }
        return *this;
    }
    [[nodiscard]] int get() const noexcept {
        return descriptor_;
    }
    [[nodiscard]] int Release() noexcept {
        return std::exchange(descriptor_, -1);
    }

private:
    int descriptor_ = -1;
};

class ScopedExclusiveFlock final {
public:
    explicit ScopedExclusiveFlock(int descriptor) noexcept
        : descriptor_(descriptor) {
        if (descriptor_ < 0) {
            return;
        }
        for (;;) {
            if (::flock(descriptor_, LOCK_EX) == 0) {
                locked_ = true;
                return;
            }
            if (errno != EINTR) {
                return;
            }
        }
    }
    ~ScopedExclusiveFlock() {
        if (!locked_) {
            return;
        }
        while (::flock(descriptor_, LOCK_UN) != 0 &&
               errno == EINTR) {
        }
    }
    ScopedExclusiveFlock(
        const ScopedExclusiveFlock&) = delete;
    ScopedExclusiveFlock& operator=(
        const ScopedExclusiveFlock&) = delete;
    [[nodiscard]] bool locked() const noexcept {
        return locked_;
    }

private:
    int descriptor_ = -1;
    bool locked_ = false;
};

[[nodiscard]] bool IsGregorianDate(
    std::uint32_t value) noexcept {
    if (value < 10000101U ||
        value > 99991231U) {
        return false;
    }
    const std::uint32_t year = value / 10000U;
    const std::uint32_t month =
        (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (month == 0U || month > 12U) {
        return false;
    }
    constexpr std::array<std::uint32_t, 12U> days{{
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U}};
    std::uint32_t maximum = days[month - 1U];
    const bool leap =
        year % 4U == 0U &&
        (year % 100U != 0U || year % 400U == 0U);
    if (month == 2U && leap) {
        maximum = 29U;
    }
    return day != 0U && day <= maximum;
}

[[nodiscard]] int OpenDirectoryAt(
    int parent,
    const char* name) noexcept {
    for (;;) {
        const int descriptor = ::openat(
            parent,
            name,
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_CLOEXEC | O_NOATIME);
        if (descriptor >= 0 || errno != EINTR) {
            return descriptor;
        }
    }
}

[[nodiscard]] bool PrivateOwnedDirectory(
    int descriptor,
    struct stat* status) noexcept {
    struct stat inspected {};
    if (::fstat(descriptor, &inspected) != 0 ||
        !S_ISDIR(inspected.st_mode) ||
        inspected.st_uid != ::geteuid() ||
        (inspected.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
        return false;
    }
    if (status != nullptr) {
        *status = inspected;
    }
    return true;
}

[[nodiscard]] bool SameNamedDirectory(
    int parent,
    const char* name,
    const struct stat& opened) noexcept {
    struct stat named {};
    return ::fstatat(
               parent,
               name,
               &named,
               AT_SYMLINK_NOFOLLOW) == 0 &&
           S_ISDIR(named.st_mode) &&
           named.st_dev == opened.st_dev &&
           named.st_ino == opened.st_ino;
}

struct OpenedTarget final {
    ScopedFd route;
    struct stat root_status {};
    struct stat route_status {};
};

[[nodiscard]] bool SameInode(
    const struct stat& left,
    const struct stat& right) noexcept {
    return left.st_dev == right.st_dev &&
           left.st_ino == right.st_ino;
}

[[nodiscard]] bool IsLowerHex(
    std::string_view text) noexcept {
    for (const char character : text) {
        if (!((character >= '0' &&
               character <= '9') ||
              (character >= 'a' &&
               character <= 'f'))) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool IsSealedCertificateFilename(
    std::string_view name) noexcept {
    constexpr std::string_view prefix =
        "sealed-raw-";
    constexpr std::string_view suffix = ".json";
    constexpr std::size_t expected_size =
        prefix.size() + 32U + 1U + 64U +
        suffix.size();
    return name.size() == expected_size &&
           name.substr(0U, prefix.size()) == prefix &&
           name[prefix.size() + 32U] == '-' &&
           name.substr(name.size() - suffix.size()) ==
               suffix &&
           IsLowerHex(
               name.substr(prefix.size(), 32U)) &&
           IsLowerHex(
               name.substr(
                   prefix.size() + 33U, 64U));
}

[[nodiscard]] bool IsRecoveryMaintenanceReportFilename(
    std::string_view name) noexcept {
    constexpr std::string_view prefix = "recovery-";
    constexpr std::string_view suffix = ".json";
    constexpr std::size_t expected_size =
        prefix.size() + 32U + suffix.size();
    return name.size() == expected_size &&
           name.substr(0U, prefix.size()) == prefix &&
           name.substr(name.size() - suffix.size()) ==
               suffix &&
           IsLowerHex(name.substr(prefix.size(), 32U));
}

[[nodiscard]] bool ValidateRecoveryReportReceiptFiles(
    int route_directory_fd,
    int maintenance_directory_fd,
    int report_fd,
    const std::string& filename,
    const RawV1Digest& expected_sha256,
    RecoveryMaintenanceReportV1* parsed_report,
    std::string* error) noexcept {
    struct stat route {};
    struct stat maintenance_before {};
    struct stat named_maintenance_before {};
    struct stat report_before {};
    struct stat named_report_before {};
    if (parsed_report == nullptr ||
        route_directory_fd < 0 ||
        maintenance_directory_fd < 0 ||
        report_fd < 0 ||
        !IsRecoveryMaintenanceReportFilename(filename) ||
        ::fstat(route_directory_fd, &route) != 0 ||
        ::fstat(
            maintenance_directory_fd,
            &maintenance_before) != 0 ||
        ::fstatat(
            route_directory_fd,
            "maintenance",
            &named_maintenance_before,
            AT_SYMLINK_NOFOLLOW) != 0 ||
        !S_ISDIR(maintenance_before.st_mode) ||
        maintenance_before.st_uid != ::geteuid() ||
        (maintenance_before.st_mode & 07777U) != 0700U ||
        maintenance_before.st_dev != route.st_dev ||
        !SameInode(
            maintenance_before,
            named_maintenance_before) ||
        ::fstat(report_fd, &report_before) != 0 ||
        ::fstatat(
            maintenance_directory_fd,
            filename.c_str(),
            &named_report_before,
            AT_SYMLINK_NOFOLLOW) != 0 ||
        !S_ISREG(report_before.st_mode) ||
        report_before.st_uid != ::geteuid() ||
        (report_before.st_mode & 07777U) != 0600U ||
        report_before.st_nlink !=
            static_cast<nlink_t>(1) ||
        report_before.st_size <= 0 ||
        static_cast<std::uint64_t>(
            report_before.st_size) >
            kRecoveryMaintenanceReportV1MaximumBytes ||
        report_before.st_dev != maintenance_before.st_dev ||
        !SameInode(report_before, named_report_before)) {
        SetError(
            error,
            "recovery maintenance report receipt files are no longer safely named");
        return false;
    }

    try {
        std::string exact_bytes(
            static_cast<std::size_t>(report_before.st_size),
            '\0');
        std::size_t completed = 0U;
        while (completed < exact_bytes.size()) {
            const ssize_t read = ::pread(
                report_fd,
                exact_bytes.data() +
                    static_cast<std::ptrdiff_t>(completed),
                exact_bytes.size() - completed,
                static_cast<off_t>(completed));
            if (read > 0) {
                completed +=
                    static_cast<std::size_t>(read);
                continue;
            }
            if (read < 0 && errno == EINTR) {
                continue;
            }
            SetError(
                error,
                "recovery maintenance report receipt readback failed");
            return false;
        }
        const auto exact_span = std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(
                exact_bytes.data()),
            exact_bytes.size());
        if (l2flow::common::ComputeSha256(exact_span) !=
            expected_sha256) {
            SetError(
                error,
                "recovery maintenance report receipt content hash changed");
            return false;
        }
        RecoveryMaintenanceReportV1 decoded{};
        if (ParseRecoveryMaintenanceReportV1Jcs(
                exact_bytes, &decoded) !=
            RecoveryMaintenanceReportV1Error::kNone) {
            SetError(
                error,
                "recovery maintenance report receipt no longer has exact valid JCS");
            return false;
        }
        std::string canonical;
        if (EncodeRecoveryMaintenanceReportV1Jcs(
                decoded, &canonical) !=
                RecoveryMaintenanceReportV1Error::kNone ||
            canonical != exact_bytes) {
            SetError(
                error,
                "recovery maintenance report receipt is not canonical");
            return false;
        }

        struct stat maintenance_after {};
        struct stat named_maintenance_after {};
        struct stat report_after {};
        struct stat named_report_after {};
        if (::fstat(
                maintenance_directory_fd,
                &maintenance_after) != 0 ||
            ::fstatat(
                route_directory_fd,
                "maintenance",
                &named_maintenance_after,
                AT_SYMLINK_NOFOLLOW) != 0 ||
            ::fstat(report_fd, &report_after) != 0 ||
            ::fstatat(
                maintenance_directory_fd,
                filename.c_str(),
                &named_report_after,
                AT_SYMLINK_NOFOLLOW) != 0 ||
            !SameInode(
                maintenance_before,
                maintenance_after) ||
            !SameInode(
                maintenance_after,
                named_maintenance_after) ||
            maintenance_after.st_mode !=
                maintenance_before.st_mode ||
            maintenance_after.st_uid !=
                maintenance_before.st_uid ||
            !SameInode(report_before, report_after) ||
            !SameInode(report_after, named_report_after) ||
            report_after.st_size != report_before.st_size ||
            report_after.st_mode != report_before.st_mode ||
            report_after.st_uid != report_before.st_uid ||
            report_after.st_nlink != report_before.st_nlink) {
            SetError(
                error,
                "recovery maintenance report receipt changed during activation readback");
            return false;
        }
        *parsed_report = std::move(decoded);
        return true;
    } catch (...) {
        SetError(
            error,
            "cannot allocate recovery maintenance report activation readback");
        return false;
    }
}

[[nodiscard]] bool ValidateCertificateReceiptFiles(
    int route_directory_fd,
    int maintenance_directory_fd,
    int certificate_fd,
    const std::string& filename,
    const RawV1Digest& expected_sha256,
    std::string* error) noexcept {
    struct stat route {};
    struct stat maintenance_before {};
    struct stat named_maintenance_before {};
    struct stat certificate_before {};
    struct stat named_certificate_before {};
    if (route_directory_fd < 0 ||
        maintenance_directory_fd < 0 ||
        certificate_fd < 0 ||
        !IsSealedCertificateFilename(filename) ||
        ::fstat(route_directory_fd, &route) != 0 ||
        ::fstat(
            maintenance_directory_fd,
            &maintenance_before) != 0 ||
        ::fstatat(
            route_directory_fd,
            "maintenance",
            &named_maintenance_before,
            AT_SYMLINK_NOFOLLOW) != 0 ||
        !S_ISDIR(maintenance_before.st_mode) ||
        maintenance_before.st_uid != ::geteuid() ||
        (maintenance_before.st_mode & 07777U) != 0700U ||
        maintenance_before.st_dev != route.st_dev ||
        !SameInode(
            maintenance_before,
            named_maintenance_before) ||
        ::fstat(certificate_fd, &certificate_before) != 0 ||
        ::fstatat(
            maintenance_directory_fd,
            filename.c_str(),
            &named_certificate_before,
            AT_SYMLINK_NOFOLLOW) != 0 ||
        !S_ISREG(certificate_before.st_mode) ||
        certificate_before.st_uid != ::geteuid() ||
        (certificate_before.st_mode & 07777U) != 0600U ||
        certificate_before.st_nlink !=
            static_cast<nlink_t>(1) ||
        certificate_before.st_size <= 0 ||
        static_cast<std::uint64_t>(
            certificate_before.st_size) >
            kSealedRawCertificateV1MaximumBytes ||
        certificate_before.st_dev !=
            maintenance_before.st_dev ||
        !SameInode(
            certificate_before,
            named_certificate_before)) {
        SetError(
            error,
            "sealed Raw certificate receipt files are no longer safely named");
        return false;
    }

    std::array<
        std::byte,
        kSealedRawCertificateV1MaximumBytes>
        bytes{};
    const std::size_t size =
        static_cast<std::size_t>(
            certificate_before.st_size);
    std::size_t completed = 0U;
    while (completed < size) {
        const ssize_t read = ::pread(
            certificate_fd,
            bytes.data() +
                static_cast<std::ptrdiff_t>(
                    completed),
            size - completed,
            static_cast<off_t>(completed));
        if (read > 0) {
            completed +=
                static_cast<std::size_t>(read);
            continue;
        }
        if (read < 0 && errno == EINTR) {
            continue;
        }
        SetError(
            error,
            "sealed Raw certificate receipt readback failed");
        return false;
    }
    if (l2flow::common::ComputeSha256(
            std::span<const std::byte>(
                bytes.data(), size)) !=
        expected_sha256) {
        SetError(
            error,
            "sealed Raw certificate receipt content hash changed");
        return false;
    }

    struct stat maintenance_after {};
    struct stat named_maintenance_after {};
    struct stat certificate_after {};
    struct stat named_certificate_after {};
    if (::fstat(
            maintenance_directory_fd,
            &maintenance_after) != 0 ||
        ::fstatat(
            route_directory_fd,
            "maintenance",
            &named_maintenance_after,
            AT_SYMLINK_NOFOLLOW) != 0 ||
        ::fstat(certificate_fd, &certificate_after) != 0 ||
        ::fstatat(
            maintenance_directory_fd,
            filename.c_str(),
            &named_certificate_after,
            AT_SYMLINK_NOFOLLOW) != 0 ||
        !SameInode(
            maintenance_before,
            maintenance_after) ||
        !SameInode(
            maintenance_after,
            named_maintenance_after) ||
        maintenance_after.st_mode !=
            maintenance_before.st_mode ||
        maintenance_after.st_uid !=
            maintenance_before.st_uid ||
        !SameInode(
            certificate_before,
            certificate_after) ||
        !SameInode(
            certificate_after,
            named_certificate_after) ||
        certificate_after.st_size !=
            certificate_before.st_size ||
        certificate_after.st_mode !=
            certificate_before.st_mode ||
        certificate_after.st_uid !=
            certificate_before.st_uid ||
        certificate_after.st_nlink !=
            certificate_before.st_nlink) {
        SetError(
            error,
            "sealed Raw certificate receipt changed during unregister readback");
        return false;
    }
    return true;
}

[[nodiscard]] bool IsEmptyAnchorTombstoneFilename(
    std::string_view name) noexcept {
    constexpr std::string_view prefix = "empty-anchor-";
    constexpr std::string_view suffix = ".json";
    constexpr std::size_t expected_size =
        prefix.size() + 32U + suffix.size();
    return name.size() == expected_size &&
           name.substr(0U, prefix.size()) == prefix &&
           name.substr(name.size() - suffix.size()) ==
               suffix &&
           IsLowerHex(name.substr(prefix.size(), 32U));
}

[[nodiscard]] bool ReadBoundedNamedEvidenceFile(
    int parent_fd,
    int fd,
    const std::string& filename,
    std::uint64_t maximum_bytes,
    std::uint64_t expected_device,
    std::uint64_t expected_inode,
    std::string* bytes,
    struct stat* output_status,
    std::string* error) noexcept {
    if (parent_fd < 0 || fd < 0 ||
        filename.empty() || bytes == nullptr ||
        output_status == nullptr) {
        SetError(error, "terminal evidence argument is invalid");
        return false;
    }
    try {
        struct stat before {};
        struct stat named_before {};
        if (::fstat(fd, &before) != 0 ||
            ::fstatat(
                parent_fd,
                filename.c_str(),
                &named_before,
                AT_SYMLINK_NOFOLLOW) != 0 ||
            !S_ISREG(before.st_mode) ||
            before.st_uid != ::geteuid() ||
            (before.st_mode & 07777U) != 0600U ||
            before.st_nlink !=
                static_cast<nlink_t>(1) ||
            before.st_size <= 0 ||
            static_cast<std::uint64_t>(
                before.st_size) > maximum_bytes ||
            static_cast<std::uint64_t>(
                before.st_dev) != expected_device ||
            static_cast<std::uint64_t>(
                before.st_ino) != expected_inode ||
            !SameInode(before, named_before)) {
            SetError(
                error,
                "terminal sidecar is no longer the retained safe named inode");
            return false;
        }
        std::string readback(
            static_cast<std::size_t>(before.st_size),
            '\0');
        std::size_t completed = 0U;
        while (completed < readback.size()) {
            const ssize_t read = ::pread(
                fd,
                readback.data() +
                    static_cast<std::ptrdiff_t>(
                        completed),
                readback.size() - completed,
                static_cast<off_t>(completed));
            if (read > 0) {
                completed +=
                    static_cast<std::size_t>(read);
                continue;
            }
            if (read < 0 && errno == EINTR) {
                continue;
            }
            SetError(
                error,
                "terminal sidecar retained-fd readback failed");
            return false;
        }
        struct stat after {};
        struct stat named_after {};
        if (::fstat(fd, &after) != 0 ||
            ::fstatat(
                parent_fd,
                filename.c_str(),
                &named_after,
                AT_SYMLINK_NOFOLLOW) != 0 ||
            !SameInode(before, after) ||
            !SameInode(after, named_after) ||
            before.st_mode != after.st_mode ||
            before.st_uid != after.st_uid ||
            before.st_nlink != after.st_nlink ||
            before.st_size != after.st_size) {
            SetError(
                error,
                "terminal sidecar changed during retained-fd readback");
            return false;
        }
        bytes->swap(readback);
        *output_status = after;
        return true;
    } catch (...) {
        SetError(
            error,
            "cannot allocate terminal sidecar readback");
        return false;
    }
}

[[nodiscard]] bool ValidateTerminalJournalEvidence(
    int route_directory_fd,
    int journal_fd,
    const RecoveryMaintenanceReportV1& report,
    const RawV1JournalHeaderWire& expected_header,
    std::uint64_t expected_device,
    std::uint64_t expected_inode,
    std::string* error) noexcept {
    const std::uint64_t expected_size =
        report.result ==
                RecoveryMaintenanceResultV1::
                    kEmptyAnchorOnly
            ? static_cast<std::uint64_t>(
                  kRawV1JournalHeaderBytes)
            : report.recovery_range
                  .final_durable_journal_size;
    struct stat before {};
    struct stat named_before {};
    const int flags =
        journal_fd < 0
            ? -1
            : ::fcntl(journal_fd, F_GETFL);
    if (route_directory_fd < 0 ||
        journal_fd < 0 ||
        expected_size < kRawV1JournalHeaderBytes ||
        ::fstat(journal_fd, &before) != 0 ||
        ::fstatat(
            route_directory_fd,
            kRawJournalFilename,
            &named_before,
            AT_SYMLINK_NOFOLLOW) != 0 ||
        !S_ISREG(before.st_mode) ||
        before.st_uid != ::geteuid() ||
        (before.st_mode & 07777U) != 0600U ||
        before.st_nlink != static_cast<nlink_t>(1) ||
        before.st_size < 0 ||
        static_cast<std::uint64_t>(before.st_size) !=
            expected_size ||
        static_cast<std::uint64_t>(before.st_dev) !=
            expected_device ||
        static_cast<std::uint64_t>(before.st_ino) !=
            expected_inode ||
        !SameInode(before, named_before) ||
        flags < 0 ||
        (flags & O_ACCMODE) != O_RDONLY ||
        (flags & O_APPEND) != 0) {
        SetError(
            error,
            "terminal journal is no longer the retained safe named inode");
        return false;
    }

    RawV1JournalHeaderWire header{};
    std::size_t completed = 0U;
    while (completed < header.size()) {
        const ssize_t read = ::pread(
            journal_fd,
            header.data() +
                static_cast<std::ptrdiff_t>(completed),
            header.size() - completed,
            static_cast<off_t>(completed));
        if (read > 0) {
            completed +=
                static_cast<std::size_t>(read);
            continue;
        }
        if (read < 0 && errno == EINTR) {
            continue;
        }
        SetError(
            error,
            "terminal journal header readback failed");
        return false;
    }

    DurableJournalHeaderV1 decoded{};
    RawV1JournalHeaderWire canonical{};
    struct stat after {};
    struct stat named_after {};
    if (header != expected_header ||
        l2flow::common::ComputeSha256(header) !=
            report.journal_header_sha256 ||
        DecodeDurableJournalHeaderV1(
            header, &decoded) != RawV1Error::kNone ||
        EncodeDurableJournalHeaderV1(
            decoded, &canonical) != RawV1Error::kNone ||
        canonical != header ||
        decoded.raw_schema_sha256 !=
            kFrozenRawSchemaSha256 ||
        decoded.source_stream_id !=
            report.namespace_identity.source_stream_id ||
        decoded.capture_date !=
            report.namespace_identity.capture_date ||
        decoded.stream_day_id !=
            report.namespace_identity.stream_day_id ||
        ::fstat(journal_fd, &after) != 0 ||
        ::fstatat(
            route_directory_fd,
            kRawJournalFilename,
            &named_after,
            AT_SYMLINK_NOFOLLOW) != 0 ||
        !SameInode(before, after) ||
        !SameInode(after, named_after) ||
        before.st_mode != after.st_mode ||
        before.st_uid != after.st_uid ||
        before.st_nlink != after.st_nlink ||
        before.st_size != after.st_size) {
        SetError(
            error,
            "terminal journal evidence changed or disagrees with the report");
        return false;
    }
    return true;
}

[[nodiscard]] bool ValidateTerminalReceiptFiles(
    int route_directory_fd,
    int maintenance_directory_fd,
    int journal_fd,
    int sidecar_fd,
    int report_fd,
    RecoveryMaintenanceResultV1 result,
    const std::string& report_filename,
    const RawV1Digest& report_sha256,
    const std::string& sidecar_filename,
    const RawV1Digest& sidecar_sha256,
    const RawV1Digest& journal_header_sha256,
    const RecoveryMaintenanceReportV1&
        expected_report,
    const RawV1JournalHeaderWire& journal_header,
    std::uint64_t maintenance_device,
    std::uint64_t maintenance_inode,
    std::uint64_t journal_device,
    std::uint64_t journal_inode,
    std::uint64_t sidecar_device,
    std::uint64_t sidecar_inode,
    std::uint64_t report_device,
    std::uint64_t report_inode,
    std::string* error) noexcept {
    RecoveryMaintenanceReportV1 parsed_report{};
    if (!ValidateRecoveryReportReceiptFiles(
            route_directory_fd,
            maintenance_directory_fd,
            report_fd,
            report_filename,
            report_sha256,
            &parsed_report,
            error)) {
        return false;
    }
    struct stat maintenance_status {};
    struct stat report_status {};
    if (::fstat(
            maintenance_directory_fd,
            &maintenance_status) != 0 ||
        ::fstat(report_fd, &report_status) != 0 ||
        static_cast<std::uint64_t>(
            maintenance_status.st_dev) !=
            maintenance_device ||
        static_cast<std::uint64_t>(
            maintenance_status.st_ino) !=
            maintenance_inode ||
        static_cast<std::uint64_t>(
            report_status.st_dev) != report_device ||
        static_cast<std::uint64_t>(
            report_status.st_ino) != report_inode ||
        parsed_report.result != result ||
        parsed_report.intent !=
            RecoveryMaintenanceIntentV1::
                kRecoverSealOnly ||
        parsed_report.journal_header_sha256 !=
            journal_header_sha256) {
        SetError(
            error,
            "terminal report receipt identity or tag disagrees with retained evidence");
        return false;
    }
    try {
        std::string parsed_jcs;
        std::string expected_jcs;
        if (EncodeRecoveryMaintenanceReportV1Jcs(
                parsed_report, &parsed_jcs) !=
                RecoveryMaintenanceReportV1Error::kNone ||
            EncodeRecoveryMaintenanceReportV1Jcs(
                expected_report, &expected_jcs) !=
                RecoveryMaintenanceReportV1Error::kNone ||
            parsed_jcs != expected_jcs ||
            l2flow::common::ComputeSha256(expected_jcs) !=
                report_sha256) {
            SetError(
                error,
                "terminal report receipt frontier facts disagree with exact report bytes");
            return false;
        }

        std::string sidecar_bytes;
        struct stat sidecar_status {};
        const bool sealed =
            result ==
            RecoveryMaintenanceResultV1::kSealedRaw;
        if (!(sealed
                  ? IsSealedCertificateFilename(
                        sidecar_filename)
                  : IsEmptyAnchorTombstoneFilename(
                        sidecar_filename)) ||
            !ReadBoundedNamedEvidenceFile(
                maintenance_directory_fd,
                sidecar_fd,
                sidecar_filename,
                sealed
                    ? kSealedRawCertificateV1MaximumBytes
                    : kEmptyAnchorTombstoneV1MaximumBytes,
                sidecar_device,
                sidecar_inode,
                &sidecar_bytes,
                &sidecar_status,
                error) ||
            l2flow::common::ComputeSha256(
                sidecar_bytes) != sidecar_sha256) {
            return false;
        }

        if (sealed) {
            SealedRawCertificateV1 certificate{};
            std::string canonical;
            std::string filename;
            if (parsed_report
                        .current_sealed_raw_certificate_sha256 !=
                    sidecar_sha256 ||
                ParseSealedRawCertificateV1Jcs(
                    sidecar_bytes, &certificate) !=
                    SealedRawCertificateV1Error::kNone ||
                EncodeSealedRawCertificateV1Jcs(
                    certificate, &canonical) !=
                    SealedRawCertificateV1Error::kNone ||
                canonical != sidecar_bytes ||
                SealedRawCertificateV1Filename(
                    certificate, &filename) !=
                    SealedRawCertificateV1Error::kNone ||
                filename != sidecar_filename ||
                certificate.namespace_identity
                        .source_stream_id !=
                    parsed_report.namespace_identity
                        .source_stream_id ||
                certificate.namespace_identity
                        .capture_date !=
                    parsed_report.namespace_identity
                        .capture_date ||
                certificate.namespace_identity
                        .stream_day_id !=
                    parsed_report.namespace_identity
                        .stream_day_id ||
                certificate.journal_header_sha256 !=
                    journal_header_sha256 ||
                certificate.closed_entry_count !=
                    parsed_report.closed_frontier
                        .closed_entry_count ||
                certificate.closed_prefix_sha256 !=
                    parsed_report.closed_frontier
                        .closed_prefix_sha256 ||
                certificate
                        .accepted_sealed_marker_bytes !=
                    parsed_report.sealed_boundary
                        .accepted_sealed_marker_bytes ||
                certificate
                        .accepted_sealed_marker_sha256 !=
                    parsed_report.sealed_boundary
                        .accepted_sealed_marker_sha256 ||
                certificate.last_segment_sequence !=
                    parsed_report.sealed_boundary
                        .last_segment_sequence ||
                certificate.last_segment_flags !=
                    parsed_report.sealed_boundary
                        .last_segment_flags ||
                certificate
                        .last_segment_base_wal_pos !=
                    parsed_report.sealed_boundary
                        .last_segment_base_wal_pos ||
                certificate
                        .last_segment_logical_length !=
                    parsed_report.sealed_boundary
                        .last_segment_logical_length ||
                certificate.last_segment_sha256 !=
                    parsed_report.sealed_boundary
                        .last_segment_sha256 ||
                certificate.terminal_durable_cursor
                        .segment_sequence !=
                    parsed_report.final_durable_cursor
                        .segment_sequence ||
                certificate.terminal_durable_cursor
                        .global_wal_pos !=
                    parsed_report.final_durable_cursor
                        .global_wal_pos ||
                certificate.terminal_durable_cursor
                        .ingress_sequence !=
                    parsed_report.final_durable_cursor
                        .ingress_sequence ||
                certificate.terminal_durable_cursor
                        .segment_offset !=
                    parsed_report.final_durable_cursor
                        .segment_offset) {
                SetError(
                    error,
                    "sealed terminal sidecar disagrees with the exact report frontier");
                return false;
            }
        } else {
            EmptyAnchorTombstoneV1 tombstone{};
            std::string canonical;
            std::string filename;
            if (result !=
                    RecoveryMaintenanceResultV1::
                        kEmptyAnchorOnly ||
                parsed_report
                        .current_empty_anchor_tombstone_sha256 !=
                    sidecar_sha256 ||
                ParseEmptyAnchorTombstoneV1Jcs(
                    sidecar_bytes, &tombstone) !=
                    EmptyAnchorTombstoneV1Error::kNone ||
                EncodeEmptyAnchorTombstoneV1Jcs(
                    tombstone, &canonical) !=
                    EmptyAnchorTombstoneV1Error::kNone ||
                canonical != sidecar_bytes ||
                EmptyAnchorTombstoneV1Filename(
                    tombstone, &filename) !=
                    EmptyAnchorTombstoneV1Error::kNone ||
                filename != sidecar_filename ||
                tombstone.namespace_identity
                        .source_stream_id !=
                    parsed_report.namespace_identity
                        .source_stream_id ||
                tombstone.namespace_identity
                        .capture_date !=
                    parsed_report.namespace_identity
                        .capture_date ||
                tombstone.namespace_identity
                        .stream_day_id !=
                    parsed_report.namespace_identity
                        .stream_day_id ||
                tombstone.journal_header_sha256 !=
                    journal_header_sha256 ||
                tombstone.marker_count != 0U ||
                tombstone.record_count != 0U ||
                tombstone.segment_count != 0U) {
                SetError(
                    error,
                    "empty-anchor terminal sidecar disagrees with the exact report");
                return false;
            }
        }
        return ValidateTerminalJournalEvidence(
            route_directory_fd,
            journal_fd,
            parsed_report,
            journal_header,
            journal_device,
            journal_inode,
            error);
    } catch (...) {
        SetError(
            error,
            "cannot validate terminal report receipt evidence");
        return false;
    }
}

[[nodiscard]] bool OpenExistingTarget(
    int retained_raw_root_fd,
    const RawReserveRegistryEntryKeyV1& key,
    std::string_view stream_slug,
    OpenedTarget* target,
    std::string* error) noexcept {
    if (target == nullptr ||
        key.route.source_stream_id == 0U ||
        !IsGregorianDate(key.route.capture_date) ||
        !IsCanonicalRawStreamRouteV1(
            key.route.source_stream_id,
            stream_slug)) {
        errno = EINVAL;
        SetError(
            error,
            "existing Raw mutation target is not canonical");
        return false;
    }

    try {
        const std::string date_name =
            "capture_date=" +
            std::to_string(key.route.capture_date);
        const std::string route_name =
            "stream=" +
            std::to_string(
                key.route.source_stream_id) +
            "-" + std::string(stream_slug);
        struct stat root_status {};
        if (!PrivateOwnedDirectory(
                retained_raw_root_fd,
                &root_status)) {
            errno = ESTALE;
            SetError(
                error,
                "retained Raw root identity is no longer safe");
            return false;
        }
        ScopedFd date(OpenDirectoryAt(
            retained_raw_root_fd,
            date_name.c_str()));
        struct stat date_status {};
        if (date.get() < 0 ||
            !PrivateOwnedDirectory(
                date.get(), &date_status) ||
            date_status.st_dev != root_status.st_dev ||
            !SameNamedDirectory(
                retained_raw_root_fd,
                date_name.c_str(),
                date_status)) {
            errno = ESTALE;
            SetError(
                error,
                "canonical Raw capture-date target is unavailable");
            return false;
        }
        ScopedFd route(OpenDirectoryAt(
            date.get(), route_name.c_str()));
        struct stat route_status {};
        if (route.get() < 0 ||
            !PrivateOwnedDirectory(
                route.get(), &route_status) ||
            route_status.st_dev != root_status.st_dev ||
            !SameNamedDirectory(
                date.get(),
                route_name.c_str(),
                route_status)) {
            errno = ESTALE;
            SetError(
                error,
                "canonical Raw stream target is unavailable");
            return false;
        }
        target->root_status = root_status;
        target->route_status = route_status;
        target->route = std::move(route);
        return true;
    } catch (...) {
        errno = ENOMEM;
        SetError(
            error,
            "cannot allocate canonical Raw target name");
        return false;
    }
}

struct ContinuationReplacementCandidate final {
    ScopedFd descriptor;
    std::string name;
    struct stat status {};
};

[[nodiscard]] bool ParseEightDigitSequence(
    std::string_view text,
    std::uint32_t* sequence) noexcept {
    if (sequence == nullptr || text.size() != 8U) {
        return false;
    }
    std::uint32_t value = 0U;
    for (const char character : text) {
        if (character < '0' || character > '9') {
            return false;
        }
        value =
            value * 10U +
            static_cast<std::uint32_t>(
                character - '0');
    }
    *sequence = value;
    return true;
}

[[nodiscard]] bool ParseRawSegmentFinalName(
    std::string_view name,
    std::uint32_t* sequence) noexcept {
    constexpr std::string_view prefix = "segment-";
    constexpr std::string_view suffix = ".raw";
    return name.size() ==
               prefix.size() + 8U + suffix.size() &&
           name.substr(0U, prefix.size()) == prefix &&
           name.substr(name.size() - suffix.size()) ==
               suffix &&
           ParseEightDigitSequence(
               name.substr(prefix.size(), 8U),
               sequence);
}

[[nodiscard]] bool ParseRawSegmentTemporaryName(
    std::string_view name,
    std::uint32_t* sequence) noexcept {
    constexpr std::string_view prefix = ".segment-";
    constexpr std::string_view suffix =
        ".raw.raw-segment.tmp";
    return name.size() ==
               prefix.size() + 8U + suffix.size() &&
           name.substr(0U, prefix.size()) == prefix &&
           name.substr(name.size() - suffix.size()) ==
               suffix &&
           ParseEightDigitSequence(
               name.substr(prefix.size(), 8U),
               sequence);
}

[[nodiscard]] bool MakeContinuationCandidateNames(
    std::uint32_t sequence,
    std::string* final_name,
    std::string* temporary_name) noexcept {
    if (final_name == nullptr ||
        temporary_name == nullptr ||
        sequence == 0U ||
        sequence >
            kRawSegmentArtifactMaximumSequenceV1) {
        return false;
    }
    try {
        std::string digits = std::to_string(sequence);
        digits.insert(
            0U, 8U - digits.size(), '0');
        *final_name =
            "segment-" + digits + ".raw";
        *temporary_name =
            "." + *final_name +
            ".raw-segment.tmp";
        return true;
    } catch (...) {
        return false;
    }
}

[[nodiscard]] bool OpenSoleContinuationReplacementCandidate(
    int route_directory_fd,
    std::uint32_t expected_sequence,
    ContinuationReplacementCandidate* candidate,
    std::string* error) noexcept {
    if (route_directory_fd < 0 ||
        candidate == nullptr) {
        SetError(
            error,
            "replacement continuation candidate scan is invalid");
        return false;
    }
    try {
        std::string final_name;
        std::string temporary_name;
        if (!MakeContinuationCandidateNames(
                expected_sequence,
                &final_name,
                &temporary_name)) {
            SetError(
                error,
                "replacement continuation sequence cannot form deterministic names");
            return false;
        }

        const int duplicate =
            ::fcntl(
                route_directory_fd,
                F_DUPFD_CLOEXEC,
                0);
        if (duplicate < 0) {
            SetError(
                error,
                "cannot duplicate continuation route for secure enumeration");
            return false;
        }
        DIR* const directory = ::fdopendir(duplicate);
        if (directory == nullptr) {
            static_cast<void>(::close(duplicate));
            SetError(
                error,
                "cannot enumerate continuation route");
            return false;
        }

        std::string selected_name;
        std::size_t selected_count = 0U;
        bool conflict = false;
        errno = 0;
        for (;;) {
            const dirent* const entry =
                ::readdir(directory);
            if (entry == nullptr) {
                break;
            }
            const std::string_view name(
                entry->d_name);
            if (name == "." || name == "..") {
                continue;
            }
            std::uint32_t sequence = 0U;
            if (ParseRawSegmentFinalName(
                    name, &sequence)) {
                if (sequence == 0U ||
                    sequence > expected_sequence) {
                    conflict = true;
                    continue;
                }
                if (sequence == expected_sequence) {
                    ++selected_count;
                    selected_name =
                        std::string(name);
                }
                continue;
            }
            if (name.starts_with("segment-") &&
                name.ends_with(".raw")) {
                conflict = true;
                continue;
            }
            if (ParseRawSegmentTemporaryName(
                    name, &sequence)) {
                if (sequence == 0U ||
                    sequence != expected_sequence) {
                    conflict = true;
                    continue;
                }
                ++selected_count;
                selected_name = std::string(name);
                continue;
            }
            if (name.starts_with(".segment-") &&
                name.ends_with(
                    ".raw.raw-segment.tmp")) {
                conflict = true;
            }
        }
        const int read_error = errno;
        static_cast<void>(::closedir(directory));
        if (read_error != 0 || conflict ||
            selected_count != 1U ||
            (selected_name != final_name &&
             selected_name != temporary_name)) {
            SetError(
                error,
                "replacement requires exactly one deterministic continuation final-or-temporary candidate and no competing sequence");
            return false;
        }

        ScopedFd descriptor;
        for (;;) {
            descriptor = ScopedFd(::openat(
                route_directory_fd,
                selected_name.c_str(),
                O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
                    O_CLOEXEC));
            if (descriptor.get() >= 0 ||
                errno != EINTR) {
                break;
            }
        }
        struct stat route {};
        struct stat opened {};
        struct stat named {};
        if (descriptor.get() < 0 ||
            ::fstat(route_directory_fd, &route) != 0 ||
            ::fstat(descriptor.get(), &opened) != 0 ||
            ::fstatat(
                route_directory_fd,
                selected_name.c_str(),
                &named,
                AT_SYMLINK_NOFOLLOW) != 0 ||
            !S_ISREG(opened.st_mode) ||
            opened.st_uid != ::geteuid() ||
            (opened.st_mode & 07777U) != 0600U ||
            opened.st_nlink !=
                static_cast<nlink_t>(1) ||
            opened.st_dev != route.st_dev ||
            !SameInode(opened, named)) {
            SetError(
                error,
                "replacement continuation candidate is not one safe named route-local inode");
            return false;
        }
        candidate->descriptor =
            std::move(descriptor);
        candidate->name =
            std::move(selected_name);
        candidate->status = opened;
        return true;
    } catch (...) {
        SetError(
            error,
            "cannot allocate replacement continuation candidate scan");
        return false;
    }
}

[[nodiscard]] bool ContinuationReplacementCandidateStillNamed(
    int route_directory_fd,
    const ContinuationReplacementCandidate& candidate,
    std::string* error) noexcept {
    struct stat opened {};
    struct stat named {};
    if (candidate.descriptor.get() < 0 ||
        ::fstat(
            candidate.descriptor.get(),
            &opened) != 0 ||
        ::fstatat(
            route_directory_fd,
            candidate.name.c_str(),
            &named,
            AT_SYMLINK_NOFOLLOW) != 0 ||
        !SameInode(opened, candidate.status) ||
        !SameInode(opened, named) ||
        opened.st_mode != candidate.status.st_mode ||
        opened.st_uid != candidate.status.st_uid ||
        opened.st_nlink != candidate.status.st_nlink ||
        opened.st_size != candidate.status.st_size) {
        SetError(
            error,
            "replacement continuation candidate changed before state publication");
        return false;
    }
    return true;
}

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
    RawReserveCoordinatorErrorV1* failure,
    RawReserveCoordinatorErrorV1 value) noexcept {
    if (failure != nullptr) {
        *failure = value;
    }
}

bool MarkerMatchesState(
    const RawReserveCoordinatorLeaseMarkerV1& marker,
    const ReserveCoordinatorStateV1& state) noexcept {
    return marker.device_id == state.header.device_id &&
           marker.quota_identity_sha256 ==
               state.header.quota_identity_sha256 &&
           marker.mount_identity_sha256 ==
               state.header.mount_identity_sha256;
}

bool EntryMatchesKey(
    const ReserveStateEntryV1& entry,
    const RawReserveRegistryEntryKeyV1& key) noexcept {
    return entry.source_stream_id ==
               key.route.source_stream_id &&
           entry.capture_date ==
               key.route.capture_date &&
           entry.stream_day_id == key.stream_day_id &&
           entry.executor_or_recovery_attempt ==
               key.recovery_attempt_id;
}

bool GrantMatchesKey(
    const ReserveStateSlotV1& slot,
    const ReserveStateEntryV1& entry,
    const ReserveFinalizationGrantKeyV1&
        key) noexcept {
    return slot.finalization_cycle_id ==
               key.finalization_cycle_id &&
           entry.source_stream_id ==
               key.source_stream_id &&
           entry.capture_date ==
               key.capture_date &&
           entry.stream_day_id ==
               key.stream_day_id &&
           entry.ack_status ==
               key.ack_status &&
           entry.writer_instance ==
               key.ack_writer_instance &&
           entry.safe_stop_template_id ==
               key.safe_stop_template_id;
}

RawReserveGenerationActionTokenV1 MakeToken(
    const ReserveStateSlotV1& slot,
    const ReserveStateEntryV1& entry) noexcept {
    RawReserveGenerationActionTokenV1 token;
    token.reserve_state_uuid =
        slot.reserve_state_uuid;
    token.state_generation = slot.generation;
    token.writer_instance_id =
        entry.writer_instance;
    token.recovery_attempt_id =
        entry.executor_or_recovery_attempt;
    token.finalization_cycle_id =
        slot.finalization_cycle_id;
    return token;
}

[[nodiscard]] bool CheckedAddU64(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        left >
            std::numeric_limits<std::uint64_t>::max() -
                right) {
        return false;
    }
    *output = left + right;
    return true;
}

[[nodiscard]] bool CheckedMulU64(
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

[[nodiscard]] bool DropWithinCap(
    std::uint64_t baseline,
    std::uint64_t observed,
    std::uint64_t cap) noexcept {
    // A higher free-space observation never replenishes the durable ledger;
    // it simply contributes a zero net drop to this terminal check.
    return observed >= baseline ||
           baseline - observed <= cap;
}

[[nodiscard]] bool ReadExactReportBytes(
    int descriptor,
    std::size_t byte_count,
    std::uint64_t expected_device,
    std::uint64_t expected_inode,
    std::uint64_t expected_blocks,
    const RawV1Digest& expected_sha256,
    std::string* bytes,
    std::string* error) noexcept {
    if (descriptor < 0 || bytes == nullptr ||
        byte_count == 0U ||
        byte_count >
            kFinalizationReportV1MaximumBytes) {
        SetError(
            error,
            "finalization report receipt size or descriptor is invalid");
        return false;
    }
    try {
        struct stat before {};
        if (::fstat(descriptor, &before) != 0 ||
            !S_ISREG(before.st_mode) ||
            before.st_uid != ::geteuid() ||
            (before.st_mode & 07777U) != 0600U ||
            before.st_nlink != static_cast<nlink_t>(1) ||
            before.st_size !=
                static_cast<off_t>(byte_count) ||
            static_cast<std::uint64_t>(before.st_dev) !=
                expected_device ||
            static_cast<std::uint64_t>(before.st_ino) !=
                expected_inode ||
            static_cast<std::uint64_t>(before.st_blocks) !=
                expected_blocks) {
            SetError(
                error,
                "finalization report retained inode changed");
            return false;
        }
        std::string exact(byte_count, '\0');
        std::size_t completed = 0U;
        while (completed < exact.size()) {
            const ssize_t result = ::pread(
                descriptor,
                exact.data() +
                    static_cast<std::ptrdiff_t>(completed),
                exact.size() - completed,
                static_cast<off_t>(completed));
            if (result > 0) {
                completed +=
                    static_cast<std::size_t>(result);
                continue;
            }
            if (result < 0 && errno == EINTR) {
                continue;
            }
            SetError(
                error,
                "finalization report retained-fd readback failed");
            return false;
        }
        struct stat after {};
        if (::fstat(descriptor, &after) != 0 ||
            before.st_dev != after.st_dev ||
            before.st_ino != after.st_ino ||
            before.st_mode != after.st_mode ||
            before.st_uid != after.st_uid ||
            before.st_nlink != after.st_nlink ||
            before.st_size != after.st_size ||
            before.st_blocks != after.st_blocks ||
            l2flow::common::ComputeSha256(
                std::string_view(exact)) !=
                expected_sha256) {
            SetError(
                error,
                "finalization report changed during retained-fd readback");
            return false;
        }
        bytes->swap(exact);
        return true;
    } catch (...) {
        SetError(
            error,
            "cannot allocate finalization report readback");
        return false;
    }
}

[[nodiscard]] bool SameNamespace(
    const RawManifestNamespaceV1& value,
    const ReserveStateEntryV1& entry) noexcept {
    return value.source_stream_id ==
               entry.source_stream_id &&
           value.capture_date == entry.capture_date &&
           value.stream_day_id == entry.stream_day_id;
}

[[nodiscard]] bool HasCompletedPreexistingReport(
    const ReserveStateEntryV1& entry) noexcept {
    return std::any_of(
        entry.actions.begin(),
        entry.actions.end(),
        [](const FinalizationActionReceiptV1&
               action) noexcept {
            return action.action_kind ==
                       FinalizationActionKindV1::
                           kPreexistingRecoveryReport &&
                   action.action_state ==
                       FinalizationActionStateV1::
                           kComplete;
        });
}

[[nodiscard]] bool ValidateRoutedReportModel(
    const FinalizationReportV1& report,
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& slot,
    const ReserveStateEntryV1& entry,
    const ReserveStateV1Digest&
        immutable_grant_sha256) noexcept {
    if (ValidateFinalizationReportV1(report) !=
            FinalizationReportV1Error::kNone ||
        report.reserve_state_uuid !=
            header.reserve_state_uuid ||
        report.finalization_cycle_id !=
            slot.finalization_cycle_id ||
        !SameNamespace(
            report.namespace_identity, entry) ||
        report.ack_status != entry.ack_status ||
        report.ack_writer_instance !=
            entry.writer_instance ||
        report.immutable_grant_sha256 !=
            immutable_grant_sha256 ||
        report.preexisting_recovery_report.has_value() !=
            HasCompletedPreexistingReport(entry)) {
        return false;
    }
    if (entry.grant_flags ==
        kReserveGrantRawAnchorOnly) {
        return report.result ==
                   FinalizationReportResultV1::
                       kEmptyAnchorOnly &&
               report.journal_header_sha256 ==
                   entry.anchor_only_payload
                       .journal_header_sha256;
    }
    if (entry.grant_flags !=
            kReserveGrantRawFinalization ||
        report.result !=
            FinalizationReportResultV1::kSealedRaw) {
        return false;
    }
    if (entry.ack_status != ReserveAckStatusV1::kAcked) {
        return true;
    }
    if (!report.initial_append_cursor.has_value() ||
        !report.initial_durable_cursor.has_value() ||
        !report.final_append_cursor.has_value() ||
        !report.final_durable_cursor.has_value() ||
        report.gap_classification !=
            FinalizationReportGapClassificationV1::
                kAckedRingDrained ||
        report.initial_append_cursor->global_wal_pos !=
            entry.raw_counters.append_global_wal_pos ||
        report.initial_append_cursor->ingress_sequence !=
            entry.raw_counters.append_ingress_sequence ||
        report.initial_durable_cursor->global_wal_pos !=
            entry.raw_counters.durable_global_wal_pos ||
        report.initial_durable_cursor->ingress_sequence !=
            entry.raw_counters.durable_ingress_sequence) {
        return false;
    }

    std::uint64_t expected_final_ingress = 0U;
    std::uint64_t expected_final_wal = 0U;
    std::uint64_t continuation_header_bytes =
        report.segments.size() == 2U
            ? static_cast<std::uint64_t>(
                  kRawV1SegmentHeaderBytes)
            : 0U;
    return CheckedAddU64(
               entry.raw_counters.append_ingress_sequence,
               entry.raw_counters.queued_record_count,
               &expected_final_ingress) &&
           CheckedAddU64(
               entry.raw_counters.append_global_wal_pos,
               entry.raw_counters.queued_framed_wal_bytes,
               &expected_final_wal) &&
           CheckedAddU64(
               expected_final_wal,
               continuation_header_bytes,
               &expected_final_wal) &&
           report.final_append_cursor->ingress_sequence ==
               expected_final_ingress &&
           report.final_append_cursor->global_wal_pos ==
               expected_final_wal &&
           *report.final_append_cursor ==
               *report.final_durable_cursor;
}

[[nodiscard]] bool ValidateScaffoldingReportModel(
    const ScaffoldingFinalizationReportV1& report,
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& slot,
    const ReserveStateEntryV1& entry,
    const ReserveStateV1Digest&
        immutable_grant_sha256) noexcept {
    return ValidateScaffoldingFinalizationReportV1(
               report) ==
               ScaffoldingFinalizationReportV1Error::
                   kNone &&
           entry.grant_flags ==
               kReserveGrantScaffoldingOnly &&
           report.reserve_state_uuid ==
               header.reserve_state_uuid &&
           report.finalization_cycle_id ==
               slot.finalization_cycle_id &&
           SameNamespace(
               report.planned_namespace, entry) &&
           report.planned_recovery_attempt_id ==
               entry.scaffolding_payload
                   .recovery_attempt_id &&
           report.immutable_grant_sha256 ==
               immutable_grant_sha256 &&
           report.object_snapshot_sha256 ==
               entry.scaffolding_payload
                   .object_snapshot_sha256 &&
           report.observed_object_bitmap ==
               entry.scaffolding_payload
                   .observed_object_bitmap &&
           report.required_action_bitmap ==
               entry.scaffolding_payload
                   .required_action_bitmap;
}

[[nodiscard]] bool ValidateFinalizationCapacity(
    int retained_raw_root_fd,
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& slot,
    const ReserveStateEntryV1& entry,
    RawEmergencyReserveCapacityProbeV1* probe,
    std::string* error) noexcept {
    if (probe == nullptr ||
        retained_raw_root_fd < 0 ||
        entry.precharged_bytes != entry.grant_bytes) {
        SetError(
            error,
            "finalization post-publication capacity probe is unavailable");
        return false;
    }
    std::uint64_t inode_cap = 0U;
    for (const FinalizationActionReceiptV1&
             action : entry.actions) {
        if (action.action_kind ==
            FinalizationActionKindV1::kUnused) {
            break;
        }
        if (!CheckedAddU64(
                inode_cap,
                static_cast<std::uint64_t>(
                    action.inode_cap),
                &inode_cap)) {
            SetError(
                error,
                "finalization inode cap sum overflows");
            return false;
        }
    }

    RawEmergencyReserveCapacityProbeRequestV1 request{};
    request.stage =
        RawEmergencyReserveProbeStageV1::
            kFinalizationPostPublish;
    request.pool.reserve_state_uuid =
        header.reserve_state_uuid;
    request.pool.device_id = header.device_id;
    request.pool.quota_identity_sha256 =
        header.quota_identity_sha256;
    request.pool.mount_identity_sha256 =
        header.mount_identity_sha256;
    request.byte_probe_version =
        header.byte_probe_version;
    request.inode_probe_version =
        header.inode_probe_version;
    request.required_bytes = entry.precharged_bytes;
    request.required_inodes = inode_cap;
    RawEmergencyReserveCapacityObservationV1
        observation{};
    if (!probe->Observe(
            retained_raw_root_fd,
            request,
            &observation,
            error) ||
        observation.pool != request.pool ||
        observation.byte_probe_version !=
            request.byte_probe_version ||
        observation.inode_probe_version !=
            request.inode_probe_version ||
        !observation.filesystem_bytes_proven ||
        !observation.quota_bytes_proven ||
        !observation.filesystem_inodes_proven ||
        !observation.quota_inodes_proven ||
        !DropWithinCap(
            entry.activation_fs_free_baseline,
            observation.filesystem_free_bytes,
            entry.precharged_bytes) ||
        !DropWithinCap(
            entry.activation_quota_free_baseline,
            observation.quota_free_bytes,
            entry.precharged_bytes) ||
        !DropWithinCap(
            slot.active_fs_free_inode_baseline,
            observation.filesystem_free_inodes,
            inode_cap) ||
        !DropWithinCap(
            slot.active_quota_free_inode_baseline,
            observation.quota_free_inodes,
            inode_cap)) {
        SetError(
            error,
            "finalization post-publication allocation exceeds the immutable grant");
        return false;
    }
    return true;
}

}  // namespace

std::string_view RawReserveCoordinatorErrorNameV1(
    RawReserveCoordinatorErrorV1 error) noexcept {
    switch (error) {
        case RawReserveCoordinatorErrorV1::kNone:
            return "none";
        case RawReserveCoordinatorErrorV1::kInvalidArgument:
            return "invalid_argument";
        case RawReserveCoordinatorErrorV1::kLeaseFailure:
            return "lease_failure";
        case RawReserveCoordinatorErrorV1::kStateFailure:
            return "state_failure";
        case RawReserveCoordinatorErrorV1::kIdentityMismatch:
            return "identity_mismatch";
        case RawReserveCoordinatorErrorV1::
            kTransitionGateFailure:
            return "transition_gate_failure";
        case RawReserveCoordinatorErrorV1::
            kCodecTransitionRejected:
            return "codec_transition_rejected";
        case RawReserveCoordinatorErrorV1::
            kStatePublishFailure:
            return "state_publish_failure";
        case RawReserveCoordinatorErrorV1::kRouteNotFound:
            return "route_not_found";
        case RawReserveCoordinatorErrorV1::
            kRouteIdentityMismatch:
            return "route_identity_mismatch";
        case RawReserveCoordinatorErrorV1::
            kRouteStatusMismatch:
            return "route_status_mismatch";
        case RawReserveCoordinatorErrorV1::
            kActionGateFailure:
            return "action_gate_failure";
        case RawReserveCoordinatorErrorV1::
            kActionGenerationChanged:
            return "action_generation_changed";
        case RawReserveCoordinatorErrorV1::
            kTargetUnavailable:
            return "target_unavailable";
        case RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged:
            return "target_identity_changed";
        case RawReserveCoordinatorErrorV1::
            kReserveInventoryFailure:
            return "reserve_inventory_failure";
        case RawReserveCoordinatorErrorV1::
            kReserveReleaseFailure:
            return "reserve_release_failure";
        case RawReserveCoordinatorErrorV1::
            kRootSyncFailure:
            return "root_sync_failure";
        case RawReserveCoordinatorErrorV1::
            kCapacityProbeFailure:
            return "capacity_probe_failure";
        case RawReserveCoordinatorErrorV1::
            kAllocationFailure:
            return "allocation_failure";
    }
    return "unknown";
}

RawReserveMutationTargetAnchorV1::
RawReserveMutationTargetAnchorV1(
    std::uint64_t raw_root_device,
    std::uint64_t raw_root_inode,
    std::uint64_t route_device,
    std::uint64_t route_inode,
    RawReserveCoordinatorLeaseDigestV1
        mount_identity_sha256,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    std::string stream_slug) noexcept
    : raw_root_device_(raw_root_device),
      raw_root_inode_(raw_root_inode),
      route_device_(route_device),
      route_inode_(route_inode),
      mount_identity_sha256_(
          std::move(mount_identity_sha256)),
      source_stream_id_(source_stream_id),
      capture_date_(capture_date),
      stream_slug_(std::move(stream_slug)) {}

bool ValidateRawReserveMutationTargetProviderV1(
    const RawReserveMutationTargetProviderV1& provider,
    const RawReserveMutationTargetAnchorV1&
        target) noexcept {
    struct stat status {};
    return PrivateOwnedDirectory(
               provider
                   .RawReserveMutationTargetDirectoryDescriptorV1(),
               &status) &&
           static_cast<std::uint64_t>(status.st_dev) ==
               target.route_device() &&
           static_cast<std::uint64_t>(status.st_ino) ==
               target.route_inode();
}

RawReserveAuthorizedActionV1::
RawReserveAuthorizedActionV1(
    std::shared_ptr<
        RawReserveRegistryCoordinatorV1> coordinator,
    std::unique_ptr<
        RawReserveGenerationActionGateV1> gate,
    RawReserveRegistryEntryKeyV1 key,
    ReserveRegistryStatusV1 required_status,
    ReserveRecoveryIntentV1 recovery_intent,
    std::size_t entry_index,
    std::optional<
        RawReserveMutationTargetAnchorV1> target,
    int route_directory_fd) noexcept
    : coordinator_(coordinator),
      gate_(std::move(gate)),
      key_(std::move(key)),
      required_status_(required_status),
      recovery_intent_(recovery_intent),
      entry_index_(entry_index),
      target_(std::move(target)),
      route_directory_fd_(route_directory_fd) {}

RawReserveAuthorizedActionV1::
~RawReserveAuthorizedActionV1() {
    if (route_directory_fd_ >= 0) {
        static_cast<void>(
            ::close(route_directory_fd_));
    }
}

bool RawReserveAuthorizedActionV1::ValidateLatest(
    std::string* error) const noexcept {
    return coordinator_ != nullptr &&
           gate_ != nullptr &&
           coordinator_->ValidateActionLatest(
               *this, error);
}

bool RawReserveAuthorizedActionV1::Authorizes(
    const RawFreshStateAuthorizationV1& facts)
    const noexcept {
    const RawFreshRegistryStageV1 expected_stage =
        required_status_ ==
                ReserveRegistryStatusV1::kScaffolding
            ? RawFreshRegistryStageV1::kScaffolding
            : RawFreshRegistryStageV1::kInit;
    if ((required_status_ !=
             ReserveRegistryStatusV1::kScaffolding &&
         required_status_ !=
             ReserveRegistryStatusV1::kInit) ||
        facts.source_stream_id !=
            key_.route.source_stream_id ||
        facts.capture_date !=
            key_.route.capture_date ||
        facts.stream_day_id != key_.stream_day_id ||
        facts.recovery_attempt !=
            key_.recovery_attempt_id ||
        facts.registry_stage != expected_stage ||
        facts.durable_state_generation !=
            gate_->token().state_generation) {
        return false;
    }
    return ValidateLatest(nullptr);
}

RawReserveFinalizationActionV1::
RawReserveFinalizationActionV1(
    std::shared_ptr<
        RawReserveRegistryCoordinatorV1> coordinator,
    std::unique_ptr<
        RawReserveGenerationActionGateV1> gate,
    ReserveFinalizationActionKeyV1 key,
    ReserveStateV1Digest immutable_grant_sha256,
    FinalizationActionPlanV1 plan,
    std::uint8_t grant_flags,
    std::uint64_t byte_cap,
    std::uint32_t inode_cap,
    std::uint64_t debit_generation,
    ReserveStateV1Identity executor_instance,
    std::size_t entry_index,
    std::optional<
        RawReserveMutationTargetAnchorV1> target,
    int raw_root_directory_fd,
    int route_directory_fd) noexcept
    : coordinator_(std::move(coordinator)),
      gate_(std::move(gate)),
      key_(std::move(key)),
      immutable_grant_sha256_(
          immutable_grant_sha256),
      plan_(plan),
      grant_flags_(grant_flags),
      byte_cap_(byte_cap),
      inode_cap_(inode_cap),
      debit_generation_(debit_generation),
      executor_instance_(executor_instance),
      entry_index_(entry_index),
      target_(std::move(target)),
      raw_root_directory_fd_(raw_root_directory_fd),
      route_directory_fd_(route_directory_fd) {}

RawReserveFinalizationActionV1::
~RawReserveFinalizationActionV1() {
    if (route_directory_fd_ >= 0) {
        static_cast<void>(
            ::close(route_directory_fd_));
    }
    if (raw_root_directory_fd_ >= 0) {
        static_cast<void>(
            ::close(raw_root_directory_fd_));
    }
}

bool RawReserveFinalizationActionV1::
ValidateLatest(
    std::string* error) const noexcept {
    return coordinator_ != nullptr &&
           gate_ != nullptr &&
           coordinator_
               ->ValidateFinalizationActionLatest(
                   *this, error);
}

RawReserveRegistryCoordinatorV1::
RawReserveRegistryCoordinatorV1(
    std::unique_ptr<
        RawReserveCoordinatorLeaseV1> lease,
    std::unique_ptr<RawReserveStateFileV1>
        state_file) noexcept
    : lease_(std::move(lease)),
      state_file_(std::move(state_file)) {}

RawReserveRegistryCoordinatorV1::
~RawReserveRegistryCoordinatorV1() = default;

bool RawReserveRegistryCoordinatorV1::SyncRawRoot(
    std::string* error) const noexcept {
    const int descriptor =
        lease_ == nullptr
            ? -1
            : lease_->root_directory_descriptor();
    if (descriptor < 0) {
        SetError(
            error,
            "retained Raw root is unavailable");
        return false;
    }
    for (;;) {
        if (::fsync(descriptor) == 0) {
            return true;
        }
        if (errno == EINTR) {
            continue;
        }
        SetError(
            error,
            std::string("cannot fsync retained Raw root: ") +
                std::strerror(errno));
        return false;
    }
}

ReserveCoordinatorStateV1
RawReserveRegistryCoordinatorV1::state() const {
    std::lock_guard<std::mutex> state_lock(
        state_mutex_);
    return state_file_->state();
}

std::shared_ptr<RawReserveRegistryCoordinatorV1>
RawReserveRegistryCoordinatorV1::Retain() noexcept {
    try {
        return shared_from_this();
    } catch (...) {
        return nullptr;
    }
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
RegisterFreshScaffolding(
    const RawReserveFreshScaffoldingV1& request,
    std::string* error) noexcept {
    return Transition<
        RawReserveFreshScaffoldingV1,
        BuildRawReserveRegisterFreshScaffoldingV1>(
        request, error);
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::PublishInit(
    const RawReserveRegistryEntryKeyV1& key,
    std::string* error) noexcept {
    return Transition<
        RawReserveRegistryEntryKeyV1,
        BuildRawReservePublishInitV1>(
        key, error);
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
PublishReleasingIntent(
    const ReserveReleaseIntentV1& request,
    std::string* error) noexcept {
    return EmergencyTransition<
        ReserveReleaseIntentV1,
        BuildReserveReleasingIntentV1>(
        request, error);
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
PublishReleasingPrepared(
    const ReserveReleasePreparedV1& request,
    std::string* error) noexcept {
    return EmergencyTransition<
        ReserveReleasePreparedV1,
        BuildReserveReleasingPreparedV1>(
        request, error);
}

RawReserveRegistryCoordinatorV1::
PreparedReleaseResultV1
RawReserveRegistryCoordinatorV1::
ReleasePreparedAndPublishConsumed(
    RawEmergencyReserveCapacityProbeV1*
        capacity_probe,
    const RawEmergencyReserveMutationHooksV1*
        hooks,
    std::string* error) noexcept {
    PreparedReleaseResultV1 result{};
    SetError(error, {});
    if (capacity_probe == nullptr) {
        result.coordinator_error =
            RawReserveCoordinatorErrorV1::
                kInvalidArgument;
        SetError(
            error,
            "prepared reserve release requires a capacity probe");
        return result;
    }

    RawReserveCoordinatorGateError gate_failure =
        RawReserveCoordinatorGateError::kNone;
    auto transition_guard =
        AcquireRawReserveCoordinatorTransitionGuardAtV1(
            lease_->root_directory_descriptor(),
            lease_->anchor(),
            &gate_failure,
            error);
    if (transition_guard == nullptr) {
        result.coordinator_error =
            RawReserveCoordinatorErrorV1::
                kTransitionGateFailure;
        return result;
    }

    std::lock_guard<std::mutex> state_lock(
        state_mutex_);
    if (state_file_->Reload(
            &result.codec_error, error) !=
        RawReserveStatePosixError::kNone) {
        result.coordinator_error =
            RawReserveCoordinatorErrorV1::
                kStateFailure;
        return result;
    }
    const ReserveCoordinatorStateV1& state =
        state_file_->state();
    if (state.selected_slot >= state.slots.size() ||
        state.slots[state.selected_slot]
                .coordinator_state !=
            ReserveCoordinatorPhaseV1::
                kReleasingPrepared) {
        result.coordinator_error =
            RawReserveCoordinatorErrorV1::
                kCodecTransitionRejected;
        SetError(
            error,
            "physical reserve release requires RELEASING_PREPARED");
        return result;
    }

    // Validate and freeze the exact CONSUMED successor before the first
    // irreversible unlink.  A codec failure can therefore never strand a
    // physically released reserve solely because the logical successor was
    // malformed.
    ReserveStateSlotV1 consumed{};
    result.codec_error =
        BuildReserveConsumedV1(
            state.header,
            state.slots[state.selected_slot],
            &consumed);
    if (result.codec_error !=
        ReserveStateV1Error::kNone) {
        result.coordinator_error =
            RawReserveCoordinatorErrorV1::
                kCodecTransitionRejected;
        SetError(
            error,
            "RELEASING_PREPARED state has no valid CONSUMED successor");
        return result;
    }

    auto inventory =
        AttachRawEmergencyReserveInventoryV1(
            *state_file_,
            capacity_probe,
            &result.reserve_error,
            error);
    if (inventory == nullptr) {
        result.coordinator_error =
            RawReserveCoordinatorErrorV1::
                kReserveInventoryFailure;
        return result;
    }
    result.reserve_error =
        ReleaseRawEmergencyReserveV1(
            *inventory,
            capacity_probe,
            hooks,
            error);
    if (result.reserve_error !=
        RawEmergencyReservePosixErrorV1::kNone) {
        result.coordinator_error =
            RawReserveCoordinatorErrorV1::
                kReserveReleaseFailure;
        return result;
    }
    result.physical_release_complete = true;

    if (state_file_->PublishNext(
            consumed,
            &result.codec_error,
            error) !=
        RawReserveStatePosixError::kNone) {
        result.coordinator_error =
            RawReserveCoordinatorErrorV1::
                kStatePublishFailure;
        return result;
    }
    result.consumed_state_published = true;
    if (!SyncRawRoot(error)) {
        result.coordinator_error =
            RawReserveCoordinatorErrorV1::
                kRootSyncFailure;
        return result;
    }
    result.raw_root_synced = true;
    return result;
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
ActivateNextFinalizationGrant(
    const ReserveGrantActivationV1& request,
    std::string* error) noexcept {
    return EmergencyTransition<
        ReserveGrantActivationV1,
        BuildReserveActivateNextGrantV1>(
        request, error);
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
DebitFinalizationAction(
    const ReserveFinalizationActionKeyV1& request,
    std::string* error) noexcept {
    return EmergencyTransition<
        ReserveFinalizationActionKeyV1,
        BuildReserveDebitActionV1>(
        request, error);
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
CompleteFinalizationAction(
    const ReserveFinalizationActionKeyV1& request,
    std::string* error) noexcept {
    if (request.action_kind ==
        FinalizationActionKindV1::kContinuation) {
        SetError(
            error,
            "CONTINUATION completion requires its typed durable receipt");
        return RawReserveCoordinatorErrorV1::
            kInvalidArgument;
    }
    return EmergencyTransition<
        ReserveFinalizationActionKeyV1,
        BuildReserveCompleteActionV1>(
        request, error);
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
CompleteFinalizationContinuation(
    std::unique_ptr<
        RawFinalizationContinuationReceiptV1>&&
        supplied_receipt,
    RawEmergencyReserveCapacityProbeV1*
        capacity_probe,
    std::string* error) noexcept {
    std::unique_ptr<
        RawFinalizationContinuationReceiptV1> receipt =
        std::move(supplied_receipt);
    SetError(error, {});
    if (receipt == nullptr ||
        capacity_probe == nullptr ||
        receipt->consumed_ ||
        receipt->action_ == nullptr) {
        SetError(
            error,
            "continuation completion requires one live typed receipt and capacity probe");
        return RawReserveCoordinatorErrorV1::
            kInvalidArgument;
    }

    // Validate while the receipt still owns the shared action gate. This
    // freezes a trustworthy handoff snapshot; the opaque receipt is consumed
    // even if the later exclusive transition or publication fails.
    if (receipt->key_ != receipt->action_->key() ||
        receipt->generation_token_ !=
            receipt->action_->generation_token() ||
        receipt->immutable_grant_sha256_ !=
            receipt->action_->
                immutable_grant_sha256() ||
        receipt->plan_ != receipt->action_->plan() ||
        receipt->grant_flags_ !=
            receipt->action_->grant_flags() ||
        receipt->byte_cap_ !=
            receipt->action_->byte_cap() ||
        receipt->inode_cap_ !=
            receipt->action_->inode_cap() ||
        receipt->debit_generation_ !=
            receipt->action_->debit_generation() ||
        receipt->executor_instance_ !=
            receipt->action_->executor_instance()) {
        SetError(
            error,
            "continuation receipt action binding changed");
        return RawReserveCoordinatorErrorV1::
            kActionGenerationChanged;
    }
    if (!receipt->action_->ValidateLatest(error)) {
        return RawReserveCoordinatorErrorV1::
            kActionGenerationChanged;
    }
    if (!receipt->Validate(error)) {
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }
    const RawReserveMutationTargetAnchorV1* target =
        receipt->action_->target();
    if (target == nullptr ||
        receipt->grant_flags_ !=
            kReserveGrantRawFinalization ||
        receipt->key_.action_kind !=
            FinalizationActionKindV1::kContinuation ||
        receipt->plan_.object_type !=
            FinalizationActionKindV1::kContinuation ||
        !IsCanonicalRawStreamRouteV1(
            receipt->key_.grant.source_stream_id,
            target->stream_slug())) {
        SetError(
            error,
            "continuation receipt does not bind one routed RAW_FINALIZATION continuation");
        return RawReserveCoordinatorErrorV1::
            kActionGenerationChanged;
    }

    std::string stream_slug;
    try {
        stream_slug = target->stream_slug();
    } catch (...) {
        SetError(
            error,
            "cannot retain continuation route slug during gate handoff");
        return RawReserveCoordinatorErrorV1::
            kAllocationFailure;
    }
    const std::uint64_t expected_root_device =
        target->raw_root_device();
    const std::uint64_t expected_root_inode =
        target->raw_root_inode();
    const std::uint64_t expected_route_device =
        target->route_device();
    const std::uint64_t expected_route_inode =
        target->route_inode();

    const ReserveFinalizationActionKeyV1 key =
        receipt->key_;
    const RawReserveGenerationActionTokenV1 token =
        receipt->generation_token_;
    const ReserveStateV1Digest immutable_grant_sha256 =
        receipt->immutable_grant_sha256_;
    const FinalizationActionPlanV1 plan =
        receipt->plan_;
    const std::uint8_t grant_flags =
        receipt->grant_flags_;
    const std::uint64_t byte_cap =
        receipt->byte_cap_;
    const std::uint32_t inode_cap =
        receipt->inode_cap_;
    const std::uint64_t debit_generation =
        receipt->debit_generation_;
    const ReserveStateV1Identity executor_instance =
        receipt->executor_instance_;

    receipt->consumed_ = true;
    receipt->action_.reset();

    RawReserveCoordinatorGateError gate_failure =
        RawReserveCoordinatorGateError::kNone;
    auto transition_guard =
        AcquireRawReserveCoordinatorTransitionGuardAtV1(
            lease_->root_directory_descriptor(),
            lease_->anchor(),
            &gate_failure,
            error);
    if (transition_guard == nullptr) {
        return RawReserveCoordinatorErrorV1::
            kTransitionGateFailure;
    }

    ScopedExclusiveFlock route_lock(
        receipt->route_fd_);
    if (!route_lock.locked()) {
        SetError(
            error,
            "cannot exclusively lock the continuation route");
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }
    if (!receipt->ValidateRetainedArtifacts(error)) {
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }

    struct stat coordinator_root {};
    struct stat retained_root {};
    struct stat retained_route {};
    if (!PrivateOwnedDirectory(
            lease_->root_directory_descriptor(),
            &coordinator_root) ||
        !PrivateOwnedDirectory(
            receipt->raw_root_fd_,
            &retained_root) ||
        !PrivateOwnedDirectory(
            receipt->route_fd_,
            &retained_route) ||
        !SameInode(coordinator_root, retained_root) ||
        static_cast<std::uint64_t>(
            retained_root.st_dev) !=
            expected_root_device ||
        static_cast<std::uint64_t>(
            retained_root.st_ino) !=
            expected_root_inode ||
        static_cast<std::uint64_t>(
            retained_route.st_dev) !=
            expected_route_device ||
        static_cast<std::uint64_t>(
            retained_route.st_ino) !=
            expected_route_inode) {
        SetError(
            error,
            "continuation receipt root or route identity changed");
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }

    RawReserveRegistryEntryKeyV1 route_key{};
    route_key.route.source_stream_id =
        key.grant.source_stream_id;
    route_key.route.capture_date =
        key.grant.capture_date;
    route_key.stream_day_id =
        key.grant.stream_day_id;
    OpenedTarget latest_target{};
    if (!OpenExistingTarget(
            receipt->raw_root_fd_,
            route_key,
            stream_slug,
            &latest_target,
            error) ||
        static_cast<std::uint64_t>(
            latest_target.root_status.st_dev) !=
            expected_root_device ||
        static_cast<std::uint64_t>(
            latest_target.root_status.st_ino) !=
            expected_root_inode ||
        static_cast<std::uint64_t>(
            latest_target.route_status.st_dev) !=
            expected_route_device ||
        static_cast<std::uint64_t>(
            latest_target.route_status.st_ino) !=
            expected_route_inode) {
        SetError(
            error,
            "continuation canonical route changed during gate handoff");
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }

    std::lock_guard<std::mutex> state_lock(
        state_mutex_);
    ReserveStateV1Error codec_error =
        ReserveStateV1Error::kNone;
    if (state_file_->Reload(
            &codec_error, error) !=
        RawReserveStatePosixError::kNone) {
        return RawReserveCoordinatorErrorV1::
            kStateFailure;
    }
    const ReserveCoordinatorStateV1& state =
        state_file_->state();
    if (state.selected_slot >= state.slots.size()) {
        SetError(
            error,
            "continuation completion selected slot is invalid");
        return RawReserveCoordinatorErrorV1::
            kStateFailure;
    }
    const ReserveStateSlotV1& before =
        state.slots[state.selected_slot];
    std::size_t active_count = 0U;
    for (std::size_t index = 0U;
         index <
         static_cast<std::size_t>(before.entry_count);
         ++index) {
        if (before.entries[index].grant_status ==
            ReserveGrantStatusV1::kActive) {
            ++active_count;
        }
    }
    if (before.coordinator_state !=
            ReserveCoordinatorPhaseV1::kConsumed ||
        active_count != 1U ||
        before.active_entry_index ==
            kReserveStateV1NoActiveEntry ||
        before.active_entry_index >=
            before.entry_count ||
        key.action_id >=
            kReserveStateV1ActionCapacity) {
        SetError(
            error,
            "continuation receipt no longer selects one CONSUMED ACTIVE grant");
        return RawReserveCoordinatorErrorV1::
            kActionGenerationChanged;
    }

    const std::size_t entry_index =
        before.active_entry_index;
    const std::size_t action_index =
        static_cast<std::size_t>(key.action_id);
    const ReserveStateEntryV1& entry =
        before.entries[entry_index];
    const FinalizationActionReceiptV1&
        durable_receipt =
            entry.actions[action_index];
    const FinalizationActionPlanV1& durable_plan =
        entry.plans[action_index];
    ReserveStateV1Digest durable_grant_sha256{};
    std::uint64_t durable_byte_cap = 0U;
    if (!GrantMatchesKey(
            before, entry, key.grant) ||
        entry.grant_status !=
            ReserveGrantStatusV1::kActive ||
        entry.ack_status !=
            ReserveAckStatusV1::kAcked ||
        entry.grant_flags != grant_flags ||
        grant_flags !=
            kReserveGrantRawFinalization ||
        entry.executor_or_recovery_attempt !=
            executor_instance ||
        MakeToken(before, entry) != token ||
        token.state_generation != before.generation ||
        key.action_kind !=
            FinalizationActionKindV1::kContinuation ||
        durable_receipt.action_id != key.action_id ||
        durable_receipt.action_kind !=
            key.action_kind ||
        durable_receipt.object_plan_sha256 !=
            key.object_plan_sha256 ||
        durable_receipt.action_state !=
            FinalizationActionStateV1::kDebited ||
        durable_receipt.debit_generation !=
            debit_generation ||
        durable_receipt.inode_cap != inode_cap ||
        durable_plan != plan ||
        plan.plan_version != 1U ||
        plan.object_type !=
            FinalizationActionKindV1::kContinuation ||
        plan.plan_flags != 0U ||
        plan.causal_id !=
            before.finalization_cycle_id ||
        !CheckedMulU64(
            state.header.allocation_quantum_bytes,
            static_cast<std::uint64_t>(
                durable_receipt.byte_cap_quanta),
            &durable_byte_cap) ||
        durable_byte_cap != byte_cap ||
        ComputeImmutableFinalizationGrantSha256V1(
            state.header,
            before,
            entry_index,
            &durable_grant_sha256) !=
            ReserveStateV1Error::kNone ||
        durable_grant_sha256 !=
            immutable_grant_sha256) {
        SetError(
            error,
            "continuation receipt key, token, plan, cap, executor, or immutable grant is stale");
        return RawReserveCoordinatorErrorV1::
            kActionGenerationChanged;
    }

    const SegmentHeaderV1& header =
        receipt->continuation_header_;
    const RawWalWriterSnapshot& initialized =
        receipt->initialized_snapshot_;
    const RawWalWriterSnapshot& sealed =
        receipt->sealed_snapshot_;
    const bool live_exact_drain =
        receipt->completion_mode_ ==
        RawFinalizationContinuationCompletionModeV1::
            kLiveExactDrain;
    const bool replacement_seal_only =
        receipt->completion_mode_ ==
        RawFinalizationContinuationCompletionModeV1::
            kReplacementSealOnly;
    const std::uint64_t recovered_record_count =
        receipt->recovered_record_count_;
    const std::uint64_t recovered_framed_wal_bytes =
        receipt->recovered_framed_wal_bytes_;
    std::uint64_t expected_first_ingress = 0U;
    std::uint64_t expected_initialized_wal = 0U;
    std::uint64_t expected_final_wal = 0U;
    std::uint64_t expected_final_ingress = 0U;
    std::uint64_t expected_logical_size = 0U;
    if (!CheckedAddU64(
            entry.raw_counters
                .append_ingress_sequence,
            1U,
            &expected_first_ingress) ||
        !CheckedAddU64(
            entry.raw_counters.append_global_wal_pos,
            kRawV1SegmentHeaderBytes,
            &expected_initialized_wal) ||
        !CheckedAddU64(
            expected_initialized_wal,
            recovered_framed_wal_bytes,
            &expected_final_wal) ||
        !CheckedAddU64(
            entry.raw_counters
                .append_ingress_sequence,
            recovered_record_count,
            &expected_final_ingress) ||
        !CheckedAddU64(
            kRawV1SegmentHeaderBytes,
            recovered_framed_wal_bytes,
            &expected_logical_size)) {
        SetError(
            error,
            "continuation durable suffix counters overflow");
        return RawReserveCoordinatorErrorV1::
            kCodecTransitionRejected;
    }
    const RawWalCursor expected_initialized_cursor{
        expected_initialized_wal,
        entry.raw_counters.append_ingress_sequence,
        kRawV1SegmentHeaderBytes};
    const RawWalCursor expected_final_cursor{
        expected_final_wal,
        expected_final_ingress,
        expected_logical_size};
    if (header.source_stream_id !=
            entry.source_stream_id ||
        header.capture_date != entry.capture_date ||
        header.stream_day_id !=
            entry.stream_day_id ||
        header.segment_sequence !=
            plan.object_sequence ||
        header.segment_flags !=
            kRawV1FinalizationContinuation ||
        header.reserve_state_uuid !=
            state.header.reserve_state_uuid ||
        header.finalization_cycle_id !=
            before.finalization_cycle_id ||
        header.immutable_grant_sha256 !=
            durable_grant_sha256 ||
        header.segment_base_wal_pos !=
            plan.range_start ||
        plan.range_start !=
            entry.raw_counters.append_global_wal_pos ||
        header.first_ingress_sequence !=
            expected_first_ingress ||
        plan.range_end_or_size !=
            receipt->allocation_observation_
                .continuation_allocation_cap ||
        plan.range_end_or_size >
            entry.continuation_allocation_cap ||
        (!live_exact_drain &&
         !replacement_seal_only) ||
        receipt->frozen_record_count_ !=
            entry.raw_counters.queued_record_count ||
        receipt->frozen_framed_wal_bytes_ !=
            entry.raw_counters
                .queued_framed_wal_bytes ||
        recovered_record_count >
            receipt->frozen_record_count_ ||
        recovered_framed_wal_bytes >
            receipt->frozen_framed_wal_bytes_ ||
        ((recovered_record_count == 0U) !=
         (recovered_framed_wal_bytes == 0U)) ||
        (live_exact_drain &&
         (recovered_record_count !=
              receipt->frozen_record_count_ ||
          recovered_framed_wal_bytes !=
              receipt->frozen_framed_wal_bytes_)) ||
        receipt->final_cursor_ !=
            expected_final_cursor ||
        initialized.append !=
            expected_initialized_cursor ||
        initialized.durable !=
            expected_initialized_cursor ||
        !initialized.initialized ||
        initialized.sealed || initialized.closed ||
        initialized.fatal ||
        sealed.append != expected_final_cursor ||
        sealed.durable != expected_final_cursor ||
        !sealed.initialized || !sealed.sealed ||
        sealed.closed || sealed.fatal) {
        SetError(
            error,
            "continuation header, frozen suffix, or terminal cursor does not match the durable grant");
        return RawReserveCoordinatorErrorV1::
            kCodecTransitionRejected;
    }

    DurableMarkerV1 header_marker{};
    DurableMarkerV1 terminal_marker{};
    if (DecodeDurableMarkerV1(
            receipt->header_marker_wire_,
            &header_marker) !=
            RawV1Error::kNone ||
        DecodeDurableMarkerV1(
            receipt->sealed_marker_wire_,
            &terminal_marker) !=
            RawV1Error::kNone ||
        header_marker.source_stream_id !=
            header.source_stream_id ||
        header_marker.segment_sequence !=
            header.segment_sequence ||
        header_marker.durable_global_wal_pos !=
            expected_initialized_cursor.global_wal_pos ||
        header_marker.durable_ingress_sequence !=
            expected_initialized_cursor.ingress_sequence ||
        header_marker.durable_segment_offset !=
            expected_initialized_cursor.segment_offset ||
        header_marker.marker_flags != 0U ||
        terminal_marker.source_stream_id !=
            header.source_stream_id ||
        terminal_marker.segment_sequence !=
            header.segment_sequence ||
        terminal_marker.durable_global_wal_pos !=
            expected_final_cursor.global_wal_pos ||
        terminal_marker.durable_ingress_sequence !=
            expected_final_cursor.ingress_sequence ||
        terminal_marker.durable_segment_offset !=
            expected_final_cursor.segment_offset ||
        terminal_marker.marker_flags !=
            kRawV1SegmentSealed) {
        SetError(
            error,
            "continuation journal markers do not bind the expected initialized and sealed cursors");
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }

    const RawManifestV1& manifest =
        receipt->open_manifest_;
    if (!manifest.open_entry.has_value()) {
        SetError(
            error,
            "continuation receipt has no exact open-manifest commitment");
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }
    const RawManifestSegmentEntryV1& open =
        *manifest.open_entry;
    const RawManifestNamespaceV1 expected_namespace{
        entry.capture_date,
        entry.source_stream_id,
        entry.stream_day_id};
    if (manifest.namespace_identity !=
            expected_namespace ||
        open.namespace_identity !=
            expected_namespace ||
        open.segment_sequence !=
            header.segment_sequence ||
        open.segment_flags !=
            kRawV1FinalizationContinuation ||
        open.state !=
            RawManifestSegmentStateV1::kOpen ||
        open.segment_base_wal_pos !=
            header.segment_base_wal_pos ||
        open.segment_logical_length !=
            kRawV1SegmentHeaderBytes ||
        open.segment_sha256 !=
            l2flow::common::ComputeSha256(
                receipt->
                    continuation_header_wire_) ||
        open.record_count != 0U ||
        open.actual_first_ingress_sequence
            .has_value() ||
        open.actual_last_ingress_sequence
            .has_value() ||
        open.next_expected_first_ingress_sequence !=
            header.first_ingress_sequence ||
        open.accepted_marker_bytes !=
            receipt->header_marker_wire_ ||
        open.accepted_marker_sha256 !=
            receipt->header_marker_sha256_ ||
        open.reserve_state_uuid !=
            state.header.reserve_state_uuid ||
        open.finalization_cycle_id !=
            before.finalization_cycle_id ||
        open.immutable_grant_sha256 !=
            durable_grant_sha256) {
        SetError(
            error,
            "continuation open manifest does not bind the exact header-only publication");
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }

    const RawFinalizationContinuationAllocationObservationV1&
        allocation =
            receipt->allocation_observation_;
    const auto PositiveDelta =
        [](std::uint64_t before_bytes,
           std::uint64_t after_bytes) noexcept {
            return after_bytes > before_bytes
                       ? after_bytes - before_bytes
                       : 0U;
        };
    std::uint64_t observed_delta =
        allocation.continuation_allocated_bytes;
    if (!CheckedAddU64(
            observed_delta,
            PositiveDelta(
                allocation
                    .old_segment_allocated_bytes_before,
                allocation
                    .old_segment_allocated_bytes_after),
            &observed_delta) ||
        !CheckedAddU64(
            observed_delta,
            PositiveDelta(
                allocation
                    .journal_allocated_bytes_before,
                allocation
                    .journal_allocated_bytes_after),
            &observed_delta) ||
        !CheckedAddU64(
            observed_delta,
            PositiveDelta(
                allocation
                    .manifest_allocated_bytes_before,
                allocation
                    .manifest_allocated_bytes_after),
            &observed_delta) ||
        allocation.authorized_byte_cap !=
            byte_cap ||
        allocation.authorized_inode_cap !=
            inode_cap ||
        allocation.continuation_allocation_cap !=
            plan.range_end_or_size ||
        allocation.continuation_allocation_cap >
            entry.continuation_allocation_cap ||
        allocation.continuation_logical_size !=
            expected_logical_size ||
        allocation.continuation_allocated_bytes >
            allocation.continuation_allocation_cap ||
        observed_delta !=
            allocation
                .conservative_positive_allocation_delta ||
        observed_delta > byte_cap ||
        inode_cap < 2U ||
        receipt->segment_inode_.device !=
            receipt->manifest_inode_.device ||
        receipt->segment_inode_.inode ==
            receipt->manifest_inode_.inode) {
        SetError(
            error,
            "continuation allocation exceeds the durable byte, inode, or continuation cap");
        return RawReserveCoordinatorErrorV1::
            kCapacityProbeFailure;
    }
    if (!ValidateFinalizationCapacity(
            receipt->raw_root_fd_,
            state.header,
            before,
            entry,
            capacity_probe,
            error)) {
        return RawReserveCoordinatorErrorV1::
            kCapacityProbeFailure;
    }

    if (!receipt->ValidateRetainedArtifacts(error)) {
        SetError(
            error,
            "continuation artifacts changed before the COMPLETE state barrier");
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }
    OpenedTarget final_target{};
    if (!OpenExistingTarget(
            receipt->raw_root_fd_,
            route_key,
            stream_slug,
            &final_target,
            error) ||
        static_cast<std::uint64_t>(
            final_target.root_status.st_dev) !=
            expected_root_device ||
        static_cast<std::uint64_t>(
            final_target.root_status.st_ino) !=
            expected_root_inode ||
        static_cast<std::uint64_t>(
            final_target.route_status.st_dev) !=
            expected_route_device ||
        static_cast<std::uint64_t>(
            final_target.route_status.st_ino) !=
            expected_route_inode) {
        SetError(
            error,
            "continuation root or canonical route changed before the COMPLETE state barrier");
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }

    ReserveStateSlotV1 candidate{};
    codec_error =
        BuildReserveCompleteActionV1(
            state.header,
            before,
            key,
            &candidate);
    if (codec_error != ReserveStateV1Error::kNone) {
        SetError(
            error,
            "typed continuation COMPLETE transition was rejected");
        return RawReserveCoordinatorErrorV1::
            kCodecTransitionRejected;
    }
    if (state_file_->PublishNext(
            candidate, &codec_error, error) !=
        RawReserveStatePosixError::kNone) {
        return RawReserveCoordinatorErrorV1::
            kStatePublishFailure;
    }
    if (!SyncRawRoot(error)) {
        return RawReserveCoordinatorErrorV1::
            kRootSyncFailure;
    }
    return RawReserveCoordinatorErrorV1::kNone;
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
ReplaceDebitedContinuationExecutor(
    const ReserveFinalizationActionKeyV1& key,
    std::string_view canonical_stream_slug,
    const ReserveStateV1Identity&
        replacement_executor,
    std::string* error) noexcept {
    SetError(error, {});
    if (key.action_kind !=
            FinalizationActionKindV1::kContinuation ||
        key.action_id >=
            kReserveStateV1ActionCapacity ||
        l2flow::common::IsZeroIdentity(
            replacement_executor) ||
        !IsCanonicalRawStreamRouteV1(
            key.grant.source_stream_id,
            canonical_stream_slug)) {
        SetError(
            error,
            "replacement continuation executor request is invalid");
        return RawReserveCoordinatorErrorV1::
            kInvalidArgument;
    }

    RawReserveCoordinatorGateError gate_failure =
        RawReserveCoordinatorGateError::kNone;
    auto transition_guard =
        AcquireRawReserveCoordinatorTransitionGuardAtV1(
            lease_->root_directory_descriptor(),
            lease_->anchor(),
            &gate_failure,
            error);
    if (transition_guard == nullptr) {
        return RawReserveCoordinatorErrorV1::
            kTransitionGateFailure;
    }

    RawReserveRegistryEntryKeyV1 route_key{};
    route_key.route.source_stream_id =
        key.grant.source_stream_id;
    route_key.route.capture_date =
        key.grant.capture_date;
    route_key.stream_day_id =
        key.grant.stream_day_id;
    OpenedTarget target{};
    if (!OpenExistingTarget(
            lease_->root_directory_descriptor(),
            route_key,
            canonical_stream_slug,
            &target,
            error)) {
        return RawReserveCoordinatorErrorV1::
            kTargetUnavailable;
    }
    ScopedExclusiveFlock route_lock(
        target.route.get());
    if (!route_lock.locked()) {
        SetError(
            error,
            "cannot exclusively lock replacement continuation route");
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }

    std::lock_guard<std::mutex> state_lock(
        state_mutex_);
    ReserveStateV1Error codec_error =
        ReserveStateV1Error::kNone;
    if (state_file_->Reload(
            &codec_error, error) !=
        RawReserveStatePosixError::kNone) {
        return RawReserveCoordinatorErrorV1::
            kStateFailure;
    }
    const ReserveCoordinatorStateV1& state =
        state_file_->state();
    if (state.selected_slot >= state.slots.size()) {
        SetError(
            error,
            "replacement continuation selected slot is invalid");
        return RawReserveCoordinatorErrorV1::
            kStateFailure;
    }
    const ReserveStateSlotV1& before =
        state.slots[state.selected_slot];
    if (before.coordinator_state !=
            ReserveCoordinatorPhaseV1::kConsumed ||
        before.active_entry_index ==
            kReserveStateV1NoActiveEntry ||
        before.active_entry_index >=
            before.entry_count) {
        SetError(
            error,
            "replacement continuation requires one CONSUMED ACTIVE grant");
        return RawReserveCoordinatorErrorV1::
            kActionGenerationChanged;
    }

    std::size_t active_count = 0U;
    std::size_t debited_count = 0U;
    for (std::size_t entry_index = 0U;
         entry_index <
         static_cast<std::size_t>(
             before.entry_count);
         ++entry_index) {
        const ReserveStateEntryV1& entry =
            before.entries[entry_index];
        if (entry.grant_status ==
            ReserveGrantStatusV1::kActive) {
            ++active_count;
        }
        for (const FinalizationActionReceiptV1&
                 action : entry.actions) {
            if (action.action_kind ==
                FinalizationActionKindV1::kUnused) {
                break;
            }
            if (action.action_state ==
                FinalizationActionStateV1::kDebited) {
                ++debited_count;
            }
        }
    }

    const std::size_t entry_index =
        before.active_entry_index;
    const std::size_t action_index =
        static_cast<std::size_t>(key.action_id);
    const ReserveStateEntryV1& entry =
        before.entries[entry_index];
    const FinalizationActionReceiptV1&
        durable_receipt =
            entry.actions[action_index];
    const FinalizationActionPlanV1& plan =
        entry.plans[action_index];
    if (active_count != 1U ||
        debited_count != 1U ||
        !GrantMatchesKey(
            before, entry, key.grant) ||
        entry.grant_status !=
            ReserveGrantStatusV1::kActive ||
        entry.ack_status !=
            ReserveAckStatusV1::kAcked ||
        entry.grant_flags !=
            kReserveGrantRawFinalization ||
        l2flow::common::IsZeroIdentity(
            entry.executor_or_recovery_attempt) ||
        entry.executor_or_recovery_attempt ==
            replacement_executor ||
        durable_receipt.action_id !=
            key.action_id ||
        durable_receipt.action_kind !=
            FinalizationActionKindV1::kContinuation ||
        durable_receipt.object_plan_sha256 !=
            key.object_plan_sha256 ||
        durable_receipt.action_state !=
            FinalizationActionStateV1::kDebited ||
        durable_receipt.debit_generation == 0U ||
        plan.plan_version != 1U ||
        plan.object_type !=
            FinalizationActionKindV1::kContinuation ||
        plan.plan_flags != 0U ||
        plan.object_sequence == 0U ||
        plan.object_sequence >
            kRawSegmentArtifactMaximumSequenceV1 ||
        plan.causal_id !=
            before.finalization_cycle_id) {
        SetError(
            error,
            "replacement continuation no longer binds the sole exact DEBITED action");
        return RawReserveCoordinatorErrorV1::
            kActionGenerationChanged;
    }

    ContinuationReplacementCandidate
        retained_candidate{};
    if (!OpenSoleContinuationReplacementCandidate(
            target.route.get(),
            plan.object_sequence,
            &retained_candidate,
            error)) {
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }
    for (;;) {
        if (::fsync(target.route.get()) == 0) {
            break;
        }
        if (errno != EINTR) {
            SetError(
                error,
                "cannot sync replacement continuation candidate parent");
            return RawReserveCoordinatorErrorV1::
                kTargetIdentityChanged;
        }
    }
    if (!ContinuationReplacementCandidateStillNamed(
            target.route.get(),
            retained_candidate,
            error)) {
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }

    if (before.generation ==
        std::numeric_limits<std::uint64_t>::max()) {
        SetError(
            error,
            "replacement continuation generation cannot wrap");
        return RawReserveCoordinatorErrorV1::
            kCodecTransitionRejected;
    }
    ReserveStateSlotV1 candidate = before;
    candidate.generation = before.generation + 1U;
    candidate.entries[entry_index]
        .executor_or_recovery_attempt =
        replacement_executor;
    if (state_file_->PublishNext(
            candidate, &codec_error, error) !=
        RawReserveStatePosixError::kNone) {
        return RawReserveCoordinatorErrorV1::
            kStatePublishFailure;
    }
    if (!SyncRawRoot(error)) {
        return RawReserveCoordinatorErrorV1::
            kRootSyncFailure;
    }
    return RawReserveCoordinatorErrorV1::kNone;
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
FailDebitedFinalizationAction(
    const ReserveFinalizationActionKeyV1& request,
    std::string* error) noexcept {
    return EmergencyTransition<
        ReserveFinalizationActionKeyV1,
        BuildReserveFailDebitedActionV1>(
        request, error);
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
FailActiveFinalizationGrant(
    const ReserveGrantFailureV1& request,
    std::string* error) noexcept {
    return EmergencyTransition<
        ReserveGrantFailureV1,
        BuildReserveFailActiveGrantV1>(
        request, error);
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
MarkFinalizationGrantDone(
    const ReserveGrantTerminalReportV1& request,
    std::string* error) noexcept {
    return EmergencyTransition<
        ReserveGrantTerminalReportV1,
        BuildReserveMarkGrantDoneV1>(
        request, error);
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
CompleteReportAndMarkFinalizationGrantDone(
    const ReserveAtomicReportCompletionV1& request,
    std::string* error) noexcept {
    return EmergencyTransition<
        ReserveAtomicReportCompletionV1,
        BuildReserveCompleteReportAndMarkGrantDoneV1>(
        request, error);
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
CompleteFinalizationReport(
    std::unique_ptr<FinalizationReportReceiptV1>&&
        supplied_receipt,
    RawEmergencyReserveCapacityProbeV1*
        capacity_probe,
    std::string* error) noexcept {
    std::unique_ptr<FinalizationReportReceiptV1>
        receipt = std::move(supplied_receipt);
    if (receipt == nullptr) {
        SetError(
            error,
            "finalization report receipt is absent");
        return RawReserveCoordinatorErrorV1::
            kInvalidArgument;
    }
    return CompleteFinalizationReportImpl(
        receipt.get(), nullptr, capacity_probe, error);
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
CompleteScaffoldingFinalizationReport(
    std::unique_ptr<
        ScaffoldingFinalizationReportReceiptV1>&&
        supplied_receipt,
    RawEmergencyReserveCapacityProbeV1*
        capacity_probe,
    std::string* error) noexcept {
    std::unique_ptr<
        ScaffoldingFinalizationReportReceiptV1>
        receipt = std::move(supplied_receipt);
    if (receipt == nullptr) {
        SetError(
            error,
            "scaffolding finalization report receipt is absent");
        return RawReserveCoordinatorErrorV1::
            kInvalidArgument;
    }
    return CompleteFinalizationReportImpl(
        nullptr, receipt.get(), capacity_probe, error);
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
CompleteFinalizationReportImpl(
    FinalizationReportReceiptV1* routed_receipt,
    ScaffoldingFinalizationReportReceiptV1*
        scaffolding_receipt,
    RawEmergencyReserveCapacityProbeV1*
        capacity_probe,
    std::string* error) noexcept {
    SetError(error, {});
    const bool routed = routed_receipt != nullptr;
    if (routed ==
            (scaffolding_receipt != nullptr) ||
        capacity_probe == nullptr ||
        (routed && routed_receipt->consumed_) ||
        (!routed && scaffolding_receipt->consumed_)) {
        SetError(
            error,
            "finalization report receipt is absent, ambiguous, consumed, or lacks a capacity probe");
        return RawReserveCoordinatorErrorV1::
            kInvalidArgument;
    }

    RawReserveCoordinatorGateError gate_failure =
        RawReserveCoordinatorGateError::kNone;
    auto transition_guard =
        AcquireRawReserveCoordinatorTransitionGuardAtV1(
            lease_->root_directory_descriptor(),
            lease_->anchor(),
            &gate_failure,
            error);
    if (transition_guard == nullptr) {
        return RawReserveCoordinatorErrorV1::
            kTransitionGateFailure;
    }

    const int report_parent_fd =
        routed
            ? routed_receipt->report_parent_fd_
            : scaffolding_receipt->report_parent_fd_;
    ScopedExclusiveFlock report_parent_lock(
        report_parent_fd);
    if (!report_parent_lock.locked()) {
        SetError(
            error,
            "cannot exclusively lock the finalization report parent");
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }

    const bool retained_valid =
        routed
            ? routed_receipt->Validate(error)
            : scaffolding_receipt->Validate(error);
    if (!retained_valid) {
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }

    const ReserveFinalizationActionKeyV1& key =
        routed ? routed_receipt->key_
               : scaffolding_receipt->key_;
    const RawReserveGenerationActionTokenV1& token =
        routed ? routed_receipt->token_
               : scaffolding_receipt->token_;
    const ReserveStateV1Digest&
        receipt_immutable_grant_sha256 =
            routed
                ? routed_receipt
                      ->immutable_grant_sha256_
                : scaffolding_receipt
                      ->immutable_grant_sha256_;
    const FinalizationActionPlanV1& receipt_plan =
        routed ? routed_receipt->plan_
               : scaffolding_receipt->plan_;
    const std::uint8_t receipt_grant_flags =
        routed ? routed_receipt->grant_flags_
               : scaffolding_receipt->grant_flags_;
    const std::uint64_t receipt_byte_cap =
        routed ? routed_receipt->byte_cap_
               : scaffolding_receipt->byte_cap_;
    const std::uint32_t receipt_inode_cap =
        routed ? routed_receipt->inode_cap_
               : scaffolding_receipt->inode_cap_;
    const std::uint64_t receipt_debit_generation =
        routed ? routed_receipt->debit_generation_
               : scaffolding_receipt
                     ->debit_generation_;
    const ReserveStateV1Identity&
        receipt_executor_instance =
            routed
                ? routed_receipt->executor_instance_
                : scaffolding_receipt
                      ->executor_instance_;
    const RawV1Digest& report_sha256 =
        routed ? routed_receipt->report_sha256_
               : scaffolding_receipt
                     ->report_sha256_;
    const std::size_t report_byte_count =
        routed ? routed_receipt->byte_count_
               : scaffolding_receipt->byte_count_;
    const std::string& report_filename =
        routed ? routed_receipt->filename_
               : scaffolding_receipt->filename_;
    const int retained_root_fd =
        routed ? routed_receipt->raw_root_fd_
               : scaffolding_receipt->raw_root_fd_;
    const int report_fd =
        routed ? routed_receipt->report_fd_
               : scaffolding_receipt->report_fd_;
    const std::uint64_t report_device =
        routed ? routed_receipt->report_device_
               : scaffolding_receipt->report_device_;
    const std::uint64_t report_inode =
        routed ? routed_receipt->report_inode_
               : scaffolding_receipt->report_inode_;
    const std::uint64_t report_blocks =
        routed ? routed_receipt->report_blocks_
               : scaffolding_receipt->report_blocks_;

    std::string report_bytes;
    if (!ReadExactReportBytes(
            report_fd,
            report_byte_count,
            report_device,
            report_inode,
            report_blocks,
            report_sha256,
            &report_bytes,
            error)) {
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }

    FinalizationReportV1 routed_model{};
    ScaffoldingFinalizationReportV1
        scaffolding_model{};
    std::string canonical;
    std::string parsed_filename;
    if (routed) {
        if (ParseFinalizationReportV1Jcs(
                report_bytes, &routed_model) !=
                FinalizationReportV1Error::kNone ||
            EncodeFinalizationReportV1Jcs(
                routed_model, &canonical) !=
                FinalizationReportV1Error::kNone ||
            FinalizationReportV1Filename(
                routed_model, &parsed_filename) !=
                FinalizationReportV1Error::kNone) {
            SetError(
                error,
                "routed finalization report is not exact canonical JCS");
            return RawReserveCoordinatorErrorV1::
                kTargetIdentityChanged;
        }
    } else if (
        ParseScaffoldingFinalizationReportV1Jcs(
            report_bytes, &scaffolding_model) !=
            ScaffoldingFinalizationReportV1Error::
                kNone ||
        EncodeScaffoldingFinalizationReportV1Jcs(
            scaffolding_model, &canonical) !=
            ScaffoldingFinalizationReportV1Error::
                kNone ||
        ScaffoldingFinalizationReportV1Filename(
            scaffolding_model, &parsed_filename) !=
            ScaffoldingFinalizationReportV1Error::
                kNone) {
        SetError(
            error,
            "scaffolding finalization report is not exact canonical JCS");
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }
    if (canonical != report_bytes ||
        parsed_filename != report_filename ||
        !(routed
              ? routed_receipt->Validate(error)
              : scaffolding_receipt->Validate(
                    error))) {
        SetError(
            error,
            "finalization report changed during canonical revalidation");
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }

    struct stat coordinator_root {};
    struct stat receipt_root {};
    if (!PrivateOwnedDirectory(
            lease_->root_directory_descriptor(),
            &coordinator_root) ||
        !PrivateOwnedDirectory(
            retained_root_fd, &receipt_root) ||
        !SameInode(
            coordinator_root, receipt_root)) {
        SetError(
            error,
            "finalization report receipt belongs to a different Raw root");
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }

    std::lock_guard<std::mutex> state_lock(
        state_mutex_);
    ReserveStateV1Error codec_error =
        ReserveStateV1Error::kNone;
    if (state_file_->Reload(
            &codec_error, error) !=
        RawReserveStatePosixError::kNone) {
        return RawReserveCoordinatorErrorV1::
            kStateFailure;
    }
    const ReserveCoordinatorStateV1& state =
        state_file_->state();
    if (state.selected_slot >= state.slots.size()) {
        SetError(
            error,
            "reserve selected slot is invalid");
        return RawReserveCoordinatorErrorV1::
            kStateFailure;
    }
    const ReserveStateSlotV1& before =
        state.slots[state.selected_slot];
    if (before.coordinator_state !=
            ReserveCoordinatorPhaseV1::kConsumed ||
        before.active_entry_index ==
            kReserveStateV1NoActiveEntry ||
        before.active_entry_index >=
            before.entry_count ||
        key.action_id >=
            kReserveStateV1ActionCapacity) {
        SetError(
            error,
            "terminal report no longer selects one CONSUMED ACTIVE grant");
        return RawReserveCoordinatorErrorV1::
            kActionGenerationChanged;
    }
    const std::size_t entry_index =
        before.active_entry_index;
    const std::size_t action_index =
        static_cast<std::size_t>(key.action_id);
    const ReserveStateEntryV1& entry =
        before.entries[entry_index];
    const FinalizationActionReceiptV1&
        durable_receipt =
            entry.actions[action_index];
    const FinalizationActionPlanV1& durable_plan =
        entry.plans[action_index];
    ReserveStateV1Digest
        durable_immutable_grant_sha256{};
    std::uint64_t durable_byte_cap = 0U;
    if (!GrantMatchesKey(
            before, entry, key.grant) ||
        entry.grant_status !=
            ReserveGrantStatusV1::kActive ||
        entry.executor_or_recovery_attempt !=
            receipt_executor_instance ||
        entry.grant_flags != receipt_grant_flags ||
        MakeToken(before, entry) != token ||
        key.action_kind !=
            FinalizationActionKindV1::
                kFinalizationReport ||
        durable_receipt.action_id != key.action_id ||
        durable_receipt.action_kind !=
            key.action_kind ||
        durable_receipt.object_plan_sha256 !=
            key.object_plan_sha256 ||
        durable_receipt.action_state !=
            FinalizationActionStateV1::kDebited ||
        durable_receipt.debit_generation !=
            receipt_debit_generation ||
        durable_receipt.inode_cap !=
            receipt_inode_cap ||
        durable_plan != receipt_plan ||
        durable_plan.object_type !=
            FinalizationActionKindV1::
                kFinalizationReport ||
        !CheckedMulU64(
            state.header.allocation_quantum_bytes,
            static_cast<std::uint64_t>(
                durable_receipt.byte_cap_quanta),
            &durable_byte_cap) ||
        durable_byte_cap != receipt_byte_cap ||
        ComputeImmutableFinalizationGrantSha256V1(
            state.header,
            before,
            entry_index,
            &durable_immutable_grant_sha256) !=
            ReserveStateV1Error::kNone ||
        durable_immutable_grant_sha256 !=
            receipt_immutable_grant_sha256) {
        SetError(
            error,
            "finalization report receipt action, plan, cap, executor, or immutable grant is stale");
        return RawReserveCoordinatorErrorV1::
            kActionGenerationChanged;
    }

    const std::uint16_t prestate_flags =
        receipt_plan.plan_flags;
    const bool newly_allocated_report =
        (prestate_flags &
         (kFinalizationPlanExistingFinal |
          kFinalizationPlanCompleteTmpOnly)) == 0U;
    std::uint64_t report_allocated_bytes = 0U;
    if (!CheckedMulU64(
            report_blocks, UINT64_C(512),
            &report_allocated_bytes) ||
        (newly_allocated_report &&
         (report_allocated_bytes >
              receipt_byte_cap ||
          receipt_inode_cap == 0U))) {
        SetError(
            error,
            "finalization report allocation exceeds its durable action cap");
        return RawReserveCoordinatorErrorV1::
            kCapacityProbeFailure;
    }

    const bool model_valid =
        routed
            ? ValidateRoutedReportModel(
                  routed_model,
                  state.header,
                  before,
                  entry,
                  durable_immutable_grant_sha256)
            : ValidateScaffoldingReportModel(
                  scaffolding_model,
                  state.header,
                  before,
                  entry,
                  durable_immutable_grant_sha256);
    if (!model_valid) {
        SetError(
            error,
            "finalization report model does not match the durable grant");
        return RawReserveCoordinatorErrorV1::
            kCodecTransitionRejected;
    }
    if (!ValidateFinalizationCapacity(
            retained_root_fd,
            state.header,
            before,
            entry,
            capacity_probe,
            error)) {
        return RawReserveCoordinatorErrorV1::
            kCapacityProbeFailure;
    }

    // Close any same-UID mutation window immediately before committing the
    // report hash.  The parent flock serializes cooperating publishers; the
    // retained-fd/name/inode/hash revalidation remains the authority.
    if (!(routed
              ? routed_receipt->Validate(error)
              : scaffolding_receipt->Validate(
                    error))) {
        SetError(
            error,
            "finalization report changed before the terminal state barrier");
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }
    if (routed) {
        routed_receipt->consumed_ = true;
    } else {
        scaffolding_receipt->consumed_ = true;
    }

    ReserveAtomicReportCompletionV1 completion{};
    completion.report_action = key;
    completion.maintenance_report_sha256 =
        report_sha256;
    ReserveStateSlotV1 candidate{};
    codec_error =
        BuildReserveCompleteReportAndMarkGrantDoneV1(
            state.header,
            before,
            completion,
            &candidate);
    if (codec_error != ReserveStateV1Error::kNone) {
        SetError(
            error,
            "atomic finalization report COMPLETE + grant DONE transition was rejected");
        return RawReserveCoordinatorErrorV1::
            kCodecTransitionRejected;
    }
    if (state_file_->PublishNext(
            candidate, &codec_error, error) !=
        RawReserveStatePosixError::kNone) {
        return RawReserveCoordinatorErrorV1::
            kStatePublishFailure;
    }
    if (!SyncRawRoot(error)) {
        return RawReserveCoordinatorErrorV1::
            kRootSyncFailure;
    }
    return RawReserveCoordinatorErrorV1::kNone;
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
RegisterExistingAnchorRecovering(
    const RawReserveExistingAnchorRecoveryV1& request,
    std::string* error) noexcept {
    return Transition<
        RawReserveExistingAnchorRecoveryV1,
        BuildRawReserveRegisterExistingAnchorRecoveringV1>(
        request, error);
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
TakeoverScaffolding(
    const RawReserveScaffoldingTakeoverV1& request,
    std::string* error) noexcept {
    return Transition<
        RawReserveScaffoldingTakeoverV1,
        BuildRawReserveTakeoverScaffoldingV1>(
        request, error);
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::TakeoverInit(
    const RawReserveWriterTakeoverV1& request,
    std::string* error) noexcept {
    return Transition<
        RawReserveWriterTakeoverV1,
        BuildRawReserveTakeoverInitV1>(
        request, error);
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::TakeoverRecovering(
    const RawReserveWriterTakeoverV1& request,
    std::string* error) noexcept {
    return Transition<
        RawReserveWriterTakeoverV1,
        BuildRawReserveTakeoverRecoveringV1>(
        request, error);
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::TakeoverActive(
    const RawReserveActiveTakeoverV1& request,
    std::string* error) noexcept {
    return Transition<
        RawReserveActiveTakeoverV1,
        BuildRawReserveTakeoverActiveV1>(
        request, error);
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::PublishActive(
    const RawReserveRegistryEntryKeyV1& key,
    std::string* error) noexcept {
    RawReserveCoordinatorGateError gate_failure =
        RawReserveCoordinatorGateError::kNone;
    auto transition_guard =
        AcquireRawReserveCoordinatorTransitionGuardAtV1(
            lease_->root_directory_descriptor(),
            lease_->anchor(),
            &gate_failure,
            error);
    if (transition_guard == nullptr) {
        return RawReserveCoordinatorErrorV1::
            kTransitionGateFailure;
    }

    std::lock_guard<std::mutex> state_lock(
        state_mutex_);
    std::size_t entry_index = 0U;
    if (!ReloadAndFind(
            key,
            ReserveRegistryStatusV1::kInit,
            &entry_index,
            error)) {
        SetError(
            error,
            "receipt-free ACTIVE publication is restricted to fresh INIT");
        return RawReserveCoordinatorErrorV1::
            kRouteStatusMismatch;
    }
    const ReserveCoordinatorStateV1& state =
        state_file_->state();
    if (state.selected_slot >= state.slots.size()) {
        return RawReserveCoordinatorErrorV1::
            kStateFailure;
    }
    const ReserveStateSlotV1& before =
        state.slots[state.selected_slot];
    ReserveStateSlotV1 candidate{};
    ReserveStateV1Error codec_error =
        BuildRawReservePublishActiveV1(
            state.header, before, key, &candidate);
    if (codec_error != ReserveStateV1Error::kNone) {
        return RawReserveCoordinatorErrorV1::
            kCodecTransitionRejected;
    }
    if (state_file_->PublishNext(
            candidate, &codec_error, error) !=
        RawReserveStatePosixError::kNone) {
        return RawReserveCoordinatorErrorV1::
            kStatePublishFailure;
    }
    return RawReserveCoordinatorErrorV1::kNone;
}

std::unique_ptr<RawReserveActiveActivationReceiptV1>
RawReserveRegistryCoordinatorV1::PublishFreshActive(
    const RawReserveRegistryEntryKeyV1& key,
    std::string_view stream_slug,
    RawReserveCoordinatorErrorV1* failure,
    std::string* error) noexcept {
    SetFailure(
        failure, RawReserveCoordinatorErrorV1::kNone);
    SetError(error, {});
    if (!IsCanonicalRawStreamRouteV1(
            key.route.source_stream_id,
            stream_slug)) {
        SetError(
            error,
            "fresh ACTIVE target slug is not canonical");
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::kInvalidArgument);
        return nullptr;
    }

    RawReserveCoordinatorGateError gate_failure =
        RawReserveCoordinatorGateError::kNone;
    auto transition_guard =
        AcquireRawReserveCoordinatorTransitionGuardAtV1(
            lease_->root_directory_descriptor(),
            lease_->anchor(),
            &gate_failure,
            error);
    if (transition_guard == nullptr) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kTransitionGateFailure);
        return nullptr;
    }

    std::lock_guard<std::mutex> state_lock(
        state_mutex_);
    std::size_t entry_index = 0U;
    if (!ReloadAndFind(
            key,
            ReserveRegistryStatusV1::kInit,
            &entry_index,
            error)) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kRouteStatusMismatch);
        return nullptr;
    }
    const ReserveCoordinatorStateV1& state =
        state_file_->state();
    if (state.selected_slot >= state.slots.size()) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::kStateFailure);
        return nullptr;
    }
    const ReserveStateSlotV1& before =
        state.slots[state.selected_slot];
    if (entry_index >= before.entry_count) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::kStateFailure);
        return nullptr;
    }

    OpenedTarget latest{};
    if (!OpenExistingTarget(
            lease_->root_directory_descriptor(),
            key,
            stream_slug,
            &latest,
            error)) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kTargetUnavailable);
        return nullptr;
    }
    std::optional<RawReserveMutationTargetAnchorV1>
        target;
    try {
        target =
            RawReserveMutationTargetAnchorV1(
                static_cast<std::uint64_t>(
                    latest.root_status.st_dev),
                static_cast<std::uint64_t>(
                    latest.root_status.st_ino),
                static_cast<std::uint64_t>(
                    latest.route_status.st_dev),
                static_cast<std::uint64_t>(
                    latest.route_status.st_ino),
                lease_->anchor()
                    .marker.mount_identity_sha256,
                key.route.source_stream_id,
                key.route.capture_date,
                std::string(stream_slug));
    } catch (...) {
        SetError(
            error,
            "cannot allocate immutable fresh Raw target");
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kAllocationFailure);
        return nullptr;
    }

    ReserveStateSlotV1 candidate{};
    const ReserveStateV1Error codec_error =
        BuildRawReservePublishActiveV1(
            state.header, before, key, &candidate);
    if (codec_error != ReserveStateV1Error::kNone ||
        entry_index >= candidate.entry_count ||
        candidate.entries[entry_index].registry_status !=
            ReserveRegistryStatusV1::kActive) {
        SetError(
            error,
            "fresh ACTIVE transition was rejected");
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kCodecTransitionRejected);
        return nullptr;
    }

    std::unique_ptr<
        RawReserveActiveActivationReceiptV1>
        activation;
    try {
        activation = std::unique_ptr<
            RawReserveActiveActivationReceiptV1>(
            new RawReserveActiveActivationReceiptV1(
                key,
                before.entries[entry_index]
                    .writer_instance,
                MakeToken(
                    candidate,
                    candidate.entries[entry_index]),
                std::move(*target)));
    } catch (...) {
        SetError(
            error,
            "cannot allocate fresh ACTIVE activation receipt");
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kAllocationFailure);
        return nullptr;
    }

    ReserveStateV1Error publish_codec_error =
        ReserveStateV1Error::kNone;
    if (state_file_->PublishNext(
            candidate,
            &publish_codec_error,
            error) !=
        RawReserveStatePosixError::kNone) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kStatePublishFailure);
        return nullptr;
    }
    return activation;
}

std::unique_ptr<RawReserveActiveActivationReceiptV1>
RawReserveRegistryCoordinatorV1::PublishRecoveredActive(
    std::unique_ptr<
        RecoveryMaintenanceReportReceiptV1>&&
        supplied_receipt,
    RawReserveCoordinatorErrorV1* failure,
    std::string* error) noexcept {
    SetFailure(
        failure, RawReserveCoordinatorErrorV1::kNone);
    SetError(error, {});
    std::unique_ptr<RecoveryMaintenanceReportReceiptV1>
        receipt = std::move(supplied_receipt);
    if (receipt == nullptr || receipt->consumed_ ||
        receipt->filename_.empty()) {
        SetError(
            error,
            "recovery maintenance report receipt is absent or consumed");
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::kInvalidArgument);
        return nullptr;
    }
    receipt->consumed_ = true;

    RawReserveCoordinatorGateError gate_failure =
        RawReserveCoordinatorGateError::kNone;
    auto transition_guard =
        AcquireRawReserveCoordinatorTransitionGuardAtV1(
            lease_->root_directory_descriptor(),
            lease_->anchor(),
            &gate_failure,
            error);
    if (transition_guard == nullptr) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kTransitionGateFailure);
        return nullptr;
    }

    std::lock_guard<std::mutex> state_lock(
        state_mutex_);
    std::size_t entry_index = 0U;
    if (!ReloadAndFind(
            receipt->key_,
            ReserveRegistryStatusV1::kRecovering,
            &entry_index,
            error)) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kActionGenerationChanged);
        return nullptr;
    }
    const ReserveCoordinatorStateV1& state =
        state_file_->state();
    if (state.selected_slot >= state.slots.size()) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::kStateFailure);
        return nullptr;
    }
    const ReserveStateSlotV1& before =
        state.slots[state.selected_slot];
    if (entry_index >= before.entry_count ||
        MakeToken(
            before,
            before.entries[entry_index]) !=
            receipt->token_ ||
        before.entries[entry_index].writer_instance !=
            receipt->writer_instance_ ||
        before.entries[entry_index].recovery_intent !=
            ReserveRecoveryIntentV1::kResumeConnect) {
        SetError(
            error,
            "recovery maintenance report receipt generation or writer is stale");
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kActionGenerationChanged);
        return nullptr;
    }

    const RawReserveMutationTargetAnchorV1& expected =
        receipt->target_;
    OpenedTarget latest{};
    if (expected.source_stream_id() !=
            receipt->key_.route.source_stream_id ||
        expected.capture_date() !=
            receipt->key_.route.capture_date ||
        expected.mount_identity_sha256() !=
            lease_->anchor().marker.mount_identity_sha256 ||
        !OpenExistingTarget(
            lease_->root_directory_descriptor(),
            receipt->key_,
            expected.stream_slug(),
            &latest,
            error)) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kTargetIdentityChanged);
        return nullptr;
    }
    if (static_cast<std::uint64_t>(
            latest.root_status.st_dev) !=
            expected.raw_root_device() ||
        static_cast<std::uint64_t>(
            latest.root_status.st_ino) !=
            expected.raw_root_inode() ||
        static_cast<std::uint64_t>(
            latest.route_status.st_dev) !=
            expected.route_device() ||
        static_cast<std::uint64_t>(
            latest.route_status.st_ino) !=
            expected.route_inode()) {
        SetError(
            error,
            "recovery maintenance report route target changed before activation");
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kTargetIdentityChanged);
        return nullptr;
    }

    RecoveryMaintenanceReportV1 report{};
    if (!ValidateRecoveryReportReceiptFiles(
            latest.route.get(),
            receipt->maintenance_directory_fd_,
            receipt->report_fd_,
            receipt->filename_,
            receipt->report_sha256_,
            &report,
            error)) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kTargetIdentityChanged);
        return nullptr;
    }
    const auto& open = report.open_boundary;
    const RawV1Digest& open_header_sha256 =
        open.open_variant ==
                RecoveryMaintenanceOpenVariantV1::
                    kReuseOpen
            ? open.reuse_segment.segment_header_sha256
            : open.new_segment.segment_header_sha256;
    const std::uint32_t open_segment_sequence =
        open.open_variant ==
                RecoveryMaintenanceOpenVariantV1::
                    kReuseOpen
            ? open.reuse_segment.segment_sequence
            : open.new_segment.segment_sequence;
    if (report.namespace_identity.source_stream_id !=
            receipt->key_.route.source_stream_id ||
        report.namespace_identity.capture_date !=
            receipt->key_.route.capture_date ||
        report.namespace_identity.stream_day_id !=
            receipt->key_.stream_day_id ||
        report.recovery_attempt_id !=
            receipt->key_.recovery_attempt_id ||
        report.closed_frontier.closed_entry_count !=
            receipt->closed_entry_count_ ||
        report.closed_frontier.closed_prefix_sha256 !=
            receipt->closed_prefix_sha256_ ||
        open.manifest_generation !=
            receipt->manifest_generation_ ||
        open.manifest_entry_commitment_sha256 !=
            receipt
                ->manifest_entry_commitment_sha256_ ||
        open_segment_sequence !=
            receipt->segment_sequence_ ||
        open_header_sha256 !=
            receipt->segment_header_sha256_ ||
        open.endpoint_marker.marker_bytes !=
            receipt->endpoint_marker_bytes_ ||
        open.endpoint_marker.marker_sha256 !=
            receipt->endpoint_marker_sha256_ ||
        report.final_durable_cursor !=
            receipt->control_cursor_) {
        SetError(
            error,
            "recovery maintenance report receipt facts disagree with exact report bytes");
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kIdentityMismatch);
        return nullptr;
    }

    ReserveStateSlotV1 candidate{};
    const ReserveStateV1Error codec_error =
        BuildRawReservePublishActiveV1(
            state.header,
            before,
            receipt->key_,
            &candidate);
    if (codec_error != ReserveStateV1Error::kNone ||
        entry_index >= candidate.entry_count ||
        candidate.entries[entry_index].registry_status !=
            ReserveRegistryStatusV1::kActive) {
        SetError(
            error,
            "recovered ACTIVE transition was rejected");
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kCodecTransitionRejected);
        return nullptr;
    }

    std::unique_ptr<
        RawReserveActiveActivationReceiptV1>
        activation;
    try {
        activation = std::unique_ptr<
            RawReserveActiveActivationReceiptV1>(
            new RawReserveActiveActivationReceiptV1(
                receipt->key_,
                receipt->writer_instance_,
                MakeToken(
                    candidate,
                    candidate.entries[entry_index]),
                receipt->target_));
    } catch (...) {
        SetError(
            error,
            "cannot allocate recovered ACTIVE activation receipt");
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kAllocationFailure);
        return nullptr;
    }

    ReserveStateV1Error publish_codec_error =
        ReserveStateV1Error::kNone;
    if (state_file_->PublishNext(
            candidate,
            &publish_codec_error,
            error) !=
        RawReserveStatePosixError::kNone) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kStatePublishFailure);
        return nullptr;
    }
    return activation;
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::UnregisterActive(
    std::unique_ptr<
        SealedRawCertificateReceiptV1>&& supplied_receipt,
    std::string* error) noexcept {
    std::unique_ptr<SealedRawCertificateReceiptV1>
        receipt = std::move(supplied_receipt);
    SetError(error, {});
    if (receipt == nullptr || receipt->consumed_ ||
        receipt->filename_.empty()) {
        SetError(
            error,
            "sealed Raw certificate receipt is absent or consumed");
        return RawReserveCoordinatorErrorV1::
            kInvalidArgument;
    }
    receipt->consumed_ = true;

    RawReserveCoordinatorGateError gate_failure =
        RawReserveCoordinatorGateError::kNone;
    auto transition_guard =
        AcquireRawReserveCoordinatorTransitionGuardAtV1(
            lease_->root_directory_descriptor(),
            lease_->anchor(),
            &gate_failure,
            error);
    if (transition_guard == nullptr) {
        return RawReserveCoordinatorErrorV1::
            kTransitionGateFailure;
    }

    std::lock_guard<std::mutex> state_lock(
        state_mutex_);
    std::size_t entry_index = 0U;
    if (!ReloadAndFind(
            receipt->key_,
            ReserveRegistryStatusV1::kActive,
            &entry_index,
            error)) {
        return RawReserveCoordinatorErrorV1::
            kActionGenerationChanged;
    }
    const ReserveCoordinatorStateV1& state =
        state_file_->state();
    if (state.selected_slot >= state.slots.size()) {
        SetError(
            error,
            "reserve selected slot is invalid");
        return RawReserveCoordinatorErrorV1::
            kStateFailure;
    }
    const ReserveStateSlotV1& before =
        state.slots[state.selected_slot];
    if (entry_index >= before.entry_count ||
        MakeToken(
            before,
            before.entries[entry_index]) !=
            receipt->token_) {
        SetError(
            error,
            "sealed Raw certificate receipt generation is stale");
        return RawReserveCoordinatorErrorV1::
            kActionGenerationChanged;
    }

    const RawReserveMutationTargetAnchorV1& expected =
        receipt->target_;
    OpenedTarget latest{};
    if (expected.source_stream_id() !=
            receipt->key_.route.source_stream_id ||
        expected.capture_date() !=
            receipt->key_.route.capture_date ||
        expected.mount_identity_sha256() !=
            lease_->anchor().marker.mount_identity_sha256 ||
        !OpenExistingTarget(
            lease_->root_directory_descriptor(),
            receipt->key_,
            expected.stream_slug(),
            &latest,
            error)) {
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }
    if (static_cast<std::uint64_t>(
            latest.root_status.st_dev) !=
            expected.raw_root_device() ||
        static_cast<std::uint64_t>(
            latest.root_status.st_ino) !=
            expected.raw_root_inode() ||
        static_cast<std::uint64_t>(
            latest.route_status.st_dev) !=
            expected.route_device() ||
        static_cast<std::uint64_t>(
            latest.route_status.st_ino) !=
            expected.route_inode()) {
        SetError(
            error,
            "sealed Raw certificate route target changed before unregister");
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }
    if (!ValidateCertificateReceiptFiles(
            latest.route.get(),
            receipt->maintenance_directory_fd_,
            receipt->certificate_fd_,
            receipt->filename_,
            receipt->certificate_sha256_,
            error)) {
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }

    ReserveStateSlotV1 candidate{};
    const ReserveStateV1Error codec_error =
        BuildRawReserveUnregisterActiveV1(
            state.header,
            before,
            receipt->key_,
            &candidate);
    if (codec_error != ReserveStateV1Error::kNone) {
        SetError(
            error,
            "sealed Raw certificate unregister transition was rejected");
        return RawReserveCoordinatorErrorV1::
            kCodecTransitionRejected;
    }
    ReserveStateV1Error publish_codec_error =
        ReserveStateV1Error::kNone;
    if (state_file_->PublishNext(
            candidate,
            &publish_codec_error,
            error) !=
        RawReserveStatePosixError::kNone) {
        return RawReserveCoordinatorErrorV1::
            kStatePublishFailure;
    }
    return RawReserveCoordinatorErrorV1::kNone;
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::UnregisterActive(
    std::nullptr_t,
    std::string* error) noexcept {
    SetError(
        error,
        "sealed Raw certificate receipt is absent or consumed");
    return RawReserveCoordinatorErrorV1::
        kInvalidArgument;
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
UnregisterRecoveredTerminal(
    std::unique_ptr<
        RecoveryTerminalReportReceiptV1>&&
        supplied_receipt,
    std::string* error) noexcept {
    std::unique_ptr<RecoveryTerminalReportReceiptV1>
        receipt = std::move(supplied_receipt);
    SetError(error, {});
    if (receipt == nullptr || receipt->consumed_ ||
        receipt->report_filename_.empty() ||
        receipt->sidecar_filename_.empty() ||
        (receipt->result_ !=
             RecoveryMaintenanceResultV1::kSealedRaw &&
         receipt->result_ !=
             RecoveryMaintenanceResultV1::
                 kEmptyAnchorOnly)) {
        SetError(
            error,
            "terminal recovery report receipt is absent, consumed, or has a non-terminal tag");
        return RawReserveCoordinatorErrorV1::
            kInvalidArgument;
    }
    receipt->consumed_ = true;

    RawReserveCoordinatorGateError gate_failure =
        RawReserveCoordinatorGateError::kNone;
    auto transition_guard =
        AcquireRawReserveCoordinatorTransitionGuardAtV1(
            lease_->root_directory_descriptor(),
            lease_->anchor(),
            &gate_failure,
            error);
    if (transition_guard == nullptr) {
        return RawReserveCoordinatorErrorV1::
            kTransitionGateFailure;
    }

    std::lock_guard<std::mutex> state_lock(
        state_mutex_);
    std::size_t entry_index = 0U;
    if (!ReloadAndFind(
            receipt->key_,
            ReserveRegistryStatusV1::kRecovering,
            &entry_index,
            error)) {
        return RawReserveCoordinatorErrorV1::
            kActionGenerationChanged;
    }
    const ReserveCoordinatorStateV1& state =
        state_file_->state();
    if (state.selected_slot >= state.slots.size()) {
        SetError(
            error,
            "reserve selected slot is invalid");
        return RawReserveCoordinatorErrorV1::
            kStateFailure;
    }
    const ReserveStateSlotV1& before =
        state.slots[state.selected_slot];
    if (entry_index >= before.entry_count ||
        MakeToken(
            before,
            before.entries[entry_index]) !=
            receipt->token_ ||
        before.entries[entry_index].writer_instance !=
            receipt->writer_instance_ ||
        before.entries[entry_index].recovery_intent !=
            ReserveRecoveryIntentV1::
                kRecoverSealOnly ||
        receipt->token_.recovery_attempt_id !=
            receipt->key_.recovery_attempt_id ||
        receipt->report_model_.recovery_attempt_id !=
            receipt->key_.recovery_attempt_id ||
        receipt->report_model_.result !=
            receipt->result_ ||
        receipt->report_model_.intent !=
            RecoveryMaintenanceIntentV1::
                kRecoverSealOnly) {
        SetError(
            error,
            "terminal recovery report receipt generation, attempt, writer, or intent is stale");
        return RawReserveCoordinatorErrorV1::
            kActionGenerationChanged;
    }

    const RawReserveMutationTargetAnchorV1& expected =
        receipt->target_;
    OpenedTarget latest{};
    if (expected.source_stream_id() !=
            receipt->key_.route.source_stream_id ||
        expected.capture_date() !=
            receipt->key_.route.capture_date ||
        expected.mount_identity_sha256() !=
            lease_->anchor().marker.mount_identity_sha256 ||
        !OpenExistingTarget(
            lease_->root_directory_descriptor(),
            receipt->key_,
            expected.stream_slug(),
            &latest,
            error)) {
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }
    if (static_cast<std::uint64_t>(
            latest.root_status.st_dev) !=
            expected.raw_root_device() ||
        static_cast<std::uint64_t>(
            latest.root_status.st_ino) !=
            expected.raw_root_inode() ||
        static_cast<std::uint64_t>(
            latest.route_status.st_dev) !=
            expected.route_device() ||
        static_cast<std::uint64_t>(
            latest.route_status.st_ino) !=
            expected.route_inode()) {
        SetError(
            error,
            "terminal recovery route target changed before unregister");
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }

    if (!ValidateTerminalReceiptFiles(
            latest.route.get(),
            receipt->maintenance_directory_fd_,
            receipt->journal_fd_,
            receipt->sidecar_fd_,
            receipt->report_fd_,
            receipt->result_,
            receipt->report_filename_,
            receipt->report_sha256_,
            receipt->sidecar_filename_,
            receipt->sidecar_sha256_,
            receipt->journal_header_sha256_,
            receipt->report_model_,
            receipt->journal_header_bytes_,
            receipt->maintenance_device_,
            receipt->maintenance_inode_,
            receipt->journal_device_,
            receipt->journal_inode_,
            receipt->sidecar_device_,
            receipt->sidecar_inode_,
            receipt->report_device_,
            receipt->report_inode_,
            error)) {
        return RawReserveCoordinatorErrorV1::
            kTargetIdentityChanged;
    }

    ReserveStateSlotV1 candidate{};
    const ReserveStateV1Error codec_error =
        BuildRawReserveUnregisterTerminalRecoveringV1(
            state.header,
            before,
            receipt->key_,
            &candidate);
    if (codec_error != ReserveStateV1Error::kNone ||
        candidate.entry_count + 1U !=
            before.entry_count) {
        SetError(
            error,
            "terminal RECOVERING unregister transition was rejected");
        return RawReserveCoordinatorErrorV1::
            kCodecTransitionRejected;
    }

    ReserveStateV1Error publish_codec_error =
        ReserveStateV1Error::kNone;
    if (state_file_->PublishNext(
            candidate,
            &publish_codec_error,
            error) !=
        RawReserveStatePosixError::kNone) {
        return RawReserveCoordinatorErrorV1::
            kStatePublishFailure;
    }
    if (!SyncRawRoot(error)) {
        return RawReserveCoordinatorErrorV1::
            kRootSyncFailure;
    }
    return RawReserveCoordinatorErrorV1::kNone;
}

RawReserveCoordinatorErrorV1
RawReserveRegistryCoordinatorV1::
UnregisterRecoveredTerminal(
    std::nullptr_t,
    std::string* error) noexcept {
    SetError(
        error,
        "terminal recovery report receipt is absent or consumed");
    return RawReserveCoordinatorErrorV1::
        kInvalidArgument;
}

bool RawReserveRegistryCoordinatorV1::ReloadAndFind(
    const RawReserveRegistryEntryKeyV1& key,
    ReserveRegistryStatusV1 required_status,
    std::size_t* entry_index,
    std::string* error) noexcept {
    ReserveStateV1Error codec_error =
        ReserveStateV1Error::kNone;
    if (state_file_->Reload(
            &codec_error, error) !=
        RawReserveStatePosixError::kNone) {
        return false;
    }
    const ReserveCoordinatorStateV1& state =
        state_file_->state();
    if (state.selected_slot >= state.slots.size()) {
        SetError(error, "reserve selected slot is invalid");
        return false;
    }
    const ReserveStateSlotV1& slot =
        state.slots[state.selected_slot];
    if (slot.coordinator_state !=
        ReserveCoordinatorPhaseV1::kProvisioned) {
        SetError(
            error,
            "normal registry action requires PROVISIONED state");
        return false;
    }
    for (std::size_t index = 0U;
         index < slot.entry_count;
         ++index) {
        const ReserveStateEntryV1& entry =
            slot.entries[index];
        if (entry.source_stream_id ==
                key.route.source_stream_id &&
            entry.capture_date ==
                key.route.capture_date) {
            if (!EntryMatchesKey(entry, key)) {
                SetError(
                    error,
                    "reserve route identity does not match requested key");
                return false;
            }
            if (entry.registry_status !=
                required_status) {
                SetError(
                    error,
                    "reserve route registry status does not match action");
                return false;
            }
            *entry_index = index;
            return true;
        }
    }
    SetError(error, "reserve route is not registered");
    return false;
}

std::unique_ptr<RawReserveAuthorizedActionV1>
RawReserveRegistryCoordinatorV1::AcquireAction(
    const RawReserveRegistryEntryKeyV1& key,
    ReserveRegistryStatusV1 required_status,
    RawReserveCoordinatorErrorV1* failure,
    std::string* error) noexcept {
    return AcquireActionImpl(
        key,
        required_status,
        nullptr,
        failure,
        error);
}

std::unique_ptr<RawReserveAuthorizedActionV1>
RawReserveRegistryCoordinatorV1::
AcquireActionForExistingRoute(
    const RawReserveRegistryEntryKeyV1& key,
    ReserveRegistryStatusV1 required_status,
    std::string_view stream_slug,
    RawReserveCoordinatorErrorV1* failure,
    std::string* error) noexcept {
    try {
        const std::string canonical_slug(stream_slug);
        return AcquireActionImpl(
            key,
            required_status,
            &canonical_slug,
            failure,
            error);
    } catch (...) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kAllocationFailure);
        SetError(
            error,
            "cannot allocate Raw target slug");
        return nullptr;
    }
}

std::unique_ptr<RawReserveFinalizationActionV1>
RawReserveRegistryCoordinatorV1::
AcquireDebitedFinalizationAction(
    const ReserveFinalizationActionKeyV1& key,
    std::string_view stream_slug,
    RawReserveCoordinatorErrorV1* failure,
    std::string* error) noexcept {
    try {
        const std::string canonical_slug(stream_slug);
        return AcquireDebitedFinalizationActionImpl(
            key,
            &canonical_slug,
            failure,
            error);
    } catch (...) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kAllocationFailure);
        SetError(
            error,
            "cannot allocate finalization target slug");
        return nullptr;
    }
}

std::unique_ptr<RawReserveFinalizationActionV1>
RawReserveRegistryCoordinatorV1::
AcquireDebitedScaffoldingAction(
    const ReserveFinalizationActionKeyV1& key,
    RawReserveCoordinatorErrorV1* failure,
    std::string* error) noexcept {
    return AcquireDebitedFinalizationActionImpl(
        key, nullptr, failure, error);
}

std::unique_ptr<RawReserveAuthorizedActionV1>
RawReserveRegistryCoordinatorV1::AcquireActionImpl(
    const RawReserveRegistryEntryKeyV1& key,
    ReserveRegistryStatusV1 required_status,
    const std::string* stream_slug,
    RawReserveCoordinatorErrorV1* failure,
    std::string* error) noexcept {
    SetFailure(failure, RawReserveCoordinatorErrorV1::kNone);
    SetError(error, {});
    if (required_status ==
            ReserveRegistryStatusV1::kUnused ||
        (stream_slug != nullptr &&
         !IsCanonicalRawStreamRouteV1(
             key.route.source_stream_id,
             *stream_slug))) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kInvalidArgument);
        SetError(
            error,
            required_status ==
                    ReserveRegistryStatusV1::kUnused
                ? "action status cannot be UNUSED"
                : "existing action target slug is not canonical");
        return nullptr;
    }

    RawReserveCoordinatorGateError gate_error =
        RawReserveCoordinatorGateError::kNone;
    auto gate =
        AcquireUnboundRawReserveGenerationActionGateAtV1(
            lease_->root_directory_descriptor(),
            lease_->anchor(),
            &gate_error,
            error);
    if (gate == nullptr) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kActionGateFailure);
        return nullptr;
    }

    // Lock before decoding either state slot. An exclusive transition cannot
    // write while this independent shared OFD description is alive.
    std::lock_guard<std::mutex> state_lock(
        state_mutex_);
    std::size_t locked_index = 0U;
    if (!ReloadAndFind(
            key,
            required_status,
            &locked_index,
            error)) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kActionGenerationChanged);
        return nullptr;
    }
    const ReserveCoordinatorStateV1& locked_state =
        state_file_->state();
    const ReserveStateSlotV1& locked_slot =
        locked_state.slots[
            locked_state.selected_slot];
    const RawReserveGenerationActionTokenV1 token =
        MakeToken(
            locked_slot,
            locked_slot.entries[locked_index]);
    if (!BindRawReserveGenerationActionGateTokenV1(
            *gate, token, error) ||
        !ValidateRawReserveGenerationStateV1(
            *gate,
            locked_state,
            locked_index,
            error)) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kActionGenerationChanged);
        return nullptr;
    }

    OpenedTarget opened_target{};
    std::optional<
        RawReserveMutationTargetAnchorV1> target;
    if (stream_slug != nullptr) {
        if (!OpenExistingTarget(
                lease_->root_directory_descriptor(),
                key,
                *stream_slug,
                &opened_target,
                error)) {
            SetFailure(
                failure,
                errno == ENOMEM
                    ? RawReserveCoordinatorErrorV1::
                          kAllocationFailure
                    : RawReserveCoordinatorErrorV1::
                          kTargetUnavailable);
            return nullptr;
        }
        try {
            target =
                RawReserveMutationTargetAnchorV1(
                    static_cast<std::uint64_t>(
                        opened_target.root_status.st_dev),
                    static_cast<std::uint64_t>(
                        opened_target.root_status.st_ino),
                    static_cast<std::uint64_t>(
                        opened_target.route_status.st_dev),
                    static_cast<std::uint64_t>(
                        opened_target.route_status.st_ino),
                    lease_->anchor()
                        .marker.mount_identity_sha256,
                    key.route.source_stream_id,
                    key.route.capture_date,
                    *stream_slug);
        } catch (...) {
            SetFailure(
                failure,
                RawReserveCoordinatorErrorV1::
                    kAllocationFailure);
            SetError(
                error,
                "cannot allocate immutable Raw mutation target");
            return nullptr;
        }
    }

    auto* const action =
        new (std::nothrow)
            RawReserveAuthorizedActionV1(
                Retain(),
                std::move(gate),
                key,
                required_status,
                locked_slot.entries[locked_index]
                    .recovery_intent,
                locked_index,
                std::move(target),
                opened_target.route.get());
    if (action != nullptr) {
        static_cast<void>(
            opened_target.route.Release());
    }
    if (action == nullptr ||
        action->coordinator_ == nullptr) {
        delete action;
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kAllocationFailure);
        SetError(
            error,
            "cannot allocate reserve authorized action");
        return nullptr;
    }
    return std::unique_ptr<
        RawReserveAuthorizedActionV1>(action);
}

std::unique_ptr<RawReserveFinalizationActionV1>
RawReserveRegistryCoordinatorV1::
AcquireDebitedFinalizationActionImpl(
    const ReserveFinalizationActionKeyV1& key,
    const std::string* stream_slug,
    RawReserveCoordinatorErrorV1* failure,
    std::string* error) noexcept {
    SetFailure(
        failure, RawReserveCoordinatorErrorV1::kNone);
    SetError(error, {});
    if ((stream_slug != nullptr &&
         !IsCanonicalRawStreamRouteV1(
             key.grant.source_stream_id,
             *stream_slug)) ||
        key.action_id >=
            kReserveStateV1ActionCapacity ||
        key.action_kind ==
            FinalizationActionKindV1::kUnused) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kInvalidArgument);
        SetError(
            error,
            "finalization action key or target slug is invalid");
        return nullptr;
    }

    RawReserveCoordinatorGateError gate_error =
        RawReserveCoordinatorGateError::kNone;
    auto gate =
        AcquireUnboundRawReserveGenerationActionGateAtV1(
            lease_->root_directory_descriptor(),
            lease_->anchor(),
            &gate_error,
            error);
    if (gate == nullptr) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kActionGateFailure);
        return nullptr;
    }

    std::lock_guard<std::mutex> state_lock(
        state_mutex_);
    ReserveStateV1Error codec_error =
        ReserveStateV1Error::kNone;
    if (state_file_->Reload(
            &codec_error, error) !=
        RawReserveStatePosixError::kNone) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kStateFailure);
        return nullptr;
    }
    const ReserveCoordinatorStateV1& state =
        state_file_->state();
    if (state.selected_slot >= state.slots.size()) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kStateFailure);
        return nullptr;
    }
    const ReserveStateSlotV1& slot =
        state.slots[state.selected_slot];
    if (slot.coordinator_state !=
            ReserveCoordinatorPhaseV1::kConsumed ||
        slot.active_entry_index ==
            kReserveStateV1NoActiveEntry ||
        slot.active_entry_index >= slot.entry_count) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kRouteStatusMismatch);
        SetError(
            error,
            "DEBITED finalization action requires one CONSUMED ACTIVE grant");
        return nullptr;
    }
    const std::size_t entry_index =
        slot.active_entry_index;
    const ReserveStateEntryV1& entry =
        slot.entries[entry_index];
    const std::size_t action_index =
        static_cast<std::size_t>(key.action_id);
    const FinalizationActionReceiptV1& receipt =
        entry.actions[action_index];
    const FinalizationActionPlanV1& plan =
        entry.plans[action_index];
    if (!GrantMatchesKey(
            slot, entry, key.grant) ||
        entry.grant_status !=
            ReserveGrantStatusV1::kActive ||
        l2flow::common::IsZeroIdentity(
            entry.executor_or_recovery_attempt) ||
        receipt.action_id != key.action_id ||
        receipt.action_kind != key.action_kind ||
        receipt.object_plan_sha256 !=
            key.object_plan_sha256 ||
        receipt.action_state !=
            FinalizationActionStateV1::kDebited ||
        receipt.debit_generation == 0U) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kActionGenerationChanged);
        SetError(
            error,
            "requested finalization action is not the exact durable DEBITED receipt");
        return nullptr;
    }
    if ((stream_slug == nullptr) !=
        (entry.grant_flags ==
         kReserveGrantScaffoldingOnly)) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kTargetUnavailable);
        SetError(
            error,
            "grant flag and finalization target domain disagree");
        return nullptr;
    }

    ReserveStateV1Digest immutable_grant_sha256{};
    if (ComputeImmutableFinalizationGrantSha256V1(
            state.header,
            slot,
            entry_index,
            &immutable_grant_sha256) !=
        ReserveStateV1Error::kNone) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kCodecTransitionRejected);
        SetError(
            error,
            "cannot compute immutable finalization grant hash");
        return nullptr;
    }
    if (receipt.byte_cap_quanta != 0U &&
        state.header.allocation_quantum_bytes >
            std::numeric_limits<std::uint64_t>::max() /
                receipt.byte_cap_quanta) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kCodecTransitionRejected);
        SetError(
            error,
            "finalization action byte cap overflows");
        return nullptr;
    }
    const std::uint64_t byte_cap =
        state.header.allocation_quantum_bytes *
        receipt.byte_cap_quanta;

    const RawReserveGenerationActionTokenV1 token =
        MakeToken(slot, entry);
    if (!BindRawReserveGenerationActionGateTokenV1(
            *gate, token, error) ||
        !ValidateRawReserveGenerationStateV1(
            *gate, state, entry_index, error)) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kActionGenerationChanged);
        return nullptr;
    }

    ScopedFd retained_root(
        OpenDirectoryAt(
            lease_->root_directory_descriptor(),
            "."));
    struct stat expected_root {};
    struct stat retained_root_status {};
    if (retained_root.get() < 0 ||
        !PrivateOwnedDirectory(
            lease_->root_directory_descriptor(),
            &expected_root) ||
        !PrivateOwnedDirectory(
            retained_root.get(),
            &retained_root_status) ||
        !SameInode(
            expected_root,
            retained_root_status)) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kTargetIdentityChanged);
        SetError(
            error,
            "retained Raw root changed before finalization action");
        return nullptr;
    }

    OpenedTarget opened_target{};
    std::optional<
        RawReserveMutationTargetAnchorV1> target;
    if (stream_slug != nullptr) {
        RawReserveRegistryEntryKeyV1 route_key{};
        route_key.route.source_stream_id =
            key.grant.source_stream_id;
        route_key.route.capture_date =
            key.grant.capture_date;
        route_key.stream_day_id =
            key.grant.stream_day_id;
        if (!OpenExistingTarget(
                retained_root.get(),
                route_key,
                *stream_slug,
                &opened_target,
                error)) {
            SetFailure(
                failure,
                RawReserveCoordinatorErrorV1::
                    kTargetUnavailable);
            return nullptr;
        }
        try {
            target =
                RawReserveMutationTargetAnchorV1(
                    static_cast<std::uint64_t>(
                        opened_target
                            .root_status.st_dev),
                    static_cast<std::uint64_t>(
                        opened_target
                            .root_status.st_ino),
                    static_cast<std::uint64_t>(
                        opened_target
                            .route_status.st_dev),
                    static_cast<std::uint64_t>(
                        opened_target
                            .route_status.st_ino),
                    lease_->anchor().marker
                        .mount_identity_sha256,
                    key.grant.source_stream_id,
                    key.grant.capture_date,
                    *stream_slug);
        } catch (...) {
            SetFailure(
                failure,
                RawReserveCoordinatorErrorV1::
                    kAllocationFailure);
            SetError(
                error,
                "cannot allocate immutable finalization target");
            return nullptr;
        }
    }

    auto* const action =
        new (std::nothrow)
            RawReserveFinalizationActionV1(
                Retain(),
                std::move(gate),
                key,
                immutable_grant_sha256,
                plan,
                entry.grant_flags,
                byte_cap,
                receipt.inode_cap,
                receipt.debit_generation,
                entry.executor_or_recovery_attempt,
                entry_index,
                std::move(target),
                retained_root.get(),
                opened_target.route.get());
    if (action != nullptr) {
        static_cast<void>(retained_root.Release());
        static_cast<void>(
            opened_target.route.Release());
    }
    if (action == nullptr ||
        action->coordinator_ == nullptr) {
        delete action;
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kAllocationFailure);
        SetError(
            error,
            "cannot allocate finalization action capability");
        return nullptr;
    }
    return std::unique_ptr<
        RawReserveFinalizationActionV1>(action);
}

bool RawReserveRegistryCoordinatorV1::
ValidateActionLatest(
    const RawReserveAuthorizedActionV1& action,
    std::string* error) noexcept {
    std::lock_guard<std::mutex> state_lock(
        state_mutex_);
    std::size_t index = 0U;
    if (!ReloadAndFind(
            action.key_,
            action.required_status_,
            &index,
            error) ||
        index != action.entry_index_) {
        return false;
    }
    const ReserveCoordinatorStateV1& state =
        state_file_->state();
    const ReserveStateSlotV1& slot =
        state.slots[state.selected_slot];
    if (slot.entries[index].recovery_intent !=
        action.recovery_intent_) {
        return false;
    }
    return ValidateRawReserveGenerationStateV1(
               *action.gate_,
               state,
               index,
               error) &&
           ValidateTargetLatest(action, error);
}

bool RawReserveRegistryCoordinatorV1::
ValidateFinalizationActionLatest(
    const RawReserveFinalizationActionV1& action,
    std::string* error) noexcept {
    std::lock_guard<std::mutex> state_lock(
        state_mutex_);
    ReserveStateV1Error codec_error =
        ReserveStateV1Error::kNone;
    if (action.gate_ == nullptr ||
        state_file_->Reload(
            &codec_error, error) !=
            RawReserveStatePosixError::kNone) {
        return false;
    }
    const ReserveCoordinatorStateV1& state =
        state_file_->state();
    if (state.selected_slot >= state.slots.size()) {
        SetError(
            error,
            "finalization action selected slot is invalid");
        return false;
    }
    const ReserveStateSlotV1& slot =
        state.slots[state.selected_slot];
    if (slot.coordinator_state !=
            ReserveCoordinatorPhaseV1::kConsumed ||
        action.entry_index_ >= slot.entry_count ||
        slot.active_entry_index !=
            action.entry_index_ ||
        action.key_.action_id >=
            kReserveStateV1ActionCapacity) {
        SetError(
            error,
            "finalization action no longer selects the sole ACTIVE grant");
        return false;
    }
    const ReserveStateEntryV1& entry =
        slot.entries[action.entry_index_];
    const auto& receipt =
        entry.actions[action.key_.action_id];
    const auto& plan =
        entry.plans[action.key_.action_id];
    ReserveStateV1Digest immutable_grant_sha256{};
    if (!GrantMatchesKey(
            slot, entry, action.key_.grant) ||
        entry.grant_status !=
            ReserveGrantStatusV1::kActive ||
        entry.executor_or_recovery_attempt !=
            action.executor_instance_ ||
        entry.grant_flags != action.grant_flags_ ||
        receipt.action_id !=
            action.key_.action_id ||
        receipt.action_kind !=
            action.key_.action_kind ||
        receipt.object_plan_sha256 !=
            action.key_.object_plan_sha256 ||
        receipt.action_state !=
            FinalizationActionStateV1::kDebited ||
        receipt.debit_generation !=
            action.debit_generation_ ||
        receipt.inode_cap != action.inode_cap_ ||
        plan != action.plan_ ||
        (receipt.byte_cap_quanta != 0U &&
         state.header.allocation_quantum_bytes >
             std::numeric_limits<std::uint64_t>::max() /
                 receipt.byte_cap_quanta) ||
        state.header.allocation_quantum_bytes *
                receipt.byte_cap_quanta !=
            action.byte_cap_ ||
        ComputeImmutableFinalizationGrantSha256V1(
            state.header,
            slot,
            action.entry_index_,
            &immutable_grant_sha256) !=
            ReserveStateV1Error::kNone ||
        immutable_grant_sha256 !=
            action.immutable_grant_sha256_ ||
        !ValidateRawReserveGenerationStateV1(
            *action.gate_,
            state,
            action.entry_index_,
            error)) {
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "finalization action durable facts changed");
        }
        return false;
    }

    struct stat coordinator_root {};
    struct stat retained_root {};
    if (action.raw_root_directory_fd_ < 0 ||
        !PrivateOwnedDirectory(
            lease_->root_directory_descriptor(),
            &coordinator_root) ||
        !PrivateOwnedDirectory(
            action.raw_root_directory_fd_,
            &retained_root) ||
        !SameInode(
            coordinator_root, retained_root)) {
        SetError(
            error,
            "finalization action retained Raw root changed identity");
        return false;
    }
    if (action.grant_flags_ ==
        kReserveGrantScaffoldingOnly) {
        if (action.target_.has_value() ||
            action.route_directory_fd_ >= 0) {
            SetError(
                error,
                "SCAFFOLDING_ONLY action unexpectedly has a Raw route");
            return false;
        }
        return true;
    }
    if (!action.target_.has_value() ||
        action.route_directory_fd_ < 0) {
        SetError(
            error,
            "Raw finalization action lost its route target");
        return false;
    }

    const RawReserveMutationTargetAnchorV1& expected =
        action.target_.value();
    struct stat retained_route {};
    if (expected.source_stream_id() !=
            action.key_.grant.source_stream_id ||
        expected.capture_date() !=
            action.key_.grant.capture_date ||
        expected.mount_identity_sha256() !=
            lease_->anchor().marker
                .mount_identity_sha256 ||
        !PrivateOwnedDirectory(
            action.route_directory_fd_,
            &retained_route) ||
        static_cast<std::uint64_t>(
            retained_route.st_dev) !=
            expected.route_device() ||
        static_cast<std::uint64_t>(
            retained_route.st_ino) !=
            expected.route_inode()) {
        SetError(
            error,
            "Raw finalization action retained route changed identity");
        return false;
    }

    RawReserveRegistryEntryKeyV1 route_key{};
    route_key.route.source_stream_id =
        action.key_.grant.source_stream_id;
    route_key.route.capture_date =
        action.key_.grant.capture_date;
    route_key.stream_day_id =
        action.key_.grant.stream_day_id;
    OpenedTarget latest{};
    if (!OpenExistingTarget(
            action.raw_root_directory_fd_,
            route_key,
            expected.stream_slug(),
            &latest,
            error) ||
        static_cast<std::uint64_t>(
            latest.root_status.st_dev) !=
            expected.raw_root_device() ||
        static_cast<std::uint64_t>(
            latest.root_status.st_ino) !=
            expected.raw_root_inode() ||
        static_cast<std::uint64_t>(
            latest.route_status.st_dev) !=
            expected.route_device() ||
        static_cast<std::uint64_t>(
            latest.route_status.st_ino) !=
            expected.route_inode()) {
        SetError(
            error,
            "Raw finalization action canonical route changed identity");
        return false;
    }
    return true;
}

bool RawReserveRegistryCoordinatorV1::
ValidateTargetLatest(
    const RawReserveAuthorizedActionV1& action,
    std::string* error) const noexcept {
    if (!action.target_.has_value()) {
        return action.route_directory_fd_ < 0;
    }
    if (action.route_directory_fd_ < 0) {
        SetError(
            error,
            "Raw mutation target lost its retained route descriptor");
        return false;
    }

    const RawReserveMutationTargetAnchorV1& expected =
        action.target_.value();
    if (expected.source_stream_id() !=
            action.key_.route.source_stream_id ||
        expected.capture_date() !=
            action.key_.route.capture_date ||
        expected.mount_identity_sha256() !=
            lease_->anchor()
                .marker.mount_identity_sha256) {
        SetError(
            error,
            "Raw mutation target no longer matches coordinator identity");
        return false;
    }

    struct stat retained_route {};
    if (!PrivateOwnedDirectory(
            action.route_directory_fd_,
            &retained_route) ||
        static_cast<std::uint64_t>(
            retained_route.st_dev) !=
            expected.route_device() ||
        static_cast<std::uint64_t>(
            retained_route.st_ino) !=
            expected.route_inode()) {
        SetError(
            error,
            "retained Raw route target identity changed");
        return false;
    }

    OpenedTarget latest{};
    if (!OpenExistingTarget(
            lease_->root_directory_descriptor(),
            action.key_,
            expected.stream_slug(),
            &latest,
            error)) {
        return false;
    }
    if (static_cast<std::uint64_t>(
            latest.root_status.st_dev) !=
            expected.raw_root_device() ||
        static_cast<std::uint64_t>(
            latest.root_status.st_ino) !=
            expected.raw_root_inode() ||
        static_cast<std::uint64_t>(
            latest.route_status.st_dev) !=
            expected.route_device() ||
        static_cast<std::uint64_t>(
            latest.route_status.st_ino) !=
            expected.route_inode()) {
        SetError(
            error,
            "canonical Raw mutation target pathname changed identity");
        return false;
    }
    return true;
}

std::shared_ptr<RawReserveRegistryCoordinatorV1>
AttachRawReserveRegistryCoordinatorAtV1(
    int retained_raw_root_fd,
    const RawReserveCoordinatorLeaseMarkerV1&
        expected_marker,
    RawReserveCoordinatorErrorV1* failure,
    std::string* error) noexcept {
    SetFailure(failure, RawReserveCoordinatorErrorV1::kNone);
    SetError(error, {});
    RawReserveCoordinatorGateError gate_error =
        RawReserveCoordinatorGateError::kNone;
    auto lease = AcquireRawReserveCoordinatorLeaseAtV1(
        retained_raw_root_fd,
        expected_marker,
        &gate_error,
        error);
    if (lease == nullptr) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::kLeaseFailure);
        return nullptr;
    }
    RawReserveStatePosixError state_error =
        RawReserveStatePosixError::kNone;
    ReserveStateV1Error codec_error =
        ReserveStateV1Error::kNone;
    auto state = AttachRawReserveStateAtV1(
        lease->root_directory_descriptor(),
        &state_error,
        &codec_error,
        error);
    if (state == nullptr) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::kStateFailure);
        return nullptr;
    }
    if (!MarkerMatchesState(
            expected_marker, state->state())) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kIdentityMismatch);
        SetError(
            error,
            "coordinator lease pool identity does not match reserve state header");
        return nullptr;
    }
    try {
        return std::shared_ptr<
            RawReserveRegistryCoordinatorV1>(
            new RawReserveRegistryCoordinatorV1(
                std::move(lease),
                std::move(state)));
    } catch (...) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kAllocationFailure);
        SetError(
            error,
            "cannot allocate reserve registry coordinator");
        return nullptr;
    }
}

std::shared_ptr<RawReserveRegistryCoordinatorV1>
PublishFreshRawReserveRegistryCoordinatorAtV1(
    int retained_raw_root_fd,
    const RawReserveCoordinatorLeaseMarkerV1&
        expected_marker,
    const ReserveCoordinatorStateV1& bootstrap,
    RawReserveCoordinatorErrorV1* failure,
    std::string* error) noexcept {
    SetFailure(failure, RawReserveCoordinatorErrorV1::kNone);
    SetError(error, {});
    if (!MarkerMatchesState(
            expected_marker, bootstrap)) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kIdentityMismatch);
        SetError(
            error,
            "fresh state header does not match coordinator lease marker");
        return nullptr;
    }
    RawReserveCoordinatorGateError gate_error =
        RawReserveCoordinatorGateError::kNone;
    auto lease = AcquireRawReserveCoordinatorLeaseAtV1(
        retained_raw_root_fd,
        expected_marker,
        &gate_error,
        error);
    if (lease == nullptr) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::kLeaseFailure);
        return nullptr;
    }
    RawReserveStatePosixError state_error =
        RawReserveStatePosixError::kNone;
    ReserveStateV1Error codec_error =
        ReserveStateV1Error::kNone;
    auto state = PublishFreshRawReserveStateAtV1(
        lease->root_directory_descriptor(),
        bootstrap,
        &state_error,
        &codec_error,
        error);
    if (state == nullptr) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::kStateFailure);
        return nullptr;
    }
    try {
        return std::shared_ptr<
            RawReserveRegistryCoordinatorV1>(
            new RawReserveRegistryCoordinatorV1(
                std::move(lease),
                std::move(state)));
    } catch (...) {
        SetFailure(
            failure,
            RawReserveCoordinatorErrorV1::
                kAllocationFailure);
        SetError(
            error,
            "cannot allocate fresh reserve registry coordinator");
        return nullptr;
    }
}

}  // namespace l2flow::ingress
