#include "l2flow/control/control_record_posix_sink.h"

#include "l2flow/common/identity128.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
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

namespace l2flow::control {
namespace {

void SetDiagnostic(
    std::string* diagnostic,
    std::string_view message) noexcept {
    if (diagnostic == nullptr) {
        return;
    }
    try {
        diagnostic->assign(message);
    } catch (...) {
    }
}

[[nodiscard]] int OpenAtNoIntr(
    int directory_fd,
    const char* name,
    int flags,
    mode_t mode = 0U) noexcept {
    for (;;) {
        const int result = (flags & O_CREAT) != 0
            ? ::openat(directory_fd, name, flags, mode)
            : ::openat(directory_fd, name, flags);
        if (result >= 0 || errno != EINTR) {
            return result;
        }
    }
}

[[nodiscard]] bool FsyncNoIntr(int descriptor) noexcept {
    for (;;) {
        if (::fsync(descriptor) == 0) {
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
        if (result == 0L) {
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
    return left.st_dev == right.st_dev && left.st_ino == right.st_ino;
}

[[nodiscard]] bool SafeDirectory(const struct stat& status) noexcept {
    return S_ISDIR(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           (status.st_mode & static_cast<mode_t>(07777U)) ==
               static_cast<mode_t>(0700U);
}

[[nodiscard]] bool SafeRecordFile(
    const struct stat& file,
    const struct stat& directory,
    bool exact_size) noexcept {
    if (!S_ISREG(file.st_mode) || file.st_uid != ::geteuid() ||
        (file.st_mode & static_cast<mode_t>(07777U)) !=
            static_cast<mode_t>(0600U) ||
        file.st_nlink != static_cast<nlink_t>(1U) ||
        file.st_dev != directory.st_dev || file.st_size < 0) {
        return false;
    }
    const std::uint64_t size =
        static_cast<std::uint64_t>(file.st_size);
    return exact_size
        ? size == kControlRecordV1Bytes
        : size <= kControlRecordV1Bytes;
}

[[nodiscard]] bool DirectoryStillMatches(
    int directory_fd,
    const struct stat& expected) noexcept {
    struct stat actual {};
    return ::fstat(directory_fd, &actual) == 0 &&
           SafeDirectory(actual) && SameInode(actual, expected);
}

[[nodiscard]] bool NameMatchesFile(
    int directory_fd,
    const char* name,
    const struct stat& expected) noexcept {
    struct stat named {};
    return ::fstatat(
               directory_fd,
               name,
               &named,
               AT_SYMLINK_NOFOLLOW) == 0 &&
           SameInode(named, expected) &&
           named.st_size == expected.st_size &&
           named.st_mode == expected.st_mode &&
           named.st_nlink == expected.st_nlink;
}

[[nodiscard]] bool DeadlineValid(
    const ControlRecordPosixSinkV1& sink,
    std::uint64_t deadline) noexcept;

[[nodiscard]] bool ReadExactWire(
    int descriptor,
    ControlRecordWireV1* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    std::size_t completed = 0U;
    while (completed < output->size()) {
        const ssize_t result = ::pread(
            descriptor,
            output->data() + completed,
            output->size() - completed,
            static_cast<off_t>(completed));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            return false;
        }
        const std::size_t progress = static_cast<std::size_t>(result);
        if (progress > output->size() - completed) {
            return false;
        }
        completed += progress;
    }
    std::byte trailing{};
    for (;;) {
        const ssize_t result = ::pread(
            descriptor,
            &trailing,
            1U,
            static_cast<off_t>(output->size()));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        return result == 0;
    }
}

[[nodiscard]] bool WriteExactWire(
    int descriptor,
    const ControlRecordWireV1& wire) noexcept {
    std::size_t completed = 0U;
    while (completed < wire.size()) {
        const ssize_t result = ::pwrite(
            descriptor,
            wire.data() + completed,
            wire.size() - completed,
            static_cast<off_t>(completed));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            return false;
        }
        const std::size_t progress = static_cast<std::size_t>(result);
        if (progress > wire.size() - completed) {
            return false;
        }
        completed += progress;
    }
    return true;
}

enum class ExistingWireResult : std::uint8_t {
    kAbsent = 0U,
    kIdentical,
    kConflict,
    kUnsafe,
};

[[nodiscard]] ExistingWireResult InspectExistingWire(
    int directory_fd,
    const struct stat& directory,
    const std::string& name,
    const ControlRecordWireV1& expected,
    bool exact_size) noexcept {
    const int descriptor = OpenAtNoIntr(
        directory_fd,
        name.c_str(),
        O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
    if (descriptor < 0) {
        return errno == ENOENT
            ? ExistingWireResult::kAbsent
            : ExistingWireResult::kUnsafe;
    }
    struct stat before {};
    ControlRecordWireV1 observed{};
    const bool valid =
        ::fstat(descriptor, &before) == 0 &&
        SafeRecordFile(before, directory, exact_size) &&
        NameMatchesFile(directory_fd, name.c_str(), before) &&
        ReadExactWire(descriptor, &observed);
    struct stat after {};
    const bool stable = valid && ::fstat(descriptor, &after) == 0 &&
        SameInode(before, after) && before.st_size == after.st_size &&
        before.st_mtim.tv_sec == after.st_mtim.tv_sec &&
        before.st_mtim.tv_nsec == after.st_mtim.tv_nsec &&
        NameMatchesFile(directory_fd, name.c_str(), after);
    static_cast<void>(::close(descriptor));
    if (!stable) {
        return ExistingWireResult::kUnsafe;
    }
    return observed == expected
        ? ExistingWireResult::kIdentical
        : ExistingWireResult::kConflict;
}

[[nodiscard]] std::uint64_t DefaultMonotonicNow(void*) noexcept {
    const auto now = std::chrono::steady_clock::now()
        .time_since_epoch();
    const auto nanoseconds =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
    return nanoseconds <= 0
        ? 0U
        : static_cast<std::uint64_t>(nanoseconds);
}

}  // namespace

struct ControlRecordPosixSinkCreateAccessV1 final {
    [[nodiscard]] static std::unique_ptr<ControlRecordPosixSinkV1> Make(
        int directory_fd,
        const struct stat& directory,
        ControlRecordPosixSinkOptionsV1 options) {
        return std::unique_ptr<ControlRecordPosixSinkV1>(
            new ControlRecordPosixSinkV1(
                directory_fd,
                static_cast<std::uint64_t>(directory.st_dev),
                static_cast<std::uint64_t>(directory.st_ino),
                options));
    }
};

ControlRecordPosixSinkV1::ControlRecordPosixSinkV1(
    int directory_fd,
    std::uint64_t directory_device,
    std::uint64_t directory_inode,
    ControlRecordPosixSinkOptionsV1 options) noexcept
    : directory_fd_(directory_fd),
      directory_device_(directory_device),
      directory_inode_(directory_inode),
      options_(options) {}

ControlRecordPosixSinkV1::~ControlRecordPosixSinkV1() {
    if (directory_fd_ >= 0) {
        static_cast<void>(::close(directory_fd_));
    }
}

std::uint64_t ControlRecordPosixSinkV1::MonotonicNowNs()
    const noexcept {
    const ControlRecordPosixMonotonicNowV1 now =
        options_.monotonic_now == nullptr
            ? &DefaultMonotonicNow
            : options_.monotonic_now;
    return now(options_.monotonic_clock_context);
}

namespace {

bool DeadlineValid(
    const ControlRecordPosixSinkV1& sink,
    std::uint64_t deadline) noexcept {
    const std::uint64_t now = sink.MonotonicNowNs();
    return now != 0U && now <= deadline;
}

}  // namespace

bool ControlRecordV1Filename(
    const ControlRecordV1& record,
    std::string* filename) noexcept {
    if (filename == nullptr ||
        ValidateControlRecordV1(record) != ControlRecordV1Error::kNone) {
        return false;
    }
    try {
        *filename =
            "control-record-v1-d" + std::to_string(record.capture_date) +
            "-s" + std::to_string(record.source_stream_id) +
            "-n" + l2flow::common::Identity128Hex(record.stream_day_id) +
            "-i" + std::to_string(record.origin_ingress_sequence) +
            "-e" + std::to_string(record.origin_record_end_wal_pos) +
            ".bin";
        return filename->find('/') == std::string::npos;
    } catch (...) {
        return false;
    }
}

ControlRecordPublishResultV1 ControlRecordPosixSinkV1::Publish(
    const ControlRecordV1& record,
    const ControlRecordWireV1& canonical_wire,
    std::uint64_t deadline_monotonic_ns) noexcept {
    try {
        struct stat directory {};
        if (directory_fd_ < 0 ||
            ::fstat(directory_fd_, &directory) != 0 ||
            !SafeDirectory(directory) ||
            static_cast<std::uint64_t>(directory.st_dev) !=
                directory_device_ ||
            static_cast<std::uint64_t>(directory.st_ino) !=
                directory_inode_ ||
            !DeadlineValid(*this, deadline_monotonic_ns)) {
            return ControlRecordPublishResultV1::kFailure;
        }
        ControlRecordWireV1 encoded{};
        if (EncodeControlRecordV1(record, &encoded) !=
                ControlRecordV1Error::kNone ||
            encoded != canonical_wire) {
            return ControlRecordPublishResultV1::kConflict;
        }
        std::string final_name;
        if (!ControlRecordV1Filename(record, &final_name)) {
            return ControlRecordPublishResultV1::kFailure;
        }
        const std::string temporary_name = "." + final_name + ".tmp";

        ExistingWireResult final = InspectExistingWire(
            directory_fd_, directory, final_name, canonical_wire, true);
        if (final == ExistingWireResult::kIdentical) {
            return DeadlineValid(*this, deadline_monotonic_ns) &&
                    FsyncNoIntr(directory_fd_) &&
                    DeadlineValid(*this, deadline_monotonic_ns)
                ? ControlRecordPublishResultV1::kAcceptedIdentical
                : ControlRecordPublishResultV1::kFailure;
        }
        if (final == ExistingWireResult::kConflict) {
            return ControlRecordPublishResultV1::kConflict;
        }
        if (final == ExistingWireResult::kUnsafe) {
            return ControlRecordPublishResultV1::kFailure;
        }

        ExistingWireResult temporary = InspectExistingWire(
            directory_fd_, directory, temporary_name, canonical_wire, true);
        bool adopting_temporary = false;
        if (temporary == ExistingWireResult::kIdentical) {
            adopting_temporary = true;
        } else if (temporary == ExistingWireResult::kConflict) {
            return ControlRecordPublishResultV1::kConflict;
        } else if (temporary == ExistingWireResult::kUnsafe) {
            // A partial or metadata-unsafe deterministic temporary is never
            // unlinked blindly. The exclusive directory writer lock prevents
            // concurrent production mutation, but recovery still fails closed
            // so an operator can retain the crash artifact for diagnosis.
            return ControlRecordPublishResultV1::kFailure;
        }

        int temporary_fd = -1;
        struct stat temporary_status {};
        if (adopting_temporary) {
            temporary_fd = OpenAtNoIntr(
                directory_fd_,
                temporary_name.c_str(),
                O_RDWR | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
        } else {
            temporary_fd = OpenAtNoIntr(
                directory_fd_,
                temporary_name.c_str(),
                O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
                    O_NONBLOCK | O_CLOEXEC,
                0600U);
        }
        if (temporary_fd < 0 ||
            ::fchmod(temporary_fd, 0600U) != 0 ||
            ::fstat(temporary_fd, &temporary_status) != 0 ||
            !SafeRecordFile(
                temporary_status, directory, adopting_temporary) ||
            !NameMatchesFile(
                directory_fd_, temporary_name.c_str(), temporary_status) ||
            !DeadlineValid(*this, deadline_monotonic_ns)) {
            if (temporary_fd >= 0) {
                static_cast<void>(::close(temporary_fd));
            }
            return ControlRecordPublishResultV1::kFailure;
        }
        if (!adopting_temporary &&
            !WriteExactWire(temporary_fd, canonical_wire)) {
            static_cast<void>(::close(temporary_fd));
            return ControlRecordPublishResultV1::kFailure;
        }
        if (!DeadlineValid(*this, deadline_monotonic_ns) ||
            !FsyncNoIntr(temporary_fd) ||
            !DeadlineValid(*this, deadline_monotonic_ns)) {
            static_cast<void>(::close(temporary_fd));
            return ControlRecordPublishResultV1::kFailure;
        }
        struct stat synchronized {};
        ControlRecordWireV1 readback{};
        const bool synchronized_valid =
            ::fstat(temporary_fd, &synchronized) == 0 &&
            SafeRecordFile(synchronized, directory, true) &&
            SameInode(temporary_status, synchronized) &&
            NameMatchesFile(
                directory_fd_, temporary_name.c_str(), synchronized) &&
            ReadExactWire(temporary_fd, &readback) &&
            readback == canonical_wire &&
            DirectoryStillMatches(directory_fd_, directory);
        static_cast<void>(::close(temporary_fd));
        if (!synchronized_valid ||
            !DeadlineValid(*this, deadline_monotonic_ns)) {
            return ControlRecordPublishResultV1::kFailure;
        }

        if (!RenameNoReplace(
                directory_fd_,
                temporary_name.c_str(),
                final_name.c_str())) {
            if (errno != EEXIST) {
                return ControlRecordPublishResultV1::kFailure;
            }
            final = InspectExistingWire(
                directory_fd_, directory, final_name, canonical_wire, true);
            if (final == ExistingWireResult::kConflict) {
                return ControlRecordPublishResultV1::kConflict;
            }
            if (final != ExistingWireResult::kIdentical) {
                return ControlRecordPublishResultV1::kFailure;
            }
        }
        if (!DeadlineValid(*this, deadline_monotonic_ns) ||
            !FsyncNoIntr(directory_fd_) ||
            !DeadlineValid(*this, deadline_monotonic_ns) ||
            !DirectoryStillMatches(directory_fd_, directory)) {
            return ControlRecordPublishResultV1::kFailure;
        }
        final = InspectExistingWire(
            directory_fd_, directory, final_name, canonical_wire, true);
        if (final == ExistingWireResult::kConflict) {
            return ControlRecordPublishResultV1::kConflict;
        }
        return final == ExistingWireResult::kIdentical &&
                       DeadlineValid(*this, deadline_monotonic_ns)
            ? ControlRecordPublishResultV1::kPublishedNew
            : ControlRecordPublishResultV1::kFailure;
    } catch (...) {
        return ControlRecordPublishResultV1::kFailure;
    }
}

ControlRecordPosixSinkCreateErrorV1
CreateControlRecordPosixSinkV1At(
    int retained_directory_fd,
    ControlRecordPosixSinkOptionsV1 options,
    std::unique_ptr<ControlRecordPosixSinkV1>* output,
    std::string* diagnostic) noexcept {
    if (output == nullptr) {
        return ControlRecordPosixSinkCreateErrorV1::kInvalidArgument;
    }
    output->reset();
    if (retained_directory_fd < 0) {
        return ControlRecordPosixSinkCreateErrorV1::kInvalidArgument;
    }
    try {
        struct stat retained {};
        if (::fstat(retained_directory_fd, &retained) != 0 ||
            !SafeDirectory(retained)) {
            SetDiagnostic(diagnostic, "derived sink directory is unsafe");
            return ControlRecordPosixSinkCreateErrorV1::kUnsafeDirectory;
        }
        const int directory_fd = OpenAtNoIntr(
            retained_directory_fd,
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC);
        struct stat reopened {};
        if (directory_fd < 0 ||
            ::fstat(directory_fd, &reopened) != 0 ||
            !SafeDirectory(reopened) ||
            !SameInode(retained, reopened)) {
            if (directory_fd >= 0) {
                static_cast<void>(::close(directory_fd));
            }
            SetDiagnostic(diagnostic, "derived sink directory changed");
            return ControlRecordPosixSinkCreateErrorV1::kUnsafeDirectory;
        }
        if (::flock(directory_fd, LOCK_EX | LOCK_NB) != 0) {
            static_cast<void>(::close(directory_fd));
            SetDiagnostic(diagnostic, "derived sink already has a writer");
            return ControlRecordPosixSinkCreateErrorV1::kDirectoryBusy;
        }
        output->reset(
            ControlRecordPosixSinkCreateAccessV1::Make(
                directory_fd, reopened, options).release());
        SetDiagnostic(diagnostic, {});
        return ControlRecordPosixSinkCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return ControlRecordPosixSinkCreateErrorV1::kAllocationFailure;
    } catch (...) {
        return ControlRecordPosixSinkCreateErrorV1::kUnsafeDirectory;
    }
}

}  // namespace l2flow::control
