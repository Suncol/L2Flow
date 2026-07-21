#include "l2flow/ingress/raw_reserve_state_posix.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <limits>
#include <new>
#include <span>
#include <string>
#include <utility>

#include <fcntl.h>
#include <linux/fs.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace l2flow::ingress {
namespace {

class ScopedFd final {
public:
    explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}

    ~ScopedFd() {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
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

    void Reset(int fd = -1) noexcept {
        if (fd_ >= 0) {
            static_cast<void>(::close(fd_));
        }
        fd_ = fd;
    }

private:
    int fd_ = -1;
};

struct AttachedState final {
    ReserveCoordinatorStateV1 state{};
    ReserveStateV1FileWire wire{};
};

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
    RawReserveStatePosixError* failure,
    RawReserveStatePosixError value) noexcept {
    if (failure != nullptr) {
        *failure = value;
    }
}

void SetCodecError(
    ReserveStateV1Error* codec_error,
    ReserveStateV1Error value) noexcept {
    if (codec_error != nullptr) {
        *codec_error = value;
    }
}

[[nodiscard]] bool FsyncLoop(int fd) noexcept {
    for (;;) {
        if (::fsync(fd) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool PreadAll(
    int fd,
    std::span<std::byte> bytes,
    std::uint64_t file_offset = 0U) noexcept {
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        const std::size_t request = std::min(
            bytes.size() - completed,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const std::uint64_t absolute =
            file_offset + static_cast<std::uint64_t>(completed);
        if (absolute >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
            errno = EOVERFLOW;
            return false;
        }
        const ssize_t result = ::pread(
            fd,
            bytes.data() + completed,
            request,
            static_cast<off_t>(absolute));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (result == 0) {
            errno = EIO;
            return false;
        }
        completed += static_cast<std::size_t>(result);
    }
    return true;
}

[[nodiscard]] bool PwriteAll(
    int fd,
    std::span<const std::byte> bytes,
    std::uint64_t file_offset = 0U) noexcept {
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        const std::size_t request = std::min(
            bytes.size() - completed,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const std::uint64_t absolute =
            file_offset + static_cast<std::uint64_t>(completed);
        if (absolute >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
            errno = EOVERFLOW;
            return false;
        }
        const ssize_t result = ::pwrite(
            fd,
            bytes.data() + completed,
            request,
            static_cast<off_t>(absolute));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (result == 0) {
            errno = EIO;
            return false;
        }
        completed += static_cast<std::size_t>(result);
    }
    return true;
}

[[nodiscard]] int OpenAtLoop(
    int directory_fd,
    const char* name,
    int flags,
    mode_t mode = 0U) noexcept {
    for (;;) {
        const int fd = ::openat(
            directory_fd, name, flags, mode);
        if (fd >= 0 || errno != EINTR) {
            return fd;
        }
    }
}

[[nodiscard]] bool SameNamedInode(
    int directory_fd,
    const char* name,
    int fd) noexcept {
    struct stat named {};
    struct stat opened {};
    return ::fstatat(
               directory_fd,
               name,
               &named,
               AT_SYMLINK_NOFOLLOW) == 0 &&
           ::fstat(fd, &opened) == 0 &&
           named.st_dev == opened.st_dev &&
           named.st_ino == opened.st_ino;
}

[[nodiscard]] bool ValidateDirectory(
    int directory_fd,
    std::string* error) noexcept {
    struct stat status {};
    if (directory_fd < 0 ||
        ::fstat(directory_fd, &status) != 0) {
        SetError(
            error,
            std::string(
                "cannot inspect retained reserve root: ") +
                std::strerror(errno));
        return false;
    }
    if (!S_ISDIR(status.st_mode) ||
        status.st_uid != ::geteuid() ||
        (status.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
        SetError(
            error,
            "reserve root must be a private directory owned by the service UID");
        return false;
    }
    return true;
}

[[nodiscard]] int RetainDirectory(
    int directory_fd,
    std::string* error) noexcept {
    if (!ValidateDirectory(directory_fd, error)) {
        return -1;
    }
    const int retained = OpenAtLoop(
        directory_fd,
        ".",
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC |
            O_NOATIME);
    if (retained < 0 ||
        !ValidateDirectory(retained, error)) {
        if (retained < 0) {
            SetError(
                error,
                std::string(
                    "cannot retain reserve root directory: ") +
                    std::strerror(errno));
        } else {
            static_cast<void>(::close(retained));
        }
        return -1;
    }
    struct stat supplied_status {};
    struct stat retained_status {};
    if (::fstat(directory_fd, &supplied_status) != 0 ||
        ::fstat(retained, &retained_status) != 0 ||
        supplied_status.st_dev != retained_status.st_dev ||
        supplied_status.st_ino != retained_status.st_ino) {
        SetError(
            error,
            "retained reserve root alias changed inode");
        static_cast<void>(::close(retained));
        return -1;
    }
    return retained;
}

[[nodiscard]] bool ValidateStateFd(
    int directory_fd,
    const char* name,
    int fd,
    std::string* error) noexcept {
    struct stat directory_status {};
    struct stat state_status {};
    if (::fstat(directory_fd, &directory_status) != 0 ||
        ::fstat(fd, &state_status) != 0) {
        SetError(
            error,
            std::string(
                "cannot inspect reserve state inode: ") +
                std::strerror(errno));
        return false;
    }
    const int flags = ::fcntl(fd, F_GETFL);
    if (!S_ISREG(state_status.st_mode) ||
        state_status.st_uid != ::geteuid() ||
        (state_status.st_mode & 0777U) != 0600U ||
        state_status.st_nlink != 1 ||
        state_status.st_dev != directory_status.st_dev ||
        state_status.st_size !=
            static_cast<off_t>(kReserveStateV1FileBytes) ||
        flags < 0 ||
        (flags & O_ACCMODE) != O_RDWR ||
        (flags & O_APPEND) != 0 ||
        !SameNamedInode(directory_fd, name, fd)) {
        SetError(
            error,
            "reserve state has an unsafe type, owner, mode, link count, device, size, flags, or name-to-inode binding");
        return false;
    }
    return true;
}

[[nodiscard]] int OpenExistingState(
    int directory_fd,
    const char* name,
    RawReserveStatePosixError* failure,
    std::string* error) noexcept {
    const int fd = OpenAtLoop(
        directory_fd,
        name,
        O_RDWR | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC |
            O_NOATIME);
    if (fd < 0) {
        if (errno == ENOENT) {
            SetFailure(
                failure,
                RawReserveStatePosixError::kNotFound);
            SetError(error, "reserve state is not present");
        } else {
            SetFailure(
                failure,
                RawReserveStatePosixError::kUnsafeFile);
            SetError(
                error,
                std::string(
                    "cannot securely open reserve state: ") +
                    std::strerror(errno));
        }
        return -1;
    }
    if (!ValidateStateFd(directory_fd, name, fd, error)) {
        SetFailure(
            failure,
            RawReserveStatePosixError::kUnsafeFile);
        static_cast<void>(::close(fd));
        return -1;
    }
    return fd;
}

[[nodiscard]] bool LoadAndDecode(
    int directory_fd,
    const char* name,
    int state_fd,
    AttachedState* attached,
    RawReserveStatePosixError* failure,
    ReserveStateV1Error* codec_error,
    std::string* error) noexcept {
    ReserveStateV1FileWire wire{};
    if (!PreadAll(state_fd, wire)) {
        SetFailure(
            failure,
            RawReserveStatePosixError::kReadbackFailure);
        SetError(
            error,
            std::string("cannot read reserve state: ") +
                std::strerror(errno));
        return false;
    }
    if (!ValidateStateFd(
            directory_fd, name, state_fd, error)) {
        SetFailure(
            failure,
            RawReserveStatePosixError::kUnsafeFile);
        return false;
    }
    ReserveCoordinatorStateV1 state{};
    const ReserveStateV1Error decoded =
        DecodeAndSelectReserveCoordinatorStateV1(
            wire, &state);
    SetCodecError(codec_error, decoded);
    if (decoded != ReserveStateV1Error::kNone) {
        SetFailure(
            failure,
            RawReserveStatePosixError::kStateCorruption);
        SetError(
            error,
            std::string(
                "reserve state codec rejected the published file: ") +
                std::string(ReserveStateV1ErrorName(decoded)));
        return false;
    }
    attached->state = state;
    attached->wire = wire;
    return true;
}

[[nodiscard]] bool NameExists(
    int directory_fd,
    const char* name,
    bool* exists,
    std::string* error) noexcept {
    struct stat status {};
    if (::fstatat(
            directory_fd,
            name,
            &status,
            AT_SYMLINK_NOFOLLOW) == 0) {
        *exists = true;
        return true;
    }
    if (errno == ENOENT) {
        *exists = false;
        return true;
    }
    SetError(
        error,
        std::string("cannot inspect reserve state name ") +
            name + ": " + std::strerror(errno));
    return false;
}

[[nodiscard]] int RenameNoReplace(
    int directory_fd,
    const char* old_name,
    const char* new_name) noexcept {
    return static_cast<int>(
        ::syscall(
            SYS_renameat2,
            directory_fd,
            old_name,
            directory_fd,
            new_name,
            RENAME_NOREPLACE));
}

[[nodiscard]] bool IsFreshBootstrap(
    const ReserveCoordinatorStateV1& state) noexcept {
    return state.selected_slot == 0U &&
           state.slots[0U].generation == 1U &&
           state.slots[1U].generation == 1U &&
           state.slots[0U].coordinator_state ==
               ReserveCoordinatorPhaseV1::kProvisioned &&
           state.slots[1U].coordinator_state ==
               ReserveCoordinatorPhaseV1::kProvisioned &&
           state.slots[0U] == state.slots[1U];
}

}  // namespace

std::string_view RawReserveStatePosixErrorName(
    RawReserveStatePosixError error) noexcept {
    switch (error) {
        case RawReserveStatePosixError::kNone:
            return "none";
        case RawReserveStatePosixError::kInvalidArgument:
            return "invalid_argument";
        case RawReserveStatePosixError::kUnsafeDirectory:
            return "unsafe_directory";
        case RawReserveStatePosixError::kNotFound:
            return "not_found";
        case RawReserveStatePosixError::kUnsafeFile:
            return "unsafe_file";
        case RawReserveStatePosixError::kAmbiguousTemporary:
            return "ambiguous_temporary";
        case RawReserveStatePosixError::kCodecFailure:
            return "codec_failure";
        case RawReserveStatePosixError::kStateCorruption:
            return "state_corruption";
        case RawReserveStatePosixError::kWriteFailure:
            return "write_failure";
        case RawReserveStatePosixError::kSyncFailure:
            return "sync_failure";
        case RawReserveStatePosixError::kPublishConflict:
            return "publish_conflict";
        case RawReserveStatePosixError::kReadbackFailure:
            return "readback_failure";
        case RawReserveStatePosixError::kInvalidTransition:
            return "invalid_transition";
        case RawReserveStatePosixError::kGenerationOverflow:
            return "generation_overflow";
        case RawReserveStatePosixError::kAllocationFailure:
            return "allocation_failure";
        case RawReserveStatePosixError::kPoisoned:
            return "poisoned";
    }
    return "unknown";
}

RawReserveStateFileV1::RawReserveStateFileV1(
    int directory_fd,
    int state_fd,
    ReserveCoordinatorStateV1 state,
    ReserveStateV1FileWire wire) noexcept
    : directory_fd_(directory_fd),
      state_fd_(state_fd),
      state_(std::move(state)),
      wire_(std::move(wire)) {}

RawReserveStateFileV1::~RawReserveStateFileV1() {
    if (state_fd_ >= 0) {
        static_cast<void>(::close(state_fd_));
    }
    if (directory_fd_ >= 0) {
        static_cast<void>(::close(directory_fd_));
    }
}

RawReserveStatePosixError
RawReserveStateFileV1::Reload(
    ReserveStateV1Error* codec_error,
    std::string* error) noexcept {
    SetCodecError(codec_error, ReserveStateV1Error::kNone);
    SetError(error, {});
    if (poisoned_) {
        SetError(
            error,
            "reserve state handle is poisoned and must be reattached");
        return RawReserveStatePosixError::kPoisoned;
    }

    AttachedState current{};
    RawReserveStatePosixError failure =
        RawReserveStatePosixError::kNone;
    if (!LoadAndDecode(
            directory_fd_,
            kRawReserveStateFilename,
            state_fd_,
            &current,
            &failure,
            codec_error,
            error)) {
        poisoned_ = true;
        return failure;
    }
    if (state_.selected_slot >= state_.slots.size() ||
        current.state.selected_slot >=
            current.state.slots.size()) {
        poisoned_ = true;
        SetError(
            error,
            "reserve state reload has an invalid selected slot");
        return RawReserveStatePosixError::kStateCorruption;
    }
    const std::uint64_t cached_generation =
        state_.slots[state_.selected_slot].generation;
    const std::uint64_t current_generation =
        current.state
            .slots[current.state.selected_slot]
            .generation;
    if (current.state.header != state_.header ||
        current_generation < cached_generation ||
        (current_generation == cached_generation &&
         (current.wire != wire_ ||
          current.state != state_))) {
        poisoned_ = true;
        SetError(
            error,
            current_generation < cached_generation
                ? "reserve state generation rolled back"
                : "reserve state changed without a newer generation");
        return RawReserveStatePosixError::kStateCorruption;
    }
    wire_ = std::move(current.wire);
    state_ = std::move(current.state);
    return RawReserveStatePosixError::kNone;
}

RawReserveStatePosixError
RawReserveStateFileV1::PublishNext(
    const ReserveStateSlotV1& next,
    ReserveStateV1Error* codec_error,
    std::string* error) noexcept {
    SetCodecError(codec_error, ReserveStateV1Error::kNone);
    SetError(error, {});
    if (poisoned_) {
        SetError(
            error,
            "reserve state handle is poisoned and must be reattached");
        return RawReserveStatePosixError::kPoisoned;
    }
    if (state_.selected_slot >= state_.slots.size()) {
        poisoned_ = true;
        SetError(error, "cached reserve state selection is invalid");
        return RawReserveStatePosixError::kStateCorruption;
    }

    // Never overwrite an inactive slot that became corrupt behind this
    // handle. A mutation begins only from the exact fully decoded image that
    // this retained descriptor originally accepted.
    AttachedState current{};
    RawReserveStatePosixError load_failure =
        RawReserveStatePosixError::kNone;
    if (!LoadAndDecode(
            directory_fd_,
            kRawReserveStateFilename,
            state_fd_,
            &current,
            &load_failure,
            codec_error,
            error)) {
        poisoned_ = true;
        return load_failure;
    }
    if (current.wire != wire_ ||
        current.state != state_) {
        poisoned_ = true;
        SetError(
            error,
            "reserve state changed outside the retained coordinator handle");
        return RawReserveStatePosixError::kStateCorruption;
    }

    const std::size_t selected = state_.selected_slot;
    const std::size_t target = selected == 0U ? 1U : 0U;
    const ReserveStateSlotV1& before =
        state_.slots[selected];
    const std::size_t before_offset =
        kReserveStateV1HeaderBytes +
        (selected * kReserveStateV1SlotBytes);
    ReserveStateV1SlotWire before_wire{};
    std::copy_n(
        wire_.data() + before_offset,
        kReserveStateV1SlotBytes,
        before_wire.begin());

    if (next == before) {
        SetCodecError(
            codec_error, ReserveStateV1Error::kNone);
        return RawReserveStatePosixError::kNone;
    }
    if (before.generation ==
        std::numeric_limits<std::uint64_t>::max()) {
        SetCodecError(
            codec_error,
            ReserveStateV1Error::kInvalidGeneration);
        SetError(
            error,
            "reserve state generation cannot wrap");
        return RawReserveStatePosixError::kGenerationOverflow;
    }

    ReserveStateV1SlotWire next_wire{};
    const ReserveStateV1Error encode_error =
        EncodeReserveStateSlotV1(
            state_.header, next, &next_wire);
    SetCodecError(codec_error, encode_error);
    if (encode_error != ReserveStateV1Error::kNone) {
        SetError(
            error,
            std::string(
                "next reserve state slot is invalid: ") +
                std::string(
                    ReserveStateV1ErrorName(encode_error)));
        return RawReserveStatePosixError::kInvalidTransition;
    }

    if (next.generation == before.generation) {
        SetCodecError(
            codec_error,
            ReserveStateV1Error::kInvalidGeneration);
        SetError(
            error,
            "same-generation reserve state retry is not byte-identical");
        return RawReserveStatePosixError::kInvalidTransition;
    }
    if (next.generation != before.generation + 1U) {
        SetCodecError(
            codec_error,
            ReserveStateV1Error::kInvalidGeneration);
        SetError(
            error,
            "next reserve state generation is not exactly current+1");
        return RawReserveStatePosixError::kInvalidTransition;
    }

    const ReserveStateV1Error transition_error =
        ValidateReserveStateSlotPairV1(
            state_.header,
            before,
            before_wire,
            next,
            next_wire);
    SetCodecError(codec_error, transition_error);
    if (transition_error != ReserveStateV1Error::kNone) {
        SetError(
            error,
            std::string(
                "reserve state transition was rejected: ") +
                std::string(
                    ReserveStateV1ErrorName(
                        transition_error)));
        return RawReserveStatePosixError::kInvalidTransition;
    }

    const std::uint64_t target_offset =
        static_cast<std::uint64_t>(
            kReserveStateV1HeaderBytes +
            (target * kReserveStateV1SlotBytes));
    if (!PwriteAll(state_fd_, next_wire, target_offset)) {
        poisoned_ = true;
        SetError(
            error,
            std::string(
                "cannot write next reserve state slot: ") +
                std::strerror(errno));
        return RawReserveStatePosixError::kWriteFailure;
    }
    if (!FsyncLoop(state_fd_)) {
        poisoned_ = true;
        SetError(
            error,
            std::string(
                "cannot fsync next reserve state slot: ") +
                std::strerror(errno));
        return RawReserveStatePosixError::kSyncFailure;
    }

    ReserveStateV1FileWire expected = wire_;
    const std::size_t target_index =
        kReserveStateV1HeaderBytes +
        (target * kReserveStateV1SlotBytes);
    std::copy(
        next_wire.begin(),
        next_wire.end(),
        expected.begin() +
            static_cast<std::ptrdiff_t>(target_index));

    AttachedState readback{};
    RawReserveStatePosixError readback_failure =
        RawReserveStatePosixError::kNone;
    if (!LoadAndDecode(
            directory_fd_,
            kRawReserveStateFilename,
            state_fd_,
            &readback,
            &readback_failure,
            codec_error,
            error)) {
        poisoned_ = true;
        return readback_failure;
    }
    if (readback.wire != expected ||
        readback.state.selected_slot != target ||
        readback.state.slots[target] != next) {
        poisoned_ = true;
        SetError(
            error,
            "reserve state transition readback did not match the exact published slot");
        return RawReserveStatePosixError::kReadbackFailure;
    }

    wire_ = std::move(readback.wire);
    state_ = std::move(readback.state);
    SetCodecError(codec_error, ReserveStateV1Error::kNone);
    return RawReserveStatePosixError::kNone;
}

std::unique_ptr<RawReserveStateFileV1>
AttachRawReserveStateAtV1(
    int retained_root_directory_fd,
    RawReserveStatePosixError* failure,
    ReserveStateV1Error* codec_error,
    std::string* error) noexcept {
    SetFailure(failure, RawReserveStatePosixError::kNone);
    SetCodecError(codec_error, ReserveStateV1Error::kNone);
    SetError(error, {});

    ScopedFd directory(
        RetainDirectory(
            retained_root_directory_fd, error));
    if (directory.get() < 0) {
        SetFailure(
            failure,
            RawReserveStatePosixError::kUnsafeDirectory);
        return nullptr;
    }

    RawReserveStatePosixError open_failure =
        RawReserveStatePosixError::kNone;
    ScopedFd state_fd(
        OpenExistingState(
            directory.get(),
            kRawReserveStateFilename,
            &open_failure,
            error));
    if (state_fd.get() < 0) {
        SetFailure(failure, open_failure);
        return nullptr;
    }

    AttachedState attached{};
    if (!LoadAndDecode(
            directory.get(),
            kRawReserveStateFilename,
            state_fd.get(),
            &attached,
            failure,
            codec_error,
            error)) {
        return nullptr;
    }

    auto* const object = new (std::nothrow)
        RawReserveStateFileV1(
            directory.Release(),
            state_fd.Release(),
            std::move(attached.state),
            std::move(attached.wire));
    if (object == nullptr) {
        SetFailure(
            failure,
            RawReserveStatePosixError::kAllocationFailure);
        SetError(
            error,
            "cannot allocate retained reserve state handle");
        return nullptr;
    }
    return std::unique_ptr<RawReserveStateFileV1>(object);
}

std::unique_ptr<RawReserveStateFileV1>
PublishFreshRawReserveStateAtV1(
    int retained_root_directory_fd,
    const ReserveCoordinatorStateV1& bootstrap,
    RawReserveStatePosixError* failure,
    ReserveStateV1Error* codec_error,
    std::string* error) noexcept {
    SetFailure(failure, RawReserveStatePosixError::kNone);
    SetCodecError(codec_error, ReserveStateV1Error::kNone);
    SetError(error, {});

    if (!IsFreshBootstrap(bootstrap)) {
        SetFailure(
            failure,
            RawReserveStatePosixError::kInvalidArgument);
        SetCodecError(
            codec_error,
            ReserveStateV1Error::kInvalidGeneration);
        SetError(
            error,
            "fresh reserve state must contain two byte-identical generation=1 PROVISIONED slots and select slot 0");
        return nullptr;
    }

    ReserveStateV1FileWire intended{};
    const ReserveStateV1Error encode_error =
        EncodeReserveCoordinatorStateV1(
            bootstrap, &intended);
    SetCodecError(codec_error, encode_error);
    if (encode_error != ReserveStateV1Error::kNone) {
        SetFailure(
            failure,
            RawReserveStatePosixError::kCodecFailure);
        SetError(
            error,
            std::string(
                "fresh reserve state cannot be encoded: ") +
                std::string(
                    ReserveStateV1ErrorName(encode_error)));
        return nullptr;
    }

    ScopedFd directory(
        RetainDirectory(
            retained_root_directory_fd, error));
    if (directory.get() < 0) {
        SetFailure(
            failure,
            RawReserveStatePosixError::kUnsafeDirectory);
        return nullptr;
    }

    bool final_exists = false;
    bool temporary_exists = false;
    if (!NameExists(
            directory.get(),
            kRawReserveStateFilename,
            &final_exists,
            error) ||
        !NameExists(
            directory.get(),
            kRawReserveStateTemporaryFilename,
            &temporary_exists,
            error)) {
        SetFailure(
            failure,
            RawReserveStatePosixError::kUnsafeFile);
        return nullptr;
    }
    if (final_exists && temporary_exists) {
        SetFailure(
            failure,
            RawReserveStatePosixError::kAmbiguousTemporary);
        SetError(
            error,
            "fixed reserve state and typed temporary coexist");
        return nullptr;
    }

    if (final_exists) {
        RawReserveStatePosixError open_failure =
            RawReserveStatePosixError::kNone;
        ScopedFd state_fd(
            OpenExistingState(
                directory.get(),
                kRawReserveStateFilename,
                &open_failure,
                error));
        if (state_fd.get() < 0) {
            SetFailure(failure, open_failure);
            return nullptr;
        }
        AttachedState attached{};
        if (!LoadAndDecode(
                directory.get(),
                kRawReserveStateFilename,
                state_fd.get(),
                &attached,
                failure,
                codec_error,
                error)) {
            return nullptr;
        }
        if (attached.wire != intended) {
            SetFailure(
                failure,
                RawReserveStatePosixError::kPublishConflict);
            SetError(
                error,
                "existing reserve state is not the exact requested bootstrap");
            return nullptr;
        }
        if (!FsyncLoop(state_fd.get()) ||
            !FsyncLoop(directory.get())) {
            SetFailure(
                failure,
                RawReserveStatePosixError::kSyncFailure);
            SetError(
                error,
                std::string(
                    "cannot re-establish existing reserve state durability: ") +
                    std::strerror(errno));
            return nullptr;
        }
        AttachedState readback{};
        if (!LoadAndDecode(
                directory.get(),
                kRawReserveStateFilename,
                state_fd.get(),
                &readback,
                failure,
                codec_error,
                error)) {
            return nullptr;
        }
        if (readback.wire != intended) {
            SetFailure(
                failure,
                RawReserveStatePosixError::kReadbackFailure);
            SetError(
                error,
                "existing reserve state changed during durability readback");
            return nullptr;
        }
        auto* const object = new (std::nothrow)
            RawReserveStateFileV1(
                directory.Release(),
                state_fd.Release(),
                std::move(readback.state),
                std::move(readback.wire));
        if (object == nullptr) {
            SetFailure(
                failure,
                RawReserveStatePosixError::kAllocationFailure);
            SetError(
                error,
                "cannot allocate retained reserve state handle");
            return nullptr;
        }
        SetCodecError(
            codec_error, ReserveStateV1Error::kNone);
        return std::unique_ptr<RawReserveStateFileV1>(object);
    }

    ScopedFd state_fd{};
    if (temporary_exists) {
        RawReserveStatePosixError open_failure =
            RawReserveStatePosixError::kNone;
        state_fd.Reset(
            OpenExistingState(
                directory.get(),
                kRawReserveStateTemporaryFilename,
                &open_failure,
                error));
        if (state_fd.get() < 0) {
            SetFailure(failure, open_failure);
            return nullptr;
        }
        ReserveStateV1FileWire temporary_wire{};
        if (!PreadAll(state_fd.get(), temporary_wire) ||
            temporary_wire != intended ||
            !ValidateStateFd(
                directory.get(),
                kRawReserveStateTemporaryFilename,
                state_fd.get(),
                error)) {
            SetFailure(
                failure,
                RawReserveStatePosixError::kAmbiguousTemporary);
            SetError(
                error,
                "existing reserve state temporary is partial, unsafe, or mismatching");
            return nullptr;
        }
    } else {
        state_fd.Reset(
            OpenAtLoop(
                directory.get(),
                kRawReserveStateTemporaryFilename,
                O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
                    O_NONBLOCK | O_CLOEXEC | O_NOATIME,
                0600));
        if (state_fd.get() < 0) {
            SetFailure(
                failure,
                errno == EEXIST
                    ? RawReserveStatePosixError::
                          kPublishConflict
                    : RawReserveStatePosixError::
                          kWriteFailure);
            SetError(
                error,
                std::string(
                    "cannot create reserve state temporary: ") +
                    std::strerror(errno));
            return nullptr;
        }
        if (::fchmod(state_fd.get(), 0600) != 0 ||
            !PwriteAll(state_fd.get(), intended) ||
            !ValidateStateFd(
                directory.get(),
                kRawReserveStateTemporaryFilename,
                state_fd.get(),
                error)) {
            SetFailure(
                failure,
                RawReserveStatePosixError::kWriteFailure);
            if (error == nullptr || error->empty()) {
                SetError(
                    error,
                    std::string(
                        "cannot construct complete reserve state temporary: ") +
                        std::strerror(errno));
            }
            return nullptr;
        }
    }

    if (!FsyncLoop(state_fd.get())) {
        SetFailure(
            failure,
            RawReserveStatePosixError::kSyncFailure);
        SetError(
            error,
            std::string(
                "cannot fsync reserve state temporary: ") +
                std::strerror(errno));
        return nullptr;
    }
    if (!ValidateStateFd(
            directory.get(),
            kRawReserveStateTemporaryFilename,
            state_fd.get(),
            error)) {
        SetFailure(
            failure,
            RawReserveStatePosixError::kAmbiguousTemporary);
        return nullptr;
    }
    if (RenameNoReplace(
            directory.get(),
            kRawReserveStateTemporaryFilename,
            kRawReserveStateFilename) != 0) {
        SetFailure(
            failure,
            RawReserveStatePosixError::kPublishConflict);
        SetError(
            error,
            std::string(
                "cannot publish reserve state with NOREPLACE: ") +
                std::strerror(errno));
        return nullptr;
    }
    if (!FsyncLoop(directory.get())) {
        SetFailure(
            failure,
            RawReserveStatePosixError::kSyncFailure);
        SetError(
            error,
            std::string(
                "cannot fsync reserve root after state publication: ") +
                std::strerror(errno));
        return nullptr;
    }
    if (!ValidateStateFd(
            directory.get(),
            kRawReserveStateFilename,
            state_fd.get(),
            error)) {
        SetFailure(
            failure,
            RawReserveStatePosixError::kReadbackFailure);
        return nullptr;
    }

    AttachedState attached{};
    if (!LoadAndDecode(
            directory.get(),
            kRawReserveStateFilename,
            state_fd.get(),
            &attached,
            failure,
            codec_error,
            error)) {
        return nullptr;
    }
    if (attached.wire != intended ||
        attached.state.selected_slot != 0U ||
        attached.state != bootstrap) {
        SetFailure(
            failure,
            RawReserveStatePosixError::kReadbackFailure);
        SetError(
            error,
            "published reserve state readback differs from the exact bootstrap");
        return nullptr;
    }

    auto* const object = new (std::nothrow)
        RawReserveStateFileV1(
            directory.Release(),
            state_fd.Release(),
            std::move(attached.state),
            std::move(attached.wire));
    if (object == nullptr) {
        SetFailure(
            failure,
            RawReserveStatePosixError::kAllocationFailure);
        SetError(
            error,
            "cannot allocate retained reserve state handle");
        return nullptr;
    }
    SetCodecError(codec_error, ReserveStateV1Error::kNone);
    return std::unique_ptr<RawReserveStateFileV1>(object);
}

}  // namespace l2flow::ingress
