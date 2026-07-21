#include "l2flow/ingress/raw_recovery_posix.h"

#include "l2flow/ingress/raw_control_file.h"
#include "l2flow/ingress/raw_manifest_store.h"
#include "l2flow/ingress/raw_namespace.h"
#include "l2flow/ingress/raw_posix_io.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <dirent.h>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace l2flow::ingress {
namespace {

inline constexpr std::size_t kSegmentFilenameBytes = 20U;
inline constexpr std::uint64_t kMaximumJournalTailBytes =
    kRawV1DurableMarkerBytes - 1U;
inline constexpr std::uint64_t kMaximumEnumeratedEntries =
    UINT64_C(400'000);

class ScopedFd final {
public:
    explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}
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

class ScopedDirectory final {
public:
    explicit ScopedDirectory(DIR* directory = nullptr) noexcept
        : directory_(directory) {}
    ~ScopedDirectory() {
        if (directory_ != nullptr) {
            static_cast<void>(::closedir(directory_));
        }
    }

    ScopedDirectory(const ScopedDirectory&) = delete;
    ScopedDirectory& operator=(const ScopedDirectory&) = delete;

    [[nodiscard]] DIR* get() const noexcept {
        return directory_;
    }

private:
    DIR* directory_ = nullptr;
};

struct InspectedFile final {
    struct stat status {};
    RawPosixRecoveryFileIdentityV1 identity{};
};

struct OpenedSegment final {
    std::uint32_t sequence = 0U;
    ScopedFd descriptor;
    RawPosixRecoveryFileIdentityV1 identity{};
    std::shared_ptr<const std::vector<std::byte>> bytes;
};

class RetainedRecoveryRawWalIo final
    : public RawWalIo,
      public RawReserveMutationTargetProviderV1 {
public:
    RetainedRecoveryRawWalIo() noexcept = default;

    ~RetainedRecoveryRawWalIo() override {
        delegate_.reset();
        CloseOne(&stream_directory_fd_);
        CloseOne(&lease_fd_);
    }

    RetainedRecoveryRawWalIo(
        const RetainedRecoveryRawWalIo&) = delete;
    RetainedRecoveryRawWalIo& operator=(
        const RetainedRecoveryRawWalIo&) = delete;

    void Install(
        std::unique_ptr<RawWalIo> delegate,
        int lease_fd,
        int stream_directory_fd) noexcept {
        delegate_ = std::move(delegate);
        lease_fd_ = lease_fd;
        stream_directory_fd_ = stream_directory_fd;
    }

    [[nodiscard]] RawWalWriteResult WritevSome(
        RawWalFile file,
        std::uint64_t offset,
        std::span<const RawWalIoVector> vectors) noexcept override {
        return delegate_ == nullptr
                   ? RawWalWriteResult{0U, EBADF}
                   : delegate_->WritevSome(
                         file, offset, vectors);
    }

    [[nodiscard]] int Fdatasync(
        RawWalFile file) noexcept override {
        return delegate_ == nullptr
                   ? EBADF
                   : delegate_->Fdatasync(file);
    }

    [[nodiscard]] int Truncate(
        RawWalFile file,
        std::uint64_t logical_size) noexcept override {
        return delegate_ == nullptr
                   ? EBADF
                   : delegate_->Truncate(
                         file, logical_size);
    }

    [[nodiscard]] int Close(
        RawWalFile file) noexcept override {
        return delegate_ == nullptr
                   ? 0
                   : delegate_->Close(file);
    }

    [[nodiscard]] int
    RawReserveMutationTargetDirectoryDescriptorV1()
        const noexcept override {
        return stream_directory_fd_;
    }

private:
    static void CloseOne(int* fd) noexcept {
        if (fd != nullptr && *fd >= 0) {
            const int closing = std::exchange(*fd, -1);
            static_cast<void>(::close(closing));
        }
    }

    std::unique_ptr<RawWalIo> delegate_;
    int lease_fd_ = -1;
    int stream_directory_fd_ = -1;
};

void SetError(
    std::string* error,
    std::string_view message) noexcept {
    if (error == nullptr) {
        return;
    }
    try {
        error->assign(message.data(), message.size());
    } catch (...) {
    }
}

[[nodiscard]] bool SameInode(
    const struct stat& left,
    const struct stat& right) noexcept {
    return left.st_dev == right.st_dev &&
           left.st_ino == right.st_ino;
}

[[nodiscard]] RawPosixRecoveryFileIdentityV1 MakeIdentity(
    const struct stat& status) noexcept {
    return {
        static_cast<std::uint64_t>(status.st_dev),
        static_cast<std::uint64_t>(status.st_ino)};
}

[[nodiscard]] bool CheckedMultiplyAdd(
    std::uint64_t multiplier,
    std::uint64_t multiplicand,
    std::uint64_t addend,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        (multiplicand != 0U &&
         multiplier >
             (std::numeric_limits<std::uint64_t>::max() -
              addend) /
                 multiplicand)) {
        return false;
    }
    *result = multiplier * multiplicand + addend;
    return true;
}

[[nodiscard]] bool CheckedAdd(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        left >
            std::numeric_limits<std::uint64_t>::max() -
                right) {
        return false;
    }
    *result = left + right;
    return true;
}

[[nodiscard]] int OpenAtNoIntr(
    int directory_fd,
    const char* name,
    int flags) noexcept {
    for (;;) {
        const int fd = ::openat(directory_fd, name, flags);
        if (fd >= 0 || errno != EINTR) {
            return fd;
        }
    }
}

[[nodiscard]] bool InspectOpenDescription(
    int fd,
    std::string* error) noexcept {
    const int status_flags = ::fcntl(fd, F_GETFL);
    if (status_flags < 0) {
        SetError(error, "cannot inspect Raw recovery file status flags");
        return false;
    }
    if ((status_flags & O_ACCMODE) != O_RDWR ||
        (status_flags & O_APPEND) != 0 ||
        (status_flags & O_NONBLOCK) == 0 ||
        (status_flags & O_NOATIME) == 0) {
        SetError(
            error,
            "Raw recovery file must be O_RDWR|O_NONBLOCK|O_NOATIME without O_APPEND");
        return false;
    }
    const int descriptor_flags = ::fcntl(fd, F_GETFD);
    if (descriptor_flags < 0 ||
        (descriptor_flags & FD_CLOEXEC) == 0) {
        SetError(
            error,
            "Raw recovery file descriptor must be O_CLOEXEC");
        return false;
    }
    return true;
}

[[nodiscard]] bool ValidateFileStatus(
    const struct stat& status,
    std::string* error) noexcept {
    if (!S_ISREG(status.st_mode) ||
        status.st_uid != ::geteuid() ||
        status.st_nlink != static_cast<nlink_t>(1) ||
        (status.st_mode & 07777U) != 0600U ||
        status.st_size < 0) {
        SetError(
            error,
            "Raw recovery file must be a service-owned mode-0600 singly-linked regular file");
        return false;
    }
    return true;
}

[[nodiscard]] bool InspectNamedFile(
    int directory_fd,
    const char* name,
    int fd,
    InspectedFile* inspected,
    std::string* error) noexcept {
    if (fd < 0 || inspected == nullptr ||
        !InspectOpenDescription(fd, error)) {
        return false;
    }
    struct stat opened {};
    struct stat named {};
    if (::fstat(fd, &opened) != 0 ||
        ::fstatat(
            directory_fd,
            name,
            &named,
            AT_SYMLINK_NOFOLLOW) != 0) {
        SetError(
            error,
            "cannot inspect Raw recovery file and its final pathname");
        return false;
    }
    if (!ValidateFileStatus(opened, error) ||
        !ValidateFileStatus(named, error) ||
        !SameInode(opened, named)) {
        if (SameInode(opened, named)) {
            return false;
        }
        SetError(
            error,
            "Raw recovery final pathname no longer names the opened inode");
        return false;
    }
    inspected->status = opened;
    inspected->identity = MakeIdentity(opened);
    return true;
}

[[nodiscard]] bool RevalidateAfterRead(
    int directory_fd,
    const char* name,
    int fd,
    const struct stat& before,
    std::string* error) noexcept {
    InspectedFile after;
    if (!InspectNamedFile(
            directory_fd,
            name,
            fd,
            &after,
            error)) {
        return false;
    }
    if (!SameInode(before, after.status) ||
        before.st_size != after.status.st_size) {
        SetError(
            error,
            "Raw recovery file changed during its retained snapshot read");
        return false;
    }
    return true;
}

[[nodiscard]] bool PreadExact(
    int fd,
    std::uint64_t offset,
    std::span<std::byte> output,
    std::string* error) noexcept {
    std::size_t completed = 0U;
    while (completed < output.size()) {
        std::uint64_t current = 0U;
        if (!CheckedAdd(
                offset,
                static_cast<std::uint64_t>(completed),
                &current) ||
            current >
                static_cast<std::uint64_t>(
                    std::numeric_limits<off_t>::max())) {
            SetError(error, "Raw recovery readback offset exceeds off_t");
            return false;
        }
        const std::size_t request = std::min(
            output.size() - completed,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t result = ::pread(
            fd,
            output.data() + completed,
            request,
            static_cast<off_t>(current));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            SetError(error, "Raw recovery terminal readback failed");
            return false;
        }
        if (result == 0) {
            SetError(error, "Raw recovery terminal readback was truncated");
            return false;
        }
        completed += static_cast<std::size_t>(result);
    }
    return true;
}

[[nodiscard]] bool ReadExactSnapshot(
    int fd,
    const struct stat& status,
    std::shared_ptr<const std::vector<std::byte>>* output,
    std::string* error) {
    if (output == nullptr || status.st_size < 0) {
        SetError(error, "Raw recovery snapshot size is invalid");
        return false;
    }
    const std::uint64_t size =
        static_cast<std::uint64_t>(status.st_size);
    if (size >
        static_cast<std::uint64_t>(
            std::numeric_limits<std::size_t>::max())) {
        SetError(
            error,
            "Raw recovery file is too large for the process address space");
        return false;
    }
    auto bytes = std::make_shared<std::vector<std::byte>>();
    bytes->resize(static_cast<std::size_t>(size));

    std::size_t completed = 0U;
    while (completed < bytes->size()) {
        const std::size_t request = std::min(
            bytes->size() - completed,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        if (completed >
            static_cast<std::size_t>(
                std::numeric_limits<off_t>::max())) {
            SetError(
                error,
                "Raw recovery pread offset exceeds off_t");
            return false;
        }
        const ssize_t result = ::pread(
            fd,
            bytes->data() + completed,
            request,
            static_cast<off_t>(completed));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            SetError(error, "Raw recovery pread failed");
            return false;
        }
        if (result == 0) {
            SetError(
                error,
                "Raw recovery file became shorter during pread");
            return false;
        }
        completed += static_cast<std::size_t>(result);
    }
    *output = std::move(bytes);
    return true;
}

[[nodiscard]] bool FormatSegmentFilename(
    std::uint32_t sequence,
    std::array<char, kSegmentFilenameBytes + 1U>* output) noexcept {
    if (sequence == 0U ||
        sequence > 99'999'999U ||
        output == nullptr) {
        return false;
    }
    constexpr std::array<char, 8U> prefix{
        's', 'e', 'g', 'm', 'e', 'n', 't', '-'};
    constexpr std::array<char, 4U> suffix{
        '.', 'r', 'a', 'w'};
    output->fill('\0');
    std::copy(prefix.begin(), prefix.end(), output->begin());
    std::uint32_t remaining = sequence;
    for (std::size_t index = 0U; index < 8U; ++index) {
        const std::size_t position = 15U - index;
        (*output)[position] = static_cast<char>(
            '0' + static_cast<char>(remaining % 10U));
        remaining /= 10U;
    }
    std::copy(
        suffix.begin(),
        suffix.end(),
        output->begin() + 16);
    return remaining == 0U;
}

[[nodiscard]] bool ParseSegmentFilename(
    std::string_view name,
    std::uint32_t* sequence) noexcept {
    constexpr std::string_view prefix = "segment-";
    constexpr std::string_view suffix = ".raw";
    if (sequence == nullptr ||
        name.size() != kSegmentFilenameBytes ||
        name.substr(0U, prefix.size()) != prefix ||
        name.substr(16U, suffix.size()) != suffix) {
        return false;
    }
    std::uint32_t value = 0U;
    for (std::size_t index = 8U; index < 16U; ++index) {
        const char character = name[index];
        if (character < '0' || character > '9') {
            return false;
        }
        value =
            value * 10U +
            static_cast<std::uint32_t>(character - '0');
    }
    if (value == 0U) {
        return false;
    }
    *sequence = value;
    return true;
}

[[nodiscard]] bool ParseIndexFilename(
    std::string_view name,
    std::uint32_t* sequence) noexcept {
    constexpr std::string_view prefix = "segment-";
    constexpr std::string_view suffix = ".idx";
    if (sequence == nullptr ||
        name.size() != kSegmentFilenameBytes ||
        name.substr(0U, prefix.size()) != prefix ||
        name.substr(16U, suffix.size()) != suffix) {
        return false;
    }
    std::uint32_t value = 0U;
    for (std::size_t index = 8U; index < 16U; ++index) {
        const char character = name[index];
        if (character < '0' || character > '9') {
            return false;
        }
        value =
            value * 10U +
            static_cast<std::uint32_t>(
                character - '0');
    }
    if (value == 0U) {
        return false;
    }
    *sequence = value;
    return true;
}

[[nodiscard]] bool SameDirectory(
    int expected_fd,
    int actual_fd) noexcept {
    struct stat expected {};
    struct stat actual {};
    return ::fstat(expected_fd, &expected) == 0 &&
           ::fstat(actual_fd, &actual) == 0 &&
           S_ISDIR(expected.st_mode) &&
           S_ISDIR(actual.st_mode) &&
           SameInode(expected, actual);
}

[[nodiscard]] bool ValidateStreamDirectory(
    int directory_fd,
    std::string* error) noexcept {
    struct stat status {};
    if (directory_fd < 0 ||
        ::fstat(directory_fd, &status) != 0 ||
        !S_ISDIR(status.st_mode) ||
        status.st_uid != ::geteuid() ||
        (status.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
        SetError(
            error,
            "Raw recovery stream directory must be private and service-owned");
        return false;
    }
    return true;
}

[[nodiscard]] bool EnumerateSegmentSequences(
    int directory_fd,
    std::uint32_t maximum_segments,
    std::vector<std::uint32_t>* sequences,
    bool* dependent_artifacts_absent_proven,
    std::string* error) {
    if (sequences == nullptr ||
        dependent_artifacts_absent_proven == nullptr) {
        SetError(error, "Raw recovery sequence output is null");
        return false;
    }
    *dependent_artifacts_absent_proven = true;
    ScopedFd scan_fd(
        OpenAtNoIntr(
            directory_fd,
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC | O_NOATIME));
    if (scan_fd.get() < 0 ||
        !SameDirectory(directory_fd, scan_fd.get())) {
        SetError(
            error,
            "cannot retain Raw recovery stream directory for enumeration");
        return false;
    }
    DIR* const native = ::fdopendir(scan_fd.Release());
    if (native == nullptr) {
        SetError(
            error,
            "cannot enumerate Raw recovery stream directory");
        return false;
    }
    ScopedDirectory directory(native);

    std::uint64_t enumerated = 0U;
    for (;;) {
        errno = 0;
        struct dirent* const entry = ::readdir(directory.get());
        if (entry == nullptr) {
            if (errno != 0) {
                SetError(
                    error,
                    "Raw recovery stream directory enumeration failed");
                return false;
            }
            break;
        }
        const std::string_view name(entry->d_name);
        if (name == "." || name == "..") {
            continue;
        }
        ++enumerated;
        if (enumerated > kMaximumEnumeratedEntries) {
            SetError(
                error,
                "Raw recovery stream directory entry bound exceeded");
            return false;
        }
        if (!name.starts_with("segment-")) {
            if (name != kRawJournalFilename &&
                name != kRawWriterLeaseFilename) {
                // This is deliberately fail-closed for dependency proof.
                // Known derived artifacts (manifest/control), typed
                // publication temporaries, reports, and unknown entries all
                // prevent journal rollback even though they need not prevent
                // a read-only recovery analysis.
                *dependent_artifacts_absent_proven = false;
            }
            continue;
        }
        std::uint32_t sequence = 0U;
        if (!ParseSegmentFilename(name, &sequence)) {
            std::uint32_t index_sequence = 0U;
            if (ParseIndexFilename(
                    name, &index_sequence)) {
                *dependent_artifacts_absent_proven = false;
                continue;
            }
            SetError(
                error,
                "Raw recovery found a malformed final segment filename");
            return false;
        }
        if (sequences->size() >= maximum_segments) {
            SetError(
                error,
                "Raw recovery segment-count bound exceeded");
            return false;
        }
        sequences->push_back(sequence);
    }

    std::sort(sequences->begin(), sequences->end());
    for (std::size_t index = 0U;
         index < sequences->size();
         ++index) {
        const std::uint64_t expected =
            static_cast<std::uint64_t>(index) + 1U;
        if (expected >
                std::numeric_limits<std::uint32_t>::max() ||
            (*sequences)[index] !=
                static_cast<std::uint32_t>(expected)) {
            SetError(
                error,
                "Raw recovery final segment sequence has a duplicate or gap");
            return false;
        }
    }
    return true;
}

[[nodiscard]] int MutableFileFlagsError(int fd) noexcept {
    const int flags = ::fcntl(fd, F_GETFL);
    if (flags < 0) {
        return errno;
    }
    if ((flags & O_ACCMODE) != O_RDWR ||
        (flags & O_APPEND) != 0) {
        return EINVAL;
    }
    return 0;
}

[[nodiscard]] bool ToOffT(
    std::uint64_t value,
    off_t* converted) noexcept {
    if (converted == nullptr ||
        value >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
        return false;
    }
    *converted = static_cast<off_t>(value);
    return true;
}

[[nodiscard]] bool SameRecoveryCursor(
    const RawRecoveryCursorV1& left,
    const RawRecoveryCursorV1& right) noexcept {
    return left.segment_sequence ==
               right.segment_sequence &&
           left.global_wal_pos == right.global_wal_pos &&
           left.ingress_sequence ==
               right.ingress_sequence &&
           left.segment_offset == right.segment_offset &&
           left.marker_flags == right.marker_flags;
}

[[nodiscard]] bool SameRecoverySegmentPlan(
    const RawRecoverySegmentPlanV1& left,
    const RawRecoverySegmentPlanV1& right) noexcept {
    return left.segment_sequence ==
               right.segment_sequence &&
           left.segment_base_wal_pos ==
               right.segment_base_wal_pos &&
           left.has_accepted_marker ==
               right.has_accepted_marker &&
           left.accepted_marker_wire ==
               right.accepted_marker_wire &&
           left.durable_end_offset ==
               right.durable_end_offset &&
           left.validated_logical_end_offset ==
               right.validated_logical_end_offset &&
           left.validated_last_ingress_sequence ==
               right.validated_last_ingress_sequence &&
           left.append_only_begin_offset ==
               right.append_only_begin_offset &&
           left.append_only_end_offset ==
               right.append_only_end_offset &&
           left.tail_begin_offset ==
               right.tail_begin_offset &&
           left.tail_end_offset ==
               right.tail_end_offset &&
           left.tail == right.tail &&
           left.sealed == right.sealed;
}

[[nodiscard]] bool SameSealedRecoveryPlan(
    const RawRecoveryPlanV1& supplied,
    const RawRecoveryPlanV1& analyzed) noexcept {
    RawV1JournalHeaderWire supplied_header{};
    RawV1JournalHeaderWire analyzed_header{};
    if (!supplied.ok() || !analyzed.ok() ||
        EncodeDurableJournalHeaderV1(
            supplied.journal_header,
            &supplied_header) != RawV1Error::kNone ||
        EncodeDurableJournalHeaderV1(
            analyzed.journal_header,
            &analyzed_header) != RawV1Error::kNone ||
        supplied_header != analyzed_header ||
        supplied.accepted_journal_size !=
            analyzed.accepted_journal_size ||
        supplied.has_accepted_cursor !=
            analyzed.has_accepted_cursor ||
        (supplied.has_accepted_cursor &&
         !SameRecoveryCursor(
             supplied.accepted_cursor,
             analyzed.accepted_cursor)) ||
        supplied.journal_tail != analyzed.journal_tail ||
        supplied.initial_anchor !=
            analyzed.initial_anchor ||
        supplied.r11_orphan != analyzed.r11_orphan ||
        supplied.segments.size() !=
            analyzed.segments.size()) {
        return false;
    }
    for (std::size_t index = 0U;
         index < supplied.segments.size();
         ++index) {
        if (!SameRecoverySegmentPlan(
                supplied.segments[index],
                analyzed.segments[index])) {
            return false;
        }
    }
    return true;
}

}  // namespace

struct RawPosixRecoverySessionV1::RetainedSegment final {
    std::uint32_t sequence = 0U;
    int descriptor = -1;
    RawPosixRecoveryFileIdentityV1 identity{};
};

std::string_view
RawRecoveredSealedArtifactsFailureV1Name(
    RawRecoveredSealedArtifactsFailureV1 failure) noexcept {
    switch (failure) {
        case RawRecoveredSealedArtifactsFailureV1::kNone:
            return "none";
        case RawRecoveredSealedArtifactsFailureV1::
            kSessionUnavailable:
            return "session unavailable";
        case RawRecoveredSealedArtifactsFailureV1::
            kPlanDoesNotMatchSession:
            return "plan does not match session";
        case RawRecoveredSealedArtifactsFailureV1::
            kNotCompletelySealed:
            return "Raw stream is not completely sealed";
        case RawRecoveredSealedArtifactsFailureV1::
            kSchemaMismatch:
            return "Raw artifact schema mismatch";
        case RawRecoveredSealedArtifactsFailureV1::
            kArtifactPlanFailure:
            return "Raw artifact rebuild failed";
        case RawRecoveredSealedArtifactsFailureV1::
            kAllocationFailure:
            return "allocation failure";
    }
    return "unknown recovered sealed artifact failure";
}

RawPosixRecoverySessionV1::RawPosixRecoverySessionV1(
    int lease_fd,
    int stream_directory_fd,
    int journal_fd,
    RawPosixRecoveryFileIdentityV1 journal_identity,
    RawRecoveryInputV1 input,
    std::unique_ptr<RetainedSegment[]> segments,
    std::size_t segment_count,
    bool dependent_artifacts_absent_proven) noexcept
    : lease_fd_(lease_fd),
      stream_directory_fd_(stream_directory_fd),
      journal_fd_(journal_fd),
      journal_identity_(journal_identity),
      input_(std::move(input)),
      segments_(std::move(segments)),
      segment_count_(segment_count),
      dependent_artifacts_absent_proven_(
          dependent_artifacts_absent_proven) {}

RawPosixRecoverySessionV1::~RawPosixRecoverySessionV1() {
    for (std::size_t index = 0U;
         index < segment_count_;
         ++index) {
        if (segments_[index].descriptor >= 0) {
            static_cast<void>(
                ::close(segments_[index].descriptor));
        }
    }
    if (journal_fd_ >= 0) {
        static_cast<void>(::close(journal_fd_));
    }
    if (stream_directory_fd_ >= 0) {
        static_cast<void>(::close(stream_directory_fd_));
    }
    if (lease_fd_ >= 0) {
        static_cast<void>(::close(lease_fd_));
    }
}

std::size_t RawPosixRecoverySessionV1::segment_count()
    const noexcept {
    return segment_count_;
}

RawPosixRecoveryFileIdentityV1
RawPosixRecoverySessionV1::journal_identity() const noexcept {
    return journal_identity_;
}

bool RawPosixRecoverySessionV1::segment_identity(
    std::uint32_t segment_sequence,
    RawPosixRecoveryFileIdentityV1* identity) const noexcept {
    const RetainedSegment* const segment =
        FindSegment(segment_sequence);
    if (segment == nullptr || identity == nullptr) {
        return false;
    }
    *identity = segment->identity;
    return true;
}

int RawPosixRecoverySessionV1::journal_open_flags()
    const noexcept {
    return journal_fd_ < 0
               ? -1
               : ::fcntl(journal_fd_, F_GETFL);
}

int RawPosixRecoverySessionV1::segment_open_flags(
    std::uint32_t segment_sequence) const noexcept {
    const RetainedSegment* const segment =
        FindSegment(segment_sequence);
    return segment == nullptr
               ? -1
               : ::fcntl(segment->descriptor, F_GETFL);
}

RawRecoveredSealedArtifactsV1
RawPosixRecoverySessionV1::
PrepareRecoveredSealedRawArtifacts(
    const RawRecoveryPlanV1& plan,
    RawSegmentArtifactOptionsV1 options) const noexcept {
    RawRecoveredSealedArtifactsV1 result;
    try {
        if (lease_fd_ < 0 ||
            stream_directory_fd_ < 0 ||
            journal_fd_ < 0 ||
            segment_count_ == 0U) {
            result.failure =
                RawRecoveredSealedArtifactsFailureV1::
                    kSessionUnavailable;
            result.error_number = EBADF;
            return result;
        }
        const RawRecoveryPlanV1 analyzed =
            AnalyzeRawRecoveryV1(input_);
        if (!SameSealedRecoveryPlan(plan, analyzed)) {
            result.failure =
                RawRecoveredSealedArtifactsFailureV1::
                    kPlanDoesNotMatchSession;
            result.error_number = EINVAL;
            return result;
        }
        if (plan.journal_tail !=
                RawRecoveryJournalTailV1::kNone ||
            plan.initial_anchor !=
                RawRecoveryInitialAnchorV1::kNone ||
            plan.r11_orphan !=
                RawRecoveryR11OrphanV1::kNone ||
            !plan.has_accepted_cursor ||
            plan.accepted_cursor.marker_flags !=
                kRawV1SegmentSealed ||
            plan.segments.size() != segment_count_) {
            result.failure =
                RawRecoveredSealedArtifactsFailureV1::
                    kNotCompletelySealed;
            result.error_number = EINVAL;
            return result;
        }
        if (options.expected_raw_schema_sha256 !=
            plan.journal_header.raw_schema_sha256) {
            result.failure =
                RawRecoveredSealedArtifactsFailureV1::
                    kSchemaMismatch;
            result.error_number = EILSEQ;
            return result;
        }

        result.artifact_plans.reserve(segment_count_);
        result.existing_index_states.reserve(
            segment_count_);
        result.metadata.reserve(segment_count_);
        for (std::size_t index = 0U;
             index < segment_count_;
             ++index) {
            const RawRecoverySegmentPlanV1&
                recovered = plan.segments[index];
            const RetainedSegment& retained =
                segments_[index];
            result.evidence_segment_sequence =
                recovered.segment_sequence;
            if (recovered.segment_sequence == 0U ||
                recovered.segment_sequence !=
                    retained.sequence ||
                recovered.segment_sequence !=
                    static_cast<std::uint32_t>(
                        index + 1U) ||
                retained.descriptor < 0 ||
                !recovered.sealed ||
                !recovered.has_accepted_marker ||
                recovered.tail !=
                    RawRecoverySegmentTailV1::kNone ||
                recovered.durable_end_offset !=
                    recovered
                        .validated_logical_end_offset ||
                recovered.append_only_begin_offset !=
                    recovered.append_only_end_offset ||
                recovered.append_only_end_offset !=
                    recovered.durable_end_offset) {
                result.failure =
                    RawRecoveredSealedArtifactsFailureV1::
                        kNotCompletelySealed;
                result.error_number = EINVAL;
                result.artifact_plans.clear();
                result.existing_index_states.clear();
                result.metadata.clear();
                return result;
            }
            DurableMarkerV1 marker;
            if (DecodeDurableMarkerV1(
                    recovered.accepted_marker_wire,
                    &marker) != RawV1Error::kNone ||
                marker.marker_flags !=
                    kRawV1SegmentSealed ||
                marker.segment_sequence !=
                    recovered.segment_sequence ||
                marker.durable_segment_offset !=
                    recovered.durable_end_offset) {
                result.failure =
                    RawRecoveredSealedArtifactsFailureV1::
                        kNotCompletelySealed;
                result.error_number = EILSEQ;
                result.artifact_plans.clear();
                result.existing_index_states.clear();
                result.metadata.clear();
                return result;
            }

            RawSegmentArtifactPlanV1 artifact =
                PrepareRawSegmentArtifactPlanForPosixFdV1(
                    stream_directory_fd_,
                    retained.descriptor,
                    recovered.accepted_marker_wire,
                    options);
            if (!artifact.ok() ||
                !artifact.retained_segment_fd_bound ||
                artifact.metadata.segment
                        .segment_sequence !=
                    recovered.segment_sequence ||
                artifact.metadata
                        .accepted_sealed_marker_bytes !=
                    recovered.accepted_marker_wire ||
                artifact.metadata.logical_end_offset !=
                    recovered.durable_end_offset) {
                result.failure =
                    RawRecoveredSealedArtifactsFailureV1::
                        kArtifactPlanFailure;
                result.artifact_failure =
                    artifact.ok()
                        ? RawSegmentArtifactFailureV1::
                              kPlanInvalid
                        : artifact.failure;
                result.error_number =
                    artifact.error_number == 0
                        ? EILSEQ
                        : artifact.error_number;
                result.artifact_plans.clear();
                result.existing_index_states.clear();
                result.metadata.clear();
                return result;
            }
            const RawSegmentArtifactInspectionV1 inspection =
                InspectExistingRawSegmentArtifactAtV1(
                    stream_directory_fd_,
                    retained.descriptor,
                    artifact);
            if (!inspection.ok()) {
                result.failure =
                    RawRecoveredSealedArtifactsFailureV1::
                        kArtifactPlanFailure;
                result.artifact_failure =
                    inspection.failure;
                result.error_number =
                    inspection.error_number == 0
                        ? EILSEQ
                        : inspection.error_number;
                result.artifact_plans.clear();
                result.existing_index_states.clear();
                result.metadata.clear();
                return result;
            }
            result.metadata.push_back(
                artifact.metadata);
            result.existing_index_states.push_back(
                inspection.state);
            result.artifact_plans.push_back(
                std::move(artifact));
        }
        result.evidence_segment_sequence = 0U;
        return result;
    } catch (const std::bad_alloc&) {
        result.failure =
            RawRecoveredSealedArtifactsFailureV1::
                kAllocationFailure;
        result.error_number = ENOMEM;
        result.artifact_plans.clear();
        result.existing_index_states.clear();
        result.metadata.clear();
        return result;
    } catch (...) {
        result.failure =
            RawRecoveredSealedArtifactsFailureV1::
                kAllocationFailure;
        result.error_number = ENOMEM;
        result.artifact_plans.clear();
        result.existing_index_states.clear();
        result.metadata.clear();
        return result;
    }
}

std::unique_ptr<RawWalIo>
RawPosixRecoverySessionV1::AdoptRecoveredSealIo(
    const RawRecoveryPlanV1& plan,
    const RawRecoveryExecutionResultV1& execution,
    std::string* error) noexcept {
    SetError(error, {});
    try {
        if (!plan.ok() ||
            execution.failure !=
                RawRecoveryExecutionFailureV1::kNone ||
            !execution.cursor_publishable ||
            execution.recovered_cursor.marker_flags != 0U ||
            lease_fd_ < 0 ||
            stream_directory_fd_ < 0 ||
            journal_fd_ < 0) {
            SetError(
                error,
                "Raw recovered seal adoption requires one successful open-cursor execution");
            return nullptr;
        }

        const RawRecoverySegmentPlanV1* segment_plan =
            nullptr;
        for (const RawRecoverySegmentPlanV1& candidate :
             plan.segments) {
            if (candidate.segment_sequence ==
                execution.recovered_cursor
                    .segment_sequence) {
                segment_plan = &candidate;
                break;
            }
        }
        if (segment_plan == nullptr ||
            segment_plan->sealed ||
            execution.recovered_cursor.segment_offset !=
                segment_plan
                    ->validated_logical_end_offset ||
            execution.recovered_cursor.ingress_sequence !=
                segment_plan
                    ->validated_last_ingress_sequence) {
            SetError(
                error,
                "Raw recovered cursor does not identify the analyzed highest open segment");
            return nullptr;
        }

        std::uint64_t expected_global = 0U;
        if (!CheckedAdd(
                segment_plan->segment_base_wal_pos,
                execution.recovered_cursor
                    .segment_offset,
                &expected_global) ||
            expected_global !=
                execution.recovered_cursor
                    .global_wal_pos) {
            SetError(
                error,
                "Raw recovered cursor does not match its segment base");
            return nullptr;
        }

        const bool promotion_expected =
            plan.initial_anchor ==
                RawRecoveryInitialAnchorV1::
                    kSegmentHeaderOnly ||
            plan.r11_orphan !=
                RawRecoveryR11OrphanV1::kNone ||
            segment_plan->append_only_end_offset >
                segment_plan->append_only_begin_offset;
        std::uint64_t expected_journal_size =
            plan.accepted_journal_size;
        if (promotion_expected &&
            !CheckedAdd(
                expected_journal_size,
                kRawV1DurableMarkerBytes,
                &expected_journal_size)) {
            SetError(
                error,
                "Raw recovered journal cursor overflows");
            return nullptr;
        }
        if (execution.retained_journal_size !=
                expected_journal_size ||
            expected_journal_size <
                kRawV1JournalHeaderBytes +
                    kRawV1DurableMarkerBytes ||
            (expected_journal_size -
             kRawV1JournalHeaderBytes) %
                    kRawV1DurableMarkerBytes !=
                0U) {
            SetError(
                error,
                "Raw recovered journal size is not the analyzed terminal marker boundary");
            return nullptr;
        }

        RetainedSegment* const segment = FindSegment(
            execution.recovered_cursor.segment_sequence);
        if (segment == nullptr ||
            segment->descriptor < 0) {
            SetError(
                error,
                "Raw recovered open segment descriptor is unavailable");
            return nullptr;
        }
        std::array<char, kSegmentFilenameBytes + 1U>
            segment_filename{};
        if (!FormatSegmentFilename(
                segment->sequence,
                &segment_filename)) {
            SetError(
                error,
                "Raw recovered open segment name cannot be formatted");
            return nullptr;
        }

        InspectedFile journal_inspected;
        InspectedFile segment_inspected;
        if (!InspectNamedFile(
                stream_directory_fd_,
                kRawJournalFilename,
                journal_fd_,
                &journal_inspected,
                error) ||
            !InspectNamedFile(
                stream_directory_fd_,
                segment_filename.data(),
                segment->descriptor,
                &segment_inspected,
                error) ||
            journal_inspected.identity !=
                journal_identity_ ||
            segment_inspected.identity !=
                segment->identity) {
            if (error != nullptr && error->empty()) {
                SetError(
                    error,
                    "Raw recovered final pathname identity changed");
            }
            return nullptr;
        }

        std::uint64_t expected_segment_size =
            segment_plan->validated_logical_end_offset;
        const bool retained_preallocation =
            plan.initial_anchor ==
                RawRecoveryInitialAnchorV1::
                    kSegmentHeaderOnly ||
            (plan.r11_orphan !=
                 RawRecoveryR11OrphanV1::kNone &&
             segment_plan == &plan.segments.back());
        if (retained_preallocation) {
            expected_segment_size =
                segment_plan->tail_end_offset;
            if (expected_segment_size <
                segment_plan
                    ->validated_logical_end_offset) {
                SetError(
                    error,
                    "Raw recovered preallocation extent is invalid");
                return nullptr;
            }
        }
        if (static_cast<std::uint64_t>(
                journal_inspected.status.st_size) !=
                expected_journal_size ||
            static_cast<std::uint64_t>(
                segment_inspected.status.st_size) !=
                expected_segment_size) {
            SetError(
                error,
                "Raw recovered final file size changed after execution");
            return nullptr;
        }

        const std::size_t input_index =
            static_cast<std::size_t>(
                segment->sequence - 1U);
        if (input_index >= input_.segments.size() ||
            input_.segments[input_index].bytes == nullptr ||
            input_.segments[input_index].bytes->size() <
                kRawV1SegmentHeaderBytes ||
            input_.journal == nullptr ||
            input_.journal->size() <
                kRawV1JournalHeaderBytes) {
            SetError(
                error,
                "Raw recovered immutable header snapshot is unavailable");
            return nullptr;
        }

        RawV1SegmentHeaderWire current_segment_header{};
        RawV1JournalHeaderWire current_journal_header{};
        RawV1DurableMarkerWire terminal_marker_wire{};
        if (!PreadExact(
                segment->descriptor,
                0U,
                current_segment_header,
                error) ||
            !PreadExact(
                journal_fd_,
                0U,
                current_journal_header,
                error) ||
            !PreadExact(
                journal_fd_,
                expected_journal_size -
                    kRawV1DurableMarkerBytes,
                terminal_marker_wire,
                error) ||
            !std::equal(
                current_segment_header.begin(),
                current_segment_header.end(),
                input_.segments[input_index]
                    .bytes->begin()) ||
            !std::equal(
                current_journal_header.begin(),
                current_journal_header.end(),
                input_.journal->begin())) {
            if (error != nullptr && error->empty()) {
                SetError(
                    error,
                    "Raw recovered durable header changed after analysis");
            }
            return nullptr;
        }

        DurableMarkerV1 terminal_marker;
        if (DecodeDurableMarkerV1(
                terminal_marker_wire,
                &terminal_marker) !=
                RawV1Error::kNone ||
            terminal_marker.source_stream_id !=
                plan.journal_header.source_stream_id ||
            terminal_marker.segment_sequence !=
                execution.recovered_cursor
                    .segment_sequence ||
            terminal_marker.durable_global_wal_pos !=
                execution.recovered_cursor
                    .global_wal_pos ||
            terminal_marker.durable_ingress_sequence !=
                execution.recovered_cursor
                    .ingress_sequence ||
            terminal_marker.durable_segment_offset !=
                execution.recovered_cursor
                    .segment_offset ||
            terminal_marker.marker_flags != 0U) {
            SetError(
                error,
                "Raw recovered journal does not end in the exact open-cursor marker");
            return nullptr;
        }

        DurableMarkerV1 expected_marker;
        expected_marker.source_stream_id =
            plan.journal_header.source_stream_id;
        expected_marker.segment_sequence =
            execution.recovered_cursor
                .segment_sequence;
        expected_marker.durable_global_wal_pos =
            execution.recovered_cursor
                .global_wal_pos;
        expected_marker.durable_ingress_sequence =
            execution.recovered_cursor
                .ingress_sequence;
        expected_marker.durable_segment_offset =
            execution.recovered_cursor
                .segment_offset;
        RawV1DurableMarkerWire expected_marker_wire{};
        if (EncodeDurableMarkerV1(
                expected_marker,
                &expected_marker_wire) !=
                RawV1Error::kNone ||
            expected_marker_wire != terminal_marker_wire ||
            (!promotion_expected &&
             (!segment_plan->has_accepted_marker ||
              segment_plan->accepted_marker_wire !=
                  terminal_marker_wire))) {
            SetError(
                error,
                "Raw recovered terminal marker bytes do not match the accepted recovery evidence");
            return nullptr;
        }

        InspectedFile journal_after;
        InspectedFile segment_after;
        if (!InspectNamedFile(
                stream_directory_fd_,
                kRawJournalFilename,
                journal_fd_,
                &journal_after,
                error) ||
            !InspectNamedFile(
                stream_directory_fd_,
                segment_filename.data(),
                segment->descriptor,
                &segment_after,
                error) ||
            journal_after.identity != journal_identity_ ||
            segment_after.identity != segment->identity ||
            journal_after.status.st_size !=
                journal_inspected.status.st_size ||
            segment_after.status.st_size !=
                segment_inspected.status.st_size) {
            if (error != nullptr && error->empty()) {
                SetError(
                    error,
                    "Raw recovered final files changed during adoption readback");
            }
            return nullptr;
        }

        std::unique_ptr<RetainedRecoveryRawWalIo> retained(
            new (std::nothrow)
                RetainedRecoveryRawWalIo());
        if (retained == nullptr) {
            SetError(
                error,
                "cannot allocate retained Raw recovery seal I/O");
            return nullptr;
        }
        std::unique_ptr<RawWalIo> delegate =
            AdoptPosixRawWalIo(
                segment->descriptor,
                journal_fd_,
                error);
        if (delegate == nullptr) {
            return nullptr;
        }

        segment->descriptor = -1;
        journal_fd_ = -1;
        for (std::size_t index = 0U;
             index < segment_count_;
             ++index) {
            if (segments_[index].descriptor >= 0) {
                static_cast<void>(
                    ::close(segments_[index].descriptor));
                segments_[index].descriptor = -1;
            }
        }
        retained->Install(
            std::move(delegate),
            std::exchange(lease_fd_, -1),
            std::exchange(stream_directory_fd_, -1));
        SetError(error, {});
        return retained;
    } catch (const std::bad_alloc&) {
        SetError(
            error,
            "Raw recovered seal adoption allocation failed");
        return nullptr;
    } catch (...) {
        SetError(
            error,
            "Raw recovered seal adoption failed");
        return nullptr;
    }
}

RawPosixRecoverySessionV1::RetainedSegment*
RawPosixRecoverySessionV1::FindSegment(
    std::uint32_t segment_sequence) noexcept {
    if (segment_sequence == 0U ||
        static_cast<std::uint64_t>(segment_sequence) >
            static_cast<std::uint64_t>(segment_count_)) {
        return nullptr;
    }
    RetainedSegment* const segment =
        &segments_[
            static_cast<std::size_t>(segment_sequence - 1U)];
    return segment->sequence == segment_sequence
               ? segment
               : nullptr;
}

const RawPosixRecoverySessionV1::RetainedSegment*
RawPosixRecoverySessionV1::FindSegment(
    std::uint32_t segment_sequence) const noexcept {
    if (segment_sequence == 0U ||
        static_cast<std::uint64_t>(segment_sequence) >
            static_cast<std::uint64_t>(segment_count_)) {
        return nullptr;
    }
    const RetainedSegment* const segment =
        &segments_[
            static_cast<std::size_t>(segment_sequence - 1U)];
    return segment->sequence == segment_sequence
               ? segment
               : nullptr;
}

int RawPosixRecoverySessionV1::SyncParentDirectories() noexcept {
    if (stream_directory_fd_ < 0) {
        return EBADF;
    }
    return ::fsync(stream_directory_fd_) == 0 ? 0 : errno;
}

int RawPosixRecoverySessionV1::TruncateJournal(
    std::uint64_t size) noexcept {
    const int flags_error = MutableFileFlagsError(journal_fd_);
    if (flags_error != 0) {
        return flags_error;
    }
    off_t native_size = 0;
    if (!ToOffT(size, &native_size)) {
        return EOVERFLOW;
    }
    return ::ftruncate(journal_fd_, native_size) == 0
               ? 0
               : errno;
}

int RawPosixRecoverySessionV1::SyncJournal() noexcept {
    const int flags_error = MutableFileFlagsError(journal_fd_);
    if (flags_error != 0) {
        return flags_error;
    }
    return ::fdatasync(journal_fd_) == 0 ? 0 : errno;
}

int RawPosixRecoverySessionV1::TruncateSegment(
    std::uint32_t segment_sequence,
    std::uint64_t size) noexcept {
    RetainedSegment* const segment =
        FindSegment(segment_sequence);
    if (segment == nullptr) {
        return ENOENT;
    }
    const int flags_error =
        MutableFileFlagsError(segment->descriptor);
    if (flags_error != 0) {
        return flags_error;
    }
    off_t native_size = 0;
    if (!ToOffT(size, &native_size)) {
        return EOVERFLOW;
    }
    return ::ftruncate(segment->descriptor, native_size) == 0
               ? 0
               : errno;
}

int RawPosixRecoverySessionV1::SyncSegment(
    std::uint32_t segment_sequence,
    bool include_allocation_metadata) noexcept {
    RetainedSegment* const segment =
        FindSegment(segment_sequence);
    if (segment == nullptr) {
        return ENOENT;
    }
    const int flags_error =
        MutableFileFlagsError(segment->descriptor);
    if (flags_error != 0) {
        return flags_error;
    }
    const int result =
        include_allocation_metadata
            ? ::fsync(segment->descriptor)
            : ::fdatasync(segment->descriptor);
    return result == 0 ? 0 : errno;
}

RawRecoveryWriteResult
RawPosixRecoverySessionV1::WriteJournalSome(
    std::uint64_t offset,
    std::span<const std::byte> bytes) noexcept {
    const int flags_error = MutableFileFlagsError(journal_fd_);
    if (flags_error != 0) {
        return {0U, flags_error};
    }
    if (bytes.empty()) {
        return {};
    }
    const std::uint64_t maximum_offset =
        static_cast<std::uint64_t>(
            std::numeric_limits<off_t>::max());
    if (offset > maximum_offset) {
        return {0U, EOVERFLOW};
    }
    const std::uint64_t addressable =
        maximum_offset - offset + 1U;
    const std::uint64_t requested64 = std::min(
        {
            static_cast<std::uint64_t>(bytes.size()),
            static_cast<std::uint64_t>(
                std::numeric_limits<ssize_t>::max()),
            addressable});
    if (requested64 == 0U ||
        requested64 >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
        return {0U, EOVERFLOW};
    }
    const ssize_t result = ::pwrite(
        journal_fd_,
        bytes.data(),
        static_cast<std::size_t>(requested64),
        static_cast<off_t>(offset));
    if (result < 0) {
        return {0U, errno};
    }
    return {static_cast<std::size_t>(result), 0};
}

std::unique_ptr<RawPosixRecoverySessionV1>
LoadRawPosixRecoverySessionV1(
    const RawWriterLease& lease,
    RawPosixRecoveryLimitsV1 limits,
    std::string* error) noexcept {
    SetError(error, {});
    try {
        if (limits.max_segments >
                kRawPosixRecoveryAbsoluteMaxSegments ||
            limits.max_segment_bytes == 0U ||
            limits.max_total_segment_bytes == 0U) {
            SetError(error, "Raw recovery limits are invalid");
            return nullptr;
        }

        std::uint64_t journal_limit_without_tail = 0U;
        if (!CheckedMultiplyAdd(
                limits.max_journal_markers,
                kRawV1DurableMarkerBytes,
                kRawV1JournalHeaderBytes,
                &journal_limit_without_tail)) {
            SetError(
                error,
                "Raw recovery journal marker limit overflows");
            return nullptr;
        }
        std::uint64_t journal_limit = 0U;
        if (!CheckedAdd(
                journal_limit_without_tail,
                kMaximumJournalTailBytes,
                &journal_limit)) {
            SetError(
                error,
                "Raw recovery journal byte limit overflows");
            return nullptr;
        }

        if (lease.descriptor() < 0 ||
            lease.directory_descriptor() < 0 ||
            lease.source_stream_id() == 0U ||
            lease.capture_date() == 0U ||
            !ValidateStreamDirectory(
                lease.directory_descriptor(), error)) {
            if (error != nullptr && error->empty()) {
                SetError(
                    error,
                    "Raw recovery requires a valid retained writer lease");
            }
            return nullptr;
        }

        ScopedFd retained_lease(
            ::fcntl(
                lease.descriptor(),
                F_DUPFD_CLOEXEC,
                0));
        ScopedFd stream_directory(
            OpenAtNoIntr(
                lease.directory_descriptor(),
                ".",
                O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                    O_NONBLOCK | O_CLOEXEC |
                    O_NOATIME));
        if (retained_lease.get() < 0 ||
            stream_directory.get() < 0 ||
            !SameDirectory(
                lease.directory_descriptor(),
                stream_directory.get()) ||
            !ValidateStreamDirectory(
                stream_directory.get(), error)) {
            SetError(
                error,
                "Raw recovery cannot retain its writer lease and stream directory");
            return nullptr;
        }

        std::vector<std::uint32_t> sequences;
        sequences.reserve(
            static_cast<std::size_t>(limits.max_segments));
        bool dependent_artifacts_absent_proven = false;
        if (!EnumerateSegmentSequences(
                stream_directory.get(),
                limits.max_segments,
                &sequences,
                &dependent_artifacts_absent_proven,
                error)) {
            return nullptr;
        }

        ScopedFd journal_fd(
            OpenAtNoIntr(
                stream_directory.get(),
                kRawJournalFilename,
                O_RDWR | O_NOFOLLOW | O_NONBLOCK |
                    O_CLOEXEC | O_NOATIME));
        if (journal_fd.get() < 0) {
            SetError(
                error,
                "Raw recovery cannot secure-open final durable.journal");
            return nullptr;
        }
        InspectedFile journal_inspected;
        if (!InspectNamedFile(
                stream_directory.get(),
                kRawJournalFilename,
                journal_fd.get(),
                &journal_inspected,
                error)) {
            return nullptr;
        }
        const std::uint64_t journal_size =
            static_cast<std::uint64_t>(
                journal_inspected.status.st_size);
        if (journal_size > journal_limit) {
            SetError(
                error,
                "Raw recovery durable.journal exceeds its marker-count bound");
            return nullptr;
        }
        std::shared_ptr<const std::vector<std::byte>>
            journal_bytes;
        if (!ReadExactSnapshot(
                journal_fd.get(),
                journal_inspected.status,
                &journal_bytes,
                error) ||
            !RevalidateAfterRead(
                stream_directory.get(),
                kRawJournalFilename,
                journal_fd.get(),
                journal_inspected.status,
                error)) {
            return nullptr;
        }

        std::vector<OpenedSegment> opened_segments;
        opened_segments.reserve(sequences.size());
        RawRecoveryInputV1 input;
        input.journal = journal_bytes;
        input.segments.reserve(sequences.size());
        std::uint64_t total_segment_bytes = 0U;

        for (const std::uint32_t sequence : sequences) {
            std::array<char, kSegmentFilenameBytes + 1U>
                filename{};
            if (!FormatSegmentFilename(
                    sequence, &filename)) {
                SetError(
                    error,
                    "Raw recovery segment sequence cannot be formatted");
                return nullptr;
            }
            ScopedFd segment_fd(
                OpenAtNoIntr(
                    stream_directory.get(),
                    filename.data(),
                    O_RDWR | O_NOFOLLOW | O_NONBLOCK |
                        O_CLOEXEC | O_NOATIME));
            if (segment_fd.get() < 0) {
                SetError(
                    error,
                    "Raw recovery cannot secure-open a final segment");
                return nullptr;
            }
            InspectedFile inspected;
            if (!InspectNamedFile(
                    stream_directory.get(),
                    filename.data(),
                    segment_fd.get(),
                    &inspected,
                    error)) {
                return nullptr;
            }
            if (inspected.identity ==
                    journal_inspected.identity ||
                std::any_of(
                    opened_segments.begin(),
                    opened_segments.end(),
                    [&inspected](
                        const OpenedSegment& existing) {
                        return existing.identity ==
                               inspected.identity;
                    })) {
                SetError(
                    error,
                    "Raw recovery final names alias one retained inode");
                return nullptr;
            }
            const std::uint64_t physical_size =
                static_cast<std::uint64_t>(
                    inspected.status.st_size);
            if (physical_size >
                    limits.max_segment_bytes ||
                !CheckedAdd(
                    total_segment_bytes,
                    physical_size,
                    &total_segment_bytes) ||
                total_segment_bytes >
                    limits.max_total_segment_bytes) {
                SetError(
                    error,
                    "Raw recovery segment snapshot byte bound exceeded");
                return nullptr;
            }
            std::shared_ptr<const std::vector<std::byte>>
                segment_bytes;
            if (!ReadExactSnapshot(
                    segment_fd.get(),
                    inspected.status,
                    &segment_bytes,
                    error) ||
                !RevalidateAfterRead(
                    stream_directory.get(),
                    filename.data(),
                    segment_fd.get(),
                    inspected.status,
                    error)) {
                return nullptr;
            }
            input.segments.push_back({segment_bytes});
            OpenedSegment retained;
            retained.sequence = sequence;
            retained.descriptor = std::move(segment_fd);
            retained.identity = inspected.identity;
            retained.bytes = std::move(segment_bytes);
            opened_segments.push_back(std::move(retained));
        }

        std::unique_ptr<
            RawPosixRecoverySessionV1::RetainedSegment[]>
            retained_segments;
        if (!opened_segments.empty()) {
            retained_segments =
                std::make_unique<
                    RawPosixRecoverySessionV1::
                        RetainedSegment[]>(
                    opened_segments.size());
        }
        for (std::size_t index = 0U;
             index < opened_segments.size();
             ++index) {
            retained_segments[index].sequence =
                opened_segments[index].sequence;
            retained_segments[index].descriptor =
                opened_segments[index].descriptor.Release();
            retained_segments[index].identity =
                opened_segments[index].identity;
        }

        return std::unique_ptr<RawPosixRecoverySessionV1>(
            new RawPosixRecoverySessionV1(
                retained_lease.Release(),
                stream_directory.Release(),
                journal_fd.Release(),
                journal_inspected.identity,
                std::move(input),
                std::move(retained_segments),
                opened_segments.size(),
                dependent_artifacts_absent_proven));
    } catch (const std::bad_alloc&) {
        SetError(
            error,
            "Raw recovery snapshot allocation failed");
        return nullptr;
    } catch (...) {
        SetError(
            error,
            "Raw recovery snapshot construction failed");
        return nullptr;
    }
}

}  // namespace l2flow::ingress
