#include "l2flow/control/control_checkpoint_posix_store.h"

#include "l2flow/common/identity128.h"
#include "l2flow/control/raw_frontier_v1.h"
#include "l2flow/ingress/raw_v1.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include <dirent.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace l2flow::control {
namespace {

class ScopedFd final {
public:
    ScopedFd() noexcept = default;
    explicit ScopedFd(int descriptor) noexcept
        : descriptor_(descriptor) {}
    ~ScopedFd() {
        Reset();
    }

    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;

    ScopedFd(ScopedFd&& other) noexcept
        : descriptor_(std::exchange(other.descriptor_, -1)) {}
    ScopedFd& operator=(ScopedFd&& other) noexcept {
        if (this != &other) {
            Reset(std::exchange(other.descriptor_, -1));
        }
        return *this;
    }

    [[nodiscard]] int get() const noexcept {
        return descriptor_;
    }
    [[nodiscard]] int Release() noexcept {
        return std::exchange(descriptor_, -1);
    }
    void Reset(int replacement = -1) noexcept {
        if (descriptor_ >= 0) {
            static_cast<void>(::close(descriptor_));
        }
        descriptor_ = replacement;
    }

private:
    int descriptor_ = -1;
};

class ScopedDir final {
public:
    explicit ScopedDir(DIR* directory) noexcept
        : directory_(directory) {}
    ~ScopedDir() {
        if (directory_ != nullptr) {
            static_cast<void>(::closedir(directory_));
        }
    }

    ScopedDir(const ScopedDir&) = delete;
    ScopedDir& operator=(const ScopedDir&) = delete;

    [[nodiscard]] DIR* get() const noexcept {
        return directory_;
    }

private:
    DIR* directory_ = nullptr;
};

struct CandidateInventory final {
    bool final_present = false;
    bool temporary_present = false;
    std::uint32_t count = 0U;
};

struct LoadedCandidate final {
    ScopedFd descriptor;
    struct stat status {};
};

struct ParsedCheckpointFilename final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    l2flow::common::Identity128 stream_day_id{};
    std::uint64_t processed_ingress_sequence = 0U;
    std::uint64_t processed_record_end_wal_pos = 0U;
};

struct DiscoveredCheckpoint final {
    std::string filename;
    l2flow::common::Sha256Digest sha256{};
    std::vector<std::byte> bytes;
    ControlDecoderCheckpointV1 checkpoint{};
};

enum class FrontierValidation : std::uint8_t {
    kNone = 0U,
    kInvalidCheckpointCursor,
    kInvalidSnapshot,
    kNamespaceMismatch,
    kCheckpointPastDurable,
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

[[nodiscard]] bool CheckedAddSize(
    std::size_t left,
    std::size_t right,
    std::size_t* output) noexcept {
    if (output == nullptr ||
        right >
            std::numeric_limits<std::size_t>::max() - left) {
        return false;
    }
    *output = left + right;
    return true;
}

[[nodiscard]] int OpenAtNoIntr(
    int directory_fd,
    const char* name,
    int flags,
    mode_t mode = 0U) noexcept {
    for (;;) {
        const int result =
            (flags & O_CREAT) != 0
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

[[nodiscard]] bool LockExclusiveNoIntr(int descriptor) noexcept {
    for (;;) {
        if (::flock(descriptor, LOCK_EX) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool LockSharedNoIntr(int descriptor) noexcept {
    for (;;) {
        if (::flock(descriptor, LOCK_SH) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool UnlockNoIntr(int descriptor) noexcept {
    for (;;) {
        if (::flock(descriptor, LOCK_UN) == 0) {
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
    return left.st_dev == right.st_dev &&
           left.st_ino == right.st_ino;
}

[[nodiscard]] bool SameTimespec(
    const struct timespec& left,
    const struct timespec& right) noexcept {
    return left.tv_sec == right.tv_sec &&
           left.tv_nsec == right.tv_nsec;
}

[[nodiscard]] bool StableFileMetadata(
    const struct stat& before,
    const struct stat& after) noexcept {
    return SameInode(before, after) &&
           before.st_mode == after.st_mode &&
           before.st_uid == after.st_uid &&
           before.st_gid == after.st_gid &&
           before.st_nlink == after.st_nlink &&
           before.st_size == after.st_size &&
           SameTimespec(before.st_mtim, after.st_mtim) &&
           SameTimespec(before.st_ctim, after.st_ctim);
}

[[nodiscard]] bool IsSafeDirectory(
    const struct stat& status) noexcept {
    return S_ISDIR(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           (status.st_mode & static_cast<mode_t>(07777U)) ==
               static_cast<mode_t>(0700U);
}

[[nodiscard]] bool IsSafeFile(
    const struct stat& status,
    const struct stat& directory) noexcept {
    return S_ISREG(status.st_mode) &&
           status.st_uid == ::geteuid() &&
           (status.st_mode & static_cast<mode_t>(07777U)) ==
               static_cast<mode_t>(0600U) &&
           status.st_nlink == static_cast<nlink_t>(1U) &&
           status.st_size >= 0 &&
           static_cast<std::uint64_t>(status.st_size) <=
               kControlCheckpointV1MaximumWireBytes &&
           status.st_dev == directory.st_dev;
}

[[nodiscard]] bool DirectoryMatches(
    int directory_fd,
    const struct stat& expected) noexcept {
    struct stat actual {};
    return ::fstat(directory_fd, &actual) == 0 &&
           IsSafeDirectory(actual) &&
           SameInode(actual, expected);
}

[[nodiscard]] bool NameMatchesDescriptor(
    int directory_fd,
    const char* name,
    int descriptor,
    const struct stat* expected = nullptr) noexcept {
    struct stat opened {};
    struct stat named {};
    return ::fstat(descriptor, &opened) == 0 &&
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

[[nodiscard]] bool InspectName(
    int directory_fd,
    const char* name,
    bool* present) noexcept {
    if (present == nullptr) {
        return false;
    }
    struct stat status {};
    if (::fstatat(
            directory_fd,
            name,
            &status,
            AT_SYMLINK_NOFOLLOW) == 0) {
        *present = true;
        return true;
    }
    if (errno == ENOENT) {
        *present = false;
        return true;
    }
    return false;
}

[[nodiscard]] bool InspectCandidates(
    int directory_fd,
    const struct stat& directory_status,
    const std::string& final_name,
    const std::string& temporary_name,
    CandidateInventory* output) noexcept {
    if (output == nullptr ||
        !DirectoryMatches(directory_fd, directory_status)) {
        return false;
    }
    CandidateInventory inventory{};
    if (!InspectName(
            directory_fd,
            final_name.c_str(),
            &inventory.final_present) ||
        !InspectName(
            directory_fd,
            temporary_name.c_str(),
            &inventory.temporary_present) ||
        !DirectoryMatches(directory_fd, directory_status)) {
        return false;
    }
    inventory.count =
        static_cast<std::uint32_t>(
            (inventory.final_present ? 1U : 0U) +
            (inventory.temporary_present ? 1U : 0U));
    *output = inventory;
    return true;
}

[[nodiscard]] bool RequireCandidateState(
    int directory_fd,
    const struct stat& directory_status,
    const std::string& final_name,
    const std::string& temporary_name,
    bool final_present,
    bool temporary_present) noexcept {
    CandidateInventory inventory{};
    return InspectCandidates(
               directory_fd,
               directory_status,
               final_name,
               temporary_name,
               &inventory) &&
           inventory.final_present == final_present &&
           inventory.temporary_present == temporary_present;
}

[[nodiscard]] ControlCheckpointPosixStoreErrorV1
OpenRetainedDirectory(
    int retained_directory_fd,
    ScopedFd* output,
    struct stat* status) noexcept {
    if (retained_directory_fd < 0 ||
        output == nullptr || status == nullptr) {
        return ControlCheckpointPosixStoreErrorV1::
            kInvalidArgument;
    }
    struct stat retained_status {};
    if (::fstat(retained_directory_fd, &retained_status) != 0 ||
        !IsSafeDirectory(retained_status)) {
        return ControlCheckpointPosixStoreErrorV1::
            kUnsafeDirectory;
    }
    ScopedFd candidate(OpenAtNoIntr(
        retained_directory_fd,
        ".",
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
            O_NONBLOCK | O_CLOEXEC));
    struct stat candidate_status {};
    if (candidate.get() < 0 ||
        ::fstat(candidate.get(), &candidate_status) != 0 ||
        !IsSafeDirectory(candidate_status) ||
        !SameInode(retained_status, candidate_status)) {
        return ControlCheckpointPosixStoreErrorV1::
            kUnsafeDirectory;
    }
    *status = candidate_status;
    *output = std::move(candidate);
    return ControlCheckpointPosixStoreErrorV1::kNone;
}

[[nodiscard]] bool DirectorySupportsName(
    int directory_fd,
    std::string_view name) noexcept {
    if (name.empty() ||
        name.find('/') != std::string_view::npos) {
        return false;
    }
    errno = 0;
    const long limit = ::fpathconf(directory_fd, _PC_NAME_MAX);
    if (limit < 0L) {
        return errno == 0;
    }
    return name.size() <= static_cast<std::size_t>(limit);
}

[[nodiscard]] bool ReadExact(
    int descriptor,
    std::size_t size,
    std::vector<std::byte>* output) {
    if (output == nullptr || size == 0U ||
        size >
            static_cast<std::size_t>(
                std::numeric_limits<off_t>::max()) ||
        size >
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max())) {
        return false;
    }
    std::vector<std::byte> bytes(size);
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        const ssize_t result = ::pread(
            descriptor,
            bytes.data() + completed,
            bytes.size() - completed,
            static_cast<off_t>(completed));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            return false;
        }
        const std::size_t progress =
            static_cast<std::size_t>(result);
        if (progress > bytes.size() - completed) {
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
            static_cast<off_t>(size));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result != 0) {
            return false;
        }
        break;
    }
    output->swap(bytes);
    return true;
}

[[nodiscard]] bool EqualBytes(
    std::span<const std::byte> left,
    std::span<const std::byte> right) noexcept {
    return left.size() == right.size() &&
           std::equal(left.begin(), left.end(), right.begin());
}

template <typename Unsigned>
[[nodiscard]] bool ParseCanonicalDecimal(
    std::string_view text,
    Unsigned* output) noexcept {
    static_assert(std::is_unsigned_v<Unsigned>);
    if (output == nullptr || text.empty() ||
        (text.size() > 1U && text.front() == '0')) {
        return false;
    }
    Unsigned value = 0U;
    const auto parsed = std::from_chars(
        text.data(), text.data() + text.size(), value);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != text.data() + text.size() ||
        value == 0U) {
        return false;
    }
    *output = value;
    return true;
}

[[nodiscard]] bool ParseCheckpointFilename(
    std::string_view name,
    ParsedCheckpointFilename* output) noexcept {
    if (output == nullptr ||
        !name.starts_with(
            kControlCheckpointV1FilenamePrefix) ||
        !name.ends_with(
            kControlCheckpointV1FilenameSuffix) ||
        name.size() <=
            kControlCheckpointV1FilenamePrefix.size() +
                kControlCheckpointV1FilenameSuffix.size()) {
        return false;
    }
    const std::string_view body = name.substr(
        kControlCheckpointV1FilenamePrefix.size(),
        name.size() -
            kControlCheckpointV1FilenamePrefix.size() -
            kControlCheckpointV1FilenameSuffix.size());
    if (body.empty() || body.front() != 'd') {
        return false;
    }
    const std::size_t source_marker = body.find("-s", 1U);
    const std::size_t namespace_marker =
        source_marker == std::string_view::npos
            ? std::string_view::npos
            : body.find("-n", source_marker + 2U);
    const std::size_t ingress_marker =
        namespace_marker == std::string_view::npos
            ? std::string_view::npos
            : body.find("-i", namespace_marker + 2U);
    const std::size_t wal_marker =
        ingress_marker == std::string_view::npos
            ? std::string_view::npos
            : body.find("-e", ingress_marker + 2U);
    if (source_marker == std::string_view::npos ||
        namespace_marker == std::string_view::npos ||
        ingress_marker == std::string_view::npos ||
        wal_marker == std::string_view::npos ||
        source_marker <= 1U ||
        namespace_marker <= source_marker + 2U ||
        ingress_marker != namespace_marker + 34U ||
        wal_marker <= ingress_marker + 2U ||
        wal_marker + 2U >= body.size()) {
        return false;
    }

    ParsedCheckpointFilename parsed{};
    const std::string_view date_text =
        body.substr(1U, source_marker - 1U);
    const std::string_view source_text = body.substr(
        source_marker + 2U,
        namespace_marker - (source_marker + 2U));
    const std::string_view identity_text = body.substr(
        namespace_marker + 2U, 32U);
    const std::string_view ingress_text = body.substr(
        ingress_marker + 2U,
        wal_marker - (ingress_marker + 2U));
    const std::string_view wal_text =
        body.substr(wal_marker + 2U);
    if (!ParseCanonicalDecimal(
            date_text, &parsed.capture_date) ||
        !ParseCanonicalDecimal(
            source_text, &parsed.source_stream_id) ||
        !l2flow::common::ParseIdentity128Hex(
            identity_text, &parsed.stream_day_id) ||
        !ParseCanonicalDecimal(
            ingress_text,
            &parsed.processed_ingress_sequence) ||
        !ParseCanonicalDecimal(
            wal_text,
            &parsed.processed_record_end_wal_pos)) {
        return false;
    }
    *output = parsed;
    return true;
}

[[nodiscard]] bool IsCheckpointTemporaryLike(
    std::string_view name) noexcept {
    return name.size() > 1U && name.front() == '.' &&
           name.substr(1U).starts_with(
               kControlCheckpointV1FilenamePrefix);
}

[[nodiscard]] bool FilenameNamespaceMatches(
    const ParsedCheckpointFilename& parsed,
    const l2flow::ingress::RawControlSnapshot& frontier) noexcept {
    return parsed.source_stream_id == frontier.source_stream_id &&
           parsed.capture_date == frontier.capture_date &&
           parsed.stream_day_id == frontier.stream_day_id;
}

[[nodiscard]] bool RawFrontierShapeValid(
    const l2flow::ingress::RawControlSnapshot& frontier) noexcept {
    return !l2flow::common::IsZeroIdentity(
               frontier.writer_instance) &&
           !l2flow::common::IsZeroIdentity(
               frontier.stream_day_id) &&
           frontier.source_stream_id != 0U &&
           frontier.capture_date != 0U &&
           frontier.fatal_state == 0U &&
           RawFrontierCursorShapeValidV1(frontier);
}

[[nodiscard]] FrontierValidation ValidateFrontier(
    const ControlDecoderCheckpointV1& checkpoint,
    const l2flow::ingress::RawControlSnapshot& frontier) noexcept {
    const ControlDecoderSnapshotV1& state = checkpoint.state;
    if (state.processed_record_start_wal_pos <
            l2flow::ingress::kRawV1SegmentHeaderBytes ||
        (state.processed_record_start_wal_pos %
         l2flow::ingress::kRawV1RecordAlignment) != 0U ||
        (state.processed_record_end_wal_pos %
         l2flow::ingress::kRawV1RecordAlignment) != 0U) {
        return FrontierValidation::kInvalidCheckpointCursor;
    }
    if (!RawFrontierShapeValid(frontier)) {
        return FrontierValidation::kInvalidSnapshot;
    }
    if (frontier.source_stream_id != state.source_stream_id ||
        frontier.capture_date != state.capture_date ||
        frontier.stream_day_id != state.stream_day_id) {
        return FrontierValidation::kNamespaceMismatch;
    }
    if (!RawCursorAdvancePlausibleV1(
            state.processed_record_end_wal_pos,
            state.processed_ingress_sequence,
            frontier.durable_global_wal_pos,
            frontier.durable_ingress_sequence)) {
        return FrontierValidation::kCheckpointPastDurable;
    }
    return FrontierValidation::kNone;
}

[[nodiscard]] ControlCheckpointPosixStoreErrorV1
MapFrontierValidation(FrontierValidation validation) noexcept {
    switch (validation) {
        case FrontierValidation::kNone:
            return ControlCheckpointPosixStoreErrorV1::kNone;
        case FrontierValidation::kInvalidCheckpointCursor:
            return ControlCheckpointPosixStoreErrorV1::
                kInvalidCheckpoint;
        case FrontierValidation::kInvalidSnapshot:
            return ControlCheckpointPosixStoreErrorV1::
                kInvalidRawFrontier;
        case FrontierValidation::kNamespaceMismatch:
            return ControlCheckpointPosixStoreErrorV1::
                kNamespaceMismatch;
        case FrontierValidation::kCheckpointPastDurable:
            return ControlCheckpointPosixStoreErrorV1::
                kCheckpointPastDurableFrontier;
    }
    return ControlCheckpointPosixStoreErrorV1::
        kInvalidRawFrontier;
}

[[nodiscard]] std::string_view InputDiagnostic(
    ControlCheckpointPosixStoreErrorV1 error,
    bool encoded) noexcept {
    switch (error) {
        case ControlCheckpointPosixStoreErrorV1::
            kNamespaceMismatch:
            return encoded
                       ? "encoded control checkpoint and Raw frontier namespaces differ"
                       : "control checkpoint and Raw frontier namespaces differ";
        case ControlCheckpointPosixStoreErrorV1::
            kCheckpointPastDurableFrontier:
            return encoded
                       ? "encoded control checkpoint is past the Raw durable frontier"
                       : "control checkpoint cursor is past the Raw durable frontier";
        case ControlCheckpointPosixStoreErrorV1::
            kInvalidRawFrontier:
            return "Raw durable frontier snapshot is structurally invalid";
        case ControlCheckpointPosixStoreErrorV1::
            kInvalidCheckpoint:
            return encoded
                       ? "control checkpoint wire is invalid, non-canonical, or cursor-unaligned"
                       : "control checkpoint Raw cursors are not aligned";
        default:
            return encoded
                       ? "control checkpoint wire is invalid or non-canonical"
                       : "control checkpoint publication input is invalid";
    }
}

[[nodiscard]] ControlCheckpointPosixStoreErrorV1
ValidateCanonicalWire(
    std::span<const std::byte> encoded,
    const l2flow::ingress::RawControlSnapshot& frontier,
    ControlDecoderCheckpointV1* checkpoint,
    std::vector<std::byte>* canonical) noexcept {
    if (checkpoint == nullptr || canonical == nullptr ||
        encoded.empty() ||
        encoded.size() >
            kControlCheckpointV1MaximumWireBytes) {
        return ControlCheckpointPosixStoreErrorV1::
            kInvalidCheckpoint;
    }
    ControlDecoderCheckpointV1 decoded{};
    const ControlCheckpointV1Error decode_error =
        DecodeControlDecoderCheckpointV1(encoded, &decoded);
    if (decode_error != ControlCheckpointV1Error::kNone) {
        return decode_error ==
                       ControlCheckpointV1Error::
                           kResourceExhausted
                   ? ControlCheckpointPosixStoreErrorV1::
                         kAllocationFailure
                   : ControlCheckpointPosixStoreErrorV1::
                         kInvalidCheckpoint;
    }
    std::vector<std::byte> reencoded;
    const ControlCheckpointV1Error encode_error =
        EncodeControlDecoderCheckpointV1(decoded, &reencoded);
    if (encode_error != ControlCheckpointV1Error::kNone) {
        return encode_error ==
                       ControlCheckpointV1Error::
                           kResourceExhausted
                   ? ControlCheckpointPosixStoreErrorV1::
                         kAllocationFailure
                   : ControlCheckpointPosixStoreErrorV1::
                         kInvalidCheckpoint;
    }
    if (!EqualBytes(encoded, reencoded)) {
        return ControlCheckpointPosixStoreErrorV1::
            kInvalidCheckpoint;
    }
    const ControlCheckpointPosixStoreErrorV1 frontier_error =
        MapFrontierValidation(ValidateFrontier(decoded, frontier));
    if (frontier_error !=
        ControlCheckpointPosixStoreErrorV1::kNone) {
        return frontier_error;
    }
    *checkpoint = std::move(decoded);
    *canonical = std::move(reencoded);
    return ControlCheckpointPosixStoreErrorV1::kNone;
}

[[nodiscard]] ControlCheckpointPosixStoreErrorV1
LoadDiscoveredCheckpoint(
    int directory_fd,
    const struct stat& directory_status,
    const std::string& name,
    const ParsedCheckpointFilename& parsed_name,
    const l2flow::ingress::RawControlSnapshot& frontier,
    DiscoveredCheckpoint* output) {
    if (output == nullptr) {
        return ControlCheckpointPosixStoreErrorV1::
            kInvalidArgument;
    }
    ScopedFd descriptor(OpenAtNoIntr(
        directory_fd,
        name.c_str(),
        O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC));
    if (descriptor.get() < 0) {
        return ControlCheckpointPosixStoreErrorV1::
            kUnsafeCandidate;
    }
    struct stat before {};
    constexpr std::size_t minimum_wire_bytes =
        kControlCheckpointV1HeaderBytes +
        kControlCheckpointV1TrailerBytes;
    if (::fstat(descriptor.get(), &before) != 0 ||
        !IsSafeFile(before, directory_status) ||
        static_cast<std::uint64_t>(before.st_size) <
            minimum_wire_bytes ||
        !NameMatchesDescriptor(
            directory_fd,
            name.c_str(),
            descriptor.get(),
            &before)) {
        return ControlCheckpointPosixStoreErrorV1::
            kUnsafeCandidate;
    }
    const std::size_t byte_count =
        static_cast<std::size_t>(before.st_size);
    std::vector<std::byte> bytes;
    if (!ReadExact(descriptor.get(), byte_count, &bytes)) {
        return ControlCheckpointPosixStoreErrorV1::
            kReadbackFailure;
    }
    struct stat after {};
    if (::fstat(descriptor.get(), &after) != 0 ||
        !IsSafeFile(after, directory_status) ||
        !StableFileMetadata(before, after) ||
        !NameMatchesDescriptor(
            directory_fd,
            name.c_str(),
            descriptor.get(),
            &after) ||
        !DirectoryMatches(directory_fd, directory_status)) {
        return ControlCheckpointPosixStoreErrorV1::
            kUnsafeCandidate;
    }

    ControlDecoderCheckpointV1 checkpoint{};
    std::vector<std::byte> canonical;
    const ControlCheckpointPosixStoreErrorV1 validation =
        ValidateCanonicalWire(
            bytes, frontier, &checkpoint, &canonical);
    if (validation !=
        ControlCheckpointPosixStoreErrorV1::kNone) {
        return validation;
    }
    std::string expected_name;
    const ControlCheckpointPosixStoreErrorV1 name_error =
        ControlCheckpointV1Filename(
            checkpoint, &expected_name);
    if (name_error !=
        ControlCheckpointPosixStoreErrorV1::kNone) {
        return name_error;
    }
    const ControlDecoderSnapshotV1& state = checkpoint.state;
    if (expected_name != name ||
        state.source_stream_id !=
            parsed_name.source_stream_id ||
        state.capture_date != parsed_name.capture_date ||
        state.stream_day_id != parsed_name.stream_day_id ||
        state.processed_ingress_sequence !=
            parsed_name.processed_ingress_sequence ||
        state.processed_record_end_wal_pos !=
            parsed_name.processed_record_end_wal_pos) {
        return ControlCheckpointPosixStoreErrorV1::
            kCandidateConflict;
    }
    DiscoveredCheckpoint discovered{};
    discovered.filename = name;
    discovered.sha256 =
        l2flow::common::ComputeSha256(bytes);
    discovered.bytes = std::move(bytes);
    discovered.checkpoint = std::move(checkpoint);
    *output = std::move(discovered);
    return ControlCheckpointPosixStoreErrorV1::kNone;
}

[[nodiscard]] bool WriteExact(
    int directory_fd,
    const struct stat& directory_status,
    const std::string& name,
    int descriptor,
    std::span<const std::byte> bytes) noexcept {
    if (bytes.empty() ||
        bytes.size() >
            kControlCheckpointV1MaximumWireBytes ||
        bytes.size() >
            static_cast<std::size_t>(
                std::numeric_limits<off_t>::max()) ||
        bytes.size() >
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max())) {
        return false;
    }
    std::size_t completed = 0U;
    while (completed < bytes.size()) {
        struct stat file_status {};
        if (!DirectoryMatches(directory_fd, directory_status) ||
            ::fstat(descriptor, &file_status) != 0 ||
            !IsSafeFile(file_status, directory_status) ||
            static_cast<std::uint64_t>(file_status.st_size) !=
                completed ||
            !NameMatchesDescriptor(
                directory_fd,
                name.c_str(),
                descriptor,
                &file_status)) {
            return false;
        }
        const std::size_t remaining = bytes.size() - completed;
        const ssize_t result = ::pwrite(
            descriptor,
            bytes.data() + completed,
            remaining,
            static_cast<off_t>(completed));
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            return false;
        }
        const std::size_t progress =
            static_cast<std::size_t>(result);
        if (progress > remaining) {
            return false;
        }
        completed += progress;
    }
    struct stat completed_status {};
    return DirectoryMatches(directory_fd, directory_status) &&
           ::fstat(descriptor, &completed_status) == 0 &&
           IsSafeFile(completed_status, directory_status) &&
           static_cast<std::uint64_t>(completed_status.st_size) ==
               bytes.size() &&
           NameMatchesDescriptor(
               directory_fd,
               name.c_str(),
               descriptor,
               &completed_status);
}

[[nodiscard]] ControlCheckpointPosixStoreErrorV1
ValidateOpenExactCandidate(
    int directory_fd,
    const struct stat& directory_status,
    const std::string& name,
    int descriptor,
    std::span<const std::byte> expected_bytes,
    const l2flow::common::Sha256Digest& expected_sha256,
    const l2flow::ingress::RawControlSnapshot& frontier,
    struct stat* output_status) {
    if (output_status == nullptr) {
        return ControlCheckpointPosixStoreErrorV1::
            kInvalidArgument;
    }
    struct stat before {};
    if (::fstat(descriptor, &before) != 0 ||
        !IsSafeFile(before, directory_status) ||
        !NameMatchesDescriptor(
            directory_fd,
            name.c_str(),
            descriptor,
            &before)) {
        return ControlCheckpointPosixStoreErrorV1::
            kUnsafeCandidate;
    }
    if (static_cast<std::uint64_t>(before.st_size) !=
        expected_bytes.size()) {
        return ControlCheckpointPosixStoreErrorV1::
            kCandidateConflict;
    }
    std::vector<std::byte> bytes;
    if (!ReadExact(descriptor, expected_bytes.size(), &bytes)) {
        return ControlCheckpointPosixStoreErrorV1::
            kReadbackFailure;
    }
    struct stat after {};
    if (::fstat(descriptor, &after) != 0 ||
        !IsSafeFile(after, directory_status) ||
        !StableFileMetadata(before, after) ||
        !NameMatchesDescriptor(
            directory_fd,
            name.c_str(),
            descriptor,
            &after) ||
        !DirectoryMatches(directory_fd, directory_status)) {
        return ControlCheckpointPosixStoreErrorV1::
            kUnsafeCandidate;
    }
    if (!EqualBytes(bytes, expected_bytes) ||
        l2flow::common::ComputeSha256(bytes) !=
            expected_sha256) {
        return ControlCheckpointPosixStoreErrorV1::
            kCandidateConflict;
    }
    ControlDecoderCheckpointV1 decoded{};
    std::vector<std::byte> canonical;
    const ControlCheckpointPosixStoreErrorV1 validation =
        ValidateCanonicalWire(
            bytes, frontier, &decoded, &canonical);
    if (validation !=
        ControlCheckpointPosixStoreErrorV1::kNone) {
        return validation ==
                       ControlCheckpointPosixStoreErrorV1::
                           kAllocationFailure
                   ? validation
                   : ControlCheckpointPosixStoreErrorV1::
                         kReadbackFailure;
    }
    *output_status = after;
    return ControlCheckpointPosixStoreErrorV1::kNone;
}

[[nodiscard]] ControlCheckpointPosixStoreErrorV1
LoadExactCandidate(
    int directory_fd,
    const struct stat& directory_status,
    const std::string& name,
    std::span<const std::byte> expected_bytes,
    const l2flow::common::Sha256Digest& expected_sha256,
    const l2flow::ingress::RawControlSnapshot& frontier,
    LoadedCandidate* output) {
    if (output == nullptr) {
        return ControlCheckpointPosixStoreErrorV1::
            kInvalidArgument;
    }
    ScopedFd descriptor(OpenAtNoIntr(
        directory_fd,
        name.c_str(),
        O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC));
    if (descriptor.get() < 0) {
        return errno == ENOENT
                   ? ControlCheckpointPosixStoreErrorV1::
                         kPublishConflict
                   : ControlCheckpointPosixStoreErrorV1::
                         kUnsafeCandidate;
    }
    struct stat status {};
    const ControlCheckpointPosixStoreErrorV1 error =
        ValidateOpenExactCandidate(
            directory_fd,
            directory_status,
            name,
            descriptor.get(),
            expected_bytes,
            expected_sha256,
            frontier,
            &status);
    if (error != ControlCheckpointPosixStoreErrorV1::kNone) {
        return error;
    }
    LoadedCandidate loaded{};
    loaded.descriptor = std::move(descriptor);
    loaded.status = status;
    *output = std::move(loaded);
    return ControlCheckpointPosixStoreErrorV1::kNone;
}

[[nodiscard]] ControlCheckpointPosixStoreErrorV1
SyncActualDirectory(
    int directory_fd,
    const struct stat& expected) noexcept {
    if (!DirectoryMatches(directory_fd, expected)) {
        return ControlCheckpointPosixStoreErrorV1::
            kUnsafeDirectory;
    }
    if (!FsyncNoIntr(directory_fd)) {
        return ControlCheckpointPosixStoreErrorV1::
            kSyncFailure;
    }
    if (!DirectoryMatches(directory_fd, expected)) {
        return ControlCheckpointPosixStoreErrorV1::
            kUnsafeDirectory;
    }
    return ControlCheckpointPosixStoreErrorV1::kNone;
}

[[nodiscard]] std::span<const std::byte> StringBytes(
    std::string_view text) noexcept {
    return std::as_bytes(
        std::span<const char>(text.data(), text.size()));
}

[[nodiscard]] bool UpdateU32(
    l2flow::common::Sha256Hasher* hasher,
    std::uint32_t value) noexcept {
    if (hasher == nullptr) {
        return false;
    }
    std::array<std::byte, 4U> wire{};
    for (std::size_t index = 0U; index < wire.size(); ++index) {
        wire[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
    return hasher->Update(wire);
}

[[nodiscard]] bool UpdateU64(
    l2flow::common::Sha256Hasher* hasher,
    std::uint64_t value) noexcept {
    if (hasher == nullptr) {
        return false;
    }
    std::array<std::byte, 8U> wire{};
    for (std::size_t index = 0U; index < wire.size(); ++index) {
        wire[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
    return hasher->Update(wire);
}

[[nodiscard]] bool ComputeBarrierIdentity(
    const ControlDecoderCheckpointV1& checkpoint,
    const l2flow::ingress::RawControlSnapshot& frontier,
    const l2flow::common::Sha256Digest& checkpoint_sha256,
    std::size_t byte_count,
    std::string_view filename,
    const struct stat& directory_status,
    const struct stat& file_status,
    l2flow::common::Sha256Digest* output) noexcept {
    if (output == nullptr ||
        byte_count >
            std::numeric_limits<std::uint64_t>::max() ||
        filename.size() >
            std::numeric_limits<std::uint32_t>::max()) {
        return false;
    }
    constexpr std::string_view domain =
        "L2FLOW_PHASE3_CONTROL_CHECKPOINT_POSIX_BARRIER_V1";
    constexpr std::array<std::byte, 1U> separator{
        std::byte{0}};
    constexpr std::array<std::byte, 1U> completed_barriers{
        std::byte{3}};
    l2flow::common::Sha256Hasher hasher;
    const ControlDecoderSnapshotV1& state = checkpoint.state;
    if (!hasher.Update(StringBytes(domain)) ||
        !hasher.Update(separator) ||
        !UpdateU32(&hasher, state.source_stream_id) ||
        !UpdateU32(&hasher, state.capture_date) ||
        !hasher.Update(state.stream_day_id) ||
        !UpdateU64(
            &hasher, state.processed_ingress_sequence) ||
        !UpdateU64(
            &hasher, state.processed_record_end_wal_pos) ||
        !hasher.Update(frontier.writer_instance) ||
        !hasher.Update(frontier.stream_day_id) ||
        !UpdateU32(&hasher, frontier.source_stream_id) ||
        !UpdateU32(&hasher, frontier.capture_date) ||
        !UpdateU32(&hasher, frontier.segment_sequence) ||
        !UpdateU32(&hasher, frontier.fatal_state) ||
        !UpdateU64(
            &hasher, frontier.append_global_wal_pos) ||
        !UpdateU64(
            &hasher, frontier.append_ingress_sequence) ||
        !UpdateU64(
            &hasher, frontier.append_segment_offset) ||
        !UpdateU64(
            &hasher, frontier.durable_global_wal_pos) ||
        !UpdateU64(
            &hasher, frontier.durable_ingress_sequence) ||
        !UpdateU64(
            &hasher, frontier.durable_segment_offset) ||
        !UpdateU64(&hasher, frontier.clock_epoch_label) ||
        !UpdateU64(
            &hasher, frontier.heartbeat_monotonic_ns) ||
        !UpdateU64(
            &hasher,
            static_cast<std::uint64_t>(directory_status.st_dev)) ||
        !UpdateU64(
            &hasher,
            static_cast<std::uint64_t>(directory_status.st_ino)) ||
        !UpdateU64(
            &hasher,
            static_cast<std::uint64_t>(file_status.st_dev)) ||
        !UpdateU64(
            &hasher,
            static_cast<std::uint64_t>(file_status.st_ino)) ||
        !UpdateU64(
            &hasher,
            static_cast<std::uint64_t>(file_status.st_mode)) ||
        !UpdateU64(
            &hasher,
            static_cast<std::uint64_t>(file_status.st_uid)) ||
        !UpdateU64(
            &hasher,
            static_cast<std::uint64_t>(file_status.st_gid)) ||
        !UpdateU64(
            &hasher,
            static_cast<std::uint64_t>(file_status.st_nlink)) ||
        !UpdateU64(
            &hasher,
            static_cast<std::uint64_t>(file_status.st_size)) ||
        !UpdateU64(
            &hasher,
            static_cast<std::uint64_t>(
                file_status.st_mtim.tv_sec)) ||
        !UpdateU64(
            &hasher,
            static_cast<std::uint64_t>(
                file_status.st_mtim.tv_nsec)) ||
        !UpdateU64(
            &hasher,
            static_cast<std::uint64_t>(
                file_status.st_ctim.tv_sec)) ||
        !UpdateU64(
            &hasher,
            static_cast<std::uint64_t>(
                file_status.st_ctim.tv_nsec)) ||
        !UpdateU64(
            &hasher, static_cast<std::uint64_t>(byte_count)) ||
        !hasher.Update(checkpoint_sha256) ||
        !UpdateU32(
            &hasher,
            static_cast<std::uint32_t>(filename.size())) ||
        !hasher.Update(StringBytes(filename)) ||
        !hasher.Update(completed_barriers)) {
        return false;
    }
    return hasher.Finalize(output);
}

template <typename Unsigned>
[[nodiscard]] bool AppendDecimal(
    Unsigned value,
    std::string* output) {
    static_assert(std::is_unsigned_v<Unsigned>);
    if (output == nullptr) {
        return false;
    }
    std::array<char,
               static_cast<std::size_t>(
                   std::numeric_limits<Unsigned>::digits10) +
                   1U>
        buffer{};
    const auto conversion = std::to_chars(
        buffer.data(), buffer.data() + buffer.size(), value);
    if (conversion.ec != std::errc{}) {
        return false;
    }
    output->append(
        buffer.data(),
        static_cast<std::size_t>(
            conversion.ptr - buffer.data()));
    return true;
}

void AppendIdentityHex(
    const l2flow::common::Identity128& identity,
    std::string* output) {
    constexpr std::string_view hex = "0123456789abcdef";
    for (const std::byte byte : identity) {
        const std::uint8_t octet =
            std::to_integer<std::uint8_t>(byte);
        output->push_back(hex[(octet >> 4U) & 0x0fU]);
        output->push_back(hex[octet & 0x0fU]);
    }
}

[[nodiscard]] std::uint64_t Device(
    const struct stat& status) noexcept {
    return static_cast<std::uint64_t>(status.st_dev);
}

[[nodiscard]] std::uint64_t Inode(
    const struct stat& status) noexcept {
    return static_cast<std::uint64_t>(status.st_ino);
}

}  // namespace

struct ControlCheckpointPosixPublishAccessV1 final {
    [[nodiscard]] static std::unique_ptr<
        PublishedControlCheckpointReceiptV1>
    CreateReceipt(
        const ControlDecoderCheckpointV1& checkpoint,
        l2flow::ingress::RawControlSnapshot durable_frontier,
        l2flow::common::Sha256Digest checkpoint_sha256,
        l2flow::common::Sha256Digest barrier_identity_sha256,
        std::size_t byte_count,
        std::string filename,
        int directory_fd,
        int file_fd,
        const struct stat& directory_status,
        const struct stat& file_status) {
        try {
            return std::unique_ptr<
                PublishedControlCheckpointReceiptV1>(
                new PublishedControlCheckpointReceiptV1(
                    checkpoint.state.source_stream_id,
                    checkpoint.state.capture_date,
                    checkpoint.state.stream_day_id,
                    checkpoint.state.processed_ingress_sequence,
                    checkpoint.state.processed_record_end_wal_pos,
                    std::move(durable_frontier),
                    checkpoint_sha256,
                    barrier_identity_sha256,
                    byte_count,
                    std::move(filename),
                    directory_fd,
                    file_fd,
                    Device(directory_status),
                    Inode(directory_status),
                    Device(file_status),
                    Inode(file_status)));
        } catch (...) {
            if (file_fd >= 0) {
                static_cast<void>(::close(file_fd));
            }
            if (directory_fd >= 0) {
                static_cast<void>(::close(directory_fd));
            }
            throw;
        }
    }
};

std::string_view ControlCheckpointPosixStoreErrorV1Name(
    ControlCheckpointPosixStoreErrorV1 error) noexcept {
    switch (error) {
        case ControlCheckpointPosixStoreErrorV1::kNone:
            return "none";
        case ControlCheckpointPosixStoreErrorV1::
            kInvalidArgument:
            return "invalid_argument";
        case ControlCheckpointPosixStoreErrorV1::
            kInvalidCheckpoint:
            return "invalid_checkpoint";
        case ControlCheckpointPosixStoreErrorV1::
            kInvalidRawFrontier:
            return "invalid_raw_frontier";
        case ControlCheckpointPosixStoreErrorV1::
            kNamespaceMismatch:
            return "namespace_mismatch";
        case ControlCheckpointPosixStoreErrorV1::
            kCheckpointPastDurableFrontier:
            return "checkpoint_past_durable_frontier";
        case ControlCheckpointPosixStoreErrorV1::
            kUnsafeDirectory:
            return "unsafe_directory";
        case ControlCheckpointPosixStoreErrorV1::
            kUnsafeCandidate:
            return "unsafe_candidate";
        case ControlCheckpointPosixStoreErrorV1::
            kCandidateConflict:
            return "candidate_conflict";
        case ControlCheckpointPosixStoreErrorV1::
            kTemporaryCreate:
            return "temporary_create";
        case ControlCheckpointPosixStoreErrorV1::
            kWriteFailure:
            return "write_failure";
        case ControlCheckpointPosixStoreErrorV1::
            kSyncFailure:
            return "sync_failure";
        case ControlCheckpointPosixStoreErrorV1::
            kPublishConflict:
            return "publish_conflict";
        case ControlCheckpointPosixStoreErrorV1::
            kReadbackFailure:
            return "readback_failure";
        case ControlCheckpointPosixStoreErrorV1::
            kMalformedCandidateName:
            return "malformed_candidate_name";
        case ControlCheckpointPosixStoreErrorV1::
            kCandidateLimitExceeded:
            return "candidate_limit_exceeded";
        case ControlCheckpointPosixStoreErrorV1::
            kInconsistentCandidates:
            return "inconsistent_candidates";
        case ControlCheckpointPosixStoreErrorV1::
            kNoEligibleCheckpoint:
            return "no_eligible_checkpoint";
        case ControlCheckpointPosixStoreErrorV1::
            kAllocationFailure:
            return "allocation_failure";
    }
    return "unknown";
}

ControlCheckpointPosixStoreErrorV1
ControlCheckpointV1Filename(
    const ControlDecoderCheckpointV1& checkpoint,
    std::string* filename) noexcept {
    if (filename == nullptr) {
        return ControlCheckpointPosixStoreErrorV1::
            kInvalidArgument;
    }
    if (ValidateControlDecoderCheckpointV1(checkpoint) !=
        ControlCheckpointV1Error::kNone) {
        return ControlCheckpointPosixStoreErrorV1::
            kInvalidCheckpoint;
    }
    try {
        std::size_t capacity =
            kControlCheckpointV1FilenamePrefix.size();
        constexpr std::array<std::size_t, 10U> additions{
            1U,
            10U,
            2U,
            10U,
            2U,
            32U,
            2U,
            20U,
            2U,
            20U};
        for (const std::size_t addition : additions) {
            if (!CheckedAddSize(capacity, addition, &capacity)) {
                return ControlCheckpointPosixStoreErrorV1::
                    kAllocationFailure;
            }
        }
        if (!CheckedAddSize(
                capacity,
                kControlCheckpointV1FilenameSuffix.size(),
                &capacity)) {
            return ControlCheckpointPosixStoreErrorV1::
                kAllocationFailure;
        }

        std::string candidate;
        candidate.reserve(capacity);
        candidate.append(kControlCheckpointV1FilenamePrefix);
        candidate.push_back('d');
        if (!AppendDecimal(
                checkpoint.state.capture_date, &candidate)) {
            return ControlCheckpointPosixStoreErrorV1::
                kInvalidCheckpoint;
        }
        candidate.append("-s");
        if (!AppendDecimal(
                checkpoint.state.source_stream_id, &candidate)) {
            return ControlCheckpointPosixStoreErrorV1::
                kInvalidCheckpoint;
        }
        candidate.append("-n");
        AppendIdentityHex(
            checkpoint.state.stream_day_id, &candidate);
        candidate.append("-i");
        if (!AppendDecimal(
                checkpoint.state.processed_ingress_sequence,
                &candidate)) {
            return ControlCheckpointPosixStoreErrorV1::
                kInvalidCheckpoint;
        }
        candidate.append("-e");
        if (!AppendDecimal(
                checkpoint.state.processed_record_end_wal_pos,
                &candidate)) {
            return ControlCheckpointPosixStoreErrorV1::
                kInvalidCheckpoint;
        }
        candidate.append(kControlCheckpointV1FilenameSuffix);
        if (candidate.size() > capacity) {
            return ControlCheckpointPosixStoreErrorV1::
                kAllocationFailure;
        }
        filename->swap(candidate);
        return ControlCheckpointPosixStoreErrorV1::kNone;
    } catch (...) {
        return ControlCheckpointPosixStoreErrorV1::
            kAllocationFailure;
    }
}

ControlCheckpointPosixStoreErrorV1
ControlCheckpointV1TemporaryFilename(
    const ControlDecoderCheckpointV1& checkpoint,
    std::string* filename) noexcept {
    if (filename == nullptr) {
        return ControlCheckpointPosixStoreErrorV1::
            kInvalidArgument;
    }
    try {
        std::string final_name;
        const ControlCheckpointPosixStoreErrorV1 error =
            ControlCheckpointV1Filename(
                checkpoint, &final_name);
        if (error !=
            ControlCheckpointPosixStoreErrorV1::kNone) {
            return error;
        }
        std::size_t capacity = 0U;
        if (!CheckedAddSize(
                1U, final_name.size(), &capacity) ||
            !CheckedAddSize(
                capacity,
                kControlCheckpointV1TemporarySuffix.size(),
                &capacity)) {
            return ControlCheckpointPosixStoreErrorV1::
                kAllocationFailure;
        }
        std::string candidate;
        candidate.reserve(capacity);
        candidate.push_back('.');
        candidate.append(final_name);
        candidate.append(kControlCheckpointV1TemporarySuffix);
        if (candidate.size() != capacity) {
            return ControlCheckpointPosixStoreErrorV1::
                kAllocationFailure;
        }
        filename->swap(candidate);
        return ControlCheckpointPosixStoreErrorV1::kNone;
    } catch (...) {
        return ControlCheckpointPosixStoreErrorV1::
            kAllocationFailure;
    }
}

PublishedControlCheckpointReceiptV1::
PublishedControlCheckpointReceiptV1(
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    l2flow::common::Identity128 stream_day_id,
    std::uint64_t processed_ingress_sequence,
    std::uint64_t processed_record_end_wal_pos,
    l2flow::ingress::RawControlSnapshot durable_frontier,
    l2flow::common::Sha256Digest checkpoint_sha256,
    l2flow::common::Sha256Digest barrier_identity_sha256,
    std::size_t byte_count,
    std::string filename,
    int directory_fd,
    int file_fd,
    std::uint64_t directory_device,
    std::uint64_t directory_inode,
    std::uint64_t file_device,
    std::uint64_t file_inode) noexcept
    : source_stream_id_(source_stream_id),
      capture_date_(capture_date),
      stream_day_id_(stream_day_id),
      processed_ingress_sequence_(
          processed_ingress_sequence),
      processed_record_end_wal_pos_(
          processed_record_end_wal_pos),
      durable_frontier_(std::move(durable_frontier)),
      checkpoint_sha256_(checkpoint_sha256),
      barrier_identity_sha256_(
          barrier_identity_sha256),
      byte_count_(byte_count),
      filename_(std::move(filename)),
      directory_fd_(directory_fd),
      file_fd_(file_fd),
      directory_device_(directory_device),
      directory_inode_(directory_inode),
      file_device_(file_device),
      file_inode_(file_inode),
      file_barrier_complete_(true),
      directory_barrier_complete_(true) {}

PublishedControlCheckpointReceiptV1::
~PublishedControlCheckpointReceiptV1() {
    if (file_fd_ >= 0) {
        static_cast<void>(::close(file_fd_));
    }
    if (directory_fd_ >= 0) {
        static_cast<void>(::close(directory_fd_));
    }
}

bool PublishedControlCheckpointReceiptV1::Validate(
    std::string* diagnostic) const noexcept {
    try {
        struct stat directory {};
        struct stat before {};
        if (!file_barrier_complete_ ||
            !directory_barrier_complete_ ||
            directory_fd_ < 0 || file_fd_ < 0 ||
            filename_.empty() || byte_count_ == 0U ||
            byte_count_ >
                kControlCheckpointV1MaximumWireBytes ||
            ::fstat(directory_fd_, &directory) != 0 ||
            !IsSafeDirectory(directory) ||
            Device(directory) != directory_device_ ||
            Inode(directory) != directory_inode_ ||
            ::fstat(file_fd_, &before) != 0 ||
            !IsSafeFile(before, directory) ||
            Device(before) != file_device_ ||
            Inode(before) != file_inode_ ||
            static_cast<std::uint64_t>(before.st_size) !=
                byte_count_ ||
            !NameMatchesDescriptor(
                directory_fd_,
                filename_.c_str(),
                file_fd_,
                &before)) {
            SetDiagnostic(
                diagnostic,
                "published control checkpoint identity is stale");
            return false;
        }

        std::vector<std::byte> bytes;
        if (!ReadExact(file_fd_, byte_count_, &bytes) ||
            l2flow::common::ComputeSha256(bytes) !=
                checkpoint_sha256_) {
            SetDiagnostic(
                diagnostic,
                "published control checkpoint bytes changed");
            return false;
        }
        ControlDecoderCheckpointV1 decoded{};
        std::vector<std::byte> canonical;
        if (ValidateCanonicalWire(
                bytes,
                durable_frontier_,
                &decoded,
                &canonical) !=
                ControlCheckpointPosixStoreErrorV1::kNone ||
            decoded.state.source_stream_id !=
                source_stream_id_ ||
            decoded.state.capture_date != capture_date_ ||
            decoded.state.stream_day_id != stream_day_id_ ||
            decoded.state.processed_ingress_sequence !=
                processed_ingress_sequence_ ||
            decoded.state.processed_record_end_wal_pos !=
                processed_record_end_wal_pos_) {
            SetDiagnostic(
                diagnostic,
                "published control checkpoint model or frontier changed");
            return false;
        }

        std::string expected_final;
        std::string expected_temporary;
        if (ControlCheckpointV1Filename(
                decoded, &expected_final) !=
                ControlCheckpointPosixStoreErrorV1::kNone ||
            ControlCheckpointV1TemporaryFilename(
                decoded, &expected_temporary) !=
                ControlCheckpointPosixStoreErrorV1::kNone ||
            expected_final != filename_ ||
            !RequireCandidateState(
                directory_fd_,
                directory,
                expected_final,
                expected_temporary,
                true,
                false)) {
            SetDiagnostic(
                diagnostic,
                "published control checkpoint name mapping changed");
            return false;
        }

        struct stat after {};
        l2flow::common::Sha256Digest barrier_identity{};
        if (::fstat(file_fd_, &after) != 0 ||
            !IsSafeFile(after, directory) ||
            !StableFileMetadata(before, after) ||
            Device(after) != file_device_ ||
            Inode(after) != file_inode_ ||
            !NameMatchesDescriptor(
                directory_fd_,
                filename_.c_str(),
                file_fd_,
                &after) ||
            !DirectoryMatches(directory_fd_, directory) ||
            !ComputeBarrierIdentity(
                decoded,
                durable_frontier_,
                checkpoint_sha256_,
                byte_count_,
                filename_,
                directory,
                after,
                &barrier_identity) ||
            barrier_identity != barrier_identity_sha256_) {
            SetDiagnostic(
                diagnostic,
                "published control checkpoint barrier identity changed");
            return false;
        }
        SetDiagnostic(diagnostic, "");
        return true;
    } catch (...) {
        SetDiagnostic(
            diagnostic,
            "published control checkpoint validation allocation failed");
        return false;
    }
}

ControlCheckpointPosixLoadResultV1
LoadLatestControlCheckpointV1At(
    int retained_directory_fd,
    const l2flow::ingress::RawControlSnapshot&
        current_raw_durable_frontier,
    std::string* diagnostic) noexcept {
    ControlCheckpointPosixLoadResultV1 result{};
    try {
        if (!RawFrontierShapeValid(
                current_raw_durable_frontier)) {
            result.error =
                ControlCheckpointPosixStoreErrorV1::
                    kInvalidRawFrontier;
            SetDiagnostic(
                diagnostic,
                "restart Raw durable frontier is structurally invalid");
            return result;
        }

        ScopedFd directory;
        struct stat directory_status {};
        result.error = OpenRetainedDirectory(
            retained_directory_fd,
            &directory,
            &directory_status);
        if (result.error !=
            ControlCheckpointPosixStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "restart checkpoint directory is not retained owner-only storage");
            return result;
        }
        if (!LockSharedNoIntr(directory.get()) ||
            !DirectoryMatches(
                directory.get(), directory_status)) {
            result.error =
                ControlCheckpointPosixStoreErrorV1::
                    kUnsafeDirectory;
            SetDiagnostic(
                diagnostic,
                "restart checkpoint directory cannot be safely enumerated");
            return result;
        }

        ScopedFd scan(OpenAtNoIntr(
            directory.get(),
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC));
        struct stat scan_status {};
        if (scan.get() < 0 ||
            ::fstat(scan.get(), &scan_status) != 0 ||
            !IsSafeDirectory(scan_status) ||
            !SameInode(directory_status, scan_status)) {
            result.error =
                ControlCheckpointPosixStoreErrorV1::
                    kUnsafeDirectory;
            return result;
        }
        DIR* const native = ::fdopendir(scan.get());
        if (native == nullptr) {
            result.error =
                ControlCheckpointPosixStoreErrorV1::
                    kUnsafeDirectory;
            return result;
        }
        static_cast<void>(scan.Release());
        ScopedDir entries(native);
        std::vector<DiscoveredCheckpoint> candidates;
        for (;;) {
            errno = 0;
            dirent* const entry = ::readdir(entries.get());
            if (entry == nullptr) {
                if (errno != 0) {
                    result.error =
                        ControlCheckpointPosixStoreErrorV1::
                            kUnsafeDirectory;
                    SetDiagnostic(
                        diagnostic,
                        "restart checkpoint directory enumeration failed");
                    return result;
                }
                break;
            }
            const std::string_view name(entry->d_name);
            if (name == "." || name == ".." ||
                IsCheckpointTemporaryLike(name) ||
                !name.starts_with(
                    kControlCheckpointV1FilenamePrefix)) {
                continue;
            }
            if (result.observed_final_candidate_count ==
                kControlCheckpointV1MaximumFinalCandidates) {
                result.error =
                    ControlCheckpointPosixStoreErrorV1::
                        kCandidateLimitExceeded;
                SetDiagnostic(
                    diagnostic,
                    "restart checkpoint final candidate limit exceeded");
                return result;
            }
            ++result.observed_final_candidate_count;

            ParsedCheckpointFilename parsed{};
            if (!ParseCheckpointFilename(name, &parsed)) {
                result.error =
                    ControlCheckpointPosixStoreErrorV1::
                        kMalformedCandidateName;
                SetDiagnostic(
                    diagnostic,
                    "malformed control checkpoint final candidate name");
                return result;
            }
            if (!FilenameNamespaceMatches(
                    parsed,
                    current_raw_durable_frontier)) {
                continue;
            }
            ++result.namespace_candidate_count;

            const std::string owned_name(name);
            DiscoveredCheckpoint candidate{};
            result.error = LoadDiscoveredCheckpoint(
                directory.get(),
                directory_status,
                owned_name,
                parsed,
                current_raw_durable_frontier,
                &candidate);
            if (result.error !=
                ControlCheckpointPosixStoreErrorV1::kNone) {
                SetDiagnostic(
                    diagnostic,
                    result.error ==
                            ControlCheckpointPosixStoreErrorV1::
                                kCheckpointPastDurableFrontier
                        ? "restart checkpoint final is past the current Raw durable frontier"
                        : result.error ==
                                  ControlCheckpointPosixStoreErrorV1::
                                      kUnsafeCandidate
                              ? "restart checkpoint final is not a safe immutable file"
                              : "restart checkpoint final failed canonical stable readback");
                return result;
            }
            candidates.emplace_back(std::move(candidate));
        }
        if (!DirectoryMatches(
                directory.get(), directory_status)) {
            result.error =
                ControlCheckpointPosixStoreErrorV1::
                    kUnsafeDirectory;
            return result;
        }
        if (candidates.empty()) {
            result.error =
                ControlCheckpointPosixStoreErrorV1::
                    kNoEligibleCheckpoint;
            SetDiagnostic(
                diagnostic,
                "no immutable checkpoint final exists for the Raw namespace");
            return result;
        }

        std::sort(
            candidates.begin(),
            candidates.end(),
            [](const DiscoveredCheckpoint& left,
               const DiscoveredCheckpoint& right) noexcept {
                const ControlDecoderSnapshotV1& left_state =
                    left.checkpoint.state;
                const ControlDecoderSnapshotV1& right_state =
                    right.checkpoint.state;
                if (left_state.processed_ingress_sequence !=
                    right_state.processed_ingress_sequence) {
                    return left_state.processed_ingress_sequence <
                           right_state.processed_ingress_sequence;
                }
                return left_state.processed_record_end_wal_pos <
                       right_state.processed_record_end_wal_pos;
            });
        for (std::size_t index = 1U;
             index < candidates.size();
             ++index) {
            const ControlDecoderSnapshotV1& previous =
                candidates[index - 1U].checkpoint.state;
            const ControlDecoderSnapshotV1& current =
                candidates[index].checkpoint.state;
            if (current.processed_ingress_sequence <=
                    previous.processed_ingress_sequence ||
                current.processed_record_end_wal_pos <=
                    previous.processed_record_end_wal_pos) {
                result.error =
                    ControlCheckpointPosixStoreErrorV1::
                        kInconsistentCandidates;
                SetDiagnostic(
                    diagnostic,
                    "restart checkpoint cursors do not form one increasing chain");
                return result;
            }
        }

        DiscoveredCheckpoint& best = candidates.back();
        result.filename = std::move(best.filename);
        result.checkpoint_sha256 = best.sha256;
        result.encoded_checkpoint = std::move(best.bytes);
        result.checkpoint.emplace(
            std::move(best.checkpoint));
        result.error =
            ControlCheckpointPosixStoreErrorV1::kNone;
        SetDiagnostic(diagnostic, "");
        return result;
    } catch (...) {
        result.error =
            ControlCheckpointPosixStoreErrorV1::
                kAllocationFailure;
        SetDiagnostic(
            diagnostic,
            "restart checkpoint discovery allocation failed");
        return result;
    }
}

namespace {

[[nodiscard]] ControlCheckpointPosixPublishResultV1
PublishCanonicalCheckpointV1At(
    int retained_directory_fd,
    const ControlDecoderCheckpointV1& checkpoint,
    const std::vector<std::byte>& canonical,
    const l2flow::ingress::RawControlSnapshot& durable_frontier,
    std::string* diagnostic) noexcept {
    ControlCheckpointPosixPublishResultV1 result{};
    try {
        if (canonical.empty() ||
            canonical.size() >
                kControlCheckpointV1MaximumWireBytes ||
            ValidateControlDecoderCheckpointV1(checkpoint) !=
                ControlCheckpointV1Error::kNone) {
            result.error =
                ControlCheckpointPosixStoreErrorV1::
                    kInvalidCheckpoint;
            SetDiagnostic(
                diagnostic,
                "control checkpoint model or encoding is invalid");
            return result;
        }
        result.error = MapFrontierValidation(
            ValidateFrontier(checkpoint, durable_frontier));
        if (result.error !=
            ControlCheckpointPosixStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic, InputDiagnostic(result.error, false));
            return result;
        }
        result.checkpoint_sha256 =
            l2flow::common::ComputeSha256(canonical);
        result.error = ControlCheckpointV1Filename(
            checkpoint, &result.filename);
        if (result.error !=
            ControlCheckpointPosixStoreErrorV1::kNone) {
            return result;
        }
        std::string temporary_name;
        result.error = ControlCheckpointV1TemporaryFilename(
            checkpoint, &temporary_name);
        if (result.error !=
            ControlCheckpointPosixStoreErrorV1::kNone) {
            return result;
        }

        ScopedFd directory;
        struct stat directory_status {};
        result.error = OpenRetainedDirectory(
            retained_directory_fd,
            &directory,
            &directory_status);
        if (result.error !=
            ControlCheckpointPosixStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "control checkpoint directory is not retained owner-only storage");
            return result;
        }
        if (!DirectorySupportsName(
                directory.get(), result.filename) ||
            !DirectorySupportsName(
                directory.get(), temporary_name) ||
            !LockExclusiveNoIntr(directory.get()) ||
            !DirectoryMatches(
                directory.get(), directory_status)) {
            result.error =
                ControlCheckpointPosixStoreErrorV1::
                    kUnsafeDirectory;
            SetDiagnostic(
                diagnostic,
                "control checkpoint directory cannot safely serialize publication");
            return result;
        }

        CandidateInventory inventory{};
        if (!InspectCandidates(
                directory.get(),
                directory_status,
                result.filename,
                temporary_name,
                &inventory)) {
            result.error =
                ControlCheckpointPosixStoreErrorV1::
                    kUnsafeDirectory;
            return result;
        }
        result.observed_candidate_count = inventory.count;
        if (inventory.final_present &&
            inventory.temporary_present) {
            result.error =
                ControlCheckpointPosixStoreErrorV1::
                    kCandidateConflict;
            SetDiagnostic(
                diagnostic,
                "control checkpoint final and temporary coexist");
            return result;
        }

        LoadedCandidate retained_candidate{};
        if (inventory.final_present) {
            result.error = LoadExactCandidate(
                directory.get(),
                directory_status,
                result.filename,
                canonical,
                result.checkpoint_sha256,
                durable_frontier,
                &retained_candidate);
            if (result.error !=
                ControlCheckpointPosixStoreErrorV1::kNone) {
                SetDiagnostic(
                    diagnostic,
                    "existing control checkpoint final is unsafe or conflicts");
                return result;
            }
            if (!FsyncNoIntr(
                    retained_candidate.descriptor.get())) {
                result.error =
                    ControlCheckpointPosixStoreErrorV1::
                        kSyncFailure;
                return result;
            }
            result.file_synced = true;
            struct stat synced_status {};
            result.error = ValidateOpenExactCandidate(
                directory.get(),
                directory_status,
                result.filename,
                retained_candidate.descriptor.get(),
                canonical,
                result.checkpoint_sha256,
                durable_frontier,
                &synced_status);
            if (result.error !=
                ControlCheckpointPosixStoreErrorV1::kNone) {
                return result;
            }
            if (!StableFileMetadata(
                    retained_candidate.status,
                    synced_status)) {
                result.error =
                    ControlCheckpointPosixStoreErrorV1::
                        kReadbackFailure;
                return result;
            }
            retained_candidate.status = synced_status;
            result.disposition =
                ControlCheckpointPosixDispositionV1::
                    kAcceptedExistingFinal;
        } else {
            bool adopted = false;
            if (inventory.temporary_present) {
                result.error = LoadExactCandidate(
                    directory.get(),
                    directory_status,
                    temporary_name,
                    canonical,
                    result.checkpoint_sha256,
                    durable_frontier,
                    &retained_candidate);
                if (result.error !=
                    ControlCheckpointPosixStoreErrorV1::kNone) {
                    SetDiagnostic(
                        diagnostic,
                        "existing control checkpoint temporary is partial, unsafe, or conflicting");
                    return result;
                }
                adopted = true;
            } else {
                if (!RequireCandidateState(
                        directory.get(),
                        directory_status,
                        result.filename,
                        temporary_name,
                        false,
                        false)) {
                    result.error =
                        ControlCheckpointPosixStoreErrorV1::
                            kPublishConflict;
                    return result;
                }
                ScopedFd created(OpenAtNoIntr(
                    directory.get(),
                    temporary_name.c_str(),
                    O_RDWR | O_CREAT | O_EXCL |
                        O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC,
                    static_cast<mode_t>(0600U)));
                if (created.get() < 0) {
                    result.error =
                        ControlCheckpointPosixStoreErrorV1::
                            kTemporaryCreate;
                    return result;
                }
                struct stat created_status {};
                if (::fchmod(
                        created.get(),
                        static_cast<mode_t>(0600U)) != 0 ||
                    ::fstat(
                        created.get(), &created_status) != 0 ||
                    !IsSafeFile(
                        created_status, directory_status) ||
                    created_status.st_size != 0 ||
                    !NameMatchesDescriptor(
                        directory.get(),
                        temporary_name.c_str(),
                        created.get(),
                        &created_status) ||
                    !WriteExact(
                        directory.get(),
                        directory_status,
                        temporary_name,
                        created.get(),
                        canonical)) {
                    result.error =
                        ControlCheckpointPosixStoreErrorV1::
                            kWriteFailure;
                    SetDiagnostic(
                        diagnostic,
                        "control checkpoint temporary write was incomplete");
                    return result;
                }
                struct stat exact_status {};
                result.error = ValidateOpenExactCandidate(
                    directory.get(),
                    directory_status,
                    temporary_name,
                    created.get(),
                    canonical,
                    result.checkpoint_sha256,
                    durable_frontier,
                    &exact_status);
                if (result.error !=
                    ControlCheckpointPosixStoreErrorV1::kNone) {
                    return result;
                }
                retained_candidate.descriptor =
                    std::move(created);
                retained_candidate.status = exact_status;
            }

            if (!FsyncNoIntr(
                    retained_candidate.descriptor.get())) {
                result.error =
                    ControlCheckpointPosixStoreErrorV1::
                        kSyncFailure;
                return result;
            }
            result.file_synced = true;
            struct stat pre_publish_status {};
            result.error = ValidateOpenExactCandidate(
                directory.get(),
                directory_status,
                temporary_name,
                retained_candidate.descriptor.get(),
                canonical,
                result.checkpoint_sha256,
                durable_frontier,
                &pre_publish_status);
            if (result.error !=
                ControlCheckpointPosixStoreErrorV1::kNone) {
                return result;
            }
            if (!StableFileMetadata(
                    retained_candidate.status,
                    pre_publish_status)) {
                result.error =
                    ControlCheckpointPosixStoreErrorV1::
                        kReadbackFailure;
                return result;
            }
            retained_candidate.status = pre_publish_status;
            if (!RequireCandidateState(
                    directory.get(),
                    directory_status,
                    result.filename,
                    temporary_name,
                    false,
                    true) ||
                !NameMatchesDescriptor(
                    directory.get(),
                    temporary_name.c_str(),
                    retained_candidate.descriptor.get(),
                    &retained_candidate.status) ||
                !RenameNoReplace(
                    directory.get(),
                    temporary_name.c_str(),
                    result.filename.c_str()) ||
                !RequireCandidateState(
                    directory.get(),
                    directory_status,
                    result.filename,
                    temporary_name,
                    true,
                    false) ||
                !NameMatchesDescriptor(
                    directory.get(),
                    result.filename.c_str(),
                    retained_candidate.descriptor.get())) {
                result.error =
                    ControlCheckpointPosixStoreErrorV1::
                        kPublishConflict;
                SetDiagnostic(
                    diagnostic,
                    "control checkpoint NOREPLACE publication failed");
                return result;
            }
            // renameat2 changes the inode status-change timestamp on Linux,
            // so the temporary-name metadata cannot be the final baseline.
            // Validate the renamed version on both sides of its second fsync
            // and require it not to change across that file barrier.
            struct stat renamed_status {};
            result.error = ValidateOpenExactCandidate(
                directory.get(),
                directory_status,
                result.filename,
                retained_candidate.descriptor.get(),
                canonical,
                result.checkpoint_sha256,
                durable_frontier,
                &renamed_status);
            if (result.error !=
                ControlCheckpointPosixStoreErrorV1::kNone) {
                return result;
            }
            if (!FsyncNoIntr(
                    retained_candidate.descriptor.get())) {
                result.error =
                    ControlCheckpointPosixStoreErrorV1::
                        kSyncFailure;
                return result;
            }
            struct stat published_status {};
            result.error = ValidateOpenExactCandidate(
                directory.get(),
                directory_status,
                result.filename,
                retained_candidate.descriptor.get(),
                canonical,
                result.checkpoint_sha256,
                durable_frontier,
                &published_status);
            if (result.error !=
                ControlCheckpointPosixStoreErrorV1::kNone) {
                return result;
            }
            if (!StableFileMetadata(
                    renamed_status, published_status)) {
                result.error =
                    ControlCheckpointPosixStoreErrorV1::
                        kReadbackFailure;
                return result;
            }
            retained_candidate.status = published_status;
            result.disposition =
                adopted
                    ? ControlCheckpointPosixDispositionV1::
                          kAdoptedCompleteTemporary
                    : ControlCheckpointPosixDispositionV1::
                          kPublishedNew;
        }

        result.error = SyncActualDirectory(
            directory.get(), directory_status);
        if (result.error !=
            ControlCheckpointPosixStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic,
                "actual retained control checkpoint parent did not cross its durability barrier");
            return result;
        }
        result.directory_synced = true;

        LoadedCandidate final_readback{};
        result.error = LoadExactCandidate(
            directory.get(),
            directory_status,
            result.filename,
            canonical,
            result.checkpoint_sha256,
            durable_frontier,
            &final_readback);
        if (result.error !=
            ControlCheckpointPosixStoreErrorV1::kNone) {
            if (result.error !=
                ControlCheckpointPosixStoreErrorV1::
                    kAllocationFailure) {
                result.error =
                    ControlCheckpointPosixStoreErrorV1::
                        kReadbackFailure;
            }
            SetDiagnostic(
                diagnostic,
                "published control checkpoint failed stable decode/hash readback");
            return result;
        }
        struct stat final_status {};
        if (::fstat(
                final_readback.descriptor.get(),
                &final_status) != 0 ||
            !IsSafeFile(final_status, directory_status) ||
            !NameMatchesDescriptor(
                directory.get(),
                result.filename.c_str(),
                final_readback.descriptor.get(),
                &final_status) ||
            !DirectoryMatches(
                directory.get(), directory_status) ||
            !RequireCandidateState(
                directory.get(),
                directory_status,
                result.filename,
                temporary_name,
                true,
                false)) {
            result.error =
                ControlCheckpointPosixStoreErrorV1::
                    kReadbackFailure;
            return result;
        }
        if (retained_candidate.descriptor.get() >= 0) {
            struct stat retained_status {};
            if (::fstat(
                    retained_candidate.descriptor.get(),
                    &retained_status) != 0 ||
                !StableFileMetadata(
                    retained_candidate.status,
                    retained_status) ||
                !StableFileMetadata(
                    retained_status,
                    final_status)) {
                result.error =
                    ControlCheckpointPosixStoreErrorV1::
                        kReadbackFailure;
                return result;
            }
        }

        ScopedFd receipt_directory(OpenAtNoIntr(
            directory.get(),
            ".",
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
                O_NONBLOCK | O_CLOEXEC));
        struct stat receipt_directory_status {};
        if (receipt_directory.get() < 0 ||
            ::fstat(
                receipt_directory.get(),
                &receipt_directory_status) != 0 ||
            !IsSafeDirectory(receipt_directory_status) ||
            !SameInode(
                directory_status,
                receipt_directory_status)) {
            result.error =
                ControlCheckpointPosixStoreErrorV1::
                    kUnsafeDirectory;
            return result;
        }
        if (!ComputeBarrierIdentity(
                checkpoint,
                durable_frontier,
                result.checkpoint_sha256,
                canonical.size(),
                result.filename,
                receipt_directory_status,
                final_status,
                &result.barrier_identity_sha256)) {
            result.error =
                ControlCheckpointPosixStoreErrorV1::
                    kReadbackFailure;
            return result;
        }

        std::string receipt_filename = result.filename;
        if (!UnlockNoIntr(directory.get())) {
            result.error =
                ControlCheckpointPosixStoreErrorV1::
                    kUnsafeDirectory;
            return result;
        }
        result.receipt =
            ControlCheckpointPosixPublishAccessV1::CreateReceipt(
                checkpoint,
                durable_frontier,
                result.checkpoint_sha256,
                result.barrier_identity_sha256,
                canonical.size(),
                std::move(receipt_filename),
                receipt_directory.Release(),
                final_readback.descriptor.Release(),
                receipt_directory_status,
                final_status);
        if (result.receipt == nullptr ||
            !result.receipt->Validate()) {
            result.receipt.reset();
            result.error =
                ControlCheckpointPosixStoreErrorV1::
                    kReadbackFailure;
            SetDiagnostic(
                diagnostic,
                "control checkpoint receipt failed retained-fd validation");
            return result;
        }
        result.error =
            ControlCheckpointPosixStoreErrorV1::kNone;
        SetDiagnostic(diagnostic, "");
        return result;
    } catch (...) {
        result.error =
            ControlCheckpointPosixStoreErrorV1::
                kAllocationFailure;
        SetDiagnostic(
            diagnostic,
            "control checkpoint publication allocation failed");
        return result;
    }
}

}  // namespace

ControlCheckpointPosixPublishResultV1
PublishControlCheckpointV1At(
    int retained_directory_fd,
    std::span<const std::byte> encoded_checkpoint,
    const l2flow::ingress::RawControlSnapshot& durable_frontier,
    std::string* diagnostic) noexcept {
    ControlCheckpointPosixPublishResultV1 result{};
    try {
        ControlDecoderCheckpointV1 checkpoint{};
        std::vector<std::byte> canonical;
        result.error = ValidateCanonicalWire(
            encoded_checkpoint,
            durable_frontier,
            &checkpoint,
            &canonical);
        if (result.error !=
            ControlCheckpointPosixStoreErrorV1::kNone) {
            SetDiagnostic(
                diagnostic, InputDiagnostic(result.error, true));
            return result;
        }
        return PublishCanonicalCheckpointV1At(
            retained_directory_fd,
            checkpoint,
            canonical,
            durable_frontier,
            diagnostic);
    } catch (...) {
        result.error =
            ControlCheckpointPosixStoreErrorV1::
                kAllocationFailure;
        SetDiagnostic(
            diagnostic,
            "control checkpoint wire validation allocation failed");
        return result;
    }
}

ControlCheckpointPosixPublishResultV1
PublishControlCheckpointV1At(
    int retained_directory_fd,
    const ControlDecoderCheckpointV1& checkpoint,
    const l2flow::ingress::RawControlSnapshot& durable_frontier,
    std::string* diagnostic) noexcept {
    ControlCheckpointPosixPublishResultV1 result{};
    if (ValidateControlDecoderCheckpointV1(checkpoint) !=
        ControlCheckpointV1Error::kNone) {
        result.error =
            ControlCheckpointPosixStoreErrorV1::
                kInvalidCheckpoint;
        SetDiagnostic(
            diagnostic,
            "control checkpoint model is invalid");
        return result;
    }
    std::vector<std::byte> encoded;
    const ControlCheckpointV1Error encode_error =
        EncodeControlDecoderCheckpointV1(
            checkpoint, &encoded);
    if (encode_error != ControlCheckpointV1Error::kNone) {
        result.error =
            encode_error ==
                    ControlCheckpointV1Error::kResourceExhausted
                ? ControlCheckpointPosixStoreErrorV1::
                      kAllocationFailure
                : ControlCheckpointPosixStoreErrorV1::
                      kInvalidCheckpoint;
        SetDiagnostic(
            diagnostic,
            "control checkpoint model could not be encoded");
        return result;
    }
    return PublishControlCheckpointV1At(
        retained_directory_fd,
        std::span<const std::byte>(encoded),
        durable_frontier,
        diagnostic);
}

}  // namespace l2flow::control
