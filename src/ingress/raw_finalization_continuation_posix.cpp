#include "l2flow/ingress/raw_finalization_continuation_posix.h"

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_ingress_app.h"
#include "l2flow/ingress/raw_manifest_store.h"
#include "l2flow/ingress/raw_manifest_transition.h"
#include "l2flow/ingress/raw_namespace.h"
#include "l2flow/ingress/raw_reserve_state_posix.h"
#include "l2flow/ingress/raw_v1.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

namespace l2flow::ingress {
namespace {

constexpr unsigned int kRenameNoReplace = 1U;
constexpr std::size_t kIoChunkBytes = 64U * 1024U;

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

[[nodiscard]] bool CheckedAdd(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        right >
            std::numeric_limits<std::uint64_t>::max() -
                left) {
        return false;
    }
    *result = left + right;
    return true;
}

[[nodiscard]] bool CheckedMultiply(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        (left != 0U &&
         right >
             std::numeric_limits<std::uint64_t>::max() /
                 left)) {
        return false;
    }
    *result = left * right;
    return true;
}

[[nodiscard]] bool SameWalSnapshot(
    const RawWalWriterSnapshot& left,
    const RawWalWriterSnapshot& right) noexcept {
    return left.append == right.append &&
           left.durable == right.durable &&
           left.journal_logical_size ==
               right.journal_logical_size &&
           left.initialized == right.initialized &&
           left.sealed == right.sealed &&
           left.closed == right.closed &&
           left.fatal == right.fatal;
}

[[nodiscard]] bool SameHeader(
    const SegmentHeaderV1& left,
    const SegmentHeaderV1& right) noexcept {
    RawV1SegmentHeaderWire left_wire{};
    RawV1SegmentHeaderWire right_wire{};
    return EncodeSegmentHeaderV1(
               left, &left_wire) ==
               RawV1Error::kNone &&
           EncodeSegmentHeaderV1(
               right, &right_wire) ==
               RawV1Error::kNone &&
           left_wire == right_wire;
}

[[nodiscard]] bool SameSealedMetadata(
    const RawSealedSegmentMetadataV1& left,
    const RawSealedSegmentMetadataV1& right) noexcept {
    return SameHeader(left.segment, right.segment) &&
           left.accepted_sealed_marker_bytes ==
               right.accepted_sealed_marker_bytes &&
           left.segment_sha256 ==
               right.segment_sha256 &&
           left.index_sha256 ==
               right.index_sha256 &&
           left.logical_end_offset ==
               right.logical_end_offset &&
           left.record_count == right.record_count &&
           left.actual_first_ingress_sequence ==
               right.actual_first_ingress_sequence &&
           left.actual_last_ingress_sequence ==
               right.actual_last_ingress_sequence;
}

class ScopedFd final {
public:
    explicit ScopedFd(int descriptor = -1) noexcept
        : descriptor_(descriptor) {}
    ~ScopedFd() {
        Reset();
    }
    ScopedFd(const ScopedFd&) = delete;
    ScopedFd& operator=(const ScopedFd&) = delete;
    ScopedFd(ScopedFd&& other) noexcept
        : descriptor_(other.Release()) {}
    ScopedFd& operator=(ScopedFd&& other) noexcept {
        if (this != &other) {
            Reset(other.Release());
        }
        return *this;
    }

    [[nodiscard]] int get() const noexcept {
        return descriptor_;
    }
    [[nodiscard]] int Release() noexcept {
        const int result = descriptor_;
        descriptor_ = -1;
        return result;
    }
    void Reset(int descriptor = -1) noexcept {
        if (descriptor_ >= 0) {
            for (;;) {
                if (::close(descriptor_) == 0 ||
                    errno != EINTR) {
                    break;
                }
            }
        }
        descriptor_ = descriptor;
    }

private:
    int descriptor_ = -1;
};

[[nodiscard]] int FstatNoIntr(
    int descriptor,
    struct stat* status) noexcept {
    if (descriptor < 0 || status == nullptr) {
        return EINVAL;
    }
    for (;;) {
        if (::fstat(descriptor, status) == 0) {
            return 0;
        }
        if (errno != EINTR) {
            return errno;
        }
    }
}

[[nodiscard]] int FstatAtNoIntr(
    int directory_fd,
    const char* name,
    struct stat* status) noexcept {
    if (directory_fd < 0 || name == nullptr ||
        status == nullptr) {
        return EINVAL;
    }
    for (;;) {
        if (::fstatat(
                directory_fd,
                name,
                status,
                AT_SYMLINK_NOFOLLOW) == 0) {
            return 0;
        }
        if (errno != EINTR) {
            return errno;
        }
    }
}

[[nodiscard]] int FsyncNoIntr(int descriptor) noexcept {
    for (;;) {
        if (::fsync(descriptor) == 0) {
            return 0;
        }
        if (errno != EINTR) {
            return errno;
        }
    }
}

[[nodiscard]] int FdatasyncNoIntr(int descriptor) noexcept {
    for (;;) {
        if (::fdatasync(descriptor) == 0) {
            return 0;
        }
        if (errno != EINTR) {
            return errno;
        }
    }
}

[[nodiscard]] int FtruncateNoIntr(
    int descriptor,
    std::uint64_t size) noexcept {
    if (size >
        static_cast<std::uint64_t>(
            std::numeric_limits<off_t>::max())) {
        return EOVERFLOW;
    }
    for (;;) {
        if (::ftruncate(
                descriptor,
                static_cast<off_t>(size)) == 0) {
            return 0;
        }
        if (errno != EINTR) {
            return errno;
        }
    }
}

[[nodiscard]] int OpenAtNoIntr(
    int directory_fd,
    const char* name,
    int flags,
    mode_t mode = 0U) noexcept {
    for (;;) {
        const int descriptor =
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
        if (descriptor >= 0) {
            return descriptor;
        }
        if (errno != EINTR) {
            return -1;
        }
    }
}

[[nodiscard]] bool PreadAll(
    int descriptor,
    std::uint64_t offset,
    std::span<std::byte> output,
    int* error_number) noexcept {
    if (error_number != nullptr) {
        *error_number = 0;
    }
    std::size_t done = 0U;
    while (done < output.size()) {
        const std::uint64_t current =
            offset + static_cast<std::uint64_t>(done);
        if (current >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
            if (error_number != nullptr) {
                *error_number = EOVERFLOW;
            }
            return false;
        }
        const std::size_t request = std::min(
            output.size() - done,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t count = ::pread(
            descriptor,
            output.data() + done,
            request,
            static_cast<off_t>(current));
        if (count > 0) {
            done += static_cast<std::size_t>(count);
            continue;
        }
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (error_number != nullptr) {
            *error_number =
                count == 0 ? EIO : errno;
        }
        return false;
    }
    return true;
}

[[nodiscard]] bool ReadFile(
    int descriptor,
    std::uint64_t size,
    std::string* output,
    int* error_number) noexcept {
    if (output == nullptr ||
        size >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
        if (error_number != nullptr) {
            *error_number = EOVERFLOW;
        }
        return false;
    }
    try {
        output->assign(
            static_cast<std::size_t>(size), '\0');
    } catch (...) {
        if (error_number != nullptr) {
            *error_number = ENOMEM;
        }
        return false;
    }
    return PreadAll(
        descriptor,
        0U,
        std::as_writable_bytes(
            std::span<char>(
                output->data(), output->size())),
        error_number);
}

[[nodiscard]] std::string SegmentName(
    std::uint32_t sequence,
    bool temporary = false) {
    std::array<char, 9U> digits{};
    digits[8U] = '\0';
    for (std::size_t index = 8U;
         index > 0U;
         --index) {
        digits[index - 1U] =
            static_cast<char>(
                '0' + sequence % 10U);
        sequence /= 10U;
    }
    if (temporary) {
        return ".segment-" +
               std::string(digits.data(), 8U) +
               ".raw.raw-segment.tmp";
    }
    return "segment-" +
           std::string(digits.data(), 8U) +
           ".raw";
}

[[nodiscard]] bool ParseSegmentName(
    std::string_view name,
    bool temporary,
    std::uint32_t* sequence) noexcept {
    const std::string_view prefix =
        temporary ? ".segment-" : "segment-";
    const std::string_view suffix =
        temporary
            ? ".raw.raw-segment.tmp"
            : ".raw";
    if (sequence == nullptr ||
        name.size() !=
            prefix.size() + 8U + suffix.size() ||
        !name.starts_with(prefix) ||
        !name.ends_with(suffix)) {
        return false;
    }
    std::uint32_t value = 0U;
    for (std::size_t index = prefix.size();
         index < prefix.size() + 8U;
         ++index) {
        const char digit = name[index];
        if (digit < '0' || digit > '9') {
            return false;
        }
        value =
            value * 10U +
            static_cast<std::uint32_t>(digit - '0');
    }
    if (value == 0U) {
        return false;
    }
    *sequence = value;
    return true;
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
           status.st_nlink == 1 &&
           (status.st_mode & 07777U) == 0600U &&
           status.st_size >= 0 &&
           status.st_blocks >= 0;
}

[[nodiscard]] bool SameInode(
    const struct stat& left,
    const struct stat& right) noexcept {
    return left.st_dev == right.st_dev &&
           left.st_ino == right.st_ino;
}

[[nodiscard]] bool FillRetainedInode(
    const struct stat& status,
    RawFinalizationContinuationRetainedInodeV1*
        output) noexcept {
    if (output == nullptr || status.st_size < 0 ||
        status.st_blocks < 0) {
        return false;
    }
    std::uint64_t blocks = 0U;
    if (!CheckedMultiply(
            static_cast<std::uint64_t>(
                status.st_blocks),
            512U,
            &blocks)) {
        return false;
    }
    output->device =
        static_cast<std::uint64_t>(status.st_dev);
    output->inode =
        static_cast<std::uint64_t>(status.st_ino);
    output->size =
        static_cast<std::uint64_t>(status.st_size);
    output->allocated_bytes = blocks;
    return true;
}

[[nodiscard]] bool RetainedInodeMatches(
    const struct stat& status,
    const RawFinalizationContinuationRetainedInodeV1&
        expected) noexcept {
    RawFinalizationContinuationRetainedInodeV1 actual{};
    return FillRetainedInode(status, &actual) &&
           actual == expected;
}

[[nodiscard]] bool MarkerAt(
    int journal_fd,
    std::uint64_t offset,
    RawV1DurableMarkerWire* wire,
    DurableMarkerV1* marker,
    int* error_number) noexcept {
    if (wire == nullptr || marker == nullptr ||
        !PreadAll(
            journal_fd,
            offset,
            *wire,
            error_number)) {
        return false;
    }
    return DecodeDurableMarkerV1(
               *wire, marker) ==
           RawV1Error::kNone;
}

[[nodiscard]] bool ExactMarker(
    const DurableMarkerV1& marker,
    std::uint32_t source_stream_id,
    std::uint32_t segment_sequence,
    const RawWalCursor& cursor,
    std::uint32_t flags) noexcept {
    return marker.source_stream_id ==
               source_stream_id &&
           marker.segment_sequence ==
               segment_sequence &&
           marker.durable_global_wal_pos ==
               cursor.global_wal_pos &&
           marker.durable_ingress_sequence ==
               cursor.ingress_sequence &&
           marker.durable_segment_offset ==
               cursor.segment_offset &&
           marker.marker_flags == flags;
}

[[nodiscard]] bool ClockNow(
    RawFinalizationContinuationClockNowV1 callback,
    void* context,
    clockid_t clock,
    std::uint64_t* result) noexcept {
    if (result == nullptr) {
        return false;
    }
    if (callback != nullptr) {
        *result = callback(context);
        return *result != 0U;
    }
    struct timespec value {};
    if (::clock_gettime(clock, &value) != 0 ||
        value.tv_sec < 0 || value.tv_nsec < 0 ||
        value.tv_nsec >= 1'000'000'000L) {
        return false;
    }
    const std::uint64_t seconds =
        static_cast<std::uint64_t>(value.tv_sec);
    return CheckedMultiply(
               seconds,
               UINT64_C(1'000'000'000),
               result) &&
           CheckedAdd(
               *result,
               static_cast<std::uint64_t>(
                   value.tv_nsec),
               result);
}

[[nodiscard]] int RenameAt(
    int directory_fd,
    const char* old_name,
    const char* new_name,
    bool no_replace) noexcept {
    for (;;) {
        const int result = static_cast<int>(
            ::syscall(
                SYS_renameat2,
                directory_fd,
                old_name,
                directory_fd,
                new_name,
                no_replace ? kRenameNoReplace : 0U));
        if (result == 0) {
            return 0;
        }
        if (errno != EINTR) {
            return errno;
        }
    }
}

[[nodiscard]] bool IsZeroDigest(
    const RawV1Digest& digest) noexcept {
    return std::all_of(
        digest.begin(),
        digest.end(),
        [](std::byte value) {
            return value == std::byte{0};
        });
}

}  // namespace

struct RawFinalizationContinuationPosixV1::FrozenRecord final {
    CaptureMetaV1 meta{};
    std::array<std::byte, kVendorMessageHeadBytes> head{};
    std::vector<std::byte> body;
    std::vector<std::byte> wire;
};

RawFinalizationContinuationReceiptV1::
~RawFinalizationContinuationReceiptV1() {
    CloseDescriptors();
}

void RawFinalizationContinuationReceiptV1::
CloseDescriptors() noexcept {
    for (int* descriptor :
         {&manifest_fd_,
          &segment_fd_,
          &old_index_fd_,
          &old_segment_fd_,
          &journal_fd_,
          &route_fd_,
          &raw_root_fd_}) {
        if (*descriptor >= 0) {
            static_cast<void>(::close(*descriptor));
            *descriptor = -1;
        }
    }
}

bool RawFinalizationContinuationReceiptV1::Validate(
    std::string* error) const noexcept {
    SetError(error, {});
    if (consumed_ || action_ == nullptr ||
        key_ != action_->key() ||
        generation_token_ !=
            action_->generation_token() ||
        immutable_grant_sha256_ !=
            action_->immutable_grant_sha256() ||
        plan_ != action_->plan() ||
        grant_flags_ != action_->grant_flags() ||
        byte_cap_ != action_->byte_cap() ||
        inode_cap_ != action_->inode_cap() ||
        debit_generation_ !=
            action_->debit_generation() ||
        executor_instance_ !=
            action_->executor_instance() ||
        !action_->ValidateLatest(error)) {
        SetError(
            error,
            "continuation receipt action is stale or inconsistent");
        return false;
    }
    return ValidateRetainedArtifacts(error);
}

bool RawFinalizationContinuationReceiptV1::
ValidateRetainedArtifacts(
    std::string* error) const noexcept {
    SetError(error, {});
    const bool live_mode =
        completion_mode_ ==
        RawFinalizationContinuationCompletionModeV1::
            kLiveExactDrain;
    const bool writer_capability_valid =
        live_mode
            ? retained_writer_ != nullptr &&
                  replacement_writer_lease_ == nullptr &&
                  retained_writer_->identity() ==
                      retained_writer_identity_ &&
                  SameWalSnapshot(
                      retained_writer_->Snapshot(),
                      retained_writer_snapshot_) &&
                  !retained_writer_snapshot_.fatal &&
                  !retained_writer_snapshot_.sealed &&
                  !retained_writer_snapshot_.closed
            : retained_writer_ == nullptr &&
                  replacement_writer_lease_ != nullptr &&
                  replacement_writer_lease_
                          ->descriptor() >= 0 &&
                  replacement_writer_lease_
                          ->source_stream_id() ==
                      continuation_header_
                          .source_stream_id &&
                  replacement_writer_lease_
                          ->capture_date() ==
                      continuation_header_
                          .capture_date;
    if (!writer_capability_valid ||
        raw_root_fd_ < 0 || route_fd_ < 0 ||
        journal_fd_ < 0 || old_segment_fd_ < 0 ||
        old_index_fd_ < 0 || segment_fd_ < 0 ||
        manifest_fd_ < 0 ||
        old_segment_name_.empty() ||
        old_index_name_.empty()) {
        SetError(
            error,
            "continuation receipt writer lease or retained descriptors changed");
        return false;
    }

    struct stat raw_root {};
    struct stat route {};
    struct stat journal {};
    struct stat old_segment {};
    struct stat old_index {};
    struct stat segment {};
    struct stat manifest {};
    struct stat lease {};
    struct stat named {};
    const std::string segment_name =
        SegmentName(
            continuation_header_.segment_sequence);
    if (FstatNoIntr(raw_root_fd_, &raw_root) != 0 ||
        FstatNoIntr(route_fd_, &route) != 0 ||
        FstatNoIntr(journal_fd_, &journal) != 0 ||
        FstatNoIntr(
            old_segment_fd_, &old_segment) != 0 ||
        FstatNoIntr(
            old_index_fd_, &old_index) != 0 ||
        FstatNoIntr(segment_fd_, &segment) != 0 ||
        FstatNoIntr(manifest_fd_, &manifest) != 0 ||
        !IsSafeDirectory(raw_root) ||
        !IsSafeDirectory(route) ||
        !IsSafeFile(journal) ||
        !IsSafeFile(old_segment) ||
        !IsSafeFile(old_index) ||
        !IsSafeFile(segment) ||
        !IsSafeFile(manifest) ||
        !RetainedInodeMatches(
            raw_root, raw_root_inode_) ||
        !RetainedInodeMatches(route, route_inode_) ||
        !RetainedInodeMatches(journal, journal_inode_) ||
        !RetainedInodeMatches(
            old_segment, old_segment_inode_) ||
        !RetainedInodeMatches(
            old_index, old_index_inode_) ||
        !RetainedInodeMatches(segment, segment_inode_) ||
        !RetainedInodeMatches(manifest, manifest_inode_) ||
        FstatAtNoIntr(
            route_fd_,
            kRawJournalFilename,
            &named) != 0 ||
        !SameInode(journal, named) ||
        FstatAtNoIntr(
            route_fd_,
            old_segment_name_.c_str(),
            &named) != 0 ||
        !SameInode(old_segment, named) ||
        FstatAtNoIntr(
            route_fd_,
            old_index_name_.c_str(),
            &named) != 0 ||
        !SameInode(old_index, named) ||
        FstatAtNoIntr(
            route_fd_,
            segment_name.c_str(),
            &named) != 0 ||
        !SameInode(segment, named) ||
        FstatAtNoIntr(
            route_fd_,
            kRawManifestCurrentFilename,
            &named) != 0 ||
        !SameInode(manifest, named)) {
        SetError(
            error,
            "continuation receipt retained inode changed");
        return false;
    }
    if (!live_mode &&
        (FstatNoIntr(
             replacement_writer_lease_
                 ->descriptor(),
             &lease) != 0 ||
         !IsSafeFile(lease) ||
         FstatAtNoIntr(
             route_fd_,
             kRawWriterLeaseFilename,
             &named) != 0 ||
         !SameInode(lease, named))) {
        SetError(
            error,
            "replacement continuation writer lease changed");
        return false;
    }

    RawV1SegmentHeaderWire header{};
    RawV1DurableMarkerWire marker{};
    RawV1DurableMarkerWire sealed_marker{};
    if (!PreadAll(
            segment_fd_, 0U, header, nullptr) ||
        header != continuation_header_wire_ ||
        initialized_snapshot_.journal_logical_size <
            kRawV1DurableMarkerBytes ||
        !PreadAll(
            journal_fd_,
            initialized_snapshot_.journal_logical_size -
                kRawV1DurableMarkerBytes,
            marker,
            nullptr) ||
        marker != header_marker_wire_ ||
        ComputeAcceptedMarkerSha256(marker) !=
            header_marker_sha256_ ||
        !sealed_snapshot_.sealed ||
        sealed_snapshot_.fatal ||
        sealed_snapshot_.append !=
            sealed_snapshot_.durable ||
        sealed_snapshot_.journal_logical_size <
            kRawV1DurableMarkerBytes ||
        !PreadAll(
            journal_fd_,
            sealed_snapshot_.journal_logical_size -
                kRawV1DurableMarkerBytes,
            sealed_marker,
            nullptr) ||
        sealed_marker != sealed_marker_wire_ ||
        ComputeAcceptedMarkerSha256(
            sealed_marker) !=
            sealed_marker_sha256_) {
        SetError(
            error,
            "continuation receipt header or marker changed");
        return false;
    }
    DurableMarkerV1 decoded_seal{};
    RawV1SegmentHeaderWire old_header_wire{};
    SegmentHeaderV1 decoded_old_header{};
    RawV1Digest current_old_segment_sha256{};
    RawV1Digest current_old_index_sha256{};
    RawV1Digest current_segment_sha256{};
    RawV1Digest current_journal_sha256{};
    if (!PreadAll(
            old_segment_fd_,
            0U,
            old_header_wire,
            nullptr) ||
        DecodeSegmentHeaderV1(
            old_header_wire,
            &decoded_old_header) !=
            RawV1Error::kNone ||
        !SameHeader(
            decoded_old_header,
            old_sealed_metadata_.segment) ||
        old_segment_inode_.size !=
            old_sealed_metadata_
                .logical_end_offset ||
        !l2flow::common::ComputeFileSha256ForOpenFd(
            old_segment_fd_,
            &current_old_segment_sha256,
            error,
            old_segment_inode_.size) ||
        current_old_segment_sha256 !=
            old_sealed_metadata_.segment_sha256 ||
        !l2flow::common::ComputeFileSha256ForOpenFd(
            old_index_fd_,
            &current_old_index_sha256,
            error,
            old_index_inode_.size) ||
        current_old_index_sha256 !=
            old_sealed_metadata_.index_sha256 ||
        DecodeDurableMarkerV1(
            sealed_marker,
            &decoded_seal) !=
            RawV1Error::kNone ||
        !ExactMarker(
            decoded_seal,
            continuation_header_.source_stream_id,
            continuation_header_.segment_sequence,
            sealed_snapshot_.durable,
            kRawV1SegmentSealed) ||
        segment_inode_.size !=
            sealed_snapshot_.durable.segment_offset ||
        journal_inode_.size !=
            sealed_snapshot_.journal_logical_size ||
        !l2flow::common::ComputeFileSha256ForOpenFd(
            segment_fd_,
            &current_segment_sha256,
            error,
            segment_inode_.size) ||
        current_segment_sha256 !=
            sealed_segment_sha256_ ||
        !l2flow::common::ComputeFileSha256ForOpenFd(
            journal_fd_,
            &current_journal_sha256,
            error,
            journal_inode_.size) ||
        current_journal_sha256 !=
            sealed_journal_sha256_) {
        SetError(
            error,
            "continuation receipt sealed segment or journal changed");
        return false;
    }

    std::string manifest_bytes;
    int read_error = 0;
    RawManifestV1 parsed{};
    if (manifest.st_size < 0 ||
        !ReadFile(
            manifest_fd_,
            static_cast<std::uint64_t>(
                manifest.st_size),
            &manifest_bytes,
            &read_error) ||
        l2flow::common::ComputeSha256(
            std::as_bytes(
                std::span<const char>(
                    manifest_bytes.data(),
                    manifest_bytes.size()))) !=
            open_manifest_bytes_sha256_ ||
        ParseRawManifestJcs(
            manifest_bytes,
            &parsed) != RawManifestStoreError::kNone ||
        ValidateManifestModel(
            parsed) != RawManifestV1Error::kNone) {
        SetError(
            error,
            "continuation receipt manifest changed");
        return false;
    }
    std::string expected_manifest_bytes;
    if (EncodeRawManifestJcs(
            open_manifest_,
            &expected_manifest_bytes) !=
            RawManifestV1Error::kNone ||
        expected_manifest_bytes != manifest_bytes) {
        SetError(
            error,
            "continuation receipt manifest model changed");
        return false;
    }
    RawV1Digest commitment{};
    std::uint64_t expected_final_ingress = 0U;
    if (ComputeRawManifestOpenEntryCommitmentV1(
            parsed, &commitment) !=
            RawManifestV1Error::kNone ||
        !CheckedAdd(
            continuation_header_
                    .first_ingress_sequence -
                1U,
            recovered_record_count_,
            &expected_final_ingress) ||
        final_cursor_.ingress_sequence !=
            expected_final_ingress ||
        commitment !=
            open_manifest_commitment_sha256_ ||
        allocation_observation_
                .authorized_byte_cap !=
            byte_cap_ ||
        allocation_observation_
                .authorized_inode_cap !=
            inode_cap_ ||
        allocation_observation_
                .continuation_logical_size !=
            segment_inode_.size ||
        allocation_observation_
                .continuation_allocated_bytes !=
            segment_inode_.allocated_bytes ||
        allocation_observation_
                .continuation_allocation_cap >
            byte_cap_ ||
        (live_mode &&
         (frozen_record_count_ == 0U ||
          frozen_framed_wal_bytes_ == 0U ||
          recovered_record_count_ !=
              frozen_record_count_ ||
          recovered_framed_wal_bytes_ !=
              frozen_framed_wal_bytes_)) ||
        (!live_mode &&
         (recovered_record_count_ >
              frozen_record_count_ ||
          recovered_framed_wal_bytes_ >
              frozen_framed_wal_bytes_)) ||
        final_cursor_ !=
            sealed_snapshot_.durable ||
        final_cursor_.segment_offset !=
            kRawV1SegmentHeaderBytes +
                recovered_framed_wal_bytes_ ||
        continuation_header_
                .segment_base_wal_pos +
                final_cursor_.segment_offset !=
            final_cursor_.global_wal_pos ||
        allocation_observation_
                .continuation_logical_size !=
            final_cursor_.segment_offset) {
        SetError(
            error,
            "continuation receipt commitment or allocation changed");
        return false;
    }
    return true;
}

std::string_view
RawFinalizationContinuationFailureV1Name(
    RawFinalizationContinuationFailureV1 failure) noexcept {
    switch (failure) {
        case RawFinalizationContinuationFailureV1::kNone:
            return "NONE";
        case RawFinalizationContinuationFailureV1::kInvalidInput:
            return "INVALID_INPUT";
        case RawFinalizationContinuationFailureV1::kStaleAction:
            return "STALE_ACTION";
        case RawFinalizationContinuationFailureV1::kWrongAction:
            return "WRONG_ACTION";
        case RawFinalizationContinuationFailureV1::kNoLiveAck:
            return "NO_LIVE_ACK";
        case RawFinalizationContinuationFailureV1::kAckMismatch:
            return "ACK_MISMATCH";
        case RawFinalizationContinuationFailureV1::kStateLoad:
            return "STATE_LOAD";
        case RawFinalizationContinuationFailureV1::kGrantMismatch:
            return "GRANT_MISMATCH";
        case RawFinalizationContinuationFailureV1::kCounterMismatch:
            return "COUNTER_MISMATCH";
        case RawFinalizationContinuationFailureV1::kPlanMismatch:
            return "PLAN_MISMATCH";
        case RawFinalizationContinuationFailureV1::kUnsafeNamespace:
            return "UNSAFE_NAMESPACE";
        case RawFinalizationContinuationFailureV1::kOldBoundaryMismatch:
            return "OLD_BOUNDARY_MISMATCH";
        case RawFinalizationContinuationFailureV1::
            kContinuationNotRequired:
            return "CONTINUATION_NOT_REQUIRED";
        case RawFinalizationContinuationFailureV1::kNamespaceConflict:
            return "NAMESPACE_CONFLICT";
        case RawFinalizationContinuationFailureV1::kArtifactPlan:
            return "ARTIFACT_PLAN";
        case RawFinalizationContinuationFailureV1::kArtifactPublish:
            return "ARTIFACT_PUBLISH";
        case RawFinalizationContinuationFailureV1::kManifestLoad:
            return "MANIFEST_LOAD";
        case RawFinalizationContinuationFailureV1::kManifestTransition:
            return "MANIFEST_TRANSITION";
        case RawFinalizationContinuationFailureV1::kManifestPublish:
            return "MANIFEST_PUBLISH";
        case RawFinalizationContinuationFailureV1::kClockFailure:
            return "CLOCK_FAILURE";
        case RawFinalizationContinuationFailureV1::kAllocationFailure:
            return "ALLOCATION_FAILURE";
        case RawFinalizationContinuationFailureV1::kIoFailure:
            return "IO_FAILURE";
    }
    return "UNKNOWN";
}

class RawFinalizationContinuationPosixV1::ArtifactIo final
    : public RawSegmentArtifactIoV1 {
public:
    explicit ArtifactIo(
        RawFinalizationContinuationPosixV1& owner) noexcept
        : owner_(owner) {}

    [[nodiscard]] std::uint64_t
    EffectiveUserId() const noexcept override {
        return static_cast<std::uint64_t>(::geteuid());
    }

    [[nodiscard]] int InspectDescriptor(
        int descriptor,
        RawSegmentArtifactFileInfoV1* info) noexcept override {
        if (info == nullptr) {
            return EINVAL;
        }
        struct stat status {};
        const int stat_error =
            FstatNoIntr(descriptor, &status);
        if (stat_error != 0) {
            return stat_error;
        }
        int flags = ::fcntl(descriptor, F_GETFL);
        if (flags < 0) {
            return errno;
        }
        const int descriptor_flags =
            ::fcntl(descriptor, F_GETFD);
        if (descriptor_flags < 0) {
            return errno;
        }
        FillInfo(
            status,
            flags,
            descriptor_flags,
            info);
        return 0;
    }

    [[nodiscard]] int InspectName(
        int directory_descriptor,
        std::string_view name,
        RawSegmentArtifactFileInfoV1* info) noexcept override {
        std::string terminated;
        if (info == nullptr ||
            !ToName(name, &terminated)) {
            return EINVAL;
        }
        struct stat status {};
        const int error_number =
            FstatAtNoIntr(
                directory_descriptor,
                terminated.c_str(),
                &status);
        if (error_number != 0) {
            return error_number;
        }
        FillInfo(status, -1, -1, info);
        return 0;
    }

    [[nodiscard]] RawSegmentArtifactOpenResultV1
    OpenExisting(
        int directory_descriptor,
        std::string_view name,
        bool writable) noexcept override {
        std::string terminated;
        if (!ToName(name, &terminated)) {
            return {-1, EINVAL};
        }
        const int descriptor = OpenAtNoIntr(
            directory_descriptor,
            terminated.c_str(),
            (writable ? O_RDWR : O_RDONLY) |
                O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME);
        return descriptor >= 0
                   ? RawSegmentArtifactOpenResultV1{
                         descriptor, 0}
                   : RawSegmentArtifactOpenResultV1{
                         -1, errno};
    }

    [[nodiscard]] RawSegmentArtifactOpenResultV1
    CreateExclusive(
        int directory_descriptor,
        std::string_view name) noexcept override {
        std::string terminated;
        std::string error;
        if (!ToName(name, &terminated)) {
            return {-1, EINVAL};
        }
        if (!owner_.BeforeMutation(
                RawFinalizationContinuationMutationV1::
                    kIndexTemporaryCreate,
                &error)) {
            return {-1, owner_.error_number_};
        }
        const int descriptor = OpenAtNoIntr(
            directory_descriptor,
            terminated.c_str(),
            O_RDWR | O_CREAT | O_EXCL |
                O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME,
            0600);
        return descriptor >= 0
                   ? RawSegmentArtifactOpenResultV1{
                         descriptor, 0}
                   : RawSegmentArtifactOpenResultV1{
                         -1, errno};
    }

    [[nodiscard]] RawSegmentArtifactIoStepV1 ReadSome(
        int descriptor,
        std::uint64_t offset,
        std::span<std::byte> output) noexcept override {
        if (offset >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
            return {0U, EOVERFLOW};
        }
        const std::size_t request = std::min(
            output.size(),
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t count = ::pread(
            descriptor,
            output.data(),
            request,
            static_cast<off_t>(offset));
        return count >= 0
                   ? RawSegmentArtifactIoStepV1{
                         static_cast<std::size_t>(count),
                         0}
                   : RawSegmentArtifactIoStepV1{
                         0U, errno};
    }

    [[nodiscard]] RawSegmentArtifactIoStepV1 WriteSome(
        int descriptor,
        std::uint64_t offset,
        std::span<const std::byte> input) noexcept override {
        std::string error;
        if (offset >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
            return {0U, EOVERFLOW};
        }
        if (!owner_.BeforeMutation(
                RawFinalizationContinuationMutationV1::
                    kIndexTemporaryWrite,
                &error)) {
            return {0U, owner_.error_number_};
        }
        const std::size_t request = std::min(
            input.size(),
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t count = ::pwrite(
            descriptor,
            input.data(),
            request,
            static_cast<off_t>(offset));
        return count >= 0
                   ? RawSegmentArtifactIoStepV1{
                         static_cast<std::size_t>(count),
                         0}
                   : RawSegmentArtifactIoStepV1{
                         0U, errno};
    }

    [[nodiscard]] int SyncFile(
        int descriptor) noexcept override {
        std::string error;
        if (!owner_.BeforeMutation(
                RawFinalizationContinuationMutationV1::
                    kIndexTemporarySync,
                &error)) {
            return owner_.error_number_;
        }
        return FsyncNoIntr(descriptor);
    }

    [[nodiscard]] int SyncDirectory(
        int directory_descriptor) noexcept override {
        std::string error;
        if (!owner_.BeforeMutation(
                RawFinalizationContinuationMutationV1::
                    kIndexDirectorySync,
                &error)) {
            return owner_.error_number_;
        }
        return FsyncNoIntr(directory_descriptor);
    }

    [[nodiscard]] int RenameNoReplace(
        int directory_descriptor,
        std::string_view old_name,
        std::string_view new_name) noexcept override {
        std::string old_terminated;
        std::string new_terminated;
        std::string error;
        if (!ToName(old_name, &old_terminated) ||
            !ToName(new_name, &new_terminated)) {
            return EINVAL;
        }
        if (!owner_.BeforeMutation(
                RawFinalizationContinuationMutationV1::
                    kIndexRename,
                &error)) {
            return owner_.error_number_;
        }
        return RenameAt(
            directory_descriptor,
            old_terminated.c_str(),
            new_terminated.c_str(),
            true);
    }

    [[nodiscard]] int NameMatchesDescriptor(
        int directory_descriptor,
        std::string_view name,
        int descriptor) noexcept override {
        std::string terminated;
        struct stat named {};
        struct stat opened {};
        if (!ToName(name, &terminated)) {
            return EINVAL;
        }
        const int named_error =
            FstatAtNoIntr(
                directory_descriptor,
                terminated.c_str(),
                &named);
        const int opened_error =
            FstatNoIntr(descriptor, &opened);
        if (named_error != 0) {
            return named_error;
        }
        if (opened_error != 0) {
            return opened_error;
        }
        return SameInode(named, opened)
                   ? 0
                   : ESTALE;
    }

    void Close(int descriptor) noexcept override {
        if (descriptor >= 0) {
            static_cast<void>(::close(descriptor));
        }
    }

private:
    static bool ToName(
        std::string_view name,
        std::string* output) noexcept {
        if (output == nullptr || name.empty() ||
            name.find('\0') != std::string_view::npos ||
            name.find('/') != std::string_view::npos) {
            return false;
        }
        try {
            output->assign(name);
            return true;
        } catch (...) {
            return false;
        }
    }

    static void FillInfo(
        const struct stat& status,
        int flags,
        int descriptor_flags,
        RawSegmentArtifactFileInfoV1* info) noexcept {
        *info = {};
        info->regular_file = S_ISREG(status.st_mode);
        info->directory = S_ISDIR(status.st_mode);
        info->owner_user_id =
            static_cast<std::uint64_t>(status.st_uid);
        info->permission_bits =
            static_cast<std::uint32_t>(
                status.st_mode & 07777U);
        info->link_count =
            static_cast<std::uint64_t>(status.st_nlink);
        info->size =
            status.st_size < 0
                ? std::numeric_limits<std::uint64_t>::max()
                : static_cast<std::uint64_t>(
                      status.st_size);
        info->device =
            static_cast<std::uint64_t>(status.st_dev);
        info->inode =
            static_cast<std::uint64_t>(status.st_ino);
        info->modification_seconds =
            static_cast<std::int64_t>(
                status.st_mtim.tv_sec);
        info->modification_nanoseconds =
            static_cast<std::int64_t>(
                status.st_mtim.tv_nsec);
        info->change_seconds =
            static_cast<std::int64_t>(
                status.st_ctim.tv_sec);
        info->change_nanoseconds =
            static_cast<std::int64_t>(
                status.st_ctim.tv_nsec);
        info->open_flags = flags;
        info->descriptor_flags = descriptor_flags;
    }

    RawFinalizationContinuationPosixV1& owner_;
};

RawFinalizationContinuationPosixV1::
~RawFinalizationContinuationPosixV1() {
    CloseDescriptors();
}

void RawFinalizationContinuationPosixV1::
CloseDescriptors() noexcept {
    for (int* descriptor :
         {&continuation_fd_,
          &old_segment_fd_,
          &journal_fd_}) {
        if (*descriptor >= 0) {
            static_cast<void>(::close(*descriptor));
            *descriptor = -1;
        }
    }
}

void RawFinalizationContinuationPosixV1::Fail(
    RawFinalizationContinuationFailureV1 failure,
    int error_number,
    std::string* error,
    std::string message) noexcept {
    failure_ = failure;
    error_number_ =
        error_number == 0 ? EINVAL : error_number;
    SetError(error, std::move(message));
}

std::unique_ptr<RawFinalizationContinuationPosixV1>
RawFinalizationContinuationPosixV1::Begin(
    std::unique_ptr<
        RawReserveFinalizationActionV1>&& action,
    std::unique_ptr<
        RawEmergencyWriterAckV1>&& writer_ack,
    RawFinalizationContinuationPosixOptionsV1 options,
    RawFinalizationContinuationFailureV1* failure,
    std::string* error) noexcept {
    if (failure != nullptr) {
        *failure =
            RawFinalizationContinuationFailureV1::kNone;
    }
    SetError(error, {});
    std::unique_ptr<
        RawFinalizationContinuationPosixV1> result(
        new (std::nothrow)
            RawFinalizationContinuationPosixV1());
    if (result == nullptr) {
        if (failure != nullptr) {
            *failure =
                RawFinalizationContinuationFailureV1::
                    kAllocationFailure;
        }
        SetError(
            error,
            "cannot allocate finalization continuation session");
        return nullptr;
    }
    if (!result->Initialize(
            std::move(action),
            std::move(writer_ack),
            options,
            error)) {
        if (failure != nullptr) {
            *failure = result->failure();
        }
        return nullptr;
    }
    return result;
}

std::unique_ptr<RawFinalizationContinuationPosixV1>
RawFinalizationContinuationPosixV1::
BeginReplacementSealOnly(
    std::unique_ptr<
        RawReserveFinalizationActionV1>&& action,
    RawFinalizationContinuationPosixOptionsV1 options,
    RawFinalizationContinuationFailureV1* failure,
    std::string* error) noexcept {
    if (failure != nullptr) {
        *failure =
            RawFinalizationContinuationFailureV1::kNone;
    }
    SetError(error, {});
    std::unique_ptr<
        RawFinalizationContinuationPosixV1> result(
        new (std::nothrow)
            RawFinalizationContinuationPosixV1());
    if (result == nullptr) {
        if (failure != nullptr) {
            *failure =
                RawFinalizationContinuationFailureV1::
                    kAllocationFailure;
        }
        SetError(
            error,
            "cannot allocate replacement continuation session");
        return nullptr;
    }
    if (!result->InitializeReplacement(
            std::move(action),
            options,
            error)) {
        if (failure != nullptr) {
            *failure = result->failure();
        }
        return nullptr;
    }
    return result;
}

bool RawFinalizationContinuationPosixV1::Initialize(
    std::unique_ptr<
        RawReserveFinalizationActionV1>&& action,
    std::unique_ptr<
        RawEmergencyWriterAckV1>&& writer_ack,
    RawFinalizationContinuationPosixOptionsV1 options,
    std::string* error) noexcept {
    if (action == nullptr || writer_ack == nullptr ||
        options.maximum_manifest_bytes == 0U ||
        options.maximum_namespace_entries == 0U ||
        options.artifact_options.maximum_segment_bytes <
            kRawV1SegmentHeaderBytes ||
        IsZeroDigest(
            options.artifact_options
                .expected_raw_schema_sha256)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kInvalidInput,
            EINVAL,
            error,
            "continuation input or operational bound is invalid");
        return false;
    }
    action_ = std::move(action);
    options_ = options;
    ack_facts_ = writer_ack->facts();
    if (!writer_ack->valid() ||
        !ack_facts_.exact() ||
        ack_facts_.queued_record_count == 0U ||
        ack_facts_.queued_framed_wal_bytes == 0U) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kNoLiveAck,
            EPERM,
            error,
            "continuation requires an exact live ACK with a queued suffix");
        return false;
    }
    owner_ = writer_ack->owner_;
    ack_epoch_ = writer_ack->epoch_;

    if (!ValidateLatest(error) ||
        !LoadAndValidateGrant(error)) {
        return false;
    }

    RawWalSink* consumed_writer = nullptr;
    ByteRing* consumed_ring = nullptr;
    if (owner_ == nullptr ||
        !owner_
             ->ConsumeEmergencyWriterAckForFinalization(
                 *writer_ack,
                 &consumed_writer,
                 &consumed_ring,
                 error) ||
        consumed_writer == nullptr ||
        consumed_ring == nullptr) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kNoLiveAck,
            EPERM,
            error,
            "live emergency writer ACK could not be consumed");
        return false;
    }
    writer_ = consumed_writer;
    ring_ = consumed_ring;
    writer_ack.reset();

    if (!CopyAndValidateFrozenRing(error) ||
        !OpenAndValidateNamespace(error) ||
        !InspectContinuationCandidates(error)) {
        return false;
    }

    std::uint64_t created_realtime = 0U;
    std::uint64_t created_monotonic = 0U;
    if (!ClockNow(
            options_.realtime_now,
            options_.realtime_clock_context,
            CLOCK_REALTIME,
            &created_realtime) ||
        !ClockNow(
            options_.monotonic_now,
            options_.monotonic_clock_context,
            CLOCK_MONOTONIC,
            &created_monotonic)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kClockFailure,
            errno == 0 ? EIO : errno,
            error,
            "cannot freeze continuation creation clocks");
        return false;
    }

    const FinalizationActionPlanV1& plan =
        action_->plan();
    continuation_header_ = old_header_;
    continuation_header_.segment_sequence =
        plan.object_sequence;
    continuation_header_.segment_flags =
        kRawV1FinalizationContinuation;
    continuation_header_.segment_base_wal_pos =
        plan.range_start;
    continuation_header_.first_ingress_sequence =
        ack_facts_.wal.append.ingress_sequence + 1U;
    continuation_header_.created_realtime_ns =
        created_realtime;
    continuation_header_.created_monotonic_ns =
        created_monotonic;
    continuation_header_.reserve_state_uuid =
        action_->generation_token()
            .reserve_state_uuid;
    continuation_header_.finalization_cycle_id =
        action_->key().grant
            .finalization_cycle_id;
    continuation_header_.immutable_grant_sha256 =
        action_->immutable_grant_sha256();
    if (EncodeSegmentHeaderV1(
            continuation_header_,
            &continuation_header_wire_) !=
        RawV1Error::kNone) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kPlanMismatch,
            EILSEQ,
            error,
            "continuation plan cannot form a valid Raw V1 header");
        return false;
    }

    initialized_snapshot_ = {};
    initialized_snapshot_.append = {
        continuation_header_.segment_base_wal_pos +
            kRawV1SegmentHeaderBytes,
        continuation_header_.first_ingress_sequence -
            1U,
        kRawV1SegmentHeaderBytes};
    initialized_snapshot_.durable =
        initialized_snapshot_.append;
    initialized_snapshot_.journal_logical_size =
        expected_header_marker_journal_end_;
    initialized_snapshot_.initialized = true;

    const RawManifestTransitionErrorV1 transition =
        TransitionClosedRawManifestToFinalizationContinuationOpenV1(
            closed_manifest_,
            continuation_header_,
            initialized_snapshot_,
            &open_manifest_);
    if (transition !=
        RawManifestTransitionErrorV1::kNone) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kManifestTransition,
            EILSEQ,
            error,
            "closed manifest cannot transition to the exact continuation");
        return false;
    }

    std::string open_manifest_bytes;
    if (EncodeRawManifestJcs(
            open_manifest_,
            &open_manifest_bytes) !=
            RawManifestV1Error::kNone ||
        open_manifest_bytes.size() >
            options_.maximum_manifest_bytes) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kManifestTransition,
            EFBIG,
            error,
            "continuation open manifest exceeds its bound");
        return false;
    }

    const auto RoundCharge =
        [this](
            std::uint64_t bytes,
            std::uint64_t* charge) noexcept {
            if (charge == nullptr ||
                allocation_quantum_bytes_ == 0U ||
                bytes >
                    std::numeric_limits<std::uint64_t>::max() -
                        (allocation_quantum_bytes_ - 1U)) {
                return false;
            }
            *charge =
                ((bytes + allocation_quantum_bytes_ - 1U) /
                 allocation_quantum_bytes_) *
                allocation_quantum_bytes_;
            return true;
        };
    std::uint64_t continuation_charge = 0U;
    std::uint64_t journal_charge = 0U;
    std::uint64_t manifest_charge = 0U;
    std::uint64_t total_charge = 0U;
    if (!RoundCharge(
            continuation_allocation_bytes_,
            &continuation_charge) ||
        !RoundCharge(
            kRawV1DurableMarkerBytes,
            &journal_charge) ||
        !RoundCharge(
            static_cast<std::uint64_t>(
                open_manifest_bytes.size()),
            &manifest_charge) ||
        !CheckedAdd(
            continuation_charge,
            journal_charge,
            &total_charge) ||
        !CheckedAdd(
            total_charge,
            manifest_charge,
            &total_charge) ||
        total_charge > action_->byte_cap() ||
        action_->inode_cap() < 2U) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kPlanMismatch,
            EDQUOT,
            error,
            "continuation receipt caps do not cover segment, journal, and manifest peak charge");
        return false;
    }
    return true;
}

bool RawFinalizationContinuationPosixV1::
InitializeReplacement(
    std::unique_ptr<
        RawReserveFinalizationActionV1>&& action,
    RawFinalizationContinuationPosixOptionsV1 options,
    std::string* error) noexcept {
    if (action == nullptr ||
        options.maximum_manifest_bytes == 0U ||
        options.maximum_namespace_entries == 0U ||
        options.artifact_options.maximum_segment_bytes <
            kRawV1SegmentHeaderBytes ||
        IsZeroDigest(
            options.artifact_options
                .expected_raw_schema_sha256)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kInvalidInput,
            EINVAL,
            error,
            "replacement continuation input or bound is invalid");
        return false;
    }
    completion_mode_ =
        RawFinalizationContinuationCompletionModeV1::
            kReplacementSealOnly;
    action_ = std::move(action);
    options_ = options;
    if (!ValidateLatest(error) ||
        !LoadAndValidateReplacementGrant(error) ||
        !OpenReplacementNamespace(error) ||
        !InspectContinuationCandidates(error) ||
        !target_existed_at_begin_ ||
        !LoadReplacementCandidate(error) ||
        !RecoverReplacementPrefix(error)) {
        if (!target_existed_at_begin_ &&
            failure_ ==
                RawFinalizationContinuationFailureV1::
                    kNone) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kContinuationNotRequired,
                ENOENT,
                error,
                "replacement cannot create an absent continuation");
        }
        return false;
    }

    initialized_snapshot_ = {};
    initialized_snapshot_.append = {
        continuation_header_.segment_base_wal_pos +
            kRawV1SegmentHeaderBytes,
        continuation_header_.first_ingress_sequence -
            1U,
        kRawV1SegmentHeaderBytes};
    initialized_snapshot_.durable =
        initialized_snapshot_.append;
    initialized_snapshot_.journal_logical_size =
        expected_header_marker_journal_end_;
    initialized_snapshot_.initialized = true;

    const RawManifestTransitionErrorV1 transition =
        TransitionClosedRawManifestToFinalizationContinuationOpenV1(
            closed_manifest_,
            continuation_header_,
            initialized_snapshot_,
            &open_manifest_);
    std::string open_bytes;
    std::string observed_open_bytes;
    if (transition !=
            RawManifestTransitionErrorV1::kNone ||
        EncodeRawManifestJcs(
            open_manifest_,
            &open_bytes) !=
            RawManifestV1Error::kNone ||
        open_bytes.size() >
            options_.maximum_manifest_bytes ||
        (replacement_manifest_open_present_ &&
         (EncodeRawManifestJcs(
              initial_manifest_,
              &observed_open_bytes) !=
              RawManifestV1Error::kNone ||
          observed_open_bytes != open_bytes))) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kManifestTransition,
            EILSEQ,
            error,
            "replacement manifest is not the exact continuation open transition");
        return false;
    }
    if ((recovered_record_count_ != 0U ||
         replacement_candidate_sealed_) &&
        (!replacement_header_marker_present_ ||
         !replacement_manifest_open_present_)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kOldBoundaryMismatch,
            EILSEQ,
            error,
            "replacement record prefix lacks earlier header/manifest barriers");
        return false;
    }
    if (replacement_candidate_temporary_ &&
        (recovered_record_count_ != 0U ||
         replacement_header_marker_present_ ||
         replacement_manifest_open_present_ ||
         replacement_candidate_sealed_)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kNamespaceConflict,
            EILSEQ,
            error,
            "temporary replacement candidate is beyond the only adoptable R11 state");
        return false;
    }

    const auto RoundCharge =
        [this](
            std::uint64_t bytes,
            std::uint64_t* charge) noexcept {
            if (charge == nullptr ||
                allocation_quantum_bytes_ == 0U ||
                bytes >
                    std::numeric_limits<std::uint64_t>::max() -
                        (allocation_quantum_bytes_ - 1U)) {
                return false;
            }
            *charge =
                ((bytes + allocation_quantum_bytes_ - 1U) /
                 allocation_quantum_bytes_) *
                allocation_quantum_bytes_;
            return true;
        };
    std::uint64_t segment_charge = 0U;
    std::uint64_t journal_charge = 0U;
    std::uint64_t manifest_charge = 0U;
    std::uint64_t total = 0U;
    if (!RoundCharge(
            continuation_allocation_bytes_,
            &segment_charge) ||
        !RoundCharge(
            kRawV1DurableMarkerBytes,
            &journal_charge) ||
        !RoundCharge(
            open_bytes.size(),
            &manifest_charge) ||
        !CheckedAdd(
            segment_charge,
            journal_charge,
            &total) ||
        !CheckedAdd(total, manifest_charge, &total) ||
        total > action_->byte_cap()) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kPlanMismatch,
            EDQUOT,
            error,
            "replacement continuation exceeds the immutable action cap");
        return false;
    }
    return true;
}

bool RawFinalizationContinuationPosixV1::ValidateLatest(
    std::string* error) noexcept {
    if (action_ == nullptr ||
        !action_->ValidateLatest(error)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kStaleAction,
            ESTALE,
            error,
            "DEBITED continuation action is no longer latest");
        return false;
    }
    return true;
}

bool RawFinalizationContinuationPosixV1::
LoadAndValidateGrant(
    std::string* error) noexcept {
    const ReserveFinalizationActionKeyV1& key =
        action_->key();
    const RawReserveGenerationActionTokenV1& token =
        action_->generation_token();
    const FinalizationActionPlanV1& plan =
        action_->plan();
    if (key.action_kind !=
            FinalizationActionKindV1::kContinuation ||
        key.grant.ack_status !=
            ReserveAckStatusV1::kAcked ||
        action_->grant_flags() !=
            kReserveGrantRawFinalization ||
        action_->target() == nullptr ||
        action_->route_directory_descriptor() < 0 ||
        action_->raw_root_descriptor() < 0 ||
        key.grant.ack_writer_instance !=
            ack_facts_.writer.writer_instance ||
        action_->executor_instance() !=
            key.grant.ack_writer_instance ||
        token.writer_instance_id !=
            key.grant.ack_writer_instance ||
        token.finalization_cycle_id !=
            key.grant.finalization_cycle_id ||
        plan.plan_version != 1U ||
        plan.object_type !=
            FinalizationActionKindV1::kContinuation ||
        plan.plan_flags != 0U ||
        plan.causal_id !=
            key.grant.finalization_cycle_id ||
        plan.object_sequence == 0U ||
        plan.range_end_or_size <
            kRawV1SegmentHeaderBytes) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kWrongAction,
            EPERM,
            error,
            "action is not the exact original-writer ACKED RAW_FINALIZATION continuation");
        return false;
    }

    RawReserveStatePosixError state_failure =
        RawReserveStatePosixError::kNone;
    ReserveStateV1Error codec_failure =
        ReserveStateV1Error::kNone;
    std::unique_ptr<RawReserveStateFileV1> state =
        AttachRawReserveStateAtV1(
            action_->raw_root_descriptor(),
            &state_failure,
            &codec_failure,
            error);
    if (state == nullptr) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kStateLoad,
            EIO,
            error,
            "cannot read the durable grant behind the continuation action");
        return false;
    }
    const ReserveCoordinatorStateV1& model =
        state->state();
    if (model.selected_slot >= model.slots.size()) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kStateLoad,
            EILSEQ,
            error,
            "durable reserve state has no selected slot");
        return false;
    }
    const ReserveStateSlotV1& slot =
        model.slots[model.selected_slot];
    if (slot.coordinator_state !=
            ReserveCoordinatorPhaseV1::kConsumed ||
        slot.generation != token.state_generation ||
        slot.reserve_state_uuid !=
            token.reserve_state_uuid ||
        slot.finalization_cycle_id !=
            key.grant.finalization_cycle_id ||
        slot.active_entry_index >=
            slot.entry_count) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kGrantMismatch,
            ESTALE,
            error,
            "durable continuation grant generation is not ACTIVE");
        return false;
    }
    const std::size_t entry_index =
        slot.active_entry_index;
    const ReserveStateEntryV1& entry =
        slot.entries[entry_index];
    const std::size_t action_index =
        static_cast<std::size_t>(key.action_id);
    if (entry.source_stream_id !=
            key.grant.source_stream_id ||
        entry.capture_date !=
            key.grant.capture_date ||
        entry.stream_day_id !=
            key.grant.stream_day_id ||
        entry.ack_status !=
            ReserveAckStatusV1::kAcked ||
        entry.writer_instance !=
            key.grant.ack_writer_instance ||
        entry.safe_stop_template_id !=
            key.grant.safe_stop_template_id ||
        entry.grant_status !=
            ReserveGrantStatusV1::kActive ||
        entry.grant_flags !=
            kReserveGrantRawFinalization ||
        entry.executor_or_recovery_attempt !=
            key.grant.ack_writer_instance ||
        entry.counter_validity !=
            kReserveCounterValidityMask ||
        entry.continuation_allocation_cap == 0U ||
        action_index >= entry.actions.size()) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kGrantMismatch,
            ESTALE,
            error,
            "durable ACTIVE entry does not match the ACKed continuation grant");
        return false;
    }

    const RawFinalizationCountersV1& counters =
        entry.raw_counters;
    if (counters.callback_published_records !=
            ack_facts_.callback.captured_records ||
        counters.callback_published_vendor_bytes !=
            ack_facts_.callback.captured_vendor_bytes ||
        counters.append_global_wal_pos !=
            ack_facts_.wal.append.global_wal_pos ||
        counters.append_ingress_sequence !=
            ack_facts_.wal.append.ingress_sequence ||
        counters.durable_global_wal_pos !=
            ack_facts_.wal.durable.global_wal_pos ||
        counters.durable_ingress_sequence !=
            ack_facts_.wal.durable.ingress_sequence ||
        counters.queued_record_count !=
            ack_facts_.queued_record_count ||
        counters.queued_framed_wal_bytes !=
            ack_facts_.queued_framed_wal_bytes) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kCounterMismatch,
            EILSEQ,
            error,
            "ACK callback/WAL/ring counters differ from the immutable grant");
        return false;
    }

    const FinalizationActionReceiptV1& receipt =
        entry.actions[action_index];
    if (receipt.action_id != key.action_id ||
        receipt.action_kind != key.action_kind ||
        receipt.action_state !=
            FinalizationActionStateV1::kDebited ||
        receipt.object_plan_sha256 !=
            key.object_plan_sha256 ||
        receipt.debit_generation !=
            action_->debit_generation() ||
        entry.plans[action_index] != plan ||
        plan.range_end_or_size >
            entry.continuation_allocation_cap) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kGrantMismatch,
            ESTALE,
            error,
            "durable DEBITED continuation receipt or cap changed");
        return false;
    }

    bool saw_drain = false;
    bool saw_journal = false;
    bool saw_index = false;
    bool saw_manifest = false;
    for (std::size_t index = 0U;
         index < action_index;
         ++index) {
        const FinalizationActionReceiptV1& predecessor =
            entry.actions[index];
        if (predecessor.action_state !=
            FinalizationActionStateV1::kComplete) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kGrantMismatch,
                EPERM,
                error,
                "continuation predecessors are not a COMPLETE prefix");
            return false;
        }
        saw_drain =
            saw_drain ||
            predecessor.action_kind ==
                FinalizationActionKindV1::
                    kCurrentSegmentDrain;
        saw_journal =
            saw_journal ||
            predecessor.action_kind ==
                FinalizationActionKindV1::kJournalGrowth;
        saw_index =
            saw_index ||
            predecessor.action_kind ==
                FinalizationActionKindV1::kIndex;
        saw_manifest =
            saw_manifest ||
            predecessor.action_kind ==
                FinalizationActionKindV1::kManifest;
    }
    bool saw_future_index = false;
    bool saw_future_manifest = false;
    for (std::size_t index = action_index + 1U;
         index < entry.actions.size();
         ++index) {
        const FinalizationActionReceiptV1& successor =
            entry.actions[index];
        if (successor.action_kind ==
                FinalizationActionKindV1::kUnused) {
            break;
        }
        if (successor.action_state !=
            FinalizationActionStateV1::kPending) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kGrantMismatch,
                EPERM,
                error,
                "continuation successors are not PENDING");
            return false;
        }
        saw_future_index =
            saw_future_index ||
            successor.action_kind ==
                FinalizationActionKindV1::kIndex;
        saw_future_manifest =
            saw_future_manifest ||
            successor.action_kind ==
                FinalizationActionKindV1::kManifest;
    }
    if (!saw_drain || !saw_journal ||
        !saw_index || !saw_manifest ||
        !saw_future_index ||
        !saw_future_manifest) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kGrantMismatch,
            EPERM,
            error,
            "continuation lacks old COMPLETE and continuation-finalization PENDING artifact actions");
        return false;
    }

    ReserveStateV1Digest immutable_hash{};
    if (ComputeImmutableFinalizationGrantSha256V1(
            model.header,
            slot,
            entry_index,
            &immutable_hash) !=
            ReserveStateV1Error::kNone ||
        immutable_hash !=
            action_->immutable_grant_sha256() ||
        model.header.allocation_quantum_bytes == 0U) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kGrantMismatch,
            EILSEQ,
            error,
            "immutable continuation grant hash cannot be reproduced");
        return false;
    }
    allocation_quantum_bytes_ =
        model.header.allocation_quantum_bytes;
    continuation_allocation_bytes_ =
        plan.range_end_or_size;
    return true;
}

bool RawFinalizationContinuationPosixV1::
LoadAndValidateReplacementGrant(
    std::string* error) noexcept {
    const ReserveFinalizationActionKeyV1& key =
        action_->key();
    const RawReserveGenerationActionTokenV1& token =
        action_->generation_token();
    const FinalizationActionPlanV1& plan =
        action_->plan();
    if (key.action_kind !=
            FinalizationActionKindV1::kContinuation ||
        key.grant.ack_status !=
            ReserveAckStatusV1::kAcked ||
        action_->grant_flags() !=
            kReserveGrantRawFinalization ||
        action_->target() == nullptr ||
        action_->route_directory_descriptor() < 0 ||
        action_->raw_root_descriptor() < 0 ||
        l2flow::common::IsZeroIdentity(
            action_->executor_instance()) ||
        action_->executor_instance() ==
            key.grant.ack_writer_instance ||
        token.writer_instance_id !=
            key.grant.ack_writer_instance ||
        token.recovery_attempt_id !=
            action_->executor_instance() ||
        token.finalization_cycle_id !=
            key.grant.finalization_cycle_id ||
        plan.plan_version != 1U ||
        plan.object_type !=
            FinalizationActionKindV1::kContinuation ||
        plan.plan_flags != 0U ||
        plan.causal_id !=
            key.grant.finalization_cycle_id ||
        plan.object_sequence <= 1U ||
        plan.range_end_or_size <
            kRawV1SegmentHeaderBytes) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kWrongAction,
            EPERM,
            error,
            "action is not an exact replacement seal-only continuation token");
        return false;
    }

    RawReserveStatePosixError state_failure =
        RawReserveStatePosixError::kNone;
    ReserveStateV1Error codec_failure =
        ReserveStateV1Error::kNone;
    auto state = AttachRawReserveStateAtV1(
        action_->raw_root_descriptor(),
        &state_failure,
        &codec_failure,
        error);
    if (state == nullptr) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kStateLoad,
            EIO,
            error,
            "cannot reload replacement continuation grant");
        return false;
    }
    const auto& model = state->state();
    if (model.selected_slot >= model.slots.size()) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kStateLoad,
            EILSEQ,
            error,
            "replacement state has no selected slot");
        return false;
    }
    const auto& slot =
        model.slots[model.selected_slot];
    if (slot.coordinator_state !=
            ReserveCoordinatorPhaseV1::kConsumed ||
        slot.generation != token.state_generation ||
        slot.reserve_state_uuid !=
            token.reserve_state_uuid ||
        slot.finalization_cycle_id !=
            key.grant.finalization_cycle_id ||
        slot.active_entry_index >=
            slot.entry_count) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kGrantMismatch,
            ESTALE,
            error,
            "replacement continuation grant is not the durable ACTIVE entry");
        return false;
    }
    const std::size_t entry_index =
        slot.active_entry_index;
    const auto& entry = slot.entries[entry_index];
    const std::size_t action_index =
        key.action_id;
    if (entry.source_stream_id !=
            key.grant.source_stream_id ||
        entry.capture_date !=
            key.grant.capture_date ||
        entry.stream_day_id !=
            key.grant.stream_day_id ||
        entry.ack_status !=
            key.grant.ack_status ||
        entry.writer_instance !=
            key.grant.ack_writer_instance ||
        entry.safe_stop_template_id !=
            key.grant.safe_stop_template_id ||
        entry.grant_status !=
            ReserveGrantStatusV1::kActive ||
        entry.grant_flags !=
            kReserveGrantRawFinalization ||
        entry.executor_or_recovery_attempt !=
            action_->executor_instance() ||
        action_index >= entry.actions.size()) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kGrantMismatch,
            ESTALE,
            error,
            "replacement executor or immutable grant identity changed");
        return false;
    }
    if (entry.counter_validity !=
        kReserveCounterValidityMask) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kCounterMismatch,
            EILSEQ,
            error,
            "replacement counter validity is not the frozen grant value");
        return false;
    }
    const auto& receipt =
        entry.actions[action_index];
    if (receipt.action_id != key.action_id ||
        receipt.action_kind != key.action_kind ||
        receipt.action_state !=
            FinalizationActionStateV1::kDebited ||
        receipt.object_plan_sha256 !=
            key.object_plan_sha256 ||
        receipt.debit_generation !=
            action_->debit_generation() ||
        entry.plans[action_index] != plan ||
        entry.continuation_allocation_cap == 0U ||
        plan.range_end_or_size >
            entry.continuation_allocation_cap) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kGrantMismatch,
            ESTALE,
            error,
            "replacement DEBITED continuation receipt changed");
        return false;
    }

    bool saw_drain = false;
    bool saw_journal = false;
    bool saw_index = false;
    bool saw_manifest = false;
    for (std::size_t index = 0U;
         index < action_index;
         ++index) {
        if (entry.actions[index].action_state !=
            FinalizationActionStateV1::kComplete) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kGrantMismatch,
                EPERM,
                error,
                "replacement continuation predecessors are not COMPLETE");
            return false;
        }
        const auto kind =
            entry.actions[index].action_kind;
        saw_drain =
            saw_drain ||
            kind ==
                FinalizationActionKindV1::
                    kCurrentSegmentDrain;
        saw_journal =
            saw_journal ||
            kind ==
                FinalizationActionKindV1::
                    kJournalGrowth;
        saw_index =
            saw_index ||
            kind ==
                FinalizationActionKindV1::kIndex;
        saw_manifest =
            saw_manifest ||
            kind ==
                FinalizationActionKindV1::kManifest;
    }
    bool future_index = false;
    bool future_manifest = false;
    for (std::size_t index = action_index + 1U;
         index < entry.actions.size();
         ++index) {
        const auto& successor =
            entry.actions[index];
        if (successor.action_kind ==
            FinalizationActionKindV1::kUnused) {
            break;
        }
        if (successor.action_state !=
            FinalizationActionStateV1::kPending) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kGrantMismatch,
                EPERM,
                error,
                "replacement continuation successors are not PENDING");
            return false;
        }
        future_index =
            future_index ||
            successor.action_kind ==
                FinalizationActionKindV1::kIndex;
        future_manifest =
            future_manifest ||
            successor.action_kind ==
                FinalizationActionKindV1::kManifest;
    }
    ReserveStateV1Digest immutable_hash{};
    if (!saw_drain || !saw_journal ||
        !saw_index || !saw_manifest ||
        !future_index || !future_manifest ||
        ComputeImmutableFinalizationGrantSha256V1(
            model.header,
            slot,
            entry_index,
            &immutable_hash) !=
            ReserveStateV1Error::kNone ||
        immutable_hash !=
            action_->immutable_grant_sha256() ||
        model.header.allocation_quantum_bytes == 0U) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kGrantMismatch,
            EILSEQ,
            error,
            "replacement immutable grant or action order is invalid");
        return false;
    }

    grant_ack_status_ = entry.ack_status;
    allocation_quantum_bytes_ =
        model.header.allocation_quantum_bytes;
    continuation_allocation_bytes_ =
        plan.range_end_or_size;
    ack_facts_.writer.writer_instance =
        entry.writer_instance;
    ack_facts_.writer.stream_day_id =
        entry.stream_day_id;
    ack_facts_.writer.source_stream_id =
        entry.source_stream_id;
    ack_facts_.writer.capture_date =
        entry.capture_date;
    ack_facts_.writer.segment_sequence =
        plan.object_sequence - 1U;
    ack_facts_.callback.captured_records =
        entry.raw_counters
            .callback_published_records;
    ack_facts_.callback.captured_vendor_bytes =
        entry.raw_counters
            .callback_published_vendor_bytes;
    if (!CheckedAdd(
            entry.raw_counters
                .append_ingress_sequence,
            entry.raw_counters
                .queued_record_count,
            &ack_facts_.callback
                 .captured_ingress_sequence)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kCounterMismatch,
            EOVERFLOW,
            error,
            "replacement ingress counter range overflows");
        return false;
    }
    ack_facts_.wal.append.global_wal_pos =
        entry.raw_counters.append_global_wal_pos;
    ack_facts_.wal.append.ingress_sequence =
        entry.raw_counters.append_ingress_sequence;
    ack_facts_.wal.durable.global_wal_pos =
        entry.raw_counters.durable_global_wal_pos;
    ack_facts_.wal.durable.ingress_sequence =
        entry.raw_counters.durable_ingress_sequence;
    ack_facts_.queued_record_count =
        entry.raw_counters.queued_record_count;
    ack_facts_.queued_framed_wal_bytes =
        entry.raw_counters.queued_framed_wal_bytes;
    return true;
}

bool RawFinalizationContinuationPosixV1::
CopyAndValidateFrozenRing(
    std::string* error) noexcept {
    if (ring_ == nullptr ||
        ack_facts_.queued_record_count >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max()) ||
        ack_facts_.queued_record_count >
            ack_facts_.ring_used_bytes /
                kMinimumByteRingEntryBytes ||
        ring_->published_position_.load(
            std::memory_order_acquire) !=
            ack_facts_.ring_published_position ||
        ring_->consumed_position_.load(
            std::memory_order_acquire) !=
            ack_facts_.ring_consumed_position ||
        ring_->producer_position_ !=
            ack_facts_.ring_published_position ||
        ring_->consumer_position_ !=
            ack_facts_.ring_consumed_position) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kAckMismatch,
            EILSEQ,
            error,
            "frozen ring positions differ from the consumed ACK");
        return false;
    }

    frozen_record_count_ =
        static_cast<std::size_t>(
            ack_facts_.queued_record_count);
    frozen_records_.reset(
        new (std::nothrow)
            FrozenRecord[frozen_record_count_]);
    if (frozen_records_ == nullptr) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kAllocationFailure,
            ENOMEM,
            error,
            "cannot allocate bounded frozen ring validation records");
        return false;
    }

    std::uint64_t cursor =
        ack_facts_.ring_consumed_position;
    std::uint64_t framed_bytes = 0U;
    std::uint64_t vendor_bytes = 0U;
    std::uint64_t expected_sequence =
        ack_facts_.wal.append.ingress_sequence;
    for (std::size_t index = 0U;
         index < frozen_record_count_;
         ++index) {
        if (cursor >=
                ack_facts_.ring_published_position ||
            ack_facts_.ring_published_position -
                    cursor <
                kMinimumByteRingEntryBytes) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kAckMismatch,
                EILSEQ,
                error,
                "frozen ring ended before its ACKed record count");
            return false;
        }
        CaptureMetaV1 meta{};
        std::array<std::byte, kVendorMessageHeadBytes>
            head{};
        ring_->copy_out(
            cursor, &meta, sizeof(meta));
        ring_->copy_out(
            cursor + sizeof(meta),
            head.data(),
            head.size());
        if (std::to_integer<std::uint8_t>(
                head[0U]) !=
            kVendorMessageHeadBytes) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kAckMismatch,
                EILSEQ,
                error,
                "frozen ring vendor header size is invalid");
            return false;
        }
        const std::uint32_t message_bytes =
            ByteRing::load_u32_le(head, 1U);
        if (message_bytes <
                kVendorMessageHeadBytes ||
            message_bytes >
                ring_->max_message_bytes()) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kAckMismatch,
                EILSEQ,
                error,
                "frozen ring vendor message size is invalid");
            return false;
        }
        const std::size_t body_bytes =
            static_cast<std::size_t>(
                message_bytes -
                kVendorMessageHeadBytes);
        const std::uint64_t entry_bytes =
            static_cast<std::uint64_t>(
                sizeof(CaptureMetaV1)) +
            message_bytes +
            kEntryCommitLengthBytes;
        std::uint64_t next = 0U;
        if (!CheckedAdd(
                cursor, entry_bytes, &next) ||
            next >
                ack_facts_.ring_published_position ||
            ring_->load_ring_u32_le(
                next - kEntryCommitLengthBytes) !=
                entry_bytes) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kAckMismatch,
                EILSEQ,
                error,
                "frozen ring entry commitment is invalid");
            return false;
        }

        std::vector<std::byte> body;
        try {
            body.resize(body_bytes);
        } catch (...) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kAllocationFailure,
                ENOMEM,
                error,
                "cannot allocate bounded frozen vendor body");
            return false;
        }
        if (!body.empty()) {
            ring_->copy_out(
                cursor + sizeof(CaptureMetaV1) +
                    kVendorMessageHeadBytes,
                body.data(),
                body.size());
        }
        if (meta.source_stream_id !=
                ack_facts_.writer.source_stream_id ||
            meta.capture_date !=
                ack_facts_.writer.capture_date ||
            expected_sequence ==
                std::numeric_limits<std::uint64_t>::max() ||
            meta.ingress_sequence !=
                ++expected_sequence) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kAckMismatch,
                EILSEQ,
                error,
                "frozen ring namespace or ingress sequence is not contiguous");
            return false;
        }
        FrozenRecord& frozen =
            frozen_records_[index];
        frozen.meta = meta;
        frozen.head = head;
        frozen.body = std::move(body);
        RawRecordInputV1 input{};
        input.meta = frozen.meta;
        input.vendor_head = frozen.head;
        input.vendor_body = frozen.body;
        if (EncodeRawRecordV1(
                input,
                &frozen.wire,
                nullptr) != RawV1Error::kNone ||
            !CheckedAdd(
                framed_bytes,
                static_cast<std::uint64_t>(
                    frozen.wire.size()),
                &framed_bytes) ||
            !CheckedAdd(
                vendor_bytes,
                message_bytes,
                &vendor_bytes)) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kAckMismatch,
                EILSEQ,
                error,
                "frozen ring record does not encode as exact Raw V1");
            return false;
        }
        cursor = next;
    }

    const std::uint64_t queued_vendor_bytes =
        ack_facts_.callback.captured_vendor_bytes -
        ack_facts_.capture.append.vendor_bytes;
    std::uint64_t required_size = 0U;
    if (cursor !=
            ack_facts_.ring_published_position ||
        framed_bytes !=
            ack_facts_.queued_framed_wal_bytes ||
        vendor_bytes != queued_vendor_bytes ||
        expected_sequence !=
            ack_facts_.callback
                .captured_ingress_sequence ||
        !CheckedAdd(
            kRawV1SegmentHeaderBytes,
            framed_bytes,
            &required_size) ||
        required_size >
            continuation_allocation_bytes_) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kAckMismatch,
            EILSEQ,
            error,
            "frozen ring suffix totals or continuation allocation do not match");
        return false;
    }
    try {
        ring_pop_record_ =
            std::make_unique<ByteRingRecord>(
                ring_->max_body_bytes());
    } catch (...) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kAllocationFailure,
            ENOMEM,
            error,
            "cannot preallocate the frozen ring consume buffer");
        return false;
    }
    return true;
}

bool RawFinalizationContinuationPosixV1::
OpenReplacementNamespace(
    std::string* error) noexcept {
    const int directory_fd =
        action_->route_directory_descriptor();
    const auto* target = action_->target();
    struct stat directory {};
    struct stat lease_name {};
    if (target == nullptr ||
        FstatNoIntr(directory_fd, &directory) != 0 ||
        !IsSafeDirectory(directory) ||
        static_cast<std::uint64_t>(
            directory.st_dev) !=
            target->route_device() ||
        static_cast<std::uint64_t>(
            directory.st_ino) !=
            target->route_inode() ||
        target->source_stream_id() !=
            action_->key().grant
                .source_stream_id ||
        target->capture_date() !=
            action_->key().grant.capture_date ||
        FstatAtNoIntr(
            directory_fd,
            kRawWriterLeaseFilename,
            &lease_name) != 0 ||
        !IsSafeFile(lease_name)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kUnsafeNamespace,
            EACCES,
            error,
            "replacement route or pre-existing writer lease is unsafe");
        return false;
    }
    replacement_writer_lease_ =
        AcquireRawWriterLeaseAt(
            directory_fd,
            action_->key().grant.source_stream_id,
            action_->key().grant.capture_date,
            error);
    if (replacement_writer_lease_ == nullptr) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kNoLiveAck,
            EWOULDBLOCK,
            error,
            "replacement cannot acquire the exact writer lease");
        return false;
    }

    const auto& plan = action_->plan();
    const std::uint32_t old_sequence =
        plan.object_sequence - 1U;
    const std::string old_name =
        SegmentName(old_sequence);
    old_segment_fd_ = OpenAtNoIntr(
        directory_fd,
        old_name.c_str(),
        O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
            O_CLOEXEC | O_NOATIME);
    journal_fd_ = OpenAtNoIntr(
        directory_fd,
        kRawJournalFilename,
        O_RDWR | O_NOFOLLOW | O_NONBLOCK |
            O_CLOEXEC | O_NOATIME);
    struct stat old_status {};
    struct stat journal_status {};
    struct stat named {};
    if (old_segment_fd_ < 0 || journal_fd_ < 0 ||
        FstatNoIntr(
            old_segment_fd_, &old_status) != 0 ||
        FstatNoIntr(
            journal_fd_, &journal_status) != 0 ||
        !IsSafeFile(old_status) ||
        !IsSafeFile(journal_status) ||
        old_status.st_dev != directory.st_dev ||
        journal_status.st_dev != directory.st_dev ||
        FstatAtNoIntr(
            directory_fd,
            old_name.c_str(),
            &named) != 0 ||
        !SameInode(old_status, named) ||
        FstatAtNoIntr(
            directory_fd,
            kRawJournalFilename,
            &named) != 0 ||
        !SameInode(journal_status, named)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kUnsafeNamespace,
            ESTALE,
            error,
            "replacement old segment or journal is unsafe");
        return false;
    }
    old_observed_file_size_ =
        static_cast<std::uint64_t>(
            old_status.st_size);
    if (!CheckedMultiply(
            static_cast<std::uint64_t>(
                old_status.st_blocks),
            512U,
            &old_allocated_bytes_before_) ||
        !CheckedMultiply(
            static_cast<std::uint64_t>(
                journal_status.st_blocks),
            512U,
            &journal_allocated_bytes_before_)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kAllocationFailure,
            EOVERFLOW,
            error,
            "replacement old allocation observation overflows");
        return false;
    }

    RawV1SegmentHeaderWire old_wire{};
    RawV1JournalHeaderWire journal_wire{};
    DurableJournalHeaderV1 journal_header{};
    if (!PreadAll(
            old_segment_fd_,
            0U,
            old_wire,
            nullptr) ||
        DecodeSegmentHeaderV1(
            old_wire, &old_header_) !=
            RawV1Error::kNone ||
        !PreadAll(
            journal_fd_,
            0U,
            journal_wire,
            nullptr) ||
        DecodeDurableJournalHeaderV1(
            journal_wire,
            &journal_header) !=
            RawV1Error::kNone) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kOldBoundaryMismatch,
            EILSEQ,
            error,
            "replacement old Raw header is invalid");
        return false;
    }
    if (old_header_.segment_flags != 0U ||
        old_header_.source_stream_id !=
            action_->key().grant.source_stream_id ||
        old_header_.capture_date !=
            action_->key().grant.capture_date ||
        old_header_.stream_day_id !=
            action_->key().grant.stream_day_id ||
        old_header_.segment_sequence !=
            old_sequence ||
        old_header_.raw_schema_sha256 !=
            options_.artifact_options
                .expected_raw_schema_sha256 ||
        journal_header.source_stream_id !=
            old_header_.source_stream_id ||
        journal_header.capture_date !=
            old_header_.capture_date ||
        journal_header.stream_day_id !=
            old_header_.stream_day_id ||
        journal_header.raw_schema_sha256 !=
            old_header_.raw_schema_sha256 ||
        old_header_.segment_base_wal_pos >
            plan.range_start ||
        plan.range_start -
                old_header_.segment_base_wal_pos !=
            old_observed_file_size_ ||
        plan.range_start !=
            ack_facts_.wal.append
                .global_wal_pos ||
        ack_facts_.wal.durable.global_wal_pos <
            old_header_.segment_base_wal_pos ||
        ack_facts_.wal.durable.global_wal_pos >
            ack_facts_.wal.append.global_wal_pos) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kOldBoundaryMismatch,
            EILSEQ,
            error,
            "replacement old boundary differs from immutable counters");
        return false;
    }
    ack_facts_.writer.segment_base_wal_pos =
        old_header_.segment_base_wal_pos;
    ack_facts_.writer.first_ingress_sequence =
        old_header_.first_ingress_sequence;
    ack_facts_.wal.append.segment_offset =
        old_observed_file_size_;
    ack_facts_.wal.durable.segment_offset =
        ack_facts_.wal.durable.global_wal_pos -
        old_header_.segment_base_wal_pos;

    bool found_pair = false;
    DurableMarkerV1 previous{};
    bool have_previous = false;
    for (std::uint64_t offset =
             kRawV1JournalHeaderBytes;
         offset + kRawV1DurableMarkerBytes <=
         static_cast<std::uint64_t>(
             journal_status.st_size);
         offset += kRawV1DurableMarkerBytes) {
        RawV1DurableMarkerWire wire{};
        DurableMarkerV1 marker{};
        if (!PreadAll(
                journal_fd_, offset, wire, nullptr) ||
            DecodeDurableMarkerV1(
                wire, &marker) !=
                RawV1Error::kNone) {
            break;
        }
        if (have_previous &&
            ExactMarker(
                previous,
                old_header_.source_stream_id,
                old_header_.segment_sequence,
                ack_facts_.wal.durable,
                0U) &&
            ExactMarker(
                marker,
                old_header_.source_stream_id,
                old_header_.segment_sequence,
                ack_facts_.wal.append,
                kRawV1SegmentSealed)) {
            if (found_pair) {
                Fail(
                    RawFinalizationContinuationFailureV1::
                        kOldBoundaryMismatch,
                    EILSEQ,
                    error,
                    "replacement journal has multiple old seal boundaries");
                return false;
            }
            found_pair = true;
            expected_old_journal_prefix_ =
                offset;
            expected_sealed_journal_end_ =
                offset +
                kRawV1DurableMarkerBytes;
            ack_facts_.wal.journal_logical_size =
                expected_old_journal_prefix_;
        }
        previous = marker;
        have_previous = true;
    }
    if (!found_pair ||
        !CheckedAdd(
            expected_sealed_journal_end_,
            kRawV1DurableMarkerBytes,
            &expected_header_marker_journal_end_)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kOldBoundaryMismatch,
            EILSEQ,
            error,
            "replacement journal lacks the exact old durable/seal pair");
        return false;
    }
    RawV1DurableMarkerWire old_seal{};
    std::uint64_t sealed_end = 0U;
    if (!VerifyOldSegmentSealed(
            &old_seal,
            &sealed_end,
            error) ||
        sealed_end !=
            expected_sealed_journal_end_ ||
        !VerifyOldArtifactsAndClosedManifest(
            old_seal, error)) {
        return false;
    }
    return true;
}

bool RawFinalizationContinuationPosixV1::
OpenAndValidateNamespace(
    std::string* error) noexcept {
    const int directory_fd =
        action_->route_directory_descriptor();
    const RawReserveMutationTargetAnchorV1* target =
        action_->target();
    struct stat directory {};
    if (target == nullptr ||
        FstatNoIntr(directory_fd, &directory) != 0 ||
        !IsSafeDirectory(directory) ||
        static_cast<std::uint64_t>(directory.st_dev) !=
            target->route_device() ||
        static_cast<std::uint64_t>(directory.st_ino) !=
            target->route_inode() ||
        target->source_stream_id() !=
            ack_facts_.writer.source_stream_id ||
        target->capture_date() !=
            ack_facts_.writer.capture_date) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kUnsafeNamespace,
            EACCES,
            error,
            "retained continuation route is unsafe or changed");
        return false;
    }

    const std::string old_name =
        SegmentName(
            ack_facts_.writer.segment_sequence);
    old_segment_fd_ = OpenAtNoIntr(
        directory_fd,
        old_name.c_str(),
        O_RDWR | O_NOFOLLOW | O_NONBLOCK |
            O_CLOEXEC | O_NOATIME);
    journal_fd_ = OpenAtNoIntr(
        directory_fd,
        kRawJournalFilename,
        O_RDWR | O_NOFOLLOW | O_NONBLOCK |
            O_CLOEXEC | O_NOATIME);
    struct stat old_status {};
    struct stat journal_status {};
    struct stat named {};
    if (old_segment_fd_ < 0 || journal_fd_ < 0 ||
        FstatNoIntr(
            old_segment_fd_, &old_status) != 0 ||
        FstatNoIntr(
            journal_fd_, &journal_status) != 0 ||
        !IsSafeFile(old_status) ||
        !IsSafeFile(journal_status) ||
        old_status.st_dev != directory.st_dev ||
        journal_status.st_dev != directory.st_dev ||
        FstatAtNoIntr(
            directory_fd,
            old_name.c_str(),
            &named) != 0 ||
        !SameInode(old_status, named) ||
        FstatAtNoIntr(
            directory_fd,
            kRawJournalFilename,
            &named) != 0 ||
        !SameInode(journal_status, named)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kUnsafeNamespace,
            errno == 0 ? EACCES : errno,
            error,
            "old segment or durable journal is unsafe");
        return false;
    }
    old_observed_file_size_ =
        static_cast<std::uint64_t>(
            old_status.st_size);
    if (!CheckedMultiply(
            static_cast<std::uint64_t>(
                old_status.st_blocks),
            512U,
            &old_allocated_bytes_before_) ||
        !CheckedMultiply(
            static_cast<std::uint64_t>(
                journal_status.st_blocks),
            512U,
            &journal_allocated_bytes_before_)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kUnsafeNamespace,
            EOVERFLOW,
            error,
            "old allocation observation overflows");
        return false;
    }

    RawV1SegmentHeaderWire old_wire{};
    RawV1JournalHeaderWire journal_wire{};
    DurableJournalHeaderV1 journal_header{};
    int read_error = 0;
    if (!PreadAll(
            old_segment_fd_,
            0U,
            old_wire,
            &read_error) ||
        DecodeSegmentHeaderV1(
            old_wire, &old_header_) !=
            RawV1Error::kNone ||
        !PreadAll(
            journal_fd_,
            0U,
            journal_wire,
            &read_error) ||
        DecodeDurableJournalHeaderV1(
            journal_wire,
            &journal_header) !=
            RawV1Error::kNone) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kOldBoundaryMismatch,
            read_error == 0 ? EILSEQ : read_error,
            error,
            "old Raw segment or journal header is invalid");
        return false;
    }

    const FinalizationActionPlanV1& plan =
        action_->plan();
    std::uint64_t expected_base = 0U;
    if (old_header_.segment_flags != 0U ||
        old_header_.source_stream_id !=
            action_->key().grant.source_stream_id ||
        old_header_.capture_date !=
            action_->key().grant.capture_date ||
        old_header_.stream_day_id !=
            action_->key().grant.stream_day_id ||
        old_header_.segment_sequence !=
            ack_facts_.writer.segment_sequence ||
        old_header_.segment_base_wal_pos !=
            ack_facts_.writer.segment_base_wal_pos ||
        old_header_.first_ingress_sequence !=
            ack_facts_.writer.first_ingress_sequence ||
        old_header_.raw_schema_sha256 !=
            options_.artifact_options
                .expected_raw_schema_sha256 ||
        journal_header.source_stream_id !=
            old_header_.source_stream_id ||
        journal_header.capture_date !=
            old_header_.capture_date ||
        journal_header.stream_day_id !=
            old_header_.stream_day_id ||
        journal_header.raw_schema_sha256 !=
            old_header_.raw_schema_sha256 ||
        ack_facts_.wal.append.segment_offset <
            kRawV1SegmentHeaderBytes ||
        ack_facts_.wal.append.global_wal_pos !=
            old_header_.segment_base_wal_pos +
                ack_facts_.wal.append.segment_offset ||
        old_observed_file_size_ !=
            ack_facts_.wal.append.segment_offset ||
        !CheckedAdd(
            old_header_.segment_base_wal_pos,
            old_observed_file_size_,
            &expected_base) ||
        plan.object_sequence !=
            old_header_.segment_sequence + 1U ||
        plan.range_start != expected_base ||
        plan.range_start !=
            ack_facts_.wal.append.global_wal_pos ||
        ack_facts_.wal.append.ingress_sequence ==
            std::numeric_limits<std::uint64_t>::max()) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kOldBoundaryMismatch,
            EILSEQ,
            error,
            "old current is not the exact sealed predecessor boundary");
        return false;
    }

    expected_old_journal_prefix_ =
        ack_facts_.wal.journal_logical_size;
    if (expected_old_journal_prefix_ <
            kRawV1JournalHeaderBytes +
                kRawV1DurableMarkerBytes ||
        ((expected_old_journal_prefix_ -
          kRawV1JournalHeaderBytes) %
         kRawV1DurableMarkerBytes) != 0U ||
        !CheckedAdd(
            expected_old_journal_prefix_,
            kRawV1DurableMarkerBytes,
            &expected_sealed_journal_end_) ||
        !CheckedAdd(
            expected_sealed_journal_end_,
            kRawV1DurableMarkerBytes,
            &expected_header_marker_journal_end_)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kOldBoundaryMismatch,
            EOVERFLOW,
            error,
            "ACKed journal prefix is not marker aligned");
        return false;
    }

    RawV1DurableMarkerWire terminal_wire{};
    DurableMarkerV1 terminal{};
    if (!MarkerAt(
            journal_fd_,
            expected_old_journal_prefix_ -
                kRawV1DurableMarkerBytes,
            &terminal_wire,
            &terminal,
            &read_error) ||
        !ExactMarker(
            terminal,
            old_header_.source_stream_id,
            old_header_.segment_sequence,
            ack_facts_.wal.durable,
            0U)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kOldBoundaryMismatch,
            read_error == 0 ? EILSEQ : read_error,
            error,
            "ACK durable cursor is not the terminal pre-seal marker");
        return false;
    }

    RawV1DurableMarkerWire sealed_wire{};
    std::uint64_t sealed_end = 0U;
    if (!VerifyOldSegmentSealed(
            &sealed_wire,
            &sealed_end,
            error) ||
        sealed_end !=
            expected_sealed_journal_end_ ||
        !VerifyOldArtifactsAndClosedManifest(
            sealed_wire, error)) {
        return false;
    }
    return true;
}

bool RawFinalizationContinuationPosixV1::
VerifyOldSegmentSealed(
    RawV1DurableMarkerWire* sealed_marker,
    std::uint64_t* sealed_journal_end,
    std::string* error) noexcept {
    struct stat old_status {};
    struct stat journal_status {};
    int read_error = 0;
    DurableMarkerV1 marker{};
    if (sealed_marker == nullptr ||
        sealed_journal_end == nullptr ||
        FstatNoIntr(
            old_segment_fd_, &old_status) != 0 ||
        FstatNoIntr(
            journal_fd_, &journal_status) != 0 ||
        !IsSafeFile(old_status) ||
        !IsSafeFile(journal_status) ||
        static_cast<std::uint64_t>(
            old_status.st_size) !=
            ack_facts_.wal.append.segment_offset ||
        static_cast<std::uint64_t>(
            journal_status.st_size) <
            expected_sealed_journal_end_ ||
        !MarkerAt(
            journal_fd_,
            expected_old_journal_prefix_,
            sealed_marker,
            &marker,
            &read_error) ||
        !ExactMarker(
            marker,
            old_header_.source_stream_id,
            old_header_.segment_sequence,
            ack_facts_.wal.append,
            kRawV1SegmentSealed)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kOldBoundaryMismatch,
            read_error == 0 ? EILSEQ : read_error,
            error,
            "COMPLETE predecessor lacks the exact old seal boundary");
        return false;
    }
    *sealed_journal_end =
        expected_sealed_journal_end_;
    return true;
}

bool RawFinalizationContinuationPosixV1::
VerifyOldArtifactsAndClosedManifest(
    const RawV1DurableMarkerWire& sealed_marker,
    std::string* error) noexcept {
    ArtifactIo io(*this);
    old_artifact_plan_ =
        PrepareRawSegmentArtifactPlanForFdV1(
            action_->route_directory_descriptor(),
            old_segment_fd_,
            sealed_marker,
            options_.artifact_options,
            io);
    if (!old_artifact_plan_.ok() ||
        !SameHeader(
            old_artifact_plan_.metadata.segment,
            old_header_) ||
        old_artifact_plan_.metadata
                .logical_end_offset !=
            ack_facts_.wal.append.segment_offset) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kArtifactPlan,
            old_artifact_plan_.error_number == 0
                ? EILSEQ
                : old_artifact_plan_.error_number,
            error,
            "old COMPLETE drain cannot be rebuilt as exact sealed Raw");
        return false;
    }
    const RawSegmentArtifactInspectionV1 inspection =
        InspectExistingRawSegmentArtifactAtV1(
            action_->route_directory_descriptor(),
            old_segment_fd_,
            old_artifact_plan_);
    if (!inspection.ok() ||
        inspection.state !=
            RawSegmentArtifactExistingStateV1::
                kMatchingFinal ||
        !SameSealedMetadata(
            inspection.metadata,
            old_artifact_plan_.metadata)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kArtifactPublish,
            inspection.error_number == 0
                ? ENOENT
                : inspection.error_number,
            error,
            "COMPLETE old index predecessor has no exact final artifact");
        return false;
    }

    const RawManifestNamespaceV1 expected_namespace{
        old_header_.capture_date,
        old_header_.source_stream_id,
        old_header_.stream_day_id};
    RawManifestV1 loaded{};
    struct stat manifest_status {};
    const RawManifestStoreError load =
        LoadCurrentRawManifestAt(
            action_->route_directory_descriptor(),
            expected_namespace,
            options_.maximum_manifest_bytes,
            &loaded,
            nullptr,
            nullptr,
            error);
    RawManifestSegmentEntryV1 expected_entry{};
    if (load != RawManifestStoreError::kNone ||
        FstatAtNoIntr(
            action_->route_directory_descriptor(),
            kRawManifestCurrentFilename,
            &manifest_status) != 0 ||
        !IsSafeFile(manifest_status) ||
        !CheckedMultiply(
            static_cast<std::uint64_t>(
                manifest_status.st_blocks),
            512U,
            &manifest_allocated_bytes_before_) ||
        BuildClosedRawManifestEntryV1(
            old_artifact_plan_.metadata,
            &expected_entry) !=
            RawManifestTransitionErrorV1::kNone ||
        loaded.closed_entries.empty() ||
        loaded.closed_entries.back() !=
            expected_entry ||
        loaded.closed_entry_count !=
            loaded.closed_entries.size() ||
        ValidateManifestModel(loaded) !=
            RawManifestV1Error::kNone) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kManifestLoad,
            EILSEQ,
            error,
            "COMPLETE old manifest predecessor is absent or inconsistent");
        return false;
    }
    initial_manifest_ = loaded;
    if (loaded.open_entry.has_value()) {
        if (completion_mode_ !=
                RawFinalizationContinuationCompletionModeV1::
                    kReplacementSealOnly ||
            loaded.manifest_generation <= 1U) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kManifestLoad,
                EILSEQ,
                error,
                "live continuation requires an exact closed manifest predecessor");
            return false;
        }
        replacement_manifest_open_present_ = true;
        closed_manifest_ = loaded;
        closed_manifest_.open_entry.reset();
        --closed_manifest_.manifest_generation;
        if (ValidateManifestModel(
                closed_manifest_) !=
            RawManifestV1Error::kNone) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kManifestLoad,
                EILSEQ,
                error,
                "replacement open manifest has no valid closed predecessor");
            return false;
        }
    } else {
        closed_manifest_ = std::move(loaded);
    }
    return true;
}

bool RawFinalizationContinuationPosixV1::
InspectContinuationCandidates(
    std::string* error) noexcept {
    const int route_fd =
        action_->route_directory_descriptor();
    ScopedFd scan_fd(OpenAtNoIntr(
        route_fd,
        ".",
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
            O_NONBLOCK | O_CLOEXEC | O_NOATIME));
    if (scan_fd.get() < 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kUnsafeNamespace,
            errno,
            error,
            "cannot enumerate continuation namespace");
        return false;
    }
    DIR* raw_directory =
        ::fdopendir(scan_fd.Release());
    if (raw_directory == nullptr) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kUnsafeNamespace,
            errno,
            error,
            "cannot attach continuation directory stream");
        return false;
    }
    std::unique_ptr<DIR, int (*)(DIR*)> directory(
        raw_directory, &::closedir);

    bool target_final = false;
    bool target_tmp = false;
    std::size_t entry_count = 0U;
    errno = 0;
    for (;;) {
        struct dirent* entry =
            ::readdir(directory.get());
        if (entry == nullptr) {
            if (errno != 0) {
                Fail(
                    RawFinalizationContinuationFailureV1::
                        kUnsafeNamespace,
                    errno,
                    error,
                    "continuation directory enumeration failed");
                return false;
            }
            break;
        }
        const std::string_view name(entry->d_name);
        if (name == "." || name == "..") {
            continue;
        }
        ++entry_count;
        if (entry_count >
            options_.maximum_namespace_entries) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kNamespaceConflict,
                E2BIG,
                error,
                "continuation namespace exceeds its enumeration bound");
            return false;
        }

        std::uint32_t sequence = 0U;
        const bool segment =
            ParseSegmentName(
                name, false, &sequence);
        const bool temporary =
            !segment &&
            ParseSegmentName(
                name, true, &sequence);
        if (!segment && !temporary) {
            continue;
        }
        if (sequence >
                old_header_.segment_sequence &&
            sequence !=
                action_->plan().object_sequence) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kNamespaceConflict,
                EEXIST,
                error,
                "a second or non-contiguous segment candidate exists");
            return false;
        }
        if (sequence ==
            action_->plan().object_sequence) {
            if (segment) {
                target_final = true;
            } else {
                target_tmp = true;
            }
        }
        if (segment &&
            sequence <=
                old_header_.segment_sequence) {
            ScopedFd descriptor(OpenAtNoIntr(
                route_fd,
                std::string(name).c_str(),
                O_RDONLY | O_NOFOLLOW |
                    O_NONBLOCK | O_CLOEXEC |
                    O_NOATIME));
            RawV1SegmentHeaderWire wire{};
            SegmentHeaderV1 header{};
            if (descriptor.get() < 0 ||
                !PreadAll(
                    descriptor.get(),
                    0U,
                    wire,
                    nullptr) ||
                DecodeSegmentHeaderV1(
                    wire, &header) !=
                    RawV1Error::kNone ||
                header.segment_sequence != sequence) {
                Fail(
                    RawFinalizationContinuationFailureV1::
                        kNamespaceConflict,
                    EILSEQ,
                    error,
                    "existing segment inventory contains an invalid header");
                return false;
            }
            if ((header.segment_flags &
                 kRawV1FinalizationContinuation) != 0U) {
                Fail(
                    RawFinalizationContinuationFailureV1::
                        kNamespaceConflict,
                    EEXIST,
                    error,
                    "namespace already contains a continuation segment");
                return false;
            }
        }
    }
    if (target_final && target_tmp) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kNamespaceConflict,
            EEXIST,
            error,
            "continuation final and deterministic tmp both exist");
        return false;
    }
    target_existed_at_begin_ =
        target_final || target_tmp;
    return true;
}

bool RawFinalizationContinuationPosixV1::
LoadReplacementCandidate(
    std::string* error) noexcept {
    const int route_fd =
        action_->route_directory_descriptor();
    const auto& plan = action_->plan();
    const std::string final_name =
        SegmentName(plan.object_sequence);
    const std::string temporary_name =
        SegmentName(plan.object_sequence, true);
    struct stat final_status {};
    struct stat temporary_status {};
    const bool have_final =
        FstatAtNoIntr(
            route_fd,
            final_name.c_str(),
            &final_status) == 0;
    const bool have_temporary =
        FstatAtNoIntr(
            route_fd,
            temporary_name.c_str(),
            &temporary_status) == 0;
    if (have_final == have_temporary) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kNamespaceConflict,
            have_final ? EEXIST : ENOENT,
            error,
            "replacement requires exactly one continuation final/tmp candidate");
        return false;
    }
    replacement_candidate_temporary_ =
        have_temporary;
    const char* name =
        have_final
            ? final_name.c_str()
            : temporary_name.c_str();
    continuation_fd_ = OpenAtNoIntr(
        route_fd,
        name,
        O_RDWR | O_NOFOLLOW | O_NONBLOCK |
            O_CLOEXEC | O_NOATIME);
    struct stat status {};
    struct stat named {};
    if (continuation_fd_ < 0 ||
        FstatNoIntr(
            continuation_fd_, &status) != 0 ||
        !IsSafeFile(status) ||
        FstatAtNoIntr(
            route_fd, name, &named) != 0 ||
        !SameInode(status, named) ||
        static_cast<std::uint64_t>(
            status.st_size) <
            kRawV1SegmentHeaderBytes ||
        static_cast<std::uint64_t>(
            status.st_size) >
            continuation_allocation_bytes_ ||
        (have_temporary &&
         static_cast<std::uint64_t>(
             status.st_size) !=
             continuation_allocation_bytes_) ||
        !PreadAll(
            continuation_fd_,
            0U,
            continuation_header_wire_,
            nullptr) ||
        DecodeSegmentHeaderV1(
            continuation_header_wire_,
            &continuation_header_) !=
            RawV1Error::kNone) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kNamespaceConflict,
            EILSEQ,
            error,
            "replacement continuation candidate is unsafe or malformed");
        return false;
    }
    SegmentHeaderV1 normalized =
        continuation_header_;
    normalized.segment_sequence =
        old_header_.segment_sequence;
    normalized.segment_flags =
        old_header_.segment_flags;
    normalized.segment_base_wal_pos =
        old_header_.segment_base_wal_pos;
    normalized.first_ingress_sequence =
        old_header_.first_ingress_sequence;
    normalized.created_realtime_ns =
        old_header_.created_realtime_ns;
    normalized.created_monotonic_ns =
        old_header_.created_monotonic_ns;
    normalized.reserve_state_uuid =
        old_header_.reserve_state_uuid;
    normalized.finalization_cycle_id =
        old_header_.finalization_cycle_id;
    normalized.immutable_grant_sha256 =
        old_header_.immutable_grant_sha256;
    std::uint64_t expected_first_ingress = 0U;
    if (!CheckedAdd(
            ack_facts_.wal.append.ingress_sequence,
            1U,
            &expected_first_ingress) ||
        !SameHeader(normalized, old_header_) ||
        continuation_header_.segment_sequence !=
            plan.object_sequence ||
        continuation_header_.segment_flags !=
            kRawV1FinalizationContinuation ||
        continuation_header_.segment_base_wal_pos !=
            plan.range_start ||
        continuation_header_.first_ingress_sequence !=
            expected_first_ingress ||
        continuation_header_.reserve_state_uuid !=
            action_->generation_token()
                .reserve_state_uuid ||
        continuation_header_.finalization_cycle_id !=
            action_->key().grant
                .finalization_cycle_id ||
        continuation_header_
                .immutable_grant_sha256 !=
            action_->immutable_grant_sha256()) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kNamespaceConflict,
            EILSEQ,
            error,
            "replacement candidate header does not bind the ACTIVE grant");
        return false;
    }

    RawV1DurableMarkerWire marker_wire{};
    DurableMarkerV1 marker{};
    struct stat journal_status {};
    if (FstatNoIntr(
            journal_fd_,
            &journal_status) != 0 ||
        !IsSafeFile(journal_status)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            EIO,
            error,
            "replacement journal changed before candidate validation");
        return false;
    }
    if (static_cast<std::uint64_t>(
            journal_status.st_size) >=
        expected_header_marker_journal_end_) {
        const RawWalCursor initialized{
            continuation_header_
                    .segment_base_wal_pos +
                kRawV1SegmentHeaderBytes,
            continuation_header_
                    .first_ingress_sequence -
                1U,
            kRawV1SegmentHeaderBytes};
        if (!MarkerAt(
                journal_fd_,
                expected_sealed_journal_end_,
                &marker_wire,
                &marker,
                nullptr) ||
            !ExactMarker(
                marker,
                continuation_header_
                    .source_stream_id,
                continuation_header_
                    .segment_sequence,
                initialized,
                0U)) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kOldBoundaryMismatch,
                EILSEQ,
                error,
                "replacement header-only marker conflicts");
            return false;
        }
        replacement_header_marker_present_ = true;
        header_marker_wire_ = marker_wire;
    }
    return true;
}

bool RawFinalizationContinuationPosixV1::
RecoverReplacementPrefix(
    std::string* error) noexcept {
    struct stat segment_status {};
    struct stat journal_status {};
    if (FstatNoIntr(
            continuation_fd_,
            &segment_status) != 0 ||
        FstatNoIntr(
            journal_fd_,
            &journal_status) != 0 ||
        !IsSafeFile(segment_status) ||
        !IsSafeFile(journal_status) ||
        static_cast<std::uint64_t>(
            segment_status.st_size) >
            options_.artifact_options
                .maximum_segment_bytes) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            EIO,
            error,
            "replacement continuation cannot be read within its bound");
        return false;
    }
    std::shared_ptr<std::vector<std::byte>> bytes;
    try {
        bytes =
            std::make_shared<std::vector<std::byte>>();
        bytes->resize(
            static_cast<std::size_t>(
                segment_status.st_size));
    } catch (...) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kAllocationFailure,
            ENOMEM,
            error,
            "cannot allocate bounded replacement continuation scan");
        return false;
    }
    if (!PreadAll(
            continuation_fd_,
            0U,
            std::span<std::byte>(
                bytes->data(), bytes->size()),
            nullptr)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            EIO,
            error,
            "cannot read replacement continuation candidate");
        return false;
    }
    std::shared_ptr<const std::vector<std::byte>>
        immutable = bytes;
    const RawSegmentScanResult scan =
        ScanRawSegmentV1(
            immutable,
            static_cast<std::uint64_t>(
                bytes->size()));
    const std::uint64_t logical_end =
        scan.ok()
            ? static_cast<std::uint64_t>(
                  bytes->size())
            : scan.validated_end_offset;
    if (logical_end <
            kRawV1SegmentHeaderBytes ||
        logical_end >
            static_cast<std::uint64_t>(
                bytes->size()) ||
        (!scan.ok() &&
         scan.error_offset != logical_end)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kNamespaceConflict,
            EILSEQ,
            error,
            "replacement candidate has no exact complete record prefix");
        return false;
    }
    recovered_record_count_ =
        scan.records.size();
    recovered_framed_wal_bytes_ =
        logical_end -
        kRawV1SegmentHeaderBytes;
    if (recovered_record_count_ >
            ack_facts_.queued_record_count ||
        recovered_framed_wal_bytes_ >
            ack_facts_.queued_framed_wal_bytes) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kCounterMismatch,
            EILSEQ,
            error,
            "replacement recovered prefix exceeds the frozen queued suffix");
        return false;
    }
    std::uint64_t expected_sequence =
        continuation_header_
                .first_ingress_sequence -
            1U;
    for (const RawRecordView& record :
         scan.records) {
        if (expected_sequence ==
                std::numeric_limits<
                    std::uint64_t>::max() ||
            record.header().ingress_sequence !=
                ++expected_sequence) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kCounterMismatch,
                EILSEQ,
                error,
                "replacement recovered ingress range is not contiguous");
            return false;
        }
    }
    if (replacement_candidate_temporary_) {
        if (recovered_record_count_ != 0U ||
            std::any_of(
                bytes->begin() +
                    static_cast<std::ptrdiff_t>(
                        kRawV1SegmentHeaderBytes),
                bytes->end(),
                [](std::byte value) {
                    return value != std::byte{0};
                })) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kNamespaceConflict,
                EILSEQ,
                error,
                "replacement tmp is not the exact header-only R11 candidate");
            return false;
        }
    }

    RawWalCursor cursor{
        continuation_header_.segment_base_wal_pos +
            logical_end,
        expected_sequence,
        logical_end};
    const std::uint64_t durable_offset =
        expected_header_marker_journal_end_;
    const std::uint64_t seal_offset =
        durable_offset +
        kRawV1DurableMarkerBytes;
    const std::uint64_t seal_end =
        seal_offset +
        kRawV1DurableMarkerBytes;
    const std::uint64_t journal_size =
        static_cast<std::uint64_t>(
            journal_status.st_size);
    if (journal_size >
            seal_end ||
        (journal_size >
             expected_sealed_journal_end_ &&
         journal_size <
             expected_header_marker_journal_end_)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kNamespaceConflict,
            EILSEQ,
            error,
            "replacement journal is outside the continuation marker FSM");
        return false;
    }
    if (journal_size >=
        durable_offset +
            kRawV1DurableMarkerBytes) {
        RawV1DurableMarkerWire wire{};
        DurableMarkerV1 marker{};
        if (!MarkerAt(
                journal_fd_,
                durable_offset,
                &wire,
                &marker,
                nullptr) ||
            !ExactMarker(
                marker,
                continuation_header_
                    .source_stream_id,
                continuation_header_
                    .segment_sequence,
                cursor,
                0U)) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kNamespaceConflict,
                EILSEQ,
                error,
                "replacement durable marker conflicts with recovered prefix");
            return false;
        }
    }
    if (journal_size >= seal_end) {
        DurableMarkerV1 marker{};
        if (!MarkerAt(
                journal_fd_,
                seal_offset,
                &sealed_marker_wire_,
                &marker,
                nullptr) ||
            !ExactMarker(
                marker,
                continuation_header_
                    .source_stream_id,
                continuation_header_
                    .segment_sequence,
                cursor,
                kRawV1SegmentSealed)) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kNamespaceConflict,
                EILSEQ,
                error,
                "replacement seal marker conflicts with recovered prefix");
            return false;
        }
        replacement_candidate_sealed_ = true;
    }
    return true;
}

bool RawFinalizationContinuationPosixV1::
LiveSessionStillExact(
    std::string* error) noexcept {
    if (owner_ == nullptr ||
        writer_ == nullptr || ring_ == nullptr) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kNoLiveAck,
            EPERM,
            error,
            "continuation session no longer owns a live ACKed ring");
        return false;
    }
    std::lock_guard<std::mutex> lock(
        owner_->lifecycle_mutex_);
    if (owner_->state_.load(
            std::memory_order_acquire) !=
            RawIngressAppState::kStopping ||
        !owner_->emergency_ack_issued_ ||
        !owner_->emergency_ack_consumed_ ||
        owner_->emergency_ack_epoch_ !=
            ack_epoch_ ||
        owner_->sink_.get() != writer_ ||
        &owner_->ring_ != ring_ ||
        owner_->fatal() ||
        writer_->identity() !=
            ack_facts_.writer ||
        !SameWalSnapshot(
            writer_->Snapshot(),
            ack_facts_.wal) ||
        ring_->published_position() !=
            ack_facts_.ring_published_position ||
        (!frozen_suffix_consumed_ &&
         ring_->consumed_position() !=
             ack_facts_.ring_consumed_position) ||
        (frozen_suffix_consumed_ &&
         ring_->consumed_position() !=
             ack_facts_.ring_published_position) ||
        !owner_->capture_worker_->Snapshot()
             .emergency_abandoned) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kNoLiveAck,
            ESTALE,
            error,
            "ACKed writer/ring changed after finalization handoff");
        return false;
    }
    return true;
}

bool RawFinalizationContinuationPosixV1::
BeforeMutation(
    RawFinalizationContinuationMutationV1 mutation,
    std::string* error) noexcept {
    if (!ValidateLatest(error) ||
        (completion_mode_ ==
             RawFinalizationContinuationCompletionModeV1::
                 kLiveExactDrain &&
         !LiveSessionStillExact(error)) ||
        (completion_mode_ ==
             RawFinalizationContinuationCompletionModeV1::
                 kReplacementSealOnly &&
         replacement_writer_lease_ == nullptr)) {
        return false;
    }
    if (completion_mode_ ==
        RawFinalizationContinuationCompletionModeV1::
            kReplacementSealOnly) {
        struct stat retained {};
        struct stat named {};
        if (FstatNoIntr(
                replacement_writer_lease_
                    ->descriptor(),
                &retained) != 0 ||
            !IsSafeFile(retained) ||
            FstatAtNoIntr(
                action_->route_directory_descriptor(),
                kRawWriterLeaseFilename,
                &named) != 0 ||
            !SameInode(retained, named)) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kUnsafeNamespace,
                ESTALE,
                error,
                "replacement writer lease changed before mutation");
            return false;
        }
    }
    if (options_.mutation_hook != nullptr) {
        const int injected =
            options_.mutation_hook(
                options_.mutation_hook_context,
                mutation);
        if (injected != 0) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kIoFailure,
                injected,
                error,
                "continuation mutation hook injected a failure");
            return false;
        }
    }
    return true;
}

bool RawFinalizationContinuationPosixV1::
PublishOrAdoptContinuation(
    std::string* error) noexcept {
    const int route_fd =
        action_->route_directory_descriptor();
    const std::string final_name =
        SegmentName(
            continuation_header_.segment_sequence);
    const std::string temporary_name =
        SegmentName(
            continuation_header_.segment_sequence,
            true);
    struct stat final_status {};
    struct stat temporary_status {};
    const int final_error =
        FstatAtNoIntr(
            route_fd,
            final_name.c_str(),
            &final_status);
    const int temporary_error =
        FstatAtNoIntr(
            route_fd,
            temporary_name.c_str(),
            &temporary_status);
    const bool have_final = final_error == 0;
    const bool have_temporary =
        temporary_error == 0;
    if ((final_error != 0 && final_error != ENOENT) ||
        (temporary_error != 0 &&
         temporary_error != ENOENT) ||
        (have_final && have_temporary)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kNamespaceConflict,
            EEXIST,
            error,
            "continuation final/tmp candidate set is ambiguous");
        return false;
    }

    auto ValidateCandidate =
        [this, error](
            int descriptor,
            bool exact_r11) noexcept {
            struct stat status {};
            RawV1SegmentHeaderWire header{};
            if (FstatNoIntr(descriptor, &status) != 0 ||
                !IsSafeFile(status) ||
                !PreadAll(
                    descriptor,
                    0U,
                    header,
                    nullptr) ||
                header !=
                    continuation_header_wire_ ||
                (completion_mode_ ==
                     RawFinalizationContinuationCompletionModeV1::
                         kLiveExactDrain &&
                 static_cast<std::uint64_t>(
                     status.st_size) !=
                     continuation_allocation_bytes_ &&
                 static_cast<std::uint64_t>(
                     status.st_size) !=
                     kRawV1SegmentHeaderBytes +
                         ack_facts_
                             .queued_framed_wal_bytes) ||
                (completion_mode_ ==
                     RawFinalizationContinuationCompletionModeV1::
                         kReplacementSealOnly &&
                 (static_cast<std::uint64_t>(
                      status.st_size) <
                      kRawV1SegmentHeaderBytes ||
                  static_cast<std::uint64_t>(
                      status.st_size) >
                      continuation_allocation_bytes_))) {
                Fail(
                    RawFinalizationContinuationFailureV1::
                        kNamespaceConflict,
                    EILSEQ,
                    error,
                    "continuation candidate header or bounded size conflicts");
                return false;
            }
            if (exact_r11 &&
                static_cast<std::uint64_t>(
                    status.st_size) !=
                    continuation_allocation_bytes_) {
                Fail(
                    RawFinalizationContinuationFailureV1::
                        kNamespaceConflict,
                    EILSEQ,
                    error,
                    "R11 continuation orphan is not header-only preallocated");
                return false;
            }

            std::array<std::byte, kIoChunkBytes>
                buffer{};
            std::uint64_t offset =
                kRawV1SegmentHeaderBytes;
            while (offset <
                   static_cast<std::uint64_t>(
                       status.st_size)) {
                const std::uint64_t remaining =
                    static_cast<std::uint64_t>(
                        status.st_size) -
                    offset;
                const std::size_t count =
                    static_cast<std::size_t>(
                        std::min<std::uint64_t>(
                            remaining,
                            buffer.size()));
                if (!PreadAll(
                        descriptor,
                        offset,
                        std::span<std::byte>(
                            buffer.data(), count),
                        nullptr)) {
                    Fail(
                        RawFinalizationContinuationFailureV1::
                            kIoFailure,
                        EIO,
                        error,
                        "cannot read continuation candidate tail");
                    return false;
                }
                if (exact_r11 &&
                    std::any_of(
                        buffer.begin(),
                        buffer.begin() +
                            static_cast<
                                std::ptrdiff_t>(count),
                        [](std::byte value) {
                            return value != std::byte{0};
                        })) {
                    Fail(
                        RawFinalizationContinuationFailureV1::
                            kNamespaceConflict,
                        EILSEQ,
                        error,
                        "R11 continuation orphan contains record bytes");
                    return false;
                }
                offset += count;
            }
            return true;
        };

    if (have_final || have_temporary) {
        const char* name =
            have_final
                ? final_name.c_str()
                : temporary_name.c_str();
        ScopedFd candidate(OpenAtNoIntr(
            route_fd,
            name,
            O_RDWR | O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME));
        if (candidate.get() < 0 ||
            !ValidateCandidate(
                candidate.get(),
                (target_existed_at_begin_ &&
                 completion_mode_ ==
                     RawFinalizationContinuationCompletionModeV1::
                         kLiveExactDrain) ||
                    have_temporary)) {
            if (failure_ ==
                RawFinalizationContinuationFailureV1::
                    kNone) {
                Fail(
                    RawFinalizationContinuationFailureV1::
                        kNamespaceConflict,
                    errno,
                    error,
                    "cannot securely open continuation candidate");
            }
            return false;
        }
        if (!BeforeMutation(
                RawFinalizationContinuationMutationV1::
                    kContinuationFileSync,
                error)) {
            return false;
        }
        const int sync_error =
            FsyncNoIntr(candidate.get());
        if (sync_error != 0) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kIoFailure,
                sync_error,
                error,
                "cannot synchronize adopted continuation inode");
            return false;
        }
        if (have_temporary) {
            if (!BeforeMutation(
                    RawFinalizationContinuationMutationV1::
                        kContinuationRename,
                    error)) {
                return false;
            }
            const int rename_error = RenameAt(
                route_fd,
                temporary_name.c_str(),
                final_name.c_str(),
                true);
            if (rename_error != 0) {
                Fail(
                    RawFinalizationContinuationFailureV1::
                        kIoFailure,
                    rename_error,
                    error,
                    "cannot adopt complete continuation tmp with NOREPLACE");
                return false;
            }
        }
        if (!BeforeMutation(
                RawFinalizationContinuationMutationV1::
                    kContinuationDirectorySync,
                error)) {
            return false;
        }
        const int directory_sync =
            FsyncNoIntr(route_fd);
        if (directory_sync != 0) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kIoFailure,
                directory_sync,
                error,
                "cannot synchronize continuation final name");
            return false;
        }
        continuation_fd_ = candidate.Release();
        disposition_ =
            have_temporary
                ? RawFinalizationContinuationDispositionV1::
                      kAdoptedCompleteTemporary
                : RawFinalizationContinuationDispositionV1::
                      kAdoptedPublishedOrphan;
        return true;
    }

    if (!BeforeMutation(
            RawFinalizationContinuationMutationV1::
                kContinuationTemporaryCreate,
            error)) {
        return false;
    }
    ScopedFd temporary(OpenAtNoIntr(
        route_fd,
        temporary_name.c_str(),
        O_RDWR | O_CREAT | O_EXCL |
            O_NOFOLLOW | O_NONBLOCK |
            O_CLOEXEC | O_NOATIME,
        0600));
    if (temporary.get() < 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            errno,
            error,
            "cannot create deterministic continuation tmp");
        return false;
    }

    std::size_t header_done = 0U;
    while (header_done <
           continuation_header_wire_.size()) {
        if (!BeforeMutation(
                RawFinalizationContinuationMutationV1::
                    kContinuationHeaderWrite,
                error)) {
            return false;
        }
        const ssize_t count = ::pwrite(
            temporary.get(),
            continuation_header_wire_.data() +
                header_done,
            continuation_header_wire_.size() -
                header_done,
            static_cast<off_t>(header_done));
        if (count > 0) {
            header_done +=
                static_cast<std::size_t>(count);
            continue;
        }
        if (count < 0 && errno == EINTR) {
            continue;
        }
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            count == 0 ? EIO : errno,
            error,
            "continuation header write failed");
        return false;
    }

    if (!BeforeMutation(
            RawFinalizationContinuationMutationV1::
                kContinuationPreallocate,
            error)) {
        return false;
    }
    if (continuation_allocation_bytes_ >
        static_cast<std::uint64_t>(
            std::numeric_limits<off_t>::max())) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kPlanMismatch,
            EOVERFLOW,
            error,
            "continuation allocation exceeds off_t");
        return false;
    }
    const int allocation_error =
        ::posix_fallocate(
            temporary.get(),
            0,
            static_cast<off_t>(
                continuation_allocation_bytes_));
    if (allocation_error != 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            allocation_error,
            error,
            "bounded continuation preallocation failed");
        return false;
    }
    if (!BeforeMutation(
            RawFinalizationContinuationMutationV1::
                kContinuationFileSync,
            error)) {
        return false;
    }
    const int file_sync =
        FsyncNoIntr(temporary.get());
    if (file_sync != 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            file_sync,
            error,
            "continuation tmp fsync failed");
        return false;
    }
    if (!BeforeMutation(
            RawFinalizationContinuationMutationV1::
                kContinuationRename,
            error)) {
        return false;
    }
    const int rename_error = RenameAt(
        route_fd,
        temporary_name.c_str(),
        final_name.c_str(),
        true);
    if (rename_error != 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            rename_error,
            error,
            "continuation NOREPLACE publication failed");
        return false;
    }
    if (!BeforeMutation(
            RawFinalizationContinuationMutationV1::
                kContinuationDirectorySync,
            error)) {
        return false;
    }
    const int directory_sync =
        FsyncNoIntr(route_fd);
    if (directory_sync != 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            directory_sync,
            error,
            "continuation directory barrier failed");
        return false;
    }
    continuation_fd_ = temporary.Release();
    disposition_ =
        RawFinalizationContinuationDispositionV1::
            kPublishedNew;
    return true;
}

bool RawFinalizationContinuationPosixV1::
PublishHeaderMarker(
    std::uint64_t sealed_journal_end,
    std::string* error) noexcept {
    if (sealed_journal_end !=
            expected_sealed_journal_end_ ||
        continuation_fd_ < 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kOldBoundaryMismatch,
            EINVAL,
            error,
            "continuation header marker predecessor is invalid");
        return false;
    }
    DurableMarkerV1 marker{};
    marker.source_stream_id =
        continuation_header_.source_stream_id;
    marker.segment_sequence =
        continuation_header_.segment_sequence;
    marker.durable_global_wal_pos =
        initialized_snapshot_.durable.global_wal_pos;
    marker.durable_ingress_sequence =
        initialized_snapshot_.durable.ingress_sequence;
    marker.durable_segment_offset =
        initialized_snapshot_.durable.segment_offset;
    marker.marker_flags = 0U;
    if (EncodeDurableMarkerV1(
            marker,
            &header_marker_wire_) !=
        RawV1Error::kNone) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kPlanMismatch,
            EILSEQ,
            error,
            "cannot encode continuation header-only marker");
        return false;
    }

    struct stat journal_status {};
    if (FstatNoIntr(
            journal_fd_, &journal_status) != 0 ||
        !IsSafeFile(journal_status) ||
        static_cast<std::uint64_t>(
            journal_status.st_size) <
            sealed_journal_end ||
        static_cast<std::uint64_t>(
            journal_status.st_size) >
            expected_header_marker_journal_end_ +
                2U * kRawV1DurableMarkerBytes) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kNamespaceConflict,
            EILSEQ,
            error,
            "journal suffix is outside the continuation crash FSM");
        return false;
    }
    if (static_cast<std::uint64_t>(
            journal_status.st_size) >=
        expected_header_marker_journal_end_) {
        RawV1DurableMarkerWire existing{};
        if (!PreadAll(
                journal_fd_,
                sealed_journal_end,
                existing,
                nullptr) ||
            existing != header_marker_wire_) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kNamespaceConflict,
                EILSEQ,
                error,
                "existing continuation header marker conflicts");
            return false;
        }
    } else {
        std::size_t done = 0U;
        while (done < header_marker_wire_.size()) {
            if (!BeforeMutation(
                    RawFinalizationContinuationMutationV1::
                        kHeaderMarkerWrite,
                    error)) {
                return false;
            }
            const ssize_t count = ::pwrite(
                journal_fd_,
                header_marker_wire_.data() + done,
                header_marker_wire_.size() - done,
                static_cast<off_t>(
                    sealed_journal_end + done));
            if (count > 0) {
                done +=
                    static_cast<std::size_t>(count);
                continue;
            }
            if (count < 0 && errno == EINTR) {
                continue;
            }
            Fail(
                RawFinalizationContinuationFailureV1::
                    kIoFailure,
                count == 0 ? EIO : errno,
                error,
                "continuation header marker write failed");
            return false;
        }
    }
    if (!BeforeMutation(
            RawFinalizationContinuationMutationV1::
                kHeaderMarkerJournalSync,
            error)) {
        return false;
    }
    const int sync_error =
        FdatasyncNoIntr(journal_fd_);
    if (sync_error != 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            sync_error,
            error,
            "continuation header marker journal sync failed");
        return false;
    }
    return true;
}

bool RawFinalizationContinuationPosixV1::
PublishManifestModel(
    const RawManifestV1& predecessor,
    const RawManifestV1& candidate,
    bool open_transition,
    std::string* error) noexcept {
    std::string predecessor_bytes;
    std::string candidate_bytes;
    if (EncodeRawManifestJcs(
            predecessor,
            &predecessor_bytes) !=
            RawManifestV1Error::kNone ||
        EncodeRawManifestJcs(
            candidate,
            &candidate_bytes) !=
            RawManifestV1Error::kNone ||
        candidate_bytes.empty() ||
        candidate_bytes.size() >
            options_.maximum_manifest_bytes) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kManifestTransition,
            EFBIG,
            error,
            "manifest transition cannot be encoded within its bound");
        return false;
    }

    const RawManifestNamespaceV1 expected_namespace{
        continuation_header_.capture_date,
        continuation_header_.source_stream_id,
        continuation_header_.stream_day_id};
    RawManifestV1 current{};
    std::string current_bytes;
    if (LoadCurrentRawManifestAt(
            action_->route_directory_descriptor(),
            expected_namespace,
            options_.maximum_manifest_bytes,
            &current,
            &current_bytes,
            nullptr,
            error) != RawManifestStoreError::kNone) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kManifestLoad,
            EIO,
            error,
            "cannot reload current manifest before continuation publication");
        return false;
    }
    const bool already_published =
        current_bytes == candidate_bytes;
    if (!already_published &&
        current_bytes != predecessor_bytes) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kManifestPublish,
            ESTALE,
            error,
            "current manifest is neither predecessor nor exact candidate");
        return false;
    }

    const int route_fd =
        action_->route_directory_descriptor();
    struct stat temporary_status {};
    const int temporary_stat_error =
        FstatAtNoIntr(
            route_fd,
            kRawManifestTemporaryFilename,
            &temporary_status);
    const bool have_temporary =
        temporary_stat_error == 0;
    if (temporary_stat_error != 0 &&
        temporary_stat_error != ENOENT) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kManifestPublish,
            temporary_stat_error,
            error,
            "cannot inspect deterministic manifest tmp");
        return false;
    }
    if (already_published && have_temporary) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kManifestPublish,
            EEXIST,
            error,
            "published manifest and deterministic tmp coexist");
        return false;
    }

    const auto Mutation =
        [open_transition](
            RawFinalizationContinuationMutationV1 closed,
            RawFinalizationContinuationMutationV1 open) {
            return open_transition ? open : closed;
        };

    if (already_published) {
        ScopedFd final_fd(OpenAtNoIntr(
            route_fd,
            kRawManifestCurrentFilename,
            O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME));
        struct stat status {};
        if (final_fd.get() < 0 ||
            FstatNoIntr(
                final_fd.get(), &status) != 0 ||
            !IsSafeFile(status)) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kManifestPublish,
                errno == 0 ? EACCES : errno,
                error,
                "published continuation manifest inode is unsafe");
            return false;
        }
        if (!BeforeMutation(
                Mutation(
                    RawFinalizationContinuationMutationV1::
                        kClosedManifestTemporarySync,
                    RawFinalizationContinuationMutationV1::
                        kOpenManifestTemporarySync),
                error)) {
            return false;
        }
        const int file_sync =
            FsyncNoIntr(final_fd.get());
        if (file_sync != 0) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kManifestPublish,
                file_sync,
                error,
                "cannot re-synchronize adopted continuation manifest");
            return false;
        }
        if (!BeforeMutation(
                Mutation(
                    RawFinalizationContinuationMutationV1::
                        kClosedManifestDirectorySync,
                    RawFinalizationContinuationMutationV1::
                        kOpenManifestDirectorySync),
                error)) {
            return false;
        }
        const int directory_sync =
            FsyncNoIntr(route_fd);
        if (directory_sync != 0) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kManifestPublish,
                directory_sync,
                error,
                "cannot re-synchronize adopted manifest name");
            return false;
        }
        return true;
    }

    ScopedFd current_fd(OpenAtNoIntr(
        route_fd,
        kRawManifestCurrentFilename,
        O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
            O_CLOEXEC | O_NOATIME));
    struct stat current_status {};
    if (current_fd.get() < 0 ||
        FstatNoIntr(
            current_fd.get(),
            &current_status) != 0 ||
        !IsSafeFile(current_status)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kManifestPublish,
            errno == 0 ? EACCES : errno,
            error,
            "manifest predecessor inode is unsafe");
        return false;
    }

    ScopedFd temporary;
    std::size_t completed = 0U;
    if (have_temporary) {
        temporary.Reset(OpenAtNoIntr(
            route_fd,
            kRawManifestTemporaryFilename,
            O_RDWR | O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME));
        struct stat status {};
        if (temporary.get() < 0 ||
            FstatNoIntr(
                temporary.get(), &status) != 0 ||
            !IsSafeFile(status) ||
            static_cast<std::uint64_t>(
                status.st_size) >
                candidate_bytes.size()) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kManifestPublish,
                EILSEQ,
                error,
                "deterministic manifest tmp is unsafe or oversized");
            return false;
        }
        completed =
            static_cast<std::size_t>(status.st_size);
        std::string prefix;
        int read_error = 0;
        if (!ReadFile(
                temporary.get(),
                completed,
                &prefix,
                &read_error) ||
            !std::equal(
                prefix.begin(),
                prefix.end(),
                candidate_bytes.begin())) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kManifestPublish,
                read_error == 0 ? EILSEQ : read_error,
                error,
                "deterministic manifest tmp is not an exact candidate prefix");
            return false;
        }
    } else {
        if (!BeforeMutation(
                Mutation(
                    RawFinalizationContinuationMutationV1::
                        kClosedManifestTemporaryCreate,
                    RawFinalizationContinuationMutationV1::
                        kOpenManifestTemporaryCreate),
                error)) {
            return false;
        }
        temporary.Reset(OpenAtNoIntr(
            route_fd,
            kRawManifestTemporaryFilename,
            O_RDWR | O_CREAT | O_EXCL |
                O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME,
            0600));
        if (temporary.get() < 0) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kManifestPublish,
                errno,
                error,
                "cannot create deterministic manifest tmp");
            return false;
        }
    }

    while (completed < candidate_bytes.size()) {
        if (!BeforeMutation(
                Mutation(
                    RawFinalizationContinuationMutationV1::
                        kClosedManifestTemporaryWrite,
                    RawFinalizationContinuationMutationV1::
                        kOpenManifestTemporaryWrite),
                error)) {
            return false;
        }
        const std::size_t request = std::min(
            candidate_bytes.size() - completed,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t count = ::pwrite(
            temporary.get(),
            candidate_bytes.data() + completed,
            request,
            static_cast<off_t>(completed));
        if (count > 0) {
            completed +=
                static_cast<std::size_t>(count);
            continue;
        }
        if (count < 0 && errno == EINTR) {
            continue;
        }
        Fail(
            RawFinalizationContinuationFailureV1::
                kManifestPublish,
            count == 0 ? EIO : errno,
            error,
            "manifest candidate write failed");
        return false;
    }
    if (!BeforeMutation(
            Mutation(
                RawFinalizationContinuationMutationV1::
                    kClosedManifestTemporarySync,
                RawFinalizationContinuationMutationV1::
                    kOpenManifestTemporarySync),
            error)) {
        return false;
    }
    const int temporary_sync =
        FsyncNoIntr(temporary.get());
    if (temporary_sync != 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kManifestPublish,
            temporary_sync,
            error,
            "manifest candidate fsync failed");
        return false;
    }

    struct stat named_current {};
    struct stat retained_current {};
    if (FstatAtNoIntr(
            route_fd,
            kRawManifestCurrentFilename,
            &named_current) != 0 ||
        FstatNoIntr(
            current_fd.get(),
            &retained_current) != 0 ||
        !SameInode(
            named_current,
            retained_current)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kManifestPublish,
            ESTALE,
            error,
            "manifest predecessor name changed before replacement");
        return false;
    }
    if (!BeforeMutation(
            Mutation(
                RawFinalizationContinuationMutationV1::
                    kClosedManifestRename,
                RawFinalizationContinuationMutationV1::
                    kOpenManifestRename),
            error)) {
        return false;
    }
    const int rename_error = RenameAt(
        route_fd,
        kRawManifestTemporaryFilename,
        kRawManifestCurrentFilename,
        false);
    if (rename_error != 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kManifestPublish,
            rename_error,
            error,
            "manifest atomic replacement failed");
        return false;
    }
    if (!BeforeMutation(
            Mutation(
                RawFinalizationContinuationMutationV1::
                    kClosedManifestDirectorySync,
                RawFinalizationContinuationMutationV1::
                    kOpenManifestDirectorySync),
            error)) {
        return false;
    }
    const int directory_sync =
        FsyncNoIntr(route_fd);
    if (directory_sync != 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kManifestPublish,
            directory_sync,
            error,
            "manifest directory barrier failed");
        return false;
    }

    RawManifestV1 readback{};
    std::string readback_bytes;
    if (LoadCurrentRawManifestAt(
            route_fd,
            expected_namespace,
            options_.maximum_manifest_bytes,
            &readback,
            &readback_bytes,
            nullptr,
            error,
            &predecessor) !=
            RawManifestStoreError::kNone ||
        readback_bytes != candidate_bytes) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kManifestPublish,
            EILSEQ,
            error,
            "published continuation manifest failed strict readback");
        return false;
    }
    return true;
}

bool RawFinalizationContinuationPosixV1::
PublishOpenManifest(
    std::string* error) noexcept {
    if (!PublishManifestModel(
            closed_manifest_,
            open_manifest_,
            true,
            error)) {
        return false;
    }
    RawV1Digest commitment{};
    if (ComputeRawManifestOpenEntryCommitmentV1(
            open_manifest_,
            &commitment) !=
            RawManifestV1Error::kNone ||
        IsZeroDigest(commitment)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kManifestTransition,
            EILSEQ,
            error,
            "published continuation manifest has no open-entry commitment");
        return false;
    }
    return true;
}

bool RawFinalizationContinuationPosixV1::
DrainFrozenSuffixAndSeal(
    std::string* error) noexcept {
    if (continuation_fd_ < 0 ||
        frozen_records_ == nullptr ||
        frozen_record_count_ == 0U) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kInvalidInput,
            EINVAL,
            error,
            "continuation drain has no frozen suffix or retained inode");
        return false;
    }

    const std::uint64_t logical_end =
        kRawV1SegmentHeaderBytes +
        ack_facts_.queued_framed_wal_bytes;
    struct stat segment_status {};
    if (FstatNoIntr(
            continuation_fd_,
            &segment_status) != 0 ||
        !IsSafeFile(segment_status) ||
        (static_cast<std::uint64_t>(
             segment_status.st_size) !=
             continuation_allocation_bytes_ &&
         static_cast<std::uint64_t>(
             segment_status.st_size) != logical_end)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kNamespaceConflict,
            EILSEQ,
            error,
            "continuation inode is outside the bounded drain FSM");
        return false;
    }

    std::uint64_t offset =
        kRawV1SegmentHeaderBytes;
    for (std::size_t index = 0U;
         index < frozen_record_count_;
         ++index) {
        const std::vector<std::byte>& expected =
            frozen_records_[index].wire;
        std::vector<std::byte> actual;
        try {
            actual.resize(expected.size());
        } catch (...) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kAllocationFailure,
                ENOMEM,
                error,
                "cannot allocate bounded continuation record verification");
            return false;
        }
        if (!PreadAll(
                continuation_fd_,
                offset,
                actual,
                nullptr)) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kIoFailure,
                EIO,
                error,
                "cannot inspect continuation record crash prefix");
            return false;
        }
        if (actual != expected) {
            for (std::size_t byte_index = 0U;
                 byte_index < actual.size();
                 ++byte_index) {
                if (actual[byte_index] != std::byte{0} &&
                    actual[byte_index] !=
                        expected[byte_index]) {
                    Fail(
                        RawFinalizationContinuationFailureV1::
                            kNamespaceConflict,
                        EILSEQ,
                        error,
                        "continuation record crash prefix conflicts with frozen ring");
                    return false;
                }
            }
            std::size_t completed = 0U;
            while (completed < expected.size()) {
                if (!BeforeMutation(
                        RawFinalizationContinuationMutationV1::
                            kContinuationRecordWrite,
                        error)) {
                    return false;
                }
                const std::uint64_t write_offset =
                    offset + completed;
                if (write_offset >
                    static_cast<std::uint64_t>(
                        std::numeric_limits<off_t>::max())) {
                    Fail(
                        RawFinalizationContinuationFailureV1::
                            kPlanMismatch,
                        EOVERFLOW,
                        error,
                        "continuation record offset exceeds off_t");
                    return false;
                }
                const ssize_t count = ::pwrite(
                    continuation_fd_,
                    expected.data() + completed,
                    expected.size() - completed,
                    static_cast<off_t>(
                        write_offset));
                if (count > 0) {
                    completed +=
                        static_cast<std::size_t>(count);
                    continue;
                }
                if (count < 0 && errno == EINTR) {
                    continue;
                }
                Fail(
                    RawFinalizationContinuationFailureV1::
                        kIoFailure,
                    count == 0 ? EIO : errno,
                    error,
                    "continuation record write failed");
                return false;
            }
        }
        if (!CheckedAdd(
                offset,
                static_cast<std::uint64_t>(
                    expected.size()),
                &offset)) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kPlanMismatch,
                EOVERFLOW,
                error,
                "continuation logical record end overflows");
            return false;
        }
    }
    if (offset != logical_end) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kAckMismatch,
            EILSEQ,
            error,
            "continuation record bytes do not reach ACKed framed total");
        return false;
    }
    if (!BeforeMutation(
            RawFinalizationContinuationMutationV1::
                kContinuationRecordSync,
            error)) {
        return false;
    }
    int sync_error =
        FdatasyncNoIntr(continuation_fd_);
    if (sync_error != 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            sync_error,
            error,
            "continuation record fdatasync failed");
        return false;
    }

    RawWalCursor terminal_cursor{
        continuation_header_.segment_base_wal_pos +
            logical_end,
        ack_facts_.callback
            .captured_ingress_sequence,
        logical_end};
    DurableMarkerV1 durable_marker{};
    durable_marker.source_stream_id =
        continuation_header_.source_stream_id;
    durable_marker.segment_sequence =
        continuation_header_.segment_sequence;
    durable_marker.durable_global_wal_pos =
        terminal_cursor.global_wal_pos;
    durable_marker.durable_ingress_sequence =
        terminal_cursor.ingress_sequence;
    durable_marker.durable_segment_offset =
        terminal_cursor.segment_offset;
    durable_marker.marker_flags = 0U;
    RawV1DurableMarkerWire durable_wire{};
    if (EncodeDurableMarkerV1(
            durable_marker,
            &durable_wire) !=
        RawV1Error::kNone) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kPlanMismatch,
            EILSEQ,
            error,
            "cannot encode continuation durable marker");
        return false;
    }

    const std::uint64_t durable_marker_offset =
        expected_header_marker_journal_end_;
    const std::uint64_t seal_marker_offset =
        durable_marker_offset +
        kRawV1DurableMarkerBytes;
    const std::uint64_t sealed_journal_end =
        seal_marker_offset +
        kRawV1DurableMarkerBytes;
    struct stat journal_status {};
    if (FstatNoIntr(
            journal_fd_,
            &journal_status) != 0 ||
        !IsSafeFile(journal_status) ||
        static_cast<std::uint64_t>(
            journal_status.st_size) <
            durable_marker_offset ||
        static_cast<std::uint64_t>(
            journal_status.st_size) >
            sealed_journal_end) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kNamespaceConflict,
            EILSEQ,
            error,
            "journal is outside the continuation drain marker FSM");
        return false;
    }

    const auto WriteMarker =
        [this, error](
            std::uint64_t marker_offset,
            const RawV1DurableMarkerWire& wire,
            RawFinalizationContinuationMutationV1
                mutation) noexcept {
            std::size_t completed = 0U;
            while (completed < wire.size()) {
                if (!BeforeMutation(
                        mutation, error)) {
                    return false;
                }
                const ssize_t count = ::pwrite(
                    journal_fd_,
                    wire.data() + completed,
                    wire.size() - completed,
                    static_cast<off_t>(
                        marker_offset + completed));
                if (count > 0) {
                    completed +=
                        static_cast<std::size_t>(count);
                    continue;
                }
                if (count < 0 && errno == EINTR) {
                    continue;
                }
                Fail(
                    RawFinalizationContinuationFailureV1::
                        kIoFailure,
                    count == 0 ? EIO : errno,
                    error,
                    "continuation journal marker write failed");
                return false;
            }
            return true;
        };

    if (static_cast<std::uint64_t>(
            journal_status.st_size) >=
        seal_marker_offset) {
        RawV1DurableMarkerWire existing{};
        if (!PreadAll(
                journal_fd_,
                durable_marker_offset,
                existing,
                nullptr) ||
            existing != durable_wire) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kNamespaceConflict,
                EILSEQ,
                error,
                "existing continuation durable marker conflicts");
            return false;
        }
    } else if (!WriteMarker(
                   durable_marker_offset,
                   durable_wire,
                   RawFinalizationContinuationMutationV1::
                       kContinuationDurableMarkerWrite)) {
        return false;
    }
    if (!BeforeMutation(
            RawFinalizationContinuationMutationV1::
                kContinuationDurableMarkerJournalSync,
            error)) {
        return false;
    }
    sync_error = FdatasyncNoIntr(journal_fd_);
    if (sync_error != 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            sync_error,
            error,
            "continuation durable marker sync failed");
        return false;
    }

    if (!BeforeMutation(
            RawFinalizationContinuationMutationV1::
                kContinuationLogicalTruncate,
            error)) {
        return false;
    }
    const int truncate_error =
        FtruncateNoIntr(
            continuation_fd_,
            logical_end);
    if (truncate_error != 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            truncate_error,
            error,
            "continuation logical truncate failed");
        return false;
    }
    if (!BeforeMutation(
            RawFinalizationContinuationMutationV1::
                kContinuationLogicalSync,
            error)) {
        return false;
    }
    sync_error =
        FdatasyncNoIntr(continuation_fd_);
    if (sync_error != 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            sync_error,
            error,
            "continuation logical-end sync failed");
        return false;
    }

    DurableMarkerV1 sealed_marker =
        durable_marker;
    sealed_marker.marker_flags =
        kRawV1SegmentSealed;
    if (EncodeDurableMarkerV1(
            sealed_marker,
            &sealed_marker_wire_) !=
        RawV1Error::kNone) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kPlanMismatch,
            EILSEQ,
            error,
            "cannot encode continuation seal marker");
        return false;
    }
    if (static_cast<std::uint64_t>(
            journal_status.st_size) >=
        sealed_journal_end) {
        RawV1DurableMarkerWire existing{};
        if (!PreadAll(
                journal_fd_,
                seal_marker_offset,
                existing,
                nullptr) ||
            existing != sealed_marker_wire_) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kNamespaceConflict,
                EILSEQ,
                error,
                "existing continuation seal marker conflicts");
            return false;
        }
    } else if (!WriteMarker(
                   seal_marker_offset,
                   sealed_marker_wire_,
                   RawFinalizationContinuationMutationV1::
                       kContinuationSealMarkerWrite)) {
        return false;
    }
    if (!BeforeMutation(
            RawFinalizationContinuationMutationV1::
                kContinuationSealJournalSync,
            error)) {
        return false;
    }
    sync_error = FdatasyncNoIntr(journal_fd_);
    if (sync_error != 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            sync_error,
            error,
            "continuation seal marker journal sync failed");
        return false;
    }

    sealed_snapshot_ = initialized_snapshot_;
    sealed_snapshot_.append = terminal_cursor;
    sealed_snapshot_.durable = terminal_cursor;
    sealed_snapshot_.journal_logical_size =
        sealed_journal_end;
    sealed_snapshot_.sealed = true;

    if (!BeforeMutation(
            RawFinalizationContinuationMutationV1::
                kFrozenRingConsume,
            error)) {
        return false;
    }
    if (ring_pop_record_ == nullptr) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kAllocationFailure,
            ENOMEM,
            error,
            "frozen ring consume buffer was not preallocated");
        return false;
    }
    ByteRingRecord& record = *ring_pop_record_;
    const auto SameMeta =
        [](const CaptureMetaV1& left,
           const CaptureMetaV1& right) noexcept {
            return left.source_stream_id ==
                       right.source_stream_id &&
                   left.connection_epoch_hint ==
                       right.connection_epoch_hint &&
                   left.ingress_sequence ==
                       right.ingress_sequence &&
                   left.recv_realtime_ns ==
                       right.recv_realtime_ns &&
                   left.recv_monotonic_ns ==
                       right.recv_monotonic_ns &&
                   left.capture_date ==
                       right.capture_date &&
                   left.flags == right.flags;
        };

    // Re-check the complete suffix without advancing the SPSC consumer.
    // The producer and regular consumer have already joined, so once this
    // pass succeeds every following try_pop is allocation-free and cannot
    // encounter an unvalidated entry.
    std::uint64_t inspect_cursor =
        ack_facts_.ring_consumed_position;
    for (std::size_t index = 0U;
         index < frozen_record_count_;
         ++index) {
        const FrozenRecord& expected =
            frozen_records_[index];
        const std::size_t body_size =
            expected.body.size();
        const std::uint64_t entry_size =
            static_cast<std::uint64_t>(
                sizeof(CaptureMetaV1) +
                kVendorMessageHeadBytes +
                kEntryCommitLengthBytes) +
            body_size;
        CaptureMetaV1 observed_meta{};
        std::array<std::byte, kVendorMessageHeadBytes>
            observed_head{};
        record.body.resize(body_size);
        ring_->copy_out(
            inspect_cursor,
            &observed_meta,
            sizeof(observed_meta));
        ring_->copy_out(
            inspect_cursor + sizeof(observed_meta),
            observed_head.data(),
            observed_head.size());
        if (body_size != 0U) {
            ring_->copy_out(
                inspect_cursor +
                    sizeof(observed_meta) +
                    observed_head.size(),
                record.body.data(),
                body_size);
        }
        std::uint64_t next_cursor = 0U;
        if (!CheckedAdd(
                inspect_cursor,
                entry_size,
                &next_cursor) ||
            next_cursor >
                ack_facts_.ring_published_position ||
            ring_->load_ring_u32_le(
                next_cursor -
                kEntryCommitLengthBytes) !=
                entry_size ||
            !SameMeta(
                observed_meta,
                expected.meta) ||
            observed_head != expected.head ||
            record.body != expected.body) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kAckMismatch,
                EILSEQ,
                error,
                "frozen ring changed before durable consume");
            return false;
        }
        inspect_cursor = next_cursor;
    }
    if (inspect_cursor !=
            ack_facts_.ring_published_position ||
        ring_->consumed_position() !=
            ack_facts_.ring_consumed_position) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kAckMismatch,
            EILSEQ,
            error,
            "frozen ring boundary changed before durable consume");
        return false;
    }

    for (std::size_t index = 0U;
         index < frozen_record_count_;
         ++index) {
        if (ring_->try_pop(record) !=
            ByteRingPopResult::RECORD) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kAckMismatch,
                EILSEQ,
                error,
                "frozen ring changed before durable consume");
            return false;
        }
        const FrozenRecord& expected =
            frozen_records_[index];
        if (!SameMeta(record.meta, expected.meta) ||
            record.head != expected.head ||
            record.body != expected.body) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kAckMismatch,
                EILSEQ,
                error,
                "consumed ring record differs from durable continuation bytes");
            return false;
        }
    }
    if (ring_->consumed_position() !=
            ack_facts_.ring_published_position ||
        ring_->used_bytes() != 0U) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kAckMismatch,
            EILSEQ,
            error,
            "durable continuation did not consume the complete frozen suffix");
        return false;
    }
    frozen_suffix_consumed_ = true;
    return true;
}

bool RawFinalizationContinuationPosixV1::
SealRecoveredPrefix(
    std::string* error) noexcept {
    std::uint64_t logical_end = 0U;
    std::uint64_t terminal_ingress = 0U;
    std::uint64_t terminal_global_wal = 0U;
    if (!CheckedAdd(
            kRawV1SegmentHeaderBytes,
            recovered_framed_wal_bytes_,
            &logical_end) ||
        !CheckedAdd(
            continuation_header_
                    .first_ingress_sequence -
                1U,
            recovered_record_count_,
            &terminal_ingress) ||
        !CheckedAdd(
            continuation_header_
                .segment_base_wal_pos,
            logical_end,
            &terminal_global_wal)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kCounterMismatch,
            EOVERFLOW,
            error,
            "replacement recovered cursor overflows");
        return false;
    }
    struct stat segment_status {};
    if (FstatNoIntr(
            continuation_fd_,
            &segment_status) != 0 ||
        !IsSafeFile(segment_status) ||
        static_cast<std::uint64_t>(
            segment_status.st_size) <
            logical_end ||
        static_cast<std::uint64_t>(
            segment_status.st_size) >
            continuation_allocation_bytes_) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kNamespaceConflict,
            EILSEQ,
            error,
            "replacement segment size changed before sealing");
        return false;
    }
    if (!BeforeMutation(
            RawFinalizationContinuationMutationV1::
                kContinuationRecordSync,
            error)) {
        return false;
    }
    int sync_error =
        FdatasyncNoIntr(continuation_fd_);
    if (sync_error != 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            sync_error,
            error,
            "replacement recovered prefix sync failed");
        return false;
    }

    const RawWalCursor terminal_cursor{
        terminal_global_wal,
        terminal_ingress,
        logical_end};
    DurableMarkerV1 durable_marker{};
    durable_marker.source_stream_id =
        continuation_header_.source_stream_id;
    durable_marker.segment_sequence =
        continuation_header_.segment_sequence;
    durable_marker.durable_global_wal_pos =
        terminal_cursor.global_wal_pos;
    durable_marker.durable_ingress_sequence =
        terminal_cursor.ingress_sequence;
    durable_marker.durable_segment_offset =
        terminal_cursor.segment_offset;
    RawV1DurableMarkerWire durable_wire{};
    if (EncodeDurableMarkerV1(
            durable_marker,
            &durable_wire) !=
        RawV1Error::kNone) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kPlanMismatch,
            EILSEQ,
            error,
            "replacement durable marker cannot be encoded");
        return false;
    }
    DurableMarkerV1 seal_marker =
        durable_marker;
    seal_marker.marker_flags =
        kRawV1SegmentSealed;
    if (EncodeDurableMarkerV1(
            seal_marker,
            &sealed_marker_wire_) !=
        RawV1Error::kNone) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kPlanMismatch,
            EILSEQ,
            error,
            "replacement seal marker cannot be encoded");
        return false;
    }

    const std::uint64_t durable_offset =
        expected_header_marker_journal_end_;
    const std::uint64_t seal_offset =
        durable_offset +
        kRawV1DurableMarkerBytes;
    const std::uint64_t sealed_end =
        seal_offset +
        kRawV1DurableMarkerBytes;
    const auto WriteMarker =
        [this, error](
            std::uint64_t offset,
            const RawV1DurableMarkerWire& wire,
            RawFinalizationContinuationMutationV1
                mutation) noexcept {
            std::size_t completed = 0U;
            while (completed < wire.size()) {
                if (!BeforeMutation(
                        mutation, error)) {
                    return false;
                }
                const ssize_t count = ::pwrite(
                    journal_fd_,
                    wire.data() + completed,
                    wire.size() - completed,
                    static_cast<off_t>(
                        offset + completed));
                if (count > 0) {
                    completed +=
                        static_cast<std::size_t>(
                            count);
                    continue;
                }
                if (count < 0 && errno == EINTR) {
                    continue;
                }
                Fail(
                    RawFinalizationContinuationFailureV1::
                        kIoFailure,
                    count == 0 ? EIO : errno,
                    error,
                    "replacement journal marker write failed");
                return false;
            }
            return true;
        };

    struct stat journal_status {};
    if (FstatNoIntr(
            journal_fd_,
            &journal_status) != 0 ||
        !IsSafeFile(journal_status) ||
        static_cast<std::uint64_t>(
            journal_status.st_size) <
            durable_offset ||
        static_cast<std::uint64_t>(
            journal_status.st_size) >
            sealed_end) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kNamespaceConflict,
            EILSEQ,
            error,
            "replacement journal changed before terminal markers");
        return false;
    }
    const std::uint64_t journal_size =
        static_cast<std::uint64_t>(
            journal_status.st_size);
    if (journal_size >=
        durable_offset +
            kRawV1DurableMarkerBytes) {
        RawV1DurableMarkerWire existing{};
        if (!PreadAll(
                journal_fd_,
                durable_offset,
                existing,
                nullptr) ||
            existing != durable_wire) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kNamespaceConflict,
                EILSEQ,
                error,
                "replacement durable marker changed");
            return false;
        }
    } else if (!WriteMarker(
                   durable_offset,
                   durable_wire,
                   RawFinalizationContinuationMutationV1::
                       kContinuationDurableMarkerWrite)) {
        return false;
    }
    if (!BeforeMutation(
            RawFinalizationContinuationMutationV1::
                kContinuationDurableMarkerJournalSync,
            error)) {
        return false;
    }
    sync_error = FdatasyncNoIntr(journal_fd_);
    if (sync_error != 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            sync_error,
            error,
            "replacement durable marker sync failed");
        return false;
    }

    if (!BeforeMutation(
            RawFinalizationContinuationMutationV1::
                kContinuationLogicalTruncate,
            error)) {
        return false;
    }
    const int truncate_error =
        FtruncateNoIntr(
            continuation_fd_, logical_end);
    if (truncate_error != 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            truncate_error,
            error,
            "replacement logical truncate failed");
        return false;
    }
    if (!BeforeMutation(
            RawFinalizationContinuationMutationV1::
                kContinuationLogicalSync,
            error)) {
        return false;
    }
    sync_error =
        FdatasyncNoIntr(continuation_fd_);
    if (sync_error != 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            sync_error,
            error,
            "replacement logical-end sync failed");
        return false;
    }

    if (journal_size >= sealed_end) {
        RawV1DurableMarkerWire existing{};
        if (!PreadAll(
                journal_fd_,
                seal_offset,
                existing,
                nullptr) ||
            existing != sealed_marker_wire_) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kNamespaceConflict,
                EILSEQ,
                error,
                "replacement seal marker changed");
            return false;
        }
    } else if (!WriteMarker(
                   seal_offset,
                   sealed_marker_wire_,
                   RawFinalizationContinuationMutationV1::
                       kContinuationSealMarkerWrite)) {
        return false;
    }
    if (!BeforeMutation(
            RawFinalizationContinuationMutationV1::
                kContinuationSealJournalSync,
            error)) {
        return false;
    }
    sync_error = FdatasyncNoIntr(journal_fd_);
    if (sync_error != 0) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            sync_error,
            error,
            "replacement seal marker sync failed");
        return false;
    }

    sealed_snapshot_ = initialized_snapshot_;
    sealed_snapshot_.append = terminal_cursor;
    sealed_snapshot_.durable = terminal_cursor;
    sealed_snapshot_.journal_logical_size =
        sealed_end;
    sealed_snapshot_.sealed = true;
    return true;
}

bool RawFinalizationContinuationPosixV1::
CreateOrAdopt(
    std::string* error) noexcept {
    SetError(error, {});
    if (barrier_complete_) {
        return true;
    }
    failure_ =
        RawFinalizationContinuationFailureV1::kNone;
    error_number_ = 0;
    if (action_ == nullptr ||
        (completion_mode_ ==
             RawFinalizationContinuationCompletionModeV1::
                 kLiveExactDrain &&
         frozen_suffix_consumed_) ||
        !ValidateLatest(error) ||
        (completion_mode_ ==
             RawFinalizationContinuationCompletionModeV1::
                 kLiveExactDrain &&
         !LiveSessionStillExact(error)) ||
        (completion_mode_ ==
             RawFinalizationContinuationCompletionModeV1::
                 kReplacementSealOnly &&
         replacement_writer_lease_ == nullptr)) {
        if (failure_ ==
            RawFinalizationContinuationFailureV1::
                kNone) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kInvalidInput,
                EPERM,
                error,
                "continuation session was already transferred");
        }
        return false;
    }

    RawV1DurableMarkerWire old_seal{};
    std::uint64_t old_sealed_journal_end = 0U;
    if (!VerifyOldSegmentSealed(
            &old_seal,
            &old_sealed_journal_end,
            error)) {
        return false;
    }
    if (completion_mode_ ==
        RawFinalizationContinuationCompletionModeV1::
            kReplacementSealOnly) {
        if (continuation_fd_ >= 0) {
            static_cast<void>(
                ::close(continuation_fd_));
            continuation_fd_ = -1;
        }
        if (!PublishOrAdoptContinuation(error) ||
            !PublishHeaderMarker(
                old_sealed_journal_end,
                error) ||
            !PublishOpenManifest(error) ||
            !SealRecoveredPrefix(error)) {
            return false;
        }
        barrier_complete_ = true;
        return true;
    }
    if (continuation_fd_ < 0 &&
        !PublishOrAdoptContinuation(error)) {
        return false;
    }
    if (!PublishHeaderMarker(
            old_sealed_journal_end,
            error) ||
        !PublishOpenManifest(error) ||
        !DrainFrozenSuffixAndSeal(error)) {
        return false;
    }
    barrier_complete_ = true;
    return true;
}

std::unique_ptr<
    RawFinalizationContinuationReceiptV1>
RawFinalizationContinuationPosixV1::TakeReceipt(
    std::string* error) noexcept {
    SetError(error, {});
    if (!barrier_complete_ ||
        (completion_mode_ ==
             RawFinalizationContinuationCompletionModeV1::
                 kLiveExactDrain &&
         !frozen_suffix_consumed_) ||
        action_ == nullptr ||
        continuation_fd_ < 0 ||
        journal_fd_ < 0 ||
        !ValidateLatest(error) ||
        (completion_mode_ ==
             RawFinalizationContinuationCompletionModeV1::
                 kLiveExactDrain &&
         !LiveSessionStillExact(error)) ||
        (completion_mode_ ==
             RawFinalizationContinuationCompletionModeV1::
                 kReplacementSealOnly &&
         replacement_writer_lease_ == nullptr)) {
        if (failure_ ==
            RawFinalizationContinuationFailureV1::
                kNone) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kInvalidInput,
                EPERM,
                error,
                "continuation receipt requires every durable and ring-consume barrier");
        }
        return nullptr;
    }

    std::unique_ptr<
        RawFinalizationContinuationReceiptV1> receipt(
        new (std::nothrow)
            RawFinalizationContinuationReceiptV1());
    if (receipt == nullptr) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kAllocationFailure,
            ENOMEM,
            error,
            "cannot allocate typed continuation receipt");
        return nullptr;
    }

    const int action_route_fd =
        action_->route_directory_descriptor();
    receipt->raw_root_fd_ = OpenAtNoIntr(
        action_->raw_root_descriptor(),
        ".",
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
            O_NONBLOCK | O_CLOEXEC | O_NOATIME);
    receipt->route_fd_ = OpenAtNoIntr(
        action_route_fd,
        ".",
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
            O_NONBLOCK | O_CLOEXEC | O_NOATIME);
    receipt->manifest_fd_ = OpenAtNoIntr(
        action_route_fd,
        kRawManifestCurrentFilename,
        O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
            O_CLOEXEC | O_NOATIME);
    receipt->old_index_fd_ = OpenAtNoIntr(
        action_route_fd,
        old_artifact_plan_.names.index_name.c_str(),
        O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
            O_CLOEXEC | O_NOATIME);
    struct stat raw_root {};
    struct stat route {};
    struct stat old_segment {};
    struct stat old_index {};
    struct stat journal {};
    struct stat segment {};
    struct stat manifest {};
    struct stat named {};
    const std::string segment_name =
        SegmentName(
            continuation_header_.segment_sequence);
    if (receipt->raw_root_fd_ < 0 ||
        receipt->route_fd_ < 0 ||
        receipt->manifest_fd_ < 0 ||
        receipt->old_index_fd_ < 0 ||
        FstatNoIntr(
            receipt->raw_root_fd_,
            &raw_root) != 0 ||
        FstatNoIntr(
            receipt->route_fd_, &route) != 0 ||
        FstatNoIntr(
            old_segment_fd_, &old_segment) != 0 ||
        FstatNoIntr(
            receipt->old_index_fd_,
            &old_index) != 0 ||
        FstatNoIntr(
            journal_fd_, &journal) != 0 ||
        FstatNoIntr(
            continuation_fd_, &segment) != 0 ||
        FstatNoIntr(
            receipt->manifest_fd_,
            &manifest) != 0 ||
        !IsSafeDirectory(raw_root) ||
        !IsSafeDirectory(route) ||
        !IsSafeFile(old_segment) ||
        !IsSafeFile(old_index) ||
        !IsSafeFile(journal) ||
        !IsSafeFile(segment) ||
        !IsSafeFile(manifest) ||
        FstatAtNoIntr(
            receipt->route_fd_,
            old_artifact_plan_
                .names.segment_name.c_str(),
            &named) != 0 ||
        !SameInode(old_segment, named) ||
        FstatAtNoIntr(
            receipt->route_fd_,
            old_artifact_plan_
                .names.index_name.c_str(),
            &named) != 0 ||
        !SameInode(old_index, named) ||
        FstatAtNoIntr(
            receipt->route_fd_,
            kRawJournalFilename,
            &named) != 0 ||
        !SameInode(journal, named) ||
        FstatAtNoIntr(
            receipt->route_fd_,
            segment_name.c_str(),
            &named) != 0 ||
        !SameInode(segment, named) ||
        FstatAtNoIntr(
            receipt->route_fd_,
            kRawManifestCurrentFilename,
            &named) != 0 ||
        !SameInode(manifest, named)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kUnsafeNamespace,
            errno == 0 ? ESTALE : errno,
            error,
            "cannot retain exact final continuation evidence inodes");
        return nullptr;
    }

    if (!FillRetainedInode(
            raw_root,
            &receipt->raw_root_inode_) ||
        !FillRetainedInode(
            route, &receipt->route_inode_) ||
        !FillRetainedInode(
            journal,
            &receipt->journal_inode_) ||
        !FillRetainedInode(
            old_segment,
            &receipt->old_segment_inode_) ||
        !FillRetainedInode(
            old_index,
            &receipt->old_index_inode_) ||
        !FillRetainedInode(
            segment,
            &receipt->segment_inode_) ||
        !FillRetainedInode(
            manifest,
            &receipt->manifest_inode_)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kAllocationFailure,
            EOVERFLOW,
            error,
            "continuation retained inode observation overflows");
        return nullptr;
    }

    std::string manifest_bytes;
    int read_error = 0;
    RawManifestV1 manifest_model{};
    if (!ReadFile(
            receipt->manifest_fd_,
            receipt->manifest_inode_.size,
            &manifest_bytes,
            &read_error) ||
        ParseRawManifestJcs(
            manifest_bytes,
            &manifest_model) !=
            RawManifestStoreError::kNone) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kManifestLoad,
            read_error == 0 ? EILSEQ : read_error,
            error,
            "cannot retain strict continuation manifest bytes");
        return nullptr;
    }
    std::string expected_manifest_bytes;
    RawV1Digest open_commitment{};
    RawV1Digest sealed_segment_sha256{};
    RawV1Digest sealed_journal_sha256{};
    if (EncodeRawManifestJcs(
            open_manifest_,
            &expected_manifest_bytes) !=
            RawManifestV1Error::kNone ||
        manifest_bytes !=
            expected_manifest_bytes ||
        ComputeRawManifestOpenEntryCommitmentV1(
            open_manifest_,
            &open_commitment) !=
            RawManifestV1Error::kNone ||
        !l2flow::common::ComputeFileSha256ForOpenFd(
            continuation_fd_,
            &sealed_segment_sha256,
            error,
            static_cast<std::uint64_t>(
                segment.st_size)) ||
        !l2flow::common::ComputeFileSha256ForOpenFd(
            journal_fd_,
            &sealed_journal_sha256,
            error,
            static_cast<std::uint64_t>(
                journal.st_size))) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            EILSEQ,
            error,
            "retained continuation evidence does not match its durable barrier");
        return nullptr;
    }

    RawFinalizationContinuationAllocationObservationV1
        allocation{};
    allocation.authorized_byte_cap =
        action_->byte_cap();
    allocation.authorized_inode_cap =
        action_->inode_cap();
    allocation.continuation_allocation_cap =
        continuation_allocation_bytes_;
    allocation.continuation_logical_size =
        static_cast<std::uint64_t>(segment.st_size);
    allocation.continuation_allocated_bytes =
        receipt->segment_inode_
            .allocated_bytes;
    allocation.old_segment_allocated_bytes_before =
        old_allocated_bytes_before_;
    if (!CheckedMultiply(
            static_cast<std::uint64_t>(
                old_segment.st_blocks),
            512U,
            &allocation
                 .old_segment_allocated_bytes_after) ||
        !CheckedMultiply(
            static_cast<std::uint64_t>(
                journal.st_blocks),
            512U,
            &allocation
                 .journal_allocated_bytes_after)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kAllocationFailure,
            EOVERFLOW,
            error,
            "final continuation allocation observation overflows");
        return nullptr;
    }
    allocation.journal_allocated_bytes_before =
        journal_allocated_bytes_before_;
    allocation.manifest_allocated_bytes_before =
        manifest_allocated_bytes_before_;
    allocation.manifest_allocated_bytes_after =
        receipt->manifest_inode_
            .allocated_bytes;
    const auto PositiveDelta =
        [](std::uint64_t before,
           std::uint64_t after) noexcept {
            return after > before
                       ? after - before
                       : 0U;
        };
    std::uint64_t total_delta =
        allocation.continuation_allocated_bytes;
    if (!CheckedAdd(
            total_delta,
            PositiveDelta(
                allocation
                    .old_segment_allocated_bytes_before,
                allocation
                    .old_segment_allocated_bytes_after),
            &total_delta) ||
        !CheckedAdd(
            total_delta,
            PositiveDelta(
                allocation
                    .journal_allocated_bytes_before,
                allocation
                    .journal_allocated_bytes_after),
            &total_delta) ||
        !CheckedAdd(
            total_delta,
            PositiveDelta(
                allocation
                    .manifest_allocated_bytes_before,
                allocation
                    .manifest_allocated_bytes_after),
            &total_delta) ||
        total_delta > action_->byte_cap() ||
        allocation.continuation_allocated_bytes >
            continuation_allocation_bytes_) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kPlanMismatch,
            EDQUOT,
            error,
            "observed continuation allocation exceeds the DEBITED cap");
        return nullptr;
    }
    allocation.conservative_positive_allocation_delta =
        total_delta;

    receipt->key_ = action_->key();
    receipt->generation_token_ =
        action_->generation_token();
    receipt->immutable_grant_sha256_ =
        action_->immutable_grant_sha256();
    receipt->plan_ = action_->plan();
    receipt->grant_flags_ =
        action_->grant_flags();
    receipt->byte_cap_ = action_->byte_cap();
    receipt->inode_cap_ = action_->inode_cap();
    receipt->debit_generation_ =
        action_->debit_generation();
    receipt->executor_instance_ =
        action_->executor_instance();
    receipt->continuation_header_ =
        continuation_header_;
    receipt->continuation_header_wire_ =
        continuation_header_wire_;
    receipt->header_marker_wire_ =
        header_marker_wire_;
    receipt->header_marker_sha256_ =
        ComputeAcceptedMarkerSha256(
            header_marker_wire_);
    receipt->sealed_marker_wire_ =
        sealed_marker_wire_;
    receipt->sealed_marker_sha256_ =
        ComputeAcceptedMarkerSha256(
            sealed_marker_wire_);
    receipt->sealed_segment_sha256_ =
        sealed_segment_sha256;
    receipt->sealed_journal_sha256_ =
        sealed_journal_sha256;
    receipt->open_manifest_commitment_sha256_ =
        open_commitment;
    receipt->open_manifest_bytes_sha256_ =
        l2flow::common::ComputeSha256(
            std::as_bytes(
                std::span<const char>(
                    manifest_bytes.data(),
                    manifest_bytes.size())));
    receipt->open_manifest_ = open_manifest_;
    receipt->old_sealed_metadata_ =
        old_artifact_plan_.metadata;
    receipt->old_segment_name_ =
        old_artifact_plan_.names.segment_name;
    receipt->old_index_name_ =
        old_artifact_plan_.names.index_name;
    receipt->initialized_snapshot_ =
        initialized_snapshot_;
    receipt->sealed_snapshot_ =
        sealed_snapshot_;
    receipt->frozen_record_count_ =
        ack_facts_.queued_record_count;
    receipt->frozen_framed_wal_bytes_ =
        ack_facts_.queued_framed_wal_bytes;
    receipt->completion_mode_ =
        completion_mode_;
    receipt->recovered_record_count_ =
        completion_mode_ ==
                RawFinalizationContinuationCompletionModeV1::
                    kLiveExactDrain
            ? ack_facts_.queued_record_count
            : recovered_record_count_;
    receipt->recovered_framed_wal_bytes_ =
        completion_mode_ ==
                RawFinalizationContinuationCompletionModeV1::
                    kLiveExactDrain
            ? ack_facts_.queued_framed_wal_bytes
            : recovered_framed_wal_bytes_;
    receipt->final_cursor_ =
        sealed_snapshot_.durable;
    receipt->allocation_observation_ =
        allocation;

    // Transfer the complete sink only after every continuation/ring barrier
    // is durable and all retained artifact evidence has been captured. The
    // production sink owns the writer lease, so this is the lease handoff:
    // the app may now be destroyed without opening a replacement-writer
    // window before coordinator COMPLETE is durably published.
    if (completion_mode_ ==
        RawFinalizationContinuationCompletionModeV1::
            kLiveExactDrain) {
        if (owner_ == nullptr) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kNoLiveAck,
                ESTALE,
                error,
                "continuation writer owner disappeared before receipt handoff");
            return nullptr;
        }
        std::lock_guard<std::mutex> lock(
            owner_->lifecycle_mutex_);
        if (owner_->state_.load(
                std::memory_order_acquire) !=
                RawIngressAppState::kStopping ||
            !owner_->emergency_ack_issued_ ||
            !owner_->emergency_ack_consumed_ ||
            owner_->emergency_ack_epoch_ !=
                ack_epoch_ ||
            owner_->sink_.get() != writer_ ||
            writer_->identity() !=
                ack_facts_.writer ||
            !SameWalSnapshot(
                writer_->Snapshot(),
                ack_facts_.wal) ||
            ring_->published_position() !=
                ack_facts_.ring_published_position ||
            ring_->consumed_position() !=
                ack_facts_.ring_published_position ||
            !owner_->capture_worker_->Snapshot()
                 .emergency_abandoned) {
            Fail(
                RawFinalizationContinuationFailureV1::
                    kNoLiveAck,
                ESTALE,
                error,
                "continuation writer lease changed before receipt handoff");
            return nullptr;
        }
        receipt->retained_writer_identity_ =
            ack_facts_.writer;
        receipt->retained_writer_snapshot_ =
            ack_facts_.wal;
        receipt->retained_writer_ =
            std::move(owner_->sink_);
        writer_ = receipt->retained_writer_.get();
        owner_ = nullptr;
    } else {
        receipt->replacement_writer_lease_ =
            std::move(replacement_writer_lease_);
    }

    receipt->journal_fd_ = journal_fd_;
    journal_fd_ = -1;
    receipt->old_segment_fd_ =
        old_segment_fd_;
    old_segment_fd_ = -1;
    receipt->segment_fd_ = continuation_fd_;
    continuation_fd_ = -1;
    receipt->action_ = std::move(action_);

    if (!receipt->Validate(error)) {
        Fail(
            RawFinalizationContinuationFailureV1::
                kIoFailure,
            ESTALE,
            error,
            "constructed continuation receipt failed retained-fd validation");
        return nullptr;
    }
    return receipt;
}

}  // namespace l2flow::ingress
