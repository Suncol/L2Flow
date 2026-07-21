#include "l2flow/ingress/raw_segment_artifacts.h"

#include "l2flow/common/sha256.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <linux/fs.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace l2flow::ingress {
namespace {

[[nodiscard]] int FstatNoIntr(
    int descriptor,
    struct stat* status) noexcept {
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
    int directory_descriptor,
    const char* name,
    struct stat* status) noexcept {
    for (;;) {
        if (::fstatat(
                directory_descriptor,
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

[[nodiscard]] int FcntlGetNoIntr(
    int descriptor,
    int command,
    int* value) noexcept {
    for (;;) {
        const int result = ::fcntl(descriptor, command);
        if (result >= 0) {
            *value = result;
            return 0;
        }
        if (errno != EINTR) {
            return errno;
        }
    }
}

class PosixRawSegmentArtifactIoV1 final
    : public RawSegmentArtifactIoV1 {
public:
    [[nodiscard]] std::uint64_t
    EffectiveUserId() const noexcept override {
        return static_cast<std::uint64_t>(::geteuid());
    }

    [[nodiscard]] int InspectDescriptor(
        int descriptor,
        RawSegmentArtifactFileInfoV1* info) noexcept override {
        if (descriptor < 0 || info == nullptr) {
            return EINVAL;
        }
        struct stat status {};
        int error_number =
            FstatNoIntr(descriptor, &status);
        if (error_number != 0) {
            return error_number;
        }
        int flags = 0;
        error_number =
            FcntlGetNoIntr(descriptor, F_GETFL, &flags);
        if (error_number != 0) {
            return error_number;
        }
        int descriptor_flags = 0;
        error_number = FcntlGetNoIntr(
            descriptor, F_GETFD, &descriptor_flags);
        if (error_number != 0) {
            return error_number;
        }
        FillInfo(
            status, flags, descriptor_flags, info);
        return 0;
    }

    [[nodiscard]] int InspectName(
        int directory_descriptor,
        std::string_view name,
        RawSegmentArtifactFileInfoV1* info) noexcept override {
        if (directory_descriptor < 0 || info == nullptr) {
            return EINVAL;
        }
        std::string terminated;
        const int conversion = ToCString(name, &terminated);
        if (conversion != 0) {
            return conversion;
        }
        struct stat status {};
        const int error_number = FstatAtNoIntr(
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
        const int conversion = ToCString(name, &terminated);
        if (directory_descriptor < 0 || conversion != 0) {
            return {-1, conversion == 0 ? EINVAL : conversion};
        }
        const int flags =
            (writable ? O_RDWR : O_RDONLY) |
            O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC | O_NOATIME;
        for (;;) {
            const int descriptor = ::openat(
                directory_descriptor,
                terminated.c_str(),
                flags);
            if (descriptor >= 0) {
                return {descriptor, 0};
            }
            if (errno != EINTR) {
                return {-1, errno};
            }
        }
    }

    [[nodiscard]] RawSegmentArtifactOpenResultV1
    CreateExclusive(
        int directory_descriptor,
        std::string_view name) noexcept override {
        std::string terminated;
        const int conversion = ToCString(name, &terminated);
        if (directory_descriptor < 0 || conversion != 0) {
            return {-1, conversion == 0 ? EINVAL : conversion};
        }
        constexpr int flags =
            O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW |
            O_NONBLOCK | O_CLOEXEC | O_NOATIME;
        for (;;) {
            const int descriptor = ::openat(
                directory_descriptor,
                terminated.c_str(),
                flags,
                0600);
            if (descriptor >= 0) {
                return {descriptor, 0};
            }
            if (errno != EINTR) {
                return {-1, errno};
            }
        }
    }

    [[nodiscard]] RawSegmentArtifactIoStepV1 ReadSome(
        int descriptor,
        std::uint64_t offset,
        std::span<std::byte> output) noexcept override {
        if (descriptor < 0 ||
            offset >
                static_cast<std::uint64_t>(
                    std::numeric_limits<off_t>::max())) {
            return {0U, EOVERFLOW};
        }
        if (output.empty()) {
            return {0U, 0};
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
        if (count < 0) {
            return {0U, errno};
        }
        return {static_cast<std::size_t>(count), 0};
    }

    [[nodiscard]] RawSegmentArtifactIoStepV1 WriteSome(
        int descriptor,
        std::uint64_t offset,
        std::span<const std::byte> input) noexcept override {
        if (descriptor < 0 ||
            offset >
                static_cast<std::uint64_t>(
                    std::numeric_limits<off_t>::max())) {
            return {0U, EOVERFLOW};
        }
        if (input.empty()) {
            return {0U, 0};
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
        if (count < 0) {
            return {0U, errno};
        }
        return {static_cast<std::size_t>(count), 0};
    }

    [[nodiscard]] int SyncFile(
        int descriptor) noexcept override {
        for (;;) {
            if (::fsync(descriptor) == 0) {
                return 0;
            }
            if (errno != EINTR) {
                return errno;
            }
        }
    }

    [[nodiscard]] int SyncDirectory(
        int directory_descriptor) noexcept override {
        return SyncFile(directory_descriptor);
    }

    [[nodiscard]] int RenameNoReplace(
        int directory_descriptor,
        std::string_view old_name,
        std::string_view new_name) noexcept override {
        std::string old_terminated;
        std::string new_terminated;
        int conversion = ToCString(old_name, &old_terminated);
        if (conversion == 0) {
            conversion = ToCString(new_name, &new_terminated);
        }
        if (directory_descriptor < 0 || conversion != 0) {
            return conversion == 0 ? EINVAL : conversion;
        }
        for (;;) {
            const int result = static_cast<int>(
                ::syscall(
                    SYS_renameat2,
                    directory_descriptor,
                    old_terminated.c_str(),
                    directory_descriptor,
                    new_terminated.c_str(),
                    RENAME_NOREPLACE));
            if (result == 0) {
                return 0;
            }
            if (errno != EINTR) {
                return errno;
            }
        }
    }

    [[nodiscard]] int NameMatchesDescriptor(
        int directory_descriptor,
        std::string_view name,
        int descriptor) noexcept override {
        std::string terminated;
        const int conversion = ToCString(name, &terminated);
        if (directory_descriptor < 0 ||
            descriptor < 0 ||
            conversion != 0) {
            return conversion == 0 ? EINVAL : conversion;
        }
        struct stat named {};
        struct stat opened {};
        int error_number = FstatAtNoIntr(
            directory_descriptor,
            terminated.c_str(),
            &named);
        if (error_number == 0) {
            error_number =
                FstatNoIntr(descriptor, &opened);
        }
        if (error_number != 0) {
            return error_number;
        }
        return named.st_dev == opened.st_dev &&
                       named.st_ino == opened.st_ino
                   ? 0
                   : ESTALE;
    }

    void Close(int descriptor) noexcept override {
        if (descriptor >= 0) {
            static_cast<void>(::close(descriptor));
        }
    }

private:
    static void FillInfo(
        const struct stat& status,
        int open_flags,
        int descriptor_flags,
        RawSegmentArtifactFileInfoV1* info) noexcept {
        RawSegmentArtifactFileInfoV1 result{};
        result.regular_file = S_ISREG(status.st_mode);
        result.directory = S_ISDIR(status.st_mode);
        result.owner_user_id =
            static_cast<std::uint64_t>(status.st_uid);
        result.permission_bits =
            static_cast<std::uint32_t>(status.st_mode & 07777U);
        result.link_count =
            static_cast<std::uint64_t>(status.st_nlink);
        result.size =
            status.st_size < 0
                ? std::numeric_limits<std::uint64_t>::max()
                : static_cast<std::uint64_t>(status.st_size);
        result.device =
            static_cast<std::uint64_t>(status.st_dev);
        result.inode =
            static_cast<std::uint64_t>(status.st_ino);
        result.modification_seconds =
            static_cast<std::int64_t>(status.st_mtim.tv_sec);
        result.modification_nanoseconds =
            static_cast<std::int64_t>(status.st_mtim.tv_nsec);
        result.change_seconds =
            static_cast<std::int64_t>(status.st_ctim.tv_sec);
        result.change_nanoseconds =
            static_cast<std::int64_t>(status.st_ctim.tv_nsec);
        result.open_flags = open_flags;
        result.descriptor_flags = descriptor_flags;
        *info = result;
    }

    static int ToCString(
        std::string_view name,
        std::string* output) noexcept {
        if (output == nullptr ||
            name.empty() ||
            name.find('\0') != std::string_view::npos ||
            name.find('/') != std::string_view::npos) {
            return EINVAL;
        }
        try {
            output->assign(name);
        } catch (...) {
            return ENOMEM;
        }
        return 0;
    }
};

void FailPlan(
    RawSegmentArtifactPlanV1* plan,
    RawSegmentArtifactFailureV1 failure,
    int error_number = EINVAL) noexcept {
    if (plan->failure !=
        RawSegmentArtifactFailureV1::kNone) {
        return;
    }
    plan->failure = failure;
    plan->error_number =
        error_number == 0 ? EIO : error_number;
}

void FailPublish(
    RawSegmentArtifactPublishResultV1* result,
    RawSegmentArtifactFailureV1 failure,
    int error_number = EINVAL) noexcept {
    if (result->failure !=
        RawSegmentArtifactFailureV1::kNone) {
        return;
    }
    result->failure = failure;
    result->error_number =
        error_number == 0 ? EIO : error_number;
}

[[nodiscard]] bool IsZero(
    std::span<const std::byte> bytes) noexcept {
    return std::all_of(
        bytes.begin(),
        bytes.end(),
        [](std::byte value) {
            return value == std::byte{0};
        });
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

[[nodiscard]] int FormatNames(
    std::uint32_t segment_sequence,
    RawSegmentArtifactNamesV1* names) noexcept {
    if (names == nullptr ||
        segment_sequence == 0U ||
        segment_sequence >
            kRawSegmentArtifactMaximumSequenceV1) {
        return EOVERFLOW;
    }
    std::array<char, 8U> digits{};
    std::uint32_t remaining = segment_sequence;
    for (std::size_t index = 0U;
         index < digits.size();
         ++index) {
        digits[digits.size() - 1U - index] =
            static_cast<char>(
                '0' + static_cast<int>(remaining % 10U));
        remaining /= 10U;
    }
    if (remaining != 0U) {
        return EOVERFLOW;
    }
    try {
        const std::string number(digits.begin(), digits.end());
        RawSegmentArtifactNamesV1 formatted;
        formatted.segment_name =
            "segment-" + number + ".raw";
        formatted.index_name =
            "segment-" + number + ".idx";
        formatted.index_temporary_name =
            "." + formatted.index_name + ".raw-index.tmp";
        *names = std::move(formatted);
    } catch (...) {
        return ENOMEM;
    }
    return 0;
}

[[nodiscard]] bool OptionsAreValid(
    const RawSegmentArtifactOptionsV1& options) noexcept {
    return !IsZero(options.expected_raw_schema_sha256) &&
           options.sample_record_interval != 0U &&
           options.sample_record_interval <=
               kRawIndexV1DefaultRecordInterval &&
           options.sample_raw_bytes_interval != 0U &&
           options.sample_raw_bytes_interval <=
               kRawIndexV1DefaultRawBytesInterval &&
           options.maximum_segment_bytes >=
               kRawV1SegmentHeaderBytes;
}

[[nodiscard]] bool IsSafeDirectory(
    const RawSegmentArtifactFileInfoV1& info,
    std::uint64_t effective_user_id) noexcept {
    return info.directory &&
           info.owner_user_id == effective_user_id &&
           (info.permission_bits & 0022U) == 0U;
}

[[nodiscard]] bool IsReadableAccessMode(
    int flags) noexcept {
    const int access_mode = flags & O_ACCMODE;
    return access_mode == O_RDONLY ||
           access_mode == O_RDWR;
}

[[nodiscard]] bool IsSafeSegment(
    const RawSegmentArtifactFileInfoV1& info,
    std::uint64_t effective_user_id) noexcept {
    return info.regular_file &&
           info.owner_user_id == effective_user_id &&
           info.permission_bits == 0600U &&
           info.link_count == 1U &&
           IsReadableAccessMode(info.open_flags) &&
           (info.open_flags & O_APPEND) == 0 &&
           (info.open_flags & O_NONBLOCK) != 0 &&
           (info.descriptor_flags & FD_CLOEXEC) != 0;
}

[[nodiscard]] bool IsSafeArtifact(
    const RawSegmentArtifactFileInfoV1& info,
    std::uint64_t effective_user_id,
    std::uint64_t expected_device,
    bool writable) noexcept {
    const int expected_access =
        writable ? O_RDWR : O_RDONLY;
    return info.regular_file &&
           info.owner_user_id == effective_user_id &&
           info.permission_bits == 0600U &&
           info.link_count == 1U &&
           info.device == expected_device &&
           (info.open_flags & O_ACCMODE) ==
               expected_access &&
           (info.open_flags & O_APPEND) == 0 &&
           (info.open_flags & O_NONBLOCK) != 0 &&
           (info.open_flags & O_NOATIME) != 0 &&
           (info.descriptor_flags & FD_CLOEXEC) != 0;
}

[[nodiscard]] bool ReadAll(
    RawSegmentArtifactIoV1& io,
    int descriptor,
    std::span<std::byte> output,
    int* error_number) noexcept {
    std::size_t completed = 0U;
    while (completed < output.size()) {
        const RawSegmentArtifactIoStepV1 step =
            io.ReadSome(
                descriptor,
                static_cast<std::uint64_t>(completed),
                output.subspan(completed));
        if (step.error_number == EINTR &&
            step.byte_count == 0U) {
            continue;
        }
        if (step.error_number != 0 ||
            step.byte_count == 0U ||
            step.byte_count >
                output.size() - completed) {
            if (error_number != nullptr) {
                *error_number =
                    step.error_number == 0
                        ? EIO
                        : step.error_number;
            }
            return false;
        }
        completed += step.byte_count;
    }
    return true;
}

[[nodiscard]] bool WriteAll(
    RawSegmentArtifactIoV1& io,
    int descriptor,
    std::span<const std::byte> input,
    int* error_number) noexcept {
    std::size_t completed = 0U;
    while (completed < input.size()) {
        const RawSegmentArtifactIoStepV1 step =
            io.WriteSome(
                descriptor,
                static_cast<std::uint64_t>(completed),
                input.subspan(completed));
        if (step.error_number == EINTR &&
            step.byte_count == 0U) {
            continue;
        }
        if (step.error_number != 0 ||
            step.byte_count == 0U ||
            step.byte_count >
                input.size() - completed) {
            if (error_number != nullptr) {
                *error_number =
                    step.error_number == 0
                        ? EIO
                        : step.error_number;
            }
            return false;
        }
        completed += step.byte_count;
    }
    return true;
}

[[nodiscard]] RawIndexExpectedIdentityV1
ExpectedIndexIdentity(
    const RawSegmentArtifactPlanV1& plan) noexcept {
    RawIndexExpectedIdentityV1 expected{};
    expected.capture_date =
        plan.metadata.segment.capture_date;
    expected.source_stream_id =
        plan.metadata.segment.source_stream_id;
    expected.stream_day_id =
        plan.metadata.segment.stream_day_id;
    expected.segment_sequence =
        plan.metadata.segment.segment_sequence;
    expected.segment_base_wal_pos =
        plan.metadata.segment.segment_base_wal_pos;
    expected.raw_schema_sha256 =
        plan.options.expected_raw_schema_sha256;
    expected.sample_record_interval =
        plan.options.sample_record_interval;
    expected.sample_raw_bytes_interval =
        plan.options.sample_raw_bytes_interval;
    return expected;
}

[[nodiscard]] bool SameMarker(
    const DurableMarkerV1& left,
    const DurableMarkerV1& right) noexcept {
    return left.source_stream_id ==
               right.source_stream_id &&
           left.segment_sequence ==
               right.segment_sequence &&
           left.durable_global_wal_pos ==
               right.durable_global_wal_pos &&
           left.durable_ingress_sequence ==
               right.durable_ingress_sequence &&
           left.durable_segment_offset ==
               right.durable_segment_offset &&
           left.marker_crc32c == right.marker_crc32c &&
           left.marker_flags == right.marker_flags;
}

[[nodiscard]] bool ValidatePlanCore(
    const RawSegmentArtifactPlanV1& plan) noexcept {
    if (!plan.ok() ||
        !OptionsAreValid(plan.options) ||
        plan.index_bytes.empty() ||
        plan.metadata.logical_end_offset <
            kRawV1SegmentHeaderBytes ||
        plan.metadata.logical_end_offset >
            plan.options.maximum_segment_bytes ||
        plan.metadata.segment.raw_schema_sha256 !=
            plan.options.expected_raw_schema_sha256 ||
        plan.metadata.index_sha256 !=
            l2flow::common::ComputeSha256(plan.index_bytes)) {
        return false;
    }
    RawV1SegmentHeaderWire encoded_header{};
    if (EncodeSegmentHeaderV1(
            plan.metadata.segment,
            &encoded_header) != RawV1Error::kNone) {
        return false;
    }
    DurableMarkerV1 decoded_marker{};
    std::uint64_t expected_global_end = 0U;
    if (DecodeDurableMarkerV1(
            plan.metadata.accepted_sealed_marker_bytes,
            &decoded_marker) != RawV1Error::kNone ||
        decoded_marker.marker_flags !=
            kRawV1SegmentSealed ||
        !SameMarker(
            decoded_marker,
            plan.metadata.accepted_sealed_marker) ||
        !CheckedAdd(
            plan.metadata.segment.segment_base_wal_pos,
            plan.metadata.logical_end_offset,
            &expected_global_end) ||
        decoded_marker.source_stream_id !=
            plan.metadata.segment.source_stream_id ||
        decoded_marker.segment_sequence !=
            plan.metadata.segment.segment_sequence ||
        decoded_marker.durable_segment_offset !=
            plan.metadata.logical_end_offset ||
        decoded_marker.durable_global_wal_pos !=
            expected_global_end) {
        return false;
    }
    if (plan.metadata.record_count == 0U) {
        if (plan.metadata.actual_first_ingress_sequence
                .has_value() ||
            plan.metadata.actual_last_ingress_sequence
                .has_value() ||
            decoded_marker.durable_ingress_sequence !=
                plan.metadata.segment
                        .first_ingress_sequence -
                    1U) {
            return false;
        }
    } else {
        if (!plan.metadata.actual_first_ingress_sequence
                 .has_value() ||
            !plan.metadata.actual_last_ingress_sequence
                 .has_value()) {
            return false;
        }
        const std::uint64_t first =
            *plan.metadata.actual_first_ingress_sequence;
        const std::uint64_t last =
            *plan.metadata.actual_last_ingress_sequence;
        if (first !=
                plan.metadata.segment
                    .first_ingress_sequence ||
            last !=
                decoded_marker
                    .durable_ingress_sequence ||
            last < first ||
            last - first ==
                std::numeric_limits<std::uint64_t>::max() ||
            last - first + 1U !=
                plan.metadata.record_count) {
            return false;
        }
    }
    RawSegmentArtifactNamesV1 expected_names;
    if (FormatNames(
            plan.metadata.segment.segment_sequence,
            &expected_names) != 0 ||
        plan.names.segment_name !=
            expected_names.segment_name ||
        plan.names.index_name != expected_names.index_name ||
        plan.names.index_temporary_name !=
            expected_names.index_temporary_name) {
        return false;
    }
    RawIndexFileV1 decoded{};
    if (ValidateRawIndexFileV1(
            plan.index_bytes,
            ExpectedIndexIdentity(plan),
            &decoded) != RawIndexV1Error::kNone ||
        decoded.footer.segment_sha256 !=
            plan.metadata.segment_sha256 ||
        decoded.footer.segment_logical_end_offset !=
            plan.metadata.logical_end_offset ||
        decoded.footer.segment_record_count !=
            plan.metadata.record_count ||
        decoded.footer.accepted_segment_sealed_marker !=
            plan.metadata.accepted_sealed_marker_bytes) {
        return false;
    }
    return true;
}

[[nodiscard]] bool ValidatePlanForPublication(
    const RawSegmentArtifactPlanV1& plan) noexcept {
    return ValidatePlanCore(plan) &&
           plan.retained_segment_fd_bound &&
           plan.retained_segment_snapshot.size ==
               plan.metadata.logical_end_offset;
}

[[nodiscard]] bool RevalidateRetainedSegment(
    int stream_directory_fd,
    int sealed_segment_fd,
    const RawSegmentArtifactPlanV1& plan,
    RawSegmentArtifactIoV1& io,
    RawSegmentArtifactPublishResultV1* result) noexcept {
    RawSegmentArtifactFileInfoV1 directory{};
    int error_number =
        io.InspectDescriptor(
            stream_directory_fd, &directory);
    if (error_number != 0 ||
        !IsSafeDirectory(
            directory, io.EffectiveUserId())) {
        FailPublish(
            result,
            RawSegmentArtifactFailureV1::kDirectoryUnsafe,
            error_number == 0 ? EACCES : error_number);
        return false;
    }
    RawSegmentArtifactFileInfoV1 segment{};
    error_number =
        io.InspectDescriptor(sealed_segment_fd, &segment);
    if (error_number != 0 ||
        !IsSafeSegment(
            segment, io.EffectiveUserId()) ||
        segment.device != directory.device) {
        FailPublish(
            result,
            RawSegmentArtifactFailureV1::kSegmentUnsafe,
            error_number == 0 ? EACCES : error_number);
        return false;
    }
    if (segment != plan.retained_segment_snapshot) {
        FailPublish(
            result,
            RawSegmentArtifactFailureV1::kSegmentChanged,
            ESTALE);
        return false;
    }
    error_number = io.NameMatchesDescriptor(
        stream_directory_fd,
        plan.names.segment_name,
        sealed_segment_fd);
    if (error_number != 0) {
        FailPublish(
            result,
            RawSegmentArtifactFailureV1::
                kSegmentPathMismatch,
            error_number);
        return false;
    }
    return true;
}

[[nodiscard]] bool ReadAndValidateArtifact(
    int stream_directory_fd,
    int descriptor,
    std::string_view name,
    bool writable,
    const RawSegmentArtifactPlanV1& plan,
    RawSegmentArtifactIoV1& io,
    RawSegmentArtifactFailureV1 unsafe_failure,
    RawSegmentArtifactFailureV1 content_failure,
    RawSegmentArtifactPublishResultV1* result) noexcept {
    RawSegmentArtifactFileInfoV1 before{};
    int error_number =
        io.InspectDescriptor(descriptor, &before);
    if (error_number != 0 ||
        !IsSafeArtifact(
            before,
            io.EffectiveUserId(),
            plan.retained_segment_snapshot.device,
            writable)) {
        FailPublish(
            result,
            unsafe_failure,
            error_number == 0 ? EACCES : error_number);
        return false;
    }
    if (before.size !=
        static_cast<std::uint64_t>(
            plan.index_bytes.size())) {
        FailPublish(result, content_failure, EILSEQ);
        return false;
    }
    error_number = io.NameMatchesDescriptor(
        stream_directory_fd, name, descriptor);
    if (error_number != 0) {
        FailPublish(
            result,
            RawSegmentArtifactFailureV1::
                kNameToInodeMismatch,
            error_number);
        return false;
    }
    std::vector<std::byte> readback;
    try {
        readback.resize(plan.index_bytes.size());
    } catch (...) {
        FailPublish(
            result,
            RawSegmentArtifactFailureV1::
                kAllocationFailure,
            ENOMEM);
        return false;
    }
    error_number = 0;
    if (!ReadAll(
            io, descriptor, readback, &error_number)) {
        FailPublish(
            result,
            RawSegmentArtifactFailureV1::
                kArtifactReadback,
            error_number);
        return false;
    }
    RawSegmentArtifactFileInfoV1 after{};
    error_number =
        io.InspectDescriptor(descriptor, &after);
    if (error_number != 0 || after != before) {
        FailPublish(
            result,
            RawSegmentArtifactFailureV1::
                kArtifactChanged,
            error_number == 0 ? ESTALE : error_number);
        return false;
    }
    if (readback != plan.index_bytes ||
        l2flow::common::ComputeSha256(readback) !=
            plan.metadata.index_sha256) {
        FailPublish(result, content_failure, EILSEQ);
        return false;
    }
    return true;
}

[[nodiscard]] bool ValidateNewTemporaryDescriptor(
    int descriptor,
    const RawSegmentArtifactPlanV1& plan,
    RawSegmentArtifactIoV1& io,
    std::uint64_t expected_size,
    RawSegmentArtifactPublishResultV1* result) noexcept {
    RawSegmentArtifactFileInfoV1 info{};
    const int error_number =
        io.InspectDescriptor(descriptor, &info);
    if (error_number != 0 ||
        !IsSafeArtifact(
            info,
            io.EffectiveUserId(),
            plan.retained_segment_snapshot.device,
            true)) {
        FailPublish(
            result,
            RawSegmentArtifactFailureV1::
                kTemporaryUnsafe,
            error_number == 0 ? EACCES : error_number);
        return false;
    }
    if (info.size != expected_size) {
        FailPublish(
            result,
            RawSegmentArtifactFailureV1::
                kTemporaryUnsafe,
            EILSEQ);
        return false;
    }
    return true;
}

[[nodiscard]] bool FinishRenameAndReadback(
    int stream_directory_fd,
    int temporary_descriptor,
    const RawSegmentArtifactPlanV1& plan,
    RawSegmentArtifactIoV1& io,
    RawSegmentArtifactPublishResultV1* result) noexcept {
    int error_number = io.RenameNoReplace(
        stream_directory_fd,
        plan.names.index_temporary_name,
        plan.names.index_name);
    if (error_number != 0) {
        FailPublish(
            result,
            RawSegmentArtifactFailureV1::kArtifactRename,
            error_number);
        return false;
    }
    result->namespace_mutated = true;
    error_number =
        io.SyncDirectory(stream_directory_fd);
    if (error_number != 0) {
        FailPublish(
            result,
            RawSegmentArtifactFailureV1::kDirectorySync,
            error_number);
        return false;
    }
    result->directory_synced = true;
    return ReadAndValidateArtifact(
        stream_directory_fd,
        temporary_descriptor,
        plan.names.index_name,
        true,
        plan,
        io,
        RawSegmentArtifactFailureV1::kFinalUnsafe,
        RawSegmentArtifactFailureV1::kFinalConflict,
        result);
}

}  // namespace

std::string_view RawSegmentArtifactFailureV1Name(
    RawSegmentArtifactFailureV1 failure) noexcept {
    switch (failure) {
    case RawSegmentArtifactFailureV1::kNone:
        return "none";
    case RawSegmentArtifactFailureV1::kInvalidInput:
        return "invalid input";
    case RawSegmentArtifactFailureV1::kInvalidOptions:
        return "invalid options";
    case RawSegmentArtifactFailureV1::kNameOverflow:
        return "artifact name overflow";
    case RawSegmentArtifactFailureV1::kDirectoryUnsafe:
        return "unsafe stream directory";
    case RawSegmentArtifactFailureV1::kSegmentUnsafe:
        return "unsafe retained segment";
    case RawSegmentArtifactFailureV1::kSegmentTooLarge:
        return "segment exceeds bounded plan limit";
    case RawSegmentArtifactFailureV1::kSegmentRead:
        return "retained segment read failed";
    case RawSegmentArtifactFailureV1::kSegmentChanged:
        return "retained segment changed";
    case RawSegmentArtifactFailureV1::kSegmentPathMismatch:
        return "segment pathname does not name retained fd";
    case RawSegmentArtifactFailureV1::kRawScanInvalid:
        return "Raw segment scan invalid";
    case RawSegmentArtifactFailureV1::kRawSchemaMismatch:
        return "Raw schema mismatch";
    case RawSegmentArtifactFailureV1::kSealMarkerInvalid:
        return "accepted seal marker invalid";
    case RawSegmentArtifactFailureV1::kSealMarkerMismatch:
        return "accepted seal marker mismatch";
    case RawSegmentArtifactFailureV1::kIndexBuild:
        return "Raw index build failed";
    case RawSegmentArtifactFailureV1::kIndexSelfValidation:
        return "Raw index self-validation failed";
    case RawSegmentArtifactFailureV1::kAllocationFailure:
        return "allocation failure";
    case RawSegmentArtifactFailureV1::kPlanInvalid:
        return "artifact plan invalid";
    case RawSegmentArtifactFailureV1::kCausalProofMissing:
        return "segment/seal causal proof missing";
    case RawSegmentArtifactFailureV1::kNamespaceInspection:
        return "artifact namespace inspection failed";
    case RawSegmentArtifactFailureV1::
             kAmbiguousFinalAndTemporary:
        return "ambiguous final and temporary index";
    case RawSegmentArtifactFailureV1::kFinalUnsafe:
        return "unsafe final index";
    case RawSegmentArtifactFailureV1::kFinalConflict:
        return "conflicting final index";
    case RawSegmentArtifactFailureV1::kTemporaryUnsafe:
        return "unsafe temporary index";
    case RawSegmentArtifactFailureV1::kTemporaryConflict:
        return "partial or conflicting temporary index";
    case RawSegmentArtifactFailureV1::kFinalOpen:
        return "cannot open final index";
    case RawSegmentArtifactFailureV1::kTemporaryOpen:
        return "cannot open temporary index";
    case RawSegmentArtifactFailureV1::kTemporaryCreate:
        return "cannot create temporary index";
    case RawSegmentArtifactFailureV1::kArtifactWrite:
        return "index write failed";
    case RawSegmentArtifactFailureV1::kArtifactSync:
        return "index sync failed";
    case RawSegmentArtifactFailureV1::kArtifactRename:
        return "index NOREPLACE rename failed";
    case RawSegmentArtifactFailureV1::kDirectorySync:
        return "stream directory sync failed";
    case RawSegmentArtifactFailureV1::kNameToInodeMismatch:
        return "index pathname does not name retained fd";
    case RawSegmentArtifactFailureV1::kArtifactReadback:
        return "index readback failed";
    case RawSegmentArtifactFailureV1::kArtifactChanged:
        return "index changed during validation";
    }
    return "unknown Raw segment artifact failure";
}

RawSegmentArtifactPlanV1 BuildRawSegmentArtifactPlanV1(
    std::shared_ptr<const std::vector<std::byte>>
        exact_segment_bytes,
    const RawV1DurableMarkerWire& accepted_sealed_marker,
    RawSegmentArtifactOptionsV1 options) noexcept {
    RawSegmentArtifactPlanV1 plan;
    plan.options = options;
    if (exact_segment_bytes == nullptr) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::kInvalidInput);
        return plan;
    }
    if (!OptionsAreValid(options)) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::kInvalidOptions);
        return plan;
    }
    if (exact_segment_bytes->size() >
        options.maximum_segment_bytes) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::kSegmentTooLarge,
            EFBIG);
        return plan;
    }

    const std::uint64_t logical_end =
        static_cast<std::uint64_t>(
            exact_segment_bytes->size());
    const RawSegmentScanResult scan =
        ScanRawSegmentV1(
            exact_segment_bytes, logical_end);
    if (!scan.ok() ||
        scan.validated_end_offset != logical_end) {
        plan.reader_error = scan.error;
        plan.codec_error = scan.codec_error;
        plan.evidence_offset = scan.error_offset;
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::kRawScanInvalid,
            EILSEQ);
        return plan;
    }
    if (scan.segment.raw_schema_sha256 !=
        options.expected_raw_schema_sha256) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::
                kRawSchemaMismatch,
            EILSEQ);
        return plan;
    }
    const int name_error = FormatNames(
        scan.segment.segment_sequence, &plan.names);
    if (name_error != 0) {
        FailPlan(
            &plan,
            name_error == ENOMEM
                ? RawSegmentArtifactFailureV1::
                      kAllocationFailure
                : RawSegmentArtifactFailureV1::
                      kNameOverflow,
            name_error);
        return plan;
    }

    DurableMarkerV1 marker{};
    const RawV1Error marker_error =
        DecodeDurableMarkerV1(
            accepted_sealed_marker, &marker);
    if (marker_error != RawV1Error::kNone ||
        marker.marker_flags != kRawV1SegmentSealed) {
        plan.codec_error = marker_error;
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::
                kSealMarkerInvalid,
            EILSEQ);
        return plan;
    }
    std::uint64_t expected_global_end = 0U;
    if (!CheckedAdd(
            scan.segment.segment_base_wal_pos,
            logical_end,
            &expected_global_end) ||
        marker.source_stream_id !=
            scan.segment.source_stream_id ||
        marker.segment_sequence !=
            scan.segment.segment_sequence ||
        marker.durable_segment_offset != logical_end ||
        marker.durable_global_wal_pos !=
            expected_global_end) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::
                kSealMarkerMismatch,
            EILSEQ);
        return plan;
    }
    if (scan.records.empty()) {
        if (scan.segment.first_ingress_sequence == 0U ||
            marker.durable_ingress_sequence !=
                scan.segment.first_ingress_sequence - 1U) {
            FailPlan(
                &plan,
                RawSegmentArtifactFailureV1::
                    kSealMarkerMismatch,
                EILSEQ);
            return plan;
        }
    } else if (
        marker.durable_ingress_sequence !=
        scan.records.back().header().ingress_sequence) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::
                kSealMarkerMismatch,
            EILSEQ);
        return plan;
    }

    plan.metadata.segment = scan.segment;
    plan.metadata.accepted_sealed_marker = marker;
    plan.metadata.accepted_sealed_marker_bytes =
        accepted_sealed_marker;
    plan.metadata.logical_end_offset = logical_end;
    plan.metadata.record_count =
        static_cast<std::uint64_t>(scan.records.size());
    if (!scan.records.empty()) {
        plan.metadata.actual_first_ingress_sequence =
            scan.records.front().header().ingress_sequence;
        plan.metadata.actual_last_ingress_sequence =
            scan.records.back().header().ingress_sequence;
    }
    plan.metadata.segment_sha256 =
        l2flow::common::ComputeSha256(
            std::span<const std::byte>(
                exact_segment_bytes->data(),
                exact_segment_bytes->size()));

    RawIndexHeaderV1 index_header{};
    index_header.capture_date =
        scan.segment.capture_date;
    index_header.source_stream_id =
        scan.segment.source_stream_id;
    index_header.stream_day_id =
        scan.segment.stream_day_id;
    index_header.segment_sequence =
        scan.segment.segment_sequence;
    index_header.segment_base_wal_pos =
        scan.segment.segment_base_wal_pos;
    index_header.raw_schema_sha256 =
        scan.segment.raw_schema_sha256;
    index_header.sample_record_interval =
        options.sample_record_interval;
    index_header.sample_raw_bytes_interval =
        options.sample_raw_bytes_interval;
    RawIndexBuilderV1 builder(index_header);
    for (const RawRecordView& record : scan.records) {
        const RawRecordHeaderV1& header = record.header();
        RawIndexEntryV1 entry{};
        entry.ingress_sequence =
            header.ingress_sequence;
        entry.record_start_wal_pos =
            record.record_start_wal_pos();
        entry.record_end_wal_pos =
            record.record_end_wal_pos();
        entry.segment_file_offset =
            record.record_start_offset();
        entry.vendor_sequence_id =
            header.vendor_sequence_id;
        entry.recv_monotonic_ns =
            header.recv_monotonic_ns;
        entry.connection_epoch_hint =
            header.connection_epoch_hint;
        entry.vendor_service_version =
            header.vendor_service_version;
        entry.vendor_message_id =
            header.vendor_message_id;
        entry.vendor_service_id =
            header.vendor_service_id;
        const RawIndexV1Error add_error =
            builder.AddRecord(entry);
        if (add_error != RawIndexV1Error::kNone) {
            plan.index_error = add_error;
            FailPlan(
                &plan,
                RawSegmentArtifactFailureV1::kIndexBuild,
                EILSEQ);
            return plan;
        }
    }
    plan.index_error = builder.Build(
        plan.metadata.segment_sha256,
        accepted_sealed_marker,
        &plan.index_bytes);
    if (plan.index_error != RawIndexV1Error::kNone) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::kIndexBuild,
            EILSEQ);
        return plan;
    }
    plan.metadata.index_sha256 =
        l2flow::common::ComputeSha256(plan.index_bytes);
    RawIndexFileV1 decoded_index{};
    plan.index_error = ValidateRawIndexFileV1(
        plan.index_bytes,
        ExpectedIndexIdentity(plan),
        &decoded_index);
    if (plan.index_error != RawIndexV1Error::kNone ||
        decoded_index.footer.segment_sha256 !=
            plan.metadata.segment_sha256 ||
        decoded_index.footer.accepted_segment_sealed_marker !=
            accepted_sealed_marker ||
        decoded_index.footer.segment_logical_end_offset !=
            logical_end ||
        decoded_index.footer.segment_record_count !=
            plan.metadata.record_count) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::
                kIndexSelfValidation,
            EILSEQ);
    }
    return plan;
}

RawSegmentArtifactPlanV1
BuildIncrementalRawSegmentArtifactPlanV1(
    RawSealedSegmentMetadataV1 metadata,
    const RawIndexBuilderV1& index_builder,
    RawSegmentArtifactOptionsV1 options) noexcept {
    RawSegmentArtifactPlanV1 plan;
    plan.options = options;
    plan.metadata = std::move(metadata);
    if (!OptionsAreValid(options)) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::kInvalidOptions);
        return plan;
    }
    const int name_error = FormatNames(
        plan.metadata.segment.segment_sequence,
        &plan.names);
    if (name_error != 0) {
        FailPlan(
            &plan,
            name_error == ENOMEM
                ? RawSegmentArtifactFailureV1::
                      kAllocationFailure
                : RawSegmentArtifactFailureV1::
                      kNameOverflow,
            name_error);
        return plan;
    }
    if (index_builder.error() != RawIndexV1Error::kNone ||
        index_builder.record_count() !=
            plan.metadata.record_count) {
        plan.index_error =
            index_builder.error() ==
                    RawIndexV1Error::kNone
                ? RawIndexV1Error::kEntryCountMismatch
                : index_builder.error();
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::kIndexBuild,
            EILSEQ);
        return plan;
    }
    plan.index_error = index_builder.Build(
        plan.metadata.segment_sha256,
        plan.metadata.accepted_sealed_marker_bytes,
        &plan.index_bytes);
    if (plan.index_error != RawIndexV1Error::kNone) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::kIndexBuild,
            EILSEQ);
        return plan;
    }
    plan.metadata.index_sha256 =
        l2flow::common::ComputeSha256(plan.index_bytes);
    if (!ValidatePlanCore(plan)) {
        RawIndexFileV1 decoded{};
        plan.index_error = ValidateRawIndexFileV1(
            plan.index_bytes,
            ExpectedIndexIdentity(plan),
            &decoded);
        FailPlan(
            &plan,
            plan.index_error == RawIndexV1Error::kNone
                ? RawSegmentArtifactFailureV1::kPlanInvalid
                : RawSegmentArtifactFailureV1::
                      kIndexSelfValidation,
            EILSEQ);
    }
    return plan;
}

RawSegmentArtifactPlanV1
BindRawSegmentArtifactPlanToFdV1(
    int stream_directory_fd,
    int sealed_segment_fd,
    RawSegmentArtifactPlanV1 plan,
    RawSegmentArtifactIoV1& io) noexcept {
    if (!ValidatePlanCore(plan) ||
        plan.retained_segment_fd_bound) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::kPlanInvalid);
        return plan;
    }
    RawSegmentArtifactFileInfoV1 directory{};
    int error_number =
        io.InspectDescriptor(
            stream_directory_fd, &directory);
    if (error_number != 0 ||
        !IsSafeDirectory(
            directory, io.EffectiveUserId())) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::kDirectoryUnsafe,
            error_number == 0 ? EACCES : error_number);
        return plan;
    }
    RawSegmentArtifactFileInfoV1 before{};
    error_number =
        io.InspectDescriptor(sealed_segment_fd, &before);
    if (error_number != 0 ||
        !IsSafeSegment(
            before, io.EffectiveUserId()) ||
        before.device != directory.device) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::kSegmentUnsafe,
            error_number == 0 ? EACCES : error_number);
        return plan;
    }
    if (before.size !=
        plan.metadata.logical_end_offset) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::kSegmentChanged,
            ESTALE);
        return plan;
    }

    RawV1SegmentHeaderWire read_header{};
    error_number = 0;
    if (!ReadAll(
            io,
            sealed_segment_fd,
            read_header,
            &error_number)) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::kSegmentRead,
            error_number);
        return plan;
    }
    RawSegmentArtifactFileInfoV1 after{};
    error_number =
        io.InspectDescriptor(sealed_segment_fd, &after);
    if (error_number != 0 || after != before) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::kSegmentChanged,
            error_number == 0 ? ESTALE : error_number);
        return plan;
    }
    RawV1SegmentHeaderWire expected_header{};
    if (EncodeSegmentHeaderV1(
            plan.metadata.segment,
            &expected_header) != RawV1Error::kNone ||
        read_header != expected_header) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::kSegmentChanged,
            EILSEQ);
        return plan;
    }
    error_number = io.NameMatchesDescriptor(
        stream_directory_fd,
        plan.names.segment_name,
        sealed_segment_fd);
    if (error_number != 0) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::
                kSegmentPathMismatch,
            error_number);
        return plan;
    }
    plan.retained_segment_fd_bound = true;
    plan.retained_segment_snapshot = before;
    return plan;
}

RawSegmentArtifactPlanV1
PrepareRawSegmentArtifactPlanForFdV1(
    int stream_directory_fd,
    int sealed_segment_fd,
    const RawV1DurableMarkerWire& accepted_sealed_marker,
    RawSegmentArtifactOptionsV1 options,
    RawSegmentArtifactIoV1& io) noexcept {
    RawSegmentArtifactPlanV1 failed;
    failed.options = options;
    if (!OptionsAreValid(options)) {
        FailPlan(
            &failed,
            RawSegmentArtifactFailureV1::kInvalidOptions);
        return failed;
    }
    RawSegmentArtifactFileInfoV1 directory{};
    int error_number =
        io.InspectDescriptor(
            stream_directory_fd, &directory);
    if (error_number != 0 ||
        !IsSafeDirectory(
            directory, io.EffectiveUserId())) {
        FailPlan(
            &failed,
            RawSegmentArtifactFailureV1::kDirectoryUnsafe,
            error_number == 0 ? EACCES : error_number);
        return failed;
    }
    RawSegmentArtifactFileInfoV1 before{};
    error_number =
        io.InspectDescriptor(sealed_segment_fd, &before);
    if (error_number != 0 ||
        !IsSafeSegment(
            before, io.EffectiveUserId()) ||
        before.device != directory.device) {
        FailPlan(
            &failed,
            RawSegmentArtifactFailureV1::kSegmentUnsafe,
            error_number == 0 ? EACCES : error_number);
        return failed;
    }
    if (before.size < kRawV1SegmentHeaderBytes ||
        before.size > options.maximum_segment_bytes ||
        before.size >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
        FailPlan(
            &failed,
            RawSegmentArtifactFailureV1::kSegmentTooLarge,
            EFBIG);
        return failed;
    }

    std::shared_ptr<std::vector<std::byte>> mutable_bytes;
    try {
        mutable_bytes =
            std::make_shared<std::vector<std::byte>>(
                static_cast<std::size_t>(before.size));
    } catch (...) {
        FailPlan(
            &failed,
            RawSegmentArtifactFailureV1::
                kAllocationFailure,
            ENOMEM);
        return failed;
    }
    error_number = 0;
    if (!ReadAll(
            io,
            sealed_segment_fd,
            *mutable_bytes,
            &error_number)) {
        FailPlan(
            &failed,
            RawSegmentArtifactFailureV1::kSegmentRead,
            error_number);
        return failed;
    }
    RawSegmentArtifactFileInfoV1 after{};
    error_number =
        io.InspectDescriptor(sealed_segment_fd, &after);
    if (error_number != 0 || after != before) {
        FailPlan(
            &failed,
            RawSegmentArtifactFailureV1::kSegmentChanged,
            error_number == 0 ? ESTALE : error_number);
        return failed;
    }

    std::shared_ptr<const std::vector<std::byte>>
        immutable_bytes = std::move(mutable_bytes);
    RawSegmentArtifactPlanV1 plan =
        BuildRawSegmentArtifactPlanV1(
            std::move(immutable_bytes),
            accepted_sealed_marker,
            options);
    if (!plan.ok()) {
        return plan;
    }
    error_number = io.NameMatchesDescriptor(
        stream_directory_fd,
        plan.names.segment_name,
        sealed_segment_fd);
    if (error_number != 0) {
        FailPlan(
            &plan,
            RawSegmentArtifactFailureV1::
                kSegmentPathMismatch,
            error_number);
        return plan;
    }
    plan.retained_segment_fd_bound = true;
    plan.retained_segment_snapshot = before;
    return plan;
}

RawSegmentArtifactPlanV1
PrepareRawSegmentArtifactPlanForPosixFdV1(
    int stream_directory_fd,
    int sealed_segment_fd,
    const RawV1DurableMarkerWire& accepted_sealed_marker,
    RawSegmentArtifactOptionsV1 options) noexcept {
    PosixRawSegmentArtifactIoV1 io;
    return PrepareRawSegmentArtifactPlanForFdV1(
        stream_directory_fd,
        sealed_segment_fd,
        accepted_sealed_marker,
        options,
        io);
}

RawSegmentArtifactInspectionV1
InspectExistingRawSegmentArtifactAtV1(
    int stream_directory_fd,
    int sealed_segment_fd,
    const RawSegmentArtifactPlanV1& plan) noexcept {
    PosixRawSegmentArtifactIoV1 io;
    RawSegmentArtifactInspectionV1 inspection;
    inspection.metadata = plan.metadata;
    RawSegmentArtifactPublishResultV1 scratch;
    scratch.metadata = plan.metadata;
    if (!ValidatePlanForPublication(plan)) {
        inspection.failure =
            RawSegmentArtifactFailureV1::kPlanInvalid;
        inspection.error_number = EINVAL;
        return inspection;
    }
    if (!RevalidateRetainedSegment(
            stream_directory_fd,
            sealed_segment_fd,
            plan,
            io,
            &scratch)) {
        inspection.failure = scratch.failure;
        inspection.error_number =
            scratch.error_number;
        return inspection;
    }

    RawSegmentArtifactFileInfoV1 final_info{};
    const int final_status = io.InspectName(
        stream_directory_fd,
        plan.names.index_name,
        &final_info);
    RawSegmentArtifactFileInfoV1 temporary_info{};
    const int temporary_status = io.InspectName(
        stream_directory_fd,
        plan.names.index_temporary_name,
        &temporary_info);
    const bool final_exists = final_status == 0;
    const bool temporary_exists =
        temporary_status == 0;
    if ((final_status != 0 && final_status != ENOENT) ||
        (temporary_status != 0 &&
         temporary_status != ENOENT)) {
        inspection.failure =
            RawSegmentArtifactFailureV1::
                kNamespaceInspection;
        inspection.error_number =
            final_status != 0 &&
                    final_status != ENOENT
                ? final_status
                : temporary_status;
        return inspection;
    }
    if (final_exists && temporary_exists) {
        inspection.failure =
            RawSegmentArtifactFailureV1::
                kAmbiguousFinalAndTemporary;
        inspection.error_number = EEXIST;
        return inspection;
    }
    if (!final_exists && !temporary_exists) {
        return inspection;
    }

    const bool temporary = temporary_exists;
    const std::string_view name =
        temporary
            ? std::string_view(
                  plan.names.index_temporary_name)
            : std::string_view(plan.names.index_name);
    const RawSegmentArtifactOpenResultV1 opened =
        io.OpenExisting(
            stream_directory_fd,
            name,
            temporary);
    if (opened.descriptor < 0 ||
        opened.error_number != 0) {
        inspection.failure =
            temporary
                ? RawSegmentArtifactFailureV1::
                      kTemporaryOpen
                : RawSegmentArtifactFailureV1::
                      kFinalOpen;
        inspection.error_number =
            opened.error_number == 0
                ? EIO
                : opened.error_number;
        return inspection;
    }
    const bool valid = ReadAndValidateArtifact(
        stream_directory_fd,
        opened.descriptor,
        name,
        temporary,
        plan,
        io,
        temporary
            ? RawSegmentArtifactFailureV1::
                  kTemporaryUnsafe
            : RawSegmentArtifactFailureV1::
                  kFinalUnsafe,
        temporary
            ? RawSegmentArtifactFailureV1::
                  kTemporaryConflict
            : RawSegmentArtifactFailureV1::
                  kFinalConflict,
        &scratch);
    io.Close(opened.descriptor);
    if (!valid) {
        inspection.failure = scratch.failure;
        inspection.error_number =
            scratch.error_number;
        return inspection;
    }
    inspection.state =
        temporary
            ? RawSegmentArtifactExistingStateV1::
                  kMatchingCompleteTemporary
            : RawSegmentArtifactExistingStateV1::
                  kMatchingFinal;
    return inspection;
}

RawSegmentArtifactPublishResultV1
PublishRawSegmentArtifactPlanV1(
    int stream_directory_fd,
    int sealed_segment_fd,
    const RawSegmentArtifactPlanV1& plan,
    RawSegmentArtifactCausalProofV1 causal_proof,
    RawSegmentArtifactIoV1& io) noexcept {
    RawSegmentArtifactPublishResultV1 result;
    result.metadata = plan.metadata;
    if (!ValidatePlanForPublication(plan)) {
        FailPublish(
            &result,
            RawSegmentArtifactFailureV1::kPlanInvalid);
        return result;
    }
    if (!causal_proof
             .segment_truncated_and_synced_to_logical_end ||
        !causal_proof
             .accepted_seal_marker_journal_synced ||
        causal_proof
                .synced_segment_logical_end_offset !=
            plan.metadata.logical_end_offset ||
        causal_proof.synced_segment_sha256 !=
            plan.metadata.segment_sha256 ||
        causal_proof
                .journal_synced_sealed_marker_bytes !=
            plan.metadata.accepted_sealed_marker_bytes) {
        FailPublish(
            &result,
            RawSegmentArtifactFailureV1::
                kCausalProofMissing,
            EPERM);
        return result;
    }
    if (!RevalidateRetainedSegment(
            stream_directory_fd,
            sealed_segment_fd,
            plan,
            io,
            &result)) {
        return result;
    }

    RawSegmentArtifactFileInfoV1 final_name_info{};
    const int final_status = io.InspectName(
        stream_directory_fd,
        plan.names.index_name,
        &final_name_info);
    RawSegmentArtifactFileInfoV1 temporary_name_info{};
    const int temporary_status = io.InspectName(
        stream_directory_fd,
        plan.names.index_temporary_name,
        &temporary_name_info);
    const bool final_exists = final_status == 0;
    const bool temporary_exists = temporary_status == 0;
    if ((final_status != 0 && final_status != ENOENT) ||
        (temporary_status != 0 &&
         temporary_status != ENOENT)) {
        FailPublish(
            &result,
            RawSegmentArtifactFailureV1::
                kNamespaceInspection,
            final_status != 0 && final_status != ENOENT
                ? final_status
                : temporary_status);
        return result;
    }
    if (final_exists && temporary_exists) {
        FailPublish(
            &result,
            RawSegmentArtifactFailureV1::
                kAmbiguousFinalAndTemporary,
            EEXIST);
        return result;
    }

    if (final_exists) {
        const RawSegmentArtifactOpenResultV1 opened =
            io.OpenExisting(
                stream_directory_fd,
                plan.names.index_name,
                false);
        if (opened.descriptor < 0 ||
            opened.error_number != 0) {
            FailPublish(
                &result,
                RawSegmentArtifactFailureV1::kFinalOpen,
                opened.error_number);
            return result;
        }
        const int descriptor = opened.descriptor;
        if (!ReadAndValidateArtifact(
                stream_directory_fd,
                descriptor,
                plan.names.index_name,
                false,
                plan,
                io,
                RawSegmentArtifactFailureV1::kFinalUnsafe,
                RawSegmentArtifactFailureV1::kFinalConflict,
                &result)) {
            io.Close(descriptor);
            return result;
        }
        int error_number = io.SyncFile(descriptor);
        if (error_number != 0) {
            FailPublish(
                &result,
                RawSegmentArtifactFailureV1::kArtifactSync,
                error_number);
            io.Close(descriptor);
            return result;
        }
        error_number =
            io.SyncDirectory(stream_directory_fd);
        if (error_number != 0) {
            FailPublish(
                &result,
                RawSegmentArtifactFailureV1::kDirectorySync,
                error_number);
            io.Close(descriptor);
            return result;
        }
        result.directory_synced = true;
        if (!ReadAndValidateArtifact(
                stream_directory_fd,
                descriptor,
                plan.names.index_name,
                false,
                plan,
                io,
                RawSegmentArtifactFailureV1::kFinalUnsafe,
                RawSegmentArtifactFailureV1::kFinalConflict,
                &result)) {
            io.Close(descriptor);
            return result;
        }
        io.Close(descriptor);
        result.disposition =
            RawSegmentArtifactDispositionV1::
                kAcceptedExistingFinal;
        return result;
    }

    if (temporary_exists) {
        const RawSegmentArtifactOpenResultV1 opened =
            io.OpenExisting(
                stream_directory_fd,
                plan.names.index_temporary_name,
                true);
        if (opened.descriptor < 0 ||
            opened.error_number != 0) {
            FailPublish(
                &result,
                RawSegmentArtifactFailureV1::
                    kTemporaryOpen,
                opened.error_number);
            return result;
        }
        const int descriptor = opened.descriptor;
        if (!ReadAndValidateArtifact(
                stream_directory_fd,
                descriptor,
                plan.names.index_temporary_name,
                true,
                plan,
                io,
                RawSegmentArtifactFailureV1::
                    kTemporaryUnsafe,
                RawSegmentArtifactFailureV1::
                    kTemporaryConflict,
                &result)) {
            io.Close(descriptor);
            return result;
        }
        int error_number = io.SyncFile(descriptor);
        if (error_number != 0) {
            FailPublish(
                &result,
                RawSegmentArtifactFailureV1::kArtifactSync,
                error_number);
            io.Close(descriptor);
            return result;
        }
        if (!FinishRenameAndReadback(
                stream_directory_fd,
                descriptor,
                plan,
                io,
                &result)) {
            io.Close(descriptor);
            return result;
        }
        io.Close(descriptor);
        result.disposition =
            RawSegmentArtifactDispositionV1::
                kAdoptedCompleteTemporary;
        return result;
    }

    const RawSegmentArtifactOpenResultV1 created =
        io.CreateExclusive(
            stream_directory_fd,
            plan.names.index_temporary_name);
    if (created.descriptor < 0 ||
        created.error_number != 0) {
        FailPublish(
            &result,
            RawSegmentArtifactFailureV1::
                kTemporaryCreate,
            created.error_number);
        return result;
    }
    result.namespace_mutated = true;
    const int descriptor = created.descriptor;
    if (!ValidateNewTemporaryDescriptor(
            descriptor, plan, io, 0U, &result)) {
        io.Close(descriptor);
        return result;
    }
    int error_number = 0;
    if (!WriteAll(
            io,
            descriptor,
            plan.index_bytes,
            &error_number)) {
        FailPublish(
            &result,
            RawSegmentArtifactFailureV1::kArtifactWrite,
            error_number);
        io.Close(descriptor);
        return result;
    }
    if (!ValidateNewTemporaryDescriptor(
            descriptor,
            plan,
            io,
            static_cast<std::uint64_t>(
                plan.index_bytes.size()),
            &result)) {
        io.Close(descriptor);
        return result;
    }
    error_number = io.SyncFile(descriptor);
    if (error_number != 0) {
        FailPublish(
            &result,
            RawSegmentArtifactFailureV1::kArtifactSync,
            error_number);
        io.Close(descriptor);
        return result;
    }
    if (!FinishRenameAndReadback(
            stream_directory_fd,
            descriptor,
            plan,
            io,
            &result)) {
        io.Close(descriptor);
        return result;
    }
    io.Close(descriptor);
    result.disposition =
        RawSegmentArtifactDispositionV1::kPublishedNew;
    return result;
}

RawSegmentArtifactPublishResultV1
BuildAndPublishRawSegmentArtifactAtV1(
    int stream_directory_fd,
    int sealed_segment_fd,
    const RawV1DurableMarkerWire& accepted_sealed_marker,
    RawSegmentArtifactOptionsV1 options,
    RawSegmentArtifactCausalProofV1 causal_proof) noexcept {
    PosixRawSegmentArtifactIoV1 io;
    RawSegmentArtifactPlanV1 plan =
        PrepareRawSegmentArtifactPlanForFdV1(
            stream_directory_fd,
            sealed_segment_fd,
            accepted_sealed_marker,
            options,
            io);
    if (!plan.ok()) {
        RawSegmentArtifactPublishResultV1 result;
        result.failure = plan.failure;
        result.error_number = plan.error_number;
        result.metadata = plan.metadata;
        return result;
    }
    return PublishRawSegmentArtifactPlanV1(
        stream_directory_fd,
        sealed_segment_fd,
        plan,
        causal_proof,
        io);
}

RawSegmentArtifactPublishResultV1
BindAndPublishIncrementalRawSegmentArtifactAtV1(
    int stream_directory_fd,
    RawSegmentArtifactPlanV1 plan,
    RawSegmentArtifactCausalProofV1
        causal_proof) noexcept {
    PosixRawSegmentArtifactIoV1 io;
    RawSegmentArtifactPublishResultV1 failed{};
    failed.metadata = plan.metadata;
    if (!plan.ok() ||
        plan.retained_segment_fd_bound ||
        stream_directory_fd < 0 ||
        plan.names.segment_name.empty()) {
        failed.failure =
            RawSegmentArtifactFailureV1::kPlanInvalid;
        failed.error_number = EINVAL;
        return failed;
    }

    const RawSegmentArtifactOpenResultV1 opened =
        io.OpenExisting(
            stream_directory_fd,
            plan.names.segment_name,
            false);
    if (opened.descriptor < 0 ||
        opened.error_number != 0) {
        failed.failure =
            RawSegmentArtifactFailureV1::kSegmentUnsafe;
        failed.error_number =
            opened.error_number == 0
                ? EIO
                : opened.error_number;
        return failed;
    }
    const int segment_fd = opened.descriptor;
    RawSegmentArtifactPlanV1 bound =
        BindRawSegmentArtifactPlanToFdV1(
            stream_directory_fd,
            segment_fd,
            std::move(plan),
            io);
    if (!bound.ok()) {
        failed.failure = bound.failure;
        failed.error_number = bound.error_number;
        failed.metadata = bound.metadata;
        io.Close(segment_fd);
        return failed;
    }
    RawSegmentArtifactPublishResultV1 result =
        PublishRawSegmentArtifactPlanV1(
            stream_directory_fd,
            segment_fd,
            bound,
            causal_proof,
            io);
    io.Close(segment_fd);
    return result;
}

}  // namespace l2flow::ingress
