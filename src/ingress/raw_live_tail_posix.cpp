#include "l2flow/ingress/raw_live_tail_posix.h"

#include "l2flow/common/identity128.h"
#include "l2flow/ingress/raw_control_file.h"
#include "l2flow/ingress/raw_namespace.h"
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

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace l2flow::ingress {
namespace {

inline constexpr std::size_t kSegmentFilenameBytes = 20U;

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

[[nodiscard]] int OpenAtNoIntr(
    int directory_fd,
    const char* name,
    int flags) noexcept {
    for (;;) {
        const int descriptor =
            ::openat(directory_fd, name, flags);
        if (descriptor >= 0 || errno != EINTR) {
            return descriptor;
        }
    }
}

[[nodiscard]] bool FormatSegmentFilename(
    std::uint32_t sequence,
    std::array<char, kSegmentFilenameBytes + 1U>*
        output) noexcept {
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
            static_cast<unsigned int>('0') +
            remaining % 10U);
        remaining /= 10U;
    }
    std::copy(
        suffix.begin(),
        suffix.end(),
        output->begin() + 16);
    return remaining == 0U;
}

[[nodiscard]] int ValidateReadOnlyDescription(
    int descriptor) noexcept {
    const int status_flags =
        ::fcntl(descriptor, F_GETFL);
    if (status_flags < 0) {
        return errno;
    }
    const int descriptor_flags =
        ::fcntl(descriptor, F_GETFD);
    if (descriptor_flags < 0) {
        return errno;
    }
    if ((status_flags & O_ACCMODE) != O_RDONLY ||
        (status_flags & O_APPEND) != 0 ||
        (status_flags & O_NONBLOCK) == 0 ||
        (status_flags & O_NOATIME) == 0 ||
        (descriptor_flags & FD_CLOEXEC) == 0) {
        return EINVAL;
    }
    return 0;
}

[[nodiscard]] int ValidatePrivateRegularStatus(
    const struct stat& status) noexcept {
    if (!S_ISREG(status.st_mode) ||
        status.st_size < 0) {
        return EINVAL;
    }
    if (status.st_uid != ::geteuid() ||
        (status.st_mode & 07777U) != 0600U) {
        return EACCES;
    }
    if (status.st_nlink != static_cast<nlink_t>(1)) {
        return EMLINK;
    }
    return 0;
}

[[nodiscard]] int InspectNamedRegular(
    int directory_fd,
    const char* name,
    int descriptor,
    struct stat* status) noexcept {
    if (directory_fd < 0 || name == nullptr ||
        descriptor < 0 || status == nullptr) {
        return EINVAL;
    }
    const int flags_error =
        ValidateReadOnlyDescription(descriptor);
    if (flags_error != 0) {
        return flags_error;
    }
    struct stat opened {};
    struct stat named {};
    if (::fstat(descriptor, &opened) != 0) {
        return errno;
    }
    if (::fstatat(
            directory_fd,
            name,
            &named,
            AT_SYMLINK_NOFOLLOW) != 0) {
        return errno;
    }
    const int opened_error =
        ValidatePrivateRegularStatus(opened);
    if (opened_error != 0) {
        return opened_error;
    }
    const int named_error =
        ValidatePrivateRegularStatus(named);
    if (named_error != 0) {
        return named_error;
    }
    if (!SameInode(opened, named)) {
        return ESTALE;
    }
    *status = opened;
    return 0;
}

[[nodiscard]] int ReadExact(
    int descriptor,
    std::uint64_t offset,
    std::span<std::byte> output) noexcept {
    std::size_t completed = 0U;
    while (completed < output.size()) {
        std::uint64_t current_offset = 0U;
        if (!CheckedAdd(
                offset,
                static_cast<std::uint64_t>(completed),
                &current_offset) ||
            current_offset >
                static_cast<std::uint64_t>(
                    std::numeric_limits<off_t>::max())) {
            return EOVERFLOW;
        }
        const std::size_t request = std::min(
            output.size() - completed,
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max()));
        const ssize_t result = ::pread(
            descriptor,
            output.data() + completed,
            request,
            static_cast<off_t>(current_offset));
        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }
            return errno;
        }
        if (result == 0) {
            return EIO;
        }
        completed += static_cast<std::size_t>(result);
    }
    return 0;
}

[[nodiscard]] int RetainPrivateDirectory(
    int authority_fd) noexcept {
    if (authority_fd < 0) {
        errno = EBADF;
        return -1;
    }
    struct stat authority {};
    if (::fstat(authority_fd, &authority) != 0) {
        return -1;
    }
    if (!S_ISDIR(authority.st_mode) ||
        authority.st_uid != ::geteuid() ||
        (authority.st_mode & (S_IWGRP | S_IWOTH)) != 0) {
        errno = EACCES;
        return -1;
    }
    const int retained = OpenAtNoIntr(
        authority_fd,
        ".",
        O_RDONLY | O_DIRECTORY | O_NOFOLLOW |
            O_NONBLOCK | O_CLOEXEC | O_NOATIME);
    if (retained < 0) {
        return -1;
    }
    struct stat reopened {};
    const int status_flags = ::fcntl(retained, F_GETFL);
    const int descriptor_flags = ::fcntl(retained, F_GETFD);
    if (::fstat(retained, &reopened) != 0 ||
        !S_ISDIR(reopened.st_mode) ||
        reopened.st_uid != ::geteuid() ||
        (reopened.st_mode & (S_IWGRP | S_IWOTH)) != 0 ||
        !SameInode(authority, reopened) ||
        status_flags < 0 ||
        (status_flags & O_ACCMODE) != O_RDONLY ||
        (status_flags & O_DIRECTORY) == 0 ||
        (status_flags & O_NONBLOCK) == 0 ||
        (status_flags & O_NOATIME) == 0 ||
        descriptor_flags < 0 ||
        (descriptor_flags & FD_CLOEXEC) == 0) {
        const int saved_error =
            errno == 0 ? EINVAL : errno;
        static_cast<void>(::close(retained));
        errno = saved_error;
        return -1;
    }
    return retained;
}

[[nodiscard]] bool MarkerWireEqual(
    const RawV1DurableMarkerWire& left,
    const RawV1DurableMarkerWire& right) noexcept {
    return std::equal(
        left.begin(), left.end(), right.begin());
}

}  // namespace

struct RawLiveTailPosixSource::Impl final {
    struct RetainedSegment final {
        ~RetainedSegment() {
            if (descriptor >= 0) {
                static_cast<void>(::close(descriptor));
            }
        }

        std::uint32_t sequence = 0U;
        int descriptor = -1;
        SegmentHeaderV1 header{};
    };

    struct SegmentJournalFact final {
        bool has_marker = false;
        bool sealed = false;
        DurableMarkerV1 latest{};
    };

    ~Impl() {
        control.reset();
        if (journal_fd >= 0) {
            static_cast<void>(::close(journal_fd));
        }
        if (directory_fd >= 0) {
            static_cast<void>(::close(directory_fd));
        }
    }

    [[nodiscard]] int ValidateControlDescriptor() const noexcept {
        if (control == nullptr) {
            return EBADF;
        }
        return ValidateReadOnlyDescription(
            control->descriptor());
    }

    [[nodiscard]] int OpenStableControl(
        std::unique_ptr<RawControlFileReader>* opened,
        RawControlSnapshot* snapshot,
        std::uint64_t* generation) noexcept {
        if (opened == nullptr || snapshot == nullptr ||
            generation == nullptr) {
            return EINVAL;
        }
        for (std::uint32_t attempt = 0U;
             attempt < limits.max_control_reattach_attempts;
             ++attempt) {
            std::string ignored;
            std::unique_ptr<RawControlFileReader> candidate =
                OpenRawControlFile(
                    directory_fd,
                    source_stream_id,
                    capture_date,
                    &ignored);
            if (candidate == nullptr) {
                return errno == 0 ? EINVAL : errno;
            }
            const int flags_error =
                ValidateReadOnlyDescription(
                    candidate->descriptor());
            if (flags_error != 0) {
                return flags_error;
            }
            RawControlSnapshot current;
            std::uint64_t current_generation = 0U;
            if (!candidate->Read(
                    &current, &current_generation)) {
                if (!candidate->PathStillNamesMapping()) {
                    continue;
                }
                return EAGAIN;
            }
            if (!candidate->PathStillNamesMapping()) {
                continue;
            }
            *snapshot = current;
            *generation = current_generation;
            *opened = std::move(candidate);
            return 0;
        }
        return EAGAIN;
    }

    [[nodiscard]] int ReadStableControl(
        RawControlSnapshot* snapshot,
        std::uint64_t* generation) noexcept {
        if (snapshot == nullptr || generation == nullptr) {
            return EINVAL;
        }
        for (std::uint32_t attempt = 0U;
             attempt < limits.max_control_reattach_attempts;
             ++attempt) {
            if (control == nullptr ||
                !control->PathStillNamesMapping()) {
                std::unique_ptr<RawControlFileReader>
                    replacement;
                RawControlSnapshot replacement_snapshot;
                std::uint64_t replacement_generation = 0U;
                const int open_error =
                    OpenStableControl(
                        &replacement,
                        &replacement_snapshot,
                        &replacement_generation);
                if (open_error != 0) {
                    return open_error;
                }
                control = std::move(replacement);
                *snapshot = replacement_snapshot;
                *generation = replacement_generation;
                return 0;
            }
            const int flags_error =
                ValidateControlDescriptor();
            if (flags_error != 0) {
                return flags_error;
            }
            RawControlSnapshot current;
            std::uint64_t current_generation = 0U;
            if (!control->Read(
                    &current, &current_generation)) {
                if (!control->PathStillNamesMapping()) {
                    continue;
                }
                return EAGAIN;
            }
            if (!control->PathStillNamesMapping()) {
                continue;
            }
            *snapshot = current;
            *generation = current_generation;
            return 0;
        }
        return EAGAIN;
    }

    [[nodiscard]] int EnsureSegment(
        std::uint32_t sequence,
        RetainedSegment** output) {
        if (output == nullptr || sequence == 0U ||
            sequence > limits.max_segments ||
            sequence > 99'999'999U) {
            return EINVAL;
        }
        const std::size_t index =
            static_cast<std::size_t>(sequence - 1U);
        if (segments.size() <= index) {
            segments.resize(index + 1U);
        }
        if (segments[index] != nullptr) {
            std::array<char, kSegmentFilenameBytes + 1U>
                name{};
            if (!FormatSegmentFilename(sequence, &name)) {
                return EINVAL;
            }
            struct stat status {};
            const int inspect_error = InspectNamedRegular(
                directory_fd,
                name.data(),
                segments[index]->descriptor,
                &status);
            if (inspect_error != 0) {
                return inspect_error;
            }
            const std::uint64_t size =
                static_cast<std::uint64_t>(status.st_size);
            if (size < kRawV1SegmentHeaderBytes ||
                size > limits.max_segment_bytes) {
                return EFBIG;
            }
            *output = segments[index].get();
            return 0;
        }

        std::array<char, kSegmentFilenameBytes + 1U> name{};
        if (!FormatSegmentFilename(sequence, &name)) {
            return EINVAL;
        }
        const int descriptor = OpenAtNoIntr(
            directory_fd,
            name.data(),
            O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME);
        if (descriptor < 0) {
            return errno;
        }
        struct stat before {};
        int inspect_error = InspectNamedRegular(
            directory_fd,
            name.data(),
            descriptor,
            &before);
        if (inspect_error != 0) {
            static_cast<void>(::close(descriptor));
            return inspect_error;
        }
        const std::uint64_t before_size =
            static_cast<std::uint64_t>(before.st_size);
        if (before_size < kRawV1SegmentHeaderBytes ||
            before_size > limits.max_segment_bytes) {
            static_cast<void>(::close(descriptor));
            return EFBIG;
        }

        RawV1SegmentHeaderWire wire{};
        const int read_error = ReadExact(
            descriptor, 0U, wire);
        if (read_error != 0) {
            static_cast<void>(::close(descriptor));
            return read_error;
        }
        struct stat after {};
        inspect_error = InspectNamedRegular(
            directory_fd,
            name.data(),
            descriptor,
            &after);
        if (inspect_error != 0) {
            static_cast<void>(::close(descriptor));
            return inspect_error;
        }
        const std::uint64_t after_size =
            static_cast<std::uint64_t>(after.st_size);
        if (after_size < kRawV1SegmentHeaderBytes ||
            after_size > limits.max_segment_bytes) {
            static_cast<void>(::close(descriptor));
            return EFBIG;
        }
        SegmentHeaderV1 header;
        const RawV1Error decode_error =
            DecodeSegmentHeaderV1(wire, &header);
        if (decode_error != RawV1Error::kNone ||
            header.source_stream_id != source_stream_id ||
            header.capture_date != capture_date ||
            header.stream_day_id != stream_day_id ||
            header.raw_schema_sha256 !=
                journal_header.raw_schema_sha256 ||
            header.segment_sequence != sequence) {
            static_cast<void>(::close(descriptor));
            return EILSEQ;
        }

        auto retained = std::make_unique<RetainedSegment>();
        retained->sequence = sequence;
        retained->descriptor = descriptor;
        retained->header = header;
        RetainedSegment* const result = retained.get();
        segments[index] = std::move(retained);
        ++retained_segment_count_value;
        *output = result;
        return 0;
    }

    [[nodiscard]] int CurrentSegmentStatus(
        RetainedSegment& segment,
        struct stat* status) noexcept {
        std::array<char, kSegmentFilenameBytes + 1U> name{};
        if (status == nullptr ||
            !FormatSegmentFilename(segment.sequence, &name)) {
            return EINVAL;
        }
        const int inspect_error = InspectNamedRegular(
            directory_fd,
            name.data(),
            segment.descriptor,
            status);
        if (inspect_error != 0) {
            return inspect_error;
        }
        const std::uint64_t size =
            static_cast<std::uint64_t>(status->st_size);
        if (size < kRawV1SegmentHeaderBytes ||
            size > limits.max_segment_bytes) {
            return EFBIG;
        }
        return 0;
    }

    [[nodiscard]] int ValidateMarker(
        const DurableMarkerV1& marker,
        const RawV1DurableMarkerWire& wire,
        RetainedSegment** marker_segment) {
        if (marker.source_stream_id != source_stream_id ||
            marker.segment_sequence == 0U ||
            marker.segment_sequence > limits.max_segments ||
            marker.durable_segment_offset <
                kRawV1SegmentHeaderBytes ||
            marker.durable_segment_offset %
                    kRawV1RecordAlignment !=
                0U) {
            return EILSEQ;
        }
        RetainedSegment* segment = nullptr;
        const int segment_error =
            EnsureSegment(
                marker.segment_sequence, &segment);
        if (segment_error != 0) {
            return segment_error;
        }
        struct stat segment_status {};
        const int status_error =
            CurrentSegmentStatus(
                *segment, &segment_status);
        if (status_error != 0) {
            return status_error;
        }
        const std::uint64_t segment_size =
            static_cast<std::uint64_t>(
                segment_status.st_size);
        std::uint64_t expected_global = 0U;
        if (marker.durable_segment_offset > segment_size ||
            !CheckedAdd(
                segment->header.segment_base_wal_pos,
                marker.durable_segment_offset,
                &expected_global) ||
            expected_global !=
                marker.durable_global_wal_pos) {
            return EILSEQ;
        }
        if ((marker.marker_flags &
             kRawV1SegmentSealed) != 0U &&
            segment_size !=
                marker.durable_segment_offset) {
            return EILSEQ;
        }

        if (!has_last_marker) {
            if (marker.segment_sequence != 1U ||
                segment->header.segment_base_wal_pos != 0U ||
                segment->header.first_ingress_sequence != 1U ||
                marker.durable_segment_offset !=
                    kRawV1SegmentHeaderBytes ||
                marker.durable_global_wal_pos !=
                    kRawV1SegmentHeaderBytes ||
                marker.durable_ingress_sequence != 0U ||
                marker.marker_flags != 0U) {
                return EILSEQ;
            }
        } else if (
            marker.segment_sequence ==
            last_marker.segment_sequence) {
            const bool same_cursor =
                marker.durable_segment_offset ==
                    last_marker.durable_segment_offset &&
                marker.durable_global_wal_pos ==
                    last_marker.durable_global_wal_pos &&
                marker.durable_ingress_sequence ==
                    last_marker.durable_ingress_sequence;
            if (same_cursor) {
                const bool identical =
                    MarkerWireEqual(
                        wire, last_marker_wire);
                const bool seal_transition =
                    last_marker.marker_flags == 0U &&
                    marker.marker_flags ==
                        kRawV1SegmentSealed;
                if (!identical && !seal_transition) {
                    return EILSEQ;
                }
            } else if (
                last_marker.marker_flags ==
                    kRawV1SegmentSealed ||
                marker.durable_segment_offset <=
                    last_marker.durable_segment_offset ||
                marker.durable_global_wal_pos <=
                    last_marker.durable_global_wal_pos ||
                marker.durable_ingress_sequence <
                    last_marker.durable_ingress_sequence) {
                return EILSEQ;
            }
        } else {
            if (last_marker.segment_sequence ==
                    std::numeric_limits<
                        std::uint32_t>::max() ||
                marker.segment_sequence !=
                    last_marker.segment_sequence + 1U ||
                last_marker.marker_flags !=
                    kRawV1SegmentSealed ||
                marker.durable_segment_offset !=
                    kRawV1SegmentHeaderBytes ||
                marker.durable_ingress_sequence !=
                    last_marker.durable_ingress_sequence ||
                marker.marker_flags != 0U ||
                last_marker.durable_ingress_sequence ==
                    std::numeric_limits<
                        std::uint64_t>::max()) {
                return EILSEQ;
            }
            const std::size_t previous_index =
                static_cast<std::size_t>(
                    last_marker.segment_sequence - 1U);
            if (previous_index >= segments.size() ||
                segments[previous_index] == nullptr) {
                return EILSEQ;
            }
            const SegmentHeaderV1& previous_header =
                segments[previous_index]->header;
            std::uint64_t expected_base = 0U;
            if (!CheckedAdd(
                    previous_header.segment_base_wal_pos,
                    last_marker.durable_segment_offset,
                    &expected_base) ||
                segment->header.segment_base_wal_pos !=
                    expected_base ||
                segment->header.first_ingress_sequence !=
                    last_marker.durable_ingress_sequence +
                        1U) {
                return EILSEQ;
            }
        }
        *marker_segment = segment;
        return 0;
    }

    [[nodiscard]] int RefreshJournalFacts() {
        struct stat before {};
        const int inspect_error = InspectNamedRegular(
            directory_fd,
            kRawJournalFilename,
            journal_fd,
            &before);
        if (inspect_error != 0) {
            return inspect_error;
        }
        const std::uint64_t size =
            static_cast<std::uint64_t>(before.st_size);
        std::uint64_t maximum_size = 0U;
        if (!CheckedMultiplyAdd(
                limits.max_journal_markers,
                kRawV1DurableMarkerBytes,
                kRawV1JournalHeaderBytes +
                    kRawV1DurableMarkerBytes - 1U,
                &maximum_size) ||
            size > maximum_size) {
            return EFBIG;
        }
        if (size < kRawV1JournalHeaderBytes ||
            size < maximum_observed_journal_size ||
            accepted_journal_size > size) {
            return ESTALE;
        }
        maximum_observed_journal_size =
            std::max(maximum_observed_journal_size, size);

        std::uint64_t next_end = 0U;
        while (CheckedAdd(
                   accepted_journal_size,
                   kRawV1DurableMarkerBytes,
                   &next_end) &&
               next_end <= size) {
            RawV1DurableMarkerWire wire{};
            const int read_error = ReadExact(
                journal_fd,
                accepted_journal_size,
                wire);
            if (read_error != 0) {
                return read_error;
            }
            DurableMarkerV1 marker;
            const RawV1Error decode_error =
                DecodeDurableMarkerV1(wire, &marker);
            if (decode_error != RawV1Error::kNone) {
                const bool exact_terminal_crc =
                    next_end == size &&
                    decode_error ==
                        RawV1Error::kHeaderCrcMismatch;
                if (exact_terminal_crc) {
                    break;
                }
                return EILSEQ;
            }
            RetainedSegment* marker_segment = nullptr;
            const int marker_error = ValidateMarker(
                marker, wire, &marker_segment);
            if (marker_error != 0 ||
                marker_segment == nullptr) {
                return marker_error == 0
                           ? EILSEQ
                           : marker_error;
            }
            const std::size_t fact_index =
                static_cast<std::size_t>(
                    marker.segment_sequence - 1U);
            if (journal_facts.size() <= fact_index) {
                journal_facts.resize(fact_index + 1U);
            }
            accepted_markers.push_back(marker);
            SegmentJournalFact& fact =
                journal_facts[fact_index];
            fact.has_marker = true;
            fact.sealed =
                marker.marker_flags ==
                kRawV1SegmentSealed;
            fact.latest = marker;
            last_marker = marker;
            last_marker_wire = wire;
            has_last_marker = true;
            accepted_journal_size = next_end;
        }

        struct stat after {};
        const int revalidate_error = InspectNamedRegular(
            directory_fd,
            kRawJournalFilename,
            journal_fd,
            &after);
        if (revalidate_error != 0) {
            return revalidate_error;
        }
        const std::uint64_t after_size =
            static_cast<std::uint64_t>(after.st_size);
        if (after_size < size ||
            after_size < maximum_observed_journal_size) {
            return ESTALE;
        }
        maximum_observed_journal_size =
            std::max(
                maximum_observed_journal_size,
                after_size);
        if (after_size > maximum_size) {
            return EFBIG;
        }
        return 0;
    }

    [[nodiscard]] bool ControlDurableMarkerExists(
        const RawControlSnapshot& snapshot) const noexcept {
        return std::any_of(
            accepted_markers.rbegin(),
            accepted_markers.rend(),
            [&snapshot](
                const DurableMarkerV1& marker) noexcept {
                return marker.segment_sequence ==
                           snapshot.segment_sequence &&
                       marker.durable_global_wal_pos ==
                           snapshot.durable_global_wal_pos &&
                       marker.durable_ingress_sequence ==
                           snapshot.durable_ingress_sequence &&
                       marker.durable_segment_offset ==
                           snapshot.durable_segment_offset;
            });
    }

    [[nodiscard]] bool RecoveryFrontierExists() const noexcept {
        return std::any_of(
            accepted_markers.begin(),
            accepted_markers.end(),
            [this](
                const DurableMarkerV1& marker) noexcept {
                return marker.segment_sequence ==
                           recovery_frontier.segment_sequence &&
                       marker.durable_global_wal_pos ==
                           recovery_frontier.global_wal_pos &&
                       marker.durable_ingress_sequence ==
                           recovery_frontier.ingress_sequence &&
                       marker.durable_segment_offset ==
                           recovery_frontier.segment_offset &&
                       marker.marker_flags ==
                           recovery_frontier.marker_flags;
            });
    }

    [[nodiscard]] bool ControlNotBeforeRecovery(
        const RawControlSnapshot& snapshot) const noexcept {
        return snapshot.segment_sequence >=
                   recovery_frontier.segment_sequence &&
               snapshot.durable_global_wal_pos >=
                   recovery_frontier.global_wal_pos &&
               snapshot.durable_ingress_sequence >=
                   recovery_frontier.ingress_sequence &&
               (snapshot.segment_sequence !=
                    recovery_frontier.segment_sequence ||
                snapshot.durable_segment_offset >=
                    recovery_frontier.segment_offset);
    }

    [[nodiscard]] bool ControlMonotonic(
        const RawControlSnapshot& snapshot,
        std::uint64_t generation) const noexcept {
        if (!has_last_control) {
            return true;
        }
        const bool skipped_segment =
            snapshot.segment_sequence >
                last_control.segment_sequence &&
            (last_control.segment_sequence ==
                 std::numeric_limits<std::uint32_t>::max() ||
             snapshot.segment_sequence !=
                 last_control.segment_sequence + 1U);
        if (generation < last_control_generation ||
            (generation == last_control_generation &&
             snapshot != last_control) ||
            snapshot.segment_sequence <
                last_control.segment_sequence ||
            skipped_segment ||
            snapshot.append_global_wal_pos <
                last_control.append_global_wal_pos ||
            snapshot.append_ingress_sequence <
                last_control.append_ingress_sequence ||
            snapshot.durable_global_wal_pos <
                last_control.durable_global_wal_pos ||
            snapshot.durable_ingress_sequence <
                last_control.durable_ingress_sequence ||
            snapshot.heartbeat_monotonic_ns <
                last_control.heartbeat_monotonic_ns ||
            snapshot.clock_epoch_label !=
                last_control.clock_epoch_label ||
            (last_control.fatal_state != 0U &&
             snapshot.fatal_state !=
                 last_control.fatal_state)) {
            return false;
        }
        return snapshot.segment_sequence !=
                   last_control.segment_sequence ||
               (snapshot.append_segment_offset >=
                    last_control.append_segment_offset &&
                snapshot.durable_segment_offset >=
                    last_control.durable_segment_offset);
    }

    [[nodiscard]] int ValidateAttachedControl(
        const RawControlSnapshot& snapshot) {
        if (snapshot.writer_instance != writer_instance ||
            snapshot.stream_day_id != stream_day_id ||
            snapshot.source_stream_id != source_stream_id ||
            snapshot.capture_date != capture_date ||
            snapshot.segment_sequence == 0U ||
            snapshot.segment_sequence > limits.max_segments ||
            snapshot.append_segment_offset <
                kRawV1SegmentHeaderBytes ||
            snapshot.durable_segment_offset <
                kRawV1SegmentHeaderBytes ||
            snapshot.durable_segment_offset >
                snapshot.append_segment_offset ||
            snapshot.durable_global_wal_pos >
                snapshot.append_global_wal_pos ||
            snapshot.durable_ingress_sequence >
                snapshot.append_ingress_sequence ||
            !ControlNotBeforeRecovery(snapshot)) {
            return EILSEQ;
        }
        const int refresh_error = RefreshJournalFacts();
        if (refresh_error != 0) {
            return refresh_error;
        }
        RetainedSegment* segment = nullptr;
        const int segment_error = EnsureSegment(
            snapshot.segment_sequence, &segment);
        if (segment_error != 0 || segment == nullptr) {
            return segment_error == 0
                       ? EILSEQ
                       : segment_error;
        }
        struct stat status {};
        const int status_error =
            CurrentSegmentStatus(*segment, &status);
        if (status_error != 0) {
            return status_error;
        }
        const std::uint64_t size =
            static_cast<std::uint64_t>(status.st_size);
        std::uint64_t expected_append_global = 0U;
        if (snapshot.append_segment_offset > size ||
            !CheckedAdd(
                segment->header.segment_base_wal_pos,
                snapshot.append_segment_offset,
                &expected_append_global) ||
            expected_append_global !=
                snapshot.append_global_wal_pos ||
            !ControlDurableMarkerExists(snapshot)) {
            return EILSEQ;
        }
        return 0;
    }

    // RawLiveTail::Next() and the service READY monitor both call this source.
    // The retained descriptor/cache/monotonic-control state is mutable, so
    // all public source operations serialize through this mutex.
    mutable std::mutex public_mutex;
    int directory_fd = -1;
    int journal_fd = -1;
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    RawLiveTailPosixLimitsV1 limits{};
    std::unique_ptr<RawControlFileReader> control;
    l2flow::common::Identity128 writer_instance{};
    l2flow::common::Identity128 stream_day_id{};
    RawRecoveryCursorV1 recovery_frontier{};
    RawControlSnapshot last_control{};
    std::uint64_t last_control_generation = 0U;
    bool has_last_control = false;
    bool identity_fenced = false;
    DurableJournalHeaderV1 journal_header{};
    std::vector<std::unique_ptr<RetainedSegment>> segments;
    std::size_t retained_segment_count_value = 0U;
    std::vector<SegmentJournalFact> journal_facts;
    std::vector<DurableMarkerV1> accepted_markers;
    DurableMarkerV1 last_marker{};
    RawV1DurableMarkerWire last_marker_wire{};
    bool has_last_marker = false;
    std::uint64_t accepted_journal_size =
        kRawV1JournalHeaderBytes;
    std::uint64_t maximum_observed_journal_size =
        kRawV1JournalHeaderBytes;
};

RawLiveTailPosixSource::RawLiveTailPosixSource(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

RawLiveTailPosixSource::~RawLiveTailPosixSource() = default;

int RawLiveTailPosixSource::ReadControl(
    RawControlSnapshot* snapshot,
    std::uint64_t* generation) noexcept {
    if (impl_ == nullptr || snapshot == nullptr ||
        generation == nullptr) {
        return EINVAL;
    }
    std::lock_guard<std::mutex> lock(impl_->public_mutex);
    if (impl_->identity_fenced) {
        return ESTALE;
    }
    try {
        RawControlSnapshot current;
        std::uint64_t current_generation = 0U;
        const int read_error =
            impl_->ReadStableControl(
                &current, &current_generation);
        if (read_error != 0) {
            return read_error;
        }

        if (current.writer_instance !=
                impl_->writer_instance ||
            current.stream_day_id != impl_->stream_day_id) {
            impl_->identity_fenced = true;
            return ESTALE;
        }
        const int validation_error =
            impl_->ValidateAttachedControl(current);
        if (validation_error != 0) {
            return validation_error;
        }
        if (!impl_->ControlMonotonic(
                current, current_generation)) {
            return ESTALE;
        }
        impl_->last_control = current;
        impl_->last_control_generation =
            current_generation;
        impl_->has_last_control = true;
        *snapshot = current;
        *generation = current_generation;
        return 0;
    } catch (const std::bad_alloc&) {
        return ENOMEM;
    } catch (...) {
        return EIO;
    }
}

int RawLiveTailPosixSource::InspectSegment(
    std::uint32_t segment_sequence,
    RawLiveSegmentInfo* info) noexcept {
    if (impl_ == nullptr || info == nullptr) {
        return EINVAL;
    }
    std::lock_guard<std::mutex> lock(impl_->public_mutex);
    if (impl_->identity_fenced) {
        return ESTALE;
    }
    try {
        const int refresh_error =
            impl_->RefreshJournalFacts();
        if (refresh_error != 0) {
            return refresh_error;
        }
        Impl::RetainedSegment* segment = nullptr;
        const int segment_error =
            impl_->EnsureSegment(
                segment_sequence, &segment);
        if (segment_error != 0 || segment == nullptr) {
            return segment_error == 0
                       ? EILSEQ
                       : segment_error;
        }
        const std::size_t fact_index =
            static_cast<std::size_t>(
                segment_sequence - 1U);
        if (fact_index >= impl_->journal_facts.size() ||
            !impl_->journal_facts[fact_index].has_marker) {
            return ENODATA;
        }
        struct stat status {};
        const int status_error =
            impl_->CurrentSegmentStatus(*segment, &status);
        if (status_error != 0) {
            return status_error;
        }
        const Impl::SegmentJournalFact& fact =
            impl_->journal_facts[fact_index];
        const std::uint64_t file_size =
            static_cast<std::uint64_t>(status.st_size);
        if (fact.sealed &&
            file_size !=
                fact.latest.durable_segment_offset) {
            return EILSEQ;
        }
        info->header = segment->header;
        info->visible_end_offset =
            fact.sealed
                ? fact.latest.durable_segment_offset
                : file_size;
        info->sealed = fact.sealed;
        return 0;
    } catch (const std::bad_alloc&) {
        return ENOMEM;
    } catch (...) {
        return EIO;
    }
}

RawLiveReadResult RawLiveTailPosixSource::ReadSegmentSome(
    std::uint32_t segment_sequence,
    std::uint64_t offset,
    std::span<std::byte> output) noexcept {
    if (impl_ == nullptr) {
        return {0U, EINVAL};
    }
    std::lock_guard<std::mutex> lock(impl_->public_mutex);
    if (impl_->identity_fenced) {
        return {0U, ESTALE};
    }
    try {
        Impl::RetainedSegment* segment = nullptr;
        const int segment_error =
            impl_->EnsureSegment(
                segment_sequence, &segment);
        if (segment_error != 0 || segment == nullptr) {
            return {
                0U,
                segment_error == 0
                    ? EILSEQ
                    : segment_error};
        }
        struct stat before {};
        const int status_error =
            impl_->CurrentSegmentStatus(*segment, &before);
        if (status_error != 0) {
            return {0U, status_error};
        }
        std::uint64_t visible_end =
            static_cast<std::uint64_t>(before.st_size);
        const std::size_t fact_index =
            static_cast<std::size_t>(
                segment_sequence - 1U);
        bool sealed = false;
        if (fact_index < impl_->journal_facts.size() &&
            impl_->journal_facts[fact_index].has_marker &&
            impl_->journal_facts[fact_index].sealed) {
            sealed = true;
            visible_end =
                impl_->journal_facts[fact_index]
                    .latest.durable_segment_offset;
            if (static_cast<std::uint64_t>(
                    before.st_size) != visible_end) {
                return {0U, EILSEQ};
            }
        }
        if (output.empty() || offset >= visible_end) {
            return {0U, 0};
        }
        if (offset >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
            return {0U, EOVERFLOW};
        }
        const std::uint64_t available =
            visible_end - offset;
        const std::size_t request = std::min({
            output.size(),
            static_cast<std::size_t>(std::min(
                available,
                static_cast<std::uint64_t>(
                    std::numeric_limits<
                        std::size_t>::max()))),
            static_cast<std::size_t>(
                std::numeric_limits<ssize_t>::max())});
        const ssize_t result = ::pread(
            segment->descriptor,
            output.data(),
            request,
            static_cast<off_t>(offset));
        if (result < 0) {
            return {0U, errno};
        }

        struct stat after {};
        const int revalidate_error =
            impl_->CurrentSegmentStatus(*segment, &after);
        if (revalidate_error != 0) {
            return {0U, revalidate_error};
        }
        if (sealed &&
            static_cast<std::uint64_t>(after.st_size) !=
                visible_end) {
            return {0U, ESTALE};
        }
        return {
            static_cast<std::size_t>(result),
            0};
    } catch (const std::bad_alloc&) {
        return {0U, ENOMEM};
    } catch (...) {
        return {0U, EIO};
    }
}

int RawLiveTailPosixSource::directory_open_flags()
    const noexcept {
    if (impl_ == nullptr) {
        return -1;
    }
    std::lock_guard<std::mutex> lock(impl_->public_mutex);
    return impl_->directory_fd < 0
               ? -1
               : ::fcntl(impl_->directory_fd, F_GETFL);
}

int RawLiveTailPosixSource::control_open_flags()
    const noexcept {
    if (impl_ == nullptr) {
        return -1;
    }
    std::lock_guard<std::mutex> lock(impl_->public_mutex);
    return impl_->control == nullptr
               ? -1
               : ::fcntl(
                     impl_->control->descriptor(), F_GETFL);
}

int RawLiveTailPosixSource::journal_open_flags()
    const noexcept {
    if (impl_ == nullptr) {
        return -1;
    }
    std::lock_guard<std::mutex> lock(impl_->public_mutex);
    return impl_->journal_fd < 0
               ? -1
               : ::fcntl(impl_->journal_fd, F_GETFL);
}

int RawLiveTailPosixSource::segment_open_flags(
    std::uint32_t segment_sequence) const noexcept {
    if (impl_ == nullptr || segment_sequence == 0U) {
        return -1;
    }
    std::lock_guard<std::mutex> lock(impl_->public_mutex);
    const std::size_t index =
        static_cast<std::size_t>(
            segment_sequence - 1U);
    return index >= impl_->segments.size() ||
                   impl_->segments[index] == nullptr
               ? -1
               : ::fcntl(
                     impl_->segments[index]->descriptor,
                     F_GETFL);
}

std::size_t RawLiveTailPosixSource::retained_segment_count()
    const noexcept {
    if (impl_ == nullptr) {
        return 0U;
    }
    std::lock_guard<std::mutex> lock(impl_->public_mutex);
    return impl_->retained_segment_count_value;
}

std::unique_ptr<RawLiveTailPosixSource>
OpenRawLiveTailPosixSource(
    int stream_directory_fd,
    std::uint32_t expected_source_stream_id,
    std::uint32_t expected_capture_date,
    const RawLiveTailPosixAttachGateV1& attach_gate,
    RawLiveTailPosixLimitsV1 limits,
    std::string* error) noexcept {
    SetError(error, {});
    if (stream_directory_fd < 0 ||
        expected_source_stream_id == 0U ||
        expected_capture_date == 0U ||
        l2flow::common::IsZeroIdentity(
            attach_gate.writer_instance) ||
        l2flow::common::IsZeroIdentity(
            attach_gate.stream_day_id) ||
        attach_gate.recovered_durable.segment_sequence == 0U ||
        attach_gate.recovered_durable.segment_offset <
            kRawV1SegmentHeaderBytes ||
        attach_gate.recovered_durable.global_wal_pos <
            kRawV1SegmentHeaderBytes ||
        limits.max_segments == 0U ||
        limits.max_segments >
            kRawLiveTailPosixAbsoluteMaxSegments ||
        limits.max_segment_bytes <
            kRawV1SegmentHeaderBytes ||
        limits.max_journal_markers == 0U ||
        limits.max_control_reattach_attempts == 0U) {
        SetError(
            error,
            "invalid Raw live-tail POSIX attach arguments");
        return nullptr;
    }

    try {
        const int directory_fd =
            RetainPrivateDirectory(stream_directory_fd);
        if (directory_fd < 0) {
            SetError(
                error,
                "cannot retain a private no-atime Raw stream directory");
            return nullptr;
        }
        auto impl = std::make_unique<
            RawLiveTailPosixSource::Impl>();
        impl->directory_fd = directory_fd;
        impl->source_stream_id =
            expected_source_stream_id;
        impl->capture_date = expected_capture_date;
        impl->limits = limits;
        impl->writer_instance =
            attach_gate.writer_instance;
        impl->stream_day_id =
            attach_gate.stream_day_id;
        impl->recovery_frontier =
            attach_gate.recovered_durable;

        RawControlSnapshot initial_control;
        std::uint64_t initial_generation = 0U;
        std::unique_ptr<RawControlFileReader>
            control_reader;
        const int control_error =
            impl->OpenStableControl(
                &control_reader,
                &initial_control,
                &initial_generation);
        static_cast<void>(initial_generation);
        if (control_error != 0 ||
            initial_control.writer_instance !=
                attach_gate.writer_instance ||
            initial_control.stream_day_id !=
                attach_gate.stream_day_id) {
            SetError(
                error,
                "cannot securely attach a coherent Raw control page");
            return nullptr;
        }
        impl->control = std::move(control_reader);

        impl->journal_fd = OpenAtNoIntr(
            impl->directory_fd,
            kRawJournalFilename,
            O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME);
        if (impl->journal_fd < 0) {
            SetError(
                error,
                "cannot open Raw durability journal read-only");
            return nullptr;
        }
        struct stat journal_status {};
        const int journal_inspect_error =
            InspectNamedRegular(
                impl->directory_fd,
                kRawJournalFilename,
                impl->journal_fd,
                &journal_status);
        if (journal_inspect_error != 0 ||
            journal_status.st_size <
                static_cast<off_t>(
                    kRawV1JournalHeaderBytes)) {
            SetError(
                error,
                "Raw durability journal metadata is invalid");
            return nullptr;
        }
        RawV1JournalHeaderWire journal_wire{};
        const int journal_read_error =
            ReadExact(
                impl->journal_fd, 0U, journal_wire);
        if (journal_read_error != 0 ||
            DecodeDurableJournalHeaderV1(
                journal_wire,
                &impl->journal_header) !=
                RawV1Error::kNone ||
            impl->journal_header.source_stream_id !=
                expected_source_stream_id ||
            impl->journal_header.capture_date !=
                expected_capture_date ||
            impl->journal_header.stream_day_id !=
                impl->stream_day_id) {
            SetError(
                error,
                "Raw durability journal header identity is invalid");
            return nullptr;
        }
        struct stat journal_after {};
        if (InspectNamedRegular(
                impl->directory_fd,
                kRawJournalFilename,
                impl->journal_fd,
                &journal_after) != 0 ||
            journal_after.st_size <
                static_cast<off_t>(
                    kRawV1JournalHeaderBytes)) {
            SetError(
                error,
                "Raw durability journal changed during attach");
            return nullptr;
        }
        impl->maximum_observed_journal_size =
            static_cast<std::uint64_t>(
                journal_after.st_size);

        const int control_validation_error =
            impl->ValidateAttachedControl(initial_control);
        if (control_validation_error != 0 ||
            !impl->has_last_marker ||
            !impl->RecoveryFrontierExists()) {
            SetError(
                error,
                "Raw control cursor is not backed by the validated journal chain");
            return nullptr;
        }
        impl->last_control = initial_control;
        impl->last_control_generation =
            initial_generation;
        impl->has_last_control = true;
        return std::unique_ptr<RawLiveTailPosixSource>(
            new RawLiveTailPosixSource(
                std::move(impl)));
    } catch (const std::bad_alloc&) {
        SetError(
            error,
            "cannot allocate Raw live-tail POSIX state");
        return nullptr;
    } catch (...) {
        SetError(
            error,
            "unexpected Raw live-tail POSIX attach failure");
        return nullptr;
    }
}

}  // namespace l2flow::ingress
