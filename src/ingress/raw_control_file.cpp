#include "l2flow/ingress/raw_control_file.h"

#include "l2flow/common/identity128.h"
#include "l2flow/ingress/raw_v1.h"

#include <cerrno>
#include <cstring>
#include <new>
#include <string>
#include <utility>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace l2flow::ingress {
namespace {

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

bool SyncLoop(int fd) noexcept {
    for (;;) {
        if (::fsync(fd) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

bool CloseLoop(int fd) noexcept {
    if (fd < 0) {
        return true;
    }
    for (;;) {
        if (::close(fd) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
        // Linux closes the descriptor even when close reports EINTR, so do
        // not retry an indeterminate descriptor number.
        return false;
    }
}

int DuplicateDirectory(int fd) noexcept {
    int duplicate = -1;
    do {
        duplicate = ::fcntl(fd, F_DUPFD_CLOEXEC, 0);
    } while (duplicate < 0 && errno == EINTR);
    return duplicate;
}

bool ValidatePrivateRegular(
    int fd,
    bool require_read_only) noexcept {
    struct stat status {};
    const int flags = ::fcntl(fd, F_GETFL);
    if (::fstat(fd, &status) != 0 ||
        flags < 0 ||
        !S_ISREG(status.st_mode) ||
        status.st_uid != ::geteuid() ||
        (status.st_mode & 07777) != 0600 ||
        status.st_nlink != 1 ||
        status.st_size !=
            static_cast<off_t>(kRawControlPageBytes) ||
        (flags & O_APPEND) != 0) {
        return false;
    }
    const int access = flags & O_ACCMODE;
    return require_read_only
               ? access == O_RDONLY
               : access == O_RDWR;
}

bool SameNamedInode(
    int directory_fd,
    int file_fd) noexcept {
    struct stat named {};
    struct stat opened {};
    return ::fstatat(
               directory_fd,
               kRawControlFilename,
               &named,
               AT_SYMLINK_NOFOLLOW) == 0 &&
           ::fstat(file_fd, &opened) == 0 &&
           S_ISREG(named.st_mode) &&
           named.st_dev == opened.st_dev &&
           named.st_ino == opened.st_ino;
}

void CleanupTemporary(
    int directory_fd,
    const std::string& temporary_name) noexcept {
    if (::unlinkat(
            directory_fd,
            temporary_name.c_str(),
            0) == 0) {
        static_cast<void>(SyncLoop(directory_fd));
    }
}

}  // namespace

RawControlFileWriter::RawControlFileWriter(
    int file_fd,
    void* mapping,
    RawControlPageV1* page,
    l2flow::common::Identity128 writer_instance,
    l2flow::common::Identity128 stream_day_id,
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    RawControlSnapshot initial_snapshot) noexcept
    : file_fd_(file_fd),
      mapping_(mapping),
      page_(page),
      writer_instance_(writer_instance),
      stream_day_id_(stream_day_id),
      source_stream_id_(source_stream_id),
      capture_date_(capture_date),
      last_snapshot_(std::move(initial_snapshot)) {}

RawControlFileWriter::~RawControlFileWriter() {
    if (mapping_ != nullptr &&
        mapping_ != MAP_FAILED) {
        static_cast<void>(
            ::munmap(mapping_, kRawControlPageBytes));
    }
    static_cast<void>(CloseLoop(file_fd_));
}

bool RawControlFileWriter::Publish(
    const RawControlSnapshot& snapshot) noexcept {
    if (page_ == nullptr ||
        snapshot.writer_instance != writer_instance_ ||
        snapshot.stream_day_id != stream_day_id_ ||
        snapshot.source_stream_id != source_stream_id_ ||
        snapshot.capture_date != capture_date_ ||
        snapshot.segment_sequence == 0U ||
        snapshot.append_segment_offset <
            kRawV1SegmentHeaderBytes ||
        snapshot.durable_segment_offset <
            kRawV1SegmentHeaderBytes ||
        snapshot.durable_global_wal_pos >
            snapshot.append_global_wal_pos ||
        snapshot.durable_ingress_sequence >
            snapshot.append_ingress_sequence ||
        snapshot.append_global_wal_pos <
            last_snapshot_.append_global_wal_pos ||
        snapshot.append_ingress_sequence <
            last_snapshot_.append_ingress_sequence ||
        snapshot.durable_global_wal_pos <
            last_snapshot_.durable_global_wal_pos ||
        snapshot.durable_ingress_sequence <
            last_snapshot_.durable_ingress_sequence ||
        snapshot.heartbeat_monotonic_ns <
            last_snapshot_.heartbeat_monotonic_ns ||
        snapshot.clock_epoch_label !=
            last_snapshot_.clock_epoch_label ||
        (last_snapshot_.fatal_state != 0U &&
         snapshot.fatal_state !=
             last_snapshot_.fatal_state)) {
        return false;
    }

    const bool same_segment =
        snapshot.segment_sequence ==
        last_snapshot_.segment_sequence;
    const bool next_segment =
        last_snapshot_.segment_sequence !=
            UINT32_MAX &&
        snapshot.segment_sequence ==
            last_snapshot_.segment_sequence + 1U;
    if ((!same_segment && !next_segment) ||
        (same_segment &&
         (snapshot.append_segment_offset <
              last_snapshot_.append_segment_offset ||
          snapshot.durable_segment_offset <
              last_snapshot_.durable_segment_offset))) {
        return false;
    }

    if (!RawControlPageWriter(*page_).Publish(snapshot)) {
        return false;
    }
    last_snapshot_ = snapshot;
    return true;
}

std::unique_ptr<RawControlFileWriter>
CreateRawControlFile(
    const RawWriterLease& lease,
    const RawControlSnapshot& initial,
    std::string* error) noexcept {
    SetError(error, {});
    if (lease.directory_descriptor() < 0 ||
        initial.source_stream_id !=
            lease.source_stream_id() ||
        initial.capture_date != lease.capture_date() ||
        l2flow::common::IsZeroIdentity(
            initial.writer_instance) ||
        l2flow::common::IsZeroIdentity(
            initial.stream_day_id) ||
        initial.segment_sequence == 0U ||
        initial.append_segment_offset <
            kRawV1SegmentHeaderBytes ||
        initial.durable_segment_offset <
            kRawV1SegmentHeaderBytes ||
        initial.durable_global_wal_pos >
            initial.append_global_wal_pos ||
        initial.durable_ingress_sequence >
            initial.append_ingress_sequence) {
        SetError(error, "invalid Raw control publication identity");
        return nullptr;
    }

    try {
        const std::string temporary_name =
            ".control.page." +
            l2flow::common::Identity128Hex(
                initial.writer_instance) +
            ".raw-control.tmp";
        int file_fd = -1;
        do {
            file_fd = ::openat(
                lease.directory_descriptor(),
                temporary_name.c_str(),
                O_RDWR | O_CREAT | O_EXCL |
                    O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC,
                0600);
        } while (file_fd < 0 && errno == EINTR);
        if (file_fd < 0) {
            SetError(
                error,
                std::string(
                    "cannot create Raw control temporary: ") +
                    std::strerror(errno));
            return nullptr;
        }

        bool published = false;
        void* mapping = MAP_FAILED;
        do {
            if (::ftruncate(
                    file_fd,
                    static_cast<off_t>(
                        kRawControlPageBytes)) != 0 ||
                !ValidatePrivateRegular(file_fd, false)) {
                break;
            }
            mapping = ::mmap(
                nullptr,
                kRawControlPageBytes,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                file_fd,
                0);
            if (mapping == MAP_FAILED) {
                break;
            }
            RawControlPageV1* page = nullptr;
            if (!InitializeRawControlPage(
                    mapping,
                    kRawControlPageBytes,
                    &page) ||
                !RawControlPageWriter(*page).Publish(initial) ||
                ::msync(
                    mapping,
                    kRawControlPageBytes,
                    MS_SYNC) != 0 ||
                !SyncLoop(file_fd) ||
                ::renameat(
                    lease.directory_descriptor(),
                    temporary_name.c_str(),
                    lease.directory_descriptor(),
                    kRawControlFilename) != 0 ||
                !SyncLoop(lease.directory_descriptor()) ||
                !SameNamedInode(
                    lease.directory_descriptor(),
                    file_fd)) {
                break;
            }
            published = true;
            try {
                return std::unique_ptr<RawControlFileWriter>(
                    new RawControlFileWriter(
                        file_fd,
                        mapping,
                        page,
                        initial.writer_instance,
                        initial.stream_day_id,
                        initial.source_stream_id,
                        initial.capture_date,
                        initial));
            } catch (...) {
                errno = ENOMEM;
                break;
            }
        } while (false);

        const int saved_error =
            errno == 0 ? EIO : errno;
        if (mapping != MAP_FAILED) {
            static_cast<void>(
                ::munmap(mapping, kRawControlPageBytes));
        }
        static_cast<void>(CloseLoop(file_fd));
        if (!published) {
            CleanupTemporary(
                lease.directory_descriptor(),
                temporary_name);
        }
        SetError(
            error,
            std::string("cannot publish Raw control page: ") +
                std::strerror(saved_error));
        return nullptr;
    } catch (...) {
        SetError(error, "cannot allocate Raw control publication");
        return nullptr;
    }
}

RawControlFileReader::RawControlFileReader(
    int directory_fd,
    int file_fd,
    void* mapping) noexcept
    : directory_fd_(directory_fd),
      file_fd_(file_fd),
      mapping_(mapping) {}

RawControlFileReader::~RawControlFileReader() {
    if (mapping_ != nullptr &&
        mapping_ != MAP_FAILED) {
        static_cast<void>(
            ::munmap(mapping_, kRawControlPageBytes));
    }
    static_cast<void>(CloseLoop(file_fd_));
    static_cast<void>(CloseLoop(directory_fd_));
}

bool RawControlFileReader::Read(
    RawControlSnapshot* snapshot,
    std::uint64_t* generation) const noexcept {
    return mapping_ != nullptr &&
           mapping_ != MAP_FAILED &&
           ReadRawControlPage(
               *static_cast<const RawControlPageV1*>(
                   mapping_),
               snapshot,
               generation);
}

bool RawControlFileReader::PathStillNamesMapping() const noexcept {
    return directory_fd_ >= 0 &&
           file_fd_ >= 0 &&
           SameNamedInode(directory_fd_, file_fd_);
}

std::unique_ptr<RawControlFileReader>
OpenRawControlFile(
    int stream_directory_fd,
    std::uint32_t expected_source_stream_id,
    std::uint32_t expected_capture_date,
    std::string* error) noexcept {
    SetError(error, {});
    if (stream_directory_fd < 0 ||
        expected_source_stream_id == 0U ||
        expected_capture_date == 0U) {
        SetError(error, "invalid Raw control attach arguments");
        return nullptr;
    }

    int directory_fd =
        DuplicateDirectory(stream_directory_fd);
    if (directory_fd < 0) {
        SetError(error, "cannot retain Raw control directory");
        return nullptr;
    }
    int file_fd = -1;
    do {
        file_fd = ::openat(
            directory_fd,
            kRawControlFilename,
            O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME);
    } while (file_fd < 0 && errno == EINTR);
    if (file_fd < 0 ||
        !ValidatePrivateRegular(file_fd, true) ||
        !SameNamedInode(directory_fd, file_fd)) {
        const int saved_error =
            errno == 0 ? EINVAL : errno;
        static_cast<void>(CloseLoop(file_fd));
        static_cast<void>(CloseLoop(directory_fd));
        SetError(
            error,
            std::string("cannot securely attach Raw control page: ") +
                std::strerror(saved_error));
        return nullptr;
    }

    void* mapping = ::mmap(
        nullptr,
        kRawControlPageBytes,
        PROT_READ,
        MAP_SHARED,
        file_fd,
        0);
    if (mapping == MAP_FAILED) {
        const int saved_error = errno;
        static_cast<void>(CloseLoop(file_fd));
        static_cast<void>(CloseLoop(directory_fd));
        SetError(
            error,
            std::string("cannot map Raw control page: ") +
                std::strerror(saved_error));
        return nullptr;
    }

    RawControlSnapshot initial;
    const bool valid =
        ReadRawControlPage(
            *static_cast<const RawControlPageV1*>(
                mapping),
            &initial) &&
        initial.source_stream_id ==
            expected_source_stream_id &&
        initial.capture_date ==
            expected_capture_date &&
        !l2flow::common::IsZeroIdentity(
            initial.writer_instance) &&
        !l2flow::common::IsZeroIdentity(
            initial.stream_day_id);
    if (!valid) {
        static_cast<void>(
            ::munmap(mapping, kRawControlPageBytes));
        static_cast<void>(CloseLoop(file_fd));
        static_cast<void>(CloseLoop(directory_fd));
        SetError(error, "Raw control page identity is invalid");
        return nullptr;
    }

    try {
        return std::unique_ptr<RawControlFileReader>(
            new RawControlFileReader(
                directory_fd, file_fd, mapping));
    } catch (...) {
        static_cast<void>(
            ::munmap(mapping, kRawControlPageBytes));
        static_cast<void>(CloseLoop(file_fd));
        static_cast<void>(CloseLoop(directory_fd));
        SetError(error, "cannot allocate Raw control reader");
        return nullptr;
    }
}

}  // namespace l2flow::ingress
