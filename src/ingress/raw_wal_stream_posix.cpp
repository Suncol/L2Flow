#include "l2flow/ingress/raw_wal_stream_posix.h"

#include "l2flow/common/identity128.h"
#include "l2flow/ingress/raw_manifest_store.h"
#include "l2flow/ingress/raw_manifest_transition.h"
#include "l2flow/ingress/raw_namespace.h"
#include "l2flow/ingress/raw_posix_io.h"
#include "l2flow/ingress/raw_reserve_active_activation_receipt.h"
#include "l2flow/ingress/raw_segment_artifacts.h"
#include "l2flow/ingress/raw_v1.h"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <span>
#include <string>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>
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

[[nodiscard]] std::array<char, 21U>
SegmentFilename(
    std::uint32_t sequence) noexcept {
    std::array<char, 21U> result{
        's', 'e', 'g', 'm', 'e', 'n', 't', '-',
        '0', '0', '0', '0', '0', '0', '0', '0',
        '.', 'r', 'a', 'w', '\0'};
    for (std::size_t index = 16U;
         index > 8U;
         --index) {
        result[index - 1U] =
            static_cast<char>(
                '0' + sequence % 10U);
        sequence /= 10U;
    }
    return result;
}

[[nodiscard]] bool SameNamespace(
    const RawManifestNamespaceV1& left,
    const RawManifestNamespaceV1& right) noexcept {
    return left.capture_date == right.capture_date &&
           left.source_stream_id ==
               right.source_stream_id &&
           left.stream_day_id == right.stream_day_id;
}

[[nodiscard]] RawManifestNamespaceV1 NamespaceOf(
    const SegmentHeaderV1& segment) noexcept {
    return {
        segment.capture_date,
        segment.source_stream_id,
        segment.stream_day_id};
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

[[nodiscard]] bool SameMetadata(
    const RawSealedSegmentMetadataV1& left,
    const RawSealedSegmentMetadataV1& right) noexcept {
    return SameHeader(left.segment, right.segment) &&
           SameMarker(
               left.accepted_sealed_marker,
               right.accepted_sealed_marker) &&
           left.accepted_sealed_marker_bytes ==
               right.accepted_sealed_marker_bytes &&
           left.segment_sha256 == right.segment_sha256 &&
           left.index_sha256 == right.index_sha256 &&
           left.logical_end_offset ==
               right.logical_end_offset &&
           left.record_count == right.record_count &&
           left.actual_first_ingress_sequence ==
               right.actual_first_ingress_sequence &&
           left.actual_last_ingress_sequence ==
               right.actual_last_ingress_sequence;
}

[[nodiscard]] bool IsDefaultExistingJournal(
    const RawWalExistingJournalInit& value) noexcept {
    return value.journal_append_offset == 0U &&
           value.previous_segment_sequence == 0U &&
           value.previous_sealed_cursor ==
               RawWalCursor{} &&
           value.previous_marker_flags == 0U;
}

[[nodiscard]] bool PreadExact(
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
            errno = EOVERFLOW;
            return false;
        }
        const ssize_t result = ::pread(
            descriptor,
            output.data() +
                static_cast<std::ptrdiff_t>(completed),
            output.size() - completed,
            static_cast<off_t>(current_offset));
        if (result > 0) {
            completed +=
                static_cast<std::size_t>(result);
            continue;
        }
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result == 0) {
            errno = EIO;
        }
        return false;
    }
    return true;
}

void CloseDescriptor(int descriptor) noexcept {
    if (descriptor >= 0) {
        // On Linux the descriptor is consumed even if close reports EINTR.
        static_cast<void>(::close(descriptor));
    }
}

[[nodiscard]] bool InitialConfigValid(
    const RawWriterLease& lease,
    const RawWalWriterConfig& config,
    const RawPosixWalStreamBackendOptionsV1& options,
    SegmentHeaderV1* segment,
    DurableJournalHeaderV1* journal,
    std::uint64_t*
        expected_initialized_journal_size) noexcept {
    if (segment == nullptr || journal == nullptr ||
        expected_initialized_journal_size == nullptr ||
        lease.directory_descriptor() < 0 ||
        options.segment_preallocation_bytes <
            kRawV1SegmentHeaderBytes ||
        options.segment_preallocation_bytes >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max()) ||
        options.maximum_manifest_bytes == 0U ||
        l2flow::common::IsZeroIdentity(
            options.writer_instance) ||
        config.writer_instance !=
            options.writer_instance ||
        config.commit_observer != nullptr ||
        !config.headers_already_persisted ||
        DecodeSegmentHeaderV1(
            config.segment_header_wire,
            segment) != RawV1Error::kNone ||
        DecodeDurableJournalHeaderV1(
            config.journal_header_wire,
            journal) != RawV1Error::kNone ||
        segment->source_stream_id !=
            lease.source_stream_id() ||
        segment->capture_date != lease.capture_date() ||
        journal->source_stream_id !=
            lease.source_stream_id() ||
        journal->capture_date != lease.capture_date() ||
        segment->source_stream_id !=
            config.source_stream_id ||
        segment->capture_date != config.capture_date ||
        segment->segment_sequence !=
            config.segment_sequence ||
        segment->segment_base_wal_pos !=
            config.segment_base_wal_pos ||
        segment->first_ingress_sequence !=
            config.first_ingress_sequence ||
        segment->segment_flags != 0U ||
        segment->stream_day_id != journal->stream_day_id ||
        segment->raw_schema_sha256 !=
            journal->raw_schema_sha256 ||
        config.first_ingress_sequence == 0U ||
        config.initial_durable_ingress_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        config.first_ingress_sequence !=
            config.initial_durable_ingress_sequence + 1U) {
        return false;
    }

    RawV1SegmentHeaderWire canonical_segment{};
    RawV1JournalHeaderWire canonical_journal{};
    if (EncodeSegmentHeaderV1(
            *segment, &canonical_segment) !=
            RawV1Error::kNone ||
        EncodeDurableJournalHeaderV1(
            *journal, &canonical_journal) !=
            RawV1Error::kNone ||
        canonical_segment != config.segment_header_wire ||
        canonical_journal != config.journal_header_wire) {
        return false;
    }

    if (config.initialization_mode ==
        RawWalInitializationMode::kFreshJournal) {
        if (config.segment_sequence != 1U ||
            config.segment_base_wal_pos != 0U ||
            config.first_ingress_sequence != 1U ||
            config.initial_durable_ingress_sequence != 0U ||
            !IsDefaultExistingJournal(
                config.existing_journal)) {
            return false;
        }
        return CheckedAdd(
            kRawV1JournalHeaderBytes,
            kRawV1DurableMarkerBytes,
            expected_initialized_journal_size);
    }
    if (config.initialization_mode !=
        RawWalInitializationMode::kExistingJournal) {
        return false;
    }

    const RawWalExistingJournalInit& existing =
        config.existing_journal;
    if (config.segment_sequence <= 1U ||
        existing.previous_segment_sequence == 0U ||
        existing.previous_segment_sequence ==
            std::numeric_limits<std::uint32_t>::max() ||
        config.segment_sequence !=
            existing.previous_segment_sequence + 1U ||
        existing.previous_marker_flags !=
            kRawV1SegmentSealed ||
        existing.journal_append_offset <
            kRawV1JournalHeaderBytes +
                kRawV1DurableMarkerBytes ||
        (existing.journal_append_offset -
         kRawV1JournalHeaderBytes) %
                kRawV1DurableMarkerBytes !=
            0U ||
        config.segment_base_wal_pos !=
            existing.previous_sealed_cursor
                .global_wal_pos ||
        config.initial_durable_ingress_sequence !=
            existing.previous_sealed_cursor
                .ingress_sequence ||
        existing.previous_sealed_cursor
                .segment_offset <
            kRawV1SegmentHeaderBytes) {
        return false;
    }
    return CheckedAdd(
        existing.journal_append_offset,
        kRawV1DurableMarkerBytes,
        expected_initialized_journal_size);
}

[[nodiscard]] bool ExistingManifestMatchesInitialConfig(
    const RawManifestV1& manifest,
    const SegmentHeaderV1& segment,
    const RawWalExistingJournalInit& existing) noexcept {
    if (manifest.open_entry.has_value() ||
        manifest.closed_entries.empty() ||
        !SameNamespace(
            manifest.namespace_identity,
            NamespaceOf(segment))) {
        return false;
    }
    const RawManifestSegmentEntryV1& preceding =
        manifest.closed_entries.back();
    DurableMarkerV1 marker{};
    std::uint64_t expected_base = 0U;
    return preceding.state ==
               RawManifestSegmentStateV1::kClosed &&
           preceding.segment_sequence ==
               existing.previous_segment_sequence &&
           preceding.segment_sequence !=
               std::numeric_limits<std::uint32_t>::max() &&
           segment.segment_sequence ==
               preceding.segment_sequence + 1U &&
           CheckedAdd(
               preceding.segment_base_wal_pos,
               preceding.segment_logical_length,
               &expected_base) &&
           segment.segment_base_wal_pos == expected_base &&
           DecodeDurableMarkerV1(
               preceding.accepted_marker_bytes,
               &marker) == RawV1Error::kNone &&
           marker.marker_flags == kRawV1SegmentSealed &&
           marker.segment_sequence ==
               preceding.segment_sequence &&
           marker.durable_global_wal_pos ==
               existing.previous_sealed_cursor
                   .global_wal_pos &&
           marker.durable_ingress_sequence ==
               existing.previous_sealed_cursor
                   .ingress_sequence &&
           marker.durable_segment_offset ==
               existing.previous_sealed_cursor
                   .segment_offset &&
           marker.durable_ingress_sequence !=
               std::numeric_limits<std::uint64_t>::max() &&
           segment.first_ingress_sequence ==
               marker.durable_ingress_sequence + 1U;
}

[[nodiscard]] bool SameFileIdentityAndSize(
    const struct stat& left,
    const struct stat& right) noexcept {
    return left.st_dev == right.st_dev &&
           left.st_ino == right.st_ino &&
           left.st_size == right.st_size &&
           left.st_mtim.tv_sec == right.st_mtim.tv_sec &&
           left.st_mtim.tv_nsec == right.st_mtim.tv_nsec &&
           left.st_ctim.tv_sec == right.st_ctim.tv_sec &&
           left.st_ctim.tv_nsec == right.st_ctim.tv_nsec;
}

[[nodiscard]] int OpenStableRecoveryFile(
    int directory_descriptor,
    const char* name,
    std::uint64_t expected_size,
    struct stat* snapshot) noexcept {
    if (directory_descriptor < 0 || name == nullptr ||
        snapshot == nullptr ||
        expected_size >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
        errno = EINVAL;
        return -1;
    }
    int descriptor = -1;
    do {
        descriptor = ::openat(
            directory_descriptor,
            name,
            O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME);
    } while (descriptor < 0 && errno == EINTR);
    if (descriptor < 0) {
        return -1;
    }
    struct stat opened {};
    struct stat named {};
    const int status_flags =
        ::fcntl(descriptor, F_GETFL);
    const int descriptor_flags =
        ::fcntl(descriptor, F_GETFD);
    const bool valid =
        ::fstat(descriptor, &opened) == 0 &&
        ::fstatat(
            directory_descriptor,
            name,
            &named,
            AT_SYMLINK_NOFOLLOW) == 0 &&
        S_ISREG(opened.st_mode) &&
        S_ISREG(named.st_mode) &&
        opened.st_uid == ::geteuid() &&
        named.st_uid == ::geteuid() &&
        (opened.st_mode & 07777) == 0600 &&
        (named.st_mode & 07777) == 0600 &&
        opened.st_nlink == 1 &&
        named.st_nlink == 1 &&
        opened.st_size >= 0 &&
        static_cast<std::uint64_t>(opened.st_size) ==
            expected_size &&
        opened.st_dev == named.st_dev &&
        opened.st_ino == named.st_ino &&
        status_flags >= 0 &&
        (status_flags & O_ACCMODE) == O_RDONLY &&
        (status_flags & O_APPEND) == 0 &&
        (status_flags & O_NONBLOCK) != 0 &&
        descriptor_flags >= 0 &&
        (descriptor_flags & FD_CLOEXEC) != 0;
    if (!valid) {
        const int saved_errno =
            errno == 0 ? EILSEQ : errno;
        CloseDescriptor(descriptor);
        errno = saved_errno;
        return -1;
    }
    *snapshot = opened;
    return descriptor;
}

[[nodiscard]] bool RevalidateStableRecoveryFile(
    int directory_descriptor,
    const char* name,
    int descriptor,
    const struct stat& before) noexcept {
    struct stat opened {};
    struct stat named {};
    return ::fstat(descriptor, &opened) == 0 &&
           ::fstatat(
               directory_descriptor,
               name,
               &named,
               AT_SYMLINK_NOFOLLOW) == 0 &&
           SameFileIdentityAndSize(before, opened) &&
           opened.st_dev == named.st_dev &&
           opened.st_ino == named.st_ino &&
           opened.st_size == named.st_size;
}

[[nodiscard]] bool ClosedEntryMatchesTerminal(
    const RawManifestSegmentEntryV1& entry,
    const SegmentHeaderV1& segment,
    const RawWalCursor& accepted_cursor) noexcept {
    DurableMarkerV1 marker{};
    std::uint64_t expected_global = 0U;
    return entry.namespace_identity ==
               NamespaceOf(segment) &&
           entry.segment_sequence ==
               segment.segment_sequence &&
           entry.segment_flags ==
               segment.segment_flags &&
           entry.state ==
               RawManifestSegmentStateV1::kClosed &&
           entry.segment_base_wal_pos ==
               segment.segment_base_wal_pos &&
           entry.next_expected_first_ingress_sequence ==
               segment.first_ingress_sequence &&
           entry.host_uuid == segment.host_uuid &&
           entry.linux_boot_id ==
               segment.linux_boot_id &&
           entry.clock_epoch_algorithm ==
               segment.clock_epoch_algorithm &&
           entry.clock_epoch_digest ==
               segment.clock_epoch_digest &&
           entry.clock_epoch_label ==
               segment.clock_epoch_label &&
           entry.sdk_archive_sha256 ==
               segment.sdk_archive_sha256 &&
           entry.libmdl_api_sha256 ==
               segment.libmdl_api_sha256 &&
           entry.endpoint_contract_sha256 ==
               segment.endpoint_contract_sha256 &&
           entry.config_sha256 ==
               segment.config_sha256 &&
           entry.raw_schema_sha256 ==
               segment.raw_schema_sha256 &&
           entry.build_manifest_sha256 ==
               segment.build_manifest_sha256 &&
           entry.reserve_state_uuid ==
               segment.reserve_state_uuid &&
           entry.finalization_cycle_id ==
               segment.finalization_cycle_id &&
           entry.immutable_grant_sha256 ==
               segment.immutable_grant_sha256 &&
           !entry.maintenance_report_locator.has_value() &&
           !entry.archive_locator.has_value() &&
           entry.segment_logical_length >=
               kRawV1SegmentHeaderBytes &&
           CheckedAdd(
               entry.segment_base_wal_pos,
               entry.segment_logical_length,
               &expected_global) &&
           expected_global ==
               accepted_cursor.global_wal_pos &&
           entry.segment_logical_length ==
               accepted_cursor.segment_offset &&
           DecodeDurableMarkerV1(
               entry.accepted_marker_bytes,
               &marker) == RawV1Error::kNone &&
           marker.source_stream_id ==
               segment.source_stream_id &&
           marker.segment_sequence ==
               segment.segment_sequence &&
           marker.marker_flags ==
               kRawV1SegmentSealed &&
           marker.durable_global_wal_pos ==
               accepted_cursor.global_wal_pos &&
           marker.durable_ingress_sequence ==
               accepted_cursor.ingress_sequence &&
           marker.durable_segment_offset ==
               accepted_cursor.segment_offset;
}

[[nodiscard]] bool ValidateRecoveredClosedState(
    const RawWriterLease& lease,
    const RawRecoveredClosedWalStateV1& recovered,
    const RawPosixWalStreamBackendOptionsV1& options,
    const RawSegmentArtifactOptionsV1&
        artifact_options,
    const RawReserveRegistryEntryKeyV1& key,
    std::string* error) noexcept {
    const SegmentHeaderV1& segment =
        recovered.terminal_segment;
    RawV1SegmentHeaderWire segment_wire{};
    DurableJournalHeaderV1 journal{};
    RawV1JournalHeaderWire canonical_journal{};
    if (lease.directory_descriptor() < 0 ||
        options.segment_preallocation_bytes <
            kRawV1SegmentHeaderBytes ||
        options.segment_preallocation_bytes >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max()) ||
        options.maximum_manifest_bytes == 0U ||
        l2flow::common::IsZeroIdentity(
            options.writer_instance) ||
        key.route.source_stream_id !=
            lease.source_stream_id() ||
        key.route.capture_date !=
            lease.capture_date() ||
        key.stream_day_id != segment.stream_day_id ||
        segment.source_stream_id !=
            lease.source_stream_id() ||
        segment.capture_date !=
            lease.capture_date() ||
        segment.segment_flags != 0U ||
        artifact_options.expected_raw_schema_sha256 !=
            segment.raw_schema_sha256 ||
        EncodeSegmentHeaderV1(
            segment, &segment_wire) !=
            RawV1Error::kNone ||
        DecodeDurableJournalHeaderV1(
            recovered.journal_header_wire,
            &journal) != RawV1Error::kNone ||
        EncodeDurableJournalHeaderV1(
            journal, &canonical_journal) !=
            RawV1Error::kNone ||
        canonical_journal !=
            recovered.journal_header_wire ||
        journal.source_stream_id !=
            segment.source_stream_id ||
        journal.capture_date != segment.capture_date ||
        journal.stream_day_id !=
            segment.stream_day_id ||
        journal.raw_schema_sha256 !=
            segment.raw_schema_sha256 ||
        recovered.journal_logical_size <
            kRawV1JournalHeaderBytes +
                kRawV1DurableMarkerBytes ||
        (recovered.journal_logical_size -
         kRawV1JournalHeaderBytes) %
                kRawV1DurableMarkerBytes !=
            0U ||
        ValidateManifestModel(
            recovered.closed_manifest) !=
            RawManifestV1Error::kNone ||
        recovered.closed_manifest.open_entry
            .has_value() ||
        recovered.closed_manifest.closed_entries
            .empty() ||
        recovered.closed_manifest
                .namespace_identity !=
            NamespaceOf(segment) ||
        !ClosedEntryMatchesTerminal(
            recovered.closed_manifest
                .closed_entries.back(),
            segment,
            recovered.accepted_sealed_cursor)) {
        SetError(
            error,
            "invalid recovered closed Raw WAL state");
        return false;
    }

    std::string expected_manifest_bytes;
    if (EncodeRawManifestJcs(
            recovered.closed_manifest,
            &expected_manifest_bytes) !=
        RawManifestV1Error::kNone) {
        SetError(
            error,
            "cannot encode recovered closed Raw manifest");
        return false;
    }
    RawManifestV1 loaded_manifest{};
    std::string loaded_manifest_bytes;
    if (LoadCurrentRawManifestAt(
            lease.directory_descriptor(),
            NamespaceOf(segment),
            options.maximum_manifest_bytes,
            &loaded_manifest,
            &loaded_manifest_bytes,
            nullptr,
            error) != RawManifestStoreError::kNone ||
        loaded_manifest_bytes !=
            expected_manifest_bytes) {
        SetError(
            error,
            "published Raw manifest does not match recovered closed state");
        return false;
    }

    const std::array<char, 21U> segment_name =
        SegmentFilename(segment.segment_sequence);
    struct stat segment_snapshot {};
    const int segment_fd = OpenStableRecoveryFile(
        lease.directory_descriptor(),
        segment_name.data(),
        recovered.closed_manifest.closed_entries
            .back()
            .segment_logical_length,
        &segment_snapshot);
    if (segment_fd < 0) {
        SetError(
            error,
            "cannot securely verify recovered terminal Raw segment");
        return false;
    }
    RawV1SegmentHeaderWire disk_segment_wire{};
    const bool segment_matches =
        PreadExact(segment_fd, 0U, disk_segment_wire) &&
        disk_segment_wire == segment_wire &&
        RevalidateStableRecoveryFile(
            lease.directory_descriptor(),
            segment_name.data(),
            segment_fd,
            segment_snapshot);
    CloseDescriptor(segment_fd);
    if (!segment_matches) {
        SetError(
            error,
            "recovered terminal Raw segment changed or mismatched");
        return false;
    }

    struct stat journal_snapshot {};
    const int journal_fd = OpenStableRecoveryFile(
        lease.directory_descriptor(),
        kRawJournalFilename,
        recovered.journal_logical_size,
        &journal_snapshot);
    if (journal_fd < 0) {
        SetError(
            error,
            "cannot securely verify recovered Raw journal");
        return false;
    }
    RawV1JournalHeaderWire disk_journal_header{};
    RawV1DurableMarkerWire disk_terminal_marker{};
    const bool journal_matches =
        PreadExact(
            journal_fd, 0U, disk_journal_header) &&
        PreadExact(
            journal_fd,
            recovered.journal_logical_size -
                kRawV1DurableMarkerBytes,
            disk_terminal_marker) &&
        disk_journal_header ==
            recovered.journal_header_wire &&
        disk_terminal_marker ==
            recovered.closed_manifest
                .closed_entries.back()
                .accepted_marker_bytes &&
        RevalidateStableRecoveryFile(
            lease.directory_descriptor(),
            kRawJournalFilename,
            journal_fd,
            journal_snapshot);
    CloseDescriptor(journal_fd);
    if (!journal_matches) {
        SetError(
            error,
            "recovered Raw journal changed or mismatched");
        return false;
    }
    return true;
}

class BorrowedRawTargetDirectoryV1 final
    : public RawReserveMutationTargetProviderV1 {
public:
    explicit BorrowedRawTargetDirectoryV1(
        int directory_fd) noexcept
        : directory_fd_(directory_fd) {}

    [[nodiscard]] int
    RawReserveMutationTargetDirectoryDescriptorV1()
        const noexcept override {
        return directory_fd_;
    }

private:
    int directory_fd_ = -1;
};

class RawFreshActivePosixStreamOwnerV1 final
    : public RawActiveBoundWalSinkV1 {
public:
    RawFreshActivePosixStreamOwnerV1(
        std::unique_ptr<RawWriterLease> lease,
        std::unique_ptr<
            RawReserveAuthorizedWalStreamBackendV1>
            backend,
        std::unique_ptr<RawWalStreamWriter> stream)
        noexcept
        : lease_(std::move(lease)),
          backend_(std::move(backend)),
          stream_(std::move(stream)) {}

    ~RawFreshActivePosixStreamOwnerV1() override =
        default;

    [[nodiscard]] bool AppendRecord(
        const RawWalRecordInputV1& input) noexcept override {
        return stream_->AppendRecord(input);
    }
    [[nodiscard]] bool FlushDurable() noexcept override {
        return stream_->FlushDurable();
    }
    [[nodiscard]] bool SealAndClose() noexcept override {
        return stream_->SealAndClose();
    }
    [[nodiscard]] RawWalWriterSnapshot Snapshot()
        const noexcept override {
        return stream_->Snapshot();
    }
    [[nodiscard]] RawWalFailure failure()
        const noexcept override {
        return stream_->failure();
    }
    [[nodiscard]] RawWalSinkIdentityV1 identity()
        const noexcept override {
        return stream_->identity();
    }
    [[nodiscard]] int
    RawReserveMutationTargetDirectoryDescriptorV1()
        const noexcept override {
        return lease_->directory_descriptor();
    }
    [[nodiscard]] const RawReserveRegistryEntryKeyV1&
    authorization_key() const noexcept override {
        return backend_->authorization_key();
    }
    [[nodiscard]] const
        l2flow::common::Identity128&
    authorized_writer_instance()
        const noexcept override {
        return backend_->authorized_writer_instance();
    }
    [[nodiscard]] bool active_binding_validated()
        const noexcept override {
        return backend_->active_binding_validated();
    }
    [[nodiscard]] RawWriterLease&
    retained_writer_lease() noexcept override {
        return *lease_;
    }

    [[nodiscard]] bool PromoteToActive(
        std::unique_ptr<
            RawReserveActiveActivationReceiptV1>&&
            receipt,
        std::string* error) noexcept {
        return backend_->PromoteToActive(
            std::move(receipt), error);
    }
    [[nodiscard]] RawReserveAuthorizedWalFailureV1
    authorization_failure() const noexcept {
        return backend_->failure();
    }

private:
    // Destruction order is intentional: stream -> authorized/POSIX backend
    // -> lease, so every borrowed reference stays valid during teardown.
    std::unique_ptr<RawWriterLease> lease_;
    std::unique_ptr<
        RawReserveAuthorizedWalStreamBackendV1>
        backend_;
    std::unique_ptr<RawWalStreamWriter> stream_;
};

[[nodiscard]] bool IsDefaultRecoveredOpen(
    const RawWalRecoveredOpenInit& value) noexcept {
    return value.journal_append_offset == 0U &&
           value.recovered_cursor == RawWalCursor{} &&
           value.accepted_marker_flags == 0U;
}

[[nodiscard]] bool FreshFactoryPreflight(
    const RawWriterLease& lease,
    const RawFreshJournalAnchor& journal_anchor,
    const RawWalWriterConfig& logical_config,
    const RawPosixWalStreamBackendOptionsV1&
        backend_options,
    const RawSegmentArtifactOptionsV1&
        artifact_options,
    const RawWalStreamLimitsV1& stream_limits,
    const RawReserveRegistryEntryKeyV1& key,
    RawWalWriterConfig* persisted_config,
    SegmentHeaderV1* segment) noexcept {
    if (persisted_config == nullptr ||
        segment == nullptr ||
        logical_config.headers_already_persisted ||
        logical_config.initialization_mode !=
            RawWalInitializationMode::kFreshJournal ||
        !IsDefaultRecoveredOpen(
            logical_config.recovered_open) ||
        logical_config.commit_observer != nullptr ||
        key.route.source_stream_id == 0U ||
        key.route.capture_date == 0U ||
        key.route.source_stream_id !=
            lease.source_stream_id() ||
        key.route.capture_date !=
            lease.capture_date() ||
        journal_anchor.scaffolding_authorization()
                .source_stream_id !=
            key.route.source_stream_id ||
        journal_anchor.scaffolding_authorization()
                .capture_date !=
            key.route.capture_date ||
        journal_anchor.scaffolding_authorization()
                .stream_day_id !=
            key.stream_day_id ||
        journal_anchor.scaffolding_authorization()
                .recovery_attempt !=
            key.recovery_attempt_id ||
        journal_anchor.scaffolding_authorization()
                .registry_stage !=
            RawFreshRegistryStageV1::kScaffolding ||
        artifact_options.expected_raw_schema_sha256 ==
            RawV1Digest{} ||
        artifact_options.sample_record_interval == 0U ||
        artifact_options.sample_record_interval >
            kRawIndexV1DefaultRecordInterval ||
        artifact_options.sample_raw_bytes_interval ==
            0U ||
        artifact_options.sample_raw_bytes_interval >
            kRawIndexV1DefaultRawBytesInterval ||
        stream_limits.segment_max_age_ns == 0U ||
        stream_limits.maximum_record_bytes == 0U ||
        stream_limits.maximum_record_bytes >
            std::numeric_limits<std::uint32_t>::max() ||
        backend_options.segment_preallocation_bytes <
            stream_limits.segment_target_bytes ||
        artifact_options.maximum_segment_bytes <
            stream_limits.segment_target_bytes) {
        return false;
    }

    std::uint64_t minimum_segment_bytes = 0U;
    if (!CheckedAdd(
            kRawV1SegmentHeaderBytes,
            stream_limits.maximum_record_bytes,
            &minimum_segment_bytes) ||
        stream_limits.segment_target_bytes <
            minimum_segment_bytes) {
        return false;
    }

    *persisted_config = logical_config;
    persisted_config->headers_already_persisted = true;
    DurableJournalHeaderV1 journal{};
    std::uint64_t expected_journal_size = 0U;
    if (!InitialConfigValid(
            lease,
            *persisted_config,
            backend_options,
            segment,
            &journal,
            &expected_journal_size) ||
        key.stream_day_id != segment->stream_day_id ||
        artifact_options.expected_raw_schema_sha256 !=
            segment->raw_schema_sha256) {
        static_cast<void>(journal);
        static_cast<void>(expected_journal_size);
        return false;
    }
    return true;
}

}  // namespace

const char* RawPosixWalStreamBackendFailureV1Name(
    RawPosixWalStreamBackendFailureV1 failure) noexcept {
    switch (failure) {
    case RawPosixWalStreamBackendFailureV1::kNone:
        return "none";
    case RawPosixWalStreamBackendFailureV1::
        kInvalidConfiguration:
        return "invalid_configuration";
    case RawPosixWalStreamBackendFailureV1::kManifestLoad:
        return "manifest_load";
    case RawPosixWalStreamBackendFailureV1::kInvalidState:
        return "invalid_state";
    case RawPosixWalStreamBackendFailureV1::
        kClosedSnapshotMismatch:
        return "closed_snapshot_mismatch";
    case RawPosixWalStreamBackendFailureV1::
        kJournalVerification:
        return "journal_verification";
    case RawPosixWalStreamBackendFailureV1::
        kArtifactPublish:
        return "artifact_publish";
    case RawPosixWalStreamBackendFailureV1::
        kManifestTransition:
        return "manifest_transition";
    case RawPosixWalStreamBackendFailureV1::
        kManifestPublish:
        return "manifest_publish";
    case RawPosixWalStreamBackendFailureV1::
        kRotationMismatch:
        return "rotation_mismatch";
    case RawPosixWalStreamBackendFailureV1::kClockFailure:
        return "clock_failure";
    case RawPosixWalStreamBackendFailureV1::
        kBootstrapCreate:
        return "bootstrap_create";
    case RawPosixWalStreamBackendFailureV1::kIoAdoption:
        return "io_adoption";
    case RawPosixWalStreamBackendFailureV1::
        kOpenSnapshotMismatch:
        return "open_snapshot_mismatch";
    case RawPosixWalStreamBackendFailureV1::
        kControlPublish:
        return "control_publish";
    }
    return "unknown";
}

std::string_view
RawFreshActivePosixStreamFailureV1Name(
    RawFreshActivePosixStreamFailureV1 failure) noexcept {
    switch (failure) {
    case RawFreshActivePosixStreamFailureV1::kNone:
        return "none";
    case RawFreshActivePosixStreamFailureV1::kInvalidInput:
        return "invalid_input";
    case RawFreshActivePosixStreamFailureV1::
        kInitAuthorization:
        return "init_authorization";
    case RawFreshActivePosixStreamFailureV1::
        kExistingManifest:
        return "existing_manifest";
    case RawFreshActivePosixStreamFailureV1::
        kInitialSegmentCreate:
        return "initial_segment_create";
    case RawFreshActivePosixStreamFailureV1::kIoAdoption:
        return "io_adoption";
    case RawFreshActivePosixStreamFailureV1::kBackendCreate:
        return "backend_create";
    case RawFreshActivePosixStreamFailureV1::
        kBackendAuthorization:
        return "backend_authorization";
    case RawFreshActivePosixStreamFailureV1::
        kInitialIoBinding:
        return "initial_io_binding";
    case RawFreshActivePosixStreamFailureV1::
        kStreamConstruction:
        return "stream_construction";
    case RawFreshActivePosixStreamFailureV1::
        kStreamInitialize:
        return "stream_initialize";
    case RawFreshActivePosixStreamFailureV1::
        kOwningStreamAllocation:
        return "owning_stream_allocation";
    case RawFreshActivePosixStreamFailureV1::
        kActivePublication:
        return "active_publication";
    case RawFreshActivePosixStreamFailureV1::
        kActivePromotion:
        return "active_promotion";
    }
    return "unknown";
}

RawPosixWalStreamBackendV1::
    RawPosixWalStreamBackendV1(
        RawWriterLease& lease,
        RawPosixWalStreamBackendOptionsV1 options,
        RawV1JournalHeaderWire journal_header,
        RawManifestNamespaceV1 namespace_identity,
        SegmentHeaderV1 initial_segment,
        std::uint64_t expected_initialized_journal_size,
        RawManifestV1 initial_manifest,
        bool have_initial_manifest) noexcept
    : lease_(lease),
      options_(options),
      journal_header_(journal_header),
      namespace_identity_(namespace_identity),
      current_segment_(initial_segment),
      manifest_(std::move(initial_manifest)),
      have_manifest_(have_initial_manifest),
      expected_initialized_journal_size_(
          expected_initialized_journal_size) {}

RawPosixWalStreamBackendV1::
    ~RawPosixWalStreamBackendV1() = default;

bool RawPosixWalStreamBackendV1::Failed() const noexcept {
    return failure() !=
           RawPosixWalStreamBackendFailureV1::kNone;
}

void RawPosixWalStreamBackendV1::Fail(
    RawPosixWalStreamBackendFailureV1 failure_value,
    int error_number) noexcept {
    if (failure_.load(std::memory_order_acquire) !=
        static_cast<std::uint8_t>(
            RawPosixWalStreamBackendFailureV1::kNone)) {
        return;
    }
    error_number_.store(
        error_number == 0 ? EIO : error_number,
        std::memory_order_relaxed);
    std::uint8_t expected =
        static_cast<std::uint8_t>(
            RawPosixWalStreamBackendFailureV1::kNone);
    static_cast<void>(failure_.compare_exchange_strong(
        expected,
        static_cast<std::uint8_t>(failure_value),
        std::memory_order_release,
        std::memory_order_acquire));
}

bool RawPosixWalStreamBackendV1::ReadClock(
    bool realtime,
    std::uint64_t* value) noexcept {
    if (value == nullptr) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kClockFailure,
            EINVAL);
        return false;
    }
    RawWalStreamBackendClockNowV1 callback =
        realtime ? options_.realtime_now
                 : options_.monotonic_now;
    void* context =
        realtime ? options_.realtime_clock_context
                 : options_.monotonic_clock_context;
    if (callback != nullptr) {
        *value = callback(context);
        if (*value == 0U) {
            Fail(
                RawPosixWalStreamBackendFailureV1::
                    kClockFailure,
                ERANGE);
            return false;
        }
        return true;
    }

    struct timespec now {};
    const clockid_t clock =
        realtime ? CLOCK_REALTIME : CLOCK_MONOTONIC;
    int result = 0;
    do {
        result = ::clock_gettime(clock, &now);
    } while (result != 0 && errno == EINTR);
    if (result != 0 || now.tv_sec < 0 ||
        now.tv_nsec < 0 ||
        now.tv_nsec >= 1'000'000'000L ||
        static_cast<std::uint64_t>(now.tv_sec) >
            std::numeric_limits<std::uint64_t>::max() /
                UINT64_C(1'000'000'000)) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kClockFailure,
            result == 0 ? ERANGE : errno);
        return false;
    }
    const std::uint64_t seconds =
        static_cast<std::uint64_t>(now.tv_sec) *
        UINT64_C(1'000'000'000);
    if (!CheckedAdd(
            seconds,
            static_cast<std::uint64_t>(now.tv_nsec),
            value) ||
        *value == 0U) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kClockFailure,
            ERANGE);
        return false;
    }
    return true;
}

bool RawPosixWalStreamBackendV1::
    ValidateOpenSnapshot(
        const SegmentHeaderV1& segment,
        const RawWalWriterSnapshot&
            snapshot) const noexcept {
    std::uint64_t expected_global = 0U;
    return SameHeader(segment, current_segment_) &&
           segment.first_ingress_sequence != 0U &&
           snapshot.initialized &&
           !snapshot.sealed &&
           !snapshot.closed &&
           !snapshot.fatal &&
           snapshot.append == snapshot.durable &&
           snapshot.append.segment_offset ==
               kRawV1SegmentHeaderBytes &&
           snapshot.append.ingress_sequence ==
               segment.first_ingress_sequence - 1U &&
           CheckedAdd(
               segment.segment_base_wal_pos,
               kRawV1SegmentHeaderBytes,
               &expected_global) &&
           snapshot.append.global_wal_pos ==
               expected_global &&
           snapshot.journal_logical_size ==
               expected_initialized_journal_size_;
}

bool RawPosixWalStreamBackendV1::
    ValidateSealedSnapshot(
        const RawSegmentArtifactPlanV1& plan,
        const RawWalWriterSnapshot&
            snapshot) const noexcept {
    if (!plan.ok() ||
        plan.retained_segment_fd_bound ||
        !SameHeader(
            plan.metadata.segment,
            current_segment_) ||
        plan.options.expected_raw_schema_sha256 !=
            current_segment_.raw_schema_sha256 ||
        !snapshot.initialized ||
        !snapshot.sealed ||
        !snapshot.closed ||
        snapshot.fatal ||
        snapshot.append != snapshot.durable ||
        snapshot.durable.segment_offset !=
            plan.metadata.logical_end_offset ||
        snapshot.journal_logical_size <
            kRawV1JournalHeaderBytes +
                kRawV1DurableMarkerBytes ||
        (snapshot.journal_logical_size -
         kRawV1JournalHeaderBytes) %
                kRawV1DurableMarkerBytes !=
            0U) {
        return false;
    }

    const DurableMarkerV1& marker =
        plan.metadata.accepted_sealed_marker;
    DurableMarkerV1 decoded_marker{};
    RawV1DurableMarkerWire canonical_marker{};
    std::uint64_t expected_global = 0U;
    return marker.source_stream_id ==
               current_segment_.source_stream_id &&
           marker.segment_sequence ==
               current_segment_.segment_sequence &&
           marker.marker_flags == kRawV1SegmentSealed &&
           marker.durable_global_wal_pos ==
               snapshot.durable.global_wal_pos &&
           marker.durable_ingress_sequence ==
               snapshot.durable.ingress_sequence &&
           marker.durable_segment_offset ==
               snapshot.durable.segment_offset &&
           CheckedAdd(
               current_segment_.segment_base_wal_pos,
               plan.metadata.logical_end_offset,
               &expected_global) &&
           expected_global ==
               snapshot.durable.global_wal_pos &&
           DecodeDurableMarkerV1(
               plan.metadata
                   .accepted_sealed_marker_bytes,
               &decoded_marker) == RawV1Error::kNone &&
           SameMarker(marker, decoded_marker) &&
           EncodeDurableMarkerV1(
               marker, &canonical_marker) ==
               RawV1Error::kNone &&
           canonical_marker ==
               plan.metadata
                   .accepted_sealed_marker_bytes;
}

bool RawPosixWalStreamBackendV1::VerifyJournalSeal(
    const RawSegmentArtifactPlanV1& plan,
    const RawWalWriterSnapshot&
        sealed_snapshot) noexcept {
    int journal_fd = -1;
    do {
        journal_fd = ::openat(
            lease_.directory_descriptor(),
            kRawJournalFilename,
            O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME);
    } while (journal_fd < 0 && errno == EINTR);
    if (journal_fd < 0) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kJournalVerification,
            errno);
        return false;
    }

    struct stat opened {};
    struct stat named {};
    const bool metadata_valid =
        ::fstat(journal_fd, &opened) == 0 &&
        ::fstatat(
            lease_.directory_descriptor(),
            kRawJournalFilename,
            &named,
            AT_SYMLINK_NOFOLLOW) == 0 &&
        S_ISREG(opened.st_mode) &&
        S_ISREG(named.st_mode) &&
        opened.st_uid == ::geteuid() &&
        (opened.st_mode & 07777) == 0600 &&
        opened.st_nlink == 1 &&
        opened.st_dev == named.st_dev &&
        opened.st_ino == named.st_ino &&
        opened.st_size >= 0 &&
        static_cast<std::uint64_t>(opened.st_size) ==
            sealed_snapshot.journal_logical_size;
    if (!metadata_valid) {
        const int saved_errno = errno == 0 ? EILSEQ : errno;
        CloseDescriptor(journal_fd);
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kJournalVerification,
            saved_errno);
        return false;
    }

    RawV1JournalHeaderWire header{};
    RawV1DurableMarkerWire marker{};
    const bool bytes_valid =
        PreadExact(journal_fd, 0U, header) &&
        PreadExact(
            journal_fd,
            sealed_snapshot.journal_logical_size -
                kRawV1DurableMarkerBytes,
            marker);
    const int saved_errno = errno;
    CloseDescriptor(journal_fd);
    if (!bytes_valid || header != journal_header_ ||
        marker !=
            plan.metadata
                .accepted_sealed_marker_bytes) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kJournalVerification,
            !bytes_valid && saved_errno != 0
                ? saved_errno
                : EILSEQ);
        return false;
    }
    return true;
}

bool RawPosixWalStreamBackendV1::
    PublishClosedSegment(
        RawSegmentArtifactPlanV1 plan,
        const RawWalWriterSnapshot&
            sealed_snapshot) noexcept {
    if (Failed()) {
        return false;
    }
    if (phase_ != Phase::kOpen ||
        !have_manifest_ ||
        !manifest_.open_entry.has_value()) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kInvalidState,
            EPERM);
        return false;
    }
    if (!ValidateSealedSnapshot(
            plan, sealed_snapshot)) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kClosedSnapshotMismatch,
            EILSEQ);
        return false;
    }
    if (!VerifyJournalSeal(plan, sealed_snapshot)) {
        return false;
    }

    const RawSealedSegmentMetadataV1 metadata =
        plan.metadata;
    RawSegmentArtifactCausalProofV1 proof{};
    proof.segment_truncated_and_synced_to_logical_end =
        true;
    proof.accepted_seal_marker_journal_synced = true;
    proof.synced_segment_logical_end_offset =
        metadata.logical_end_offset;
    proof.synced_segment_sha256 =
        metadata.segment_sha256;
    proof.journal_synced_sealed_marker_bytes =
        metadata.accepted_sealed_marker_bytes;
    RawSegmentArtifactPublishResultV1 artifact =
        BindAndPublishIncrementalRawSegmentArtifactAtV1(
            lease_.directory_descriptor(),
            std::move(plan),
            proof);
    if (!artifact.ok() ||
        !SameMetadata(artifact.metadata, metadata)) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kArtifactPublish,
            artifact.error_number == 0
                ? EIO
                : artifact.error_number);
        return false;
    }

    RawManifestV1 candidate{};
    if (TransitionOpenRawManifestToClosedV1(
            manifest_, metadata, &candidate) !=
        RawManifestTransitionErrorV1::kNone) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kManifestTransition,
            EILSEQ);
        return false;
    }
    RawManifestV1Error model_error =
        RawManifestV1Error::kNone;
    std::string error;
    if (PublishCurrentRawManifest(
            lease_,
            namespace_identity_,
            candidate,
            options_.maximum_manifest_bytes,
            &model_error,
            &error) != RawManifestStoreError::kNone) {
        static_cast<void>(model_error);
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kManifestPublish,
            EIO);
        return false;
    }

    manifest_ = std::move(candidate);
    closed_journal_logical_size_ =
        sealed_snapshot.journal_logical_size;
    phase_ = Phase::kClosed;
    return true;
}

bool RawPosixWalStreamBackendV1::CreateNextSegment(
    const RawWalRotationPlan& rotation,
    std::uint64_t opened_monotonic_ns,
    RawWalNextSegmentBootstrapV1*
        bootstrap) noexcept {
    if (Failed()) {
        return false;
    }
    if (phase_ != Phase::kClosed ||
        bootstrap == nullptr ||
        bootstrap->io != nullptr ||
        !have_manifest_ ||
        manifest_.open_entry.has_value() ||
        manifest_.closed_entries.empty()) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kInvalidState,
            EPERM);
        return false;
    }

    const RawManifestSegmentEntryV1& closed =
        manifest_.closed_entries.back();
    DurableMarkerV1 marker{};
    std::uint64_t expected_next_base = 0U;
    const bool rotation_valid =
        rotation.ok() &&
        current_segment_.segment_sequence !=
            std::numeric_limits<std::uint32_t>::max() &&
        rotation.next_segment_sequence ==
            current_segment_.segment_sequence + 1U &&
        rotation.next_segment_flags == 0U &&
        closed.segment_sequence ==
            current_segment_.segment_sequence &&
        DecodeDurableMarkerV1(
            closed.accepted_marker_bytes,
            &marker) == RawV1Error::kNone &&
        marker.marker_flags == kRawV1SegmentSealed &&
        CheckedAdd(
            closed.segment_base_wal_pos,
            closed.segment_logical_length,
            &expected_next_base) &&
        rotation.next_segment_base_wal_pos ==
            expected_next_base &&
        marker.durable_ingress_sequence !=
            std::numeric_limits<std::uint64_t>::max() &&
        rotation.next_first_ingress_sequence ==
            marker.durable_ingress_sequence + 1U &&
        rotation.initial_durable_ingress_sequence ==
            marker.durable_ingress_sequence &&
        rotation.existing_journal
                .journal_append_offset ==
            closed_journal_logical_size_ &&
        rotation.existing_journal
                .previous_segment_sequence ==
            current_segment_.segment_sequence &&
        rotation.existing_journal
                .previous_sealed_cursor ==
            RawWalCursor{
                marker.durable_global_wal_pos,
                marker.durable_ingress_sequence,
                marker.durable_segment_offset} &&
        rotation.existing_journal
                .previous_marker_flags ==
            kRawV1SegmentSealed &&
        opened_monotonic_ns != 0U &&
        opened_monotonic_ns >=
            current_segment_.created_monotonic_ns;
    if (!rotation_valid) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kRotationMismatch,
            EILSEQ);
        return false;
    }

    std::uint64_t created_realtime_ns = 0U;
    if (!ReadClock(true, &created_realtime_ns)) {
        return false;
    }
    SegmentHeaderV1 next = current_segment_;
    next.segment_sequence =
        rotation.next_segment_sequence;
    next.segment_flags = rotation.next_segment_flags;
    next.segment_base_wal_pos =
        rotation.next_segment_base_wal_pos;
    next.first_ingress_sequence =
        rotation.next_first_ingress_sequence;
    next.created_realtime_ns = created_realtime_ns;
    next.created_monotonic_ns = opened_monotonic_ns;
    next.reserve_state_uuid = {};
    next.finalization_cycle_id = {};
    next.immutable_grant_sha256 = {};

    RawV1SegmentHeaderWire next_wire{};
    if (EncodeSegmentHeaderV1(
            next, &next_wire) != RawV1Error::kNone) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kRotationMismatch,
            EILSEQ);
        return false;
    }

    std::string error;
    std::unique_ptr<RawBootstrapFiles> files =
        CreateRotatedRawBootstrap(
            lease_,
            next_wire,
            journal_header_,
            rotation.existing_journal,
            options_.segment_preallocation_bytes,
            &error);
    if (files == nullptr) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kBootstrapCreate,
            EIO);
        return false;
    }
    const int segment_fd = files->ReleaseSegmentFd();
    const int journal_fd = files->ReleaseJournalFd();
    const std::array<char, 21U> segment_filename =
        SegmentFilename(next.segment_sequence);
    std::unique_ptr<RawWalIo> io =
        AdoptTargetBoundPosixRawWalIo(
            lease_.directory_descriptor(),
            std::string_view(
                segment_filename.data(), 20U),
            segment_fd,
            journal_fd,
            &error);
    if (io == nullptr) {
        CloseDescriptor(segment_fd);
        CloseDescriptor(journal_fd);
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kIoAdoption,
            EIO);
        return false;
    }

    RawWalWriterConfig config{};
    config.segment_header_wire = next_wire;
    config.journal_header_wire = journal_header_;
    config.source_stream_id = next.source_stream_id;
    config.capture_date = next.capture_date;
    config.segment_sequence = next.segment_sequence;
    config.segment_base_wal_pos =
        next.segment_base_wal_pos;
    config.first_ingress_sequence =
        next.first_ingress_sequence;
    config.initial_durable_ingress_sequence =
        rotation.initial_durable_ingress_sequence;
    config.writer_instance =
        options_.writer_instance;
    config.initialization_mode =
        RawWalInitializationMode::kExistingJournal;
    config.existing_journal =
        rotation.existing_journal;
    config.headers_already_persisted = true;
    config.commit_observer = nullptr;

    RawWalNextSegmentBootstrapV1 candidate{};
    candidate.writer_config = config;
    candidate.io = std::move(io);
    *bootstrap = std::move(candidate);
    current_segment_ = next;
    if (!CheckedAdd(
            rotation.existing_journal
                .journal_append_offset,
            kRawV1DurableMarkerBytes,
            &expected_initialized_journal_size_)) {
        // This was checked by the rotation plan and bootstrap. Preserve a
        // fail-stop guard if either contract changes.
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kRotationMismatch,
            EOVERFLOW);
        return false;
    }
    phase_ = Phase::kAwaitOpen;
    return true;
}

bool RawPosixWalStreamBackendV1::PublishOpenManifest(
    const SegmentHeaderV1& segment,
    const RawWalWriterSnapshot&
        initialized_snapshot) noexcept {
    if (Failed()) {
        return false;
    }
    if (phase_ != Phase::kAwaitOpen) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kInvalidState,
            EPERM);
        return false;
    }
    if (!ValidateOpenSnapshot(
            segment, initialized_snapshot)) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kOpenSnapshotMismatch,
            EILSEQ);
        return false;
    }

    RawManifestV1 candidate{};
    RawManifestTransitionErrorV1 result =
        RawManifestTransitionErrorV1::
            kFinalizationContinuationUnsupported;
    if (have_manifest_) {
        result =
            (segment.segment_flags &
             kRawV1FinalizationContinuation) != 0U
                ? TransitionClosedRawManifestToFinalizationContinuationOpenV1(
                      manifest_,
                      segment,
                      initialized_snapshot,
                      &candidate)
                : TransitionClosedRawManifestToNextOpenV1(
                      manifest_,
                      segment,
                      initialized_snapshot,
                      &candidate);
    } else if ((segment.segment_flags &
                kRawV1FinalizationContinuation) == 0U) {
        result = BuildFreshOpenRawManifestV1(
            segment,
            initialized_snapshot,
            &candidate);
    }
    if (result != RawManifestTransitionErrorV1::kNone) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kManifestTransition,
            EILSEQ);
        return false;
    }

    RawManifestV1Error model_error =
        RawManifestV1Error::kNone;
    std::string error;
    if (PublishCurrentRawManifest(
            lease_,
            namespace_identity_,
            candidate,
            options_.maximum_manifest_bytes,
            &model_error,
            &error) != RawManifestStoreError::kNone) {
        static_cast<void>(model_error);
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kManifestPublish,
            EIO);
        return false;
    }
    manifest_ = std::move(candidate);
    have_manifest_ = true;
    phase_ = Phase::kOpen;
    return true;
}

bool RawPosixWalStreamBackendV1::
    ControlPathStillNamesWriter() const noexcept {
    if (control_writer_ == nullptr ||
        control_writer_->descriptor() < 0) {
        return false;
    }
    struct stat opened {};
    struct stat named {};
    return ::fstat(
               control_writer_->descriptor(),
               &opened) == 0 &&
           ::fstatat(
               lease_.directory_descriptor(),
               kRawControlFilename,
               &named,
               AT_SYMLINK_NOFOLLOW) == 0 &&
           S_ISREG(opened.st_mode) &&
           S_ISREG(named.st_mode) &&
           opened.st_uid == ::geteuid() &&
           (opened.st_mode & 07777) == 0600 &&
           opened.st_nlink == 1 &&
           opened.st_dev == named.st_dev &&
           opened.st_ino == named.st_ino;
}

bool RawPosixWalStreamBackendV1::PublishControl(
    const SegmentHeaderV1& segment,
    const RawWalWriterSnapshot&
        snapshot) noexcept {
    if (Failed()) {
        return false;
    }
    std::uint64_t append_global = 0U;
    std::uint64_t durable_global = 0U;
    if (phase_ != Phase::kOpen ||
        !SameHeader(segment, current_segment_) ||
        !snapshot.initialized ||
        snapshot.sealed ||
        snapshot.closed ||
        snapshot.append.segment_offset <
            kRawV1SegmentHeaderBytes ||
        snapshot.durable.segment_offset <
            kRawV1SegmentHeaderBytes ||
        snapshot.durable.segment_offset >
            snapshot.append.segment_offset ||
        snapshot.durable.ingress_sequence >
            snapshot.append.ingress_sequence ||
        segment.first_ingress_sequence == 0U ||
        snapshot.append.ingress_sequence <
            segment.first_ingress_sequence - 1U ||
        !CheckedAdd(
            segment.segment_base_wal_pos,
            snapshot.append.segment_offset,
            &append_global) ||
        !CheckedAdd(
            segment.segment_base_wal_pos,
            snapshot.durable.segment_offset,
            &durable_global) ||
        snapshot.append.global_wal_pos !=
            append_global ||
        snapshot.durable.global_wal_pos !=
            durable_global ||
        snapshot.journal_logical_size <
            kRawV1JournalHeaderBytes +
                kRawV1DurableMarkerBytes ||
        (snapshot.journal_logical_size -
         kRawV1JournalHeaderBytes) %
                kRawV1DurableMarkerBytes !=
            0U) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kControlPublish,
            EILSEQ);
        return false;
    }

    std::uint64_t heartbeat = 0U;
    if (!ReadClock(false, &heartbeat)) {
        return false;
    }
    if (have_control_snapshot_ &&
        heartbeat <
            last_heartbeat_monotonic_ns_) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kClockFailure,
            ERANGE);
        return false;
    }

    RawControlSnapshot control{};
    control.writer_instance =
        options_.writer_instance;
    control.stream_day_id = segment.stream_day_id;
    control.source_stream_id =
        segment.source_stream_id;
    control.capture_date = segment.capture_date;
    control.segment_sequence =
        segment.segment_sequence;
    control.fatal_state = snapshot.fatal ? 1U : 0U;
    control.append_global_wal_pos =
        snapshot.append.global_wal_pos;
    control.append_ingress_sequence =
        snapshot.append.ingress_sequence;
    control.append_segment_offset =
        snapshot.append.segment_offset;
    control.durable_global_wal_pos =
        snapshot.durable.global_wal_pos;
    control.durable_ingress_sequence =
        snapshot.durable.ingress_sequence;
    control.durable_segment_offset =
        snapshot.durable.segment_offset;
    control.clock_epoch_label =
        segment.clock_epoch_label;
    control.heartbeat_monotonic_ns = heartbeat;

    if (control_writer_ == nullptr) {
        std::string error;
        std::unique_ptr<RawControlFileWriter> writer =
            CreateRawControlFile(
                lease_, control, &error);
        if (writer == nullptr) {
            Fail(
                RawPosixWalStreamBackendFailureV1::
                    kControlPublish,
                EIO);
            return false;
        }
        control_writer_ = std::move(writer);
    } else if (!ControlPathStillNamesWriter() ||
               !control_writer_->Publish(control)) {
        Fail(
            RawPosixWalStreamBackendFailureV1::
                kControlPublish,
            EIO);
        return false;
    }

    last_control_snapshot_ = control;
    have_control_snapshot_ = true;
    last_heartbeat_monotonic_ns_ = heartbeat;
    return true;
}

std::unique_ptr<RawPosixWalStreamBackendV1>
CreateRawPosixWalStreamBackendV1(
    RawWriterLease& lease,
    const RawWalWriterConfig& initial_writer_config,
    RawPosixWalStreamBackendOptionsV1 options,
    std::string* error) noexcept {
    SetError(error, {});
    SegmentHeaderV1 segment{};
    DurableJournalHeaderV1 journal{};
    std::uint64_t expected_initialized_journal_size =
        0U;
    if (!InitialConfigValid(
            lease,
            initial_writer_config,
            options,
            &segment,
            &journal,
            &expected_initialized_journal_size)) {
        static_cast<void>(journal);
        SetError(
            error,
            "invalid initial POSIX Raw WAL stream configuration");
        return nullptr;
    }

    const RawManifestNamespaceV1 namespace_identity =
        NamespaceOf(segment);
    RawManifestV1 initial_manifest{};
    RawManifestV1Error model_error =
        RawManifestV1Error::kNone;
    std::string load_error;
    const RawManifestStoreError load_result =
        LoadCurrentRawManifestAt(
            lease.directory_descriptor(),
            namespace_identity,
            options.maximum_manifest_bytes,
            &initial_manifest,
            nullptr,
            &model_error,
            &load_error);
    const bool fresh =
        initial_writer_config.initialization_mode ==
        RawWalInitializationMode::kFreshJournal;
    bool have_manifest = false;
    if (fresh) {
        if (load_result !=
            RawManifestStoreError::kNotFound) {
            SetError(
                error,
                "fresh Raw WAL stream requires manifest.json to be absent");
            return nullptr;
        }
    } else {
        if (load_result != RawManifestStoreError::kNone ||
            !ExistingManifestMatchesInitialConfig(
                initial_manifest,
                segment,
                initial_writer_config.existing_journal)) {
            static_cast<void>(model_error);
            SetError(
                error,
                "rotated Raw WAL stream requires its exact closed manifest predecessor");
            return nullptr;
        }
        have_manifest = true;
    }

    try {
        std::unique_ptr<RawPosixWalStreamBackendV1>
            backend(
                new RawPosixWalStreamBackendV1(
                    lease,
                    options,
                    initial_writer_config
                        .journal_header_wire,
                    namespace_identity,
                    segment,
                    expected_initialized_journal_size,
                    std::move(initial_manifest),
                    have_manifest));
        return backend;
    } catch (...) {
        SetError(
            error,
            "cannot allocate POSIX Raw WAL stream backend");
        return nullptr;
    }
}

RawFreshActivePosixStreamResultV1
CreateFreshActiveRawPosixStreamV1(
    std::unique_ptr<RawWriterLease> lease,
    std::unique_ptr<RawFreshJournalAnchor>
        journal_anchor,
    RawWalWriterConfig logical_writer_config,
    RawPosixWalStreamBackendOptionsV1 backend_options,
    RawSegmentArtifactOptionsV1 artifact_options,
    RawWalStreamLimitsV1 stream_limits,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawReserveRegistryEntryKeyV1 key,
    std::string_view stream_slug,
    std::string* error) noexcept {
    RawFreshActivePosixStreamResultV1 result{};
    SetError(error, {});
    RawWalWriterConfig persisted_config{};
    SegmentHeaderV1 segment{};
    if (lease == nullptr ||
        journal_anchor == nullptr ||
        stream_slug.empty() ||
        !FreshFactoryPreflight(
            *lease,
            *journal_anchor,
            logical_writer_config,
            backend_options,
            artifact_options,
            stream_limits,
            key,
            &persisted_config,
            &segment)) {
        result.failure =
            RawFreshActivePosixStreamFailureV1::
                kInvalidInput;
        SetError(
            error,
            "invalid fresh ACTIVE POSIX Raw stream input");
        return result;
    }

    auto init_action =
        coordinator.AcquireActionForExistingRoute(
            key,
            ReserveRegistryStatusV1::kInit,
            stream_slug,
            &result.coordinator_failure,
            error);
    BorrowedRawTargetDirectoryV1 lease_target(
        lease->directory_descriptor());
    if (init_action == nullptr ||
        init_action->target() == nullptr ||
        init_action->recovery_intent() !=
            ReserveRecoveryIntentV1::kResumeConnect ||
        init_action->token().writer_instance_id !=
            backend_options.writer_instance ||
        init_action->token().recovery_attempt_id !=
            key.recovery_attempt_id ||
        !ValidateRawReserveMutationTargetProviderV1(
            lease_target, *init_action->target()) ||
        !init_action->ValidateLatest(nullptr)) {
        result.failure =
            RawFreshActivePosixStreamFailureV1::
                kInitAuthorization;
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "fresh Raw stream lacks exact target-bound INIT writer authorization");
        }
        return result;
    }

    RawManifestV1 unexpected_manifest{};
    const RawManifestStoreError manifest_result =
        LoadCurrentRawManifestAt(
            lease->directory_descriptor(),
            NamespaceOf(segment),
            backend_options.maximum_manifest_bytes,
            &unexpected_manifest,
            nullptr,
            nullptr,
            error);
    if (manifest_result !=
        RawManifestStoreError::kNotFound) {
        result.failure =
            RawFreshActivePosixStreamFailureV1::
                kExistingManifest;
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "fresh INIT route already has a Raw manifest");
        }
        return result;
    }

    RawFreshStateAuthorizationV1 init_authorization{};
    init_authorization.source_stream_id =
        key.route.source_stream_id;
    init_authorization.capture_date =
        key.route.capture_date;
    init_authorization.stream_day_id =
        key.stream_day_id;
    init_authorization.recovery_attempt =
        key.recovery_attempt_id;
    init_authorization.registry_stage =
        RawFreshRegistryStageV1::kInit;
    init_authorization.durable_state_generation =
        init_action->token().state_generation;
    std::unique_ptr<RawBootstrapFiles> bootstrap =
        CreateInitialRawSegment(
            *lease,
            *journal_anchor,
            persisted_config.segment_header_wire,
            init_authorization,
            *init_action,
            backend_options.segment_preallocation_bytes,
            error);
    if (bootstrap == nullptr) {
        result.failure =
            RawFreshActivePosixStreamFailureV1::
                kInitialSegmentCreate;
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "cannot publish coordinator-authorized initial Raw segment");
        }
        return result;
    }
    result.initial_segment_published = true;
    journal_anchor.reset();

    const int segment_fd =
        bootstrap->ReleaseSegmentFd();
    const int journal_fd =
        bootstrap->ReleaseJournalFd();
    bootstrap.reset();
    std::unique_ptr<RawWalIo> initial_io =
        AdoptTargetBoundPosixRawWalIo(
            lease->directory_descriptor(),
            kRawFirstSegmentFilename,
            segment_fd,
            journal_fd,
            error);
    if (initial_io == nullptr) {
        CloseDescriptor(segment_fd);
        CloseDescriptor(journal_fd);
        result.failure =
            RawFreshActivePosixStreamFailureV1::
                kIoAdoption;
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "cannot adopt target-bound initial Raw descriptors");
        }
        return result;
    }

    std::unique_ptr<RawPosixWalStreamBackendV1>
        posix_backend =
            CreateRawPosixWalStreamBackendV1(
                *lease,
                persisted_config,
                backend_options,
                error);
    if (posix_backend == nullptr) {
        result.failure =
            RawFreshActivePosixStreamFailureV1::
                kBackendCreate;
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "cannot create fresh POSIX Raw stream backend");
        }
        return result;
    }

    std::unique_ptr<
        RawReserveAuthorizedWalStreamBackendV1>
        backend =
            GateRawWalStreamBackendWithCoordinatorForWriterV1(
                std::move(posix_backend),
                coordinator,
                key,
                ReserveRegistryStatusV1::kInit,
                stream_slug,
                backend_options.writer_instance,
                error);
    if (backend == nullptr) {
        result.failure =
            RawFreshActivePosixStreamFailureV1::
                kBackendAuthorization;
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "cannot bind fresh Raw backend to INIT writer authorization");
        }
        return result;
    }

    initial_io =
        backend->BindInitialIo(
            std::move(initial_io), error);
    if (initial_io == nullptr) {
        result.failure =
            RawFreshActivePosixStreamFailureV1::
                kInitialIoBinding;
        result.authorization_failure =
            backend->failure();
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "cannot bind initial Raw I/O to the shared INIT authorization");
        }
        return result;
    }

    std::unique_ptr<RawWalStreamWriter> stream;
    try {
        stream = std::make_unique<RawWalStreamWriter>(
            persisted_config,
            std::move(initial_io),
            artifact_options,
            stream_limits,
            *backend);
    } catch (...) {
    }
    if (stream == nullptr) {
        result.failure =
            RawFreshActivePosixStreamFailureV1::
                kStreamConstruction;
        result.authorization_failure =
            backend->failure();
        SetError(
            error,
            "cannot construct fresh Raw WAL stream");
        return result;
    }
    if (!stream->Initialize()) {
        result.failure =
            RawFreshActivePosixStreamFailureV1::
                kStreamInitialize;
        result.authorization_failure =
            backend->failure();
        result.wal_failure = stream->failure();
        SetError(
            error,
            "cannot complete fresh Raw header marker/open manifest/control boundary");
        return result;
    }

    std::unique_ptr<
        RawFreshActivePosixStreamOwnerV1>
        owned_stream;
    try {
        owned_stream = std::make_unique<
            RawFreshActivePosixStreamOwnerV1>(
                std::move(lease),
                std::move(backend),
                std::move(stream));
    } catch (...) {
    }
    if (owned_stream == nullptr) {
        result.failure =
            RawFreshActivePosixStreamFailureV1::
                kOwningStreamAllocation;
        SetError(
            error,
            "cannot allocate owning fresh Raw stream");
        return result;
    }

    // PublishFreshActive() takes the exclusive generation transition gate.
    // The long-lived INIT shared action must therefore be released first;
    // the initialized backend/I/O binding remains local and cannot perform
    // an ACTIVE syscall until it consumes the returned receipt.
    init_action.reset();
    result.publication_state =
        RawFreshActivePublicationStateV1::
            kPublicationIndeterminate;
    std::unique_ptr<
        RawReserveActiveActivationReceiptV1> receipt =
            coordinator.PublishFreshActive(
                key,
                stream_slug,
                &result.coordinator_failure,
                error);
    if (receipt == nullptr) {
        result.failure =
            RawFreshActivePosixStreamFailureV1::
                kActivePublication;
        // Keep the result indeterminate for every failed publication call.
        // Besides an uncertain PublishNext barrier, an independently
        // serialized publisher may already have advanced this route before
        // this call reloaded INIT. Without this invocation's opaque receipt
        // the local stream must never infer permission from a status read.
        result.authorization_failure =
            owned_stream->authorization_failure();
        result.wal_failure =
            owned_stream->failure();
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "fresh Raw ACTIVE state publication failed");
        }
        return result;
    }

    result.publication_state =
        RawFreshActivePublicationStateV1::
            kPublishedAwaitingLocalPromotion;
    if (!owned_stream->PromoteToActive(
            std::move(receipt), error) ||
        !owned_stream->active_binding_validated()) {
        result.failure =
            RawFreshActivePosixStreamFailureV1::
                kActivePromotion;
        result.authorization_failure =
            owned_stream->authorization_failure();
        result.wal_failure =
            owned_stream->failure();
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "fresh Raw ACTIVE binding promotion failed");
        }
        return result;
    }

    result.publication_state =
        RawFreshActivePublicationStateV1::kActiveBound;
    result.sink =
        std::unique_ptr<RawActiveBoundWalSinkV1>(
            std::move(owned_stream));
    return result;
}

RawRecoveredClosedPosixStreamV1::
    RawRecoveredClosedPosixStreamV1(
        std::unique_ptr<RawWriterLease> lease,
        std::unique_ptr<
            RawReserveAuthorizedWalStreamBackendV1>
            backend,
        std::unique_ptr<RawWalStreamWriter> stream)
        noexcept
    : lease_(std::move(lease)),
      backend_(std::move(backend)),
      stream_(std::move(stream)) {}

RawRecoveredClosedPosixStreamV1::
    ~RawRecoveredClosedPosixStreamV1() = default;

bool RawRecoveredClosedPosixStreamV1::AppendRecord(
    const RawWalRecordInputV1& input) noexcept {
    return stream_->AppendRecord(input);
}

bool RawRecoveredClosedPosixStreamV1::
FlushDurable() noexcept {
    return stream_->FlushDurable();
}

bool RawRecoveredClosedPosixStreamV1::
SealAndClose() noexcept {
    return stream_->SealAndClose();
}

RawWalWriterSnapshot
RawRecoveredClosedPosixStreamV1::Snapshot()
    const noexcept {
    return stream_->Snapshot();
}

RawWalFailure
RawRecoveredClosedPosixStreamV1::failure()
    const noexcept {
    return stream_->failure();
}

RawWalSinkIdentityV1
RawRecoveredClosedPosixStreamV1::identity()
    const noexcept {
    return stream_->identity();
}

std::unique_ptr<RawRecoveredClosedPosixStreamV1>
CreateRecoveredClosedRawPosixStreamV1(
    std::unique_ptr<RawWriterLease> lease,
    RawRecoveredClosedWalStateV1 recovered,
    RawPosixWalStreamBackendOptionsV1 backend_options,
    RawSegmentArtifactOptionsV1 artifact_options,
    RawWalStreamLimitsV1 stream_limits,
    std::uint64_t opened_monotonic_ns,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawReserveRegistryEntryKeyV1 key,
    std::string_view stream_slug,
    std::string* error) noexcept {
    SetError(error, {});
    if (lease == nullptr ||
        opened_monotonic_ns == 0U ||
        stream_slug.empty() ||
        !ValidateRecoveredClosedState(
            *lease,
            recovered,
            backend_options,
            artifact_options,
            key,
            error)) {
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "invalid recovered closed Raw stream input");
        }
        return nullptr;
    }

    RawWalWriterSnapshot sealed_snapshot{};
    sealed_snapshot.append =
        recovered.accepted_sealed_cursor;
    sealed_snapshot.durable =
        recovered.accepted_sealed_cursor;
    sealed_snapshot.journal_logical_size =
        recovered.journal_logical_size;
    sealed_snapshot.initialized = true;
    sealed_snapshot.sealed = true;
    sealed_snapshot.closed = true;
    const RawWalRotationPlan rotation =
        PlanRawWalRotation(
            recovered.terminal_segment,
            sealed_snapshot);
    if (!rotation.ok()) {
        SetError(
            error,
            "recovered closed Raw cursor cannot form a normal rotation plan");
        return nullptr;
    }

    std::unique_ptr<RawPosixWalStreamBackendV1>
        posix_backend;
    try {
        posix_backend.reset(
            new RawPosixWalStreamBackendV1(
                *lease,
                backend_options,
                recovered.journal_header_wire,
                NamespaceOf(
                    recovered.terminal_segment),
                recovered.terminal_segment,
                0U,
                std::move(
                    recovered.closed_manifest),
                true));
    } catch (...) {
        SetError(
            error,
            "cannot allocate recovered closed POSIX Raw backend");
        return nullptr;
    }
    posix_backend->phase_ =
        RawPosixWalStreamBackendV1::Phase::kClosed;
    posix_backend->closed_journal_logical_size_ =
        recovered.journal_logical_size;

    std::unique_ptr<
        RawReserveAuthorizedWalStreamBackendV1>
        backend =
            GateRawWalStreamBackendWithCoordinatorForWriterV1(
                std::move(posix_backend),
                coordinator,
                key,
                ReserveRegistryStatusV1::kRecovering,
                stream_slug,
                backend_options.writer_instance,
                error);
    if (backend == nullptr) {
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "cannot acquire writer-bound recovery backend");
        }
        return nullptr;
    }

    RawWalNextSegmentBootstrapV1 bootstrap{};
    if (!backend->CreateNextSegment(
            rotation,
            opened_monotonic_ns,
            &bootstrap) ||
        bootstrap.io == nullptr) {
        SetError(
            error,
            "cannot create coordinator-authorized resumed Raw segment");
        return nullptr;
    }

    std::unique_ptr<RawWalStreamWriter> stream;
    try {
        stream = std::make_unique<RawWalStreamWriter>(
            bootstrap.writer_config,
            std::move(bootstrap.io),
            artifact_options,
            stream_limits,
            *backend);
    } catch (...) {
        SetError(
            error,
            "invalid resumed Raw WAL stream configuration");
        return nullptr;
    }
    if (!stream->Initialize()) {
        SetError(
            error,
            "cannot publish resumed Raw open manifest/control boundary");
        return nullptr;
    }

    try {
        return std::unique_ptr<
            RawRecoveredClosedPosixStreamV1>(
            new RawRecoveredClosedPosixStreamV1(
                std::move(lease),
                std::move(backend),
                std::move(stream)));
    } catch (...) {
        SetError(
            error,
            "cannot allocate owning resumed Raw stream");
        return nullptr;
    }
}

std::unique_ptr<RawActiveBoundWalSinkV1>
PromoteRecoveredClosedRawPosixStreamToActiveV1(
    std::unique_ptr<
        RawRecoveredClosedPosixStreamV1>&&
        recovering_stream,
    std::unique_ptr<
        RawReserveActiveActivationReceiptV1>&& receipt,
    std::string* error) noexcept {
    auto owned_stream =
        std::move(recovering_stream);
    auto owned_receipt = std::move(receipt);
    SetError(error, {});
    if (owned_stream == nullptr ||
        owned_receipt == nullptr) {
        SetError(
            error,
            "invalid recovered Raw ACTIVE promotion input");
        return nullptr;
    }
    if (!owned_stream->backend_
             ->PromoteToActive(
                 std::move(owned_receipt), error) ||
        !owned_stream
             ->active_binding_validated()) {
        if (error != nullptr && error->empty()) {
            SetError(
                error,
                "recovered Raw stream ACTIVE promotion failed");
        }
        return nullptr;
    }
    return std::unique_ptr<
        RawActiveBoundWalSinkV1>(
        std::move(owned_stream));
}

}  // namespace l2flow::ingress
