#include "l2flow/ingress/raw_clean_stop_gate.h"

#include "l2flow/common/identity128.h"
#include "l2flow/ingress/raw_reserve_authorized_wal.h"
#include "l2flow/ingress/raw_manifest_store.h"
#include "l2flow/ingress/raw_namespace.h"
#include "l2flow/ingress/raw_sealed_certificate_store.h"
#include "l2flow/ingress/raw_sealed_certificate_v1.h"
#include "l2flow/ingress/raw_v1.h"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace l2flow::ingress {
namespace {

class LeaseTargetProvider final
    : public RawReserveMutationTargetProviderV1 {
public:
    explicit LeaseTargetProvider(int descriptor) noexcept
        : descriptor_(descriptor) {}

    [[nodiscard]] int
    RawReserveMutationTargetDirectoryDescriptorV1()
        const noexcept override {
        return descriptor_;
    }

private:
    int descriptor_ = -1;
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

[[nodiscard]] bool SameStableFile(
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

[[nodiscard]] bool PreadExact(
    int descriptor,
    std::uint64_t offset,
    std::span<std::byte> output) noexcept {
    std::size_t completed = 0U;
    while (completed < output.size()) {
        if (offset >
                static_cast<std::uint64_t>(
                    std::numeric_limits<off_t>::max()) ||
            completed >
                static_cast<std::size_t>(
                    std::numeric_limits<off_t>::max()) ||
            offset >
                static_cast<std::uint64_t>(
                    std::numeric_limits<off_t>::max()) -
                    static_cast<std::uint64_t>(
                        completed)) {
            errno = EOVERFLOW;
            return false;
        }
        const off_t current =
            static_cast<off_t>(
                offset +
                static_cast<std::uint64_t>(
                    completed));
        const ssize_t result = ::pread(
            descriptor,
            output.data() +
                static_cast<std::ptrdiff_t>(completed),
            output.size() - completed,
            current);
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

struct JournalEvidence final {
    RawV1JournalHeaderWire header{};
    RawV1DurableMarkerWire last_marker{};
};

[[nodiscard]] bool LoadJournalEvidence(
    const RawWriterLease& lease,
    int retained_journal_fd,
    const RawIngressCleanStopEvidenceV1& evidence,
    JournalEvidence* output,
    std::string* diagnostic) noexcept {
    if (output == nullptr ||
        evidence.final_wal.journal_logical_size <
            kRawV1JournalHeaderBytes +
                kRawV1DurableMarkerBytes ||
        (evidence.final_wal.journal_logical_size -
         kRawV1JournalHeaderBytes) %
                kRawV1DurableMarkerBytes !=
            0U ||
        evidence.final_wal.journal_logical_size >
            static_cast<std::uint64_t>(
                std::numeric_limits<off_t>::max())) {
        SetError(
            diagnostic,
            "terminal Raw journal size is invalid");
        return false;
    }

    struct stat directory_status {};
    if (::fstat(
            lease.directory_descriptor(),
            &directory_status) != 0 ||
        !S_ISDIR(directory_status.st_mode) ||
        directory_status.st_uid != ::geteuid() ||
        (directory_status.st_mode & 07777) != 0700) {
        SetError(
            diagnostic,
            "writer lease directory is no longer private");
        return false;
    }

    if (retained_journal_fd < 0) {
        SetError(
            diagnostic,
            "terminal Raw journal descriptor is unavailable");
        return false;
    }

    struct stat before {};
    struct stat named {};
    const int status_flags =
        ::fcntl(retained_journal_fd, F_GETFL);
    const int descriptor_flags =
        ::fcntl(retained_journal_fd, F_GETFD);
    if (::fstat(retained_journal_fd, &before) != 0 ||
        ::fstatat(
            lease.directory_descriptor(),
            kRawJournalFilename,
            &named,
            AT_SYMLINK_NOFOLLOW) != 0 ||
        status_flags < 0 ||
        descriptor_flags < 0 ||
        !S_ISREG(before.st_mode) ||
        !S_ISREG(named.st_mode) ||
        before.st_uid != ::geteuid() ||
        (before.st_mode & 07777) != 0600 ||
        before.st_nlink != 1 ||
        before.st_dev != directory_status.st_dev ||
        before.st_dev != named.st_dev ||
        before.st_ino != named.st_ino ||
        before.st_size !=
            static_cast<off_t>(
                evidence.final_wal
                    .journal_logical_size) ||
        (status_flags & O_ACCMODE) != O_RDONLY ||
        (status_flags & O_APPEND) != 0 ||
        (status_flags & O_NOATIME) == 0 ||
        (descriptor_flags & FD_CLOEXEC) == 0) {
        SetError(
            diagnostic,
            "terminal Raw journal inode is unsafe");
        return false;
    }

    JournalEvidence candidate{};
    const std::uint64_t marker_offset =
        evidence.final_wal.journal_logical_size -
        kRawV1DurableMarkerBytes;
    if (!PreadExact(
            retained_journal_fd,
            0U,
            candidate.header) ||
        !PreadExact(
            retained_journal_fd,
            marker_offset,
            candidate.last_marker)) {
        SetError(
            diagnostic,
            "cannot read exact terminal Raw journal evidence");
        return false;
    }

    struct stat after {};
    struct stat named_after {};
    if (::fstat(retained_journal_fd, &after) != 0 ||
        ::fstatat(
            lease.directory_descriptor(),
            kRawJournalFilename,
            &named_after,
            AT_SYMLINK_NOFOLLOW) != 0 ||
        !SameStableFile(before, after) ||
        after.st_dev != named_after.st_dev ||
        after.st_ino != named_after.st_ino ||
        after.st_size != named_after.st_size) {
        SetError(
            diagnostic,
            "terminal Raw journal changed during validation");
        return false;
    }

    DurableJournalHeaderV1 header{};
    DurableMarkerV1 marker{};
    RawV1JournalHeaderWire canonical_header{};
    RawV1DurableMarkerWire canonical_marker{};
    if (DecodeDurableJournalHeaderV1(
            candidate.header,
            &header) != RawV1Error::kNone ||
        EncodeDurableJournalHeaderV1(
            header,
            &canonical_header) != RawV1Error::kNone ||
        canonical_header != candidate.header ||
        DecodeDurableMarkerV1(
            candidate.last_marker,
            &marker) != RawV1Error::kNone ||
        EncodeDurableMarkerV1(
            marker,
            &canonical_marker) != RawV1Error::kNone ||
        canonical_marker != candidate.last_marker ||
        header.source_stream_id !=
            evidence.final_sink_identity
                .source_stream_id ||
        header.capture_date !=
            evidence.final_sink_identity.capture_date ||
        header.stream_day_id !=
            evidence.final_sink_identity.stream_day_id ||
        marker.source_stream_id !=
            evidence.final_sink_identity
                .source_stream_id ||
        marker.segment_sequence !=
            evidence.final_sink_identity
                .segment_sequence ||
        marker.marker_flags != kRawV1SegmentSealed ||
        marker.durable_global_wal_pos !=
            evidence.final_wal.durable
                .global_wal_pos ||
        marker.durable_ingress_sequence !=
            evidence.final_wal.durable
                .ingress_sequence ||
        marker.durable_segment_offset !=
            evidence.final_wal.durable
                .segment_offset) {
        SetError(
            diagnostic,
            "terminal Raw journal evidence disagrees with the clean-stop frontier");
        return false;
    }

    *output = candidate;
    return true;
}

[[nodiscard]] int OpenRetainedJournalForGate(
    const RawWriterLease& lease,
    std::string* error) noexcept {
    struct stat directory_status {};
    if (lease.directory_descriptor() < 0 ||
        ::fstat(
            lease.directory_descriptor(),
            &directory_status) != 0 ||
        !S_ISDIR(directory_status.st_mode) ||
        directory_status.st_uid != ::geteuid() ||
        (directory_status.st_mode & 07777) != 0700) {
        SetError(
            error,
            "writer lease directory is no longer private");
        return -1;
    }

    int descriptor = -1;
    do {
        descriptor = ::openat(
            lease.directory_descriptor(),
            kRawJournalFilename,
            O_RDONLY | O_NOFOLLOW | O_NONBLOCK |
                O_CLOEXEC | O_NOATIME);
    } while (descriptor < 0 && errno == EINTR);
    if (descriptor < 0) {
        SetError(
            error,
            "cannot retain the canonical Raw journal");
        return -1;
    }

    struct stat opened {};
    struct stat named {};
    const int status_flags =
        ::fcntl(descriptor, F_GETFL);
    const int descriptor_flags =
        ::fcntl(descriptor, F_GETFD);
    if (::fstat(descriptor, &opened) != 0 ||
        ::fstatat(
            lease.directory_descriptor(),
            kRawJournalFilename,
            &named,
            AT_SYMLINK_NOFOLLOW) != 0 ||
        status_flags < 0 ||
        descriptor_flags < 0 ||
        !S_ISREG(opened.st_mode) ||
        !S_ISREG(named.st_mode) ||
        opened.st_uid != ::geteuid() ||
        (opened.st_mode & 07777) != 0600 ||
        opened.st_nlink != 1 ||
        opened.st_dev != directory_status.st_dev ||
        !SameStableFile(opened, named) ||
        opened.st_size <
            static_cast<off_t>(
                kRawV1JournalHeaderBytes +
                kRawV1DurableMarkerBytes) ||
        (status_flags & O_ACCMODE) != O_RDONLY ||
        (status_flags & O_APPEND) != 0 ||
        (status_flags & O_NOATIME) == 0 ||
        (descriptor_flags & FD_CLOEXEC) == 0) {
        static_cast<void>(::close(descriptor));
        SetError(
            error,
            "canonical Raw journal inode is unsafe");
        return -1;
    }
    return descriptor;
}

[[nodiscard]] bool EvidenceMatchesKey(
    const RawIngressCleanStopEvidenceV1& evidence,
    const RawReserveRegistryEntryKeyV1& key) noexcept {
    return evidence.exact() &&
           evidence.final_wal.initialized &&
           evidence.final_wal.sealed &&
           evidence.final_wal.closed &&
           !evidence.final_wal.fatal &&
           evidence.final_wal.append ==
               evidence.final_wal.durable &&
           evidence.final_sink_identity
                   .source_stream_id ==
               key.route.source_stream_id &&
           evidence.final_sink_identity.capture_date ==
               key.route.capture_date &&
           evidence.final_sink_identity.stream_day_id ==
               key.stream_day_id &&
           evidence.started_runtime.source_stream_id ==
               key.route.source_stream_id &&
           evidence.started_runtime.capture_date ==
               key.route.capture_date &&
           evidence.started_runtime.stream_day_id ==
               key.stream_day_id &&
           evidence.started_runtime.writer_instance ==
               evidence.final_sink_identity
                   .writer_instance &&
           !l2flow::common::IsZeroIdentity(
               evidence.final_sink_identity
                   .writer_instance);
}

}  // namespace

std::string_view RawCleanStopGateFailureV1Name(
    RawCleanStopGateFailureV1 failure) noexcept {
    switch (failure) {
        case RawCleanStopGateFailureV1::kNone:
            return "none";
        case RawCleanStopGateFailureV1::
            kInvalidConfiguration:
            return "invalid_configuration";
        case RawCleanStopGateFailureV1::kAlreadyFailed:
            return "already_failed";
        case RawCleanStopGateFailureV1::
            kEvidenceMismatch:
            return "evidence_mismatch";
        case RawCleanStopGateFailureV1::
            kAuthorizationRejected:
            return "authorization_rejected";
        case RawCleanStopGateFailureV1::kTargetMismatch:
            return "target_mismatch";
        case RawCleanStopGateFailureV1::
            kJournalEvidenceInvalid:
            return "journal_evidence_invalid";
        case RawCleanStopGateFailureV1::
            kManifestEvidenceInvalid:
            return "manifest_evidence_invalid";
        case RawCleanStopGateFailureV1::
            kCertificateBuildFailed:
            return "certificate_build_failed";
        case RawCleanStopGateFailureV1::
            kCertificatePublishFailed:
            return "certificate_publish_failed";
        case RawCleanStopGateFailureV1::
            kCoordinatorUnregisterFailed:
            return "coordinator_unregister_failed";
        case RawCleanStopGateFailureV1::
            kAllocationFailure:
            return "allocation_failure";
    }
    return "unknown";
}

RawPosixCleanStopGateV1::RawPosixCleanStopGateV1(
    RawWriterLease& lease,
    int retained_journal_fd,
    std::shared_ptr<
        RawReserveRegistryCoordinatorV1> coordinator,
    RawReserveRegistryEntryKeyV1 key,
    std::string stream_slug,
    std::size_t maximum_manifest_bytes) noexcept
    : lease_(lease),
      retained_journal_fd_(retained_journal_fd),
      coordinator_(std::move(coordinator)),
      key_(std::move(key)),
      stream_slug_(std::move(stream_slug)),
      maximum_manifest_bytes_(
          maximum_manifest_bytes) {}

RawPosixCleanStopGateV1::~RawPosixCleanStopGateV1() {
    if (retained_journal_fd_ >= 0) {
        static_cast<void>(
            ::close(retained_journal_fd_));
    }
}

void RawPosixCleanStopGateV1::Fail(
    RawCleanStopGateFailureV1 failure,
    std::string_view diagnostic) noexcept {
    failure_.store(
        static_cast<std::uint8_t>(failure),
        std::memory_order_release);
    try {
        diagnostic_.assign(
            diagnostic.data(), diagnostic.size());
    } catch (...) {
    }
}

std::string RawPosixCleanStopGateV1::diagnostic() const {
    const std::lock_guard<std::mutex> lock(mutex_);
    return diagnostic_;
}

bool RawPosixCleanStopGateV1::Complete(
    const RawIngressCleanStopEvidenceV1&
        evidence) noexcept {
    const std::lock_guard<std::mutex> lock(mutex_);
    if (completed_) {
        return true;
    }
    if (attempted_) {
        if (failure() ==
            RawCleanStopGateFailureV1::kNone) {
            Fail(
                RawCleanStopGateFailureV1::
                    kAlreadyFailed,
                "clean-stop gate was already attempted");
        }
        return false;
    }
    attempted_ = true;

    try {
    if (!EvidenceMatchesKey(evidence, key_)) {
        Fail(
            RawCleanStopGateFailureV1::
                kEvidenceMismatch,
            "clean-stop evidence does not match the registered Raw route");
        return false;
    }

    RawReserveCoordinatorErrorV1 action_failure =
        RawReserveCoordinatorErrorV1::kNone;
    std::string detail;
    std::unique_ptr<RawReserveAuthorizedActionV1>
        action =
            coordinator_->
                AcquireActionForExistingRoute(
                    key_,
                    ReserveRegistryStatusV1::kActive,
                    stream_slug_,
                    &action_failure,
                    &detail);
    if (action == nullptr) {
        Fail(
            RawCleanStopGateFailureV1::
                kAuthorizationRejected,
            detail.empty()
                ? std::string(
                      RawReserveCoordinatorErrorNameV1(
                          action_failure))
                : std::move(detail));
        return false;
    }
    if (action->token().writer_instance_id !=
            evidence.final_sink_identity
                .writer_instance ||
        action->token().recovery_attempt_id !=
            key_.recovery_attempt_id) {
        Fail(
            RawCleanStopGateFailureV1::
                kAuthorizationRejected,
            "ACTIVE action belongs to a different writer generation");
        return false;
    }
    const RawReserveMutationTargetAnchorV1*
        target = action->target();
    const LeaseTargetProvider provider(
        lease_.directory_descriptor());
    if (target == nullptr ||
        !ValidateRawReserveMutationTargetProviderV1(
            provider, *target)) {
        Fail(
            RawCleanStopGateFailureV1::
                kTargetMismatch,
            "writer lease is not bound to the authorized Raw route");
        return false;
    }

    JournalEvidence journal{};
    if (!LoadJournalEvidence(
            lease_,
            retained_journal_fd_,
            evidence,
            &journal,
            &detail) ||
        !action->ValidateLatest(&detail)) {
        Fail(
            RawCleanStopGateFailureV1::
                kJournalEvidenceInvalid,
            detail.empty()
                ? "terminal Raw journal validation failed"
                : std::move(detail));
        return false;
    }

    RawManifestNamespaceV1 expected_namespace{
        key_.route.capture_date,
        key_.route.source_stream_id,
        key_.stream_day_id};
    RawManifestV1 manifest{};
    RawManifestV1Error model_error =
        RawManifestV1Error::kNone;
    if (LoadCurrentRawManifestAt(
            lease_.directory_descriptor(),
            expected_namespace,
            maximum_manifest_bytes_,
            &manifest,
            nullptr,
            &model_error,
            &detail) != RawManifestStoreError::kNone ||
        manifest.open_entry.has_value() ||
        manifest.closed_entries.empty() ||
        manifest.closed_entries.back()
                .accepted_marker_bytes !=
            journal.last_marker ||
        !action->ValidateLatest(&detail)) {
        static_cast<void>(model_error);
        Fail(
            RawCleanStopGateFailureV1::
                kManifestEvidenceInvalid,
            detail.empty()
                ? "terminal closed manifest validation failed"
                : std::move(detail));
        return false;
    }

    std::unique_ptr<BuiltSealedRawCertificateV1>
        certificate;
    const SealedRawCertificateV1Error build_error =
        BuildSealedRawCertificateCapabilityV1(
            journal.header,
            manifest,
            evidence.final_sink_identity,
            evidence.final_wal,
            &certificate);
    if (build_error !=
            SealedRawCertificateV1Error::kNone ||
        certificate == nullptr) {
        Fail(
            RawCleanStopGateFailureV1::
                kCertificateBuildFailed,
            std::string(
                SealedRawCertificateV1ErrorName(
                    build_error)));
        return false;
    }

    SealedRawCertificatePublishResultV1
        publication =
            PublishSealedRawCertificateV1(
                lease_,
                std::move(action),
                *certificate,
                &detail);
    if (!publication.ok() ||
        publication.unregister_receipt == nullptr) {
        Fail(
            RawCleanStopGateFailureV1::
                kCertificatePublishFailed,
            detail.empty()
                ? std::string(
                      SealedRawCertificateStoreErrorV1Name(
                          publication.error))
                : std::move(detail));
        return false;
    }

    const RawReserveCoordinatorErrorV1
        unregister_error =
            coordinator_->UnregisterActive(
                std::move(
                    publication.unregister_receipt),
                &detail);
    if (unregister_error !=
        RawReserveCoordinatorErrorV1::kNone) {
        Fail(
            RawCleanStopGateFailureV1::
                kCoordinatorUnregisterFailed,
            detail.empty()
                ? std::string(
                      RawReserveCoordinatorErrorNameV1(
                          unregister_error))
                : std::move(detail));
        return false;
    }

    completed_ = true;
    failure_.store(
        static_cast<std::uint8_t>(
            RawCleanStopGateFailureV1::kNone),
        std::memory_order_release);
    diagnostic_.clear();
    return true;
    } catch (...) {
        failure_.store(
            static_cast<std::uint8_t>(
                RawCleanStopGateFailureV1::
                    kAllocationFailure),
            std::memory_order_release);
        diagnostic_.clear();
        return false;
    }
}

std::unique_ptr<RawPosixCleanStopGateV1>
CreateRawPosixCleanStopGateV1(
    RawWriterLease& lease,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawReserveRegistryEntryKeyV1 key,
    std::string_view stream_slug,
    std::size_t maximum_manifest_bytes,
    std::string* error) noexcept {
    SetError(error, {});
    if (lease.directory_descriptor() < 0 ||
        lease.source_stream_id() == 0U ||
        lease.capture_date() == 0U ||
        key.route.source_stream_id !=
            lease.source_stream_id() ||
        key.route.capture_date !=
            lease.capture_date() ||
        l2flow::common::IsZeroIdentity(
            key.stream_day_id) ||
        l2flow::common::IsZeroIdentity(
            key.recovery_attempt_id) ||
        stream_slug.empty() ||
        maximum_manifest_bytes == 0U) {
        SetError(
            error,
            "invalid POSIX Raw clean-stop gate configuration");
        return nullptr;
    }
    std::shared_ptr<RawReserveRegistryCoordinatorV1>
        retained = coordinator.Retain();
    if (retained == nullptr) {
        SetError(
            error,
            "Raw reserve coordinator lifetime is unavailable");
        return nullptr;
    }
    const int journal_fd =
        OpenRetainedJournalForGate(lease, error);
    if (journal_fd < 0) {
        return nullptr;
    }
    try {
        return std::unique_ptr<RawPosixCleanStopGateV1>(
            new RawPosixCleanStopGateV1(
                lease,
                journal_fd,
                std::move(retained),
                std::move(key),
                std::string(stream_slug),
                maximum_manifest_bytes));
    } catch (...) {
        static_cast<void>(::close(journal_fd));
        SetError(
            error,
            "cannot allocate POSIX Raw clean-stop gate");
        return nullptr;
    }
}

std::unique_ptr<RawPosixCleanStopGateV1>
CreateRawPosixCleanStopGateV1(
    RawActiveBoundWalSinkV1& active_sink,
    RawReserveRegistryCoordinatorV1& coordinator,
    RawReserveRegistryEntryKeyV1 key,
    std::string_view stream_slug,
    std::size_t maximum_manifest_bytes,
    std::string* error) noexcept {
    SetError(error, {});
    const RawWalSinkIdentityV1 sink_identity =
        active_sink.identity();
    if (!active_sink.active_binding_validated() ||
        active_sink.authorization_key() != key ||
        active_sink.authorized_writer_instance() !=
            sink_identity.writer_instance ||
        sink_identity.source_stream_id !=
            key.route.source_stream_id ||
        sink_identity.capture_date !=
            key.route.capture_date ||
        sink_identity.stream_day_id !=
            key.stream_day_id) {
        SetError(
            error,
            "active Raw sink does not match clean-stop route identity");
        return nullptr;
    }
    RawWriterLease& lease =
        active_sink.retained_writer_lease();
    if (lease.directory_descriptor() !=
        active_sink
            .RawReserveMutationTargetDirectoryDescriptorV1()) {
        SetError(
            error,
            "active Raw sink and writer lease target differ");
        return nullptr;
    }
    return CreateRawPosixCleanStopGateV1(
        lease,
        coordinator,
        std::move(key),
        stream_slug,
        maximum_manifest_bytes,
        error);
}

}  // namespace l2flow::ingress
