#pragma once

#include "l2flow/ingress/raw_recovery_executor.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/ingress/raw_segment_artifacts.h"
#include "l2flow/ingress/raw_wal_writer.h"
#include "l2flow/ingress/raw_writer_lease.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace l2flow::ingress {

inline constexpr std::uint32_t
    kRawPosixRecoveryAbsoluteMaxSegments = 100'000U;

// Limits are checked against fstat() sizes before any file-sized allocation.
// max_total_segment_bytes bounds the owned in-memory snapshot in addition to
// the per-segment bound. A journal may contain at most max_journal_markers
// complete marker slots plus one terminal partial marker.
struct RawPosixRecoveryLimitsV1 final {
    std::uint32_t max_segments =
        kRawPosixRecoveryAbsoluteMaxSegments;
    std::uint64_t max_segment_bytes =
        UINT64_C(4) * UINT64_C(1024) * UINT64_C(1024) *
        UINT64_C(1024);
    std::uint64_t max_total_segment_bytes =
        UINT64_C(8) * UINT64_C(1024) * UINT64_C(1024) *
        UINT64_C(1024);
    std::uint64_t max_journal_markers = UINT64_C(1'000'000);
};

struct RawPosixRecoveryFileIdentityV1 final {
    std::uint64_t device = 0U;
    std::uint64_t inode = 0U;

    friend bool operator==(
        const RawPosixRecoveryFileIdentityV1&,
        const RawPosixRecoveryFileIdentityV1&) = default;
};

enum class RawRecoveredSealedArtifactsFailureV1
    : std::uint8_t {
    kNone = 0U,
    kSessionUnavailable,
    kPlanDoesNotMatchSession,
    kNotCompletelySealed,
    kSchemaMismatch,
    kArtifactPlanFailure,
    kAllocationFailure,
};

[[nodiscard]] std::string_view
RawRecoveredSealedArtifactsFailureV1Name(
    RawRecoveredSealedArtifactsFailureV1 failure) noexcept;

// Read-only recovery result. artifact_plans retain the deterministic index
// bytes and the retained-fd snapshot evidence for a later coordinator-
// authorized publisher. existing_index_states reports whether each exact
// artifact is absent, already final, or a complete deterministic temporary.
// metadata is the same successful set projected in strictly increasing
// segment-sequence order for manifest reconstruction.
struct RawRecoveredSealedArtifactsV1 final {
    RawRecoveredSealedArtifactsFailureV1 failure =
        RawRecoveredSealedArtifactsFailureV1::kNone;
    RawSegmentArtifactFailureV1 artifact_failure =
        RawSegmentArtifactFailureV1::kNone;
    int error_number = 0;
    std::uint32_t evidence_segment_sequence = 0U;
    std::vector<RawSegmentArtifactPlanV1> artifact_plans;
    std::vector<RawSegmentArtifactExistingStateV1>
        existing_index_states;
    std::vector<RawSealedSegmentMetadataV1> metadata;

    [[nodiscard]] bool ok() const noexcept {
        return failure ==
               RawRecoveredSealedArtifactsFailureV1::kNone;
    }
};

// One-shot recovery session. LoadRawPosixRecoverySessionV1() duplicates and
// retains the writer lease, stream directory and every final Raw file, so the
// exclusive flock and inode identities remain stable even if the caller
// releases its RawWriterLease or pathnames are replaced later.
//
// input() is the immutable pre-mutation snapshot. Analyze it exactly once
// before executing a plan; after a successful mutation, load a new session
// before running another analysis.
//
// SyncParentDirectories() synchronizes the retained stream-day directory.
// The Raw-root and capture-date directory creation barriers are an explicit
// higher-layer precondition: they must already have been completed while
// their retained descriptors were held before this existing namespace is
// handed to the recovery session.
class RawPosixRecoverySessionV1 final
    : public RawRecoveryIo,
      public RawReserveMutationTargetProviderV1 {
public:
    ~RawPosixRecoverySessionV1() override;

    RawPosixRecoverySessionV1(
        const RawPosixRecoverySessionV1&) = delete;
    RawPosixRecoverySessionV1& operator=(
        const RawPosixRecoverySessionV1&) = delete;
    RawPosixRecoverySessionV1(
        RawPosixRecoverySessionV1&&) = delete;
    RawPosixRecoverySessionV1& operator=(
        RawPosixRecoverySessionV1&&) = delete;

    [[nodiscard]] const RawRecoveryInputV1& input() const noexcept {
        return input_;
    }
    [[nodiscard]] std::size_t segment_count() const noexcept;
    [[nodiscard]] RawPosixRecoveryFileIdentityV1
    journal_identity() const noexcept;
    [[nodiscard]] bool segment_identity(
        std::uint32_t segment_sequence,
        RawPosixRecoveryFileIdentityV1* identity) const noexcept;
    [[nodiscard]] int journal_open_flags() const noexcept;
    [[nodiscard]] int segment_open_flags(
        std::uint32_t segment_sequence) const noexcept;
    [[nodiscard]] int
    RawReserveMutationTargetDirectoryDescriptorV1()
        const noexcept override {
        return stream_directory_fd_;
    }

    // Transfers the recovery-validated highest open segment, durable journal,
    // retained writer lease and stream-directory descriptor into one owning
    // RawWalIo suitable for RawWalInitializationMode::kRecoveredSealOnly.
    //
    // The execution result must be the successful terminal result for plan.
    // Before transfer this method revalidates both final pathnames against the
    // retained inodes, owner/mode/nlink/open flags, exact post-recovery sizes,
    // the unchanged segment header, and the exact terminal non-sealed marker.
    // Ownership remains in this session on every failure and transfers exactly
    // once on success.
    [[nodiscard]] std::unique_ptr<RawWalIo>
    AdoptRecoveredSealIo(
        const RawRecoveryPlanV1& plan,
        const RawRecoveryExecutionResultV1& execution,
        std::string* error = nullptr) noexcept;

    // Re-analyzes this immutable session snapshot, requires the supplied plan
    // to match it exactly and every retained segment to be sealed, then reads
    // each retained final segment in full to deterministically rebuild and
    // self-validate RawIndexV1. This method performs no filesystem mutation.
    [[nodiscard]] RawRecoveredSealedArtifactsV1
    PrepareRecoveredSealedRawArtifacts(
        const RawRecoveryPlanV1& plan,
        RawSegmentArtifactOptionsV1 options) const noexcept;
    [[nodiscard]] bool
    DependentArtifactsAbsentProven() const noexcept override {
        return dependent_artifacts_absent_proven_;
    }
    [[nodiscard]] bool R11OrphanAdoptionAuthorized(
        const RawRecoveryPlanV1&) const noexcept override {
        return false;
    }

    [[nodiscard]] int SyncParentDirectories() noexcept override;
    [[nodiscard]] int TruncateJournal(
        std::uint64_t size) noexcept override;
    [[nodiscard]] int SyncJournal() noexcept override;
    [[nodiscard]] int TruncateSegment(
        std::uint32_t segment_sequence,
        std::uint64_t size) noexcept override;
    [[nodiscard]] int SyncSegment(
        std::uint32_t segment_sequence,
        bool include_allocation_metadata) noexcept override;
    [[nodiscard]] RawRecoveryWriteResult WriteJournalSome(
        std::uint64_t offset,
        std::span<const std::byte> bytes) noexcept override;

private:
    struct RetainedSegment;

    friend std::unique_ptr<RawPosixRecoverySessionV1>
    LoadRawPosixRecoverySessionV1(
        const RawWriterLease&,
        RawPosixRecoveryLimitsV1,
        std::string*) noexcept;

    RawPosixRecoverySessionV1(
        int lease_fd,
        int stream_directory_fd,
        int journal_fd,
        RawPosixRecoveryFileIdentityV1 journal_identity,
        RawRecoveryInputV1 input,
        std::unique_ptr<RetainedSegment[]> segments,
        std::size_t segment_count,
        bool dependent_artifacts_absent_proven) noexcept;

    [[nodiscard]] RetainedSegment* FindSegment(
        std::uint32_t segment_sequence) noexcept;
    [[nodiscard]] const RetainedSegment* FindSegment(
        std::uint32_t segment_sequence) const noexcept;

    int lease_fd_ = -1;
    int stream_directory_fd_ = -1;
    int journal_fd_ = -1;
    RawPosixRecoveryFileIdentityV1 journal_identity_{};
    RawRecoveryInputV1 input_{};
    std::unique_ptr<RetainedSegment[]> segments_;
    std::size_t segment_count_ = 0U;
    bool dependent_artifacts_absent_proven_ = false;
};

// Secure-opens only final durable.journal and final segment-%08u.raw names
// relative to the lease's retained stream directory. Files are opened
// O_RDWR|O_NOFOLLOW|O_NONBLOCK|O_CLOEXEC (never O_APPEND), then required to
// be service-EUID-owned, mode 0600, singly-linked regular files whose final
// pathname still names the opened inode. Segment names must be contiguous
// from sequence 1 and are bounded by both max_segments and the absolute
// 100,000-segment ceiling.
[[nodiscard]] std::unique_ptr<RawPosixRecoverySessionV1>
LoadRawPosixRecoverySessionV1(
    const RawWriterLease& lease,
    RawPosixRecoveryLimitsV1 limits = {},
    std::string* error = nullptr) noexcept;

}  // namespace l2flow::ingress
