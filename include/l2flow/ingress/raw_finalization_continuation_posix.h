#pragma once

#include "l2flow/ingress/raw_emergency_writer_ack.h"
#include "l2flow/ingress/raw_manifest_v1.h"
#include "l2flow/ingress/raw_reserve_coordinator.h"
#include "l2flow/ingress/raw_segment_artifacts.h"
#include "l2flow/ingress/raw_writer_lease.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace l2flow::ingress {

class RawFinalizationContinuationTestPeerV1;

inline constexpr std::size_t
    kRawFinalizationContinuationDefaultMaximumManifestBytesV1 =
        16U * 1024U * 1024U;
inline constexpr std::size_t
    kRawFinalizationContinuationDefaultMaximumNamespaceEntriesV1 =
        4096U;

// The continuation receipt's generic object plan is interpreted by this
// capability-bound executor as follows:
//
//   object_sequence   exact next segment sequence
//   range_start       exact next segment_base_wal_pos
//   range_end_or_size exact bounded allocation size, including the header
//   causal_id         exact finalization_cycle_id
//
// No normal rotation API accepts this interpretation or the continuation
// segment flag.
enum class RawFinalizationContinuationMutationV1
    : std::uint8_t {
    kOldJournalTruncate = 0U,
    kOldJournalPrefixSync,
    kOldSegmentTruncate,
    kOldSegmentSync,
    kOldSealMarkerWrite,
    kOldSealJournalSync,
    kIndexTemporaryCreate,
    kIndexTemporaryWrite,
    kIndexTemporarySync,
    kIndexRename,
    kIndexDirectorySync,
    kClosedManifestTemporaryCreate,
    kClosedManifestTemporaryWrite,
    kClosedManifestTemporarySync,
    kClosedManifestRename,
    kClosedManifestDirectorySync,
    kContinuationTemporaryCreate,
    kContinuationHeaderWrite,
    kContinuationPreallocate,
    kContinuationFileSync,
    kContinuationRename,
    kContinuationDirectorySync,
    kHeaderMarkerWrite,
    kHeaderMarkerJournalSync,
    kOpenManifestTemporaryCreate,
    kOpenManifestTemporaryWrite,
    kOpenManifestTemporarySync,
    kOpenManifestRename,
    kOpenManifestDirectorySync,
    kContinuationRecordWrite,
    kContinuationRecordSync,
    kContinuationDurableMarkerWrite,
    kContinuationDurableMarkerJournalSync,
    kContinuationLogicalTruncate,
    kContinuationLogicalSync,
    kContinuationSealMarkerWrite,
    kContinuationSealJournalSync,
    kFrozenRingConsume,
};

// Return zero to admit the syscall. Returning an errno value injects a
// failure immediately before that one syscall, after ValidateLatest().
using RawFinalizationContinuationMutationHookV1 =
    int (*)(
        void* context,
        RawFinalizationContinuationMutationV1 mutation) noexcept;

using RawFinalizationContinuationClockNowV1 =
    std::uint64_t (*)(void* context) noexcept;

struct RawFinalizationContinuationPosixOptionsV1 final {
    RawSegmentArtifactOptionsV1 artifact_options{};
    std::size_t maximum_manifest_bytes =
        kRawFinalizationContinuationDefaultMaximumManifestBytesV1;
    std::size_t maximum_namespace_entries =
        kRawFinalizationContinuationDefaultMaximumNamespaceEntriesV1;

    RawFinalizationContinuationClockNowV1 realtime_now = nullptr;
    void* realtime_clock_context = nullptr;
    RawFinalizationContinuationClockNowV1 monotonic_now = nullptr;
    void* monotonic_clock_context = nullptr;

    RawFinalizationContinuationMutationHookV1 mutation_hook = nullptr;
    void* mutation_hook_context = nullptr;
};

enum class RawFinalizationContinuationFailureV1
    : std::uint8_t {
    kNone = 0U,
    kInvalidInput,
    kStaleAction,
    kWrongAction,
    kNoLiveAck,
    kAckMismatch,
    kStateLoad,
    kGrantMismatch,
    kCounterMismatch,
    kPlanMismatch,
    kUnsafeNamespace,
    kOldBoundaryMismatch,
    kContinuationNotRequired,
    kNamespaceConflict,
    kArtifactPlan,
    kArtifactPublish,
    kManifestLoad,
    kManifestTransition,
    kManifestPublish,
    kClockFailure,
    kAllocationFailure,
    kIoFailure,
};

[[nodiscard]] std::string_view
RawFinalizationContinuationFailureV1Name(
    RawFinalizationContinuationFailureV1 failure) noexcept;

enum class RawFinalizationContinuationDispositionV1
    : std::uint8_t {
    kNone = 0U,
    kPublishedNew,
    kAdoptedCompleteTemporary,
    kAdoptedPublishedOrphan,
};

enum class RawFinalizationContinuationCompletionModeV1
    : std::uint8_t {
    kLiveExactDrain = 0U,
    kReplacementSealOnly,
};

struct RawFinalizationContinuationRetainedInodeV1 final {
    std::uint64_t device = 0U;
    std::uint64_t inode = 0U;
    std::uint64_t size = 0U;
    // st_blocks converted to bytes with checked st_blocks * 512.
    std::uint64_t allocated_bytes = 0U;

    friend bool operator==(
        const RawFinalizationContinuationRetainedInodeV1&,
        const RawFinalizationContinuationRetainedInodeV1&) = default;
};

struct RawFinalizationContinuationAllocationObservationV1 final {
    std::uint64_t authorized_byte_cap = 0U;
    std::uint32_t authorized_inode_cap = 0U;
    std::uint64_t continuation_allocation_cap = 0U;
    std::uint64_t continuation_logical_size = 0U;
    std::uint64_t continuation_allocated_bytes = 0U;
    std::uint64_t old_segment_allocated_bytes_before = 0U;
    std::uint64_t old_segment_allocated_bytes_after = 0U;
    std::uint64_t journal_allocated_bytes_before = 0U;
    std::uint64_t journal_allocated_bytes_after = 0U;
    std::uint64_t manifest_allocated_bytes_before = 0U;
    std::uint64_t manifest_allocated_bytes_after = 0U;
    std::uint64_t conservative_positive_allocation_delta = 0U;

    friend bool operator==(
        const RawFinalizationContinuationAllocationObservationV1&,
        const RawFinalizationContinuationAllocationObservationV1&) =
        default;
};

// Durable proof returned by the executor rather than a caller-supplied bool.
// It owns the still-live DEBITED action and retained descriptors for every
// final name used by a later typed COMPLETE consumer. Public callers can
// inspect validity but cannot construct, copy, move, or mark it consumed.
class RawFinalizationContinuationReceiptV1 final {
public:
    ~RawFinalizationContinuationReceiptV1();

    RawFinalizationContinuationReceiptV1(
        const RawFinalizationContinuationReceiptV1&) = delete;
    RawFinalizationContinuationReceiptV1& operator=(
        const RawFinalizationContinuationReceiptV1&) = delete;
    RawFinalizationContinuationReceiptV1(
        RawFinalizationContinuationReceiptV1&&) = delete;
    RawFinalizationContinuationReceiptV1& operator=(
        RawFinalizationContinuationReceiptV1&&) = delete;

    [[nodiscard]] bool Validate(
        std::string* error = nullptr) const noexcept;
    [[nodiscard]] const SegmentHeaderV1&
    continuation_header() const noexcept {
        return continuation_header_;
    }
    [[nodiscard]] const RawWalWriterSnapshot&
    initialized_snapshot() const noexcept {
        return initialized_snapshot_;
    }
    [[nodiscard]] const RawWalWriterSnapshot&
    sealed_snapshot() const noexcept {
        return sealed_snapshot_;
    }
    [[nodiscard]] std::uint64_t
    frozen_record_count() const noexcept {
        return frozen_record_count_;
    }
    [[nodiscard]] std::uint64_t
    frozen_framed_wal_bytes() const noexcept {
        return frozen_framed_wal_bytes_;
    }
    [[nodiscard]] const RawWalCursor&
    final_cursor() const noexcept {
        return final_cursor_;
    }
    [[nodiscard]] const
        RawFinalizationContinuationAllocationObservationV1&
    allocation_observation() const noexcept {
        return allocation_observation_;
    }
    [[nodiscard]]
    RawFinalizationContinuationCompletionModeV1
    completion_mode() const noexcept {
        return completion_mode_;
    }
    [[nodiscard]] std::uint64_t
    recovered_record_count() const noexcept {
        return recovered_record_count_;
    }
    [[nodiscard]] std::uint64_t
    recovered_framed_wal_bytes() const noexcept {
        return recovered_framed_wal_bytes_;
    }

private:
    friend class RawFinalizationContinuationPosixV1;
    friend class RawReserveRegistryCoordinatorV1;
    friend class RawFinalizationContinuationTestPeerV1;

    RawFinalizationContinuationReceiptV1() = default;
    void CloseDescriptors() noexcept;
    [[nodiscard]] bool ValidateRetainedArtifacts(
        std::string* error = nullptr) const noexcept;

    std::unique_ptr<RawReserveFinalizationActionV1> action_;
    ReserveFinalizationActionKeyV1 key_{};
    RawReserveGenerationActionTokenV1 generation_token_{};
    ReserveStateV1Digest immutable_grant_sha256_{};
    FinalizationActionPlanV1 plan_{};
    std::uint8_t grant_flags_ = 0U;
    std::uint64_t byte_cap_ = 0U;
    std::uint32_t inode_cap_ = 0U;
    std::uint64_t debit_generation_ = 0U;
    ReserveStateV1Identity executor_instance_{};

    SegmentHeaderV1 continuation_header_{};
    RawV1SegmentHeaderWire continuation_header_wire_{};
    RawV1DurableMarkerWire header_marker_wire_{};
    RawV1Digest header_marker_sha256_{};
    RawV1DurableMarkerWire sealed_marker_wire_{};
    RawV1Digest sealed_marker_sha256_{};
    RawV1Digest sealed_segment_sha256_{};
    RawV1Digest sealed_journal_sha256_{};
    RawV1Digest open_manifest_commitment_sha256_{};
    RawV1Digest open_manifest_bytes_sha256_{};
    RawManifestV1 open_manifest_{};
    RawSealedSegmentMetadataV1 old_sealed_metadata_{};
    std::string old_segment_name_;
    std::string old_index_name_;
    RawWalWriterSnapshot initialized_snapshot_{};
    RawWalWriterSnapshot sealed_snapshot_{};
    std::uint64_t frozen_record_count_ = 0U;
    std::uint64_t frozen_framed_wal_bytes_ = 0U;
    RawWalCursor final_cursor_{};
    RawFinalizationContinuationAllocationObservationV1
        allocation_observation_{};
    RawFinalizationContinuationCompletionModeV1
        completion_mode_ =
            RawFinalizationContinuationCompletionModeV1::
                kLiveExactDrain;
    std::uint64_t recovered_record_count_ = 0U;
    std::uint64_t recovered_framed_wal_bytes_ = 0U;

    // Owns the exact paused Raw sink after the ring suffix has been consumed.
    // In production the sink owns the locked writer lease; retaining the
    // whole sink, rather than duplicating a process-scoped flock fd, keeps
    // that lease live through coordinator PublishNext + raw-root sync.
    std::unique_ptr<RawWalSink> retained_writer_;
    std::unique_ptr<RawWriterLease>
        replacement_writer_lease_;
    RawWalSinkIdentityV1 retained_writer_identity_{};
    RawWalWriterSnapshot retained_writer_snapshot_{};

    RawFinalizationContinuationRetainedInodeV1 route_inode_{};
    RawFinalizationContinuationRetainedInodeV1 raw_root_inode_{};
    RawFinalizationContinuationRetainedInodeV1 journal_inode_{};
    RawFinalizationContinuationRetainedInodeV1 old_segment_inode_{};
    RawFinalizationContinuationRetainedInodeV1 old_index_inode_{};
    RawFinalizationContinuationRetainedInodeV1 segment_inode_{};
    RawFinalizationContinuationRetainedInodeV1 manifest_inode_{};
    int route_fd_ = -1;
    int raw_root_fd_ = -1;
    int journal_fd_ = -1;
    int old_segment_fd_ = -1;
    int old_index_fd_ = -1;
    int segment_fd_ = -1;
    int manifest_fd_ = -1;
    bool consumed_ = false;
};

// A same-process continuation session. Begin() consumes both opaque
// capabilities, re-loads the exact durable grant while the action's shared
// generation gate is held, joins the paused regular writer without draining,
// and copies/validates the frozen ring suffix. It performs no filesystem
// mutation.
//
// The originating RawIngressApp must outlive an incomplete session. This is
// intentional: losing that process also loses the only proof that the ACKed
// ring suffix is still live, and CreateOrAdopt() then must not be retried by a
// replacement executor.
class RawFinalizationContinuationPosixV1 final {
public:
    ~RawFinalizationContinuationPosixV1();

    RawFinalizationContinuationPosixV1(
        const RawFinalizationContinuationPosixV1&) = delete;
    RawFinalizationContinuationPosixV1& operator=(
        const RawFinalizationContinuationPosixV1&) = delete;
    RawFinalizationContinuationPosixV1(
        RawFinalizationContinuationPosixV1&&) = delete;
    RawFinalizationContinuationPosixV1& operator=(
        RawFinalizationContinuationPosixV1&&) = delete;

    [[nodiscard]] static std::unique_ptr<
        RawFinalizationContinuationPosixV1>
    Begin(
        std::unique_ptr<
            RawReserveFinalizationActionV1>&& action,
        std::unique_ptr<
            RawEmergencyWriterAckV1>&& writer_ack,
        RawFinalizationContinuationPosixOptionsV1 options,
        RawFinalizationContinuationFailureV1* failure = nullptr,
        std::string* error = nullptr) noexcept;

    // A replacement for a crashed ACKED executor has no ring capability and
    // therefore may only adopt one pre-existing deterministic continuation
    // candidate. FENCED_NO_ACK grants cannot contain a continuation action or
    // a nonzero continuation cap and are rejected. Replacement never creates
    // the first continuation inode or invents missing records. A complete
    // recovered prefix may be sealed below the immutable queued suffix; the
    // receipt exposes both values for terminal gap reporting.
    [[nodiscard]] static std::unique_ptr<
        RawFinalizationContinuationPosixV1>
    BeginReplacementSealOnly(
        std::unique_ptr<
            RawReserveFinalizationActionV1>&& action,
        RawFinalizationContinuationPosixOptionsV1 options,
        RawFinalizationContinuationFailureV1* failure = nullptr,
        std::string* error = nullptr) noexcept;

    // Completes or adopts the exact continuation crash-prefix:
    //
    // continuation tmp/header/fallocate/file-sync/NOREPLACE/dir-sync ->
    // header-only marker/journal-sync -> open manifest file/dir barrier.
    //
    // Begin() has already required the immutable old-drain, journal, index
    // and closed-manifest predecessor receipts to be COMPLETE and has
    // read-only verified their exact durable artifacts. A CONTINUATION token
    // never re-executes those separately debited mutations.
    //
    // A hook-injected or ordinary transient syscall failure can be retried
    // on this same live session. No retry may create a second continuation.
    [[nodiscard]] bool CreateOrAdopt(
        std::string* error = nullptr) noexcept;

    // One-shot transfer of the live DEBITED action and all retained durable
    // evidence. Returning nullptr never completes the coordinator receipt.
    [[nodiscard]] std::unique_ptr<
        RawFinalizationContinuationReceiptV1>
    TakeReceipt(
        std::string* error = nullptr) noexcept;

    [[nodiscard]] RawFinalizationContinuationFailureV1
    failure() const noexcept {
        return failure_;
    }
    [[nodiscard]] int error_number() const noexcept {
        return error_number_;
    }
    [[nodiscard]] bool barrier_complete() const noexcept {
        return barrier_complete_;
    }
    [[nodiscard]]
    RawFinalizationContinuationDispositionV1
    disposition() const noexcept {
        return disposition_;
    }
    [[nodiscard]] const SegmentHeaderV1&
    continuation_header() const noexcept {
        return continuation_header_;
    }
    [[nodiscard]] const RawWalWriterSnapshot&
    initialized_snapshot() const noexcept {
        return initialized_snapshot_;
    }
    [[nodiscard]] const RawManifestV1*
    open_manifest() const noexcept {
        return barrier_complete_ ? &open_manifest_ : nullptr;
    }
    [[nodiscard]] std::uint64_t
    frozen_record_count() const noexcept {
        return ack_facts_.queued_record_count;
    }
    [[nodiscard]] std::uint64_t
    frozen_framed_wal_bytes() const noexcept {
        return ack_facts_.queued_framed_wal_bytes;
    }

private:
    class ArtifactIo;
    struct FrozenRecord;

    RawFinalizationContinuationPosixV1() = default;

    [[nodiscard]] bool Initialize(
        std::unique_ptr<
            RawReserveFinalizationActionV1>&& action,
        std::unique_ptr<
            RawEmergencyWriterAckV1>&& writer_ack,
        RawFinalizationContinuationPosixOptionsV1 options,
        std::string* error) noexcept;
    [[nodiscard]] bool InitializeReplacement(
        std::unique_ptr<
            RawReserveFinalizationActionV1>&& action,
        RawFinalizationContinuationPosixOptionsV1 options,
        std::string* error) noexcept;
    [[nodiscard]] bool ValidateLatest(
        std::string* error) noexcept;
    [[nodiscard]] bool BeforeMutation(
        RawFinalizationContinuationMutationV1 mutation,
        std::string* error) noexcept;
    [[nodiscard]] bool LiveSessionStillExact(
        std::string* error) noexcept;
    [[nodiscard]] bool LoadAndValidateGrant(
        std::string* error) noexcept;
    [[nodiscard]] bool LoadAndValidateReplacementGrant(
        std::string* error) noexcept;
    [[nodiscard]] bool OpenReplacementNamespace(
        std::string* error) noexcept;
    [[nodiscard]] bool LoadReplacementCandidate(
        std::string* error) noexcept;
    [[nodiscard]] bool RecoverReplacementPrefix(
        std::string* error) noexcept;
    [[nodiscard]] bool SealRecoveredPrefix(
        std::string* error) noexcept;
    [[nodiscard]] bool OpenAndValidateNamespace(
        std::string* error) noexcept;
    [[nodiscard]] bool CopyAndValidateFrozenRing(
        std::string* error) noexcept;
    [[nodiscard]] bool InspectContinuationCandidates(
        std::string* error) noexcept;
    [[nodiscard]] bool VerifyOldSegmentSealed(
        RawV1DurableMarkerWire* sealed_marker,
        std::uint64_t* sealed_journal_end,
        std::string* error) noexcept;
    [[nodiscard]] bool VerifyOldArtifactsAndClosedManifest(
        const RawV1DurableMarkerWire& sealed_marker,
        std::string* error) noexcept;
    [[nodiscard]] bool PublishOrAdoptContinuation(
        std::string* error) noexcept;
    [[nodiscard]] bool PublishHeaderMarker(
        std::uint64_t sealed_journal_end,
        std::string* error) noexcept;
    [[nodiscard]] bool PublishOpenManifest(
        std::string* error) noexcept;
    [[nodiscard]] bool DrainFrozenSuffixAndSeal(
        std::string* error) noexcept;
    [[nodiscard]] bool PublishManifestModel(
        const RawManifestV1& predecessor,
        const RawManifestV1& candidate,
        bool open_transition,
        std::string* error) noexcept;
    void Fail(
        RawFinalizationContinuationFailureV1 failure,
        int error_number,
        std::string* error,
        std::string message) noexcept;
    void CloseDescriptors() noexcept;

    std::unique_ptr<RawReserveFinalizationActionV1> action_;
    std::unique_ptr<RawWriterLease>
        replacement_writer_lease_;
    RawFinalizationContinuationPosixOptionsV1 options_{};
    RawEmergencyWriterAckFactsV1 ack_facts_{};
    RawIngressApp* owner_ = nullptr;
    RawWalSink* writer_ = nullptr;
    ByteRing* ring_ = nullptr;
    std::uint64_t ack_epoch_ = 0U;
    RawFinalizationContinuationCompletionModeV1
        completion_mode_ =
            RawFinalizationContinuationCompletionModeV1::
                kLiveExactDrain;
    ReserveAckStatusV1 grant_ack_status_ =
        ReserveAckStatusV1::kUnused;
    std::uint64_t recovered_record_count_ = 0U;
    std::uint64_t recovered_framed_wal_bytes_ = 0U;
    bool replacement_candidate_temporary_ = false;
    bool replacement_header_marker_present_ = false;
    bool replacement_manifest_open_present_ = false;
    bool replacement_candidate_sealed_ = false;

    std::unique_ptr<FrozenRecord[]> frozen_records_;
    std::unique_ptr<ByteRingRecord> ring_pop_record_;
    std::size_t frozen_record_count_ = 0U;

    SegmentHeaderV1 old_header_{};
    SegmentHeaderV1 continuation_header_{};
    RawV1SegmentHeaderWire continuation_header_wire_{};
    RawV1DurableMarkerWire header_marker_wire_{};
    RawV1DurableMarkerWire sealed_marker_wire_{};
    RawWalWriterSnapshot initialized_snapshot_{};
    RawWalWriterSnapshot sealed_snapshot_{};
    RawManifestV1 initial_manifest_{};
    RawManifestV1 closed_manifest_{};
    RawManifestV1 open_manifest_{};
    RawSegmentArtifactPlanV1 old_artifact_plan_{};

    std::uint64_t continuation_allocation_bytes_ = 0U;
    std::uint64_t allocation_quantum_bytes_ = 0U;
    std::uint64_t old_observed_file_size_ = 0U;
    std::uint64_t expected_old_journal_prefix_ = 0U;
    std::uint64_t expected_sealed_journal_end_ = 0U;
    std::uint64_t expected_header_marker_journal_end_ = 0U;
    bool target_existed_at_begin_ = false;
    std::uint64_t old_allocated_bytes_before_ = 0U;
    std::uint64_t journal_allocated_bytes_before_ = 0U;
    std::uint64_t manifest_allocated_bytes_before_ = 0U;

    int old_segment_fd_ = -1;
    int continuation_fd_ = -1;
    int journal_fd_ = -1;

    RawFinalizationContinuationFailureV1 failure_ =
        RawFinalizationContinuationFailureV1::kNone;
    int error_number_ = 0;
    RawFinalizationContinuationDispositionV1 disposition_ =
        RawFinalizationContinuationDispositionV1::kNone;
    bool frozen_suffix_consumed_ = false;
    bool barrier_complete_ = false;
};

}  // namespace l2flow::ingress
