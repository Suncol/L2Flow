#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/ingress/capture_meta.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

namespace l2flow::ingress {

struct SegmentHeaderV1;

inline constexpr std::size_t kRawWalSegmentHeaderBytes = 4096U;
inline constexpr std::size_t kRawWalJournalHeaderBytes = 4096U;
inline constexpr std::size_t kRawWalRecordHeaderBytes = 96U;
inline constexpr std::size_t kRawWalRecordTrailerBytes = 16U;
inline constexpr std::size_t kRawWalDurableMarkerBytes = 48U;
inline constexpr std::uint32_t kRawWalSegmentSealedFlag = 0x00000001U;

enum class RawWalFile : std::uint8_t {
    kSegment = 0U,
    kJournal = 1U,
};

// One immutable buffer in a pwritev-style request. RawWalIo implementations
// must consume the vectors in order and report one aggregate byte count.
struct RawWalIoVector final {
    std::span<const std::byte> bytes;
};

struct RawWalWriteResult final {
    std::size_t bytes_written = 0U;
    int error_number = 0;
};

// Syscall-shaped seam for the single-writer state machine. Production
// implementations use explicit-offset pwritev; tests can deterministically
// inject EINTR, short/zero writes and sync/truncate/close failures.
class RawWalIo {
public:
    virtual ~RawWalIo() = default;

    // Optional bounded mutation scope. A fencing wrapper may retain one
    // already-validated shared generation action across several immediately
    // adjacent syscalls, but EndMutationBatch() must release it. Plain I/O
    // implementations need no special handling.
    [[nodiscard]] virtual bool BeginMutationBatch() noexcept {
        return true;
    }
    virtual void EndMutationBatch() noexcept {}

    [[nodiscard]] virtual RawWalWriteResult WritevSome(
        RawWalFile file,
        std::uint64_t offset,
        std::span<const RawWalIoVector> vectors) noexcept = 0;
    [[nodiscard]] virtual int Fdatasync(RawWalFile file) noexcept = 0;
    [[nodiscard]] virtual int Truncate(
        RawWalFile file,
        std::uint64_t logical_size) noexcept = 0;
    [[nodiscard]] virtual int Close(RawWalFile file) noexcept = 0;
};

enum class RawWalFailureKind : std::uint8_t {
    kNone = 0U,
    kInvalidState,
    kInvalidConfiguration,
    kInvalidRecord,
    kCursorOverflow,
    kSegmentWrite,
    kSegmentSync,
    kSegmentTruncate,
    kJournalWrite,
    kJournalSync,
    kClose,
    kCommitObserver,
    kRotationPolicy,
    kRotationArtifact,
    kRotationFactory,
    kManifestPublish,
    kControlPublish,
};

struct RawWalFailure final {
    RawWalFailureKind kind = RawWalFailureKind::kNone;
    int error_number = 0;
};

struct RawWalCursor final {
    std::uint64_t global_wal_pos = 0U;
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t segment_offset = 0U;

    [[nodiscard]] friend constexpr bool operator==(
        const RawWalCursor&,
        const RawWalCursor&) noexcept = default;
};

struct RawWalWriterSnapshot final {
    RawWalCursor append;
    RawWalCursor durable;
    // Complete bytes appended to the journal descriptor. This may lead the
    // durable cursor while a just-appended marker is awaiting fdatasync.
    std::uint64_t journal_logical_size = 0U;
    // These are monotonic state flags. A concurrent snapshot may
    // conservatively observe an older false value with newer cursor triples;
    // only each append triple and each durable triple are seqlock-coherent.
    bool initialized = false;
    bool sealed = false;
    bool closed = false;
    bool fatal = false;
};

struct RawWalSinkIdentityV1 final {
    l2flow::common::Identity128 writer_instance{};
    l2flow::common::Identity128 stream_day_id{};
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    std::uint32_t segment_sequence = 0U;
    std::uint64_t segment_base_wal_pos = 0U;
    std::uint64_t first_ingress_sequence = 0U;

    [[nodiscard]] friend constexpr bool operator==(
        const RawWalSinkIdentityV1&,
        const RawWalSinkIdentityV1&) noexcept = default;
};

enum class RawWalInitializationMode : std::uint8_t {
    // Create the first segment of a stream-day and write the journal header.
    kFreshJournal = 0U,
    // Attach a new normal segment to a caller-verified, already sealed
    // journal prefix. The journal header is never rewritten in this mode.
    kExistingJournal,
    // Attach the highest recovered open segment at an exact accepted
    // marker boundary solely to seal it. Initialize performs no write in
    // this mode, and AppendRecord is permanently forbidden. A normal next
    // segment must be created through kExistingJournal after the seal.
    kRecoveredSealOnly,
};

struct RawWalExistingJournalInit final {
    // Exclusive end of the caller-verified journal prefix. The value is
    // relative to the journal file and must be marker aligned.
    std::uint64_t journal_append_offset = 0U;
    std::uint32_t previous_segment_sequence = 0U;
    RawWalCursor previous_sealed_cursor{};
    std::uint32_t previous_marker_flags = 0U;
};

struct RawWalRecoveredOpenInit final {
    // Exclusive end of the recovery-validated journal prefix. It must end
    // immediately after the accepted non-sealed marker for this segment.
    std::uint64_t journal_append_offset = 0U;
    RawWalCursor recovered_cursor{};
    std::uint32_t accepted_marker_flags = 0U;
};

// Allocation-free writer-thread hook for incrementally maintained artifacts.
// A callback runs only after the corresponding bytes are complete (and, for
// the sealed marker, journal-synced). Returning false fail-stops the writer;
// the observer must outlive RawWalWriter. The byte spans are valid only for
// the duration of the call and must not be retained.
class RawWalCommitObserver {
public:
    virtual ~RawWalCommitObserver() = default;

    [[nodiscard]] virtual bool OnSegmentOpened(
        std::span<const std::byte> segment_header_wire) noexcept = 0;
    [[nodiscard]] virtual bool OnRecordCommitted(
        std::span<const std::byte> record_wire,
        std::uint64_t record_start_segment_offset,
        std::uint64_t record_start_global_wal_pos) noexcept = 0;
    [[nodiscard]] virtual bool OnSegmentSealed(
        std::span<const std::byte> accepted_marker_wire,
        const RawWalCursor& sealed_cursor) noexcept = 0;
};

// The fixed headers must have been emitted by the Raw V1 codec. The duplicated
// namespace/cursor fields are checked against those wire headers during
// Initialize(); they are never an alternative source of file-format truth.
struct RawWalWriterConfig final {
    std::array<std::byte, kRawWalSegmentHeaderBytes> segment_header_wire{};
    std::array<std::byte, kRawWalJournalHeaderBytes> journal_header_wire{};
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    std::uint32_t segment_sequence = 0U;
    std::uint64_t segment_base_wal_pos = 0U;
    std::uint64_t first_ingress_sequence = 0U;
    // For a fresh stream-day this is zero. For a rotated segment this is the
    // ingress sequence of the preceding SEGMENT_SEALED marker.
    std::uint64_t initial_durable_ingress_sequence = 0U;
    // Runtime capability identity, separate from the portable Raw header.
    // Production stream construction sets the exact ACTIVE writer instance.
    l2flow::common::Identity128 writer_instance{};
    RawWalInitializationMode initialization_mode =
        RawWalInitializationMode::kFreshJournal;
    RawWalExistingJournalInit existing_journal{};
    RawWalRecoveredOpenInit recovered_open{};
    // Secure namespace factories publish headers from typed temporary inodes
    // only after their file/parent barriers. Such final inodes must not be
    // rewritten during Initialize(), because a crash during a redundant
    // rewrite could destroy an already valid anchor/header. The caller is
    // responsible for retained-fd readback and identity validation before
    // setting this flag. The default keeps the syscall-level writer usable
    // with empty test/in-memory files.
    bool headers_already_persisted = false;
    // Optional incremental SHA/index observer. This is deliberately not an
    // ownership pointer and is never called from Snapshot().
    RawWalCommitObserver* commit_observer = nullptr;
};

enum class RawWalRotationError : std::uint8_t {
    kNone = 0U,
    kInvalidHeader,
    kFinalizationContinuationUnsupported,
    kSnapshotNotSealed,
    kSnapshotCursorMismatch,
    kJournalOffsetInvalid,
    kSegmentSequenceOverflow,
    kWalPositionOverflow,
    kIngressSequenceOverflow,
};

// A normal-rotation plan derived only from a fully sealed writer snapshot and
// its decoded immutable segment header. It does not create or mutate a file.
// In particular, it never manufactures a FINALIZATION_CONTINUATION segment.
struct RawWalRotationPlan final {
    RawWalRotationError error = RawWalRotationError::kNone;
    std::uint32_t next_segment_sequence = 0U;
    std::uint32_t next_segment_flags = 0U;
    std::uint64_t next_segment_base_wal_pos = 0U;
    std::uint64_t next_first_ingress_sequence = 0U;
    std::uint64_t initial_durable_ingress_sequence = 0U;
    RawWalExistingJournalInit existing_journal{};

    [[nodiscard]] bool ok() const noexcept {
        return error == RawWalRotationError::kNone;
    }
};

[[nodiscard]] RawWalRotationPlan PlanRawWalRotation(
    const SegmentHeaderV1& sealed_segment_header,
    const RawWalWriterSnapshot& sealed_snapshot) noexcept;

struct RawWalRecordInputV1 final {
    CaptureMetaV1 meta{};
    std::span<const std::byte> vendor_head;
    std::span<const std::byte> vendor_body;
};

// Stream-shaped sink used by the sole ring consumer. RawWalWriter implements
// one physical segment; RawWalStreamWriter composes multiple such segments
// behind the same contract without giving the ring a second consumer.
class RawWalSink {
public:
    virtual ~RawWalSink() = default;

    // The sole capture writer uses this scope only for a bounded number of
    // adjacent appends. Implementations without an external generation fence
    // may keep the default no-op behavior.
    [[nodiscard]] virtual bool BeginMutationBatch() noexcept {
        return true;
    }
    virtual void EndMutationBatch() noexcept {}

    [[nodiscard]] virtual bool AppendRecord(
        const RawWalRecordInputV1& input) noexcept = 0;
    [[nodiscard]] virtual bool FlushDurable() noexcept = 0;
    [[nodiscard]] virtual bool SealAndClose() noexcept = 0;
    [[nodiscard]] virtual RawWalWriterSnapshot
    Snapshot() const noexcept = 0;
    [[nodiscard]] virtual RawWalFailure
    failure() const noexcept = 0;
    [[nodiscard]] virtual RawWalSinkIdentityV1
    identity() const noexcept = 0;
};

// One instance owns exactly one segment descriptor and one stream-day journal
// descriptor. The factory is a prerequisite: it must secure-open exclusive
// non-O_APPEND descriptors, write only into unpublished typed temporary files
// where required, and perform publish/parent-directory barriers. This class
// deliberately does not open paths, acquire leases, preallocate, publish
// names, or fsync directories. Rotation is composed by sealing one instance,
// reopening the verified journal, and constructing the next instance.
//
// All mutating calls are made by one writer thread. Snapshot() is safe for
// concurrent observers and publishes internally coherent append and durable
// cursor triples only at complete record/marker boundaries.
class RawWalWriter final : public RawWalSink {
public:
    RawWalWriter(
        RawWalWriterConfig config,
        std::unique_ptr<RawWalIo> io);
    ~RawWalWriter();

    RawWalWriter(const RawWalWriter&) = delete;
    RawWalWriter& operator=(const RawWalWriter&) = delete;
    RawWalWriter(RawWalWriter&&) = delete;
    RawWalWriter& operator=(RawWalWriter&&) = delete;

    // Without a namespace-factory barrier, fresh mode writes/synchronizes the
    // journal anchor and both modes write/synchronize the segment header.
    // With headers_already_persisted, those redundant final-inode writes are
    // skipped. Fresh/existing-journal paths then append/synchronize the
    // header-only marker. kRecoveredSealOnly instead publishes only the
    // caller-validated recovered cursor and performs no write; its next
    // mutation must be SealAndClose(). No append/durable cursor is published
    // until every required barrier succeeds.
    [[nodiscard]] bool Initialize() noexcept;

    [[nodiscard]] bool BeginMutationBatch() noexcept override;
    void EndMutationBatch() noexcept override;

    // Encodes one Raw V1 record. Header/payload/padding are completed before a
    // separate final trailer write. Append advances only after that trailer is
    // complete.
    [[nodiscard]] bool AppendRecord(
        const RawWalRecordInputV1& input) noexcept override;

    // Makes all complete records through the current append boundary durable:
    // segment fdatasync -> marker append -> journal fdatasync -> publication.
    [[nodiscard]] bool FlushDurable() noexcept override;

    // Clean close truncates to the logical end, synchronizes the segment,
    // appends/synchronizes the authoritative SEGMENT_SEALED marker, and only
    // then closes both descriptors.
    [[nodiscard]] bool SealAndClose() noexcept override;

    [[nodiscard]] RawWalWriterSnapshot
    Snapshot() const noexcept override;
    [[nodiscard]] RawWalFailure
    failure() const noexcept override;
    [[nodiscard]] RawWalSinkIdentityV1
    identity() const noexcept override;

private:
    [[nodiscard]] bool ValidateConfiguration() noexcept;
    [[nodiscard]] bool WriteAll(
        RawWalFile file,
        std::uint64_t offset,
        std::span<const RawWalIoVector> vectors,
        RawWalFailureKind failure_kind) noexcept;
    [[nodiscard]] bool Sync(
        RawWalFile file,
        RawWalFailureKind failure_kind) noexcept;
    [[nodiscard]] bool TruncateSegment(
        std::uint64_t logical_size) noexcept;
    [[nodiscard]] bool AppendMarker(
        std::uint32_t marker_flags,
        bool publish_durable) noexcept;
    [[nodiscard]] bool CloseBoth() noexcept;
    void Trip(
        RawWalFailureKind kind,
        int error_number) noexcept;
    void PublishAppend(
        std::uint64_t segment_offset,
        std::uint64_t ingress_sequence) noexcept;
    void PublishDurable(
        std::uint64_t segment_offset,
        std::uint64_t ingress_sequence) noexcept;

    RawWalWriterConfig config_;
    std::unique_ptr<RawWalIo> io_;
    std::uint64_t logical_end_offset_ = 0U;
    std::uint64_t last_ingress_sequence_ = 0U;
    std::uint64_t journal_write_offset_ = 0U;
    bool mutation_batch_open_ = false;
    bool segment_closed_ = false;
    bool journal_closed_ = false;

    std::atomic<std::uint64_t> snapshot_generation_{0U};
    std::atomic<std::uint64_t> append_global_wal_pos_{0U};
    std::atomic<std::uint64_t> append_ingress_sequence_{0U};
    std::atomic<std::uint64_t> append_segment_offset_{0U};
    std::atomic<std::uint64_t> durable_global_wal_pos_{0U};
    std::atomic<std::uint64_t> durable_ingress_sequence_{0U};
    std::atomic<std::uint64_t> durable_segment_offset_{0U};
    std::atomic<std::uint64_t> journal_logical_size_{0U};
    std::atomic<bool> initialized_{false};
    std::atomic<bool> sealed_{false};
    std::atomic<bool> closed_{false};
    std::atomic<bool> fatal_{false};
    std::atomic<std::uint8_t> failure_kind_{
        static_cast<std::uint8_t>(RawWalFailureKind::kNone)};
    std::atomic<int> failure_errno_{0};
};

}  // namespace l2flow::ingress
