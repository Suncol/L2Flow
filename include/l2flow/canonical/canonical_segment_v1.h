#pragma once

#include "l2flow/canonical/canonical_schema_v1.h"
#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string_view>

namespace l2flow::canonical {

inline constexpr std::size_t kCanonicalSegmentHeaderBytesV1 = 4096U;
inline constexpr std::size_t kCanonicalSegmentControlBytesV1 = 4096U;
inline constexpr std::size_t kCanonicalSegmentDataOffsetV1 =
    kCanonicalSegmentHeaderBytesV1 + kCanonicalSegmentControlBytesV1;
inline constexpr std::size_t kCanonicalSegmentManifestBytesV1 = 4096U;

// Segment metadata is encoded field-by-field as little-endian bytes.  The
// Canonical records themselves use the direct-mmap V1 schema and therefore
// require a little-endian host at Create/Open time.
struct CanonicalSegmentDescriptorV1 final {
    CanonicalEventTypeV1 event_type = CanonicalEventTypeV1::kUnknown;
    std::uint32_t record_size = 0U;
    std::uint32_t source_stream_id = 0U;
    std::uint32_t shard = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t origin_capture_date = 0U;
    l2flow::common::Identity128 origin_stream_day_id{};
    // Exact feeder/Raw frontier namespace.  Canonical generation below is a
    // separate output-generation identity and must not be substituted for
    // either of these fields.
    l2flow::common::Identity128 origin_source_writer_instance{};
    std::uint64_t origin_source_generation = 0U;
    ClockEpochIdentityV1 clock_epoch{};
    l2flow::common::Sha256Digest schema_sha256{};
    l2flow::common::Sha256Digest dtype_sha256{};
    std::uint64_t registry_version = 0U;
    l2flow::common::Sha256Digest registry_sha256{};
    l2flow::common::Sha256Digest normalizer_build_sha256{};
    l2flow::common::Sha256Digest normalizer_config_sha256{};
    std::uint64_t generation = 0U;
    std::uint64_t segment_sequence = 0U;
    std::uint64_t capacity_records = 0U;
};

struct CanonicalSegmentCreateOptionsV1 final {
    std::filesystem::path segment_path{};
    std::filesystem::path manifest_path{};
    CanonicalSegmentDescriptorV1 descriptor{};
    std::int64_t created_realtime_ns = 0;
    std::int64_t created_monotonic_ns = 0;
};

struct CanonicalSegmentSealOptionsV1 final {
    std::int64_t closed_realtime_ns = 0;
    std::int64_t closed_monotonic_ns = 0;
};

struct CanonicalSegmentHeaderViewV1 final {
    CanonicalSegmentDescriptorV1 descriptor{};
    bool sealed = false;
    // Runtime-only latch for an open generation whose cross-family bundle
    // failed after physical publication.  Open files are discarded wholesale
    // on recovery; this latch additionally prevents the live writer from
    // sealing a locally consistent but globally uncommitted prefix.
    bool generation_fatal = false;
    std::uint64_t published_records = 0U;
    std::uint64_t first_shard_event_id = 0U;
    std::uint64_t last_shard_event_id = 0U;
    std::uint64_t first_origin_ingress_sequence = 0U;
    std::uint64_t last_origin_ingress_sequence = 0U;
    std::uint64_t first_origin_wal_end_pos = 0U;
    std::uint64_t last_origin_wal_end_pos = 0U;
    std::uint64_t processed_raw_ingress_sequence = 0U;
    std::uint64_t processed_raw_wal_pos = 0U;
    std::int64_t created_realtime_ns = 0;
    std::int64_t created_monotonic_ns = 0;
    std::int64_t closed_realtime_ns = 0;
    std::int64_t closed_monotonic_ns = 0;
    l2flow::common::Sha256Digest record_stream_sha256{};
    l2flow::common::Sha256Digest segment_integrity_sha256{};
};

// A coherent control-page observation.  safe publication depends on the
// seqlock generation and acquire/release ordering, not on notify_epoch.
struct CanonicalSegmentControlSnapshotV1 final {
    std::uint64_t published_records = 0U;
    std::uint64_t last_shard_event_id = 0U;
    std::uint64_t last_origin_wal_end_pos = 0U;
    std::uint64_t processed_raw_ingress_sequence = 0U;
    std::uint64_t processed_raw_wal_pos = 0U;
    std::uint64_t notify_epoch = 0U;
    bool closed = false;
    bool generation_fatal = false;
};

struct CanonicalSegmentSealResultV1 final {
    std::uint64_t published_records = 0U;
    l2flow::common::Sha256Digest record_stream_sha256{};
    l2flow::common::Sha256Digest segment_integrity_sha256{};
    l2flow::common::Sha256Digest segment_header_sha256{};
    l2flow::common::Sha256Digest manifest_sha256{};
};

struct CanonicalSegmentManifestViewV1 final {
    CanonicalSegmentHeaderViewV1 segment{};
    l2flow::common::Sha256Digest segment_header_sha256{};
    l2flow::common::Sha256Digest manifest_sha256{};
};

enum class CanonicalSegmentErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidArgument,
    kUnsupportedHostEndian,
    kInvalidDescriptor,
    kSchemaHashMismatch,
    kDtypeHashMismatch,
    kAlreadyExists,
    kOpenFailed,
    kWrongFileType,
    kWrongFileMode,
    kSingleWriterLockUnavailable,
    kSizeOverflow,
    kPreallocationFailed,
    kMapFailed,
    kIoError,
    kCorruptHeader,
    kHeaderIdentityMismatch,
    kCorruptControlPage,
    kControlBusy,
    kSegmentFull,
    kSegmentSealed,
    kGenerationFatal,
    kInvalidRecord,
    kRecordIdentityMismatch,
    kNonMonotonicCursor,
    kRecordNotPublished,
    kHashMismatch,
    kManifestConflict,
    kWaitError,
};

[[nodiscard]] std::string_view CanonicalSegmentErrorNameV1(
    CanonicalSegmentErrorV1 error) noexcept;

class CanonicalSegmentWriterV1 final {
public:
    ~CanonicalSegmentWriterV1();

    CanonicalSegmentWriterV1(const CanonicalSegmentWriterV1&) = delete;
    CanonicalSegmentWriterV1& operator=(const CanonicalSegmentWriterV1&) =
        delete;
    CanonicalSegmentWriterV1(CanonicalSegmentWriterV1&&) = delete;
    CanonicalSegmentWriterV1& operator=(CanonicalSegmentWriterV1&&) = delete;

    [[nodiscard]] static CanonicalSegmentErrorV1 Create(
        const CanonicalSegmentCreateOptionsV1& options,
        std::unique_ptr<CanonicalSegmentWriterV1>* writer);

    // PublishRecord/Advance/Seal are serialized by the owning single-writer
    // thread;
    // this object does not add an in-process mutex.  record_bytes must contain
    // exactly one complete record of the family in
    // descriptor().  The record is copied in full before the release
    // publication of published_records.  It deliberately does not advance
    // processed Raw progress: a multi-family bundle first publishes every
    // record, then commits normalizer state, then advances progress.
    [[nodiscard]] CanonicalSegmentErrorV1 PublishRecord(
        std::span<const std::byte> record_bytes) noexcept;

    // Allocation-free, non-mutating simulation of publishing this ordered
    // subset to the current segment.  It validates control coherence,
    // capacity, identity, event-ID continuity and Raw-origin monotonicity.
    [[nodiscard]] CanonicalSegmentErrorV1 PreflightRecords(
        std::span<const std::span<const std::byte>> records) const noexcept;

    [[nodiscard]] std::uint64_t RemainingRecords() const noexcept {
        return header_.descriptor.capacity_records -
               header_.published_records;
    }

    // Advances the Raw-consumption cursor without inventing a Canonical
    // business event (for example after a suppressed exact duplicate or a
    // poisoned scope).  These cursors are still sealed into the final header.
    // (0, positive_wal) is a valid day-start/header-only prefix.
    [[nodiscard]] CanonicalSegmentErrorV1 AdvanceProcessedRaw(
        std::uint64_t processed_raw_ingress_sequence,
        std::uint64_t processed_raw_wal_pos) noexcept;

    // Irreversible for this writer.  Used by the bundle coordinator after a
    // post-publication failure; subsequent Preflight/Publish/Advance/Seal
    // operations fail with kGenerationFatal.
    [[nodiscard]] CanonicalSegmentErrorV1 MarkGenerationFatal() noexcept;

    [[nodiscard]] CanonicalSegmentErrorV1 ReadControl(
        CanonicalSegmentControlSnapshotV1* snapshot) const noexcept;

    // Seal is deliberately local to this one segment.  It synchronizes the
    // final header and published record bytes, computes two domain-distinct
    // hashes, then publishes a fixed manifest via
    // tmp+fsync+RENAME_NOREPLACE+directory-fsync.  It does not prove that a
    // SourceFrontier is caught up or that sibling family/shard segments share
    // the same cursor; production generation cutover needs a separate
    // coordinated finalizer.  A manifest error never reopens the already
    // sealed segment.
    [[nodiscard]] CanonicalSegmentErrorV1 Seal(
        const CanonicalSegmentSealOptionsV1& options,
        CanonicalSegmentSealResultV1* result) noexcept;

    [[nodiscard]] const CanonicalSegmentDescriptorV1& descriptor()
        const noexcept {
        return header_.descriptor;
    }
    [[nodiscard]] const CanonicalSegmentHeaderViewV1& header() const noexcept {
        return header_;
    }

private:
    CanonicalSegmentWriterV1(
        int file_descriptor,
        void* mapping,
        std::size_t mapping_bytes,
        std::filesystem::path manifest_path,
        CanonicalSegmentHeaderViewV1 header) noexcept;

    int file_descriptor_ = -1;
    void* mapping_ = nullptr;
    std::size_t mapping_bytes_ = 0U;
    std::filesystem::path manifest_path_{};
    CanonicalSegmentHeaderViewV1 header_{};
};

class CanonicalSegmentReaderV1 final {
public:
    ~CanonicalSegmentReaderV1();

    CanonicalSegmentReaderV1(const CanonicalSegmentReaderV1&) = delete;
    CanonicalSegmentReaderV1& operator=(const CanonicalSegmentReaderV1&) =
        delete;
    CanonicalSegmentReaderV1(CanonicalSegmentReaderV1&&) = delete;
    CanonicalSegmentReaderV1& operator=(CanonicalSegmentReaderV1&&) = delete;

    // expected is mandatory: attach never accepts a merely self-consistent
    // segment from a different schema/registry/clock/generation namespace.
    [[nodiscard]] static CanonicalSegmentErrorV1 Open(
        const std::filesystem::path& segment_path,
        const CanonicalSegmentDescriptorV1& expected,
        std::unique_ptr<CanonicalSegmentReaderV1>* reader);

    [[nodiscard]] CanonicalSegmentErrorV1 ReadControl(
        CanonicalSegmentControlSnapshotV1* snapshot) const noexcept;

    // Physical/debug visibility only.  The returned view remains valid only
    // while this reader lives.  Bounds are checked against one acquire
    // control snapshot; bytes outside the published prefix are never exposed.
    // Factor/mux consumers must additionally pass the SourceFrontier
    // processed-prefix gate (ReadCommittedCanonicalRecordV1).
    [[nodiscard]] CanonicalSegmentErrorV1 PublishedRecord(
        std::uint64_t record_index,
        std::span<const std::byte>* record_bytes) const noexcept;

    // Linux futex is only a latency hint.  kNone includes timeout, wake and
    // value-changed-before-wait; callers must always poll ReadControl again.
    [[nodiscard]] CanonicalSegmentErrorV1 WaitForChange(
        std::uint64_t observed_notify_epoch,
        std::uint32_t timeout_milliseconds) const noexcept;

    [[nodiscard]] const CanonicalSegmentHeaderViewV1& header() const noexcept {
        return header_;
    }

private:
    CanonicalSegmentReaderV1(
        int file_descriptor,
        void* mapping,
        std::size_t mapping_bytes,
        CanonicalSegmentHeaderViewV1 header) noexcept;

    int file_descriptor_ = -1;
    void* mapping_ = nullptr;
    std::size_t mapping_bytes_ = 0U;
    CanonicalSegmentHeaderViewV1 header_{};
};

enum class CanonicalSegmentRecoveryDispositionV1 : std::uint8_t {
    // The segment header and both segment hashes validate.  Reuse still
    // requires ReadCanonicalSegmentManifestV1 at the caller's manifest path.
    kSealedHeaderAndHashesValid = 1U,
    // The whole generation must be regenerated from the last trusted sealed
    // cursor.  No published count from the volatile control page is resumed.
    kUnsealedDiscardWholeGeneration = 2U,
};

// Read-only inspection: this function never truncates, unlinks, renames or
// repairs.  A valid open segment always produces the whole-generation discard
// disposition above.
[[nodiscard]] CanonicalSegmentErrorV1 InspectCanonicalSegmentForRecoveryV1(
    const std::filesystem::path& segment_path,
    const CanonicalSegmentDescriptorV1& expected,
    CanonicalSegmentRecoveryDispositionV1* disposition,
    CanonicalSegmentHeaderViewV1* header);

[[nodiscard]] CanonicalSegmentErrorV1 ReadCanonicalSegmentManifestV1(
    const std::filesystem::path& manifest_path,
    const CanonicalSegmentHeaderViewV1& expected_segment,
    CanonicalSegmentManifestViewV1* manifest);

}  // namespace l2flow::canonical
