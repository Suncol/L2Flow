#pragma once

#include "l2flow/canonical/canonical_bundle_runtime_v1.h"
#include "l2flow/ingress/raw_control_page.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

namespace l2flow::consumer {

// Exact external attach expectation.  A self-consistent segment descriptor is
// not enough: callers pin the schema, NumPy dtype and fixed record ABI they
// were compiled/configured to consume.
struct CanonicalConsumerAttachSpecV1 final {
    l2flow::canonical::CanonicalEventTypeV1 event_type =
        l2flow::canonical::CanonicalEventTypeV1::kUnknown;
    std::uint32_t record_size = 0U;
    l2flow::common::Sha256Digest schema_sha256{};
    l2flow::common::Sha256Digest dtype_sha256{};
    std::uint64_t registry_version = 0U;
    l2flow::common::Sha256Digest registry_sha256{};
};

// The observer must acquire one coherent Raw control snapshot and must not
// throw.  The batch reader independently checks its exact Raw namespace and
// writer identity before exposing the observed durable position as metadata.
using CanonicalBatchRawDurabilityObserverV1 = bool (*)(
    void* context,
    l2flow::ingress::RawControlSnapshot* snapshot) noexcept;

struct CanonicalBatchReaderConfigV1 final {
    std::shared_ptr<const l2flow::canonical::CanonicalSegmentReaderV1>
        segment_reader;
    // Borrowed live revocation authority.  It must outlive the batch reader.
    const l2flow::canonical::SourceFrontierPageV1* source_frontier = nullptr;
    CanonicalConsumerAttachSpecV1 expected{};
    std::uint64_t initial_canonical_cursor = 0U;
    CanonicalBatchRawDurabilityObserverV1 raw_durability_observer = nullptr;
    void* raw_durability_observer_context = nullptr;
};

struct MdlBatchMetadataV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t origin_capture_date = 0U;
    std::uint32_t trade_date = 0U;
    l2flow::common::Identity128 origin_stream_day_id{};
    l2flow::canonical::CanonicalEventTypeV1 family =
        l2flow::canonical::CanonicalEventTypeV1::kUnknown;
    std::uint32_t shard_id = 0U;
    // Both cursors are exclusive next-record indexes.  This batch consumes
    // [begin_canonical_cursor, end_canonical_cursor).
    std::uint64_t begin_canonical_cursor = 0U;
    std::uint64_t end_canonical_cursor = 0U;
    std::uint64_t max_consumed_origin_wal_end_pos = 0U;
    std::uint64_t observed_raw_durable_wal_pos = 0U;
    l2flow::canonical::ClockEpochIdentityV1 clock_epoch{};
    l2flow::common::Sha256Digest schema_sha256{};
    l2flow::common::Sha256Digest dtype_sha256{};
    std::uint64_t registry_version = 0U;
    l2flow::common::Sha256Digest registry_sha256{};
    std::uint64_t batch_quality_flags = 0U;
    std::uint64_t watermark_set_id = 0U;

    // Phase-5 authorization is generation-scoped and revocable.  These are
    // deliberately present even though the stable factor input-identity wire
    // has its own narrower, documented field set.
    l2flow::common::Identity128 origin_source_writer_instance{};
    std::uint64_t origin_source_generation = 0U;
    std::uint64_t canonical_generation = 0U;
};

// Read-only, zero-copy segment view.  The shared reader retains the mmap after
// the cursor-owning batch reader is destroyed.  Logical authorization remains
// point-in-time; Commit() rechecks the Phase-5 committed gate.
class MdlBatchViewV1 final {
public:
    MdlBatchViewV1() noexcept = default;

    [[nodiscard]] std::span<const std::byte> records_bytes() const noexcept {
        return records_bytes_;
    }
    [[nodiscard]] std::span<const std::byte> record(
        std::size_t index) const noexcept;
    [[nodiscard]] std::size_t record_count() const noexcept {
        return record_count_;
    }
    [[nodiscard]] std::uint32_t record_size() const noexcept {
        return record_size_;
    }
    [[nodiscard]] const MdlBatchMetadataV1& metadata() const noexcept {
        return metadata_;
    }
    [[nodiscard]] bool valid() const noexcept {
        return mapping_owner_ != nullptr && record_count_ != 0U &&
               !records_bytes_.empty();
    }

private:
    friend class CanonicalCommittedBatchReaderV1;

    std::shared_ptr<const l2flow::canonical::CanonicalSegmentReaderV1>
        mapping_owner_;
    std::span<const std::byte> records_bytes_{};
    MdlBatchMetadataV1 metadata_{};
    std::uint64_t reader_cookie_ = 0U;
    std::size_t record_count_ = 0U;
    std::uint32_t record_size_ = 0U;
};

enum class CanonicalBatchErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kAttachMismatch,
    kCursorOutOfRange,
    kWouldBlock,
    kCommittedReadFailed,
    kNonContiguousMapping,
    kRawDurabilityObservationFailed,
    kRawNamespaceMismatch,
    kRawAuthorityInvalid,
    kInvalidBatch,
    kStaleBatch,
};

[[nodiscard]] std::string_view CanonicalBatchErrorNameV1(
    CanonicalBatchErrorV1 error) noexcept;

class CanonicalCommittedBatchReaderV1 final {
public:
    CanonicalCommittedBatchReaderV1(
        const CanonicalCommittedBatchReaderV1&) = delete;
    CanonicalCommittedBatchReaderV1& operator=(
        const CanonicalCommittedBatchReaderV1&) = delete;
    CanonicalCommittedBatchReaderV1(
        CanonicalCommittedBatchReaderV1&&) = delete;
    CanonicalCommittedBatchReaderV1& operator=(
        CanonicalCommittedBatchReaderV1&&) = delete;
    ~CanonicalCommittedBatchReaderV1() = default;

    [[nodiscard]] static CanonicalBatchErrorV1 Create(
        CanonicalBatchReaderConfigV1 config,
        std::unique_ptr<CanonicalCommittedBatchReaderV1>* output) noexcept;

    // Peek never advances the logical cursor.  After the first successful
    // Peek, retries keep the exact [begin,end) range and watermark-set ID even
    // if additional records become committed.  Commit clears that pending
    // batch and permits the next range to be selected.
    [[nodiscard]] CanonicalBatchErrorV1 Peek(
        std::size_t maximum_records,
        std::uint64_t watermark_set_id,
        MdlBatchViewV1* output) noexcept;

    // Commit advances only the exact batch currently beginning at cursor().
    // It reuses the Phase-5 committed-record gate at the batch tail so a live
    // generation FATAL cannot be hidden by an earlier successful Peek.
    [[nodiscard]] CanonicalBatchErrorV1 Commit(
        const MdlBatchViewV1& batch) noexcept;

    [[nodiscard]] std::uint64_t cursor() const noexcept {
        return cursor_;
    }
    [[nodiscard]] const CanonicalConsumerAttachSpecV1& attach_spec()
        const noexcept {
        return config_.expected;
    }

private:
    CanonicalCommittedBatchReaderV1(
        CanonicalBatchReaderConfigV1 config,
        std::uint64_t reader_cookie) noexcept;

    CanonicalBatchReaderConfigV1 config_{};
    std::uint64_t cursor_ = 0U;
    std::uint64_t reader_cookie_ = 0U;
    std::uint64_t pending_end_cursor_ = 0U;
    std::uint64_t pending_watermark_set_id_ = 0U;
};

}  // namespace l2flow::consumer
