#include "l2flow/consumer/canonical_batch_reader_v1.h"

#include "l2flow/canonical/canonical_schema_v1.h"
#include "l2flow/control/raw_frontier_v1.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <limits>
#include <new>
#include <utility>

namespace l2flow::consumer {
namespace {

std::atomic<std::uint64_t> g_next_reader_cookie{1U};

[[nodiscard]] bool DigestNonzero(
    const l2flow::common::Sha256Digest& digest) noexcept {
    return std::any_of(
        digest.begin(), digest.end(),
        [](std::byte value) { return value != std::byte{0U}; });
}

[[nodiscard]] bool IdentityMatches(
    const l2flow::canonical::CanonicalSegmentDescriptorV1& descriptor,
    const l2flow::ingress::RawControlSnapshot& snapshot) noexcept {
    return snapshot.source_stream_id == descriptor.source_stream_id &&
           snapshot.capture_date == descriptor.origin_capture_date &&
           snapshot.stream_day_id == descriptor.origin_stream_day_id &&
           snapshot.writer_instance ==
               descriptor.origin_source_writer_instance;
}

[[nodiscard]] bool RawSnapshotCoherent(
    const l2flow::ingress::RawControlSnapshot& snapshot) noexcept {
    return snapshot.source_stream_id != 0U && snapshot.capture_date != 0U &&
           !l2flow::common::IsZeroIdentity(snapshot.stream_day_id) &&
           !l2flow::common::IsZeroIdentity(snapshot.writer_instance) &&
           snapshot.fatal_state == 0U &&
           l2flow::control::RawFrontierCursorShapeValidV1(snapshot);
}

[[nodiscard]] bool AttachMatches(
    const CanonicalConsumerAttachSpecV1& expected,
    const l2flow::canonical::CanonicalSegmentDescriptorV1& actual) noexcept {
    return expected.event_type !=
               l2flow::canonical::CanonicalEventTypeV1::kUnknown &&
           expected.record_size != 0U &&
           DigestNonzero(expected.schema_sha256) &&
           DigestNonzero(expected.dtype_sha256) &&
           expected.registry_version != 0U &&
           DigestNonzero(expected.registry_sha256) &&
           expected.event_type == actual.event_type &&
           expected.record_size == actual.record_size &&
           expected.schema_sha256 == actual.schema_sha256 &&
           expected.dtype_sha256 == actual.dtype_sha256 &&
           expected.registry_version == actual.registry_version &&
           expected.registry_sha256 == actual.registry_sha256;
}

[[nodiscard]] CanonicalBatchErrorV1 TranslateCommittedError(
    l2flow::canonical::CanonicalCommittedReadErrorV1 error) noexcept {
    return error ==
                   l2flow::canonical::CanonicalCommittedReadErrorV1::kNone
               ? CanonicalBatchErrorV1::kNone
               : CanonicalBatchErrorV1::kCommittedReadFailed;
}

}  // namespace

std::span<const std::byte> MdlBatchViewV1::record(
    std::size_t index) const noexcept {
    if (index >= record_count_ || record_size_ == 0U) {
        return {};
    }
    const std::size_t offset = index * record_size_;
    return records_bytes_.subspan(offset, record_size_);
}

std::string_view CanonicalBatchErrorNameV1(
    CanonicalBatchErrorV1 error) noexcept {
    switch (error) {
        case CanonicalBatchErrorV1::kNone:
            return "none";
        case CanonicalBatchErrorV1::kNullOutput:
            return "null_output";
        case CanonicalBatchErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case CanonicalBatchErrorV1::kAttachMismatch:
            return "attach_mismatch";
        case CanonicalBatchErrorV1::kCursorOutOfRange:
            return "cursor_out_of_range";
        case CanonicalBatchErrorV1::kWouldBlock:
            return "would_block";
        case CanonicalBatchErrorV1::kCommittedReadFailed:
            return "committed_read_failed";
        case CanonicalBatchErrorV1::kNonContiguousMapping:
            return "noncontiguous_mapping";
        case CanonicalBatchErrorV1::kRawDurabilityObservationFailed:
            return "raw_durability_observation_failed";
        case CanonicalBatchErrorV1::kRawNamespaceMismatch:
            return "raw_namespace_mismatch";
        case CanonicalBatchErrorV1::kRawAuthorityInvalid:
            return "raw_authority_invalid";
        case CanonicalBatchErrorV1::kInvalidBatch:
            return "invalid_batch";
        case CanonicalBatchErrorV1::kStaleBatch:
            return "stale_batch";
    }
    return "invalid_batch_error";
}

CanonicalCommittedBatchReaderV1::CanonicalCommittedBatchReaderV1(
    CanonicalBatchReaderConfigV1 config,
    std::uint64_t reader_cookie) noexcept
    : config_(std::move(config)),
      cursor_(config_.initial_canonical_cursor),
      reader_cookie_(reader_cookie) {}

CanonicalBatchErrorV1 CanonicalCommittedBatchReaderV1::Create(
    CanonicalBatchReaderConfigV1 config,
    std::unique_ptr<CanonicalCommittedBatchReaderV1>* output) noexcept {
    if (output == nullptr) {
        return CanonicalBatchErrorV1::kNullOutput;
    }
    output->reset();
    if (config.segment_reader == nullptr ||
        config.source_frontier == nullptr ||
        config.raw_durability_observer == nullptr) {
        return CanonicalBatchErrorV1::kInvalidConfiguration;
    }
    const auto& descriptor = config.segment_reader->header().descriptor;
    if (!AttachMatches(config.expected, descriptor)) {
        return CanonicalBatchErrorV1::kAttachMismatch;
    }
    if (config.initial_canonical_cursor > descriptor.capacity_records) {
        return CanonicalBatchErrorV1::kCursorOutOfRange;
    }
    const std::uint64_t reader_cookie =
        g_next_reader_cookie.fetch_add(1U, std::memory_order_relaxed);
    if (reader_cookie == 0U ||
        reader_cookie == std::numeric_limits<std::uint64_t>::max()) {
        return CanonicalBatchErrorV1::kInvalidConfiguration;
    }
    try {
        output->reset(new CanonicalCommittedBatchReaderV1(
            std::move(config), reader_cookie));
    } catch (const std::bad_alloc&) {
        return CanonicalBatchErrorV1::kInvalidConfiguration;
    } catch (...) {
        return CanonicalBatchErrorV1::kInvalidConfiguration;
    }
    return CanonicalBatchErrorV1::kNone;
}

CanonicalBatchErrorV1 CanonicalCommittedBatchReaderV1::Peek(
    std::size_t maximum_records,
    std::uint64_t watermark_set_id,
    MdlBatchViewV1* output) noexcept {
    if (output == nullptr) {
        return CanonicalBatchErrorV1::kNullOutput;
    }
    *output = MdlBatchViewV1{};
    if (maximum_records == 0U || watermark_set_id == 0U) {
        return CanonicalBatchErrorV1::kInvalidConfiguration;
    }
    const auto& reader = *config_.segment_reader;
    const auto& descriptor = reader.header().descriptor;
    l2flow::canonical::CanonicalSegmentControlSnapshotV1 control{};
    if (reader.ReadControl(&control) !=
        l2flow::canonical::CanonicalSegmentErrorV1::kNone) {
        return CanonicalBatchErrorV1::kCommittedReadFailed;
    }
    if (control.generation_fatal) {
        return CanonicalBatchErrorV1::kCommittedReadFailed;
    }
    if (cursor_ > control.published_records) {
        return CanonicalBatchErrorV1::kCursorOutOfRange;
    }
    const bool retrying_pending = pending_end_cursor_ != 0U;
    if (retrying_pending &&
        (pending_end_cursor_ <= cursor_ ||
         pending_watermark_set_id_ != watermark_set_id ||
         pending_end_cursor_ - cursor_ >
             static_cast<std::uint64_t>(maximum_records))) {
        return CanonicalBatchErrorV1::kInvalidBatch;
    }
    const std::uint64_t available = control.published_records - cursor_;
    const std::uint64_t requested = retrying_pending
        ? pending_end_cursor_ - cursor_
        : std::min<std::uint64_t>(
              available, static_cast<std::uint64_t>(maximum_records));
    if (requested == 0U) {
        return CanonicalBatchErrorV1::kWouldBlock;
    }

    const std::byte* first_pointer = nullptr;
    std::size_t accepted = 0U;
    std::uint64_t maximum_wal = 0U;
    std::uint64_t quality_flags = 0U;
    for (std::uint64_t ordinal = 0U; ordinal < requested; ++ordinal) {
        std::span<const std::byte> record{};
        l2flow::canonical::SourceFrontierV1 proof{};
        const auto committed_error =
            l2flow::canonical::ReadCommittedCanonicalRecordV1(
                *config_.source_frontier,
                reader,
                cursor_ + ordinal,
                &record,
                &proof);
        if (committed_error !=
            l2flow::canonical::CanonicalCommittedReadErrorV1::kNone) {
            if (committed_error ==
                    l2flow::canonical::CanonicalCommittedReadErrorV1::
                        kNotCommitted) {
                if (accepted == 0U) {
                    return CanonicalBatchErrorV1::kWouldBlock;
                }
                break;
            }
            return TranslateCommittedError(committed_error);
        }
        if (record.size() != descriptor.record_size) {
            return CanonicalBatchErrorV1::kCommittedReadFailed;
        }
        if (accepted == 0U) {
            first_pointer = record.data();
        } else {
            const std::size_t expected_offset =
                accepted * static_cast<std::size_t>(descriptor.record_size);
            if (record.data() != first_pointer + expected_offset) {
                return CanonicalBatchErrorV1::kNonContiguousMapping;
            }
        }
        l2flow::canonical::CanonicalHeaderV1 header{};
        std::memcpy(&header, record.data(), sizeof(header));
        maximum_wal = std::max(
            maximum_wal, header.origin_wal_end_pos);
        quality_flags |= header.quality_flags | proof.quality_flags;
        ++accepted;
    }
    if (accepted == 0U || first_pointer == nullptr) {
        return CanonicalBatchErrorV1::kWouldBlock;
    }
    if (accepted >
        std::numeric_limits<std::size_t>::max() /
            static_cast<std::size_t>(descriptor.record_size)) {
        return CanonicalBatchErrorV1::kInvalidBatch;
    }

    l2flow::ingress::RawControlSnapshot raw_snapshot{};
    if (!config_.raw_durability_observer(
            config_.raw_durability_observer_context, &raw_snapshot)) {
        return CanonicalBatchErrorV1::kRawDurabilityObservationFailed;
    }
    if (!IdentityMatches(descriptor, raw_snapshot)) {
        return CanonicalBatchErrorV1::kRawNamespaceMismatch;
    }
    if (!RawSnapshotCoherent(raw_snapshot)) {
        return CanonicalBatchErrorV1::kRawAuthorityInvalid;
    }

    MdlBatchViewV1 candidate{};
    candidate.mapping_owner_ = config_.segment_reader;
    candidate.records_bytes_ = std::span<const std::byte>(
        first_pointer,
        accepted * static_cast<std::size_t>(descriptor.record_size));
    candidate.record_count_ = accepted;
    candidate.record_size_ = descriptor.record_size;
    candidate.reader_cookie_ = reader_cookie_;
    MdlBatchMetadataV1& metadata = candidate.metadata_;
    metadata.source_stream_id = descriptor.source_stream_id;
    metadata.origin_capture_date = descriptor.origin_capture_date;
    metadata.trade_date = descriptor.trade_date;
    metadata.origin_stream_day_id = descriptor.origin_stream_day_id;
    metadata.family = descriptor.event_type;
    metadata.shard_id = descriptor.shard;
    metadata.begin_canonical_cursor = cursor_;
    metadata.end_canonical_cursor = cursor_ + accepted;
    metadata.max_consumed_origin_wal_end_pos = maximum_wal;
    metadata.observed_raw_durable_wal_pos =
        raw_snapshot.durable_global_wal_pos;
    metadata.clock_epoch = descriptor.clock_epoch;
    metadata.schema_sha256 = descriptor.schema_sha256;
    metadata.dtype_sha256 = descriptor.dtype_sha256;
    metadata.registry_version = descriptor.registry_version;
    metadata.registry_sha256 = descriptor.registry_sha256;
    metadata.batch_quality_flags = quality_flags;
    metadata.watermark_set_id = watermark_set_id;
    metadata.origin_source_writer_instance =
        descriptor.origin_source_writer_instance;
    metadata.origin_source_generation =
        descriptor.origin_source_generation;
    metadata.canonical_generation = descriptor.generation;
    if (!retrying_pending) {
        pending_end_cursor_ = metadata.end_canonical_cursor;
        pending_watermark_set_id_ = watermark_set_id;
    }
    *output = std::move(candidate);
    return CanonicalBatchErrorV1::kNone;
}

CanonicalBatchErrorV1 CanonicalCommittedBatchReaderV1::Commit(
    const MdlBatchViewV1& batch) noexcept {
    if (!batch.valid() || batch.reader_cookie_ != reader_cookie_ ||
        batch.mapping_owner_.get() != config_.segment_reader.get() ||
        batch.metadata_.begin_canonical_cursor != cursor_ ||
        batch.metadata_.end_canonical_cursor <= cursor_ ||
        batch.metadata_.end_canonical_cursor - cursor_ !=
            batch.record_count_) {
        return batch.metadata_.begin_canonical_cursor < cursor_
                   ? CanonicalBatchErrorV1::kStaleBatch
                   : CanonicalBatchErrorV1::kInvalidBatch;
    }
    std::span<const std::byte> tail{};
    l2flow::canonical::SourceFrontierV1 proof{};
    const auto committed_error =
        l2flow::canonical::ReadCommittedCanonicalRecordV1(
            *config_.source_frontier,
            *config_.segment_reader,
            batch.metadata_.end_canonical_cursor - 1U,
            &tail,
            &proof);
    if (committed_error !=
        l2flow::canonical::CanonicalCommittedReadErrorV1::kNone) {
        return TranslateCommittedError(committed_error);
    }
    if (proof.source_state != l2flow::canonical::SourceStateV1::kHealthy ||
        proof.writer_instance !=
            batch.metadata_.origin_source_writer_instance ||
        proof.generation != batch.metadata_.origin_source_generation ||
        proof.processed_global_wal_pos <
            batch.metadata_.max_consumed_origin_wal_end_pos) {
        return CanonicalBatchErrorV1::kCommittedReadFailed;
    }
    const std::span<const std::byte> expected_tail =
        batch.record(batch.record_count_ - 1U);
    if (tail.data() != expected_tail.data() ||
        tail.size() != expected_tail.size()) {
        return CanonicalBatchErrorV1::kInvalidBatch;
    }
    cursor_ = batch.metadata_.end_canonical_cursor;
    pending_end_cursor_ = 0U;
    pending_watermark_set_id_ = 0U;
    return CanonicalBatchErrorV1::kNone;
}

}  // namespace l2flow::consumer
