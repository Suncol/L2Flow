#include "l2flow/ingress/raw_segment_accumulator.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <utility>

namespace l2flow::ingress {
namespace {

[[nodiscard]] bool IsZero(
    std::span<const std::byte> bytes) noexcept {
    return std::all_of(
        bytes.begin(),
        bytes.end(),
        [](std::byte value) {
            return value == std::byte{0};
        });
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

}  // namespace

std::string_view RawSegmentAccumulatorErrorV1Name(
    RawSegmentAccumulatorErrorV1 error) noexcept {
    switch (error) {
        case RawSegmentAccumulatorErrorV1::kNone:
            return "none";
        case RawSegmentAccumulatorErrorV1::kInvalidOptions:
            return "invalid_options";
        case RawSegmentAccumulatorErrorV1::kInvalidState:
            return "invalid_state";
        case RawSegmentAccumulatorErrorV1::
            kInvalidSegmentHeader:
            return "invalid_segment_header";
        case RawSegmentAccumulatorErrorV1::kSchemaMismatch:
            return "schema_mismatch";
        case RawSegmentAccumulatorErrorV1::
            kRecordCoordinateMismatch:
            return "record_coordinate_mismatch";
        case RawSegmentAccumulatorErrorV1::kInvalidRecord:
            return "invalid_record";
        case RawSegmentAccumulatorErrorV1::kIndexUpdate:
            return "index_update";
        case RawSegmentAccumulatorErrorV1::kHashUpdate:
            return "hash_update";
        case RawSegmentAccumulatorErrorV1::
            kInvalidSealMarker:
            return "invalid_seal_marker";
        case RawSegmentAccumulatorErrorV1::
            kSealCursorMismatch:
            return "seal_cursor_mismatch";
        case RawSegmentAccumulatorErrorV1::kHashFinalize:
            return "hash_finalize";
        case RawSegmentAccumulatorErrorV1::kIndexFinalize:
            return "index_finalize";
        case RawSegmentAccumulatorErrorV1::
            kIndexSelfValidation:
            return "index_self_validation";
    }
    return "unknown";
}

RawSegmentAccumulatorV1::RawSegmentAccumulatorV1(
    RawSegmentArtifactOptionsV1 options) noexcept
    : options_(std::move(options)) {
    if (IsZero(options_.expected_raw_schema_sha256) ||
        options_.sample_record_interval == 0U ||
        options_.sample_record_interval >
            kRawIndexV1DefaultRecordInterval ||
        options_.sample_raw_bytes_interval == 0U ||
        options_.sample_raw_bytes_interval >
            kRawIndexV1DefaultRawBytesInterval ||
        options_.maximum_segment_bytes <
            kRawV1SegmentHeaderBytes) {
        Trip(
            RawSegmentAccumulatorErrorV1::
                kInvalidOptions);
    }
}

bool RawSegmentAccumulatorV1::OnSegmentOpened(
    std::span<const std::byte>
        segment_header_wire) noexcept {
    if (error_ !=
            RawSegmentAccumulatorErrorV1::kNone ||
        opened_ || sealed_ || artifact_plan_taken_) {
        Trip(RawSegmentAccumulatorErrorV1::kInvalidState);
        return false;
    }
    SegmentHeaderV1 decoded{};
    if (DecodeSegmentHeaderV1(
            segment_header_wire,
            &decoded) != RawV1Error::kNone ||
        decoded.segment_flags != 0U ||
        segment_header_wire.size() !=
            kRawV1SegmentHeaderBytes) {
        Trip(
            RawSegmentAccumulatorErrorV1::
                kInvalidSegmentHeader);
        return false;
    }
    if (decoded.raw_schema_sha256 !=
        options_.expected_raw_schema_sha256) {
        Trip(
            RawSegmentAccumulatorErrorV1::
                kSchemaMismatch);
        return false;
    }

    RawIndexHeaderV1 index_header{};
    index_header.capture_date = decoded.capture_date;
    index_header.source_stream_id =
        decoded.source_stream_id;
    index_header.stream_day_id =
        decoded.stream_day_id;
    index_header.segment_sequence =
        decoded.segment_sequence;
    index_header.segment_base_wal_pos =
        decoded.segment_base_wal_pos;
    index_header.raw_schema_sha256 =
        decoded.raw_schema_sha256;
    index_header.sample_record_interval =
        options_.sample_record_interval;
    index_header.sample_raw_bytes_interval =
        options_.sample_raw_bytes_interval;
    index_builder_.emplace(index_header);
    if (index_builder_->error() !=
        RawIndexV1Error::kNone) {
        Trip(
            RawSegmentAccumulatorErrorV1::
                kInvalidSegmentHeader);
        return false;
    }
    if (!hasher_.Update(segment_header_wire)) {
        Trip(
            RawSegmentAccumulatorErrorV1::
                kHashUpdate);
        return false;
    }
    segment_ = decoded;
    opened_ = true;
    return true;
}

bool RawSegmentAccumulatorV1::OnRecordCommitted(
    std::span<const std::byte> record_wire,
    std::uint64_t record_start_segment_offset,
    std::uint64_t
        record_start_global_wal_pos) noexcept {
    if (error_ !=
            RawSegmentAccumulatorErrorV1::kNone ||
        !opened_ || sealed_ || artifact_plan_taken_ ||
        !index_builder_.has_value()) {
        Trip(RawSegmentAccumulatorErrorV1::kInvalidState);
        return false;
    }
    std::uint64_t expected_start_wal_pos = 0U;
    std::uint64_t record_end_wal_pos = 0U;
    std::uint64_t expected_ingress_sequence = 0U;
    if (record_start_segment_offset !=
            hasher_.total_bytes() ||
        !CheckedAdd(
            segment_.segment_base_wal_pos,
            record_start_segment_offset,
            &expected_start_wal_pos) ||
        expected_start_wal_pos !=
            record_start_global_wal_pos ||
        record_wire.size() >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint64_t>::max()) ||
        !CheckedAdd(
            record_start_global_wal_pos,
            static_cast<std::uint64_t>(
                record_wire.size()),
            &record_end_wal_pos) ||
        !CheckedAdd(
            segment_.first_ingress_sequence,
            record_count_,
            &expected_ingress_sequence) ||
        record_start_segment_offset >
            options_.maximum_segment_bytes ||
        static_cast<std::uint64_t>(
            record_wire.size()) >
            options_.maximum_segment_bytes -
                record_start_segment_offset) {
        Trip(
            RawSegmentAccumulatorErrorV1::
                kRecordCoordinateMismatch);
        return false;
    }

    const RawRecordNamespaceV1 expected_namespace{
        segment_.source_stream_id,
        segment_.capture_date};
    RawRecordViewV1 decoded{};
    if (DecodeRawRecordV1(
            record_wire,
            &decoded,
            &expected_namespace) !=
            RawV1Error::kNone ||
        decoded.header.ingress_sequence !=
            expected_ingress_sequence) {
        Trip(
            RawSegmentAccumulatorErrorV1::
                kInvalidRecord);
        return false;
    }

    RawIndexEntryV1 entry{};
    entry.ingress_sequence =
        decoded.header.ingress_sequence;
    entry.record_start_wal_pos =
        record_start_global_wal_pos;
    entry.record_end_wal_pos =
        record_end_wal_pos;
    entry.segment_file_offset =
        record_start_segment_offset;
    entry.vendor_sequence_id =
        decoded.header.vendor_sequence_id;
    entry.recv_monotonic_ns =
        decoded.header.recv_monotonic_ns;
    entry.connection_epoch_hint =
        decoded.header.connection_epoch_hint;
    entry.vendor_service_version =
        decoded.header.vendor_service_version;
    entry.vendor_message_id =
        decoded.header.vendor_message_id;
    entry.vendor_service_id =
        decoded.header.vendor_service_id;
    if (index_builder_->AddRecord(entry) !=
        RawIndexV1Error::kNone) {
        Trip(
            RawSegmentAccumulatorErrorV1::
                kIndexUpdate);
        return false;
    }
    if (!hasher_.Update(record_wire)) {
        Trip(
            RawSegmentAccumulatorErrorV1::
                kHashUpdate);
        return false;
    }

    if (!first_ingress_sequence_.has_value()) {
        first_ingress_sequence_ =
            decoded.header.ingress_sequence;
    }
    last_ingress_sequence_ =
        decoded.header.ingress_sequence;
    ++record_count_;
    return true;
}

bool RawSegmentAccumulatorV1::OnSegmentSealed(
    std::span<const std::byte>
        accepted_marker_wire,
    const RawWalCursor& sealed_cursor) noexcept {
    if (error_ !=
            RawSegmentAccumulatorErrorV1::kNone ||
        !opened_ || sealed_ || artifact_plan_taken_ ||
        !index_builder_.has_value()) {
        Trip(RawSegmentAccumulatorErrorV1::kInvalidState);
        return false;
    }
    DurableMarkerV1 marker{};
    if (accepted_marker_wire.size() !=
            kRawV1DurableMarkerBytes ||
        DecodeDurableMarkerV1(
            accepted_marker_wire,
            &marker) != RawV1Error::kNone ||
        marker.marker_flags !=
            kRawV1SegmentSealed) {
        Trip(
            RawSegmentAccumulatorErrorV1::
                kInvalidSealMarker);
        return false;
    }

    std::uint64_t expected_global_wal_pos = 0U;
    const std::uint64_t expected_ingress_sequence =
        last_ingress_sequence_.value_or(
            segment_.first_ingress_sequence - 1U);
    if (!CheckedAdd(
            segment_.segment_base_wal_pos,
            hasher_.total_bytes(),
            &expected_global_wal_pos) ||
        marker.source_stream_id !=
            segment_.source_stream_id ||
        marker.segment_sequence !=
            segment_.segment_sequence ||
        marker.durable_segment_offset !=
            hasher_.total_bytes() ||
        marker.durable_global_wal_pos !=
            expected_global_wal_pos ||
        marker.durable_ingress_sequence !=
            expected_ingress_sequence ||
        sealed_cursor.segment_offset !=
            marker.durable_segment_offset ||
        sealed_cursor.global_wal_pos !=
            marker.durable_global_wal_pos ||
        sealed_cursor.ingress_sequence !=
            marker.durable_ingress_sequence) {
        Trip(
            RawSegmentAccumulatorErrorV1::
                kSealCursorMismatch);
        return false;
    }

    RawV1Digest segment_sha256{};
    if (!hasher_.Finalize(&segment_sha256)) {
        Trip(
            RawSegmentAccumulatorErrorV1::
                kHashFinalize);
        return false;
    }
    RawV1DurableMarkerWire marker_wire{};
    std::copy(
        accepted_marker_wire.begin(),
        accepted_marker_wire.end(),
        marker_wire.begin());
    RawSealedSegmentMetadataV1 metadata{};
    metadata.segment = segment_;
    metadata.accepted_sealed_marker = marker;
    metadata.accepted_sealed_marker_bytes =
        marker_wire;
    metadata.segment_sha256 = segment_sha256;
    metadata.logical_end_offset =
        hasher_.total_bytes();
    metadata.record_count = record_count_;
    metadata.actual_first_ingress_sequence =
        first_ingress_sequence_;
    metadata.actual_last_ingress_sequence =
        last_ingress_sequence_;
    RawSegmentArtifactPlanV1 plan =
        BuildIncrementalRawSegmentArtifactPlanV1(
            metadata, *index_builder_, options_);
    if (!plan.ok()) {
        Trip(
            plan.failure ==
                    RawSegmentArtifactFailureV1::
                        kIndexSelfValidation
                ? RawSegmentAccumulatorErrorV1::
                      kIndexSelfValidation
                : RawSegmentAccumulatorErrorV1::
                      kIndexFinalize);
        return false;
    }
    artifact_plan_ = std::move(plan);
    sealed_ = true;
    return true;
}

bool RawSegmentAccumulatorV1::TakeArtifactPlan(
    RawSegmentArtifactPlanV1* plan) noexcept {
    if (plan == nullptr ||
        error_ !=
            RawSegmentAccumulatorErrorV1::kNone ||
        !sealed_ || artifact_plan_taken_) {
        return false;
    }
    *plan = std::move(artifact_plan_);
    artifact_plan_taken_ = true;
    return true;
}

void RawSegmentAccumulatorV1::Trip(
    RawSegmentAccumulatorErrorV1 error) noexcept {
    if (error_ ==
        RawSegmentAccumulatorErrorV1::kNone) {
        error_ = error;
    }
}

}  // namespace l2flow::ingress
