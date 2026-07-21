#include "l2flow/ingress/raw_reader.h"

#include <limits>
#include <new>
#include <utility>

namespace l2flow::ingress {
namespace {

[[nodiscard]] bool AddU64(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        left > std::numeric_limits<std::uint64_t>::max() - right) {
        return false;
    }
    *result = left + right;
    return true;
}

[[nodiscard]] bool U64ToSize(
    std::uint64_t value,
    std::size_t* result) noexcept {
    if (result == nullptr) {
        return false;
    }
    if constexpr (
        sizeof(std::size_t) < sizeof(std::uint64_t)) {
        if (value >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
            return false;
        }
    }
    *result = static_cast<std::size_t>(value);
    return true;
}

[[nodiscard]] RawReaderError MapRecordError(
    RawV1Error error) noexcept {
    switch (error) {
        case RawV1Error::kNone:
            return RawReaderError::kNone;
        case RawV1Error::kPayloadCrcMismatch:
            return RawReaderError::kPayloadCrcMismatch;
        case RawV1Error::kNonzeroPadding:
            return RawReaderError::kNonZeroPadding;
        case RawV1Error::kTrailerMismatch:
            return RawReaderError::kTrailerInvalid;
        case RawV1Error::kInvalidVendorHead:
        case RawV1Error::kVendorFieldMismatch:
            return RawReaderError::kVendorHeadMismatch;
        case RawV1Error::kSizeOverflow:
        case RawV1Error::kRecordSizeMismatch:
            return RawReaderError::kRecordSizeInvalid;
        case RawV1Error::kAllocationFailure:
            return RawReaderError::kResourceExhausted;
        case RawV1Error::kNullOutput:
        case RawV1Error::kInvalidWireSize:
        case RawV1Error::kInvalidMagic:
        case RawV1Error::kUnsupportedVersion:
        case RawV1Error::kInvalidEndian:
        case RawV1Error::kInvalidHeaderSize:
        case RawV1Error::kUnknownFlags:
        case RawV1Error::kNonzeroReserved:
        case RawV1Error::kInvalidIdentity:
        case RawV1Error::kHeaderCrcMismatch:
            return RawReaderError::kRecordHeaderInvalid;
    }
    return RawReaderError::kRecordHeaderInvalid;
}

void SetError(
    RawSegmentScanResult* result,
    RawReaderError error,
    std::uint64_t error_offset,
    RawV1Error codec_error = RawV1Error::kNone) noexcept {
    if (result == nullptr) {
        return;
    }
    result->error = error;
    result->codec_error = codec_error;
    result->error_offset = error_offset;
}

}  // namespace

struct RawRecordViewFactory final {
    [[nodiscard]] static RawRecordView Make(
        const std::shared_ptr<const std::vector<std::byte>>& owner,
        const RawRecordViewV1& decoded,
        std::uint64_t record_start_offset,
        std::uint64_t record_end_offset,
        std::uint64_t record_start_wal_pos,
        std::uint64_t record_end_wal_pos) noexcept {
        return RawRecordView{
            owner,
            decoded.header,
            decoded.vendor_head,
            decoded.vendor_body,
            record_start_offset,
            record_end_offset,
            record_start_wal_pos,
            record_end_wal_pos};
    }
};

RawRecordView::RawRecordView(
    std::shared_ptr<const std::vector<std::byte>> owner,
    RawRecordHeaderV1 header,
    std::span<const std::byte> vendor_head,
    std::span<const std::byte> vendor_body,
    std::uint64_t record_start_offset,
    std::uint64_t record_end_offset,
    std::uint64_t record_start_wal_pos,
    std::uint64_t record_end_wal_pos) noexcept
    : owner_(std::move(owner)),
      header_(header),
      vendor_head_(vendor_head),
      vendor_body_(vendor_body),
      record_start_offset_(record_start_offset),
      record_end_offset_(record_end_offset),
      record_start_wal_pos_(record_start_wal_pos),
      record_end_wal_pos_(record_end_wal_pos) {}

RawSegmentScanResult ScanRawSegmentV1(
    std::shared_ptr<const std::vector<std::byte>> buffer,
    std::uint64_t durable_limit) noexcept {
    RawSegmentScanResult result;
    if (buffer == nullptr) {
        SetError(&result, RawReaderError::kNullBuffer, 0U);
        return result;
    }
    if (buffer->size() < kRawV1SegmentHeaderBytes) {
        SetError(
            &result,
            RawReaderError::kSegmentHeaderTruncated,
            static_cast<std::uint64_t>(buffer->size()));
        return result;
    }
    if (durable_limit < kRawV1SegmentHeaderBytes) {
        SetError(
            &result,
            RawReaderError::kDurableLimitBeforeData,
            durable_limit);
        return result;
    }

    std::size_t durable_size = 0U;
    if (!U64ToSize(durable_limit, &durable_size)) {
        SetError(
            &result,
            RawReaderError::kDurableLimitPastBuffer,
            durable_limit);
        return result;
    }
    if (durable_size > buffer->size()) {
        SetError(
            &result,
            RawReaderError::kDurableLimitPastBuffer,
            static_cast<std::uint64_t>(buffer->size()));
        return result;
    }

    SegmentHeaderV1 segment;
    const RawV1Error segment_error = DecodeSegmentHeaderV1(
        std::span<const std::byte>(
            buffer->data(), kRawV1SegmentHeaderBytes),
        &segment);
    if (segment_error != RawV1Error::kNone) {
        SetError(
            &result,
            RawReaderError::kSegmentHeaderInvalid,
            0U,
            segment_error);
        return result;
    }

    result.segment = segment;

    result.validated_end_offset = kRawV1SegmentHeaderBytes;
    if (!AddU64(
            segment.segment_base_wal_pos,
            result.validated_end_offset,
            &result.validated_end_wal_pos)) {
        SetError(
            &result,
            RawReaderError::kCursorOverflow,
            result.validated_end_offset);
        return result;
    }

    const RawRecordNamespaceV1 expected_namespace{
        segment.source_stream_id,
        segment.capture_date};
    std::uint64_t expected_sequence =
        segment.first_ingress_sequence;
    bool sequence_exhausted = false;
    std::size_t offset = kRawV1SegmentHeaderBytes;

    try {
        while (offset < durable_size) {
            const std::size_t remaining = durable_size - offset;
            if (remaining < kRawV1RecordHeaderBytes) {
                SetError(
                    &result,
                    RawReaderError::kDurableLimitNotRecordBoundary,
                    static_cast<std::uint64_t>(offset));
                return result;
            }

            RawRecordHeaderV1 record_header;
            const RawV1Error header_error =
                DecodeRawRecordHeaderV1(
                    std::span<const std::byte>(
                        buffer->data() + offset,
                        kRawV1RecordHeaderBytes),
                    &record_header);
            if (header_error != RawV1Error::kNone) {
                SetError(
                    &result,
                    MapRecordError(header_error),
                    static_cast<std::uint64_t>(offset),
                    header_error);
                return result;
            }
            if (record_header.source_stream_id !=
                    segment.source_stream_id ||
                record_header.capture_date !=
                    segment.capture_date) {
                SetError(
                    &result,
                    RawReaderError::kNamespaceMismatch,
                    static_cast<std::uint64_t>(offset));
                return result;
            }
            if (sequence_exhausted ||
                record_header.ingress_sequence !=
                    expected_sequence) {
                SetError(
                    &result,
                    RawReaderError::kIngressSequenceMismatch,
                    static_cast<std::uint64_t>(offset));
                return result;
            }

            const std::size_t record_size =
                static_cast<std::size_t>(
                    record_header.record_size);
            if (record_size > remaining) {
                SetError(
                    &result,
                    RawReaderError::kRecordPastDurableLimit,
                    static_cast<std::uint64_t>(offset));
                return result;
            }

            RawRecordViewV1 decoded;
            const RawV1Error record_error =
                DecodeRawRecordV1(
                    std::span<const std::byte>(
                        buffer->data() + offset,
                        record_size),
                    &decoded,
                    &expected_namespace);
            if (record_error != RawV1Error::kNone) {
                SetError(
                    &result,
                    MapRecordError(record_error),
                    static_cast<std::uint64_t>(offset),
                    record_error);
                return result;
            }

            const std::uint64_t start_offset =
                static_cast<std::uint64_t>(offset);
            std::uint64_t end_offset = 0U;
            std::uint64_t start_wal_pos = 0U;
            std::uint64_t end_wal_pos = 0U;
            if (!AddU64(
                    start_offset,
                    static_cast<std::uint64_t>(record_size),
                    &end_offset) ||
                !AddU64(
                    segment.segment_base_wal_pos,
                    start_offset,
                    &start_wal_pos) ||
                !AddU64(
                    start_wal_pos,
                    static_cast<std::uint64_t>(record_size),
                    &end_wal_pos)) {
                SetError(
                    &result,
                    RawReaderError::kCursorOverflow,
                    start_offset);
                return result;
            }

            result.records.push_back(
                RawRecordViewFactory::Make(
                    buffer,
                    decoded,
                    start_offset,
                    end_offset,
                    start_wal_pos,
                    end_wal_pos));
            result.validated_end_offset = end_offset;
            result.validated_end_wal_pos = end_wal_pos;
            offset += record_size;

            if (expected_sequence ==
                std::numeric_limits<std::uint64_t>::max()) {
                sequence_exhausted = true;
            } else {
                ++expected_sequence;
            }
        }
    } catch (const std::bad_alloc&) {
        SetError(
            &result,
            RawReaderError::kResourceExhausted,
            static_cast<std::uint64_t>(offset));
        return result;
    } catch (...) {
        SetError(
            &result,
            RawReaderError::kResourceExhausted,
            static_cast<std::uint64_t>(offset));
        return result;
    }

    return result;
}

RawOwnedRecordResult DecodeOwnedRawRecordV1(
    std::shared_ptr<const std::vector<std::byte>> wire,
    const SegmentHeaderV1& segment,
    std::uint64_t record_start_offset,
    std::uint64_t expected_ingress_sequence) noexcept {
    RawOwnedRecordResult result;
    if (wire == nullptr) {
        result.error = RawReaderError::kNullBuffer;
        return result;
    }
    if (record_start_offset < kRawV1SegmentHeaderBytes ||
        expected_ingress_sequence == 0U) {
        result.error = RawReaderError::kRecordSizeInvalid;
        return result;
    }

    RawRecordViewV1 decoded;
    const RawRecordNamespaceV1 expected_namespace{
        segment.source_stream_id,
        segment.capture_date};
    const RawV1Error codec_error =
        DecodeRawRecordV1(
            std::span<const std::byte>(
                wire->data(), wire->size()),
            &decoded,
            &expected_namespace);
    if (codec_error != RawV1Error::kNone) {
        result.error = MapRecordError(codec_error);
        result.codec_error = codec_error;
        return result;
    }
    if (decoded.header.ingress_sequence !=
        expected_ingress_sequence) {
        result.error =
            RawReaderError::kIngressSequenceMismatch;
        return result;
    }

    std::uint64_t record_end_offset = 0U;
    std::uint64_t record_start_wal_pos = 0U;
    std::uint64_t record_end_wal_pos = 0U;
    if (!AddU64(
            record_start_offset,
            static_cast<std::uint64_t>(wire->size()),
            &record_end_offset) ||
        !AddU64(
            segment.segment_base_wal_pos,
            record_start_offset,
            &record_start_wal_pos) ||
        !AddU64(
            record_start_wal_pos,
            static_cast<std::uint64_t>(wire->size()),
            &record_end_wal_pos)) {
        result.error = RawReaderError::kCursorOverflow;
        return result;
    }

    try {
        result.record.emplace(
            RawRecordViewFactory::Make(
                wire,
                decoded,
                record_start_offset,
                record_end_offset,
                record_start_wal_pos,
                record_end_wal_pos));
    } catch (...) {
        result.error = RawReaderError::kResourceExhausted;
        return result;
    }
    return result;
}

}  // namespace l2flow::ingress
