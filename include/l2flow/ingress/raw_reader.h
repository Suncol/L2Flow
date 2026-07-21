#pragma once

#include "l2flow/ingress/raw_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace l2flow::ingress {

inline constexpr std::size_t kRawReaderSegmentHeaderBytes =
    kRawV1SegmentHeaderBytes;
inline constexpr std::size_t kRawReaderRecordHeaderBytes =
    kRawV1RecordHeaderBytes;
inline constexpr std::size_t kRawReaderRecordTrailerBytes =
    kRawV1RecordTrailerBytes;

// Errors are reported at the first byte that could not be accepted. A result
// with a non-kNone error may contain a fully validated prefix, but callers must
// not treat that prefix as proof that the requested durable range is valid.
enum class RawReaderError : std::uint8_t {
    kNone = 0U,
    kNullBuffer,
    kSegmentHeaderTruncated,
    kSegmentHeaderInvalid,
    kDurableLimitBeforeData,
    kDurableLimitPastBuffer,
    kCursorOverflow,
    kRecordHeaderInvalid,
    kRecordSizeInvalid,
    kRecordPastDurableLimit,
    kPayloadCrcMismatch,
    kNonZeroPadding,
    kTrailerInvalid,
    kNamespaceMismatch,
    kIngressSequenceMismatch,
    kVendorHeadMismatch,
    kDurableLimitNotRecordBoundary,
    kResourceExhausted,
};

// A view owns a shared lifetime token for the immutable byte vector behind its
// spans. Copying or moving the view therefore cannot leave head/body dangling.
class RawRecordView final {
public:
    RawRecordView(const RawRecordView&) = default;
    RawRecordView& operator=(const RawRecordView&) = default;
    RawRecordView(RawRecordView&&) noexcept = default;
    RawRecordView& operator=(RawRecordView&&) noexcept = default;
    ~RawRecordView() = default;

    [[nodiscard]] const RawRecordHeaderV1& header() const noexcept {
        return header_;
    }
    [[nodiscard]] const RawRecordHeaderV1& metadata() const noexcept {
        return header_;
    }
    [[nodiscard]] std::span<const std::byte> vendor_head() const noexcept {
        return vendor_head_;
    }
    [[nodiscard]] std::span<const std::byte> vendor_body() const noexcept {
        return vendor_body_;
    }
    [[nodiscard]] std::uint64_t record_start_offset() const noexcept {
        return record_start_offset_;
    }
    [[nodiscard]] std::uint64_t record_end_offset() const noexcept {
        return record_end_offset_;
    }
    [[nodiscard]] std::uint64_t record_start_wal_pos() const noexcept {
        return record_start_wal_pos_;
    }
    [[nodiscard]] std::uint64_t record_end_wal_pos() const noexcept {
        return record_end_wal_pos_;
    }

private:
    friend struct RawRecordViewFactory;

    RawRecordView(
        std::shared_ptr<const std::vector<std::byte>> owner,
        RawRecordHeaderV1 header,
        std::span<const std::byte> vendor_head,
        std::span<const std::byte> vendor_body,
        std::uint64_t record_start_offset,
        std::uint64_t record_end_offset,
        std::uint64_t record_start_wal_pos,
        std::uint64_t record_end_wal_pos) noexcept;

    std::shared_ptr<const std::vector<std::byte>> owner_;
    RawRecordHeaderV1 header_{};
    std::span<const std::byte> vendor_head_{};
    std::span<const std::byte> vendor_body_{};
    std::uint64_t record_start_offset_ = 0U;
    std::uint64_t record_end_offset_ = 0U;
    std::uint64_t record_start_wal_pos_ = 0U;
    std::uint64_t record_end_wal_pos_ = 0U;
};

struct RawSegmentScanResult final {
    RawReaderError error = RawReaderError::kNone;
    // Preserves the exact codec reason when error came from Raw V1 decoding.
    RawV1Error codec_error = RawV1Error::kNone;
    std::uint64_t error_offset = 0U;
    // End of the complete, validated record prefix in segment and stream-day
    // WAL coordinates. These are exclusive cursors.
    std::uint64_t validated_end_offset = 0U;
    std::uint64_t validated_end_wal_pos = 0U;
    SegmentHeaderV1 segment{};
    std::vector<RawRecordView> records;

    [[nodiscard]] bool ok() const noexcept {
        return error == RawReaderError::kNone;
    }
};

// Result for validating one owned record copied with pread from an open
// segment. The backing vector contains exactly one complete record; the
// returned view nevertheless reports the caller-supplied segment/WAL
// coordinates and keeps that vector alive.
struct RawOwnedRecordResult final {
    RawReaderError error = RawReaderError::kNone;
    RawV1Error codec_error = RawV1Error::kNone;
    std::optional<RawRecordView> record;

    [[nodiscard]] bool ok() const noexcept {
        return error == RawReaderError::kNone &&
               record.has_value();
    }
};

// Validates exactly [0, durable_limit), where durable_limit is an exclusive
// segment-file cursor obtained from a durable journal marker. Bytes after that
// cursor are deliberately invisible, even when present in the owned buffer.
[[nodiscard]] RawSegmentScanResult ScanRawSegmentV1(
    std::shared_ptr<const std::vector<std::byte>> buffer,
    std::uint64_t durable_limit) noexcept;

// Validates exactly one complete Raw V1 record copied from an open segment.
// This is the safe building block for append-visible/live readers: it never
// exposes spans into a mutable mmap and it rejects namespace, sequence,
// framing, CRC, padding, trailer, and coordinate inconsistencies.
[[nodiscard]] RawOwnedRecordResult DecodeOwnedRawRecordV1(
    std::shared_ptr<const std::vector<std::byte>> wire,
    const SegmentHeaderV1& segment,
    std::uint64_t record_start_offset,
    std::uint64_t expected_ingress_sequence) noexcept;

}  // namespace l2flow::ingress
