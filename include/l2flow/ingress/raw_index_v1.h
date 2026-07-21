#pragma once

#include "l2flow/common/sha256.h"
#include "l2flow/ingress/raw_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

namespace l2flow::ingress {

inline constexpr std::uint16_t kRawIndexV1FormatVersion = 1U;
inline constexpr std::uint8_t kRawIndexV1LittleEndian = 1U;
inline constexpr std::size_t kRawIndexV1HeaderBytes = 4096U;
inline constexpr std::size_t kRawIndexV1EntryBytes = 64U;
inline constexpr std::size_t kRawIndexV1FooterBytes = 4096U;

// Stored little-endian, yielding the byte strings MRI1 and MRF1.
inline constexpr std::uint32_t kRawIndexV1HeaderMagic =
    0x3149524dU;
inline constexpr std::uint32_t kRawIndexV1FooterMagic =
    0x3146524dU;

// V1 has no assigned header or footer flag.
inline constexpr std::uint32_t kRawIndexV1HeaderFlagsMask = 0U;
inline constexpr std::uint32_t kRawIndexV1FooterFlagsMask = 0U;

// A producer may sample more frequently, but never less frequently, than
// either V1 ceiling.
inline constexpr std::uint64_t kRawIndexV1DefaultRecordInterval =
    4096U;
inline constexpr std::uint64_t kRawIndexV1DefaultRawBytesInterval =
    4U * 1024U * 1024U;

using RawIndexV1HeaderWire =
    std::array<std::byte, kRawIndexV1HeaderBytes>;
using RawIndexV1EntryWire =
    std::array<std::byte, kRawIndexV1EntryBytes>;
using RawIndexV1FooterWire =
    std::array<std::byte, kRawIndexV1FooterBytes>;

// Exact portable wire offsets. No C++ object in this header is authorized for
// direct persistence.
namespace raw_index_v1_offset {

namespace header {
inline constexpr std::size_t kMagic = 0U;
inline constexpr std::size_t kFormatVersion = 4U;
inline constexpr std::size_t kEndian = 6U;
inline constexpr std::size_t kReserved0 = 7U;
inline constexpr std::size_t kHeaderSize = 8U;
inline constexpr std::size_t kEntrySize = 12U;
inline constexpr std::size_t kFooterSize = 16U;
inline constexpr std::size_t kFlags = 20U;
inline constexpr std::size_t kCaptureDate = 24U;
inline constexpr std::size_t kSourceStreamId = 28U;
inline constexpr std::size_t kStreamDayId = 32U;
inline constexpr std::size_t kSegmentSequence = 48U;
inline constexpr std::size_t kReserved1 = 52U;
inline constexpr std::size_t kSegmentBaseWalPos = 56U;
inline constexpr std::size_t kRawSchemaSha256 = 64U;
inline constexpr std::size_t kSampleRecordInterval = 96U;
inline constexpr std::size_t kSampleRawBytesInterval = 104U;
inline constexpr std::size_t kHeaderCrc32c = 112U;
inline constexpr std::size_t kReservedTail = 116U;
}  // namespace header

namespace entry {
inline constexpr std::size_t kIngressSequence = 0U;
inline constexpr std::size_t kRecordStartWalPos = 8U;
inline constexpr std::size_t kRecordEndWalPos = 16U;
inline constexpr std::size_t kSegmentFileOffset = 24U;
inline constexpr std::size_t kVendorSequenceId = 32U;
inline constexpr std::size_t kRecvMonotonicNs = 40U;
inline constexpr std::size_t kConnectionEpochHint = 48U;
inline constexpr std::size_t kVendorServiceVersion = 52U;
inline constexpr std::size_t kVendorMessageId = 54U;
inline constexpr std::size_t kVendorServiceId = 56U;
inline constexpr std::size_t kReserved = 57U;
inline constexpr std::size_t kEntryCrc32c = 60U;
}  // namespace entry

namespace footer {
inline constexpr std::size_t kMagic = 0U;
inline constexpr std::size_t kFormatVersion = 4U;
inline constexpr std::size_t kEndian = 6U;
inline constexpr std::size_t kReserved0 = 7U;
inline constexpr std::size_t kFooterSize = 8U;
inline constexpr std::size_t kFlags = 12U;
inline constexpr std::size_t kEntryCount = 16U;
inline constexpr std::size_t kSegmentRecordCount = 24U;
inline constexpr std::size_t kSegmentLogicalEndOffset = 32U;
inline constexpr std::size_t kSegmentSha256 = 40U;
inline constexpr std::size_t kAcceptedSegmentSealedMarker = 72U;
inline constexpr std::size_t kIndexFileCrc32c = 120U;
inline constexpr std::size_t kReservedTail = 124U;
}  // namespace footer

}  // namespace raw_index_v1_offset

enum class RawIndexV1Error : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidWireSize,
    kInvalidMagic,
    kUnsupportedVersion,
    kInvalidEndian,
    kInvalidHeaderSize,
    kInvalidEntrySize,
    kInvalidFooterSize,
    kUnknownFlags,
    kNonzeroReserved,
    kInvalidIdentity,
    kInvalidSamplingThreshold,
    kHeaderCrcMismatch,
    kEntryCrcMismatch,
    kFileCrcMismatch,
    kLengthOverflow,
    kEntryCountMismatch,
    kSchemaMismatch,
    kNamespaceMismatch,
    kSegmentMismatch,
    kInvalidEntry,
    kEntryOrderViolation,
    kInvalidFooter,
    kInvalidSealMarker,
    kMarkerMismatch,
    kAllocationFailure,
};

[[nodiscard]] std::string_view RawIndexV1ErrorName(
    RawIndexV1Error error) noexcept;

struct RawIndexHeaderV1 final {
    std::uint32_t flags = 0U;
    std::uint32_t capture_date = 0U;
    std::uint32_t source_stream_id = 0U;
    RawV1Identity stream_day_id{};
    std::uint32_t segment_sequence = 0U;
    std::uint64_t segment_base_wal_pos = 0U;
    RawV1Digest raw_schema_sha256{};
    std::uint64_t sample_record_interval =
        kRawIndexV1DefaultRecordInterval;
    std::uint64_t sample_raw_bytes_interval =
        kRawIndexV1DefaultRawBytesInterval;
    // Populated by Decode and ignored/recomputed by Encode.
    std::uint32_t header_crc32c = 0U;
};

struct RawIndexEntryV1 final {
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t record_start_wal_pos = 0U;
    std::uint64_t record_end_wal_pos = 0U;
    std::uint64_t segment_file_offset = 0U;
    std::uint64_t vendor_sequence_id = 0U;
    std::uint64_t recv_monotonic_ns = 0U;
    std::uint32_t connection_epoch_hint = 0U;
    std::uint16_t vendor_service_version = 0U;
    std::uint16_t vendor_message_id = 0U;
    std::uint8_t vendor_service_id = 0U;
    // Populated by Decode and ignored/recomputed by Encode.
    std::uint32_t entry_crc32c = 0U;
};

struct RawIndexFooterV1 final {
    std::uint32_t flags = 0U;
    std::uint64_t entry_count = 0U;
    std::uint64_t segment_record_count = 0U;
    std::uint64_t segment_logical_end_offset = 0U;
    RawV1Digest segment_sha256{};
    // Exact accepted 48-byte SEGMENT_SEALED marker, including its CRC.
    RawV1DurableMarkerWire accepted_segment_sealed_marker{};
    // Direct footer encoding writes this value. BuildRawIndexFileV1 always
    // ignores it and computes the complete-file checksum.
    std::uint32_t index_file_crc32c = 0U;
};

struct RawIndexFileV1 final {
    RawIndexHeaderV1 header{};
    std::vector<RawIndexEntryV1> entries;
    RawIndexFooterV1 footer{};
    DurableMarkerV1 accepted_segment_sealed_marker{};
};

// Validation context supplied by the Raw segment/journal owner. This is what
// turns a structurally valid index into an index for one exact namespace,
// segment and Raw schema.
struct RawIndexExpectedIdentityV1 final {
    std::uint32_t capture_date = 0U;
    std::uint32_t source_stream_id = 0U;
    RawV1Identity stream_day_id{};
    std::uint32_t segment_sequence = 0U;
    std::uint64_t segment_base_wal_pos = 0U;
    RawV1Digest raw_schema_sha256{};
    std::uint64_t sample_record_interval =
        kRawIndexV1DefaultRecordInterval;
    std::uint64_t sample_raw_bytes_interval =
        kRawIndexV1DefaultRawBytesInterval;
};

[[nodiscard]] RawIndexV1Error EncodeRawIndexHeaderV1(
    const RawIndexHeaderV1& header,
    RawIndexV1HeaderWire* wire) noexcept;
[[nodiscard]] RawIndexV1Error DecodeRawIndexHeaderV1(
    std::span<const std::byte> wire,
    RawIndexHeaderV1* header) noexcept;

[[nodiscard]] RawIndexV1Error EncodeRawIndexEntryV1(
    const RawIndexEntryV1& entry,
    RawIndexV1EntryWire* wire) noexcept;
[[nodiscard]] RawIndexV1Error DecodeRawIndexEntryV1(
    std::span<const std::byte> wire,
    RawIndexEntryV1* entry) noexcept;

[[nodiscard]] RawIndexV1Error EncodeRawIndexFooterV1(
    const RawIndexFooterV1& footer,
    RawIndexV1FooterWire* wire) noexcept;
[[nodiscard]] RawIndexV1Error DecodeRawIndexFooterV1(
    std::span<const std::byte> wire,
    RawIndexFooterV1* footer) noexcept;

// Builds exactly 4096 + 64*entry_count + 4096 bytes. Header/entry CRCs and the
// complete-file CRC are computed by this function.
[[nodiscard]] RawIndexV1Error BuildRawIndexFileV1(
    const RawIndexHeaderV1& header,
    std::span<const RawIndexEntryV1> entries,
    const RawIndexFooterV1& footer,
    std::vector<std::byte>* wire) noexcept;

// Decode performs all intrinsic framing, reserved, CRC, ordering, marker and
// length checks. Validate additionally rejects a mismatch against the exact
// Raw segment/journal identity supplied by the caller.
[[nodiscard]] RawIndexV1Error DecodeRawIndexFileV1(
    std::span<const std::byte> wire,
    RawIndexFileV1* index) noexcept;
[[nodiscard]] RawIndexV1Error ValidateRawIndexFileV1(
    std::span<const std::byte> wire,
    const RawIndexExpectedIdentityV1& expected,
    RawIndexFileV1* index = nullptr) noexcept;

// Feed every record, not merely sampled records. The builder always includes
// the first and final record and adds an entry whenever either configured
// threshold is reached, so no sampling gap exceeds a V1 ceiling.
class RawIndexBuilderV1 final {
public:
    explicit RawIndexBuilderV1(RawIndexHeaderV1 header) noexcept;

    [[nodiscard]] RawIndexV1Error AddRecord(
        const RawIndexEntryV1& record) noexcept;
    [[nodiscard]] RawIndexV1Error Build(
        const RawV1Digest& segment_sha256,
        const RawV1DurableMarkerWire& accepted_sealed_marker,
        std::vector<std::byte>* wire) const noexcept;

    [[nodiscard]] RawIndexV1Error error() const noexcept {
        return error_;
    }
    [[nodiscard]] std::uint64_t record_count() const noexcept {
        return record_count_;
    }
    [[nodiscard]] std::span<const RawIndexEntryV1>
    sampled_entries() const noexcept {
        return sampled_entries_;
    }

private:
    RawIndexHeaderV1 header_{};
    std::vector<RawIndexEntryV1> sampled_entries_;
    RawIndexEntryV1 last_record_{};
    std::uint64_t record_count_ = 0U;
    std::uint64_t last_sample_record_count_ = 0U;
    std::uint64_t last_sample_end_wal_pos_ = 0U;
    bool have_last_record_ = false;
    RawIndexV1Error error_ = RawIndexV1Error::kNone;
};

}  // namespace l2flow::ingress
