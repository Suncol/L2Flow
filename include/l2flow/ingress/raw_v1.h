#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ingress/capture_meta.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <type_traits>
#include <vector>

namespace l2flow::ingress {

inline constexpr std::uint16_t kRawV1FormatVersion = 1U;
inline constexpr std::uint8_t kRawV1LittleEndian = 1U;
inline constexpr std::size_t kRawV1RecordAlignment = 8U;

inline constexpr std::size_t kRawV1SegmentHeaderBytes = 4096U;
inline constexpr std::size_t kRawV1RecordHeaderBytes = 96U;
inline constexpr std::size_t kRawV1RecordTrailerBytes = 16U;
inline constexpr std::size_t kRawV1JournalHeaderBytes = 4096U;
inline constexpr std::size_t kRawV1DurableMarkerBytes = 48U;

// Stored little-endian, yielding the byte strings MRS1, MRW1, MRC1, MDJ1,
// and MDR1 respectively.
inline constexpr std::uint32_t kRawV1SegmentMagic = 0x3153524dU;
inline constexpr std::uint32_t kRawV1RecordMagic = 0x3157524dU;
inline constexpr std::uint32_t kRawV1RecordCommitMagic = 0x3143524dU;
inline constexpr std::uint32_t kRawV1JournalMagic = 0x314a444dU;
inline constexpr std::uint32_t kRawV1DurableMarkerMagic = 0x3152444dU;

inline constexpr std::uint32_t kRawV1FinalizationContinuation =
    0x00000001U;
inline constexpr std::uint32_t kRawV1SegmentFlagsMask =
    kRawV1FinalizationContinuation;
// No Raw-record flag has been assigned in V1.
inline constexpr std::uint32_t kRawV1RecordFlagsMask = 0U;
inline constexpr std::uint32_t kRawV1SegmentSealed =
    0x00000001U;
inline constexpr std::uint32_t kRawV1MarkerFlagsMask =
    kRawV1SegmentSealed;

using RawV1Digest = l2flow::common::Sha256Digest;
using RawV1Identity = l2flow::common::Identity128;
using RawV1VendorHead =
    std::array<std::byte, kVendorMessageHeadBytes>;
using RawV1SegmentHeaderWire =
    std::array<std::byte, kRawV1SegmentHeaderBytes>;
using RawV1RecordHeaderWire =
    std::array<std::byte, kRawV1RecordHeaderBytes>;
using RawV1RecordTrailerWire =
    std::array<std::byte, kRawV1RecordTrailerBytes>;
using RawV1JournalHeaderWire =
    std::array<std::byte, kRawV1JournalHeaderBytes>;
using RawV1DurableMarkerWire =
    std::array<std::byte, kRawV1DurableMarkerBytes>;

// Exact wire offsets. These constants, rather than C++ object layout, are the
// Raw V1 schema used by the codecs.
namespace raw_v1_offset {

namespace segment {
inline constexpr std::size_t kMagic = 0U;
inline constexpr std::size_t kFormatVersion = 4U;
inline constexpr std::size_t kEndian = 6U;
inline constexpr std::size_t kReserved0 = 7U;
inline constexpr std::size_t kHeaderSize = 8U;
inline constexpr std::size_t kSourceStreamId = 12U;
inline constexpr std::size_t kCaptureDate = 16U;
inline constexpr std::size_t kStreamDayId = 20U;
inline constexpr std::size_t kSegmentSequence = 36U;
inline constexpr std::size_t kSegmentFlags = 40U;
inline constexpr std::size_t kReserved1 = 44U;
inline constexpr std::size_t kReserveStateUuid = 48U;
inline constexpr std::size_t kFinalizationCycleId = 64U;
inline constexpr std::size_t kImmutableGrantSha256 = 80U;
inline constexpr std::size_t kSegmentBaseWalPos = 112U;
inline constexpr std::size_t kFirstIngressSequence = 120U;
inline constexpr std::size_t kCreatedRealtimeNs = 128U;
inline constexpr std::size_t kCreatedMonotonicNs = 136U;
inline constexpr std::size_t kHostUuid = 144U;
inline constexpr std::size_t kLinuxBootId = 160U;
inline constexpr std::size_t kClockEpochAlgorithm = 176U;
inline constexpr std::size_t kReserved2 = 180U;
inline constexpr std::size_t kClockEpochDigest = 184U;
inline constexpr std::size_t kClockEpochLabel = 216U;
inline constexpr std::size_t kSdkArchiveSha256 = 224U;
inline constexpr std::size_t kLibmdlApiSha256 = 256U;
inline constexpr std::size_t kEndpointContractSha256 = 288U;
inline constexpr std::size_t kConfigSha256 = 320U;
inline constexpr std::size_t kRawSchemaSha256 = 352U;
inline constexpr std::size_t kBuildManifestSha256 = 384U;
inline constexpr std::size_t kHeaderCrc32c = 416U;
inline constexpr std::size_t kReservedTail = 420U;
}  // namespace segment

namespace record_header {
inline constexpr std::size_t kMagic = 0U;
inline constexpr std::size_t kVersion = 4U;
inline constexpr std::size_t kHeaderSize = 6U;
inline constexpr std::size_t kRecordSize = 8U;
inline constexpr std::size_t kFlags = 12U;
inline constexpr std::size_t kSourceStreamId = 16U;
inline constexpr std::size_t kConnectionEpochHint = 20U;
inline constexpr std::size_t kIngressSequence = 24U;
inline constexpr std::size_t kRecvRealtimeNs = 32U;
inline constexpr std::size_t kRecvMonotonicNs = 40U;
inline constexpr std::size_t kCaptureDate = 48U;
inline constexpr std::size_t kVendorLocalTimeRaw = 52U;
inline constexpr std::size_t kVendorSequenceId = 56U;
inline constexpr std::size_t kVendorMessageSize = 64U;
inline constexpr std::size_t kVendorBodySize = 68U;
inline constexpr std::size_t kVendorServiceVersion = 72U;
inline constexpr std::size_t kVendorMessageId = 74U;
inline constexpr std::size_t kVendorServiceId = 76U;
inline constexpr std::size_t kVendorMessageEncoding = 77U;
inline constexpr std::size_t kVendorHeadSize = 78U;
inline constexpr std::size_t kReserved0 = 79U;
inline constexpr std::size_t kPayloadCrc32c = 80U;
inline constexpr std::size_t kHeaderCrc32c = 84U;
inline constexpr std::size_t kReserved1 = 88U;
}  // namespace record_header

namespace record_trailer {
inline constexpr std::size_t kRecordSize = 0U;
inline constexpr std::size_t kCommitMagic = 4U;
inline constexpr std::size_t kIngressSequence = 8U;
}  // namespace record_trailer

namespace journal {
inline constexpr std::size_t kMagic = 0U;
inline constexpr std::size_t kFormatVersion = 4U;
inline constexpr std::size_t kEndian = 6U;
inline constexpr std::size_t kReserved0 = 7U;
inline constexpr std::size_t kHeaderSize = 8U;
inline constexpr std::size_t kCaptureDate = 12U;
inline constexpr std::size_t kSourceStreamId = 16U;
inline constexpr std::size_t kStreamDayId = 20U;
inline constexpr std::size_t kReserved1 = 36U;
inline constexpr std::size_t kRawSchemaSha256 = 40U;
inline constexpr std::size_t kCreatedHostUuid = 72U;
inline constexpr std::size_t kCreatedLinuxBootId = 88U;
inline constexpr std::size_t kCreatedClockEpochAlgorithm = 104U;
inline constexpr std::size_t kReserved2 = 108U;
inline constexpr std::size_t kCreatedClockEpochDigest = 112U;
inline constexpr std::size_t kCreatedClockEpochLabel = 144U;
inline constexpr std::size_t kHeaderCrc32c = 152U;
inline constexpr std::size_t kReservedTail = 156U;
}  // namespace journal

namespace marker {
inline constexpr std::size_t kMagic = 0U;
inline constexpr std::size_t kVersion = 4U;
inline constexpr std::size_t kMarkerSize = 6U;
inline constexpr std::size_t kSourceStreamId = 8U;
inline constexpr std::size_t kSegmentSequence = 12U;
inline constexpr std::size_t kDurableGlobalWalPos = 16U;
inline constexpr std::size_t kDurableIngressSequence = 24U;
inline constexpr std::size_t kDurableSegmentOffset = 32U;
inline constexpr std::size_t kMarkerCrc32c = 40U;
inline constexpr std::size_t kMarkerFlags = 44U;
}  // namespace marker

}  // namespace raw_v1_offset

enum class RawV1Error : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidWireSize,
    kInvalidMagic,
    kUnsupportedVersion,
    kInvalidEndian,
    kInvalidHeaderSize,
    kUnknownFlags,
    kNonzeroReserved,
    kInvalidIdentity,
    kSizeOverflow,
    kRecordSizeMismatch,
    kInvalidVendorHead,
    kVendorFieldMismatch,
    kHeaderCrcMismatch,
    kPayloadCrcMismatch,
    kNonzeroPadding,
    kTrailerMismatch,
    kAllocationFailure,
};

[[nodiscard]] std::string_view RawV1ErrorName(
    RawV1Error error) noexcept;

// These are logical typed values. They are deliberately neither packed nor
// authorized for direct persistence. CRC members are populated by Decode and
// ignored/recomputed by Encode.
struct SegmentHeaderV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    RawV1Identity stream_day_id{};
    std::uint32_t segment_sequence = 0U;
    std::uint32_t segment_flags = 0U;
    RawV1Identity reserve_state_uuid{};
    RawV1Identity finalization_cycle_id{};
    RawV1Digest immutable_grant_sha256{};
    std::uint64_t segment_base_wal_pos = 0U;
    std::uint64_t first_ingress_sequence = 0U;
    std::uint64_t created_realtime_ns = 0U;
    std::uint64_t created_monotonic_ns = 0U;
    RawV1Identity host_uuid{};
    RawV1Identity linux_boot_id{};
    std::uint32_t clock_epoch_algorithm = 0U;
    RawV1Digest clock_epoch_digest{};
    std::uint64_t clock_epoch_label = 0U;
    // All-zero is the explicit "not computed" value for an operator-selected
    // path-only SDK.  Nonzero values retain the historical pinned-provenance
    // meaning.  These fields never authorize the production SDK path.
    RawV1Digest sdk_archive_sha256{};
    RawV1Digest libmdl_api_sha256{};
    RawV1Digest endpoint_contract_sha256{};
    RawV1Digest config_sha256{};
    RawV1Digest raw_schema_sha256{};
    RawV1Digest build_manifest_sha256{};
    std::uint32_t header_crc32c = 0U;
};

struct RawRecordHeaderV1 final {
    std::uint32_t record_size = 0U;
    std::uint32_t flags = 0U;
    std::uint32_t source_stream_id = 0U;
    std::uint32_t connection_epoch_hint = 0U;
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t recv_realtime_ns = 0U;
    std::uint64_t recv_monotonic_ns = 0U;
    std::uint32_t capture_date = 0U;
    std::uint32_t vendor_local_time_raw = 0U;
    std::uint64_t vendor_sequence_id = 0U;
    std::uint32_t vendor_message_size = 0U;
    std::uint32_t vendor_body_size = 0U;
    std::uint16_t vendor_service_version = 0U;
    std::uint16_t vendor_message_id = 0U;
    std::uint8_t vendor_service_id = 0U;
    std::uint8_t vendor_message_encoding = 0U;
    std::uint8_t vendor_head_size = 0U;
    std::uint32_t payload_crc32c = 0U;
    std::uint32_t header_crc32c = 0U;
};

struct RawRecordTrailerV1 final {
    std::uint32_t record_size = 0U;
    std::uint64_t ingress_sequence = 0U;
};

struct DurableJournalHeaderV1 final {
    std::uint32_t capture_date = 0U;
    std::uint32_t source_stream_id = 0U;
    RawV1Identity stream_day_id{};
    RawV1Digest raw_schema_sha256{};
    RawV1Identity created_host_uuid{};
    RawV1Identity created_linux_boot_id{};
    std::uint32_t created_clock_epoch_algorithm = 0U;
    RawV1Digest created_clock_epoch_digest{};
    std::uint64_t created_clock_epoch_label = 0U;
    std::uint32_t header_crc32c = 0U;
};

struct DurableMarkerV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t segment_sequence = 0U;
    std::uint64_t durable_global_wal_pos = 0U;
    std::uint64_t durable_ingress_sequence = 0U;
    std::uint64_t durable_segment_offset = 0U;
    std::uint32_t marker_crc32c = 0U;
    std::uint32_t marker_flags = 0U;
};

struct RawRecordLayoutV1 final {
    std::uint32_t vendor_message_size = 0U;
    std::uint32_t padding_size = 0U;
    std::uint32_t record_size = 0U;
};

struct RawRecordInputV1 final {
    CaptureMetaV1 meta{};
    RawV1VendorHead vendor_head{};
    std::span<const std::byte> vendor_body;
};

struct RawRecordNamespaceV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
};

// Spans refer to the validated input buffer and are invalidated with it.
struct RawRecordViewV1 final {
    RawRecordHeaderV1 header{};
    std::span<const std::byte> vendor_head;
    std::span<const std::byte> vendor_body;
    std::span<const std::byte> padding;
    RawRecordTrailerV1 trailer{};
};

static_assert(std::is_standard_layout_v<SegmentHeaderV1>);
static_assert(std::is_standard_layout_v<RawRecordHeaderV1>);
static_assert(std::is_standard_layout_v<RawRecordTrailerV1>);
static_assert(std::is_standard_layout_v<DurableJournalHeaderV1>);
static_assert(std::is_standard_layout_v<DurableMarkerV1>);

[[nodiscard]] RawV1Error ComputeRawRecordLayoutV1(
    std::size_t vendor_body_size,
    RawRecordLayoutV1* layout) noexcept;

[[nodiscard]] RawV1Error EncodeSegmentHeaderV1(
    const SegmentHeaderV1& header,
    RawV1SegmentHeaderWire* wire) noexcept;
[[nodiscard]] RawV1Error DecodeSegmentHeaderV1(
    std::span<const std::byte> wire,
    SegmentHeaderV1* header) noexcept;
[[nodiscard]] RawV1Error ValidateSegmentHeaderV1(
    std::span<const std::byte> wire) noexcept;

[[nodiscard]] RawV1Error EncodeRawRecordHeaderV1(
    const RawRecordHeaderV1& header,
    RawV1RecordHeaderWire* wire) noexcept;
[[nodiscard]] RawV1Error DecodeRawRecordHeaderV1(
    std::span<const std::byte> wire,
    RawRecordHeaderV1* header) noexcept;
[[nodiscard]] RawV1Error ValidateRawRecordHeaderV1(
    std::span<const std::byte> wire) noexcept;

[[nodiscard]] RawV1Error EncodeRawRecordTrailerV1(
    const RawRecordTrailerV1& trailer,
    RawV1RecordTrailerWire* wire) noexcept;
[[nodiscard]] RawV1Error DecodeRawRecordTrailerV1(
    std::span<const std::byte> wire,
    RawRecordTrailerV1* trailer) noexcept;
[[nodiscard]] RawV1Error ValidateRawRecordTrailerV1(
    std::span<const std::byte> wire) noexcept;

[[nodiscard]] RawV1Error EncodeDurableJournalHeaderV1(
    const DurableJournalHeaderV1& header,
    RawV1JournalHeaderWire* wire) noexcept;
[[nodiscard]] RawV1Error DecodeDurableJournalHeaderV1(
    std::span<const std::byte> wire,
    DurableJournalHeaderV1* header) noexcept;
[[nodiscard]] RawV1Error ValidateDurableJournalHeaderV1(
    std::span<const std::byte> wire) noexcept;

[[nodiscard]] RawV1Error EncodeDurableMarkerV1(
    const DurableMarkerV1& marker,
    RawV1DurableMarkerWire* wire) noexcept;
[[nodiscard]] RawV1Error DecodeDurableMarkerV1(
    std::span<const std::byte> wire,
    DurableMarkerV1* marker) noexcept;
[[nodiscard]] RawV1Error ValidateDurableMarkerV1(
    std::span<const std::byte> wire) noexcept;

[[nodiscard]] RawV1Error EncodeRawRecordV1(
    const RawRecordInputV1& input,
    std::vector<std::byte>* wire,
    RawRecordHeaderV1* encoded_header = nullptr) noexcept;
[[nodiscard]] RawV1Error DecodeRawRecordV1(
    std::span<const std::byte> wire,
    RawRecordViewV1* record,
    const RawRecordNamespaceV1* expected_namespace = nullptr) noexcept;
[[nodiscard]] RawV1Error ValidateRawRecordV1(
    std::span<const std::byte> wire,
    const RawRecordNamespaceV1* expected_namespace = nullptr) noexcept;

}  // namespace l2flow::ingress
