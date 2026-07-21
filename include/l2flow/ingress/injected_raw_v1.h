#pragma once

#include "l2flow/ingress/raw_fault_transform.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

namespace l2flow::ingress {

inline constexpr std::uint16_t kInjectedRawV1FormatVersion = 1U;
inline constexpr std::uint8_t kInjectedRawV1LittleEndian = 1U;
inline constexpr std::size_t kInjectedRawV1RecordAlignment = 8U;
inline constexpr std::size_t kInjectedRawV1SegmentHeaderBytes = 4096U;
inline constexpr std::size_t kInjectedRawV1RecordHeaderBytes = 128U;
inline constexpr std::size_t kInjectedRawV1ParentLocatorBytes = 64U;
inline constexpr std::size_t kInjectedRawV1RecordTrailerBytes = 24U;

inline constexpr std::uint32_t kInjectedRawV1RecordMagic =
    0x31575249U;  // IRW1 as little-endian bytes.
inline constexpr std::uint32_t kInjectedRawV1CommitMagic =
    0x31435249U;  // IRC1 as little-endian bytes.
inline constexpr std::array<std::byte, 8U>
    kInjectedRawV1SegmentMagic{
        std::byte{'L'},
        std::byte{'2'},
        std::byte{'I'},
        std::byte{'R'},
        std::byte{'S'},
        std::byte{'E'},
        std::byte{'G'},
        std::byte{'1'}};
static_assert(
    kInjectedRawV1RecordMagic != kRawV1RecordMagic);
static_assert(
    kInjectedRawV1CommitMagic !=
    kRawV1RecordCommitMagic);
static_assert(
    kInjectedRawV1SegmentMagic[0U] != std::byte{'M'});

inline constexpr std::uint32_t kInjectedRawV1Synthetic =
    0x00000001U;
inline constexpr std::uint32_t kInjectedRawV1SegmentFlagsMask =
    kInjectedRawV1Synthetic;
inline constexpr std::uint32_t kInjectedRawV1RecordMutated =
    0x00000001U;
inline constexpr std::uint32_t kInjectedRawV1RecordFlagsMask =
    kInjectedRawV1RecordMutated;

inline constexpr std::uint32_t
    kInjectedRawV1MaximumVendorBodyBytes = 16'777'216U;
inline constexpr std::uint32_t
    kInjectedRawV1MinimumRecordBytes = 240U;
inline constexpr std::uint32_t
    kInjectedRawV1MaximumRecordBytes = 16'777'456U;
inline constexpr std::uint64_t
    kInjectedRawV1MaximumRecordCount = 2'000'000U;
inline constexpr std::uint64_t
    kInjectedRawV1MaximumDroppedLocatorCount = 1'000'000U;
inline constexpr std::uint64_t
    kInjectedRawV1MaximumLogicalBytes = 2'147'483'648U;

// Exact repository bytes and SHA-256 for schemas/injected_raw_v1.json.
inline constexpr std::uint64_t kInjectedRawV1SchemaBytes = 13'239U;
inline constexpr RawV1Digest kInjectedRawV1SchemaSha256{{
    std::byte{0x01U}, std::byte{0x6eU}, std::byte{0xebU},
    std::byte{0xfaU}, std::byte{0x0eU}, std::byte{0xaeU},
    std::byte{0xa5U}, std::byte{0xedU}, std::byte{0xc0U},
    std::byte{0xb4U}, std::byte{0x11U}, std::byte{0x50U},
    std::byte{0x36U}, std::byte{0x72U}, std::byte{0x4bU},
    std::byte{0x8bU}, std::byte{0x0dU}, std::byte{0xddU},
    std::byte{0xdaU}, std::byte{0xf7U}, std::byte{0xecU},
    std::byte{0x0cU}, std::byte{0x1bU}, std::byte{0x22U},
    std::byte{0x2eU}, std::byte{0x0eU}, std::byte{0x4eU},
    std::byte{0x6aU}, std::byte{0x42U}, std::byte{0x5cU},
    std::byte{0x89U}, std::byte{0x1bU}}};

using InjectedRawV1SegmentHeaderWire =
    std::array<std::byte, kInjectedRawV1SegmentHeaderBytes>;
using InjectedRawV1RecordHeaderWire =
    std::array<std::byte, kInjectedRawV1RecordHeaderBytes>;
using InjectedRawV1ParentLocatorWire =
    std::array<std::byte, kInjectedRawV1ParentLocatorBytes>;
using InjectedRawV1RecordTrailerWire =
    std::array<std::byte, kInjectedRawV1RecordTrailerBytes>;

namespace injected_raw_v1_offset {

namespace segment {
inline constexpr std::size_t kMagic = 0U;
inline constexpr std::size_t kFormatVersion = 8U;
inline constexpr std::size_t kEndian = 10U;
inline constexpr std::size_t kReserved0 = 11U;
inline constexpr std::size_t kHeaderSize = 12U;
inline constexpr std::size_t kFlags = 16U;
inline constexpr std::size_t kReserved1 = 20U;
inline constexpr std::size_t kRunId = 24U;
inline constexpr std::size_t kSyntheticNamespaceId = 40U;
inline constexpr std::size_t kRawSchemaSha256 = 56U;
inline constexpr std::size_t kInjectedSchemaSha256 = 88U;
inline constexpr std::size_t kParentRawIdentitySha256 = 120U;
inline constexpr std::size_t kFaultRuleSha256 = 152U;
inline constexpr std::size_t kFaultSeed = 184U;
inline constexpr std::size_t kRecordCount = 192U;
inline constexpr std::size_t kDroppedLocatorCount = 200U;
inline constexpr std::size_t kRecordsOffset = 208U;
inline constexpr std::size_t kDroppedLocatorsOffset = 216U;
inline constexpr std::size_t kLogicalSize = 224U;
inline constexpr std::size_t kParentWalBegin = 232U;
inline constexpr std::size_t kParentWalEnd = 240U;
inline constexpr std::size_t kHeaderCrc32c = 248U;
inline constexpr std::size_t kReservedTail = 252U;
}  // namespace segment

namespace record_header {
inline constexpr std::size_t kMagic = 0U;
inline constexpr std::size_t kFormatVersion = 4U;
inline constexpr std::size_t kEndian = 6U;
inline constexpr std::size_t kReserved0 = 7U;
inline constexpr std::size_t kHeaderSize = 8U;
inline constexpr std::size_t kParentLocatorSize = 10U;
inline constexpr std::size_t kRecordSize = 12U;
inline constexpr std::size_t kFlags = 16U;
inline constexpr std::size_t kVendorBodySize = 20U;
inline constexpr std::size_t kSyntheticIngressSequence = 24U;
inline constexpr std::size_t kSourceStreamId = 32U;
inline constexpr std::size_t kCaptureDate = 36U;
inline constexpr std::size_t kConnectionEpochHint = 40U;
inline constexpr std::size_t kVendorMessageSize = 44U;
inline constexpr std::size_t kRecvRealtimeNs = 48U;
inline constexpr std::size_t kRecvMonotonicNs = 56U;
inline constexpr std::size_t kMutationVendorBodyOffset = 64U;
inline constexpr std::size_t kMutationKind = 72U;
inline constexpr std::size_t kMutationBefore = 73U;
inline constexpr std::size_t kMutationAfter = 74U;
inline constexpr std::size_t kParentProvenance = 75U;
inline constexpr std::size_t kVendorHeadSize = 76U;
inline constexpr std::size_t kReserved1 = 77U;
inline constexpr std::size_t kPayloadCrc32c = 80U;
inline constexpr std::size_t kHeaderCrc32c = 84U;
inline constexpr std::size_t kReservedTail = 88U;
}  // namespace record_header

namespace parent_locator {
inline constexpr std::size_t kCaptureDate = 0U;
inline constexpr std::size_t kSourceStreamId = 4U;
inline constexpr std::size_t kStreamDayId = 8U;
inline constexpr std::size_t kIngressSequence = 24U;
inline constexpr std::size_t kRecordStartWalPos = 32U;
inline constexpr std::size_t kRecordEndWalPos = 40U;
inline constexpr std::size_t kOccurrence = 48U;
inline constexpr std::size_t kReserved = 52U;
inline constexpr std::size_t kLocatorCrc32c = 60U;
}  // namespace parent_locator

namespace record_trailer {
inline constexpr std::size_t kCommitMagic = 0U;
inline constexpr std::size_t kRecordSize = 4U;
inline constexpr std::size_t kSyntheticIngressSequence = 8U;
inline constexpr std::size_t kTrailerCrc32c = 16U;
inline constexpr std::size_t kReserved = 20U;
}  // namespace record_trailer

}  // namespace injected_raw_v1_offset

struct InjectedRawSegmentHeaderV1 final {
    std::uint32_t flags = kInjectedRawV1Synthetic;
    RawV1Identity run_id{};
    RawV1Identity synthetic_namespace_id{};
    RawV1Digest raw_schema_sha256{};
    RawV1Digest injected_schema_sha256 =
        kInjectedRawV1SchemaSha256;
    RawV1Digest parent_raw_identity_sha256{};
    RawV1Digest fault_rule_sha256{};
    std::uint64_t fault_seed = 0U;
    std::uint64_t record_count = 0U;
    std::uint64_t dropped_locator_count = 0U;
    std::uint64_t records_offset =
        kInjectedRawV1SegmentHeaderBytes;
    std::uint64_t dropped_locators_offset =
        kInjectedRawV1SegmentHeaderBytes;
    std::uint64_t logical_size =
        kInjectedRawV1SegmentHeaderBytes;
    // Enclosing half-open Raw WAL range over every emitted and dropped
    // parent locator. Both are zero only when the segment has no parents.
    std::uint64_t parent_wal_begin = 0U;
    std::uint64_t parent_wal_end = 0U;
    std::uint32_t header_crc32c = 0U;
};

struct InjectedRawRecordHeaderV1 final {
    std::uint32_t record_size = 0U;
    std::uint32_t flags = 0U;
    std::uint32_t vendor_body_size = 0U;
    std::uint64_t synthetic_ingress_sequence = 0U;
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    std::uint32_t connection_epoch_hint = 0U;
    std::uint32_t vendor_message_size = 0U;
    std::uint64_t recv_realtime_ns = 0U;
    std::uint64_t recv_monotonic_ns = 0U;
    std::uint64_t mutation_vendor_body_offset = 0U;
    RawLogicalMutationKind mutation_kind =
        RawLogicalMutationKind::kNone;
    std::byte mutation_before{0U};
    std::byte mutation_after{0U};
    RawReplayProvenance parent_provenance =
        RawReplayProvenance::kDurable;
    std::uint8_t vendor_head_size =
        static_cast<std::uint8_t>(kVendorMessageHeadBytes);
    std::uint32_t payload_crc32c = 0U;
    std::uint32_t header_crc32c = 0U;
};

struct InjectedRawParentLocatorV1 final {
    InjectedRawParentLocator locator{};
    std::uint32_t locator_crc32c = 0U;
};

struct InjectedRawRecordTrailerV1 final {
    std::uint32_t record_size = 0U;
    std::uint64_t synthetic_ingress_sequence = 0U;
    std::uint32_t trailer_crc32c = 0U;
};

struct InjectedRawRecordLayoutV1 final {
    std::uint32_t vendor_message_size = 0U;
    std::uint32_t padding_size = 0U;
    std::uint32_t record_size = 0U;
};

enum class InjectedRawV1Error : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kNullBuffer,
    kInvalidWireSize,
    kInvalidMagic,
    kUnsupportedVersion,
    kInvalidEndian,
    kInvalidHeaderSize,
    kUnknownFlags,
    kNonzeroReserved,
    kInvalidIdentity,
    kSchemaIdentityMismatch,
    kSchemaIdentityAlias,
    kInvalidCount,
    kInvalidSize,
    kSizeOverflow,
    kInvalidSequence,
    kInvalidCaptureMeta,
    kInvalidParentLocator,
    kDuplicateParentLocator,
    kInvalidParentOccurrence,
    kInvalidProvenance,
    kInvalidRule,
    kInvalidMutation,
    kInvalidVendorHead,
    kHeaderCrcMismatch,
    kParentLocatorCrcMismatch,
    kPayloadCrcMismatch,
    kNonzeroPadding,
    kTrailerMismatch,
    kDroppedTableMismatch,
    kResourceLimitExceeded,
    kAllocationFailure,
};

[[nodiscard]] std::string_view InjectedRawV1ErrorName(
    InjectedRawV1Error error) noexcept;

[[nodiscard]] InjectedRawV1Error
ComputeInjectedRawRecordLayoutV1(
    std::size_t vendor_body_size,
    InjectedRawRecordLayoutV1* layout) noexcept;

[[nodiscard]] InjectedRawV1Error
EncodeInjectedRawSegmentHeaderV1(
    const InjectedRawSegmentHeaderV1& header,
    InjectedRawV1SegmentHeaderWire* wire) noexcept;
[[nodiscard]] InjectedRawV1Error
DecodeInjectedRawSegmentHeaderV1(
    std::span<const std::byte> wire,
    InjectedRawSegmentHeaderV1* header) noexcept;

[[nodiscard]] InjectedRawV1Error
EncodeInjectedRawRecordHeaderV1(
    const InjectedRawRecordHeaderV1& header,
    InjectedRawV1RecordHeaderWire* wire) noexcept;
[[nodiscard]] InjectedRawV1Error
DecodeInjectedRawRecordHeaderV1(
    std::span<const std::byte> wire,
    InjectedRawRecordHeaderV1* header) noexcept;

[[nodiscard]] InjectedRawV1Error
EncodeInjectedRawParentLocatorV1(
    const InjectedRawParentLocator& locator,
    InjectedRawV1ParentLocatorWire* wire) noexcept;
[[nodiscard]] InjectedRawV1Error
DecodeInjectedRawParentLocatorV1(
    std::span<const std::byte> wire,
    InjectedRawParentLocatorV1* locator) noexcept;

[[nodiscard]] InjectedRawV1Error
EncodeInjectedRawRecordTrailerV1(
    const InjectedRawRecordTrailerV1& trailer,
    InjectedRawV1RecordTrailerWire* wire) noexcept;
[[nodiscard]] InjectedRawV1Error
DecodeInjectedRawRecordTrailerV1(
    std::span<const std::byte> wire,
    InjectedRawRecordTrailerV1* trailer) noexcept;

// The only framing seam for a logical transform plan. The plan tag remains
// in-memory provenance and is never copied into the segment bytes.
[[nodiscard]] InjectedRawV1Error ProduceInjectedRawSegmentV1(
    const InjectedRawTransformPlan& plan,
    std::shared_ptr<const std::vector<std::byte>>* output)
    noexcept;

}  // namespace l2flow::ingress
