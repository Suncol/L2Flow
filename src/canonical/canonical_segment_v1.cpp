#include "l2flow/canonical/canonical_segment_v1.h"

#include "l2flow/common/crc32c.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <fcntl.h>
#include <linux/futex.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

namespace l2flow::canonical {
namespace {

constexpr std::uint32_t kSegmentMagic = 0x35455343U;   // "CSE5"
constexpr std::uint16_t kSegmentVersion = 1U;
constexpr std::uint32_t kEndianMarker = 0x01020304U;
constexpr std::uint16_t kSegmentFlagSealed = 1U << 0U;
constexpr std::uint32_t kManifestMagic = 0x35464d43U;  // "CMF5"
constexpr std::uint16_t kManifestVersion = 1U;
constexpr std::uint64_t kControlMagicAndVersion =
    (std::uint64_t{1U} << 32U) | std::uint64_t{0x35434d43U};
constexpr std::string_view kIntegrityDomain =
    "L2FLOW_CANONICAL_SEGMENT_INTEGRITY_V1";

namespace header_offset {
constexpr std::size_t kMagic = 0U;
constexpr std::size_t kVersion = 4U;
constexpr std::size_t kHeaderBytes = 6U;
constexpr std::size_t kEndian = 8U;
constexpr std::size_t kEventType = 12U;
constexpr std::size_t kFlags = 14U;
constexpr std::size_t kRecordSize = 16U;
constexpr std::size_t kSourceStreamId = 20U;
constexpr std::size_t kShard = 24U;
constexpr std::size_t kTradeDate = 28U;
constexpr std::size_t kOriginCaptureDate = 32U;
constexpr std::size_t kClockAlgorithm = 36U;
constexpr std::size_t kOriginStreamDayId = 40U;
constexpr std::size_t kClockDigest = 56U;
constexpr std::size_t kClockLabel = 88U;
constexpr std::size_t kSchemaSha256 = 96U;
constexpr std::size_t kDtypeSha256 = 128U;
constexpr std::size_t kRegistrySha256 = 160U;
constexpr std::size_t kBuildSha256 = 192U;
constexpr std::size_t kConfigSha256 = 224U;
constexpr std::size_t kRegistryVersion = 256U;
constexpr std::size_t kGeneration = 264U;
constexpr std::size_t kSegmentSequence = 272U;
constexpr std::size_t kCapacityRecords = 280U;
constexpr std::size_t kPublishedRecords = 288U;
constexpr std::size_t kFirstShardEventId = 296U;
constexpr std::size_t kLastShardEventId = 304U;
constexpr std::size_t kFirstOriginIngressSequence = 312U;
constexpr std::size_t kLastOriginIngressSequence = 320U;
constexpr std::size_t kFirstOriginWalEndPos = 328U;
constexpr std::size_t kLastOriginWalEndPos = 336U;
constexpr std::size_t kProcessedRawIngressSequence = 344U;
constexpr std::size_t kProcessedRawWalPos = 352U;
constexpr std::size_t kCreatedRealtimeNs = 360U;
constexpr std::size_t kCreatedMonotonicNs = 368U;
constexpr std::size_t kClosedRealtimeNs = 376U;
constexpr std::size_t kClosedMonotonicNs = 384U;
constexpr std::size_t kRecordStreamSha256 = 392U;
constexpr std::size_t kIntegritySha256 = 424U;
constexpr std::size_t kHeaderCrc32c = 456U;
constexpr std::size_t kOriginSourceWriterInstance = 460U;
constexpr std::size_t kOriginSourceGeneration = 476U;
constexpr std::size_t kReservedBegin = 484U;
}  // namespace header_offset

namespace control_offset {
constexpr std::size_t kSequence = 0U;
constexpr std::size_t kMagicAndVersion = 8U;
constexpr std::size_t kGeneration = 16U;
constexpr std::size_t kSegmentSequence = 24U;
constexpr std::size_t kRecordSize = 32U;
constexpr std::size_t kCapacityRecords = 40U;
constexpr std::size_t kPublishedRecords = 48U;
constexpr std::size_t kLastShardEventId = 56U;
constexpr std::size_t kLastOriginWalEndPos = 64U;
constexpr std::size_t kProcessedRawIngressSequence = 72U;
constexpr std::size_t kProcessedRawWalPos = 80U;
constexpr std::size_t kClosed = 88U;
constexpr std::size_t kNotifyEpoch = 96U;
constexpr std::size_t kSourceStreamId = 104U;
constexpr std::size_t kGenerationFatal = 112U;
}  // namespace control_offset

namespace manifest_offset {
constexpr std::size_t kMagic = 0U;
constexpr std::size_t kVersion = 4U;
constexpr std::size_t kManifestBytes = 6U;
constexpr std::size_t kEventType = 8U;
constexpr std::size_t kFlags = 10U;
constexpr std::size_t kRecordSize = 12U;
constexpr std::size_t kSourceStreamId = 16U;
constexpr std::size_t kShard = 20U;
constexpr std::size_t kTradeDate = 24U;
constexpr std::size_t kOriginCaptureDate = 28U;
constexpr std::size_t kOriginStreamDayId = 32U;
constexpr std::size_t kClockAlgorithm = 48U;
constexpr std::size_t kClockDigest = 56U;
constexpr std::size_t kClockLabel = 88U;
constexpr std::size_t kSchemaSha256 = 96U;
constexpr std::size_t kDtypeSha256 = 128U;
constexpr std::size_t kRegistrySha256 = 160U;
constexpr std::size_t kBuildSha256 = 192U;
constexpr std::size_t kConfigSha256 = 224U;
constexpr std::size_t kRegistryVersion = 256U;
constexpr std::size_t kGeneration = 264U;
constexpr std::size_t kSegmentSequence = 272U;
constexpr std::size_t kCapacityRecords = 280U;
constexpr std::size_t kPublishedRecords = 288U;
constexpr std::size_t kFirstShardEventId = 296U;
constexpr std::size_t kLastShardEventId = 304U;
constexpr std::size_t kFirstOriginIngressSequence = 312U;
constexpr std::size_t kLastOriginIngressSequence = 320U;
constexpr std::size_t kFirstOriginWalEndPos = 328U;
constexpr std::size_t kLastOriginWalEndPos = 336U;
constexpr std::size_t kProcessedRawIngressSequence = 344U;
constexpr std::size_t kProcessedRawWalPos = 352U;
constexpr std::size_t kClosedRealtimeNs = 360U;
constexpr std::size_t kClosedMonotonicNs = 368U;
constexpr std::size_t kRecordStreamSha256 = 376U;
constexpr std::size_t kIntegritySha256 = 408U;
constexpr std::size_t kHeaderSha256 = 440U;
constexpr std::size_t kSegmentFileBytes = 472U;
constexpr std::size_t kManifestCrc32c = 480U;
constexpr std::size_t kOriginSourceWriterInstance = 484U;
constexpr std::size_t kOriginSourceGeneration = 500U;
}  // namespace manifest_offset

using HeaderBytes =
    std::array<std::byte, kCanonicalSegmentHeaderBytesV1>;
using ManifestBytes =
    std::array<std::byte, kCanonicalSegmentManifestBytesV1>;

template <typename Integer>
void StoreLittleEndian(
    std::span<std::byte> bytes,
    std::size_t offset,
    Integer value) noexcept {
    static_assert(std::is_integral_v<Integer>);
    using Unsigned = std::make_unsigned_t<Integer>;
    Unsigned encoded{};
    if constexpr (std::is_signed_v<Integer>) {
        encoded = std::bit_cast<Unsigned>(value);
    } else {
        encoded = value;
    }
    for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            (encoded >> (index * 8U)) & Unsigned{0xffU});
    }
}

template <typename Integer>
[[nodiscard]] Integer LoadLittleEndian(
    std::span<const std::byte> bytes,
    std::size_t offset) noexcept {
    static_assert(std::is_integral_v<Integer>);
    using Unsigned = std::make_unsigned_t<Integer>;
    std::uint64_t accumulator = 0U;
    for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
        accumulator |= static_cast<std::uint64_t>(
                           std::to_integer<unsigned int>(
                               bytes[offset + index]))
                       << (index * 8U);
    }
    const Unsigned decoded = static_cast<Unsigned>(accumulator);
    if constexpr (std::is_signed_v<Integer>) {
        return std::bit_cast<Integer>(decoded);
    } else {
        return decoded;
    }
}

template <std::size_t Size>
void StoreArray(
    std::span<std::byte> bytes,
    std::size_t offset,
    const std::array<std::byte, Size>& value) noexcept {
    std::copy(value.begin(), value.end(), bytes.begin() +
              static_cast<std::ptrdiff_t>(offset));
}

template <std::size_t Size>
[[nodiscard]] std::array<std::byte, Size> LoadArray(
    std::span<const std::byte> bytes,
    std::size_t offset) noexcept {
    std::array<std::byte, Size> value{};
    std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                Size,
                value.begin());
    return value;
}

template <std::size_t Size>
[[nodiscard]] bool ArrayIsZero(
    const std::array<std::byte, Size>& value) noexcept {
    return std::all_of(value.begin(), value.end(), [](std::byte byte) {
        return byte == std::byte{0U};
    });
}

[[nodiscard]] bool BytesAreZero(
    std::span<const std::byte> bytes) noexcept {
    return std::all_of(bytes.begin(), bytes.end(), [](std::byte byte) {
        return byte == std::byte{0U};
    });
}

[[nodiscard]] std::uint32_t ExpectedRecordSize(
    CanonicalEventTypeV1 event_type) noexcept {
    switch (event_type) {
        case CanonicalEventTypeV1::kSnapshot:
            return static_cast<std::uint32_t>(
                kCanonicalSnapshotRecordBytesV1);
        case CanonicalEventTypeV1::kTick:
            return static_cast<std::uint32_t>(kCanonicalTickRecordBytesV1);
        case CanonicalEventTypeV1::kQuality:
            return static_cast<std::uint32_t>(
                kCanonicalQualityRecordBytesV1);
        case CanonicalEventTypeV1::kControl:
            return static_cast<std::uint32_t>(
                kCanonicalControlRecordBytesV1);
        case CanonicalEventTypeV1::kUnknown:
            return 0U;
    }
    return 0U;
}

[[nodiscard]] bool DescriptorHashesNonzero(
    const CanonicalSegmentDescriptorV1& descriptor) noexcept {
    return !ArrayIsZero(descriptor.schema_sha256) &&
           !ArrayIsZero(descriptor.dtype_sha256) &&
           !ArrayIsZero(descriptor.registry_sha256) &&
           !ArrayIsZero(descriptor.normalizer_build_sha256) &&
           !ArrayIsZero(descriptor.normalizer_config_sha256);
}

[[nodiscard]] CanonicalSegmentErrorV1 ValidateDescriptor(
    const CanonicalSegmentDescriptorV1& descriptor) noexcept {
    if (!CanonicalHostIsLittleEndianV1()) {
        return CanonicalSegmentErrorV1::kUnsupportedHostEndian;
    }
    if (descriptor.record_size == 0U ||
        descriptor.record_size != ExpectedRecordSize(descriptor.event_type) ||
        descriptor.source_stream_id == 0U ||
        descriptor.trade_date == 0U ||
        descriptor.origin_capture_date == 0U ||
        ArrayIsZero(descriptor.origin_stream_day_id) ||
        ArrayIsZero(descriptor.origin_source_writer_instance) ||
        descriptor.origin_source_generation == 0U ||
        !ClockEpochIdentityV1Valid(descriptor.clock_epoch) ||
        !DescriptorHashesNonzero(descriptor) ||
        descriptor.registry_version == 0U ||
        descriptor.generation == 0U ||
        descriptor.segment_sequence == 0U ||
        descriptor.capacity_records == 0U) {
        return CanonicalSegmentErrorV1::kInvalidDescriptor;
    }
    if (descriptor.schema_sha256 !=
        CanonicalSchemaDescriptorSha256V1()) {
        return CanonicalSegmentErrorV1::kSchemaHashMismatch;
    }
    if (descriptor.dtype_sha256 !=
        CanonicalDtypeDescriptorSha256V1()) {
        return CanonicalSegmentErrorV1::kDtypeHashMismatch;
    }
    return CanonicalSegmentErrorV1::kNone;
}

[[nodiscard]] bool DescriptorEqual(
    const CanonicalSegmentDescriptorV1& left,
    const CanonicalSegmentDescriptorV1& right) noexcept {
    return left.event_type == right.event_type &&
           left.record_size == right.record_size &&
           left.source_stream_id == right.source_stream_id &&
           left.shard == right.shard &&
           left.trade_date == right.trade_date &&
           left.origin_capture_date == right.origin_capture_date &&
           left.origin_stream_day_id == right.origin_stream_day_id &&
           left.origin_source_writer_instance ==
               right.origin_source_writer_instance &&
           left.origin_source_generation ==
               right.origin_source_generation &&
           left.clock_epoch == right.clock_epoch &&
           left.schema_sha256 == right.schema_sha256 &&
           left.dtype_sha256 == right.dtype_sha256 &&
           left.registry_version == right.registry_version &&
           left.registry_sha256 == right.registry_sha256 &&
           left.normalizer_build_sha256 ==
               right.normalizer_build_sha256 &&
           left.normalizer_config_sha256 ==
               right.normalizer_config_sha256 &&
           left.generation == right.generation &&
           left.segment_sequence == right.segment_sequence &&
           left.capacity_records == right.capacity_records;
}

// One Raw record has one exact end cursor.  Multiple Canonical records may
// share that origin (different sub_index), but the same ingress sequence may
// never be paired with two WAL positions.
[[nodiscard]] bool RawOriginAtOrAfter(
    std::uint64_t earlier_sequence,
    std::uint64_t earlier_wal_pos,
    std::uint64_t later_sequence,
    std::uint64_t later_wal_pos) noexcept {
    if ((earlier_sequence == 0U) != (earlier_wal_pos == 0U) ||
        (later_sequence == 0U) != (later_wal_pos == 0U)) {
        return false;
    }
    return later_sequence == earlier_sequence
        ? later_wal_pos == earlier_wal_pos
        : later_sequence > earlier_sequence &&
              later_wal_pos > earlier_wal_pos;
}

// A processed Raw prefix is different: a segment-header/epoch boundary may
// advance WAL while ingress stays unchanged, including the day-start prefix
// (0, header_end).  A greater ingress must strictly advance WAL.
[[nodiscard]] bool RawProgressAtOrAfter(
    std::uint64_t earlier_sequence,
    std::uint64_t earlier_wal_pos,
    std::uint64_t later_sequence,
    std::uint64_t later_wal_pos) noexcept {
    if ((earlier_sequence != 0U && earlier_wal_pos == 0U) ||
        (later_sequence != 0U && later_wal_pos == 0U)) {
        return false;
    }
    return later_sequence >= earlier_sequence &&
           later_wal_pos >= earlier_wal_pos &&
           (later_sequence == earlier_sequence ||
            later_wal_pos > earlier_wal_pos);
}

[[nodiscard]] CanonicalSegmentErrorV1 ComputeMappingBytes(
    const CanonicalSegmentDescriptorV1& descriptor,
    std::size_t* mapping_bytes) noexcept {
    if (mapping_bytes == nullptr || descriptor.record_size == 0U) {
        return CanonicalSegmentErrorV1::kInvalidArgument;
    }
    constexpr std::uint64_t prefix =
        static_cast<std::uint64_t>(kCanonicalSegmentDataOffsetV1);
    const std::uint64_t record_size = descriptor.record_size;
    if (descriptor.capacity_records >
        (std::numeric_limits<std::uint64_t>::max() - prefix) / record_size) {
        return CanonicalSegmentErrorV1::kSizeOverflow;
    }
    const std::uint64_t total =
        prefix + descriptor.capacity_records * record_size;
    if (total > static_cast<std::uint64_t>(
                    std::numeric_limits<std::size_t>::max()) ||
        total > static_cast<std::uint64_t>(
                    std::numeric_limits<off_t>::max())) {
        return CanonicalSegmentErrorV1::kSizeOverflow;
    }
    *mapping_bytes = static_cast<std::size_t>(total);
    return CanonicalSegmentErrorV1::kNone;
}

void EncodeHeader(
    const CanonicalSegmentHeaderViewV1& header,
    HeaderBytes* output) noexcept {
    output->fill(std::byte{0U});
    std::span<std::byte> bytes(*output);
    const CanonicalSegmentDescriptorV1& descriptor = header.descriptor;
    StoreLittleEndian(bytes, header_offset::kMagic, kSegmentMagic);
    StoreLittleEndian(bytes, header_offset::kVersion, kSegmentVersion);
    StoreLittleEndian(
        bytes,
        header_offset::kHeaderBytes,
        static_cast<std::uint16_t>(kCanonicalSegmentHeaderBytesV1));
    StoreLittleEndian(bytes, header_offset::kEndian, kEndianMarker);
    StoreLittleEndian(
        bytes,
        header_offset::kEventType,
        static_cast<std::uint16_t>(descriptor.event_type));
    StoreLittleEndian(
        bytes,
        header_offset::kFlags,
        header.sealed ? kSegmentFlagSealed : std::uint16_t{0U});
    StoreLittleEndian(bytes, header_offset::kRecordSize,
                      descriptor.record_size);
    StoreLittleEndian(bytes, header_offset::kSourceStreamId,
                      descriptor.source_stream_id);
    StoreLittleEndian(bytes, header_offset::kShard, descriptor.shard);
    StoreLittleEndian(bytes, header_offset::kTradeDate,
                      descriptor.trade_date);
    StoreLittleEndian(bytes, header_offset::kOriginCaptureDate,
                      descriptor.origin_capture_date);
    StoreLittleEndian(bytes, header_offset::kClockAlgorithm,
                      descriptor.clock_epoch.algorithm);
    StoreArray(bytes, header_offset::kOriginStreamDayId,
               descriptor.origin_stream_day_id);
    StoreArray(bytes, header_offset::kClockDigest,
               descriptor.clock_epoch.digest);
    StoreLittleEndian(bytes, header_offset::kClockLabel,
                      descriptor.clock_epoch.label);
    StoreArray(bytes, header_offset::kSchemaSha256,
               descriptor.schema_sha256);
    StoreArray(bytes, header_offset::kDtypeSha256,
               descriptor.dtype_sha256);
    StoreArray(bytes, header_offset::kRegistrySha256,
               descriptor.registry_sha256);
    StoreArray(bytes, header_offset::kBuildSha256,
               descriptor.normalizer_build_sha256);
    StoreArray(bytes, header_offset::kConfigSha256,
               descriptor.normalizer_config_sha256);
    StoreLittleEndian(bytes, header_offset::kRegistryVersion,
                      descriptor.registry_version);
    StoreLittleEndian(bytes, header_offset::kGeneration,
                      descriptor.generation);
    StoreLittleEndian(bytes, header_offset::kSegmentSequence,
                      descriptor.segment_sequence);
    StoreLittleEndian(bytes, header_offset::kCapacityRecords,
                      descriptor.capacity_records);
    StoreLittleEndian(bytes, header_offset::kPublishedRecords,
                      header.published_records);
    StoreLittleEndian(bytes, header_offset::kFirstShardEventId,
                      header.first_shard_event_id);
    StoreLittleEndian(bytes, header_offset::kLastShardEventId,
                      header.last_shard_event_id);
    StoreLittleEndian(bytes, header_offset::kFirstOriginIngressSequence,
                      header.first_origin_ingress_sequence);
    StoreLittleEndian(bytes, header_offset::kLastOriginIngressSequence,
                      header.last_origin_ingress_sequence);
    StoreLittleEndian(bytes, header_offset::kFirstOriginWalEndPos,
                      header.first_origin_wal_end_pos);
    StoreLittleEndian(bytes, header_offset::kLastOriginWalEndPos,
                      header.last_origin_wal_end_pos);
    StoreLittleEndian(bytes, header_offset::kProcessedRawIngressSequence,
                      header.processed_raw_ingress_sequence);
    StoreLittleEndian(bytes, header_offset::kProcessedRawWalPos,
                      header.processed_raw_wal_pos);
    StoreLittleEndian(bytes, header_offset::kCreatedRealtimeNs,
                      header.created_realtime_ns);
    StoreLittleEndian(bytes, header_offset::kCreatedMonotonicNs,
                      header.created_monotonic_ns);
    StoreLittleEndian(bytes, header_offset::kClosedRealtimeNs,
                      header.closed_realtime_ns);
    StoreLittleEndian(bytes, header_offset::kClosedMonotonicNs,
                      header.closed_monotonic_ns);
    StoreArray(bytes, header_offset::kRecordStreamSha256,
               header.record_stream_sha256);
    StoreArray(bytes, header_offset::kIntegritySha256,
               header.segment_integrity_sha256);
    StoreArray(bytes, header_offset::kOriginSourceWriterInstance,
               descriptor.origin_source_writer_instance);
    StoreLittleEndian(bytes, header_offset::kOriginSourceGeneration,
                      descriptor.origin_source_generation);
    StoreLittleEndian(bytes, header_offset::kHeaderCrc32c,
                      std::uint32_t{0U});
    const std::uint32_t crc = l2flow::common::ComputeCrc32c(bytes);
    StoreLittleEndian(bytes, header_offset::kHeaderCrc32c, crc);
}

[[nodiscard]] CanonicalSegmentErrorV1 DecodeHeader(
    const HeaderBytes& input,
    CanonicalSegmentHeaderViewV1* header) noexcept {
    if (header == nullptr) {
        return CanonicalSegmentErrorV1::kInvalidArgument;
    }
    const std::span<const std::byte> bytes(input);
    if (LoadLittleEndian<std::uint32_t>(bytes, header_offset::kMagic) !=
            kSegmentMagic ||
        LoadLittleEndian<std::uint16_t>(bytes, header_offset::kVersion) !=
            kSegmentVersion ||
        LoadLittleEndian<std::uint16_t>(bytes, header_offset::kHeaderBytes) !=
            kCanonicalSegmentHeaderBytesV1 ||
        LoadLittleEndian<std::uint32_t>(bytes, header_offset::kEndian) !=
            kEndianMarker ||
        !BytesAreZero(bytes.subspan(
            header_offset::kReservedBegin,
            kCanonicalSegmentHeaderBytesV1 -
                header_offset::kReservedBegin))) {
        return CanonicalSegmentErrorV1::kCorruptHeader;
    }
    HeaderBytes crc_bytes = input;
    const std::uint32_t stored_crc = LoadLittleEndian<std::uint32_t>(
        bytes, header_offset::kHeaderCrc32c);
    StoreLittleEndian(std::span<std::byte>(crc_bytes),
                      header_offset::kHeaderCrc32c,
                      std::uint32_t{0U});
    if (l2flow::common::ComputeCrc32c(
            std::span<const std::byte>(crc_bytes)) != stored_crc) {
        return CanonicalSegmentErrorV1::kCorruptHeader;
    }

    CanonicalSegmentHeaderViewV1 decoded{};
    CanonicalSegmentDescriptorV1& descriptor = decoded.descriptor;
    descriptor.event_type = static_cast<CanonicalEventTypeV1>(
        LoadLittleEndian<std::uint16_t>(bytes,
                                        header_offset::kEventType));
    const std::uint16_t flags = LoadLittleEndian<std::uint16_t>(
        bytes, header_offset::kFlags);
    if ((flags & ~kSegmentFlagSealed) != 0U) {
        return CanonicalSegmentErrorV1::kCorruptHeader;
    }
    decoded.sealed = (flags & kSegmentFlagSealed) != 0U;
    descriptor.record_size = LoadLittleEndian<std::uint32_t>(
        bytes, header_offset::kRecordSize);
    descriptor.source_stream_id = LoadLittleEndian<std::uint32_t>(
        bytes, header_offset::kSourceStreamId);
    descriptor.shard = LoadLittleEndian<std::uint32_t>(
        bytes, header_offset::kShard);
    descriptor.trade_date = LoadLittleEndian<std::uint32_t>(
        bytes, header_offset::kTradeDate);
    descriptor.origin_capture_date = LoadLittleEndian<std::uint32_t>(
        bytes, header_offset::kOriginCaptureDate);
    descriptor.clock_epoch.algorithm = LoadLittleEndian<std::uint32_t>(
        bytes, header_offset::kClockAlgorithm);
    descriptor.origin_stream_day_id = LoadArray<16U>(
        bytes, header_offset::kOriginStreamDayId);
    descriptor.origin_source_writer_instance = LoadArray<16U>(
        bytes, header_offset::kOriginSourceWriterInstance);
    descriptor.origin_source_generation = LoadLittleEndian<std::uint64_t>(
        bytes, header_offset::kOriginSourceGeneration);
    descriptor.clock_epoch.digest = LoadArray<32U>(
        bytes, header_offset::kClockDigest);
    descriptor.clock_epoch.label = LoadLittleEndian<std::uint64_t>(
        bytes, header_offset::kClockLabel);
    descriptor.schema_sha256 = LoadArray<32U>(
        bytes, header_offset::kSchemaSha256);
    descriptor.dtype_sha256 = LoadArray<32U>(
        bytes, header_offset::kDtypeSha256);
    descriptor.registry_sha256 = LoadArray<32U>(
        bytes, header_offset::kRegistrySha256);
    descriptor.normalizer_build_sha256 = LoadArray<32U>(
        bytes, header_offset::kBuildSha256);
    descriptor.normalizer_config_sha256 = LoadArray<32U>(
        bytes, header_offset::kConfigSha256);
    descriptor.registry_version = LoadLittleEndian<std::uint64_t>(
        bytes, header_offset::kRegistryVersion);
    descriptor.generation = LoadLittleEndian<std::uint64_t>(
        bytes, header_offset::kGeneration);
    descriptor.segment_sequence = LoadLittleEndian<std::uint64_t>(
        bytes, header_offset::kSegmentSequence);
    descriptor.capacity_records = LoadLittleEndian<std::uint64_t>(
        bytes, header_offset::kCapacityRecords);
    decoded.published_records = LoadLittleEndian<std::uint64_t>(
        bytes, header_offset::kPublishedRecords);
    decoded.first_shard_event_id = LoadLittleEndian<std::uint64_t>(
        bytes, header_offset::kFirstShardEventId);
    decoded.last_shard_event_id = LoadLittleEndian<std::uint64_t>(
        bytes, header_offset::kLastShardEventId);
    decoded.first_origin_ingress_sequence = LoadLittleEndian<std::uint64_t>(
        bytes, header_offset::kFirstOriginIngressSequence);
    decoded.last_origin_ingress_sequence = LoadLittleEndian<std::uint64_t>(
        bytes, header_offset::kLastOriginIngressSequence);
    decoded.first_origin_wal_end_pos = LoadLittleEndian<std::uint64_t>(
        bytes, header_offset::kFirstOriginWalEndPos);
    decoded.last_origin_wal_end_pos = LoadLittleEndian<std::uint64_t>(
        bytes, header_offset::kLastOriginWalEndPos);
    decoded.processed_raw_ingress_sequence =
        LoadLittleEndian<std::uint64_t>(
            bytes, header_offset::kProcessedRawIngressSequence);
    decoded.processed_raw_wal_pos = LoadLittleEndian<std::uint64_t>(
        bytes, header_offset::kProcessedRawWalPos);
    decoded.created_realtime_ns = LoadLittleEndian<std::int64_t>(
        bytes, header_offset::kCreatedRealtimeNs);
    decoded.created_monotonic_ns = LoadLittleEndian<std::int64_t>(
        bytes, header_offset::kCreatedMonotonicNs);
    decoded.closed_realtime_ns = LoadLittleEndian<std::int64_t>(
        bytes, header_offset::kClosedRealtimeNs);
    decoded.closed_monotonic_ns = LoadLittleEndian<std::int64_t>(
        bytes, header_offset::kClosedMonotonicNs);
    decoded.record_stream_sha256 = LoadArray<32U>(
        bytes, header_offset::kRecordStreamSha256);
    decoded.segment_integrity_sha256 = LoadArray<32U>(
        bytes, header_offset::kIntegritySha256);

    const CanonicalSegmentErrorV1 descriptor_error =
        ValidateDescriptor(descriptor);
    if (descriptor_error != CanonicalSegmentErrorV1::kNone) {
        return descriptor_error;
    }
    if (decoded.created_realtime_ns <= 0 ||
        decoded.created_monotonic_ns < 0 ||
        decoded.published_records > descriptor.capacity_records) {
        return CanonicalSegmentErrorV1::kCorruptHeader;
    }

    if (!decoded.sealed) {
        if (decoded.published_records != 0U ||
            decoded.first_shard_event_id != 0U ||
            decoded.last_shard_event_id != 0U ||
            decoded.first_origin_ingress_sequence != 0U ||
            decoded.last_origin_ingress_sequence != 0U ||
            decoded.first_origin_wal_end_pos != 0U ||
            decoded.last_origin_wal_end_pos != 0U ||
            decoded.processed_raw_ingress_sequence != 0U ||
            decoded.processed_raw_wal_pos != 0U ||
            decoded.closed_realtime_ns != 0 ||
            decoded.closed_monotonic_ns != 0 ||
            !ArrayIsZero(decoded.record_stream_sha256) ||
            !ArrayIsZero(decoded.segment_integrity_sha256)) {
            return CanonicalSegmentErrorV1::kCorruptHeader;
        }
    } else {
        const bool no_records = decoded.published_records == 0U;
        const bool summary_zero = decoded.first_shard_event_id == 0U &&
            decoded.last_shard_event_id == 0U &&
            decoded.first_origin_ingress_sequence == 0U &&
            decoded.last_origin_ingress_sequence == 0U &&
            decoded.first_origin_wal_end_pos == 0U &&
            decoded.last_origin_wal_end_pos == 0U;
        const bool processed_cursor_valid =
            decoded.processed_raw_ingress_sequence == 0U ||
            decoded.processed_raw_wal_pos != 0U;
        if (no_records != summary_zero || !processed_cursor_valid ||
            decoded.closed_realtime_ns <= 0 ||
            decoded.closed_monotonic_ns < decoded.created_monotonic_ns ||
            ArrayIsZero(decoded.record_stream_sha256) ||
            ArrayIsZero(decoded.segment_integrity_sha256)) {
            return CanonicalSegmentErrorV1::kCorruptHeader;
        }
        if (!no_records) {
            if (decoded.first_shard_event_id == 0U ||
                decoded.last_shard_event_id <
                    decoded.first_shard_event_id ||
                decoded.last_shard_event_id -
                        decoded.first_shard_event_id + 1U !=
                    decoded.published_records ||
                decoded.first_origin_ingress_sequence == 0U ||
                decoded.first_origin_wal_end_pos == 0U ||
                !RawOriginAtOrAfter(
                    decoded.first_origin_ingress_sequence,
                    decoded.first_origin_wal_end_pos,
                    decoded.last_origin_ingress_sequence,
                    decoded.last_origin_wal_end_pos) ||
                !RawProgressAtOrAfter(
                    decoded.last_origin_ingress_sequence,
                    decoded.last_origin_wal_end_pos,
                    decoded.processed_raw_ingress_sequence,
                    decoded.processed_raw_wal_pos)) {
                return CanonicalSegmentErrorV1::kCorruptHeader;
            }
        }
    }
    *header = decoded;
    return CanonicalSegmentErrorV1::kNone;
}

[[nodiscard]] std::uint64_t* ControlWord(
    std::byte* control,
    std::size_t offset) noexcept {
    return reinterpret_cast<std::uint64_t*>(control + offset);
}

[[nodiscard]] const std::uint64_t* ControlWord(
    const std::byte* control,
    std::size_t offset) noexcept {
    return reinterpret_cast<const std::uint64_t*>(control + offset);
}

void AtomicStore(
    std::byte* control,
    std::size_t offset,
    std::uint64_t value,
    int order) noexcept {
    __atomic_store_n(ControlWord(control, offset), value, order);
}

[[nodiscard]] std::uint64_t AtomicLoad(
    const std::byte* control,
    std::size_t offset,
    int order) noexcept {
    return __atomic_load_n(ControlWord(control, offset), order);
}

void InitializeControl(
    std::byte* control,
    const CanonicalSegmentDescriptorV1& descriptor) noexcept {
    std::memset(control, 0, kCanonicalSegmentControlBytesV1);
    AtomicStore(control, control_offset::kMagicAndVersion,
                kControlMagicAndVersion, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kGeneration,
                descriptor.generation, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kSegmentSequence,
                descriptor.segment_sequence, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kRecordSize,
                descriptor.record_size, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kCapacityRecords,
                descriptor.capacity_records, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kPublishedRecords,
                std::uint64_t{0U}, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kLastShardEventId,
                std::uint64_t{0U}, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kLastOriginWalEndPos,
                std::uint64_t{0U}, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kProcessedRawIngressSequence,
                std::uint64_t{0U}, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kProcessedRawWalPos,
                std::uint64_t{0U}, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kClosed,
                std::uint64_t{0U}, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kNotifyEpoch,
                std::uint64_t{0U}, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kSourceStreamId,
                descriptor.source_stream_id, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kGenerationFatal,
                std::uint64_t{0U}, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kSequence,
                std::uint64_t{0U}, __ATOMIC_RELEASE);
}

[[nodiscard]] CanonicalSegmentErrorV1 ReadControlPage(
    const std::byte* control,
    const CanonicalSegmentDescriptorV1& descriptor,
    CanonicalSegmentControlSnapshotV1* snapshot) noexcept {
    if (control == nullptr || snapshot == nullptr) {
        return CanonicalSegmentErrorV1::kInvalidArgument;
    }
    if (!__atomic_always_lock_free(sizeof(std::uint64_t), nullptr)) {
        return CanonicalSegmentErrorV1::kUnsupportedHostEndian;
    }
    for (std::size_t attempt = 0U; attempt < 100000U; ++attempt) {
        const std::uint64_t first = AtomicLoad(
            control, control_offset::kSequence, __ATOMIC_ACQUIRE);
        if ((first & 1U) != 0U) {
            continue;
        }
        const std::uint64_t magic = AtomicLoad(
            control, control_offset::kMagicAndVersion, __ATOMIC_RELAXED);
        const std::uint64_t generation = AtomicLoad(
            control, control_offset::kGeneration, __ATOMIC_RELAXED);
        const std::uint64_t segment_sequence = AtomicLoad(
            control, control_offset::kSegmentSequence, __ATOMIC_RELAXED);
        const std::uint64_t record_size = AtomicLoad(
            control, control_offset::kRecordSize, __ATOMIC_RELAXED);
        const std::uint64_t capacity = AtomicLoad(
            control, control_offset::kCapacityRecords, __ATOMIC_RELAXED);
        CanonicalSegmentControlSnapshotV1 observed{};
        observed.published_records = AtomicLoad(
            control, control_offset::kPublishedRecords, __ATOMIC_RELAXED);
        observed.last_shard_event_id = AtomicLoad(
            control, control_offset::kLastShardEventId, __ATOMIC_RELAXED);
        observed.last_origin_wal_end_pos = AtomicLoad(
            control, control_offset::kLastOriginWalEndPos,
            __ATOMIC_RELAXED);
        observed.processed_raw_ingress_sequence = AtomicLoad(
            control, control_offset::kProcessedRawIngressSequence,
            __ATOMIC_RELAXED);
        observed.processed_raw_wal_pos = AtomicLoad(
            control, control_offset::kProcessedRawWalPos,
            __ATOMIC_RELAXED);
        const std::uint64_t closed = AtomicLoad(
            control, control_offset::kClosed, __ATOMIC_RELAXED);
        observed.notify_epoch = AtomicLoad(
            control, control_offset::kNotifyEpoch, __ATOMIC_RELAXED);
        const std::uint64_t source_stream_id = AtomicLoad(
            control, control_offset::kSourceStreamId, __ATOMIC_RELAXED);
        const std::uint64_t generation_fatal = AtomicLoad(
            control, control_offset::kGenerationFatal, __ATOMIC_RELAXED);
        const std::uint64_t second = AtomicLoad(
            control, control_offset::kSequence, __ATOMIC_ACQUIRE);
        if (first != second || (second & 1U) != 0U) {
            continue;
        }
        if (magic != kControlMagicAndVersion ||
            generation != descriptor.generation ||
            segment_sequence != descriptor.segment_sequence ||
            record_size != descriptor.record_size ||
            capacity != descriptor.capacity_records ||
            source_stream_id != descriptor.source_stream_id ||
            observed.published_records > capacity || closed > 1U ||
            generation_fatal > 1U ||
            (observed.published_records == 0U &&
             (observed.last_shard_event_id != 0U ||
              observed.last_origin_wal_end_pos != 0U)) ||
            (observed.published_records != 0U &&
             (observed.last_shard_event_id == 0U ||
              observed.last_origin_wal_end_pos == 0U)) ||
            (observed.processed_raw_ingress_sequence != 0U &&
             observed.processed_raw_wal_pos == 0U)) {
            return CanonicalSegmentErrorV1::kCorruptControlPage;
        }
        observed.closed = closed != 0U;
        observed.generation_fatal = generation_fatal != 0U;
        *snapshot = observed;
        return CanonicalSegmentErrorV1::kNone;
    }
    return CanonicalSegmentErrorV1::kControlBusy;
}

[[nodiscard]] CanonicalSegmentErrorV1 PublishControlPage(
    std::byte* control,
    const CanonicalSegmentDescriptorV1& descriptor,
    const CanonicalSegmentControlSnapshotV1& snapshot) noexcept {
    if (control == nullptr ||
        snapshot.published_records > descriptor.capacity_records ||
        (snapshot.processed_raw_ingress_sequence != 0U &&
         snapshot.processed_raw_wal_pos == 0U)) {
        return CanonicalSegmentErrorV1::kInvalidArgument;
    }
    std::uint64_t even = AtomicLoad(
        control, control_offset::kSequence, __ATOMIC_ACQUIRE);
    if ((even & 1U) != 0U ||
        even > std::numeric_limits<std::uint64_t>::max() - 2U) {
        return CanonicalSegmentErrorV1::kCorruptControlPage;
    }
    const std::uint64_t odd = even + 1U;
    if (!__atomic_compare_exchange_n(
            ControlWord(control, control_offset::kSequence),
            &even,
            odd,
            false,
            __ATOMIC_ACQ_REL,
            __ATOMIC_ACQUIRE)) {
        return CanonicalSegmentErrorV1::kControlBusy;
    }
    AtomicStore(control, control_offset::kPublishedRecords,
                snapshot.published_records, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kLastShardEventId,
                snapshot.last_shard_event_id, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kLastOriginWalEndPos,
                snapshot.last_origin_wal_end_pos, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kProcessedRawIngressSequence,
                snapshot.processed_raw_ingress_sequence,
                __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kProcessedRawWalPos,
                snapshot.processed_raw_wal_pos, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kClosed,
                snapshot.closed ? std::uint64_t{1U} : std::uint64_t{0U},
                __ATOMIC_RELAXED);
    AtomicStore(
        control,
        control_offset::kGenerationFatal,
        snapshot.generation_fatal ? std::uint64_t{1U} : std::uint64_t{0U},
        __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kNotifyEpoch,
                snapshot.notify_epoch, __ATOMIC_RELAXED);
    AtomicStore(control, control_offset::kSequence,
                odd + 1U, __ATOMIC_RELEASE);

    auto* notify_address = reinterpret_cast<std::uint32_t*>(
        control + control_offset::kNotifyEpoch);
    static_cast<void>(::syscall(
        SYS_futex,
        notify_address,
        FUTEX_WAKE,
        INT_MAX,
        nullptr,
        nullptr,
        0));
    return CanonicalSegmentErrorV1::kNone;
}

[[nodiscard]] bool CloseDescriptor(int descriptor) noexcept {
    if (descriptor < 0) {
        return true;
    }
    if (::close(descriptor) == 0) {
        return true;
    }
    // On Linux the descriptor state after EINTR is not safe to retry.
    return false;
}

[[nodiscard]] bool FsyncLoop(int descriptor) noexcept {
    for (;;) {
        if (::fsync(descriptor) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool MsyncLoop(
    void* mapping,
    std::size_t mapping_bytes) noexcept {
    for (;;) {
        if (::msync(mapping, mapping_bytes, MS_SYNC) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

[[nodiscard]] bool PreadExact(
    int descriptor,
    std::span<std::byte> bytes,
    std::uint64_t offset) noexcept {
    std::size_t consumed = 0U;
    while (consumed < bytes.size()) {
        if (offset + consumed > static_cast<std::uint64_t>(
                                    std::numeric_limits<off_t>::max())) {
            return false;
        }
        const ssize_t count = ::pread(
            descriptor,
            bytes.data() + consumed,
            bytes.size() - consumed,
            static_cast<off_t>(offset + consumed));
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (count == 0) {
            return false;
        }
        consumed += static_cast<std::size_t>(count);
    }
    return true;
}

[[nodiscard]] bool WriteExact(
    int descriptor,
    std::span<const std::byte> bytes) noexcept {
    std::size_t consumed = 0U;
    while (consumed < bytes.size()) {
        const ssize_t count = ::write(
            descriptor,
            bytes.data() + consumed,
            bytes.size() - consumed);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (count == 0) {
            return false;
        }
        consumed += static_cast<std::size_t>(count);
    }
    return true;
}

[[nodiscard]] CanonicalSegmentErrorV1 ValidateRegularFile(
    int descriptor,
    std::size_t expected_size,
    bool require_read_only) noexcept {
    struct stat status {};
    const int flags = ::fcntl(descriptor, F_GETFL);
    if (::fstat(descriptor, &status) != 0 || flags < 0) {
        return CanonicalSegmentErrorV1::kIoError;
    }
    if (!S_ISREG(status.st_mode) || status.st_uid != ::geteuid() ||
        status.st_nlink != 1) {
        return CanonicalSegmentErrorV1::kWrongFileType;
    }
    if ((status.st_mode & 07777) != 0600) {
        return CanonicalSegmentErrorV1::kWrongFileMode;
    }
    if (status.st_size < 0 ||
        static_cast<std::uint64_t>(status.st_size) !=
            static_cast<std::uint64_t>(expected_size)) {
        return CanonicalSegmentErrorV1::kCorruptHeader;
    }
    const int access_mode = flags & O_ACCMODE;
    if ((require_read_only && access_mode != O_RDONLY) ||
        (!require_read_only && access_mode != O_RDWR) ||
        (flags & O_APPEND) != 0) {
        return CanonicalSegmentErrorV1::kWrongFileMode;
    }
    return CanonicalSegmentErrorV1::kNone;
}

[[nodiscard]] CanonicalValidationErrorV1 ValidateRecordBytes(
    CanonicalEventTypeV1 event_type,
    std::span<const std::byte> bytes) noexcept {
    switch (event_type) {
        case CanonicalEventTypeV1::kSnapshot: {
            if (bytes.size() != sizeof(CanonicalSnapshotRecordV1)) {
                return CanonicalValidationErrorV1::kInvalidRecordSize;
            }
            CanonicalSnapshotRecordV1 record{};
            std::memcpy(&record, bytes.data(), sizeof(record));
            return ValidateCanonicalSnapshotRecordV1(record);
        }
        case CanonicalEventTypeV1::kTick: {
            if (bytes.size() != sizeof(CanonicalTickRecordV1)) {
                return CanonicalValidationErrorV1::kInvalidRecordSize;
            }
            CanonicalTickRecordV1 record{};
            std::memcpy(&record, bytes.data(), sizeof(record));
            return ValidateCanonicalTickRecordV1(record);
        }
        case CanonicalEventTypeV1::kQuality: {
            if (bytes.size() != sizeof(CanonicalQualityRecordV1)) {
                return CanonicalValidationErrorV1::kInvalidRecordSize;
            }
            CanonicalQualityRecordV1 record{};
            std::memcpy(&record, bytes.data(), sizeof(record));
            return ValidateCanonicalQualityRecordV1(record);
        }
        case CanonicalEventTypeV1::kControl: {
            if (bytes.size() != sizeof(CanonicalControlRecordV1)) {
                return CanonicalValidationErrorV1::kInvalidRecordSize;
            }
            CanonicalControlRecordV1 record{};
            std::memcpy(&record, bytes.data(), sizeof(record));
            return ValidateCanonicalControlRecordV1(record);
        }
        case CanonicalEventTypeV1::kUnknown:
            return CanonicalValidationErrorV1::kUnknownEventType;
    }
    return CanonicalValidationErrorV1::kUnknownEventType;
}

[[nodiscard]] CanonicalHeaderV1 LoadCanonicalRecordHeader(
    std::span<const std::byte> record_bytes) noexcept {
    CanonicalHeaderV1 header{};
    if (record_bytes.size() >= sizeof(header)) {
        std::memcpy(&header, record_bytes.data(), sizeof(header));
    }
    return header;
}

[[nodiscard]] l2flow::common::Sha256Digest ComputeIntegrityDigest(
    HeaderBytes header_bytes,
    std::span<const std::byte> records) noexcept {
    std::fill_n(
        header_bytes.begin() +
            static_cast<std::ptrdiff_t>(header_offset::kIntegritySha256),
        l2flow::common::Sha256Digest{}.size(),
        std::byte{0U});
    StoreLittleEndian(std::span<std::byte>(header_bytes),
                      header_offset::kHeaderCrc32c,
                      std::uint32_t{0U});
    l2flow::common::Sha256Hasher hasher;
    l2flow::common::Sha256Digest digest{};
    const auto domain_bytes = std::as_bytes(std::span(
        kIntegrityDomain.data(), kIntegrityDomain.size()));
    if (!hasher.Update(domain_bytes) ||
        !hasher.Update(std::span<const std::byte>(header_bytes)) ||
        !hasher.Update(records) || !hasher.Finalize(&digest)) {
        return {};
    }
    return digest;
}

[[nodiscard]] CanonicalSegmentErrorV1 VerifySealedHashes(
    const std::byte* mapping,
    const CanonicalSegmentHeaderViewV1& header,
    const HeaderBytes& header_bytes) noexcept {
    if (mapping == nullptr || !header.sealed ||
        header.published_records > header.descriptor.capacity_records) {
        return CanonicalSegmentErrorV1::kCorruptHeader;
    }
    const std::uint64_t record_bytes_u64 =
        header.published_records * header.descriptor.record_size;
    if (record_bytes_u64 > static_cast<std::uint64_t>(
                               std::numeric_limits<std::size_t>::max())) {
        return CanonicalSegmentErrorV1::kSizeOverflow;
    }
    const std::span<const std::byte> records(
        mapping + kCanonicalSegmentDataOffsetV1,
        static_cast<std::size_t>(record_bytes_u64));
    if (l2flow::common::ComputeSha256(records) !=
        header.record_stream_sha256) {
        return CanonicalSegmentErrorV1::kHashMismatch;
    }
    if (ComputeIntegrityDigest(header_bytes, records) !=
        header.segment_integrity_sha256) {
        return CanonicalSegmentErrorV1::kHashMismatch;
    }
    return CanonicalSegmentErrorV1::kNone;
}

void BuildManifestBytes(
    const CanonicalSegmentHeaderViewV1& header,
    const l2flow::common::Sha256Digest& header_sha256,
    ManifestBytes* output) noexcept {
    output->fill(std::byte{0U});
    std::span<std::byte> bytes(*output);
    const CanonicalSegmentDescriptorV1& descriptor = header.descriptor;
    StoreLittleEndian(bytes, manifest_offset::kMagic, kManifestMagic);
    StoreLittleEndian(bytes, manifest_offset::kVersion, kManifestVersion);
    StoreLittleEndian(
        bytes,
        manifest_offset::kManifestBytes,
        static_cast<std::uint16_t>(kCanonicalSegmentManifestBytesV1));
    StoreLittleEndian(bytes, manifest_offset::kEventType,
                      static_cast<std::uint16_t>(descriptor.event_type));
    StoreLittleEndian(bytes, manifest_offset::kFlags,
                      kSegmentFlagSealed);
    StoreLittleEndian(bytes, manifest_offset::kRecordSize,
                      descriptor.record_size);
    StoreLittleEndian(bytes, manifest_offset::kSourceStreamId,
                      descriptor.source_stream_id);
    StoreLittleEndian(bytes, manifest_offset::kShard, descriptor.shard);
    StoreLittleEndian(bytes, manifest_offset::kTradeDate,
                      descriptor.trade_date);
    StoreLittleEndian(bytes, manifest_offset::kOriginCaptureDate,
                      descriptor.origin_capture_date);
    StoreArray(bytes, manifest_offset::kOriginStreamDayId,
               descriptor.origin_stream_day_id);
    StoreLittleEndian(bytes, manifest_offset::kClockAlgorithm,
                      descriptor.clock_epoch.algorithm);
    StoreArray(bytes, manifest_offset::kClockDigest,
               descriptor.clock_epoch.digest);
    StoreLittleEndian(bytes, manifest_offset::kClockLabel,
                      descriptor.clock_epoch.label);
    StoreArray(bytes, manifest_offset::kSchemaSha256,
               descriptor.schema_sha256);
    StoreArray(bytes, manifest_offset::kDtypeSha256,
               descriptor.dtype_sha256);
    StoreArray(bytes, manifest_offset::kRegistrySha256,
               descriptor.registry_sha256);
    StoreArray(bytes, manifest_offset::kBuildSha256,
               descriptor.normalizer_build_sha256);
    StoreArray(bytes, manifest_offset::kConfigSha256,
               descriptor.normalizer_config_sha256);
    StoreLittleEndian(bytes, manifest_offset::kRegistryVersion,
                      descriptor.registry_version);
    StoreLittleEndian(bytes, manifest_offset::kGeneration,
                      descriptor.generation);
    StoreLittleEndian(bytes, manifest_offset::kSegmentSequence,
                      descriptor.segment_sequence);
    StoreLittleEndian(bytes, manifest_offset::kCapacityRecords,
                      descriptor.capacity_records);
    StoreLittleEndian(bytes, manifest_offset::kPublishedRecords,
                      header.published_records);
    StoreLittleEndian(bytes, manifest_offset::kFirstShardEventId,
                      header.first_shard_event_id);
    StoreLittleEndian(bytes, manifest_offset::kLastShardEventId,
                      header.last_shard_event_id);
    StoreLittleEndian(bytes, manifest_offset::kFirstOriginIngressSequence,
                      header.first_origin_ingress_sequence);
    StoreLittleEndian(bytes, manifest_offset::kLastOriginIngressSequence,
                      header.last_origin_ingress_sequence);
    StoreLittleEndian(bytes, manifest_offset::kFirstOriginWalEndPos,
                      header.first_origin_wal_end_pos);
    StoreLittleEndian(bytes, manifest_offset::kLastOriginWalEndPos,
                      header.last_origin_wal_end_pos);
    StoreLittleEndian(bytes, manifest_offset::kProcessedRawIngressSequence,
                      header.processed_raw_ingress_sequence);
    StoreLittleEndian(bytes, manifest_offset::kProcessedRawWalPos,
                      header.processed_raw_wal_pos);
    StoreLittleEndian(bytes, manifest_offset::kClosedRealtimeNs,
                      header.closed_realtime_ns);
    StoreLittleEndian(bytes, manifest_offset::kClosedMonotonicNs,
                      header.closed_monotonic_ns);
    StoreArray(bytes, manifest_offset::kRecordStreamSha256,
               header.record_stream_sha256);
    StoreArray(bytes, manifest_offset::kIntegritySha256,
               header.segment_integrity_sha256);
    StoreArray(bytes, manifest_offset::kHeaderSha256, header_sha256);
    StoreArray(bytes, manifest_offset::kOriginSourceWriterInstance,
               descriptor.origin_source_writer_instance);
    StoreLittleEndian(bytes, manifest_offset::kOriginSourceGeneration,
                      descriptor.origin_source_generation);
    std::size_t segment_file_bytes = 0U;
    if (ComputeMappingBytes(descriptor, &segment_file_bytes) ==
        CanonicalSegmentErrorV1::kNone) {
        StoreLittleEndian(bytes, manifest_offset::kSegmentFileBytes,
                          static_cast<std::uint64_t>(segment_file_bytes));
    }
    StoreLittleEndian(bytes, manifest_offset::kManifestCrc32c,
                      std::uint32_t{0U});
    const std::uint32_t crc = l2flow::common::ComputeCrc32c(bytes);
    StoreLittleEndian(bytes, manifest_offset::kManifestCrc32c, crc);
}

[[nodiscard]] CanonicalSegmentErrorV1 OpenManifestDirectory(
    const std::filesystem::path& manifest_path,
    int* directory_descriptor,
    std::string* final_name,
    std::string* temporary_name) {
    if (directory_descriptor == nullptr || final_name == nullptr ||
        temporary_name == nullptr || manifest_path.empty()) {
        return CanonicalSegmentErrorV1::kInvalidArgument;
    }
    const std::filesystem::path filename = manifest_path.filename();
    if (filename.empty() || filename == "." || filename == "..") {
        return CanonicalSegmentErrorV1::kInvalidArgument;
    }
    *final_name = filename.string();
    *temporary_name = *final_name + ".tmp";
    std::filesystem::path parent = manifest_path.parent_path();
    if (parent.empty()) {
        parent = ".";
    }
    int descriptor = -1;
    do {
        descriptor = ::open(parent.c_str(),
                            O_RDONLY | O_DIRECTORY | O_CLOEXEC |
                                O_NOFOLLOW | O_NONBLOCK);
    } while (descriptor < 0 && errno == EINTR);
    if (descriptor < 0) {
        return CanonicalSegmentErrorV1::kOpenFailed;
    }
    *directory_descriptor = descriptor;
    return CanonicalSegmentErrorV1::kNone;
}

[[nodiscard]] bool SyncParentDirectory(
    const std::filesystem::path& path) {
    std::filesystem::path parent = path.parent_path();
    if (parent.empty()) {
        parent = ".";
    }
    int descriptor = -1;
    do {
        descriptor = ::open(parent.c_str(),
                            O_RDONLY | O_DIRECTORY | O_CLOEXEC |
                                O_NOFOLLOW | O_NONBLOCK);
    } while (descriptor < 0 && errno == EINTR);
    if (descriptor < 0) {
        return false;
    }
    const bool synced = FsyncLoop(descriptor);
    const bool closed = CloseDescriptor(descriptor);
    return synced && closed;
}

[[nodiscard]] CanonicalSegmentErrorV1 ReadFixedManifestAt(
    int directory_descriptor,
    const std::string& name,
    ManifestBytes* bytes,
    bool writable) noexcept {
    if (bytes == nullptr) {
        return CanonicalSegmentErrorV1::kInvalidArgument;
    }
    const int access = writable ? O_RDWR : O_RDONLY;
    int descriptor = -1;
    do {
        descriptor = ::openat(directory_descriptor,
                              name.c_str(),
                              access | O_CLOEXEC | O_NOFOLLOW |
                                  O_NONBLOCK | O_NOCTTY);
    } while (descriptor < 0 && errno == EINTR);
    if (descriptor < 0) {
        return CanonicalSegmentErrorV1::kOpenFailed;
    }
    const CanonicalSegmentErrorV1 file_error = ValidateRegularFile(
        descriptor, kCanonicalSegmentManifestBytesV1, !writable);
    if (file_error != CanonicalSegmentErrorV1::kNone) {
        static_cast<void>(CloseDescriptor(descriptor));
        return file_error;
    }
    const bool read_ok = PreadExact(
        descriptor, std::span<std::byte>(*bytes), std::uint64_t{0U});
    const bool sync_ok = !writable || FsyncLoop(descriptor);
    const bool close_ok = CloseDescriptor(descriptor);
    if (!read_ok || !sync_ok || !close_ok) {
        return CanonicalSegmentErrorV1::kIoError;
    }
    return CanonicalSegmentErrorV1::kNone;
}

[[nodiscard]] CanonicalSegmentErrorV1 PublishManifest(
    const std::filesystem::path& manifest_path,
    const ManifestBytes& expected) {
    int directory_descriptor = -1;
    std::string final_name;
    std::string temporary_name;
    const CanonicalSegmentErrorV1 directory_error = OpenManifestDirectory(
        manifest_path,
        &directory_descriptor,
        &final_name,
        &temporary_name);
    if (directory_error != CanonicalSegmentErrorV1::kNone) {
        return directory_error;
    }

    ManifestBytes final_bytes{};
    const CanonicalSegmentErrorV1 final_read = ReadFixedManifestAt(
        directory_descriptor, final_name, &final_bytes, true);
    if (final_read == CanonicalSegmentErrorV1::kNone) {
        ManifestBytes unexpected_tmp{};
        const CanonicalSegmentErrorV1 tmp_read = ReadFixedManifestAt(
            directory_descriptor,
            temporary_name,
            &unexpected_tmp,
            false);
        const bool tmp_absent =
            tmp_read == CanonicalSegmentErrorV1::kOpenFailed &&
            errno == ENOENT;
        const bool exact = final_bytes == expected;
        const bool directory_sync = exact && tmp_absent &&
            FsyncLoop(directory_descriptor);
        static_cast<void>(CloseDescriptor(directory_descriptor));
        return directory_sync
                   ? CanonicalSegmentErrorV1::kNone
                   : CanonicalSegmentErrorV1::kManifestConflict;
    }
    if (!(final_read == CanonicalSegmentErrorV1::kOpenFailed &&
          errno == ENOENT)) {
        static_cast<void>(CloseDescriptor(directory_descriptor));
        return CanonicalSegmentErrorV1::kManifestConflict;
    }

    ManifestBytes tmp_bytes{};
    const CanonicalSegmentErrorV1 tmp_read = ReadFixedManifestAt(
        directory_descriptor, temporary_name, &tmp_bytes, true);
    if (tmp_read == CanonicalSegmentErrorV1::kNone) {
        if (tmp_bytes != expected) {
            static_cast<void>(CloseDescriptor(directory_descriptor));
            return CanonicalSegmentErrorV1::kManifestConflict;
        }
    } else if (tmp_read == CanonicalSegmentErrorV1::kOpenFailed &&
               errno == ENOENT) {
        int temporary_descriptor = -1;
        do {
            temporary_descriptor = ::openat(
                directory_descriptor,
                temporary_name.c_str(),
                O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC |
                    O_NOFOLLOW | O_NONBLOCK | O_NOCTTY,
                0600);
        } while (temporary_descriptor < 0 && errno == EINTR);
        if (temporary_descriptor < 0) {
            static_cast<void>(CloseDescriptor(directory_descriptor));
            return errno == EEXIST
                       ? CanonicalSegmentErrorV1::kManifestConflict
                       : CanonicalSegmentErrorV1::kOpenFailed;
        }
        const bool mode_ok = ::fchmod(temporary_descriptor, 0600) == 0;
        const bool write_ok = mode_ok && WriteExact(
            temporary_descriptor, std::span<const std::byte>(expected));
        const bool sync_ok = write_ok && FsyncLoop(temporary_descriptor);
        const bool close_ok = CloseDescriptor(temporary_descriptor);
        if (!sync_ok || !close_ok) {
            static_cast<void>(CloseDescriptor(directory_descriptor));
            return CanonicalSegmentErrorV1::kIoError;
        }
    } else {
        static_cast<void>(CloseDescriptor(directory_descriptor));
        return CanonicalSegmentErrorV1::kManifestConflict;
    }

    const long rename_result = ::syscall(
        SYS_renameat2,
        directory_descriptor,
        temporary_name.c_str(),
        directory_descriptor,
        final_name.c_str(),
        RENAME_NOREPLACE);
    if (rename_result != 0) {
        static_cast<void>(CloseDescriptor(directory_descriptor));
        return errno == EEXIST
                   ? CanonicalSegmentErrorV1::kManifestConflict
                   : CanonicalSegmentErrorV1::kIoError;
    }
    if (!FsyncLoop(directory_descriptor)) {
        static_cast<void>(CloseDescriptor(directory_descriptor));
        return CanonicalSegmentErrorV1::kIoError;
    }
    ManifestBytes readback{};
    const CanonicalSegmentErrorV1 readback_error = ReadFixedManifestAt(
        directory_descriptor, final_name, &readback, false);
    const bool close_ok = CloseDescriptor(directory_descriptor);
    if (readback_error != CanonicalSegmentErrorV1::kNone ||
        readback != expected || !close_ok) {
        return CanonicalSegmentErrorV1::kIoError;
    }
    return CanonicalSegmentErrorV1::kNone;
}

[[nodiscard]] std::span<std::byte> MutableMappingSpan(
    void* mapping,
    std::size_t mapping_bytes) noexcept {
    return {static_cast<std::byte*>(mapping), mapping_bytes};
}

[[nodiscard]] std::span<const std::byte> MappingSpan(
    const void* mapping,
    std::size_t mapping_bytes) noexcept {
    return {static_cast<const std::byte*>(mapping), mapping_bytes};
}

[[nodiscard]] CanonicalSegmentControlSnapshotV1 SealedControlSnapshot(
    const CanonicalSegmentHeaderViewV1& header) noexcept {
    CanonicalSegmentControlSnapshotV1 snapshot{};
    snapshot.published_records = header.published_records;
    snapshot.last_shard_event_id = header.last_shard_event_id;
    snapshot.last_origin_wal_end_pos = header.last_origin_wal_end_pos;
    snapshot.processed_raw_ingress_sequence =
        header.processed_raw_ingress_sequence;
    snapshot.processed_raw_wal_pos = header.processed_raw_wal_pos;
    snapshot.closed = true;
    return snapshot;
}

}  // namespace

std::string_view CanonicalSegmentErrorNameV1(
    CanonicalSegmentErrorV1 error) noexcept {
    switch (error) {
        case CanonicalSegmentErrorV1::kNone:
            return "none";
        case CanonicalSegmentErrorV1::kInvalidArgument:
            return "invalid_argument";
        case CanonicalSegmentErrorV1::kUnsupportedHostEndian:
            return "unsupported_host_endian";
        case CanonicalSegmentErrorV1::kInvalidDescriptor:
            return "invalid_descriptor";
        case CanonicalSegmentErrorV1::kSchemaHashMismatch:
            return "schema_hash_mismatch";
        case CanonicalSegmentErrorV1::kDtypeHashMismatch:
            return "dtype_hash_mismatch";
        case CanonicalSegmentErrorV1::kAlreadyExists:
            return "already_exists";
        case CanonicalSegmentErrorV1::kOpenFailed:
            return "open_failed";
        case CanonicalSegmentErrorV1::kWrongFileType:
            return "wrong_file_type";
        case CanonicalSegmentErrorV1::kWrongFileMode:
            return "wrong_file_mode";
        case CanonicalSegmentErrorV1::kSingleWriterLockUnavailable:
            return "single_writer_lock_unavailable";
        case CanonicalSegmentErrorV1::kSizeOverflow:
            return "size_overflow";
        case CanonicalSegmentErrorV1::kPreallocationFailed:
            return "preallocation_failed";
        case CanonicalSegmentErrorV1::kMapFailed:
            return "map_failed";
        case CanonicalSegmentErrorV1::kIoError:
            return "io_error";
        case CanonicalSegmentErrorV1::kCorruptHeader:
            return "corrupt_header";
        case CanonicalSegmentErrorV1::kHeaderIdentityMismatch:
            return "header_identity_mismatch";
        case CanonicalSegmentErrorV1::kCorruptControlPage:
            return "corrupt_control_page";
        case CanonicalSegmentErrorV1::kControlBusy:
            return "control_busy";
        case CanonicalSegmentErrorV1::kSegmentFull:
            return "segment_full";
        case CanonicalSegmentErrorV1::kSegmentSealed:
            return "segment_sealed";
        case CanonicalSegmentErrorV1::kGenerationFatal:
            return "generation_fatal";
        case CanonicalSegmentErrorV1::kInvalidRecord:
            return "invalid_record";
        case CanonicalSegmentErrorV1::kRecordIdentityMismatch:
            return "record_identity_mismatch";
        case CanonicalSegmentErrorV1::kNonMonotonicCursor:
            return "non_monotonic_cursor";
        case CanonicalSegmentErrorV1::kRecordNotPublished:
            return "record_not_published";
        case CanonicalSegmentErrorV1::kHashMismatch:
            return "hash_mismatch";
        case CanonicalSegmentErrorV1::kManifestConflict:
            return "manifest_conflict";
        case CanonicalSegmentErrorV1::kWaitError:
            return "wait_error";
    }
    return "invalid_segment_error";
}

CanonicalSegmentWriterV1::CanonicalSegmentWriterV1(
    int file_descriptor,
    void* mapping,
    std::size_t mapping_bytes,
    std::filesystem::path manifest_path,
    CanonicalSegmentHeaderViewV1 header) noexcept
    : file_descriptor_(file_descriptor),
      mapping_(mapping),
      mapping_bytes_(mapping_bytes),
      manifest_path_(std::move(manifest_path)),
      header_(std::move(header)) {}

CanonicalSegmentWriterV1::~CanonicalSegmentWriterV1() {
    if (mapping_ != nullptr && mapping_ != MAP_FAILED) {
        static_cast<void>(::munmap(mapping_, mapping_bytes_));
    }
    static_cast<void>(CloseDescriptor(file_descriptor_));
}

CanonicalSegmentErrorV1 CanonicalSegmentWriterV1::Create(
    const CanonicalSegmentCreateOptionsV1& options,
    std::unique_ptr<CanonicalSegmentWriterV1>* writer) {
    if (writer == nullptr) {
        return CanonicalSegmentErrorV1::kInvalidArgument;
    }
    writer->reset();
    if (options.segment_path.empty() || options.manifest_path.empty() ||
        options.segment_path == options.manifest_path ||
        options.created_realtime_ns <= 0 ||
        options.created_monotonic_ns < 0) {
        return CanonicalSegmentErrorV1::kInvalidArgument;
    }
    const CanonicalSegmentErrorV1 descriptor_error =
        ValidateDescriptor(options.descriptor);
    if (descriptor_error != CanonicalSegmentErrorV1::kNone) {
        return descriptor_error;
    }
    std::size_t mapping_bytes = 0U;
    const CanonicalSegmentErrorV1 size_error = ComputeMappingBytes(
        options.descriptor, &mapping_bytes);
    if (size_error != CanonicalSegmentErrorV1::kNone) {
        return size_error;
    }

    int descriptor = -1;
    do {
        descriptor = ::open(
            options.segment_path.c_str(),
            O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW |
                O_NONBLOCK | O_NOCTTY,
            0600);
    } while (descriptor < 0 && errno == EINTR);
    if (descriptor < 0) {
        return errno == EEXIST
                   ? CanonicalSegmentErrorV1::kAlreadyExists
                   : CanonicalSegmentErrorV1::kOpenFailed;
    }
    if (::fchmod(descriptor, 0600) != 0) {
        static_cast<void>(CloseDescriptor(descriptor));
        return CanonicalSegmentErrorV1::kWrongFileMode;
    }
    if (::flock(descriptor, LOCK_EX | LOCK_NB) != 0) {
        static_cast<void>(CloseDescriptor(descriptor));
        return CanonicalSegmentErrorV1::kSingleWriterLockUnavailable;
    }

    int allocation_result = 0;
    do {
        allocation_result = ::posix_fallocate(
            descriptor, 0, static_cast<off_t>(mapping_bytes));
    } while (allocation_result == EINTR);
    if (allocation_result != 0) {
        static_cast<void>(CloseDescriptor(descriptor));
        return CanonicalSegmentErrorV1::kPreallocationFailed;
    }
    const CanonicalSegmentErrorV1 file_error = ValidateRegularFile(
        descriptor, mapping_bytes, false);
    if (file_error != CanonicalSegmentErrorV1::kNone) {
        static_cast<void>(CloseDescriptor(descriptor));
        return file_error;
    }

    void* const mapping = ::mmap(
        nullptr,
        mapping_bytes,
        PROT_READ | PROT_WRITE,
        MAP_SHARED,
        descriptor,
        0);
    if (mapping == MAP_FAILED) {
        static_cast<void>(CloseDescriptor(descriptor));
        return CanonicalSegmentErrorV1::kMapFailed;
    }

    CanonicalSegmentHeaderViewV1 initial{};
    initial.descriptor = options.descriptor;
    initial.created_realtime_ns = options.created_realtime_ns;
    initial.created_monotonic_ns = options.created_monotonic_ns;
    HeaderBytes header_bytes{};
    EncodeHeader(initial, &header_bytes);
    std::span<std::byte> mapping_span = MutableMappingSpan(
        mapping, mapping_bytes);
    std::copy(header_bytes.begin(), header_bytes.end(),
              mapping_span.begin());
    InitializeControl(
        mapping_span.data() + kCanonicalSegmentHeaderBytesV1,
        options.descriptor);
    const bool file_synced =
        MsyncLoop(mapping, kCanonicalSegmentDataOffsetV1) &&
        FsyncLoop(descriptor);
    bool parent_synced = false;
    if (file_synced) {
        try {
            parent_synced = SyncParentDirectory(options.segment_path);
        } catch (...) {
            parent_synced = false;
        }
    }
    if (!file_synced || !parent_synced) {
        static_cast<void>(::munmap(mapping, mapping_bytes));
        static_cast<void>(CloseDescriptor(descriptor));
        return CanonicalSegmentErrorV1::kIoError;
    }

    std::unique_ptr<CanonicalSegmentWriterV1> created(
        new (std::nothrow) CanonicalSegmentWriterV1(
            descriptor,
            mapping,
            mapping_bytes,
            options.manifest_path,
            initial));
    if (!created) {
        static_cast<void>(::munmap(mapping, mapping_bytes));
        static_cast<void>(CloseDescriptor(descriptor));
        return CanonicalSegmentErrorV1::kIoError;
    }
    *writer = std::move(created);
    return CanonicalSegmentErrorV1::kNone;
}

CanonicalSegmentErrorV1 CanonicalSegmentWriterV1::ReadControl(
    CanonicalSegmentControlSnapshotV1* snapshot) const noexcept {
    if (mapping_ == nullptr || mapping_ == MAP_FAILED) {
        return CanonicalSegmentErrorV1::kInvalidArgument;
    }
    const std::span<const std::byte> mapping = MappingSpan(
        mapping_, mapping_bytes_);
    return ReadControlPage(
        mapping.data() + kCanonicalSegmentHeaderBytesV1,
        header_.descriptor,
        snapshot);
}

CanonicalSegmentErrorV1 CanonicalSegmentWriterV1::PreflightRecords(
    std::span<const std::span<const std::byte>> records) const noexcept {
    if (header_.generation_fatal) {
        return CanonicalSegmentErrorV1::kGenerationFatal;
    }
    if (header_.sealed) {
        return CanonicalSegmentErrorV1::kSegmentSealed;
    }
    if (records.size() > RemainingRecords()) {
        return CanonicalSegmentErrorV1::kSegmentFull;
    }
    CanonicalSegmentControlSnapshotV1 current{};
    const CanonicalSegmentErrorV1 control_error = ReadControl(&current);
    if (control_error != CanonicalSegmentErrorV1::kNone) {
        return control_error;
    }
    if (current.closed || current.generation_fatal ||
        current.published_records != header_.published_records ||
        current.last_shard_event_id != header_.last_shard_event_id ||
        current.last_origin_wal_end_pos !=
            header_.last_origin_wal_end_pos ||
        current.processed_raw_ingress_sequence !=
            header_.processed_raw_ingress_sequence ||
        current.processed_raw_wal_pos !=
            header_.processed_raw_wal_pos) {
        return CanonicalSegmentErrorV1::kCorruptControlPage;
    }

    std::uint64_t simulated_records = header_.published_records;
    std::uint64_t simulated_event_id = header_.last_shard_event_id;
    std::uint64_t simulated_ingress =
        header_.last_origin_ingress_sequence;
    std::uint64_t simulated_wal = header_.last_origin_wal_end_pos;
    for (const std::span<const std::byte> record_bytes : records) {
        if (record_bytes.size() != header_.descriptor.record_size ||
            ValidateRecordBytes(
                header_.descriptor.event_type, record_bytes) !=
                CanonicalValidationErrorV1::kNone) {
            return CanonicalSegmentErrorV1::kInvalidRecord;
        }
        const CanonicalHeaderV1 record_header =
            LoadCanonicalRecordHeader(record_bytes);
        if (record_header.event_type != header_.descriptor.event_type ||
            record_header.record_size != header_.descriptor.record_size ||
            record_header.source_stream_id !=
                header_.descriptor.source_stream_id ||
            record_header.trade_date != header_.descriptor.trade_date) {
            return CanonicalSegmentErrorV1::kRecordIdentityMismatch;
        }
        if (record_header.origin_ingress_sequence <=
                header_.processed_raw_ingress_sequence ||
            record_header.origin_wal_end_pos <=
                header_.processed_raw_wal_pos ||
            (simulated_records != 0U &&
             (simulated_event_id ==
                  std::numeric_limits<std::uint64_t>::max() ||
              record_header.shard_event_id != simulated_event_id + 1U ||
              !RawOriginAtOrAfter(
                  simulated_ingress,
                  simulated_wal,
                  record_header.origin_ingress_sequence,
                  record_header.origin_wal_end_pos)))) {
            return CanonicalSegmentErrorV1::kNonMonotonicCursor;
        }
        ++simulated_records;
        simulated_event_id = record_header.shard_event_id;
        simulated_ingress = record_header.origin_ingress_sequence;
        simulated_wal = record_header.origin_wal_end_pos;
    }
    return CanonicalSegmentErrorV1::kNone;
}

CanonicalSegmentErrorV1 CanonicalSegmentWriterV1::PublishRecord(
    std::span<const std::byte> record_bytes) noexcept {
    if (header_.generation_fatal) {
        return CanonicalSegmentErrorV1::kGenerationFatal;
    }
    if (header_.sealed) {
        return CanonicalSegmentErrorV1::kSegmentSealed;
    }
    if (record_bytes.size() != header_.descriptor.record_size ||
        ValidateRecordBytes(header_.descriptor.event_type, record_bytes) !=
            CanonicalValidationErrorV1::kNone) {
        return CanonicalSegmentErrorV1::kInvalidRecord;
    }
    const CanonicalHeaderV1 record_header =
        LoadCanonicalRecordHeader(record_bytes);
    if (record_header.event_type != header_.descriptor.event_type ||
        record_header.record_size != header_.descriptor.record_size ||
        record_header.source_stream_id !=
            header_.descriptor.source_stream_id ||
        record_header.trade_date != header_.descriptor.trade_date) {
        return CanonicalSegmentErrorV1::kRecordIdentityMismatch;
    }
    if (record_header.origin_ingress_sequence <=
            header_.processed_raw_ingress_sequence ||
        record_header.origin_wal_end_pos <=
            header_.processed_raw_wal_pos ||
        (header_.published_records != 0U &&
         (header_.last_shard_event_id ==
              std::numeric_limits<std::uint64_t>::max() ||
          record_header.shard_event_id !=
              header_.last_shard_event_id + 1U ||
          !RawOriginAtOrAfter(
              header_.last_origin_ingress_sequence,
              header_.last_origin_wal_end_pos,
              record_header.origin_ingress_sequence,
              record_header.origin_wal_end_pos)))) {
        return CanonicalSegmentErrorV1::kNonMonotonicCursor;
    }
    if (header_.published_records >=
        header_.descriptor.capacity_records) {
        return CanonicalSegmentErrorV1::kSegmentFull;
    }

    CanonicalSegmentControlSnapshotV1 current{};
    CanonicalSegmentErrorV1 control_error = ReadControl(&current);
    if (control_error != CanonicalSegmentErrorV1::kNone) {
        return control_error;
    }
    if (current.closed || current.generation_fatal ||
        current.published_records != header_.published_records ||
        current.last_shard_event_id != header_.last_shard_event_id ||
        current.last_origin_wal_end_pos !=
            header_.last_origin_wal_end_pos ||
        current.processed_raw_ingress_sequence !=
            header_.processed_raw_ingress_sequence ||
        current.processed_raw_wal_pos !=
            header_.processed_raw_wal_pos) {
        return CanonicalSegmentErrorV1::kCorruptControlPage;
    }

    const std::uint64_t byte_offset_u64 =
        static_cast<std::uint64_t>(kCanonicalSegmentDataOffsetV1) +
        header_.published_records * header_.descriptor.record_size;
    if (byte_offset_u64 > static_cast<std::uint64_t>(
                              std::numeric_limits<std::size_t>::max())) {
        return CanonicalSegmentErrorV1::kSizeOverflow;
    }
    std::span<std::byte> mapping = MutableMappingSpan(
        mapping_, mapping_bytes_);
    std::memcpy(
        mapping.data() + static_cast<std::size_t>(byte_offset_u64),
        record_bytes.data(),
        record_bytes.size());

    CanonicalSegmentControlSnapshotV1 next = current;
    ++next.published_records;
    next.last_shard_event_id = record_header.shard_event_id;
    next.last_origin_wal_end_pos = record_header.origin_wal_end_pos;
    ++next.notify_epoch;
    control_error = PublishControlPage(
        mapping.data() + kCanonicalSegmentHeaderBytesV1,
        header_.descriptor,
        next);
    if (control_error != CanonicalSegmentErrorV1::kNone) {
        // The bytes remain outside published_records and are therefore not
        // visible.  The caller must treat a control failure as fatal; no
        // cursor or event ID is committed in this object.
        return control_error;
    }

    if (header_.published_records == 0U) {
        header_.first_shard_event_id = record_header.shard_event_id;
        header_.first_origin_ingress_sequence =
            record_header.origin_ingress_sequence;
        header_.first_origin_wal_end_pos =
            record_header.origin_wal_end_pos;
    }
    header_.published_records = next.published_records;
    header_.last_shard_event_id = record_header.shard_event_id;
    header_.last_origin_ingress_sequence =
        record_header.origin_ingress_sequence;
    header_.last_origin_wal_end_pos = record_header.origin_wal_end_pos;
    return CanonicalSegmentErrorV1::kNone;
}

CanonicalSegmentErrorV1 CanonicalSegmentWriterV1::AdvanceProcessedRaw(
    std::uint64_t processed_raw_ingress_sequence,
    std::uint64_t processed_raw_wal_pos) noexcept {
    if (header_.generation_fatal) {
        return CanonicalSegmentErrorV1::kGenerationFatal;
    }
    if (header_.sealed) {
        return CanonicalSegmentErrorV1::kSegmentSealed;
    }
    if ((processed_raw_ingress_sequence != 0U &&
         processed_raw_wal_pos == 0U) ||
        !RawProgressAtOrAfter(
            header_.processed_raw_ingress_sequence,
            header_.processed_raw_wal_pos,
            processed_raw_ingress_sequence,
            processed_raw_wal_pos) ||
        !RawProgressAtOrAfter(
            header_.last_origin_ingress_sequence,
            header_.last_origin_wal_end_pos,
            processed_raw_ingress_sequence,
            processed_raw_wal_pos)) {
        return CanonicalSegmentErrorV1::kNonMonotonicCursor;
    }
    CanonicalSegmentControlSnapshotV1 current{};
    CanonicalSegmentErrorV1 error = ReadControl(&current);
    if (error != CanonicalSegmentErrorV1::kNone) {
        return error;
    }
    if (current.closed || current.generation_fatal ||
        current.published_records != header_.published_records ||
        current.last_shard_event_id != header_.last_shard_event_id ||
        current.last_origin_wal_end_pos !=
            header_.last_origin_wal_end_pos ||
        current.processed_raw_ingress_sequence !=
            header_.processed_raw_ingress_sequence ||
        current.processed_raw_wal_pos !=
            header_.processed_raw_wal_pos) {
        return CanonicalSegmentErrorV1::kCorruptControlPage;
    }
    if (processed_raw_ingress_sequence ==
            header_.processed_raw_ingress_sequence &&
        processed_raw_wal_pos == header_.processed_raw_wal_pos) {
        return CanonicalSegmentErrorV1::kNone;
    }
    current.processed_raw_ingress_sequence =
        processed_raw_ingress_sequence;
    current.processed_raw_wal_pos = processed_raw_wal_pos;
    ++current.notify_epoch;
    std::span<std::byte> mapping = MutableMappingSpan(
        mapping_, mapping_bytes_);
    error = PublishControlPage(
        mapping.data() + kCanonicalSegmentHeaderBytesV1,
        header_.descriptor,
        current);
    if (error != CanonicalSegmentErrorV1::kNone) {
        return error;
    }
    header_.processed_raw_ingress_sequence =
        processed_raw_ingress_sequence;
    header_.processed_raw_wal_pos = processed_raw_wal_pos;
    return CanonicalSegmentErrorV1::kNone;
}

CanonicalSegmentErrorV1
CanonicalSegmentWriterV1::MarkGenerationFatal() noexcept {
    if (header_.sealed) {
        return CanonicalSegmentErrorV1::kSegmentSealed;
    }
    // Set the in-process latch before touching the control page: even a torn
    // or busy control page must never permit this live writer to Seal.
    header_.generation_fatal = true;
    CanonicalSegmentControlSnapshotV1 current{};
    CanonicalSegmentErrorV1 error = ReadControl(&current);
    if (error != CanonicalSegmentErrorV1::kNone) {
        return error;
    }
    if (current.generation_fatal) {
        return CanonicalSegmentErrorV1::kNone;
    }
    if (current.closed) {
        return CanonicalSegmentErrorV1::kSegmentSealed;
    }
    current.generation_fatal = true;
    ++current.notify_epoch;
    std::span<std::byte> mapping = MutableMappingSpan(
        mapping_, mapping_bytes_);
    return PublishControlPage(
        mapping.data() + kCanonicalSegmentHeaderBytesV1,
        header_.descriptor,
        current);
}

CanonicalSegmentErrorV1 CanonicalSegmentWriterV1::Seal(
    const CanonicalSegmentSealOptionsV1& options,
    CanonicalSegmentSealResultV1* result) noexcept {
    if (result == nullptr) {
        return CanonicalSegmentErrorV1::kInvalidArgument;
    }
    *result = {};
    if (header_.generation_fatal) {
        return CanonicalSegmentErrorV1::kGenerationFatal;
    }
    if (!header_.sealed) {
        if (options.closed_realtime_ns <= 0 ||
            options.closed_monotonic_ns <
                header_.created_monotonic_ns) {
            return CanonicalSegmentErrorV1::kInvalidArgument;
        }
        CanonicalSegmentControlSnapshotV1 control{};
        CanonicalSegmentErrorV1 control_error = ReadControl(&control);
        if (control_error != CanonicalSegmentErrorV1::kNone) {
            return control_error;
        }
        if (control.closed || control.generation_fatal ||
            control.published_records != header_.published_records ||
            control.last_shard_event_id != header_.last_shard_event_id ||
            control.last_origin_wal_end_pos !=
                header_.last_origin_wal_end_pos ||
            control.processed_raw_ingress_sequence !=
                header_.processed_raw_ingress_sequence ||
            control.processed_raw_wal_pos !=
                header_.processed_raw_wal_pos) {
            return CanonicalSegmentErrorV1::kCorruptControlPage;
        }
        if (header_.published_records != 0U &&
            !RawProgressAtOrAfter(
                header_.last_origin_ingress_sequence,
                header_.last_origin_wal_end_pos,
                header_.processed_raw_ingress_sequence,
                header_.processed_raw_wal_pos)) {
            return CanonicalSegmentErrorV1::kNonMonotonicCursor;
        }

        const std::uint64_t record_bytes_u64 =
            header_.published_records * header_.descriptor.record_size;
        if (record_bytes_u64 > static_cast<std::uint64_t>(
                                   std::numeric_limits<std::size_t>::max())) {
            return CanonicalSegmentErrorV1::kSizeOverflow;
        }
        std::span<std::byte> mapping = MutableMappingSpan(
            mapping_, mapping_bytes_);
        const std::span<const std::byte> records(
            mapping.data() + kCanonicalSegmentDataOffsetV1,
            static_cast<std::size_t>(record_bytes_u64));
        CanonicalSegmentHeaderViewV1 final_header = header_;
        final_header.sealed = true;
        final_header.closed_realtime_ns = options.closed_realtime_ns;
        final_header.closed_monotonic_ns = options.closed_monotonic_ns;
        final_header.record_stream_sha256 =
            l2flow::common::ComputeSha256(records);
        final_header.segment_integrity_sha256 = {};
        HeaderBytes provisional_bytes{};
        EncodeHeader(final_header, &provisional_bytes);
        final_header.segment_integrity_sha256 =
            ComputeIntegrityDigest(provisional_bytes, records);
        if (ArrayIsZero(final_header.record_stream_sha256) ||
            ArrayIsZero(final_header.segment_integrity_sha256)) {
            return CanonicalSegmentErrorV1::kHashMismatch;
        }
        // The object becomes permanently non-appendable before the first
        // final-header write.  A failed sync may be retried by Seal, but never
        // by appending more records to an indeterminate on-disk header.
        header_ = final_header;
    }

    HeaderBytes durable_header_bytes{};
    EncodeHeader(header_, &durable_header_bytes);
    std::span<std::byte> durable_mapping = MutableMappingSpan(
        mapping_, mapping_bytes_);
    std::copy(durable_header_bytes.begin(),
              durable_header_bytes.end(),
              durable_mapping.begin());
    if (!MsyncLoop(mapping_, mapping_bytes_) ||
        !FsyncLoop(file_descriptor_)) {
        return CanonicalSegmentErrorV1::kIoError;
    }
    // From this point the durable sealed header is authoritative even if the
    // volatile control close or manifest publication later fails.

    CanonicalSegmentControlSnapshotV1 control{};
    CanonicalSegmentErrorV1 control_error = ReadControl(&control);
    if (control_error != CanonicalSegmentErrorV1::kNone) {
        return control_error;
    }
    if (control.published_records != header_.published_records ||
        control.last_shard_event_id != header_.last_shard_event_id ||
        control.last_origin_wal_end_pos != header_.last_origin_wal_end_pos ||
        control.processed_raw_ingress_sequence !=
            header_.processed_raw_ingress_sequence ||
        control.processed_raw_wal_pos != header_.processed_raw_wal_pos) {
        return CanonicalSegmentErrorV1::kCorruptControlPage;
    }
    if (!control.closed) {
        control.closed = true;
        ++control.notify_epoch;
        std::span<std::byte> mapping = MutableMappingSpan(
            mapping_, mapping_bytes_);
        control_error = PublishControlPage(
            mapping.data() + kCanonicalSegmentHeaderBytesV1,
            header_.descriptor,
            control);
        if (control_error != CanonicalSegmentErrorV1::kNone) {
            return control_error;
        }
    }

    HeaderBytes final_header_bytes{};
    EncodeHeader(header_, &final_header_bytes);
    const l2flow::common::Sha256Digest header_sha256 =
        l2flow::common::ComputeSha256(
            std::span<const std::byte>(final_header_bytes));
    ManifestBytes manifest_bytes{};
    BuildManifestBytes(header_, header_sha256, &manifest_bytes);
    result->published_records = header_.published_records;
    result->record_stream_sha256 = header_.record_stream_sha256;
    result->segment_integrity_sha256 =
        header_.segment_integrity_sha256;
    result->segment_header_sha256 = header_sha256;
    result->manifest_sha256 = l2flow::common::ComputeSha256(
        std::span<const std::byte>(manifest_bytes));

    try {
        return PublishManifest(manifest_path_, manifest_bytes);
    } catch (...) {
        return CanonicalSegmentErrorV1::kIoError;
    }
}

CanonicalSegmentReaderV1::CanonicalSegmentReaderV1(
    int file_descriptor,
    void* mapping,
    std::size_t mapping_bytes,
    CanonicalSegmentHeaderViewV1 header) noexcept
    : file_descriptor_(file_descriptor),
      mapping_(mapping),
      mapping_bytes_(mapping_bytes),
      header_(std::move(header)) {}

CanonicalSegmentReaderV1::~CanonicalSegmentReaderV1() {
    if (mapping_ != nullptr && mapping_ != MAP_FAILED) {
        static_cast<void>(::munmap(mapping_, mapping_bytes_));
    }
    static_cast<void>(CloseDescriptor(file_descriptor_));
}

CanonicalSegmentErrorV1 CanonicalSegmentReaderV1::Open(
    const std::filesystem::path& segment_path,
    const CanonicalSegmentDescriptorV1& expected,
    std::unique_ptr<CanonicalSegmentReaderV1>* reader) {
    if (reader == nullptr || segment_path.empty()) {
        return CanonicalSegmentErrorV1::kInvalidArgument;
    }
    reader->reset();
    const CanonicalSegmentErrorV1 descriptor_error =
        ValidateDescriptor(expected);
    if (descriptor_error != CanonicalSegmentErrorV1::kNone) {
        return descriptor_error;
    }
    std::size_t mapping_bytes = 0U;
    const CanonicalSegmentErrorV1 size_error = ComputeMappingBytes(
        expected, &mapping_bytes);
    if (size_error != CanonicalSegmentErrorV1::kNone) {
        return size_error;
    }

    int descriptor = -1;
    do {
        descriptor = ::open(
            segment_path.c_str(),
            O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK | O_NOCTTY);
    } while (descriptor < 0 && errno == EINTR);
    if (descriptor < 0) {
        return CanonicalSegmentErrorV1::kOpenFailed;
    }
    const CanonicalSegmentErrorV1 file_error = ValidateRegularFile(
        descriptor, mapping_bytes, true);
    if (file_error != CanonicalSegmentErrorV1::kNone) {
        static_cast<void>(CloseDescriptor(descriptor));
        return file_error;
    }
    HeaderBytes header_bytes{};
    if (!PreadExact(descriptor,
                    std::span<std::byte>(header_bytes),
                    std::uint64_t{0U})) {
        static_cast<void>(CloseDescriptor(descriptor));
        return CanonicalSegmentErrorV1::kIoError;
    }
    CanonicalSegmentHeaderViewV1 header{};
    const CanonicalSegmentErrorV1 header_error = DecodeHeader(
        header_bytes, &header);
    if (header_error != CanonicalSegmentErrorV1::kNone) {
        static_cast<void>(CloseDescriptor(descriptor));
        return header_error;
    }
    if (!DescriptorEqual(header.descriptor, expected)) {
        static_cast<void>(CloseDescriptor(descriptor));
        return CanonicalSegmentErrorV1::kHeaderIdentityMismatch;
    }

    void* const mapping = ::mmap(
        nullptr,
        mapping_bytes,
        PROT_READ,
        MAP_SHARED,
        descriptor,
        0);
    if (mapping == MAP_FAILED) {
        static_cast<void>(CloseDescriptor(descriptor));
        return CanonicalSegmentErrorV1::kMapFailed;
    }
    const auto* mapping_bytes_pointer = static_cast<const std::byte*>(mapping);
    if (std::memcmp(mapping_bytes_pointer,
                    header_bytes.data(),
                    header_bytes.size()) != 0) {
        static_cast<void>(::munmap(mapping, mapping_bytes));
        static_cast<void>(CloseDescriptor(descriptor));
        return CanonicalSegmentErrorV1::kCorruptHeader;
    }
    if (header.sealed) {
        const CanonicalSegmentErrorV1 hash_error = VerifySealedHashes(
            mapping_bytes_pointer, header, header_bytes);
        if (hash_error != CanonicalSegmentErrorV1::kNone) {
            static_cast<void>(::munmap(mapping, mapping_bytes));
            static_cast<void>(CloseDescriptor(descriptor));
            return hash_error;
        }
    } else {
        CanonicalSegmentControlSnapshotV1 control{};
        const CanonicalSegmentErrorV1 control_error = ReadControlPage(
            mapping_bytes_pointer + kCanonicalSegmentHeaderBytesV1,
            expected,
            &control);
        if (control_error != CanonicalSegmentErrorV1::kNone) {
            static_cast<void>(::munmap(mapping, mapping_bytes));
            static_cast<void>(CloseDescriptor(descriptor));
            return control_error;
        }
    }

    std::unique_ptr<CanonicalSegmentReaderV1> opened(
        new (std::nothrow) CanonicalSegmentReaderV1(
            descriptor,
            mapping,
            mapping_bytes,
            header));
    if (!opened) {
        static_cast<void>(::munmap(mapping, mapping_bytes));
        static_cast<void>(CloseDescriptor(descriptor));
        return CanonicalSegmentErrorV1::kIoError;
    }
    *reader = std::move(opened);
    return CanonicalSegmentErrorV1::kNone;
}

CanonicalSegmentErrorV1 CanonicalSegmentReaderV1::ReadControl(
    CanonicalSegmentControlSnapshotV1* snapshot) const noexcept {
    if (snapshot == nullptr || mapping_ == nullptr ||
        mapping_ == MAP_FAILED) {
        return CanonicalSegmentErrorV1::kInvalidArgument;
    }
    if (header_.sealed) {
        *snapshot = SealedControlSnapshot(header_);
        return CanonicalSegmentErrorV1::kNone;
    }
    const std::span<const std::byte> mapping = MappingSpan(
        mapping_, mapping_bytes_);
    return ReadControlPage(
        mapping.data() + kCanonicalSegmentHeaderBytesV1,
        header_.descriptor,
        snapshot);
}

CanonicalSegmentErrorV1 CanonicalSegmentReaderV1::PublishedRecord(
    std::uint64_t record_index,
    std::span<const std::byte>* record_bytes) const noexcept {
    if (record_bytes == nullptr) {
        return CanonicalSegmentErrorV1::kInvalidArgument;
    }
    *record_bytes = {};
    CanonicalSegmentControlSnapshotV1 control{};
    const CanonicalSegmentErrorV1 control_error = ReadControl(&control);
    if (control_error != CanonicalSegmentErrorV1::kNone) {
        return control_error;
    }
    if (record_index >= control.published_records) {
        return CanonicalSegmentErrorV1::kRecordNotPublished;
    }
    const std::uint64_t offset_u64 =
        static_cast<std::uint64_t>(kCanonicalSegmentDataOffsetV1) +
        record_index * header_.descriptor.record_size;
    if (offset_u64 > static_cast<std::uint64_t>(
                         std::numeric_limits<std::size_t>::max())) {
        return CanonicalSegmentErrorV1::kSizeOverflow;
    }
    const std::span<const std::byte> mapping = MappingSpan(
        mapping_, mapping_bytes_);
    *record_bytes = mapping.subspan(
        static_cast<std::size_t>(offset_u64),
        header_.descriptor.record_size);
    return CanonicalSegmentErrorV1::kNone;
}

CanonicalSegmentErrorV1 CanonicalSegmentReaderV1::WaitForChange(
    std::uint64_t observed_notify_epoch,
    std::uint32_t timeout_milliseconds) const noexcept {
    if (mapping_ == nullptr || mapping_ == MAP_FAILED) {
        return CanonicalSegmentErrorV1::kInvalidArgument;
    }
    if (header_.sealed) {
        return CanonicalSegmentErrorV1::kNone;
    }
    const auto* mapping = static_cast<const std::byte*>(mapping_);
    const std::byte* const control =
        mapping + kCanonicalSegmentHeaderBytesV1;
    const std::uint64_t current = AtomicLoad(
        control, control_offset::kNotifyEpoch, __ATOMIC_ACQUIRE);
    if (current != observed_notify_epoch) {
        return CanonicalSegmentErrorV1::kNone;
    }
    struct timespec timeout {};
    timeout.tv_sec = static_cast<time_t>(timeout_milliseconds / 1000U);
    timeout.tv_nsec = static_cast<long>(
        (timeout_milliseconds % 1000U) * 1000000U);
    auto* address = const_cast<std::uint32_t*>(
        reinterpret_cast<const std::uint32_t*>(
            control + control_offset::kNotifyEpoch));
    const long wait_result = ::syscall(
        SYS_futex,
        address,
        FUTEX_WAIT,
        static_cast<std::uint32_t>(observed_notify_epoch),
        &timeout,
        nullptr,
        0);
    if (wait_result == 0 || errno == EAGAIN || errno == EINTR ||
        errno == ETIMEDOUT) {
        return CanonicalSegmentErrorV1::kNone;
    }
    return CanonicalSegmentErrorV1::kWaitError;
}

CanonicalSegmentErrorV1 InspectCanonicalSegmentForRecoveryV1(
    const std::filesystem::path& segment_path,
    const CanonicalSegmentDescriptorV1& expected,
    CanonicalSegmentRecoveryDispositionV1* disposition,
    CanonicalSegmentHeaderViewV1* header) {
    if (disposition == nullptr || header == nullptr) {
        return CanonicalSegmentErrorV1::kInvalidArgument;
    }
    const CanonicalSegmentErrorV1 descriptor_error =
        ValidateDescriptor(expected);
    if (descriptor_error != CanonicalSegmentErrorV1::kNone) {
        return descriptor_error;
    }
    std::size_t mapping_bytes = 0U;
    const CanonicalSegmentErrorV1 size_error = ComputeMappingBytes(
        expected, &mapping_bytes);
    if (size_error != CanonicalSegmentErrorV1::kNone) {
        return size_error;
    }
    int descriptor = -1;
    do {
        descriptor = ::open(
            segment_path.c_str(),
            O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK | O_NOCTTY);
    } while (descriptor < 0 && errno == EINTR);
    if (descriptor < 0) {
        return CanonicalSegmentErrorV1::kOpenFailed;
    }
    const CanonicalSegmentErrorV1 file_error = ValidateRegularFile(
        descriptor, mapping_bytes, true);
    if (file_error != CanonicalSegmentErrorV1::kNone) {
        static_cast<void>(CloseDescriptor(descriptor));
        return file_error;
    }
    HeaderBytes header_bytes{};
    if (!PreadExact(descriptor,
                    std::span<std::byte>(header_bytes),
                    std::uint64_t{0U})) {
        static_cast<void>(CloseDescriptor(descriptor));
        return CanonicalSegmentErrorV1::kIoError;
    }
    CanonicalSegmentHeaderViewV1 inspected{};
    const CanonicalSegmentErrorV1 header_error = DecodeHeader(
        header_bytes, &inspected);
    if (header_error != CanonicalSegmentErrorV1::kNone) {
        static_cast<void>(CloseDescriptor(descriptor));
        return header_error;
    }
    if (!DescriptorEqual(inspected.descriptor, expected)) {
        static_cast<void>(CloseDescriptor(descriptor));
        return CanonicalSegmentErrorV1::kHeaderIdentityMismatch;
    }
    if (!inspected.sealed) {
        // Deliberately do not map or inspect the volatile control page.  It
        // may contain an odd/torn seqlock from the crashed writer and cannot
        // turn a valid open header into a resumable prefix or a corruption
        // verdict.
        const bool close_ok = CloseDescriptor(descriptor);
        if (!close_ok) {
            return CanonicalSegmentErrorV1::kIoError;
        }
        *header = inspected;
        *disposition = CanonicalSegmentRecoveryDispositionV1::
            kUnsealedDiscardWholeGeneration;
        return CanonicalSegmentErrorV1::kNone;
    }
    static_cast<void>(CloseDescriptor(descriptor));

    std::unique_ptr<CanonicalSegmentReaderV1> reader;
    const CanonicalSegmentErrorV1 open_error =
        CanonicalSegmentReaderV1::Open(
            segment_path, expected, &reader);
    if (open_error != CanonicalSegmentErrorV1::kNone) {
        return open_error;
    }
    *header = reader->header();
    *disposition = CanonicalSegmentRecoveryDispositionV1::
        kSealedHeaderAndHashesValid;
    return CanonicalSegmentErrorV1::kNone;
}

CanonicalSegmentErrorV1 ReadCanonicalSegmentManifestV1(
    const std::filesystem::path& manifest_path,
    const CanonicalSegmentHeaderViewV1& expected_segment,
    CanonicalSegmentManifestViewV1* manifest) {
    if (manifest == nullptr || !expected_segment.sealed ||
        manifest_path.empty()) {
        return CanonicalSegmentErrorV1::kInvalidArgument;
    }
    const CanonicalSegmentErrorV1 descriptor_error =
        ValidateDescriptor(expected_segment.descriptor);
    if (descriptor_error != CanonicalSegmentErrorV1::kNone) {
        return descriptor_error;
    }
    HeaderBytes header_bytes{};
    EncodeHeader(expected_segment, &header_bytes);
    const l2flow::common::Sha256Digest header_sha256 =
        l2flow::common::ComputeSha256(
            std::span<const std::byte>(header_bytes));
    ManifestBytes expected_bytes{};
    BuildManifestBytes(expected_segment, header_sha256, &expected_bytes);

    int directory_descriptor = -1;
    std::string final_name;
    std::string temporary_name;
    const CanonicalSegmentErrorV1 directory_error = OpenManifestDirectory(
        manifest_path,
        &directory_descriptor,
        &final_name,
        &temporary_name);
    if (directory_error != CanonicalSegmentErrorV1::kNone) {
        return directory_error;
    }
    ManifestBytes actual_bytes{};
    const CanonicalSegmentErrorV1 read_error = ReadFixedManifestAt(
        directory_descriptor, final_name, &actual_bytes, false);
    const bool close_ok = CloseDescriptor(directory_descriptor);
    if (read_error != CanonicalSegmentErrorV1::kNone) {
        return read_error;
    }
    if (!close_ok) {
        return CanonicalSegmentErrorV1::kIoError;
    }
    if (actual_bytes != expected_bytes) {
        return CanonicalSegmentErrorV1::kHashMismatch;
    }
    CanonicalSegmentManifestViewV1 decoded{};
    decoded.segment = expected_segment;
    decoded.segment_header_sha256 = header_sha256;
    decoded.manifest_sha256 = l2flow::common::ComputeSha256(
        std::span<const std::byte>(actual_bytes));
    *manifest = decoded;
    return CanonicalSegmentErrorV1::kNone;
}

}  // namespace l2flow::canonical
