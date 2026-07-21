#include "l2flow/ingress/raw_v1.h"

#include "l2flow/common/crc32c.h"

#include <algorithm>
#include <array>
#include <limits>
#include <utility>

namespace l2flow::ingress {
namespace {

constexpr std::size_t kVendorHeadSizeOffset = 0U;
constexpr std::size_t kVendorMessageSizeOffset = 1U;
constexpr std::size_t kVendorEncodingOffset = 5U;
constexpr std::size_t kVendorServiceIdOffset = 6U;
constexpr std::size_t kVendorServiceVersionOffset = 7U;
constexpr std::size_t kVendorMessageIdOffset = 9U;
constexpr std::size_t kVendorLocalTimeOffset = 11U;
constexpr std::size_t kVendorSequenceIdOffset = 15U;
constexpr std::size_t kCrcBytes = sizeof(std::uint32_t);

static_assert(
    raw_v1_offset::segment::kReservedTail <=
    kRawV1SegmentHeaderBytes);
static_assert(
    raw_v1_offset::record_header::kReserved1 +
        sizeof(std::uint64_t) ==
    kRawV1RecordHeaderBytes);
static_assert(
    raw_v1_offset::record_trailer::kIngressSequence +
        sizeof(std::uint64_t) ==
    kRawV1RecordTrailerBytes);
static_assert(
    raw_v1_offset::journal::kReservedTail <=
    kRawV1JournalHeaderBytes);
static_assert(
    raw_v1_offset::marker::kMarkerFlags +
        sizeof(std::uint32_t) ==
    kRawV1DurableMarkerBytes);
static_assert(
    kVendorSequenceIdOffset + sizeof(std::uint64_t) ==
    kVendorMessageHeadBytes);

void StoreU16Le(
    std::uint16_t value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    output[offset] =
        static_cast<std::byte>(value & 0xffU);
    output[offset + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32Le(
    std::uint32_t value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        output[offset + index] =
            static_cast<std::byte>((value >> shift) & 0xffU);
    }
}

void StoreU64Le(
    std::uint64_t value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        output[offset + index] =
            static_cast<std::byte>((value >> shift) & 0xffU);
    }
}

std::uint16_t LoadU16Le(
    std::span<const std::byte> input,
    std::size_t offset) noexcept {
    return static_cast<std::uint16_t>(
        std::to_integer<std::uint16_t>(input[offset]) |
        static_cast<std::uint16_t>(
            std::to_integer<std::uint16_t>(input[offset + 1U])
            << 8U));
}

std::uint32_t LoadU32Le(
    std::span<const std::byte> input,
    std::size_t offset) noexcept {
    std::uint32_t value = 0U;
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        value |=
            std::to_integer<std::uint32_t>(input[offset + index])
            << shift;
    }
    return value;
}

std::uint64_t LoadU64Le(
    std::span<const std::byte> input,
    std::size_t offset) noexcept {
    std::uint64_t value = 0U;
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        value |=
            std::to_integer<std::uint64_t>(input[offset + index])
            << shift;
    }
    return value;
}

template <std::size_t Size>
void StoreBytes(
    const std::array<std::byte, Size>& value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    std::copy(
        value.begin(), value.end(), output.data() + offset);
}

template <std::size_t Size>
void LoadBytes(
    std::span<const std::byte> input,
    std::size_t offset,
    std::array<std::byte, Size>* value) noexcept {
    std::copy_n(input.data() + offset, Size, value->begin());
}

bool IsZero(
    std::span<const std::byte> bytes) noexcept {
    return std::all_of(
        bytes.begin(), bytes.end(), [](std::byte value) {
            return value == std::byte{0};
        });
}

template <std::size_t Size>
bool IsZero(
    const std::array<std::byte, Size>& bytes) noexcept {
    return IsZero(std::span<const std::byte>(bytes));
}

bool IsZeroRange(
    std::span<const std::byte> wire,
    std::size_t offset,
    std::size_t size) noexcept {
    return IsZero(wire.subspan(offset, size));
}

std::uint32_t ComputeCrcWithZeroedField(
    std::span<const std::byte> wire,
    std::size_t crc_offset) noexcept {
    constexpr std::array<std::byte, kCrcBytes> zero_crc{};
    l2flow::common::Crc32cState state;
    state.Update(wire.first(crc_offset));
    state.Update(zero_crc);
    state.Update(wire.subspan(crc_offset + kCrcBytes));
    return state.Finalize();
}

RawV1Error ValidateSegmentLogical(
    const SegmentHeaderV1& header) noexcept {
    if ((header.segment_flags & ~kRawV1SegmentFlagsMask) != 0U) {
        return RawV1Error::kUnknownFlags;
    }
    if (header.source_stream_id == 0U ||
        header.capture_date == 0U ||
        header.segment_sequence == 0U ||
        header.first_ingress_sequence == 0U ||
        IsZero(header.stream_day_id) ||
        IsZero(header.host_uuid) ||
        IsZero(header.linux_boot_id) ||
        header.clock_epoch_algorithm == 0U ||
        IsZero(header.clock_epoch_digest) ||
        IsZero(header.sdk_archive_sha256) ||
        IsZero(header.libmdl_api_sha256) ||
        IsZero(header.endpoint_contract_sha256) ||
        IsZero(header.config_sha256) ||
        IsZero(header.raw_schema_sha256) ||
        IsZero(header.build_manifest_sha256)) {
        return RawV1Error::kInvalidIdentity;
    }
    if ((header.segment_sequence == 1U &&
         header.segment_base_wal_pos != 0U) ||
        (header.segment_sequence != 1U &&
         header.segment_base_wal_pos <
             kRawV1SegmentHeaderBytes) ||
        header.segment_base_wal_pos >
            std::numeric_limits<std::uint64_t>::max() -
                kRawV1SegmentHeaderBytes) {
        return RawV1Error::kInvalidIdentity;
    }

    const bool continuation =
        (header.segment_flags &
         kRawV1FinalizationContinuation) != 0U;
    const bool reserve_zero =
        IsZero(header.reserve_state_uuid);
    const bool cycle_zero =
        IsZero(header.finalization_cycle_id);
    const bool grant_zero =
        IsZero(header.immutable_grant_sha256);
    if ((!continuation &&
         (!reserve_zero || !cycle_zero || !grant_zero)) ||
        (continuation &&
         (reserve_zero || cycle_zero || grant_zero))) {
        return RawV1Error::kInvalidIdentity;
    }
    return RawV1Error::kNone;
}

RawV1Error ValidateRecordHeaderLogical(
    const RawRecordHeaderV1& header) noexcept {
    if ((header.flags & ~kRawV1RecordFlagsMask) != 0U) {
        return RawV1Error::kUnknownFlags;
    }
    if (header.source_stream_id == 0U ||
        header.capture_date == 0U ||
        header.ingress_sequence == 0U) {
        return RawV1Error::kInvalidIdentity;
    }
    if (header.vendor_head_size != kVendorMessageHeadBytes) {
        return RawV1Error::kInvalidVendorHead;
    }

    RawRecordLayoutV1 layout{};
    const RawV1Error layout_error =
        ComputeRawRecordLayoutV1(
            header.vendor_body_size, &layout);
    if (layout_error != RawV1Error::kNone) {
        return layout_error;
    }
    if (header.vendor_message_size !=
            layout.vendor_message_size ||
        header.record_size != layout.record_size) {
        return RawV1Error::kRecordSizeMismatch;
    }
    return RawV1Error::kNone;
}

RawV1Error ValidateTrailerLogical(
    const RawRecordTrailerV1& trailer) noexcept {
    RawRecordLayoutV1 minimum{};
    const RawV1Error layout_error =
        ComputeRawRecordLayoutV1(0U, &minimum);
    if (layout_error != RawV1Error::kNone) {
        return layout_error;
    }
    if (trailer.record_size < minimum.record_size ||
        (trailer.record_size % kRawV1RecordAlignment) != 0U) {
        return RawV1Error::kRecordSizeMismatch;
    }
    if (trailer.ingress_sequence == 0U) {
        return RawV1Error::kInvalidIdentity;
    }
    return RawV1Error::kNone;
}

RawV1Error ValidateJournalLogical(
    const DurableJournalHeaderV1& header) noexcept {
    if (header.capture_date == 0U ||
        header.source_stream_id == 0U ||
        IsZero(header.stream_day_id) ||
        IsZero(header.raw_schema_sha256) ||
        IsZero(header.created_host_uuid) ||
        IsZero(header.created_linux_boot_id) ||
        header.created_clock_epoch_algorithm == 0U ||
        IsZero(header.created_clock_epoch_digest)) {
        return RawV1Error::kInvalidIdentity;
    }
    return RawV1Error::kNone;
}

RawV1Error ValidateMarkerLogical(
    const DurableMarkerV1& marker) noexcept {
    if ((marker.marker_flags & ~kRawV1MarkerFlagsMask) != 0U) {
        return RawV1Error::kUnknownFlags;
    }
    if (marker.source_stream_id == 0U ||
        marker.segment_sequence == 0U) {
        return RawV1Error::kInvalidIdentity;
    }
    if (marker.durable_segment_offset <
            kRawV1SegmentHeaderBytes ||
        marker.durable_global_wal_pos <
            marker.durable_segment_offset) {
        return RawV1Error::kRecordSizeMismatch;
    }
    return RawV1Error::kNone;
}

RawV1Error ValidateVendorHeadAgainstRecord(
    std::span<const std::byte> vendor_head,
    const RawRecordHeaderV1& header) noexcept {
    if (vendor_head.size() != kVendorMessageHeadBytes ||
        std::to_integer<std::uint8_t>(
            vendor_head[kVendorHeadSizeOffset]) !=
            kVendorMessageHeadBytes ||
        LoadU32Le(vendor_head, kVendorMessageSizeOffset) !=
            header.vendor_message_size) {
        return RawV1Error::kInvalidVendorHead;
    }
    if (std::to_integer<std::uint8_t>(
            vendor_head[kVendorEncodingOffset]) !=
            header.vendor_message_encoding ||
        std::to_integer<std::uint8_t>(
            vendor_head[kVendorServiceIdOffset]) !=
            header.vendor_service_id ||
        LoadU16Le(vendor_head, kVendorServiceVersionOffset) !=
            header.vendor_service_version ||
        LoadU16Le(vendor_head, kVendorMessageIdOffset) !=
            header.vendor_message_id ||
        LoadU32Le(vendor_head, kVendorLocalTimeOffset) !=
            header.vendor_local_time_raw ||
        LoadU64Le(vendor_head, kVendorSequenceIdOffset) !=
            header.vendor_sequence_id) {
        return RawV1Error::kVendorFieldMismatch;
    }
    return RawV1Error::kNone;
}

}  // namespace

std::string_view RawV1ErrorName(
    RawV1Error error) noexcept {
    switch (error) {
    case RawV1Error::kNone:
        return "none";
    case RawV1Error::kNullOutput:
        return "null output";
    case RawV1Error::kInvalidWireSize:
        return "invalid wire size";
    case RawV1Error::kInvalidMagic:
        return "invalid magic";
    case RawV1Error::kUnsupportedVersion:
        return "unsupported version";
    case RawV1Error::kInvalidEndian:
        return "invalid endian";
    case RawV1Error::kInvalidHeaderSize:
        return "invalid header size";
    case RawV1Error::kUnknownFlags:
        return "unknown flags";
    case RawV1Error::kNonzeroReserved:
        return "nonzero reserved";
    case RawV1Error::kInvalidIdentity:
        return "invalid identity";
    case RawV1Error::kSizeOverflow:
        return "size overflow";
    case RawV1Error::kRecordSizeMismatch:
        return "record size mismatch";
    case RawV1Error::kInvalidVendorHead:
        return "invalid vendor head";
    case RawV1Error::kVendorFieldMismatch:
        return "vendor field mismatch";
    case RawV1Error::kHeaderCrcMismatch:
        return "header CRC mismatch";
    case RawV1Error::kPayloadCrcMismatch:
        return "payload CRC mismatch";
    case RawV1Error::kNonzeroPadding:
        return "nonzero padding";
    case RawV1Error::kTrailerMismatch:
        return "trailer mismatch";
    case RawV1Error::kAllocationFailure:
        return "allocation failure";
    }
    return "unknown Raw V1 error";
}

RawV1Error ComputeRawRecordLayoutV1(
    std::size_t vendor_body_size,
    RawRecordLayoutV1* layout) noexcept {
    if (layout == nullptr) {
        return RawV1Error::kNullOutput;
    }
    if (vendor_body_size >
        static_cast<std::size_t>(
            std::numeric_limits<std::uint32_t>::max()) -
            kVendorMessageHeadBytes) {
        return RawV1Error::kSizeOverflow;
    }

    const std::size_t vendor_message_size =
        kVendorMessageHeadBytes + vendor_body_size;
    if (vendor_message_size >
        std::numeric_limits<std::size_t>::max() -
            kRawV1RecordHeaderBytes) {
        return RawV1Error::kSizeOverflow;
    }
    const std::size_t payload_end =
        kRawV1RecordHeaderBytes + vendor_message_size;
    const std::size_t padding_size =
        (kRawV1RecordAlignment -
         (payload_end % kRawV1RecordAlignment)) %
        kRawV1RecordAlignment;
    if (payload_end >
        std::numeric_limits<std::size_t>::max() -
            padding_size -
            kRawV1RecordTrailerBytes) {
        return RawV1Error::kSizeOverflow;
    }
    const std::size_t record_size =
        payload_end + padding_size +
        kRawV1RecordTrailerBytes;
    if (record_size >
        static_cast<std::size_t>(
            std::numeric_limits<std::uint32_t>::max())) {
        return RawV1Error::kSizeOverflow;
    }

    RawRecordLayoutV1 computed{};
    computed.vendor_message_size =
        static_cast<std::uint32_t>(vendor_message_size);
    computed.padding_size =
        static_cast<std::uint32_t>(padding_size);
    computed.record_size =
        static_cast<std::uint32_t>(record_size);
    *layout = computed;
    return RawV1Error::kNone;
}

RawV1Error EncodeSegmentHeaderV1(
    const SegmentHeaderV1& header,
    RawV1SegmentHeaderWire* wire) noexcept {
    if (wire == nullptr) {
        return RawV1Error::kNullOutput;
    }
    const RawV1Error validation =
        ValidateSegmentLogical(header);
    if (validation != RawV1Error::kNone) {
        return validation;
    }

    RawV1SegmentHeaderWire encoded{};
    std::span<std::byte> bytes(encoded);
    using namespace raw_v1_offset::segment;
    StoreU32Le(kRawV1SegmentMagic, bytes, kMagic);
    StoreU16Le(kRawV1FormatVersion, bytes, kFormatVersion);
    bytes[kEndian] = static_cast<std::byte>(kRawV1LittleEndian);
    StoreU32Le(
        static_cast<std::uint32_t>(kRawV1SegmentHeaderBytes),
        bytes,
        kHeaderSize);
    StoreU32Le(header.source_stream_id, bytes, kSourceStreamId);
    StoreU32Le(header.capture_date, bytes, kCaptureDate);
    StoreBytes(header.stream_day_id, bytes, kStreamDayId);
    StoreU32Le(header.segment_sequence, bytes, kSegmentSequence);
    StoreU32Le(header.segment_flags, bytes, kSegmentFlags);
    StoreBytes(header.reserve_state_uuid, bytes, kReserveStateUuid);
    StoreBytes(
        header.finalization_cycle_id, bytes, kFinalizationCycleId);
    StoreBytes(
        header.immutable_grant_sha256,
        bytes,
        kImmutableGrantSha256);
    StoreU64Le(
        header.segment_base_wal_pos, bytes, kSegmentBaseWalPos);
    StoreU64Le(
        header.first_ingress_sequence,
        bytes,
        kFirstIngressSequence);
    StoreU64Le(
        header.created_realtime_ns, bytes, kCreatedRealtimeNs);
    StoreU64Le(
        header.created_monotonic_ns,
        bytes,
        kCreatedMonotonicNs);
    StoreBytes(header.host_uuid, bytes, kHostUuid);
    StoreBytes(header.linux_boot_id, bytes, kLinuxBootId);
    StoreU32Le(
        header.clock_epoch_algorithm,
        bytes,
        kClockEpochAlgorithm);
    StoreBytes(
        header.clock_epoch_digest, bytes, kClockEpochDigest);
    StoreU64Le(
        header.clock_epoch_label, bytes, kClockEpochLabel);
    StoreBytes(
        header.sdk_archive_sha256, bytes, kSdkArchiveSha256);
    StoreBytes(
        header.libmdl_api_sha256, bytes, kLibmdlApiSha256);
    StoreBytes(
        header.endpoint_contract_sha256,
        bytes,
        kEndpointContractSha256);
    StoreBytes(header.config_sha256, bytes, kConfigSha256);
    StoreBytes(
        header.raw_schema_sha256, bytes, kRawSchemaSha256);
    StoreBytes(
        header.build_manifest_sha256,
        bytes,
        kBuildManifestSha256);
    const std::uint32_t crc =
        l2flow::common::ComputeCrc32c(encoded);
    StoreU32Le(crc, bytes, kHeaderCrc32c);
    *wire = encoded;
    return RawV1Error::kNone;
}

RawV1Error DecodeSegmentHeaderV1(
    std::span<const std::byte> wire,
    SegmentHeaderV1* header) noexcept {
    if (header == nullptr) {
        return RawV1Error::kNullOutput;
    }
    if (wire.size() != kRawV1SegmentHeaderBytes) {
        return RawV1Error::kInvalidWireSize;
    }
    using namespace raw_v1_offset::segment;
    if (LoadU32Le(wire, kMagic) != kRawV1SegmentMagic) {
        return RawV1Error::kInvalidMagic;
    }
    if (LoadU16Le(wire, kFormatVersion) !=
        kRawV1FormatVersion) {
        return RawV1Error::kUnsupportedVersion;
    }
    if (std::to_integer<std::uint8_t>(wire[kEndian]) !=
        kRawV1LittleEndian) {
        return RawV1Error::kInvalidEndian;
    }
    if (LoadU32Le(wire, kHeaderSize) !=
        kRawV1SegmentHeaderBytes) {
        return RawV1Error::kInvalidHeaderSize;
    }
    if (!IsZeroRange(wire, kReserved0, 1U) ||
        !IsZeroRange(wire, kReserved1, 4U) ||
        !IsZeroRange(wire, kReserved2, 4U) ||
        !IsZeroRange(
            wire,
            kReservedTail,
            kRawV1SegmentHeaderBytes - kReservedTail)) {
        return RawV1Error::kNonzeroReserved;
    }
    if ((LoadU32Le(wire, kSegmentFlags) &
         ~kRawV1SegmentFlagsMask) != 0U) {
        return RawV1Error::kUnknownFlags;
    }
    const std::uint32_t stored_crc =
        LoadU32Le(wire, kHeaderCrc32c);
    if (ComputeCrcWithZeroedField(wire, kHeaderCrc32c) !=
        stored_crc) {
        return RawV1Error::kHeaderCrcMismatch;
    }

    SegmentHeaderV1 decoded{};
    decoded.source_stream_id =
        LoadU32Le(wire, kSourceStreamId);
    decoded.capture_date = LoadU32Le(wire, kCaptureDate);
    LoadBytes(wire, kStreamDayId, &decoded.stream_day_id);
    decoded.segment_sequence =
        LoadU32Le(wire, kSegmentSequence);
    decoded.segment_flags = LoadU32Le(wire, kSegmentFlags);
    LoadBytes(
        wire, kReserveStateUuid, &decoded.reserve_state_uuid);
    LoadBytes(
        wire,
        kFinalizationCycleId,
        &decoded.finalization_cycle_id);
    LoadBytes(
        wire,
        kImmutableGrantSha256,
        &decoded.immutable_grant_sha256);
    decoded.segment_base_wal_pos =
        LoadU64Le(wire, kSegmentBaseWalPos);
    decoded.first_ingress_sequence =
        LoadU64Le(wire, kFirstIngressSequence);
    decoded.created_realtime_ns =
        LoadU64Le(wire, kCreatedRealtimeNs);
    decoded.created_monotonic_ns =
        LoadU64Le(wire, kCreatedMonotonicNs);
    LoadBytes(wire, kHostUuid, &decoded.host_uuid);
    LoadBytes(wire, kLinuxBootId, &decoded.linux_boot_id);
    decoded.clock_epoch_algorithm =
        LoadU32Le(wire, kClockEpochAlgorithm);
    LoadBytes(
        wire, kClockEpochDigest, &decoded.clock_epoch_digest);
    decoded.clock_epoch_label =
        LoadU64Le(wire, kClockEpochLabel);
    LoadBytes(
        wire, kSdkArchiveSha256, &decoded.sdk_archive_sha256);
    LoadBytes(
        wire, kLibmdlApiSha256, &decoded.libmdl_api_sha256);
    LoadBytes(
        wire,
        kEndpointContractSha256,
        &decoded.endpoint_contract_sha256);
    LoadBytes(wire, kConfigSha256, &decoded.config_sha256);
    LoadBytes(
        wire, kRawSchemaSha256, &decoded.raw_schema_sha256);
    LoadBytes(
        wire,
        kBuildManifestSha256,
        &decoded.build_manifest_sha256);
    decoded.header_crc32c = stored_crc;

    const RawV1Error validation =
        ValidateSegmentLogical(decoded);
    if (validation != RawV1Error::kNone) {
        return validation;
    }
    *header = decoded;
    return RawV1Error::kNone;
}

RawV1Error ValidateSegmentHeaderV1(
    std::span<const std::byte> wire) noexcept {
    SegmentHeaderV1 decoded{};
    return DecodeSegmentHeaderV1(wire, &decoded);
}

RawV1Error EncodeRawRecordHeaderV1(
    const RawRecordHeaderV1& header,
    RawV1RecordHeaderWire* wire) noexcept {
    if (wire == nullptr) {
        return RawV1Error::kNullOutput;
    }
    const RawV1Error validation =
        ValidateRecordHeaderLogical(header);
    if (validation != RawV1Error::kNone) {
        return validation;
    }

    RawV1RecordHeaderWire encoded{};
    std::span<std::byte> bytes(encoded);
    using namespace raw_v1_offset::record_header;
    StoreU32Le(kRawV1RecordMagic, bytes, kMagic);
    StoreU16Le(kRawV1FormatVersion, bytes, kVersion);
    StoreU16Le(
        static_cast<std::uint16_t>(kRawV1RecordHeaderBytes),
        bytes,
        kHeaderSize);
    StoreU32Le(header.record_size, bytes, kRecordSize);
    StoreU32Le(header.flags, bytes, kFlags);
    StoreU32Le(
        header.source_stream_id, bytes, kSourceStreamId);
    StoreU32Le(
        header.connection_epoch_hint,
        bytes,
        kConnectionEpochHint);
    StoreU64Le(
        header.ingress_sequence, bytes, kIngressSequence);
    StoreU64Le(
        header.recv_realtime_ns, bytes, kRecvRealtimeNs);
    StoreU64Le(
        header.recv_monotonic_ns, bytes, kRecvMonotonicNs);
    StoreU32Le(header.capture_date, bytes, kCaptureDate);
    StoreU32Le(
        header.vendor_local_time_raw,
        bytes,
        kVendorLocalTimeRaw);
    StoreU64Le(
        header.vendor_sequence_id, bytes, kVendorSequenceId);
    StoreU32Le(
        header.vendor_message_size, bytes, kVendorMessageSize);
    StoreU32Le(
        header.vendor_body_size, bytes, kVendorBodySize);
    StoreU16Le(
        header.vendor_service_version,
        bytes,
        kVendorServiceVersion);
    StoreU16Le(
        header.vendor_message_id, bytes, kVendorMessageId);
    bytes[kVendorServiceId] =
        static_cast<std::byte>(header.vendor_service_id);
    bytes[kVendorMessageEncoding] =
        static_cast<std::byte>(header.vendor_message_encoding);
    bytes[kVendorHeadSize] =
        static_cast<std::byte>(header.vendor_head_size);
    StoreU32Le(
        header.payload_crc32c, bytes, kPayloadCrc32c);
    const std::uint32_t crc =
        l2flow::common::ComputeCrc32c(encoded);
    StoreU32Le(crc, bytes, kHeaderCrc32c);
    *wire = encoded;
    return RawV1Error::kNone;
}

RawV1Error DecodeRawRecordHeaderV1(
    std::span<const std::byte> wire,
    RawRecordHeaderV1* header) noexcept {
    if (header == nullptr) {
        return RawV1Error::kNullOutput;
    }
    if (wire.size() != kRawV1RecordHeaderBytes) {
        return RawV1Error::kInvalidWireSize;
    }
    using namespace raw_v1_offset::record_header;
    if (LoadU32Le(wire, kMagic) != kRawV1RecordMagic) {
        return RawV1Error::kInvalidMagic;
    }
    if (LoadU16Le(wire, kVersion) != kRawV1FormatVersion) {
        return RawV1Error::kUnsupportedVersion;
    }
    if (LoadU16Le(wire, kHeaderSize) !=
        kRawV1RecordHeaderBytes) {
        return RawV1Error::kInvalidHeaderSize;
    }
    if (!IsZeroRange(wire, kReserved0, 1U) ||
        !IsZeroRange(wire, kReserved1, 8U)) {
        return RawV1Error::kNonzeroReserved;
    }
    if ((LoadU32Le(wire, kFlags) &
         ~kRawV1RecordFlagsMask) != 0U) {
        return RawV1Error::kUnknownFlags;
    }
    const std::uint32_t stored_crc =
        LoadU32Le(wire, kHeaderCrc32c);
    if (ComputeCrcWithZeroedField(wire, kHeaderCrc32c) !=
        stored_crc) {
        return RawV1Error::kHeaderCrcMismatch;
    }

    RawRecordHeaderV1 decoded{};
    decoded.record_size = LoadU32Le(wire, kRecordSize);
    decoded.flags = LoadU32Le(wire, kFlags);
    decoded.source_stream_id =
        LoadU32Le(wire, kSourceStreamId);
    decoded.connection_epoch_hint =
        LoadU32Le(wire, kConnectionEpochHint);
    decoded.ingress_sequence =
        LoadU64Le(wire, kIngressSequence);
    decoded.recv_realtime_ns =
        LoadU64Le(wire, kRecvRealtimeNs);
    decoded.recv_monotonic_ns =
        LoadU64Le(wire, kRecvMonotonicNs);
    decoded.capture_date = LoadU32Le(wire, kCaptureDate);
    decoded.vendor_local_time_raw =
        LoadU32Le(wire, kVendorLocalTimeRaw);
    decoded.vendor_sequence_id =
        LoadU64Le(wire, kVendorSequenceId);
    decoded.vendor_message_size =
        LoadU32Le(wire, kVendorMessageSize);
    decoded.vendor_body_size =
        LoadU32Le(wire, kVendorBodySize);
    decoded.vendor_service_version =
        LoadU16Le(wire, kVendorServiceVersion);
    decoded.vendor_message_id =
        LoadU16Le(wire, kVendorMessageId);
    decoded.vendor_service_id =
        std::to_integer<std::uint8_t>(wire[kVendorServiceId]);
    decoded.vendor_message_encoding =
        std::to_integer<std::uint8_t>(
            wire[kVendorMessageEncoding]);
    decoded.vendor_head_size =
        std::to_integer<std::uint8_t>(wire[kVendorHeadSize]);
    decoded.payload_crc32c =
        LoadU32Le(wire, kPayloadCrc32c);
    decoded.header_crc32c = stored_crc;

    const RawV1Error validation =
        ValidateRecordHeaderLogical(decoded);
    if (validation != RawV1Error::kNone) {
        return validation;
    }
    *header = decoded;
    return RawV1Error::kNone;
}

RawV1Error ValidateRawRecordHeaderV1(
    std::span<const std::byte> wire) noexcept {
    RawRecordHeaderV1 decoded{};
    return DecodeRawRecordHeaderV1(wire, &decoded);
}

RawV1Error EncodeRawRecordTrailerV1(
    const RawRecordTrailerV1& trailer,
    RawV1RecordTrailerWire* wire) noexcept {
    if (wire == nullptr) {
        return RawV1Error::kNullOutput;
    }
    const RawV1Error validation =
        ValidateTrailerLogical(trailer);
    if (validation != RawV1Error::kNone) {
        return validation;
    }

    RawV1RecordTrailerWire encoded{};
    std::span<std::byte> bytes(encoded);
    using namespace raw_v1_offset::record_trailer;
    StoreU32Le(trailer.record_size, bytes, kRecordSize);
    StoreU32Le(kRawV1RecordCommitMagic, bytes, kCommitMagic);
    StoreU64Le(
        trailer.ingress_sequence, bytes, kIngressSequence);
    *wire = encoded;
    return RawV1Error::kNone;
}

RawV1Error DecodeRawRecordTrailerV1(
    std::span<const std::byte> wire,
    RawRecordTrailerV1* trailer) noexcept {
    if (trailer == nullptr) {
        return RawV1Error::kNullOutput;
    }
    if (wire.size() != kRawV1RecordTrailerBytes) {
        return RawV1Error::kInvalidWireSize;
    }
    using namespace raw_v1_offset::record_trailer;
    if (LoadU32Le(wire, kCommitMagic) !=
        kRawV1RecordCommitMagic) {
        return RawV1Error::kInvalidMagic;
    }
    RawRecordTrailerV1 decoded{};
    decoded.record_size = LoadU32Le(wire, kRecordSize);
    decoded.ingress_sequence =
        LoadU64Le(wire, kIngressSequence);
    const RawV1Error validation =
        ValidateTrailerLogical(decoded);
    if (validation != RawV1Error::kNone) {
        return validation;
    }
    *trailer = decoded;
    return RawV1Error::kNone;
}

RawV1Error ValidateRawRecordTrailerV1(
    std::span<const std::byte> wire) noexcept {
    RawRecordTrailerV1 decoded{};
    return DecodeRawRecordTrailerV1(wire, &decoded);
}

RawV1Error EncodeDurableJournalHeaderV1(
    const DurableJournalHeaderV1& header,
    RawV1JournalHeaderWire* wire) noexcept {
    if (wire == nullptr) {
        return RawV1Error::kNullOutput;
    }
    const RawV1Error validation =
        ValidateJournalLogical(header);
    if (validation != RawV1Error::kNone) {
        return validation;
    }

    RawV1JournalHeaderWire encoded{};
    std::span<std::byte> bytes(encoded);
    using namespace raw_v1_offset::journal;
    StoreU32Le(kRawV1JournalMagic, bytes, kMagic);
    StoreU16Le(kRawV1FormatVersion, bytes, kFormatVersion);
    bytes[kEndian] = static_cast<std::byte>(kRawV1LittleEndian);
    StoreU32Le(
        static_cast<std::uint32_t>(kRawV1JournalHeaderBytes),
        bytes,
        kHeaderSize);
    StoreU32Le(header.capture_date, bytes, kCaptureDate);
    StoreU32Le(
        header.source_stream_id, bytes, kSourceStreamId);
    StoreBytes(header.stream_day_id, bytes, kStreamDayId);
    StoreBytes(
        header.raw_schema_sha256, bytes, kRawSchemaSha256);
    StoreBytes(
        header.created_host_uuid, bytes, kCreatedHostUuid);
    StoreBytes(
        header.created_linux_boot_id,
        bytes,
        kCreatedLinuxBootId);
    StoreU32Le(
        header.created_clock_epoch_algorithm,
        bytes,
        kCreatedClockEpochAlgorithm);
    StoreBytes(
        header.created_clock_epoch_digest,
        bytes,
        kCreatedClockEpochDigest);
    StoreU64Le(
        header.created_clock_epoch_label,
        bytes,
        kCreatedClockEpochLabel);
    const std::uint32_t crc =
        l2flow::common::ComputeCrc32c(encoded);
    StoreU32Le(crc, bytes, kHeaderCrc32c);
    *wire = encoded;
    return RawV1Error::kNone;
}

RawV1Error DecodeDurableJournalHeaderV1(
    std::span<const std::byte> wire,
    DurableJournalHeaderV1* header) noexcept {
    if (header == nullptr) {
        return RawV1Error::kNullOutput;
    }
    if (wire.size() != kRawV1JournalHeaderBytes) {
        return RawV1Error::kInvalidWireSize;
    }
    using namespace raw_v1_offset::journal;
    if (LoadU32Le(wire, kMagic) != kRawV1JournalMagic) {
        return RawV1Error::kInvalidMagic;
    }
    if (LoadU16Le(wire, kFormatVersion) !=
        kRawV1FormatVersion) {
        return RawV1Error::kUnsupportedVersion;
    }
    if (std::to_integer<std::uint8_t>(wire[kEndian]) !=
        kRawV1LittleEndian) {
        return RawV1Error::kInvalidEndian;
    }
    if (LoadU32Le(wire, kHeaderSize) !=
        kRawV1JournalHeaderBytes) {
        return RawV1Error::kInvalidHeaderSize;
    }
    if (!IsZeroRange(wire, kReserved0, 1U) ||
        !IsZeroRange(wire, kReserved1, 4U) ||
        !IsZeroRange(wire, kReserved2, 4U) ||
        !IsZeroRange(
            wire,
            kReservedTail,
            kRawV1JournalHeaderBytes - kReservedTail)) {
        return RawV1Error::kNonzeroReserved;
    }
    const std::uint32_t stored_crc =
        LoadU32Le(wire, kHeaderCrc32c);
    if (ComputeCrcWithZeroedField(wire, kHeaderCrc32c) !=
        stored_crc) {
        return RawV1Error::kHeaderCrcMismatch;
    }

    DurableJournalHeaderV1 decoded{};
    decoded.capture_date = LoadU32Le(wire, kCaptureDate);
    decoded.source_stream_id =
        LoadU32Le(wire, kSourceStreamId);
    LoadBytes(wire, kStreamDayId, &decoded.stream_day_id);
    LoadBytes(
        wire, kRawSchemaSha256, &decoded.raw_schema_sha256);
    LoadBytes(
        wire, kCreatedHostUuid, &decoded.created_host_uuid);
    LoadBytes(
        wire,
        kCreatedLinuxBootId,
        &decoded.created_linux_boot_id);
    decoded.created_clock_epoch_algorithm =
        LoadU32Le(wire, kCreatedClockEpochAlgorithm);
    LoadBytes(
        wire,
        kCreatedClockEpochDigest,
        &decoded.created_clock_epoch_digest);
    decoded.created_clock_epoch_label =
        LoadU64Le(wire, kCreatedClockEpochLabel);
    decoded.header_crc32c = stored_crc;

    const RawV1Error validation =
        ValidateJournalLogical(decoded);
    if (validation != RawV1Error::kNone) {
        return validation;
    }
    *header = decoded;
    return RawV1Error::kNone;
}

RawV1Error ValidateDurableJournalHeaderV1(
    std::span<const std::byte> wire) noexcept {
    DurableJournalHeaderV1 decoded{};
    return DecodeDurableJournalHeaderV1(wire, &decoded);
}

RawV1Error EncodeDurableMarkerV1(
    const DurableMarkerV1& marker,
    RawV1DurableMarkerWire* wire) noexcept {
    if (wire == nullptr) {
        return RawV1Error::kNullOutput;
    }
    const RawV1Error validation =
        ValidateMarkerLogical(marker);
    if (validation != RawV1Error::kNone) {
        return validation;
    }

    RawV1DurableMarkerWire encoded{};
    std::span<std::byte> bytes(encoded);
    using namespace raw_v1_offset::marker;
    StoreU32Le(kRawV1DurableMarkerMagic, bytes, kMagic);
    StoreU16Le(kRawV1FormatVersion, bytes, kVersion);
    StoreU16Le(
        static_cast<std::uint16_t>(kRawV1DurableMarkerBytes),
        bytes,
        kMarkerSize);
    StoreU32Le(
        marker.source_stream_id, bytes, kSourceStreamId);
    StoreU32Le(
        marker.segment_sequence, bytes, kSegmentSequence);
    StoreU64Le(
        marker.durable_global_wal_pos,
        bytes,
        kDurableGlobalWalPos);
    StoreU64Le(
        marker.durable_ingress_sequence,
        bytes,
        kDurableIngressSequence);
    StoreU64Le(
        marker.durable_segment_offset,
        bytes,
        kDurableSegmentOffset);
    StoreU32Le(marker.marker_flags, bytes, kMarkerFlags);
    const std::uint32_t crc =
        l2flow::common::ComputeCrc32c(encoded);
    StoreU32Le(crc, bytes, kMarkerCrc32c);
    *wire = encoded;
    return RawV1Error::kNone;
}

RawV1Error DecodeDurableMarkerV1(
    std::span<const std::byte> wire,
    DurableMarkerV1* marker) noexcept {
    if (marker == nullptr) {
        return RawV1Error::kNullOutput;
    }
    if (wire.size() != kRawV1DurableMarkerBytes) {
        return RawV1Error::kInvalidWireSize;
    }
    using namespace raw_v1_offset::marker;
    if (LoadU32Le(wire, kMagic) !=
        kRawV1DurableMarkerMagic) {
        return RawV1Error::kInvalidMagic;
    }
    if (LoadU16Le(wire, kVersion) != kRawV1FormatVersion) {
        return RawV1Error::kUnsupportedVersion;
    }
    if (LoadU16Le(wire, kMarkerSize) !=
        kRawV1DurableMarkerBytes) {
        return RawV1Error::kInvalidHeaderSize;
    }
    if ((LoadU32Le(wire, kMarkerFlags) &
         ~kRawV1MarkerFlagsMask) != 0U) {
        return RawV1Error::kUnknownFlags;
    }
    const std::uint32_t stored_crc =
        LoadU32Le(wire, kMarkerCrc32c);
    if (ComputeCrcWithZeroedField(wire, kMarkerCrc32c) !=
        stored_crc) {
        return RawV1Error::kHeaderCrcMismatch;
    }

    DurableMarkerV1 decoded{};
    decoded.source_stream_id =
        LoadU32Le(wire, kSourceStreamId);
    decoded.segment_sequence =
        LoadU32Le(wire, kSegmentSequence);
    decoded.durable_global_wal_pos =
        LoadU64Le(wire, kDurableGlobalWalPos);
    decoded.durable_ingress_sequence =
        LoadU64Le(wire, kDurableIngressSequence);
    decoded.durable_segment_offset =
        LoadU64Le(wire, kDurableSegmentOffset);
    decoded.marker_crc32c = stored_crc;
    decoded.marker_flags = LoadU32Le(wire, kMarkerFlags);

    const RawV1Error validation =
        ValidateMarkerLogical(decoded);
    if (validation != RawV1Error::kNone) {
        return validation;
    }
    *marker = decoded;
    return RawV1Error::kNone;
}

RawV1Error ValidateDurableMarkerV1(
    std::span<const std::byte> wire) noexcept {
    DurableMarkerV1 decoded{};
    return DecodeDurableMarkerV1(wire, &decoded);
}

RawV1Error EncodeRawRecordV1(
    const RawRecordInputV1& input,
    std::vector<std::byte>* wire,
    RawRecordHeaderV1* encoded_header) noexcept {
    if (wire == nullptr) {
        return RawV1Error::kNullOutput;
    }

    RawRecordLayoutV1 layout{};
    const RawV1Error layout_error =
        ComputeRawRecordLayoutV1(
            input.vendor_body.size(), &layout);
    if (layout_error != RawV1Error::kNone) {
        return layout_error;
    }
    if (input.meta.source_stream_id == 0U ||
        input.meta.capture_date == 0U ||
        input.meta.ingress_sequence == 0U) {
        return RawV1Error::kInvalidIdentity;
    }
    if ((input.meta.flags & ~kRawV1RecordFlagsMask) != 0U) {
        return RawV1Error::kUnknownFlags;
    }

    const std::span<const std::byte> vendor_head(
        input.vendor_head);
    if (std::to_integer<std::uint8_t>(
            vendor_head[kVendorHeadSizeOffset]) !=
            kVendorMessageHeadBytes ||
        LoadU32Le(vendor_head, kVendorMessageSizeOffset) !=
            layout.vendor_message_size) {
        return RawV1Error::kInvalidVendorHead;
    }

    RawRecordHeaderV1 header{};
    header.record_size = layout.record_size;
    header.flags = input.meta.flags;
    header.source_stream_id = input.meta.source_stream_id;
    header.connection_epoch_hint =
        input.meta.connection_epoch_hint;
    header.ingress_sequence = input.meta.ingress_sequence;
    header.recv_realtime_ns = input.meta.recv_realtime_ns;
    header.recv_monotonic_ns =
        input.meta.recv_monotonic_ns;
    header.capture_date = input.meta.capture_date;
    header.vendor_local_time_raw =
        LoadU32Le(vendor_head, kVendorLocalTimeOffset);
    header.vendor_sequence_id =
        LoadU64Le(vendor_head, kVendorSequenceIdOffset);
    header.vendor_message_size = layout.vendor_message_size;
    header.vendor_body_size =
        static_cast<std::uint32_t>(input.vendor_body.size());
    header.vendor_service_version =
        LoadU16Le(vendor_head, kVendorServiceVersionOffset);
    header.vendor_message_id =
        LoadU16Le(vendor_head, kVendorMessageIdOffset);
    header.vendor_service_id =
        std::to_integer<std::uint8_t>(
            vendor_head[kVendorServiceIdOffset]);
    header.vendor_message_encoding =
        std::to_integer<std::uint8_t>(
            vendor_head[kVendorEncodingOffset]);
    header.vendor_head_size =
        static_cast<std::uint8_t>(kVendorMessageHeadBytes);

    l2flow::common::Crc32cState payload_crc;
    payload_crc.Update(vendor_head);
    payload_crc.Update(input.vendor_body);
    header.payload_crc32c = payload_crc.Finalize();

    RawV1RecordHeaderWire header_wire{};
    const RawV1Error header_error =
        EncodeRawRecordHeaderV1(header, &header_wire);
    if (header_error != RawV1Error::kNone) {
        return header_error;
    }
    header.header_crc32c = LoadU32Le(
        header_wire, raw_v1_offset::record_header::kHeaderCrc32c);

    RawRecordTrailerV1 trailer{};
    trailer.record_size = layout.record_size;
    trailer.ingress_sequence = input.meta.ingress_sequence;
    RawV1RecordTrailerWire trailer_wire{};
    const RawV1Error trailer_error =
        EncodeRawRecordTrailerV1(trailer, &trailer_wire);
    if (trailer_error != RawV1Error::kNone) {
        return trailer_error;
    }

    try {
        std::vector<std::byte> encoded(
            static_cast<std::size_t>(layout.record_size),
            std::byte{0});
        std::copy(
            header_wire.begin(),
            header_wire.end(),
            encoded.begin());
        std::copy(
            input.vendor_head.begin(),
            input.vendor_head.end(),
            encoded.begin() + kRawV1RecordHeaderBytes);
        std::copy(
            input.vendor_body.begin(),
            input.vendor_body.end(),
            encoded.begin() +
                kRawV1RecordHeaderBytes +
                kVendorMessageHeadBytes);
        std::copy(
            trailer_wire.begin(),
            trailer_wire.end(),
            encoded.end() - kRawV1RecordTrailerBytes);
        wire->swap(encoded);
    } catch (...) {
        return RawV1Error::kAllocationFailure;
    }

    if (encoded_header != nullptr) {
        *encoded_header = header;
    }
    return RawV1Error::kNone;
}

RawV1Error DecodeRawRecordV1(
    std::span<const std::byte> wire,
    RawRecordViewV1* record,
    const RawRecordNamespaceV1* expected_namespace) noexcept {
    if (record == nullptr) {
        return RawV1Error::kNullOutput;
    }
    if (wire.size() < kRawV1RecordHeaderBytes) {
        return RawV1Error::kInvalidWireSize;
    }

    RawRecordHeaderV1 header{};
    const RawV1Error header_error =
        DecodeRawRecordHeaderV1(
            wire.first(kRawV1RecordHeaderBytes), &header);
    if (header_error != RawV1Error::kNone) {
        return header_error;
    }
    if (wire.size() !=
        static_cast<std::size_t>(header.record_size)) {
        return RawV1Error::kRecordSizeMismatch;
    }
    if (expected_namespace != nullptr &&
        (header.source_stream_id !=
             expected_namespace->source_stream_id ||
         header.capture_date !=
             expected_namespace->capture_date)) {
        return RawV1Error::kInvalidIdentity;
    }

    RawRecordLayoutV1 layout{};
    const RawV1Error layout_error =
        ComputeRawRecordLayoutV1(
            header.vendor_body_size, &layout);
    if (layout_error != RawV1Error::kNone) {
        return layout_error;
    }
    const std::size_t vendor_head_offset =
        kRawV1RecordHeaderBytes;
    const std::size_t vendor_body_offset =
        vendor_head_offset + kVendorMessageHeadBytes;
    const std::size_t padding_offset =
        vendor_body_offset +
        static_cast<std::size_t>(header.vendor_body_size);
    const std::size_t trailer_offset =
        padding_offset +
        static_cast<std::size_t>(layout.padding_size);
    if (trailer_offset >
            wire.size() - kRawV1RecordTrailerBytes ||
        trailer_offset + kRawV1RecordTrailerBytes !=
            wire.size()) {
        return RawV1Error::kRecordSizeMismatch;
    }

    const std::span<const std::byte> vendor_head =
        wire.subspan(vendor_head_offset, kVendorMessageHeadBytes);
    const std::span<const std::byte> vendor_body =
        wire.subspan(
            vendor_body_offset, header.vendor_body_size);
    const std::span<const std::byte> padding =
        wire.subspan(padding_offset, layout.padding_size);
    if (!IsZero(padding)) {
        return RawV1Error::kNonzeroPadding;
    }

    const std::uint32_t payload_crc =
        l2flow::common::ComputeCrc32c(
            wire.subspan(
                vendor_head_offset,
                header.vendor_message_size));
    if (payload_crc != header.payload_crc32c) {
        return RawV1Error::kPayloadCrcMismatch;
    }

    const RawV1Error vendor_error =
        ValidateVendorHeadAgainstRecord(vendor_head, header);
    if (vendor_error != RawV1Error::kNone) {
        return vendor_error;
    }

    RawRecordTrailerV1 trailer{};
    const RawV1Error trailer_error =
        DecodeRawRecordTrailerV1(
            wire.subspan(
                trailer_offset, kRawV1RecordTrailerBytes),
            &trailer);
    if (trailer_error != RawV1Error::kNone) {
        return trailer_error == RawV1Error::kInvalidMagic
                   ? RawV1Error::kTrailerMismatch
                   : trailer_error;
    }
    if (trailer.record_size != header.record_size ||
        trailer.ingress_sequence != header.ingress_sequence) {
        return RawV1Error::kTrailerMismatch;
    }

    RawRecordViewV1 decoded{};
    decoded.header = header;
    decoded.vendor_head = vendor_head;
    decoded.vendor_body = vendor_body;
    decoded.padding = padding;
    decoded.trailer = trailer;
    *record = decoded;
    return RawV1Error::kNone;
}

RawV1Error ValidateRawRecordV1(
    std::span<const std::byte> wire,
    const RawRecordNamespaceV1* expected_namespace) noexcept {
    RawRecordViewV1 decoded{};
    return DecodeRawRecordV1(
        wire, &decoded, expected_namespace);
}

}  // namespace l2flow::ingress
