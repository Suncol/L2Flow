#include "l2flow/ingress/raw_index_v1.h"

#include "l2flow/common/crc32c.h"
#include "l2flow/common/identity128.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <utility>
#include <vector>

namespace l2flow::ingress {
namespace {

constexpr std::size_t kCrcBytes = sizeof(std::uint32_t);
constexpr std::uint64_t kMinimumRawRecordBytes =
    ((static_cast<std::uint64_t>(kRawV1RecordHeaderBytes) +
      static_cast<std::uint64_t>(kVendorMessageHeadBytes) +
      static_cast<std::uint64_t>(kRawV1RecordTrailerBytes) +
      static_cast<std::uint64_t>(kRawV1RecordAlignment) - 1U) /
     static_cast<std::uint64_t>(kRawV1RecordAlignment)) *
    static_cast<std::uint64_t>(kRawV1RecordAlignment);

std::uint16_t LoadU16Le(
    std::span<const std::byte> wire,
    std::size_t offset) noexcept {
    return static_cast<std::uint16_t>(
        std::to_integer<std::uint16_t>(wire[offset]) |
        static_cast<std::uint16_t>(
            std::to_integer<std::uint16_t>(wire[offset + 1U])
            << 8U));
}

std::uint32_t LoadU32Le(
    std::span<const std::byte> wire,
    std::size_t offset) noexcept {
    std::uint32_t value = 0U;
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        value |=
            std::to_integer<std::uint32_t>(wire[offset + index])
            << shift;
    }
    return value;
}

std::uint64_t LoadU64Le(
    std::span<const std::byte> wire,
    std::size_t offset) noexcept {
    std::uint64_t value = 0U;
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        value |=
            std::to_integer<std::uint64_t>(wire[offset + index])
            << shift;
    }
    return value;
}

void StoreU16Le(
    std::uint16_t value,
    std::span<std::byte> wire,
    std::size_t offset) noexcept {
    wire[offset] =
        static_cast<std::byte>(value & 0xffU);
    wire[offset + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32Le(
    std::uint32_t value,
    std::span<std::byte> wire,
    std::size_t offset) noexcept {
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        wire[offset + index] =
            static_cast<std::byte>((value >> shift) & 0xffU);
    }
}

void StoreU64Le(
    std::uint64_t value,
    std::span<std::byte> wire,
    std::size_t offset) noexcept {
    for (std::size_t index = 0U;
         index < sizeof(value);
         ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        wire[offset + index] =
            static_cast<std::byte>((value >> shift) & 0xffU);
    }
}

void StoreBytes(
    std::span<const std::byte> value,
    std::span<std::byte> wire,
    std::size_t offset) noexcept {
    std::copy(value.begin(), value.end(), wire.begin() + offset);
}

void LoadBytes(
    std::span<const std::byte> wire,
    std::size_t offset,
    std::span<std::byte> value) noexcept {
    std::copy_n(wire.begin() + offset, value.size(), value.begin());
}

bool IsZero(std::span<const std::byte> bytes) noexcept {
    return std::all_of(
        bytes.begin(),
        bytes.end(),
        [](std::byte value) {
            return value == std::byte{0};
        });
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

bool CheckedAdd(
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

RawIndexV1Error ComputeFileSize(
    std::uint64_t entry_count,
    std::size_t* size) noexcept {
    if (size == nullptr) {
        return RawIndexV1Error::kNullOutput;
    }
    constexpr std::uint64_t fixed_bytes =
        static_cast<std::uint64_t>(kRawIndexV1HeaderBytes) +
        static_cast<std::uint64_t>(kRawIndexV1FooterBytes);
    if (entry_count >
        (std::numeric_limits<std::uint64_t>::max() -
         fixed_bytes) /
            static_cast<std::uint64_t>(kRawIndexV1EntryBytes)) {
        return RawIndexV1Error::kLengthOverflow;
    }
    const std::uint64_t file_size =
        fixed_bytes +
        entry_count *
            static_cast<std::uint64_t>(kRawIndexV1EntryBytes);
    if (file_size >
        static_cast<std::uint64_t>(
            std::numeric_limits<std::size_t>::max())) {
        return RawIndexV1Error::kLengthOverflow;
    }
    *size = static_cast<std::size_t>(file_size);
    return RawIndexV1Error::kNone;
}

RawIndexV1Error ValidateHeaderLogical(
    const RawIndexHeaderV1& header) noexcept {
    if ((header.flags & ~kRawIndexV1HeaderFlagsMask) != 0U) {
        return RawIndexV1Error::kUnknownFlags;
    }
    if (header.capture_date == 0U ||
        header.source_stream_id == 0U ||
        header.segment_sequence == 0U ||
        l2flow::common::IsZeroIdentity(header.stream_day_id) ||
        IsZero(header.raw_schema_sha256)) {
        return RawIndexV1Error::kInvalidIdentity;
    }
    if ((header.segment_sequence == 1U &&
         header.segment_base_wal_pos != 0U) ||
        (header.segment_sequence != 1U &&
         header.segment_base_wal_pos <
             kRawV1SegmentHeaderBytes) ||
        header.segment_base_wal_pos >
            std::numeric_limits<std::uint64_t>::max() -
                kRawV1SegmentHeaderBytes) {
        return RawIndexV1Error::kInvalidIdentity;
    }
    if (header.sample_record_interval == 0U ||
        header.sample_record_interval >
            kRawIndexV1DefaultRecordInterval ||
        header.sample_raw_bytes_interval == 0U ||
        header.sample_raw_bytes_interval >
            kRawIndexV1DefaultRawBytesInterval) {
        return RawIndexV1Error::kInvalidSamplingThreshold;
    }
    return RawIndexV1Error::kNone;
}

RawIndexV1Error ValidateEntryLogical(
    const RawIndexEntryV1& entry,
    const RawIndexHeaderV1& header) noexcept {
    if (entry.ingress_sequence == 0U ||
        entry.segment_file_offset <
            kRawV1SegmentHeaderBytes ||
        (entry.segment_file_offset %
         static_cast<std::uint64_t>(kRawV1RecordAlignment)) != 0U ||
        entry.record_end_wal_pos <= entry.record_start_wal_pos) {
        return RawIndexV1Error::kInvalidEntry;
    }
    const std::uint64_t record_size =
        entry.record_end_wal_pos - entry.record_start_wal_pos;
    if (record_size < kMinimumRawRecordBytes ||
        record_size >
            std::numeric_limits<std::uint32_t>::max() ||
        (record_size %
         static_cast<std::uint64_t>(kRawV1RecordAlignment)) != 0U) {
        return RawIndexV1Error::kInvalidEntry;
    }
    std::uint64_t expected_start = 0U;
    if (!CheckedAdd(
            header.segment_base_wal_pos,
            entry.segment_file_offset,
            &expected_start) ||
        expected_start != entry.record_start_wal_pos) {
        return RawIndexV1Error::kInvalidEntry;
    }
    return RawIndexV1Error::kNone;
}

RawIndexV1Error DecodeAcceptedSeal(
    const RawIndexFooterV1& footer,
    DurableMarkerV1* marker) noexcept {
    if (marker == nullptr) {
        return RawIndexV1Error::kNullOutput;
    }
    DurableMarkerV1 decoded{};
    const RawV1Error marker_error =
        DecodeDurableMarkerV1(
            footer.accepted_segment_sealed_marker,
            &decoded);
    if (marker_error != RawV1Error::kNone ||
        decoded.marker_flags != kRawV1SegmentSealed ||
        decoded.durable_segment_offset !=
            footer.segment_logical_end_offset) {
        return RawIndexV1Error::kInvalidSealMarker;
    }
    *marker = decoded;
    return RawIndexV1Error::kNone;
}

RawIndexV1Error ValidateFooterLogical(
    const RawIndexFooterV1& footer,
    DurableMarkerV1* marker) noexcept {
    if ((footer.flags & ~kRawIndexV1FooterFlagsMask) != 0U) {
        return RawIndexV1Error::kUnknownFlags;
    }
    if (footer.segment_logical_end_offset <
            kRawV1SegmentHeaderBytes ||
        IsZero(footer.segment_sha256)) {
        return RawIndexV1Error::kInvalidFooter;
    }
    return DecodeAcceptedSeal(footer, marker);
}

RawIndexV1Error ValidateFileSemantics(
    const RawIndexHeaderV1& header,
    std::span<const RawIndexEntryV1> entries,
    const RawIndexFooterV1& footer,
    DurableMarkerV1* accepted_marker) noexcept {
    if (footer.entry_count !=
        static_cast<std::uint64_t>(entries.size()) ||
        footer.entry_count > footer.segment_record_count) {
        return RawIndexV1Error::kEntryCountMismatch;
    }
    const bool empty = footer.segment_record_count == 0U;
    if ((empty && (!entries.empty() ||
                   footer.segment_logical_end_offset !=
                       kRawV1SegmentHeaderBytes)) ||
        (!empty && entries.empty())) {
        return RawIndexV1Error::kEntryCountMismatch;
    }

    DurableMarkerV1 marker{};
    RawIndexV1Error result =
        ValidateFooterLogical(footer, &marker);
    if (result != RawIndexV1Error::kNone) {
        return result;
    }

    std::uint64_t expected_global_end = 0U;
    if (!CheckedAdd(
            header.segment_base_wal_pos,
            footer.segment_logical_end_offset,
            &expected_global_end)) {
        return RawIndexV1Error::kLengthOverflow;
    }
    if (marker.source_stream_id != header.source_stream_id ||
        marker.segment_sequence != header.segment_sequence ||
        marker.durable_global_wal_pos != expected_global_end) {
        return RawIndexV1Error::kMarkerMismatch;
    }

    if (!entries.empty()) {
        const RawIndexEntryV1* previous = nullptr;
        for (const RawIndexEntryV1& entry : entries) {
            result = ValidateEntryLogical(entry, header);
            if (result != RawIndexV1Error::kNone) {
                return result;
            }
            if (entry.record_end_wal_pos > expected_global_end) {
                return RawIndexV1Error::kInvalidEntry;
            }
            if (previous != nullptr) {
                if (entry.ingress_sequence <=
                        previous->ingress_sequence ||
                    entry.record_start_wal_pos <
                        previous->record_end_wal_pos ||
                    entry.record_end_wal_pos <=
                        previous->record_end_wal_pos ||
                    entry.segment_file_offset <=
                        previous->segment_file_offset ||
                    entry.ingress_sequence -
                            previous->ingress_sequence >
                        header.sample_record_interval) {
                    return RawIndexV1Error::kEntryOrderViolation;
                }
            }
            previous = &entry;
        }

        const RawIndexEntryV1& first = entries.front();
        const RawIndexEntryV1& last = entries.back();
        if (first.segment_file_offset !=
                kRawV1SegmentHeaderBytes ||
            last.record_end_wal_pos != expected_global_end ||
            last.ingress_sequence !=
                marker.durable_ingress_sequence) {
            return RawIndexV1Error::kInvalidEntry;
        }
        if (last.ingress_sequence < first.ingress_sequence ||
            last.ingress_sequence - first.ingress_sequence ==
                std::numeric_limits<std::uint64_t>::max() ||
            last.ingress_sequence - first.ingress_sequence + 1U !=
                footer.segment_record_count) {
            return RawIndexV1Error::kEntryCountMismatch;
        }
    }

    if (accepted_marker != nullptr) {
        *accepted_marker = marker;
    }
    return RawIndexV1Error::kNone;
}

RawIndexV1Error DecodeFileIntrinsic(
    std::span<const std::byte> wire,
    RawIndexFileV1* index) noexcept {
    constexpr std::size_t minimum_size =
        kRawIndexV1HeaderBytes + kRawIndexV1FooterBytes;
    if (wire.size() < minimum_size) {
        return RawIndexV1Error::kInvalidWireSize;
    }

    RawIndexHeaderV1 header{};
    RawIndexV1Error result =
        DecodeRawIndexHeaderV1(
            wire.first(kRawIndexV1HeaderBytes), &header);
    if (result != RawIndexV1Error::kNone) {
        return result;
    }

    const std::size_t footer_offset =
        wire.size() - kRawIndexV1FooterBytes;
    RawIndexFooterV1 footer{};
    result = DecodeRawIndexFooterV1(
        wire.subspan(footer_offset, kRawIndexV1FooterBytes),
        &footer);
    if (result != RawIndexV1Error::kNone) {
        return result;
    }

    std::size_t expected_size = 0U;
    result = ComputeFileSize(footer.entry_count, &expected_size);
    if (result != RawIndexV1Error::kNone) {
        return result;
    }
    if (wire.size() != expected_size) {
        return RawIndexV1Error::kEntryCountMismatch;
    }

    const std::size_t file_crc_offset =
        footer_offset +
        raw_index_v1_offset::footer::kIndexFileCrc32c;
    if (ComputeCrcWithZeroedField(wire, file_crc_offset) !=
        footer.index_file_crc32c) {
        return RawIndexV1Error::kFileCrcMismatch;
    }

    std::vector<RawIndexEntryV1> entries;
    try {
        entries.resize(
            static_cast<std::size_t>(footer.entry_count));
    } catch (...) {
        return RawIndexV1Error::kAllocationFailure;
    }
    std::size_t entry_offset = kRawIndexV1HeaderBytes;
    for (RawIndexEntryV1& entry : entries) {
        result = DecodeRawIndexEntryV1(
            wire.subspan(entry_offset, kRawIndexV1EntryBytes),
            &entry);
        if (result != RawIndexV1Error::kNone) {
            return result;
        }
        entry_offset += kRawIndexV1EntryBytes;
    }

    DurableMarkerV1 accepted_marker{};
    result = ValidateFileSemantics(
        header, entries, footer, &accepted_marker);
    if (result != RawIndexV1Error::kNone) {
        return result;
    }

    if (index != nullptr) {
        RawIndexFileV1 decoded{};
        decoded.header = header;
        decoded.entries = std::move(entries);
        decoded.footer = footer;
        decoded.accepted_segment_sealed_marker =
            accepted_marker;
        *index = std::move(decoded);
    }
    return RawIndexV1Error::kNone;
}

}  // namespace

std::string_view RawIndexV1ErrorName(
    RawIndexV1Error error) noexcept {
    switch (error) {
    case RawIndexV1Error::kNone:
        return "none";
    case RawIndexV1Error::kNullOutput:
        return "null output";
    case RawIndexV1Error::kInvalidWireSize:
        return "invalid wire size";
    case RawIndexV1Error::kInvalidMagic:
        return "invalid magic";
    case RawIndexV1Error::kUnsupportedVersion:
        return "unsupported version";
    case RawIndexV1Error::kInvalidEndian:
        return "invalid endian";
    case RawIndexV1Error::kInvalidHeaderSize:
        return "invalid header size";
    case RawIndexV1Error::kInvalidEntrySize:
        return "invalid entry size";
    case RawIndexV1Error::kInvalidFooterSize:
        return "invalid footer size";
    case RawIndexV1Error::kUnknownFlags:
        return "unknown flags";
    case RawIndexV1Error::kNonzeroReserved:
        return "nonzero reserved";
    case RawIndexV1Error::kInvalidIdentity:
        return "invalid identity";
    case RawIndexV1Error::kInvalidSamplingThreshold:
        return "invalid sampling threshold";
    case RawIndexV1Error::kHeaderCrcMismatch:
        return "header CRC mismatch";
    case RawIndexV1Error::kEntryCrcMismatch:
        return "entry CRC mismatch";
    case RawIndexV1Error::kFileCrcMismatch:
        return "file CRC mismatch";
    case RawIndexV1Error::kLengthOverflow:
        return "length overflow";
    case RawIndexV1Error::kEntryCountMismatch:
        return "entry count mismatch";
    case RawIndexV1Error::kSchemaMismatch:
        return "Raw schema mismatch";
    case RawIndexV1Error::kNamespaceMismatch:
        return "namespace mismatch";
    case RawIndexV1Error::kSegmentMismatch:
        return "segment mismatch";
    case RawIndexV1Error::kInvalidEntry:
        return "invalid entry";
    case RawIndexV1Error::kEntryOrderViolation:
        return "entry order violation";
    case RawIndexV1Error::kInvalidFooter:
        return "invalid footer";
    case RawIndexV1Error::kInvalidSealMarker:
        return "invalid seal marker";
    case RawIndexV1Error::kMarkerMismatch:
        return "seal marker mismatch";
    case RawIndexV1Error::kAllocationFailure:
        return "allocation failure";
    }
    return "unknown RawIndex V1 error";
}

RawIndexV1Error EncodeRawIndexHeaderV1(
    const RawIndexHeaderV1& header,
    RawIndexV1HeaderWire* wire) noexcept {
    if (wire == nullptr) {
        return RawIndexV1Error::kNullOutput;
    }
    const RawIndexV1Error validation =
        ValidateHeaderLogical(header);
    if (validation != RawIndexV1Error::kNone) {
        return validation;
    }

    RawIndexV1HeaderWire encoded{};
    std::span<std::byte> bytes(encoded);
    using namespace raw_index_v1_offset::header;
    StoreU32Le(kRawIndexV1HeaderMagic, bytes, kMagic);
    StoreU16Le(kRawIndexV1FormatVersion, bytes, kFormatVersion);
    bytes[kEndian] =
        static_cast<std::byte>(kRawIndexV1LittleEndian);
    StoreU32Le(
        static_cast<std::uint32_t>(kRawIndexV1HeaderBytes),
        bytes,
        kHeaderSize);
    StoreU32Le(
        static_cast<std::uint32_t>(kRawIndexV1EntryBytes),
        bytes,
        kEntrySize);
    StoreU32Le(
        static_cast<std::uint32_t>(kRawIndexV1FooterBytes),
        bytes,
        kFooterSize);
    StoreU32Le(header.flags, bytes, kFlags);
    StoreU32Le(header.capture_date, bytes, kCaptureDate);
    StoreU32Le(
        header.source_stream_id, bytes, kSourceStreamId);
    StoreBytes(header.stream_day_id, bytes, kStreamDayId);
    StoreU32Le(
        header.segment_sequence, bytes, kSegmentSequence);
    StoreU64Le(
        header.segment_base_wal_pos,
        bytes,
        kSegmentBaseWalPos);
    StoreBytes(
        header.raw_schema_sha256, bytes, kRawSchemaSha256);
    StoreU64Le(
        header.sample_record_interval,
        bytes,
        kSampleRecordInterval);
    StoreU64Le(
        header.sample_raw_bytes_interval,
        bytes,
        kSampleRawBytesInterval);
    const std::uint32_t crc =
        l2flow::common::ComputeCrc32c(encoded);
    StoreU32Le(crc, bytes, kHeaderCrc32c);
    *wire = encoded;
    return RawIndexV1Error::kNone;
}

RawIndexV1Error DecodeRawIndexHeaderV1(
    std::span<const std::byte> wire,
    RawIndexHeaderV1* header) noexcept {
    if (header == nullptr) {
        return RawIndexV1Error::kNullOutput;
    }
    if (wire.size() != kRawIndexV1HeaderBytes) {
        return RawIndexV1Error::kInvalidWireSize;
    }
    using namespace raw_index_v1_offset::header;
    if (LoadU32Le(wire, kMagic) != kRawIndexV1HeaderMagic) {
        return RawIndexV1Error::kInvalidMagic;
    }
    if (LoadU16Le(wire, kFormatVersion) !=
        kRawIndexV1FormatVersion) {
        return RawIndexV1Error::kUnsupportedVersion;
    }
    if (std::to_integer<std::uint8_t>(wire[kEndian]) !=
        kRawIndexV1LittleEndian) {
        return RawIndexV1Error::kInvalidEndian;
    }
    if (LoadU32Le(wire, kHeaderSize) !=
        kRawIndexV1HeaderBytes) {
        return RawIndexV1Error::kInvalidHeaderSize;
    }
    if (LoadU32Le(wire, kEntrySize) !=
        kRawIndexV1EntryBytes) {
        return RawIndexV1Error::kInvalidEntrySize;
    }
    if (LoadU32Le(wire, kFooterSize) !=
        kRawIndexV1FooterBytes) {
        return RawIndexV1Error::kInvalidFooterSize;
    }
    if (!IsZeroRange(wire, kReserved0, 1U) ||
        !IsZeroRange(wire, kReserved1, 4U) ||
        !IsZeroRange(
            wire,
            kReservedTail,
            kRawIndexV1HeaderBytes - kReservedTail)) {
        return RawIndexV1Error::kNonzeroReserved;
    }
    if ((LoadU32Le(wire, kFlags) &
         ~kRawIndexV1HeaderFlagsMask) != 0U) {
        return RawIndexV1Error::kUnknownFlags;
    }
    const std::uint32_t stored_crc =
        LoadU32Le(wire, kHeaderCrc32c);
    if (ComputeCrcWithZeroedField(wire, kHeaderCrc32c) !=
        stored_crc) {
        return RawIndexV1Error::kHeaderCrcMismatch;
    }

    RawIndexHeaderV1 decoded{};
    decoded.flags = LoadU32Le(wire, kFlags);
    decoded.capture_date = LoadU32Le(wire, kCaptureDate);
    decoded.source_stream_id =
        LoadU32Le(wire, kSourceStreamId);
    LoadBytes(
        wire, kStreamDayId, decoded.stream_day_id);
    decoded.segment_sequence =
        LoadU32Le(wire, kSegmentSequence);
    decoded.segment_base_wal_pos =
        LoadU64Le(wire, kSegmentBaseWalPos);
    LoadBytes(
        wire, kRawSchemaSha256, decoded.raw_schema_sha256);
    decoded.sample_record_interval =
        LoadU64Le(wire, kSampleRecordInterval);
    decoded.sample_raw_bytes_interval =
        LoadU64Le(wire, kSampleRawBytesInterval);
    decoded.header_crc32c = stored_crc;
    const RawIndexV1Error validation =
        ValidateHeaderLogical(decoded);
    if (validation != RawIndexV1Error::kNone) {
        return validation;
    }
    *header = decoded;
    return RawIndexV1Error::kNone;
}

RawIndexV1Error EncodeRawIndexEntryV1(
    const RawIndexEntryV1& entry,
    RawIndexV1EntryWire* wire) noexcept {
    if (wire == nullptr) {
        return RawIndexV1Error::kNullOutput;
    }
    if (entry.ingress_sequence == 0U ||
        entry.record_end_wal_pos <= entry.record_start_wal_pos ||
        entry.segment_file_offset <
            kRawV1SegmentHeaderBytes) {
        return RawIndexV1Error::kInvalidEntry;
    }

    RawIndexV1EntryWire encoded{};
    std::span<std::byte> bytes(encoded);
    using namespace raw_index_v1_offset::entry;
    StoreU64Le(
        entry.ingress_sequence, bytes, kIngressSequence);
    StoreU64Le(
        entry.record_start_wal_pos,
        bytes,
        kRecordStartWalPos);
    StoreU64Le(
        entry.record_end_wal_pos,
        bytes,
        kRecordEndWalPos);
    StoreU64Le(
        entry.segment_file_offset,
        bytes,
        kSegmentFileOffset);
    StoreU64Le(
        entry.vendor_sequence_id, bytes, kVendorSequenceId);
    StoreU64Le(
        entry.recv_monotonic_ns, bytes, kRecvMonotonicNs);
    StoreU32Le(
        entry.connection_epoch_hint,
        bytes,
        kConnectionEpochHint);
    StoreU16Le(
        entry.vendor_service_version,
        bytes,
        kVendorServiceVersion);
    StoreU16Le(
        entry.vendor_message_id, bytes, kVendorMessageId);
    bytes[kVendorServiceId] =
        static_cast<std::byte>(entry.vendor_service_id);
    const std::uint32_t crc =
        l2flow::common::ComputeCrc32c(encoded);
    StoreU32Le(crc, bytes, kEntryCrc32c);
    *wire = encoded;
    return RawIndexV1Error::kNone;
}

RawIndexV1Error DecodeRawIndexEntryV1(
    std::span<const std::byte> wire,
    RawIndexEntryV1* entry) noexcept {
    if (entry == nullptr) {
        return RawIndexV1Error::kNullOutput;
    }
    if (wire.size() != kRawIndexV1EntryBytes) {
        return RawIndexV1Error::kInvalidWireSize;
    }
    using namespace raw_index_v1_offset::entry;
    if (!IsZeroRange(wire, kReserved, 3U)) {
        return RawIndexV1Error::kNonzeroReserved;
    }
    const std::uint32_t stored_crc =
        LoadU32Le(wire, kEntryCrc32c);
    if (ComputeCrcWithZeroedField(wire, kEntryCrc32c) !=
        stored_crc) {
        return RawIndexV1Error::kEntryCrcMismatch;
    }

    RawIndexEntryV1 decoded{};
    decoded.ingress_sequence =
        LoadU64Le(wire, kIngressSequence);
    decoded.record_start_wal_pos =
        LoadU64Le(wire, kRecordStartWalPos);
    decoded.record_end_wal_pos =
        LoadU64Le(wire, kRecordEndWalPos);
    decoded.segment_file_offset =
        LoadU64Le(wire, kSegmentFileOffset);
    decoded.vendor_sequence_id =
        LoadU64Le(wire, kVendorSequenceId);
    decoded.recv_monotonic_ns =
        LoadU64Le(wire, kRecvMonotonicNs);
    decoded.connection_epoch_hint =
        LoadU32Le(wire, kConnectionEpochHint);
    decoded.vendor_service_version =
        LoadU16Le(wire, kVendorServiceVersion);
    decoded.vendor_message_id =
        LoadU16Le(wire, kVendorMessageId);
    decoded.vendor_service_id =
        std::to_integer<std::uint8_t>(wire[kVendorServiceId]);
    decoded.entry_crc32c = stored_crc;
    if (decoded.ingress_sequence == 0U ||
        decoded.record_end_wal_pos <=
            decoded.record_start_wal_pos ||
        decoded.segment_file_offset <
            kRawV1SegmentHeaderBytes) {
        return RawIndexV1Error::kInvalidEntry;
    }
    *entry = decoded;
    return RawIndexV1Error::kNone;
}

RawIndexV1Error EncodeRawIndexFooterV1(
    const RawIndexFooterV1& footer,
    RawIndexV1FooterWire* wire) noexcept {
    if (wire == nullptr) {
        return RawIndexV1Error::kNullOutput;
    }
    DurableMarkerV1 marker{};
    const RawIndexV1Error validation =
        ValidateFooterLogical(footer, &marker);
    if (validation != RawIndexV1Error::kNone) {
        return validation;
    }

    RawIndexV1FooterWire encoded{};
    std::span<std::byte> bytes(encoded);
    using namespace raw_index_v1_offset::footer;
    StoreU32Le(kRawIndexV1FooterMagic, bytes, kMagic);
    StoreU16Le(kRawIndexV1FormatVersion, bytes, kFormatVersion);
    bytes[kEndian] =
        static_cast<std::byte>(kRawIndexV1LittleEndian);
    StoreU32Le(
        static_cast<std::uint32_t>(kRawIndexV1FooterBytes),
        bytes,
        kFooterSize);
    StoreU32Le(footer.flags, bytes, kFlags);
    StoreU64Le(footer.entry_count, bytes, kEntryCount);
    StoreU64Le(
        footer.segment_record_count,
        bytes,
        kSegmentRecordCount);
    StoreU64Le(
        footer.segment_logical_end_offset,
        bytes,
        kSegmentLogicalEndOffset);
    StoreBytes(
        footer.segment_sha256, bytes, kSegmentSha256);
    StoreBytes(
        footer.accepted_segment_sealed_marker,
        bytes,
        kAcceptedSegmentSealedMarker);
    StoreU32Le(
        footer.index_file_crc32c,
        bytes,
        kIndexFileCrc32c);
    *wire = encoded;
    return RawIndexV1Error::kNone;
}

RawIndexV1Error DecodeRawIndexFooterV1(
    std::span<const std::byte> wire,
    RawIndexFooterV1* footer) noexcept {
    if (footer == nullptr) {
        return RawIndexV1Error::kNullOutput;
    }
    if (wire.size() != kRawIndexV1FooterBytes) {
        return RawIndexV1Error::kInvalidWireSize;
    }
    using namespace raw_index_v1_offset::footer;
    if (LoadU32Le(wire, kMagic) != kRawIndexV1FooterMagic) {
        return RawIndexV1Error::kInvalidMagic;
    }
    if (LoadU16Le(wire, kFormatVersion) !=
        kRawIndexV1FormatVersion) {
        return RawIndexV1Error::kUnsupportedVersion;
    }
    if (std::to_integer<std::uint8_t>(wire[kEndian]) !=
        kRawIndexV1LittleEndian) {
        return RawIndexV1Error::kInvalidEndian;
    }
    if (LoadU32Le(wire, kFooterSize) !=
        kRawIndexV1FooterBytes) {
        return RawIndexV1Error::kInvalidFooterSize;
    }
    if (!IsZeroRange(wire, kReserved0, 1U) ||
        !IsZeroRange(
            wire,
            kReservedTail,
            kRawIndexV1FooterBytes - kReservedTail)) {
        return RawIndexV1Error::kNonzeroReserved;
    }
    if ((LoadU32Le(wire, kFlags) &
         ~kRawIndexV1FooterFlagsMask) != 0U) {
        return RawIndexV1Error::kUnknownFlags;
    }

    RawIndexFooterV1 decoded{};
    decoded.flags = LoadU32Le(wire, kFlags);
    decoded.entry_count = LoadU64Le(wire, kEntryCount);
    decoded.segment_record_count =
        LoadU64Le(wire, kSegmentRecordCount);
    decoded.segment_logical_end_offset =
        LoadU64Le(wire, kSegmentLogicalEndOffset);
    LoadBytes(
        wire, kSegmentSha256, decoded.segment_sha256);
    LoadBytes(
        wire,
        kAcceptedSegmentSealedMarker,
        decoded.accepted_segment_sealed_marker);
    decoded.index_file_crc32c =
        LoadU32Le(wire, kIndexFileCrc32c);
    DurableMarkerV1 marker{};
    const RawIndexV1Error validation =
        ValidateFooterLogical(decoded, &marker);
    if (validation != RawIndexV1Error::kNone) {
        return validation;
    }
    *footer = decoded;
    return RawIndexV1Error::kNone;
}

RawIndexV1Error BuildRawIndexFileV1(
    const RawIndexHeaderV1& header,
    std::span<const RawIndexEntryV1> entries,
    const RawIndexFooterV1& footer,
    std::vector<std::byte>* wire) noexcept {
    if (wire == nullptr) {
        return RawIndexV1Error::kNullOutput;
    }
    if (entries.size() >
        static_cast<std::size_t>(
            std::numeric_limits<std::uint64_t>::max())) {
        return RawIndexV1Error::kLengthOverflow;
    }
    if (footer.entry_count !=
        static_cast<std::uint64_t>(entries.size())) {
        return RawIndexV1Error::kEntryCountMismatch;
    }

    RawIndexV1HeaderWire header_wire{};
    RawIndexV1Error result =
        EncodeRawIndexHeaderV1(header, &header_wire);
    if (result != RawIndexV1Error::kNone) {
        return result;
    }
    DurableMarkerV1 accepted_marker{};
    result = ValidateFileSemantics(
        header, entries, footer, &accepted_marker);
    if (result != RawIndexV1Error::kNone) {
        return result;
    }

    std::size_t file_size = 0U;
    result = ComputeFileSize(footer.entry_count, &file_size);
    if (result != RawIndexV1Error::kNone) {
        return result;
    }
    std::vector<std::byte> encoded;
    try {
        encoded.resize(file_size, std::byte{0});
    } catch (...) {
        return RawIndexV1Error::kAllocationFailure;
    }
    std::copy(
        header_wire.begin(), header_wire.end(), encoded.begin());

    std::size_t offset = kRawIndexV1HeaderBytes;
    for (const RawIndexEntryV1& entry : entries) {
        RawIndexV1EntryWire entry_wire{};
        result = EncodeRawIndexEntryV1(entry, &entry_wire);
        if (result != RawIndexV1Error::kNone) {
            return result;
        }
        std::copy(
            entry_wire.begin(),
            entry_wire.end(),
            encoded.begin() + offset);
        offset += kRawIndexV1EntryBytes;
    }

    RawIndexFooterV1 footer_with_zero_crc = footer;
    footer_with_zero_crc.index_file_crc32c = 0U;
    RawIndexV1FooterWire footer_wire{};
    result = EncodeRawIndexFooterV1(
        footer_with_zero_crc, &footer_wire);
    if (result != RawIndexV1Error::kNone) {
        return result;
    }
    std::copy(
        footer_wire.begin(),
        footer_wire.end(),
        encoded.begin() + offset);
    const std::uint32_t file_crc =
        l2flow::common::ComputeCrc32c(encoded);
    StoreU32Le(
        file_crc,
        encoded,
        offset +
            raw_index_v1_offset::footer::kIndexFileCrc32c);
    *wire = std::move(encoded);
    return RawIndexV1Error::kNone;
}

RawIndexV1Error DecodeRawIndexFileV1(
    std::span<const std::byte> wire,
    RawIndexFileV1* index) noexcept {
    if (index == nullptr) {
        return RawIndexV1Error::kNullOutput;
    }
    return DecodeFileIntrinsic(wire, index);
}

RawIndexV1Error ValidateRawIndexFileV1(
    std::span<const std::byte> wire,
    const RawIndexExpectedIdentityV1& expected,
    RawIndexFileV1* index) noexcept {
    RawIndexFileV1 decoded{};
    RawIndexV1Error result =
        DecodeFileIntrinsic(wire, &decoded);
    if (result != RawIndexV1Error::kNone) {
        return result;
    }
    if (decoded.header.raw_schema_sha256 !=
        expected.raw_schema_sha256) {
        return RawIndexV1Error::kSchemaMismatch;
    }
    if (decoded.header.capture_date != expected.capture_date ||
        decoded.header.source_stream_id !=
            expected.source_stream_id ||
        decoded.header.stream_day_id != expected.stream_day_id) {
        return RawIndexV1Error::kNamespaceMismatch;
    }
    if (decoded.header.segment_sequence !=
            expected.segment_sequence ||
        decoded.header.segment_base_wal_pos !=
            expected.segment_base_wal_pos ||
        decoded.header.sample_record_interval !=
            expected.sample_record_interval ||
        decoded.header.sample_raw_bytes_interval !=
            expected.sample_raw_bytes_interval) {
        return RawIndexV1Error::kSegmentMismatch;
    }
    if (index != nullptr) {
        *index = std::move(decoded);
    }
    return RawIndexV1Error::kNone;
}

RawIndexBuilderV1::RawIndexBuilderV1(
    RawIndexHeaderV1 header) noexcept
    : header_(std::move(header)) {
    RawIndexV1HeaderWire ignored{};
    error_ = EncodeRawIndexHeaderV1(header_, &ignored);
}

RawIndexV1Error RawIndexBuilderV1::AddRecord(
    const RawIndexEntryV1& record) noexcept {
    if (error_ != RawIndexV1Error::kNone) {
        return error_;
    }
    if (record_count_ ==
        std::numeric_limits<std::uint64_t>::max()) {
        error_ = RawIndexV1Error::kLengthOverflow;
        return error_;
    }
    RawIndexV1Error validation =
        ValidateEntryLogical(record, header_);
    if (validation != RawIndexV1Error::kNone) {
        error_ = validation;
        return error_;
    }
    if (!have_last_record_) {
        if (record.segment_file_offset !=
            kRawV1SegmentHeaderBytes) {
            error_ = RawIndexV1Error::kInvalidEntry;
            return error_;
        }
    } else {
        if (last_record_.ingress_sequence ==
                std::numeric_limits<std::uint64_t>::max() ||
            record.ingress_sequence !=
                last_record_.ingress_sequence + 1U ||
            record.record_start_wal_pos !=
                last_record_.record_end_wal_pos) {
            error_ = RawIndexV1Error::kEntryOrderViolation;
            return error_;
        }
    }

    const std::uint64_t next_record_count = record_count_ + 1U;
    const bool first = !have_last_record_;
    const bool record_threshold =
        !first &&
        next_record_count - last_sample_record_count_ >=
            header_.sample_record_interval;
    const bool byte_threshold =
        !first &&
        record.record_end_wal_pos -
                last_sample_end_wal_pos_ >=
            header_.sample_raw_bytes_interval;
    if (first || record_threshold || byte_threshold) {
        try {
            sampled_entries_.push_back(record);
        } catch (...) {
            error_ = RawIndexV1Error::kAllocationFailure;
            return error_;
        }
        last_sample_record_count_ = next_record_count;
        last_sample_end_wal_pos_ = record.record_end_wal_pos;
    }
    last_record_ = record;
    record_count_ = next_record_count;
    have_last_record_ = true;
    return RawIndexV1Error::kNone;
}

RawIndexV1Error RawIndexBuilderV1::Build(
    const RawV1Digest& segment_sha256,
    const RawV1DurableMarkerWire& accepted_sealed_marker,
    std::vector<std::byte>* wire) const noexcept {
    if (wire == nullptr) {
        return RawIndexV1Error::kNullOutput;
    }
    if (error_ != RawIndexV1Error::kNone) {
        return error_;
    }

    std::vector<RawIndexEntryV1> final_entries;
    try {
        final_entries = sampled_entries_;
        if (have_last_record_ &&
            (final_entries.empty() ||
             final_entries.back().ingress_sequence !=
                 last_record_.ingress_sequence)) {
            final_entries.push_back(last_record_);
        }
    } catch (...) {
        return RawIndexV1Error::kAllocationFailure;
    }

    RawIndexFooterV1 footer{};
    footer.entry_count =
        static_cast<std::uint64_t>(final_entries.size());
    footer.segment_record_count = record_count_;
    footer.segment_logical_end_offset =
        have_last_record_
            ? last_record_.record_end_wal_pos -
                  header_.segment_base_wal_pos
            : static_cast<std::uint64_t>(
                  kRawV1SegmentHeaderBytes);
    footer.segment_sha256 = segment_sha256;
    footer.accepted_segment_sealed_marker =
        accepted_sealed_marker;
    return BuildRawIndexFileV1(
        header_, final_entries, footer, wire);
}

}  // namespace l2flow::ingress
