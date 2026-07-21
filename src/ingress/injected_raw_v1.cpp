#include "l2flow/ingress/injected_raw_v1.h"

#include "l2flow/common/crc32c.h"
#include "l2flow/common/identity128.h"

#include <algorithm>
#include <limits>
#include <map>
#include <new>
#include <set>
#include <tuple>
#include <utility>

namespace l2flow::ingress {
namespace {

constexpr std::size_t kVendorHeadSizeOffset = 0U;
constexpr std::size_t kVendorMessageSizeOffset = 1U;

[[nodiscard]] std::uint16_t LoadU16Le(
    std::span<const std::byte> bytes,
    std::size_t offset) noexcept {
    return static_cast<std::uint16_t>(
        std::to_integer<std::uint8_t>(bytes[offset])) |
        static_cast<std::uint16_t>(
            static_cast<std::uint16_t>(
                std::to_integer<std::uint8_t>(
                    bytes[offset + 1U]))
            << 8U);
}

[[nodiscard]] std::uint32_t LoadU32Le(
    std::span<const std::byte> bytes,
    std::size_t offset) noexcept {
    std::uint32_t value = 0U;
    for (std::size_t index = 0U; index < 4U; ++index) {
        value |=
            static_cast<std::uint32_t>(
                std::to_integer<std::uint8_t>(
                    bytes[offset + index]))
            << (index * 8U);
    }
    return value;
}

[[nodiscard]] std::uint64_t LoadU64Le(
    std::span<const std::byte> bytes,
    std::size_t offset) noexcept {
    std::uint64_t value = 0U;
    for (std::size_t index = 0U; index < 8U; ++index) {
        value |=
            static_cast<std::uint64_t>(
                std::to_integer<std::uint8_t>(
                    bytes[offset + index]))
            << (index * 8U);
    }
    return value;
}

void StoreU16Le(
    std::uint16_t value,
    std::span<std::byte> bytes,
    std::size_t offset) noexcept {
    for (std::size_t index = 0U; index < 2U; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                (value >> (index * 8U)) & 0xffU));
    }
}

void StoreU32Le(
    std::uint32_t value,
    std::span<std::byte> bytes,
    std::size_t offset) noexcept {
    for (std::size_t index = 0U; index < 4U; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                (value >> (index * 8U)) & 0xffU));
    }
}

void StoreU64Le(
    std::uint64_t value,
    std::span<std::byte> bytes,
    std::size_t offset) noexcept {
    for (std::size_t index = 0U; index < 8U; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                (value >> (index * 8U)) & 0xffU));
    }
}

template <std::size_t Size>
void StoreBytes(
    const std::array<std::byte, Size>& value,
    std::span<std::byte> bytes,
    std::size_t offset) noexcept {
    std::copy(value.begin(), value.end(), bytes.begin() + offset);
}

template <std::size_t Size>
[[nodiscard]] std::array<std::byte, Size> LoadBytes(
    std::span<const std::byte> bytes,
    std::size_t offset) noexcept {
    std::array<std::byte, Size> value{};
    std::copy_n(bytes.begin() + offset, Size, value.begin());
    return value;
}

[[nodiscard]] bool IsZero(
    std::span<const std::byte> bytes) noexcept {
    return std::all_of(
        bytes.begin(), bytes.end(), [](std::byte value) {
            return value == std::byte{0U};
        });
}

template <std::size_t Size>
[[nodiscard]] bool IsZero(
    const std::array<std::byte, Size>& bytes) noexcept {
    return IsZero(std::span<const std::byte>(bytes));
}

[[nodiscard]] std::uint32_t ComputeZeroedFieldCrc(
    std::span<const std::byte> bytes,
    std::size_t field_offset) noexcept {
    constexpr std::array<std::byte, 4U> kZero{};
    l2flow::common::Crc32cState crc;
    crc.Update(bytes.first(field_offset));
    crc.Update(kZero);
    crc.Update(bytes.subspan(field_offset + kZero.size()));
    return crc.Finalize();
}

[[nodiscard]] bool AddU64(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        left > std::numeric_limits<std::uint64_t>::max() -
            right) {
        return false;
    }
    *result = left + right;
    return true;
}

[[nodiscard]] bool MulU64(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        (left != 0U &&
         right >
             std::numeric_limits<std::uint64_t>::max() /
                 left)) {
        return false;
    }
    *result = left * right;
    return true;
}

[[nodiscard]] bool SizeToU64(
    std::size_t value,
    std::uint64_t* result) noexcept {
    if (result == nullptr) {
        return false;
    }
    if constexpr (sizeof(std::size_t) >
                  sizeof(std::uint64_t)) {
        if (value >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint64_t>::max())) {
            return false;
        }
    }
    *result = static_cast<std::uint64_t>(value);
    return true;
}

[[nodiscard]] bool U64ToSize(
    std::uint64_t value,
    std::size_t* result) noexcept {
    if (result == nullptr) {
        return false;
    }
    if constexpr (sizeof(std::size_t) <
                  sizeof(std::uint64_t)) {
        if (value >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::size_t>::max())) {
            return false;
        }
    }
    *result = static_cast<std::size_t>(value);
    return true;
}

[[nodiscard]] bool KnownProvenance(
    RawReplayProvenance provenance) noexcept {
    switch (provenance) {
        case RawReplayProvenance::kDurable:
        case RawReplayProvenance::kRecoveredAppendOnly:
            return true;
    }
    return false;
}

[[nodiscard]] bool KnownMutation(
    RawLogicalMutationKind mutation) noexcept {
    switch (mutation) {
        case RawLogicalMutationKind::kNone:
        case RawLogicalMutationKind::kVendorBodyByteXor:
            return true;
    }
    return false;
}

[[nodiscard]] InjectedRawV1Error ValidateParentLocator(
    const InjectedRawParentLocator& locator) noexcept {
    if (locator.capture_date == 0U ||
        locator.source_stream_id == 0U ||
        common::IsZeroIdentity(locator.stream_day_id) ||
        locator.ingress_sequence == 0U ||
        locator.record_start_wal_pos == 0U ||
        locator.record_start_wal_pos >=
            locator.record_end_wal_pos ||
        locator.occurrence == 0U) {
        return InjectedRawV1Error::kInvalidParentLocator;
    }
    return InjectedRawV1Error::kNone;
}

[[nodiscard]] InjectedRawV1Error ValidateSegmentHeader(
    const InjectedRawSegmentHeaderV1& header) noexcept {
    if ((header.flags & ~kInjectedRawV1SegmentFlagsMask) !=
        0U) {
        return InjectedRawV1Error::kUnknownFlags;
    }
    if ((header.flags & kInjectedRawV1Synthetic) == 0U) {
        return InjectedRawV1Error::kUnknownFlags;
    }
    if (common::IsZeroIdentity(header.run_id) ||
        common::IsZeroIdentity(
            header.synthetic_namespace_id) ||
        header.run_id == header.synthetic_namespace_id ||
        IsZero(header.raw_schema_sha256) ||
        IsZero(header.parent_raw_identity_sha256) ||
        IsZero(header.fault_rule_sha256)) {
        return InjectedRawV1Error::kInvalidIdentity;
    }
    if (header.injected_schema_sha256 !=
        kInjectedRawV1SchemaSha256) {
        return InjectedRawV1Error::kSchemaIdentityMismatch;
    }
    if (header.raw_schema_sha256 ==
        header.injected_schema_sha256) {
        return InjectedRawV1Error::kSchemaIdentityAlias;
    }
    if (header.record_count >
            kInjectedRawV1MaximumRecordCount ||
        header.dropped_locator_count >
            kInjectedRawV1MaximumDroppedLocatorCount) {
        return InjectedRawV1Error::kInvalidCount;
    }
    const bool has_parents =
        header.record_count != 0U ||
        header.dropped_locator_count != 0U;
    if ((has_parents &&
         (header.parent_wal_begin == 0U ||
          header.parent_wal_begin >=
              header.parent_wal_end)) ||
        (!has_parents &&
         (header.parent_wal_begin != 0U ||
          header.parent_wal_end != 0U))) {
        return InjectedRawV1Error::kInvalidParentLocator;
    }
    if (header.records_offset !=
            kInjectedRawV1SegmentHeaderBytes ||
        header.dropped_locators_offset <
            header.records_offset ||
        header.logical_size <
            header.dropped_locators_offset ||
        header.logical_size >
            kInjectedRawV1MaximumLogicalBytes) {
        return InjectedRawV1Error::kInvalidSize;
    }

    std::uint64_t minimum_record_bytes = 0U;
    std::uint64_t minimum_dropped_offset = 0U;
    std::uint64_t dropped_bytes = 0U;
    std::uint64_t expected_logical_size = 0U;
    if (!MulU64(
            header.record_count,
            kInjectedRawV1MinimumRecordBytes,
            &minimum_record_bytes) ||
        !AddU64(
            header.records_offset,
            minimum_record_bytes,
            &minimum_dropped_offset) ||
        !MulU64(
            header.dropped_locator_count,
            kInjectedRawV1ParentLocatorBytes,
            &dropped_bytes) ||
        !AddU64(
            header.dropped_locators_offset,
            dropped_bytes,
            &expected_logical_size)) {
        return InjectedRawV1Error::kSizeOverflow;
    }
    if (header.dropped_locators_offset <
            minimum_dropped_offset ||
        header.logical_size != expected_logical_size) {
        return InjectedRawV1Error::kInvalidSize;
    }
    return InjectedRawV1Error::kNone;
}

[[nodiscard]] InjectedRawV1Error ValidateRecordHeader(
    const InjectedRawRecordHeaderV1& header) noexcept {
    if ((header.flags & ~kInjectedRawV1RecordFlagsMask) !=
        0U) {
        return InjectedRawV1Error::kUnknownFlags;
    }
    if (header.synthetic_ingress_sequence == 0U) {
        return InjectedRawV1Error::kInvalidSequence;
    }
    if (header.source_stream_id == 0U ||
        header.capture_date == 0U) {
        return InjectedRawV1Error::kInvalidCaptureMeta;
    }
    if (!KnownProvenance(header.parent_provenance)) {
        return InjectedRawV1Error::kInvalidProvenance;
    }
    if (!KnownMutation(header.mutation_kind)) {
        return InjectedRawV1Error::kInvalidMutation;
    }
    if (header.vendor_head_size !=
        kVendorMessageHeadBytes) {
        return InjectedRawV1Error::kInvalidVendorHead;
    }

    InjectedRawRecordLayoutV1 layout{};
    const InjectedRawV1Error layout_error =
        ComputeInjectedRawRecordLayoutV1(
            header.vendor_body_size, &layout);
    if (layout_error != InjectedRawV1Error::kNone) {
        return layout_error;
    }
    if (header.vendor_message_size !=
            layout.vendor_message_size ||
        header.record_size != layout.record_size) {
        return InjectedRawV1Error::kInvalidSize;
    }

    const bool mutated =
        (header.flags & kInjectedRawV1RecordMutated) != 0U;
    if (header.mutation_kind ==
        RawLogicalMutationKind::kNone) {
        if (mutated ||
            header.mutation_vendor_body_offset != 0U ||
            header.mutation_before != std::byte{0U} ||
            header.mutation_after != std::byte{0U}) {
            return InjectedRawV1Error::kInvalidMutation;
        }
    } else if (
        header.mutation_kind ==
        RawLogicalMutationKind::kVendorBodyByteXor) {
        if (!mutated ||
            header.mutation_vendor_body_offset >=
                header.vendor_body_size ||
            header.mutation_before ==
                header.mutation_after) {
            return InjectedRawV1Error::kInvalidMutation;
        }
    }
    return InjectedRawV1Error::kNone;
}

[[nodiscard]] InjectedRawV1Error ValidateTrailer(
    const InjectedRawRecordTrailerV1& trailer) noexcept {
    if (trailer.synthetic_ingress_sequence == 0U) {
        return InjectedRawV1Error::kInvalidSequence;
    }
    if (trailer.record_size <
            kInjectedRawV1MinimumRecordBytes ||
        trailer.record_size >
            kInjectedRawV1MaximumRecordBytes ||
        trailer.record_size %
                kInjectedRawV1RecordAlignment !=
            0U) {
        return InjectedRawV1Error::kInvalidSize;
    }
    return InjectedRawV1Error::kNone;
}

using ParentBaseKey = std::tuple<
    std::uint32_t,
    std::uint32_t,
    RawV1Identity,
    std::uint64_t,
    std::uint64_t,
    std::uint64_t>;

[[nodiscard]] ParentBaseKey MakeParentBaseKey(
    const InjectedRawParentLocator& locator) {
    return ParentBaseKey{
        locator.capture_date,
        locator.source_stream_id,
        locator.stream_day_id,
        locator.ingress_sequence,
        locator.record_start_wal_pos,
        locator.record_end_wal_pos};
}

void IncludeParentRange(
    const InjectedRawParentLocator& locator,
    bool* has_parent,
    std::uint64_t* parent_wal_begin,
    std::uint64_t* parent_wal_end) noexcept {
    if (has_parent == nullptr ||
        parent_wal_begin == nullptr ||
        parent_wal_end == nullptr) {
        return;
    }
    if (!*has_parent) {
        *has_parent = true;
        *parent_wal_begin = locator.record_start_wal_pos;
        *parent_wal_end = locator.record_end_wal_pos;
        return;
    }
    *parent_wal_begin = std::min(
        *parent_wal_begin, locator.record_start_wal_pos);
    *parent_wal_end = std::max(
        *parent_wal_end, locator.record_end_wal_pos);
}

[[nodiscard]] InjectedRawV1Error ValidatePlanIdentity(
    const InjectedRawPlanIdentity& identity) noexcept {
    if (identity.plan_magic !=
            kInjectedRawUnframedPlanMagic ||
        !identity.synthetic ||
        common::IsZeroIdentity(identity.run_id) ||
        common::IsZeroIdentity(
            identity.synthetic_namespace_id) ||
        identity.run_id ==
            identity.synthetic_namespace_id ||
        IsZero(identity.raw_schema_sha256) ||
        IsZero(identity.parent_raw_identity_sha256) ||
        IsZero(identity.fault_rule_sha256)) {
        return InjectedRawV1Error::kInvalidIdentity;
    }
    if (identity.injected_schema_identity_sha256 !=
        kInjectedRawV1SchemaSha256) {
        return InjectedRawV1Error::kSchemaIdentityMismatch;
    }
    if (identity.raw_schema_sha256 ==
        identity.injected_schema_identity_sha256) {
        return InjectedRawV1Error::kSchemaIdentityAlias;
    }
    return InjectedRawV1Error::kNone;
}

[[nodiscard]] InjectedRawV1Error ValidatePlanRule(
    const RawLogicalFaultRule& rule) noexcept {
    if (rule.algorithm !=
        RawLogicalSelectionAlgorithm::
            kSplitMix64StatelessV1 ||
        !KnownMutation(rule.mutation)) {
        return InjectedRawV1Error::kInvalidRule;
    }
    if ((rule.duplicate_one_in == 0U) !=
            (rule.duplicate_additional_copies == 0U) ||
        (rule.mutate_one_in == 0U) !=
            (rule.mutation ==
             RawLogicalMutationKind::kNone) ||
        (rule.mutation ==
             RawLogicalMutationKind::kVendorBodyByteXor &&
         rule.mutation_xor_mask == std::byte{0U}) ||
         rule.duplicate_additional_copies ==
            std::numeric_limits<std::uint32_t>::max()) {
        return InjectedRawV1Error::kInvalidRule;
    }
    return InjectedRawV1Error::kNone;
}

[[nodiscard]] bool VendorHeadMatches(
    const RawV1VendorHead& head,
    std::uint32_t vendor_message_size) noexcept {
    const std::span<const std::byte> bytes(head);
    return std::to_integer<std::uint8_t>(
               bytes[kVendorHeadSizeOffset]) ==
            kVendorMessageHeadBytes &&
        LoadU32Le(bytes, kVendorMessageSizeOffset) ==
            vendor_message_size;
}

}  // namespace

std::string_view InjectedRawV1ErrorName(
    InjectedRawV1Error error) noexcept {
    switch (error) {
        case InjectedRawV1Error::kNone:
            return "none";
        case InjectedRawV1Error::kNullOutput:
            return "null_output";
        case InjectedRawV1Error::kNullBuffer:
            return "null_buffer";
        case InjectedRawV1Error::kInvalidWireSize:
            return "invalid_wire_size";
        case InjectedRawV1Error::kInvalidMagic:
            return "invalid_magic";
        case InjectedRawV1Error::kUnsupportedVersion:
            return "unsupported_version";
        case InjectedRawV1Error::kInvalidEndian:
            return "invalid_endian";
        case InjectedRawV1Error::kInvalidHeaderSize:
            return "invalid_header_size";
        case InjectedRawV1Error::kUnknownFlags:
            return "unknown_flags";
        case InjectedRawV1Error::kNonzeroReserved:
            return "nonzero_reserved";
        case InjectedRawV1Error::kInvalidIdentity:
            return "invalid_identity";
        case InjectedRawV1Error::kSchemaIdentityMismatch:
            return "schema_identity_mismatch";
        case InjectedRawV1Error::kSchemaIdentityAlias:
            return "schema_identity_alias";
        case InjectedRawV1Error::kInvalidCount:
            return "invalid_count";
        case InjectedRawV1Error::kInvalidSize:
            return "invalid_size";
        case InjectedRawV1Error::kSizeOverflow:
            return "size_overflow";
        case InjectedRawV1Error::kInvalidSequence:
            return "invalid_sequence";
        case InjectedRawV1Error::kInvalidCaptureMeta:
            return "invalid_capture_meta";
        case InjectedRawV1Error::kInvalidParentLocator:
            return "invalid_parent_locator";
        case InjectedRawV1Error::kDuplicateParentLocator:
            return "duplicate_parent_locator";
        case InjectedRawV1Error::kInvalidParentOccurrence:
            return "invalid_parent_occurrence";
        case InjectedRawV1Error::kInvalidProvenance:
            return "invalid_provenance";
        case InjectedRawV1Error::kInvalidRule:
            return "invalid_rule";
        case InjectedRawV1Error::kInvalidMutation:
            return "invalid_mutation";
        case InjectedRawV1Error::kInvalidVendorHead:
            return "invalid_vendor_head";
        case InjectedRawV1Error::kHeaderCrcMismatch:
            return "header_crc_mismatch";
        case InjectedRawV1Error::kParentLocatorCrcMismatch:
            return "parent_locator_crc_mismatch";
        case InjectedRawV1Error::kPayloadCrcMismatch:
            return "payload_crc_mismatch";
        case InjectedRawV1Error::kNonzeroPadding:
            return "nonzero_padding";
        case InjectedRawV1Error::kTrailerMismatch:
            return "trailer_mismatch";
        case InjectedRawV1Error::kDroppedTableMismatch:
            return "dropped_table_mismatch";
        case InjectedRawV1Error::kResourceLimitExceeded:
            return "resource_limit_exceeded";
        case InjectedRawV1Error::kAllocationFailure:
            return "allocation_failure";
    }
    return "unknown";
}

InjectedRawV1Error ComputeInjectedRawRecordLayoutV1(
    std::size_t vendor_body_size,
    InjectedRawRecordLayoutV1* layout) noexcept {
    if (layout == nullptr) {
        return InjectedRawV1Error::kNullOutput;
    }
    if (vendor_body_size >
        kInjectedRawV1MaximumVendorBodyBytes) {
        return InjectedRawV1Error::kResourceLimitExceeded;
    }
    const std::size_t vendor_message_size =
        kVendorMessageHeadBytes + vendor_body_size;
    const std::size_t payload_end =
        kInjectedRawV1RecordHeaderBytes +
        kInjectedRawV1ParentLocatorBytes +
        vendor_message_size;
    const std::size_t padding_size =
        (kInjectedRawV1RecordAlignment -
         (payload_end % kInjectedRawV1RecordAlignment)) %
        kInjectedRawV1RecordAlignment;
    const std::size_t record_size =
        payload_end + padding_size +
        kInjectedRawV1RecordTrailerBytes;
    if (record_size >
            kInjectedRawV1MaximumRecordBytes ||
        record_size >
            std::numeric_limits<std::uint32_t>::max()) {
        return InjectedRawV1Error::kSizeOverflow;
    }
    *layout = InjectedRawRecordLayoutV1{
        static_cast<std::uint32_t>(vendor_message_size),
        static_cast<std::uint32_t>(padding_size),
        static_cast<std::uint32_t>(record_size)};
    return InjectedRawV1Error::kNone;
}

InjectedRawV1Error EncodeInjectedRawSegmentHeaderV1(
    const InjectedRawSegmentHeaderV1& header,
    InjectedRawV1SegmentHeaderWire* wire) noexcept {
    if (wire == nullptr) {
        return InjectedRawV1Error::kNullOutput;
    }
    const InjectedRawV1Error validation =
        ValidateSegmentHeader(header);
    if (validation != InjectedRawV1Error::kNone) {
        return validation;
    }

    InjectedRawV1SegmentHeaderWire encoded{};
    std::span<std::byte> bytes(encoded);
    using namespace injected_raw_v1_offset::segment;
    StoreBytes(kInjectedRawV1SegmentMagic, bytes, kMagic);
    StoreU16Le(
        kInjectedRawV1FormatVersion, bytes, kFormatVersion);
    bytes[kEndian] =
        static_cast<std::byte>(kInjectedRawV1LittleEndian);
    StoreU32Le(
        static_cast<std::uint32_t>(
            kInjectedRawV1SegmentHeaderBytes),
        bytes,
        kHeaderSize);
    StoreU32Le(header.flags, bytes, kFlags);
    StoreBytes(header.run_id, bytes, kRunId);
    StoreBytes(
        header.synthetic_namespace_id,
        bytes,
        kSyntheticNamespaceId);
    StoreBytes(
        header.raw_schema_sha256, bytes, kRawSchemaSha256);
    StoreBytes(
        header.injected_schema_sha256,
        bytes,
        kInjectedSchemaSha256);
    StoreBytes(
        header.parent_raw_identity_sha256,
        bytes,
        kParentRawIdentitySha256);
    StoreBytes(
        header.fault_rule_sha256,
        bytes,
        kFaultRuleSha256);
    StoreU64Le(header.fault_seed, bytes, kFaultSeed);
    StoreU64Le(header.record_count, bytes, kRecordCount);
    StoreU64Le(
        header.dropped_locator_count,
        bytes,
        kDroppedLocatorCount);
    StoreU64Le(
        header.records_offset, bytes, kRecordsOffset);
    StoreU64Le(
        header.dropped_locators_offset,
        bytes,
        kDroppedLocatorsOffset);
    StoreU64Le(header.logical_size, bytes, kLogicalSize);
    StoreU64Le(
        header.parent_wal_begin, bytes, kParentWalBegin);
    StoreU64Le(
        header.parent_wal_end, bytes, kParentWalEnd);
    StoreU32Le(
        l2flow::common::ComputeCrc32c(encoded),
        bytes,
        kHeaderCrc32c);
    *wire = encoded;
    return InjectedRawV1Error::kNone;
}

InjectedRawV1Error DecodeInjectedRawSegmentHeaderV1(
    std::span<const std::byte> wire,
    InjectedRawSegmentHeaderV1* header) noexcept {
    if (header == nullptr) {
        return InjectedRawV1Error::kNullOutput;
    }
    if (wire.size() != kInjectedRawV1SegmentHeaderBytes) {
        return InjectedRawV1Error::kInvalidWireSize;
    }
    using namespace injected_raw_v1_offset::segment;
    if (!std::equal(
            kInjectedRawV1SegmentMagic.begin(),
            kInjectedRawV1SegmentMagic.end(),
            wire.begin() + kMagic)) {
        return InjectedRawV1Error::kInvalidMagic;
    }
    if (LoadU16Le(wire, kFormatVersion) !=
        kInjectedRawV1FormatVersion) {
        return InjectedRawV1Error::kUnsupportedVersion;
    }
    if (std::to_integer<std::uint8_t>(wire[kEndian]) !=
        kInjectedRawV1LittleEndian) {
        return InjectedRawV1Error::kInvalidEndian;
    }
    if (LoadU32Le(wire, kHeaderSize) !=
        kInjectedRawV1SegmentHeaderBytes) {
        return InjectedRawV1Error::kInvalidHeaderSize;
    }
    if (wire[kReserved0] != std::byte{0U} ||
        !IsZero(wire.subspan(kReserved1, 4U)) ||
        !IsZero(wire.subspan(
            kReservedTail,
            kInjectedRawV1SegmentHeaderBytes -
                kReservedTail))) {
        return InjectedRawV1Error::kNonzeroReserved;
    }
    const std::uint32_t stored_crc =
        LoadU32Le(wire, kHeaderCrc32c);
    if (ComputeZeroedFieldCrc(wire, kHeaderCrc32c) !=
        stored_crc) {
        return InjectedRawV1Error::kHeaderCrcMismatch;
    }

    InjectedRawSegmentHeaderV1 decoded;
    decoded.flags = LoadU32Le(wire, kFlags);
    decoded.run_id =
        LoadBytes<RawV1Identity{}.size()>(wire, kRunId);
    decoded.synthetic_namespace_id =
        LoadBytes<RawV1Identity{}.size()>(
            wire, kSyntheticNamespaceId);
    decoded.raw_schema_sha256 =
        LoadBytes<RawV1Digest{}.size()>(
            wire, kRawSchemaSha256);
    decoded.injected_schema_sha256 =
        LoadBytes<RawV1Digest{}.size()>(
            wire, kInjectedSchemaSha256);
    decoded.parent_raw_identity_sha256 =
        LoadBytes<RawV1Digest{}.size()>(
            wire, kParentRawIdentitySha256);
    decoded.fault_rule_sha256 =
        LoadBytes<RawV1Digest{}.size()>(
            wire, kFaultRuleSha256);
    decoded.fault_seed = LoadU64Le(wire, kFaultSeed);
    decoded.record_count = LoadU64Le(wire, kRecordCount);
    decoded.dropped_locator_count =
        LoadU64Le(wire, kDroppedLocatorCount);
    decoded.records_offset =
        LoadU64Le(wire, kRecordsOffset);
    decoded.dropped_locators_offset =
        LoadU64Le(wire, kDroppedLocatorsOffset);
    decoded.logical_size = LoadU64Le(wire, kLogicalSize);
    decoded.parent_wal_begin =
        LoadU64Le(wire, kParentWalBegin);
    decoded.parent_wal_end =
        LoadU64Le(wire, kParentWalEnd);
    decoded.header_crc32c = stored_crc;
    const InjectedRawV1Error validation =
        ValidateSegmentHeader(decoded);
    if (validation != InjectedRawV1Error::kNone) {
        return validation;
    }
    *header = decoded;
    return InjectedRawV1Error::kNone;
}

InjectedRawV1Error EncodeInjectedRawRecordHeaderV1(
    const InjectedRawRecordHeaderV1& header,
    InjectedRawV1RecordHeaderWire* wire) noexcept {
    if (wire == nullptr) {
        return InjectedRawV1Error::kNullOutput;
    }
    const InjectedRawV1Error validation =
        ValidateRecordHeader(header);
    if (validation != InjectedRawV1Error::kNone) {
        return validation;
    }

    InjectedRawV1RecordHeaderWire encoded{};
    std::span<std::byte> bytes(encoded);
    using namespace injected_raw_v1_offset::record_header;
    StoreU32Le(kInjectedRawV1RecordMagic, bytes, kMagic);
    StoreU16Le(
        kInjectedRawV1FormatVersion, bytes, kFormatVersion);
    bytes[kEndian] =
        static_cast<std::byte>(kInjectedRawV1LittleEndian);
    StoreU16Le(
        static_cast<std::uint16_t>(
            kInjectedRawV1RecordHeaderBytes),
        bytes,
        kHeaderSize);
    StoreU16Le(
        static_cast<std::uint16_t>(
            kInjectedRawV1ParentLocatorBytes),
        bytes,
        kParentLocatorSize);
    StoreU32Le(header.record_size, bytes, kRecordSize);
    StoreU32Le(header.flags, bytes, kFlags);
    StoreU32Le(
        header.vendor_body_size, bytes, kVendorBodySize);
    StoreU64Le(
        header.synthetic_ingress_sequence,
        bytes,
        kSyntheticIngressSequence);
    StoreU32Le(
        header.source_stream_id, bytes, kSourceStreamId);
    StoreU32Le(header.capture_date, bytes, kCaptureDate);
    StoreU32Le(
        header.connection_epoch_hint,
        bytes,
        kConnectionEpochHint);
    StoreU32Le(
        header.vendor_message_size,
        bytes,
        kVendorMessageSize);
    StoreU64Le(
        header.recv_realtime_ns, bytes, kRecvRealtimeNs);
    StoreU64Le(
        header.recv_monotonic_ns,
        bytes,
        kRecvMonotonicNs);
    StoreU64Le(
        header.mutation_vendor_body_offset,
        bytes,
        kMutationVendorBodyOffset);
    bytes[kMutationKind] =
        static_cast<std::byte>(header.mutation_kind);
    bytes[kMutationBefore] = header.mutation_before;
    bytes[kMutationAfter] = header.mutation_after;
    bytes[kParentProvenance] =
        static_cast<std::byte>(header.parent_provenance);
    bytes[kVendorHeadSize] =
        static_cast<std::byte>(header.vendor_head_size);
    StoreU32Le(
        header.payload_crc32c, bytes, kPayloadCrc32c);
    StoreU32Le(
        l2flow::common::ComputeCrc32c(encoded),
        bytes,
        kHeaderCrc32c);
    *wire = encoded;
    return InjectedRawV1Error::kNone;
}

InjectedRawV1Error DecodeInjectedRawRecordHeaderV1(
    std::span<const std::byte> wire,
    InjectedRawRecordHeaderV1* header) noexcept {
    if (header == nullptr) {
        return InjectedRawV1Error::kNullOutput;
    }
    if (wire.size() != kInjectedRawV1RecordHeaderBytes) {
        return InjectedRawV1Error::kInvalidWireSize;
    }
    using namespace injected_raw_v1_offset::record_header;
    if (LoadU32Le(wire, kMagic) !=
        kInjectedRawV1RecordMagic) {
        return InjectedRawV1Error::kInvalidMagic;
    }
    if (LoadU16Le(wire, kFormatVersion) !=
        kInjectedRawV1FormatVersion) {
        return InjectedRawV1Error::kUnsupportedVersion;
    }
    if (std::to_integer<std::uint8_t>(wire[kEndian]) !=
        kInjectedRawV1LittleEndian) {
        return InjectedRawV1Error::kInvalidEndian;
    }
    if (LoadU16Le(wire, kHeaderSize) !=
            kInjectedRawV1RecordHeaderBytes ||
        LoadU16Le(wire, kParentLocatorSize) !=
            kInjectedRawV1ParentLocatorBytes) {
        return InjectedRawV1Error::kInvalidHeaderSize;
    }
    if (wire[kReserved0] != std::byte{0U} ||
        !IsZero(wire.subspan(kReserved1, 3U)) ||
        !IsZero(wire.subspan(
            kReservedTail,
            kInjectedRawV1RecordHeaderBytes -
                kReservedTail))) {
        return InjectedRawV1Error::kNonzeroReserved;
    }
    const std::uint32_t stored_crc =
        LoadU32Le(wire, kHeaderCrc32c);
    if (ComputeZeroedFieldCrc(wire, kHeaderCrc32c) !=
        stored_crc) {
        return InjectedRawV1Error::kHeaderCrcMismatch;
    }

    InjectedRawRecordHeaderV1 decoded;
    decoded.record_size = LoadU32Le(wire, kRecordSize);
    decoded.flags = LoadU32Le(wire, kFlags);
    decoded.vendor_body_size =
        LoadU32Le(wire, kVendorBodySize);
    decoded.synthetic_ingress_sequence =
        LoadU64Le(wire, kSyntheticIngressSequence);
    decoded.source_stream_id =
        LoadU32Le(wire, kSourceStreamId);
    decoded.capture_date = LoadU32Le(wire, kCaptureDate);
    decoded.connection_epoch_hint =
        LoadU32Le(wire, kConnectionEpochHint);
    decoded.vendor_message_size =
        LoadU32Le(wire, kVendorMessageSize);
    decoded.recv_realtime_ns =
        LoadU64Le(wire, kRecvRealtimeNs);
    decoded.recv_monotonic_ns =
        LoadU64Le(wire, kRecvMonotonicNs);
    decoded.mutation_vendor_body_offset =
        LoadU64Le(wire, kMutationVendorBodyOffset);
    decoded.mutation_kind =
        static_cast<RawLogicalMutationKind>(
            std::to_integer<std::uint8_t>(
                wire[kMutationKind]));
    decoded.mutation_before = wire[kMutationBefore];
    decoded.mutation_after = wire[kMutationAfter];
    decoded.parent_provenance =
        static_cast<RawReplayProvenance>(
            std::to_integer<std::uint8_t>(
                wire[kParentProvenance]));
    decoded.vendor_head_size =
        std::to_integer<std::uint8_t>(
            wire[kVendorHeadSize]);
    decoded.payload_crc32c =
        LoadU32Le(wire, kPayloadCrc32c);
    decoded.header_crc32c = stored_crc;
    const InjectedRawV1Error validation =
        ValidateRecordHeader(decoded);
    if (validation != InjectedRawV1Error::kNone) {
        return validation;
    }
    *header = decoded;
    return InjectedRawV1Error::kNone;
}

InjectedRawV1Error EncodeInjectedRawParentLocatorV1(
    const InjectedRawParentLocator& locator,
    InjectedRawV1ParentLocatorWire* wire) noexcept {
    if (wire == nullptr) {
        return InjectedRawV1Error::kNullOutput;
    }
    const InjectedRawV1Error validation =
        ValidateParentLocator(locator);
    if (validation != InjectedRawV1Error::kNone) {
        return validation;
    }

    InjectedRawV1ParentLocatorWire encoded{};
    std::span<std::byte> bytes(encoded);
    using namespace injected_raw_v1_offset::parent_locator;
    StoreU32Le(locator.capture_date, bytes, kCaptureDate);
    StoreU32Le(
        locator.source_stream_id, bytes, kSourceStreamId);
    StoreBytes(locator.stream_day_id, bytes, kStreamDayId);
    StoreU64Le(
        locator.ingress_sequence, bytes, kIngressSequence);
    StoreU64Le(
        locator.record_start_wal_pos,
        bytes,
        kRecordStartWalPos);
    StoreU64Le(
        locator.record_end_wal_pos,
        bytes,
        kRecordEndWalPos);
    StoreU32Le(locator.occurrence, bytes, kOccurrence);
    StoreU32Le(
        l2flow::common::ComputeCrc32c(encoded),
        bytes,
        kLocatorCrc32c);
    *wire = encoded;
    return InjectedRawV1Error::kNone;
}

InjectedRawV1Error DecodeInjectedRawParentLocatorV1(
    std::span<const std::byte> wire,
    InjectedRawParentLocatorV1* locator) noexcept {
    if (locator == nullptr) {
        return InjectedRawV1Error::kNullOutput;
    }
    if (wire.size() != kInjectedRawV1ParentLocatorBytes) {
        return InjectedRawV1Error::kInvalidWireSize;
    }
    using namespace injected_raw_v1_offset::parent_locator;
    if (!IsZero(wire.subspan(kReserved, 8U))) {
        return InjectedRawV1Error::kNonzeroReserved;
    }
    const std::uint32_t stored_crc =
        LoadU32Le(wire, kLocatorCrc32c);
    if (ComputeZeroedFieldCrc(wire, kLocatorCrc32c) !=
        stored_crc) {
        return InjectedRawV1Error::
            kParentLocatorCrcMismatch;
    }

    InjectedRawParentLocatorV1 decoded;
    decoded.locator.capture_date =
        LoadU32Le(wire, kCaptureDate);
    decoded.locator.source_stream_id =
        LoadU32Le(wire, kSourceStreamId);
    decoded.locator.stream_day_id =
        LoadBytes<RawV1Identity{}.size()>(
            wire, kStreamDayId);
    decoded.locator.ingress_sequence =
        LoadU64Le(wire, kIngressSequence);
    decoded.locator.record_start_wal_pos =
        LoadU64Le(wire, kRecordStartWalPos);
    decoded.locator.record_end_wal_pos =
        LoadU64Le(wire, kRecordEndWalPos);
    decoded.locator.occurrence =
        LoadU32Le(wire, kOccurrence);
    decoded.locator_crc32c = stored_crc;
    const InjectedRawV1Error validation =
        ValidateParentLocator(decoded.locator);
    if (validation != InjectedRawV1Error::kNone) {
        return validation;
    }
    *locator = decoded;
    return InjectedRawV1Error::kNone;
}

InjectedRawV1Error EncodeInjectedRawRecordTrailerV1(
    const InjectedRawRecordTrailerV1& trailer,
    InjectedRawV1RecordTrailerWire* wire) noexcept {
    if (wire == nullptr) {
        return InjectedRawV1Error::kNullOutput;
    }
    const InjectedRawV1Error validation =
        ValidateTrailer(trailer);
    if (validation != InjectedRawV1Error::kNone) {
        return validation;
    }
    InjectedRawV1RecordTrailerWire encoded{};
    std::span<std::byte> bytes(encoded);
    using namespace injected_raw_v1_offset::record_trailer;
    StoreU32Le(kInjectedRawV1CommitMagic, bytes, kCommitMagic);
    StoreU32Le(trailer.record_size, bytes, kRecordSize);
    StoreU64Le(
        trailer.synthetic_ingress_sequence,
        bytes,
        kSyntheticIngressSequence);
    StoreU32Le(
        l2flow::common::ComputeCrc32c(encoded),
        bytes,
        kTrailerCrc32c);
    *wire = encoded;
    return InjectedRawV1Error::kNone;
}

InjectedRawV1Error DecodeInjectedRawRecordTrailerV1(
    std::span<const std::byte> wire,
    InjectedRawRecordTrailerV1* trailer) noexcept {
    if (trailer == nullptr) {
        return InjectedRawV1Error::kNullOutput;
    }
    if (wire.size() != kInjectedRawV1RecordTrailerBytes) {
        return InjectedRawV1Error::kInvalidWireSize;
    }
    using namespace injected_raw_v1_offset::record_trailer;
    if (LoadU32Le(wire, kCommitMagic) !=
        kInjectedRawV1CommitMagic) {
        return InjectedRawV1Error::kInvalidMagic;
    }
    if (!IsZero(wire.subspan(kReserved, 4U))) {
        return InjectedRawV1Error::kNonzeroReserved;
    }
    const std::uint32_t stored_crc =
        LoadU32Le(wire, kTrailerCrc32c);
    if (ComputeZeroedFieldCrc(wire, kTrailerCrc32c) !=
        stored_crc) {
        return InjectedRawV1Error::kTrailerMismatch;
    }
    InjectedRawRecordTrailerV1 decoded;
    decoded.record_size = LoadU32Le(wire, kRecordSize);
    decoded.synthetic_ingress_sequence =
        LoadU64Le(wire, kSyntheticIngressSequence);
    decoded.trailer_crc32c = stored_crc;
    const InjectedRawV1Error validation =
        ValidateTrailer(decoded);
    if (validation != InjectedRawV1Error::kNone) {
        return validation;
    }
    *trailer = decoded;
    return InjectedRawV1Error::kNone;
}

InjectedRawV1Error ProduceInjectedRawSegmentV1(
    const InjectedRawTransformPlan& plan,
    std::shared_ptr<const std::vector<std::byte>>* output)
    noexcept {
    if (output == nullptr) {
        return InjectedRawV1Error::kNullOutput;
    }
    output->reset();
    const InjectedRawV1Error identity_error =
        ValidatePlanIdentity(plan.identity);
    if (identity_error != InjectedRawV1Error::kNone) {
        return identity_error;
    }
    const InjectedRawV1Error rule_error =
        ValidatePlanRule(plan.rule);
    if (rule_error != InjectedRawV1Error::kNone) {
        return rule_error;
    }

    std::uint64_t record_count = 0U;
    std::uint64_t dropped_count = 0U;
    if (!SizeToU64(plan.records.size(), &record_count) ||
        !SizeToU64(
            plan.dropped_locators.size(), &dropped_count)) {
        return InjectedRawV1Error::kSizeOverflow;
    }
    if (record_count > kInjectedRawV1MaximumRecordCount ||
        dropped_count >
            kInjectedRawV1MaximumDroppedLocatorCount) {
        return InjectedRawV1Error::kResourceLimitExceeded;
    }

    try {
        std::vector<InjectedRawRecordLayoutV1> layouts;
        layouts.reserve(plan.records.size());
        std::map<ParentBaseKey, std::set<std::uint32_t>>
            occurrences;
        std::set<ParentBaseKey> dropped_keys;
        std::uint64_t records_bytes = 0U;
        bool has_parent = false;
        std::uint64_t parent_wal_begin = 0U;
        std::uint64_t parent_wal_end = 0U;

        for (std::size_t index = 0U;
             index < plan.records.size();
             ++index) {
            const InjectedRawPlanRecord& record =
                plan.records[index];
            std::uint64_t expected_sequence = 0U;
            if (!SizeToU64(index, &expected_sequence) ||
                !AddU64(
                    expected_sequence,
                    1U,
                    &expected_sequence)) {
                return InjectedRawV1Error::kSizeOverflow;
            }
            if (record.synthetic_ingress_sequence !=
                    expected_sequence ||
                record.capture_meta.ingress_sequence !=
                    expected_sequence) {
                return InjectedRawV1Error::kInvalidSequence;
            }
            if (record.capture_meta.flags != 0U ||
                record.capture_meta.source_stream_id == 0U ||
                record.capture_meta.capture_date == 0U ||
                record.capture_meta.source_stream_id !=
                    record.parent.source_stream_id ||
                record.capture_meta.capture_date !=
                    record.parent.capture_date) {
                return InjectedRawV1Error::
                    kInvalidCaptureMeta;
            }
            if (!KnownProvenance(
                    record.parent_provenance)) {
                return InjectedRawV1Error::
                    kInvalidProvenance;
            }
            const InjectedRawV1Error parent_error =
                ValidateParentLocator(record.parent);
            if (parent_error != InjectedRawV1Error::kNone) {
                return parent_error;
            }

            InjectedRawRecordLayoutV1 layout{};
            const InjectedRawV1Error layout_error =
                ComputeInjectedRawRecordLayoutV1(
                    record.vendor_body.size(), &layout);
            if (layout_error != InjectedRawV1Error::kNone) {
                return layout_error;
            }
            if (!VendorHeadMatches(
                    record.vendor_head,
                    layout.vendor_message_size)) {
                return InjectedRawV1Error::
                    kInvalidVendorHead;
            }
            if (record.mutation.has_value()) {
                const InjectedRawMutationEvidence& mutation =
                    *record.mutation;
                std::size_t mutation_offset = 0U;
                if (mutation.kind !=
                        RawLogicalMutationKind::
                            kVendorBodyByteXor ||
                    mutation.kind != plan.rule.mutation ||
                    !U64ToSize(
                        mutation.vendor_body_offset,
                        &mutation_offset) ||
                    mutation_offset >=
                        record.vendor_body.size() ||
                    mutation.before == mutation.after ||
                    record.vendor_body[mutation_offset] !=
                        mutation.after ||
                    (mutation.before ^
                     plan.rule.mutation_xor_mask) !=
                        mutation.after) {
                    return InjectedRawV1Error::
                        kInvalidMutation;
                }
            }

            auto& parent_occurrences =
                occurrences[MakeParentBaseKey(
                    record.parent)];
            if (!parent_occurrences.insert(
                    record.parent.occurrence)
                    .second) {
                return InjectedRawV1Error::
                    kDuplicateParentLocator;
            }
            IncludeParentRange(
                record.parent,
                &has_parent,
                &parent_wal_begin,
                &parent_wal_end);
            if (!AddU64(
                    records_bytes,
                    layout.record_size,
                    &records_bytes)) {
                return InjectedRawV1Error::kSizeOverflow;
            }
            layouts.push_back(layout);
        }

        for (const InjectedRawParentLocator& locator :
             plan.dropped_locators) {
            const InjectedRawV1Error parent_error =
                ValidateParentLocator(locator);
            if (parent_error != InjectedRawV1Error::kNone) {
                return parent_error;
            }
            const ParentBaseKey key =
                MakeParentBaseKey(locator);
            if (locator.occurrence != 1U ||
                occurrences.contains(key)) {
                return InjectedRawV1Error::
                    kInvalidParentOccurrence;
            }
            if (!dropped_keys.insert(key).second) {
                return InjectedRawV1Error::
                    kDuplicateParentLocator;
            }
            IncludeParentRange(
                locator,
                &has_parent,
                &parent_wal_begin,
                &parent_wal_end);
        }
        for (const auto& [key, values] : occurrences) {
            static_cast<void>(key);
            std::uint32_t expected = 1U;
            for (std::uint32_t occurrence : values) {
                if (occurrence != expected) {
                    return InjectedRawV1Error::
                        kInvalidParentOccurrence;
                }
                if (expected ==
                    std::numeric_limits<std::uint32_t>::max()) {
                    if (std::next(values.find(occurrence)) !=
                        values.end()) {
                        return InjectedRawV1Error::
                            kInvalidParentOccurrence;
                    }
                } else {
                    ++expected;
                }
            }
        }

        std::uint64_t dropped_offset = 0U;
        std::uint64_t dropped_bytes = 0U;
        std::uint64_t logical_size = 0U;
        if (!AddU64(
                kInjectedRawV1SegmentHeaderBytes,
                records_bytes,
                &dropped_offset) ||
            !MulU64(
                dropped_count,
                kInjectedRawV1ParentLocatorBytes,
                &dropped_bytes) ||
            !AddU64(
                dropped_offset,
                dropped_bytes,
                &logical_size)) {
            return InjectedRawV1Error::kSizeOverflow;
        }
        if (logical_size >
            kInjectedRawV1MaximumLogicalBytes) {
            return InjectedRawV1Error::
                kResourceLimitExceeded;
        }
        std::size_t output_size = 0U;
        if (!U64ToSize(logical_size, &output_size)) {
            return InjectedRawV1Error::kSizeOverflow;
        }

        InjectedRawSegmentHeaderV1 segment;
        segment.run_id = plan.identity.run_id;
        segment.synthetic_namespace_id =
            plan.identity.synthetic_namespace_id;
        segment.raw_schema_sha256 =
            plan.identity.raw_schema_sha256;
        segment.injected_schema_sha256 =
            plan.identity.injected_schema_identity_sha256;
        segment.parent_raw_identity_sha256 =
            plan.identity.parent_raw_identity_sha256;
        segment.fault_rule_sha256 =
            plan.identity.fault_rule_sha256;
        segment.fault_seed = plan.rule.seed;
        segment.record_count = record_count;
        segment.dropped_locator_count = dropped_count;
        segment.dropped_locators_offset = dropped_offset;
        segment.logical_size = logical_size;
        segment.parent_wal_begin = parent_wal_begin;
        segment.parent_wal_end = parent_wal_end;

        InjectedRawV1SegmentHeaderWire segment_wire{};
        const InjectedRawV1Error segment_error =
            EncodeInjectedRawSegmentHeaderV1(
                segment, &segment_wire);
        if (segment_error != InjectedRawV1Error::kNone) {
            return segment_error;
        }

        std::vector<std::byte> encoded(
            output_size, std::byte{0U});
        std::copy(
            segment_wire.begin(),
            segment_wire.end(),
            encoded.begin());
        std::size_t cursor =
            kInjectedRawV1SegmentHeaderBytes;
        for (std::size_t index = 0U;
             index < plan.records.size();
             ++index) {
            const InjectedRawPlanRecord& record =
                plan.records[index];
            const InjectedRawRecordLayoutV1& layout =
                layouts[index];
            InjectedRawV1ParentLocatorWire parent_wire{};
            const InjectedRawV1Error parent_error =
                EncodeInjectedRawParentLocatorV1(
                    record.parent, &parent_wire);
            if (parent_error != InjectedRawV1Error::kNone) {
                return parent_error;
            }

            InjectedRawRecordHeaderV1 record_header;
            record_header.record_size = layout.record_size;
            record_header.vendor_body_size =
                static_cast<std::uint32_t>(
                    record.vendor_body.size());
            record_header.synthetic_ingress_sequence =
                record.synthetic_ingress_sequence;
            record_header.source_stream_id =
                record.capture_meta.source_stream_id;
            record_header.capture_date =
                record.capture_meta.capture_date;
            record_header.connection_epoch_hint =
                record.capture_meta.connection_epoch_hint;
            record_header.vendor_message_size =
                layout.vendor_message_size;
            record_header.recv_realtime_ns =
                record.capture_meta.recv_realtime_ns;
            record_header.recv_monotonic_ns =
                record.capture_meta.recv_monotonic_ns;
            record_header.parent_provenance =
                record.parent_provenance;
            if (record.mutation.has_value()) {
                record_header.flags =
                    kInjectedRawV1RecordMutated;
                record_header.mutation_vendor_body_offset =
                    record.mutation->vendor_body_offset;
                record_header.mutation_kind =
                    record.mutation->kind;
                record_header.mutation_before =
                    record.mutation->before;
                record_header.mutation_after =
                    record.mutation->after;
            }
            l2flow::common::Crc32cState payload_crc;
            payload_crc.Update(parent_wire);
            payload_crc.Update(record.vendor_head);
            payload_crc.Update(record.vendor_body);
            record_header.payload_crc32c =
                payload_crc.Finalize();

            InjectedRawV1RecordHeaderWire record_wire{};
            const InjectedRawV1Error record_error =
                EncodeInjectedRawRecordHeaderV1(
                    record_header, &record_wire);
            if (record_error != InjectedRawV1Error::kNone) {
                return record_error;
            }
            const InjectedRawRecordTrailerV1 trailer{
                layout.record_size,
                record.synthetic_ingress_sequence,
                0U};
            InjectedRawV1RecordTrailerWire trailer_wire{};
            const InjectedRawV1Error trailer_error =
                EncodeInjectedRawRecordTrailerV1(
                    trailer, &trailer_wire);
            if (trailer_error !=
                InjectedRawV1Error::kNone) {
                return trailer_error;
            }

            std::copy(
                record_wire.begin(),
                record_wire.end(),
                encoded.begin() + cursor);
            std::size_t write_offset =
                cursor + kInjectedRawV1RecordHeaderBytes;
            std::copy(
                parent_wire.begin(),
                parent_wire.end(),
                encoded.begin() + write_offset);
            write_offset +=
                kInjectedRawV1ParentLocatorBytes;
            std::copy(
                record.vendor_head.begin(),
                record.vendor_head.end(),
                encoded.begin() + write_offset);
            write_offset += kVendorMessageHeadBytes;
            std::copy(
                record.vendor_body.begin(),
                record.vendor_body.end(),
                encoded.begin() + write_offset);
            const std::size_t trailer_offset =
                cursor +
                static_cast<std::size_t>(
                    layout.record_size) -
                kInjectedRawV1RecordTrailerBytes;
            std::copy(
                trailer_wire.begin(),
                trailer_wire.end(),
                encoded.begin() + trailer_offset);
            cursor +=
                static_cast<std::size_t>(layout.record_size);
        }

        if (cursor != static_cast<std::size_t>(
                          dropped_offset)) {
            return InjectedRawV1Error::kInvalidSize;
        }
        for (const InjectedRawParentLocator& locator :
             plan.dropped_locators) {
            InjectedRawV1ParentLocatorWire locator_wire{};
            const InjectedRawV1Error locator_error =
                EncodeInjectedRawParentLocatorV1(
                    locator, &locator_wire);
            if (locator_error !=
                InjectedRawV1Error::kNone) {
                return locator_error;
            }
            std::copy(
                locator_wire.begin(),
                locator_wire.end(),
                encoded.begin() + cursor);
            cursor += kInjectedRawV1ParentLocatorBytes;
        }
        if (cursor != encoded.size()) {
            return InjectedRawV1Error::kInvalidSize;
        }

        *output =
            std::make_shared<const std::vector<std::byte>>(
                std::move(encoded));
    } catch (const std::bad_alloc&) {
        return InjectedRawV1Error::kAllocationFailure;
    } catch (...) {
        return InjectedRawV1Error::kAllocationFailure;
    }
    return InjectedRawV1Error::kNone;
}

}  // namespace l2flow::ingress
