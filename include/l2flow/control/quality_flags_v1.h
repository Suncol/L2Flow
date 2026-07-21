#pragma once

#include "l2flow/control/phase3_schema_v1_generated.h"

#include <bit>
#include <cstdint>

namespace l2flow::control {

[[nodiscard]] constexpr std::uint8_t QualityFlagIndexV1(
    std::uint64_t single_bit_mask) noexcept {
    return static_cast<std::uint8_t>(
        std::countr_zero(single_bit_mask));
}

// Stable bit assignments for the V1 quality bitmap in docs/design.md 10.6.
// LIVE_OK is represented by zero; it is deliberately not an independent bit.
enum class QualityFlagV1 : std::uint8_t {
    kStartUnknown = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kStartUnknown),
    kRecovering = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kRecovering),
    kSessionUnknown = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kSessionUnknown),
    kSubscriptionChanged = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kSubscriptionChanged),
    kVendorSequenceGap = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kVendorSequenceGap),
    kVendorSequenceDuplicate = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kVendorSequenceDuplicate),
    kVendorSequenceConflict = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kVendorSequenceConflict),
    kExchangeSequenceGap = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kExchangeSequenceGap),
    kExchangeSequenceBackward = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kExchangeSequenceBackward),
    kExchangeSequenceConflict = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kExchangeSequenceConflict),
    kCallbackReentry = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kCallbackReentry),
    kRawRecoveredAppendOnly = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kRawRecoveredAppendOnly),
    kRawCorruption = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kRawCorruption),
    kDecodeTruncated = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kDecodeTruncated),
    kDecodeOffsetInvalid = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kDecodeOffsetInvalid),
    kDecodeTextInvalid = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kDecodeTextInvalid),
    kNoncanonicalEmptyOffset = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kNoncanonicalEmptyOffset),
    kNullValuePresent = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kNullValuePresent),
    kUnknownEnum = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kUnknownEnum),
    kSchemaUnknown = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kSchemaUnknown),
    kNonIntegralMatchedQty = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kNonIntegralMatchedQty),
    kAmbiguousOrderReference = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kAmbiguousOrderReference),
    kSnapshotStale = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kSnapshotStale),
    kSnapshotDepthShort = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kSnapshotDepthShort),
    kQueueTruncatedTo50 = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kQueueTruncatedTo50),
    kTickReconInvalid = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kTickReconInvalid),
    kUnauthorized = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kUnauthorized),
    kConnectionSwitched = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kConnectionSwitched),
    kClockUnsynced = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kClockUnsynced),
    kClockEpochChanged = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kClockEpochChanged),
    kSourceDisconnected = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kSourceDisconnected),
    kInstrumentUnknown = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kInstrumentUnknown),
    kQtyUnitUnknown = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kQtyUnitUnknown),
    kDurabilityLag = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kDurabilityLag),
    kMarketClosed = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kMarketClosed),
    kNondeterministicLiveLatest = QualityFlagIndexV1(
        phase3_schema_v1::quality_flag_v1_mask::kNondeterministicLiveLatest),
};

[[nodiscard]] constexpr std::uint64_t QualityBit(
    QualityFlagV1 flag) noexcept {
    return std::uint64_t{1U}
           << static_cast<std::uint8_t>(flag);
}

inline constexpr std::uint64_t kLiveOkQualityV1 = 0U;

}  // namespace l2flow::control
