#pragma once

#include <cstdint>

namespace l2flow::control {

// Decoder-domain subset of the stable V1 bitmap. Explicit indices preserve
// the established decoder-facing bit meanings.
enum class QualityFlagV1 : std::uint8_t {
    kDecodeTruncated = 13U,
    kDecodeOffsetInvalid = 14U,
    kDecodeTextInvalid = 15U,
    kNoncanonicalEmptyOffset = 16U,
    kNullValuePresent = 17U,
    kUnknownEnum = 18U,
    kSchemaUnknown = 19U,
    kNonIntegralMatchedQty = 20U,
    kAmbiguousOrderReference = 21U,
    kSnapshotDepthShort = 23U,
    kQueueTruncatedTo50 = 24U,
    kInstrumentUnknown = 31U,
    kQtyUnitUnknown = 32U,
};

[[nodiscard]] constexpr std::uint64_t QualityBit(
    QualityFlagV1 flag) noexcept {
    return std::uint64_t{1U}
           << static_cast<std::uint8_t>(flag);
}

inline constexpr std::uint64_t kLiveOkQualityV1 = 0U;

}  // namespace l2flow::control
