#pragma once

#include "l2flow/common/sha256.h"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <type_traits>

namespace l2flow::canonical {

inline constexpr std::uint32_t kCanonicalMagicV1 = 0x3145434dU;
inline constexpr std::uint16_t kCanonicalSchemaVersionV1 = 1U;
inline constexpr std::size_t kCanonicalHeaderBytesV1 = 112U;
inline constexpr std::size_t kCanonicalTickPayloadBytesV1 = 80U;
inline constexpr std::size_t kCanonicalTickRecordBytesV1 = 192U;
inline constexpr std::size_t kCanonicalSnapshotPayloadBytesV1 = 1936U;
inline constexpr std::size_t kCanonicalSnapshotRecordBytesV1 = 2048U;
inline constexpr std::size_t kCanonicalQualityPayloadBytesV1 = 80U;
inline constexpr std::size_t kCanonicalQualityRecordBytesV1 = 192U;
inline constexpr std::size_t kCanonicalControlPayloadBytesV1 = 144U;
inline constexpr std::size_t kCanonicalControlRecordBytesV1 = 256U;
inline constexpr std::size_t kCanonicalDepthLevelsV1 = 10U;
inline constexpr std::size_t kCanonicalQueueEntriesV1 = 50U;

// This is a direct-mmap, naturally aligned schema. Big-endian hosts must fail
// the attach gate instead of reinterpreting these objects as little-endian.
[[nodiscard]] constexpr bool CanonicalHostIsLittleEndianV1() noexcept {
    return std::endian::native == std::endian::little;
}

// The compact label is only an index/display aid. Identity equality is the
// algorithm plus the full digest; no code in this schema derives or validates
// the caller-provided label from digest bytes.
struct ClockEpochIdentityV1 final {
    std::uint32_t algorithm = 0U;
    l2flow::common::Sha256Digest digest{};
    std::uint64_t label = 0U;
};

[[nodiscard]] constexpr bool ClockEpochIdentityV1Valid(
    const ClockEpochIdentityV1& value) noexcept {
    if (value.algorithm == 0U) {
        return false;
    }
    for (const std::byte byte : value.digest) {
        if (byte != std::byte{0U}) {
            return true;
        }
    }
    return false;
}

[[nodiscard]] constexpr bool operator==(
    const ClockEpochIdentityV1& left,
    const ClockEpochIdentityV1& right) noexcept {
    return left.algorithm == right.algorithm &&
           left.digest == right.digest;
}

enum class CanonicalEventTypeV1 : std::uint16_t {
    kUnknown = 0U,
    kSnapshot = 1U,
    kTick = 2U,
    kQuality = 3U,
    kControl = 4U,
};

enum class CanonicalMarketV1 : std::uint16_t {
    kUnknown = 0U,
    kShanghai = 1U,
    kShenzhen = 2U,
};

enum class CanonicalTickActionV1 : std::uint8_t {
    kUnknown = 0U,
    kAdd = 1U,
    kCancel = 2U,
    kTrade = 3U,
    kStatus = 4U,
};

enum class CanonicalSideV1 : std::uint8_t {
    kUnknown = 0U,
    kBuy = 1U,
    kSell = 2U,
    kBorrow = 3U,
    kLend = 4U,
};

enum class CanonicalOrderTypeV1 : std::uint8_t {
    kUnknown = 0U,
    kMarket = 1U,
    kLimit = 2U,
    kSameSideBest = 3U,
};

enum class CanonicalAggressorV1 : std::uint8_t {
    kUnknown = 0U,
    kBuy = 1U,
    kSell = 2U,
    kNeutral = 3U,
};

enum class CanonicalQuantityUnitV1 : std::uint8_t {
    kUnknown = 0U,
    kShare = 1U,
    kFundUnit = 2U,
    kLot = 3U,
    kBondPiece = 4U,
    kIndexUnit = 5U,
};

enum class CanonicalTradingPhaseV1 : std::uint8_t {
    kUnknown = 0U,
    kStart = 1U,
    kOpeningCall = 2U,
    kContinuous = 3U,
    kSuspended = 4U,
    kClosingCall = 5U,
    kClosed = 6U,
    kEnd = 7U,
};

enum class CanonicalLimitSemanticsV1 : std::uint8_t {
    kUnknown = 0U,
    kFinite = 1U,
    kNoLimit = 2U,
};

enum class CanonicalQualityTypeV1 : std::uint16_t {
    kUnknown = 0U,
    kVendorSequenceGap = 1U,
    kVendorSequenceDuplicate = 2U,
    kVendorSequenceConflict = 3U,
    kExchangeSequenceGap = 4U,
    kExchangeSequenceBackward = 5U,
    kExchangeSequenceConflict = 6U,
    kDecodeError = 7U,
    kSchemaUnknown = 8U,
    kScopePoisoned = 9U,
    kSourceState = 10U,
    kClockEpochChanged = 11U,
    kSnapshotRejected = 12U,
    kInstrumentUnknown = 13U,
    kSequenceCapacityExhausted = 14U,
    kNormalizationRejected = 15U,
    kExchangeSequenceDuplicate = 16U,
};

enum class CanonicalQualityScopeV1 : std::uint16_t {
    kUnknown = 0U,
    kStream = 1U,
    kVendorMessage = 2U,
    kChannel = 3U,
    kInstrument = 4U,
    kSnapshotFamily = 5U,
    kTickFamily = 6U,
};

// Frozen scope_id encodings.  scope_type remains part of the identity: the
// numeric spaces deliberately need not be globally disjoint.
[[nodiscard]] constexpr std::uint64_t CanonicalStreamScopeIdV1(
    std::uint32_t source_stream_id) noexcept {
    return source_stream_id;
}

[[nodiscard]] constexpr std::uint64_t CanonicalVendorMessageScopeIdV1(
    std::uint8_t service_id,
    std::uint16_t message_id) noexcept {
    return service_id == 0U || message_id == 0U
        ? 0U
        : (static_cast<std::uint64_t>(service_id) << 32U) |
              static_cast<std::uint64_t>(message_id);
}

[[nodiscard]] constexpr std::uint64_t CanonicalChannelScopeIdV1(
    CanonicalMarketV1 market,
    std::uint32_t channel) noexcept {
    const std::uint64_t kind = market == CanonicalMarketV1::kShanghai
        ? 2U
        : (market == CanonicalMarketV1::kShenzhen ? 3U : 0U);
    // Channel is an opaque upstream-validated uint32 code.  Zero is not
    // invented as invalid here; the nonzero market-kind tag still makes the
    // encoded scope id nonzero and unambiguous.
    return kind == 0U
        ? 0U
        : (kind << 32U) | static_cast<std::uint64_t>(channel);
}

[[nodiscard]] constexpr std::uint64_t CanonicalSnapshotFamilyScopeIdV1(
    CanonicalMarketV1 market) noexcept {
    return market == CanonicalMarketV1::kShanghai ||
                   market == CanonicalMarketV1::kShenzhen
        ? (std::uint64_t{1U} << 32U) |
              static_cast<std::uint64_t>(market)
        : 0U;
}

// Values intentionally match the Phase-3 logical control type, but the
// enclosing Canonical record is a distinct schema and is never a reinterpret
// cast of the standalone 256-byte Phase-3 control wire record.
enum class CanonicalControlTypeV1 : std::uint16_t {
    kUnknown = 0U,
    kConnecting = 1U,
    kConnectError = 2U,
    kDisconnected = 3U,
    kLogonSuccess = 4U,
    kLogonFailure = 5U,
    kSubscriptionAccepted = 6U,
    kSubscriptionRejected = 7U,
    kServiceStatus = 8U,
    kSessionStatus = 9U,
    kDecodeError = 10U,
};

enum class CanonicalTickValidityV1 : std::uint32_t {
    kPrice = 1U << 0U,
    kQuantity = 1U << 1U,
    kTradeAmount = 1U << 2U,
    kMatchedQuantity = 1U << 3U,
    kPrimaryOrderId = 1U << 4U,
    kBuyOrderId = 1U << 5U,
    kSellOrderId = 1U << 6U,
    kExchangeTime = 1U << 7U,
    kSide = 1U << 8U,
    kOrderType = 1U << 9U,
    kAggressor = 1U << 10U,
    kPhase = 1U << 11U,
};

[[nodiscard]] constexpr std::uint32_t CanonicalTickValidityBitV1(
    CanonicalTickValidityV1 bit) noexcept {
    return static_cast<std::uint32_t>(bit);
}

inline constexpr std::uint32_t kCanonicalTickValidityMaskV1 = 0x00000fffU;

// business_flags is schema-reserved in V1. Reviewed business semantics must
// use a new mask/version rather than silently assigning a bit later.
inline constexpr std::uint32_t kCanonicalTickBusinessFlagsMaskV1 = 0U;

enum class CanonicalSnapshotScalarValidityV1 : std::uint64_t {
    kPreClosePrice = std::uint64_t{1U} << 0U,
    kOpenPrice = std::uint64_t{1U} << 1U,
    kHighPrice = std::uint64_t{1U} << 2U,
    kLowPrice = std::uint64_t{1U} << 3U,
    kLastPrice = std::uint64_t{1U} << 4U,
    kClosePrice = std::uint64_t{1U} << 5U,
    kVolumeNative = std::uint64_t{1U} << 6U,
    kTurnoverP6 = std::uint64_t{1U} << 7U,
    kTradeCount = std::uint64_t{1U} << 8U,
    kTotalBidQuantity = std::uint64_t{1U} << 9U,
    kTotalAskQuantity = std::uint64_t{1U} << 10U,
    kWeightedBidPrice = std::uint64_t{1U} << 11U,
    kWeightedAskPrice = std::uint64_t{1U} << 12U,
    kHighLimitPrice = std::uint64_t{1U} << 13U,
    kLowLimitPrice = std::uint64_t{1U} << 14U,
    kIopv = std::uint64_t{1U} << 15U,
    kExchangeTime = std::uint64_t{1U} << 16U,
    kRawPhase = std::uint64_t{1U} << 17U,
    kNormalizedPhase = std::uint64_t{1U} << 18U,
    kImageStatus = std::uint64_t{1U} << 19U,
    kStatusCode = std::uint64_t{1U} << 20U,
    kQuantityUnit = std::uint64_t{1U} << 21U,
    kActualBidDepth = std::uint64_t{1U} << 22U,
    kActualAskDepth = std::uint64_t{1U} << 23U,
    kBid1TotalOrderCount = std::uint64_t{1U} << 24U,
    kBid1RevealedCount = std::uint64_t{1U} << 25U,
    kAsk1TotalOrderCount = std::uint64_t{1U} << 26U,
    kAsk1RevealedCount = std::uint64_t{1U} << 27U,
};

[[nodiscard]] constexpr std::uint64_t CanonicalSnapshotValidityBitV1(
    CanonicalSnapshotScalarValidityV1 bit) noexcept {
    return static_cast<std::uint64_t>(bit);
}

inline constexpr std::uint64_t kCanonicalSnapshotScalarValidityMaskV1 =
    0x000000000fffffffULL;

enum class CanonicalSnapshotFlagV1 : std::uint32_t {
    // These Phase-5-local flags intentionally do not consume or reinterpret
    // any bit in the frozen Phase-3 quality bitmap.
    kBidDepthTruncatedTo10 = 1U << 0U,
    kAskDepthTruncatedTo10 = 1U << 1U,
};

[[nodiscard]] constexpr std::uint32_t CanonicalSnapshotFlagBitV1(
    CanonicalSnapshotFlagV1 bit) noexcept {
    return static_cast<std::uint32_t>(bit);
}

inline constexpr std::uint32_t kCanonicalSnapshotFlagsMaskV1 = 0x00000003U;
inline constexpr std::uint16_t kCanonicalLevelValidityMaskV1 = 0x03ffU;
inline constexpr std::uint64_t kCanonicalQueueValidityMaskV1 =
    0x0003ffffffffffffULL;
// Exact frozen Phase-3 V1 quality bit range (bits 0 through 35).
inline constexpr std::uint64_t kCanonicalQualityFlagsMaskV1 =
    0x0000000fffffffffULL;

enum class CanonicalControlFlagV1 : std::uint16_t {
    kResponseManifestHashPresent = 1U << 0U,
    kAddressHashPresent = 1U << 1U,
    kErrorTextHashPresent = 1U << 2U,
    kRequiredFailure = 1U << 3U,
    kOptionalFailure = 1U << 4U,
};

[[nodiscard]] constexpr std::uint16_t CanonicalControlFlagBitV1(
    CanonicalControlFlagV1 bit) noexcept {
    return static_cast<std::uint16_t>(bit);
}

inline constexpr std::uint16_t kCanonicalControlFlagsMaskV1 = 0x001fU;

struct alignas(8) CanonicalHeaderV1 final {
    std::uint32_t magic = kCanonicalMagicV1;
    std::uint16_t schema_version = kCanonicalSchemaVersionV1;
    CanonicalEventTypeV1 event_type = CanonicalEventTypeV1::kUnknown;
    std::uint32_t record_size = 0U;
    std::uint32_t source_stream_id = 0U;
    std::uint32_t connection_epoch = 0U;
    std::uint32_t trade_date = 0U;
    std::uint64_t quality_flags = 0U;
    std::uint64_t shard_event_id = 0U;
    std::uint64_t origin_ingress_sequence = 0U;
    std::uint64_t origin_wal_end_pos = 0U;
    std::uint64_t vendor_sequence_id = 0U;
    std::uint64_t exchange_sequence = 0U;
    std::int64_t exchange_time_ns = 0;
    std::int64_t recv_realtime_ns = 0;
    std::int64_t recv_monotonic_ns = 0;
    std::uint32_t instrument_id = 0U;
    std::uint32_t channel = 0U;
    CanonicalMarketV1 market = CanonicalMarketV1::kUnknown;
    std::uint16_t origin_service_version = 0U;
    std::uint16_t origin_message_id = 0U;
    std::uint8_t origin_service_id = 0U;
    std::uint8_t sub_index = 0U;
};

struct alignas(8) CanonicalTickPayloadV1 final {
    std::int64_t price_p6 = 0;
    std::int64_t quantity_native = 0;
    std::int64_t trade_amount_p6 = 0;
    std::int64_t matched_quantity_native = 0;
    std::int64_t primary_order_id = 0;
    std::int64_t buy_order_id = 0;
    std::int64_t sell_order_id = 0;
    std::uint32_t validity_bitmap = 0U;
    std::uint32_t business_flags = 0U;
    std::uint64_t source_enum_bits = 0U;
    CanonicalTickActionV1 action = CanonicalTickActionV1::kUnknown;
    CanonicalSideV1 side = CanonicalSideV1::kUnknown;
    CanonicalOrderTypeV1 order_type = CanonicalOrderTypeV1::kUnknown;
    CanonicalAggressorV1 aggressor = CanonicalAggressorV1::kUnknown;
    CanonicalQuantityUnitV1 quantity_unit =
        CanonicalQuantityUnitV1::kUnknown;
    CanonicalTradingPhaseV1 phase = CanonicalTradingPhaseV1::kUnknown;
    std::uint16_t reserved = 0U;
};

struct alignas(8) CanonicalTickRecordV1 final {
    CanonicalHeaderV1 header{};
    CanonicalTickPayloadV1 payload{};
};

struct alignas(8) CanonicalSnapshotPayloadV1 final {
    std::int64_t pre_close_price_p6 = 0;
    std::int64_t open_price_p6 = 0;
    std::int64_t high_price_p6 = 0;
    std::int64_t low_price_p6 = 0;
    std::int64_t last_price_p6 = 0;
    std::int64_t close_price_p6 = 0;
    std::int64_t volume_native = 0;
    std::int64_t turnover_p6 = 0;
    std::int64_t trade_count = 0;
    std::int64_t total_bid_quantity_native = 0;
    std::int64_t total_ask_quantity_native = 0;
    std::int64_t weighted_bid_price_p6 = 0;
    std::int64_t weighted_ask_price_p6 = 0;
    std::int64_t high_limit_price_p6 = 0;
    std::int64_t low_limit_price_p6 = 0;
    std::int64_t iopv_p6 = 0;
    std::array<std::int64_t, kCanonicalDepthLevelsV1> bid_price_p6{};
    std::array<std::int64_t, kCanonicalDepthLevelsV1>
        bid_quantity_native{};
    std::array<std::int64_t, kCanonicalDepthLevelsV1> ask_price_p6{};
    std::array<std::int64_t, kCanonicalDepthLevelsV1>
        ask_quantity_native{};
    std::array<std::int64_t, kCanonicalQueueEntriesV1>
        bid1_queue_quantity_native{};
    std::array<std::int64_t, kCanonicalQueueEntriesV1>
        ask1_queue_quantity_native{};
    std::array<std::uint32_t, kCanonicalDepthLevelsV1>
        bid_order_count{};
    std::array<std::uint32_t, kCanonicalDepthLevelsV1>
        ask_order_count{};
    // raw_phase_bits and status_code_bits store at most eight source bytes in
    // little-endian slot order. Longer source strings remain in Raw.
    std::uint64_t raw_phase_bits = 0U;
    std::uint64_t status_code_bits = 0U;
    std::uint64_t scalar_validity = 0U;
    std::uint64_t bid_queue_validity = 0U;
    std::uint64_t ask_queue_validity = 0U;
    std::uint32_t snapshot_flags = 0U;
    std::uint32_t image_status = 0U;
    std::uint32_t actual_bid_depth = 0U;
    std::uint32_t actual_ask_depth = 0U;
    std::uint32_t bid1_total_order_count = 0U;
    std::uint32_t bid1_revealed_count = 0U;
    std::uint32_t ask1_total_order_count = 0U;
    std::uint32_t ask1_revealed_count = 0U;
    std::uint16_t bid_price_validity = 0U;
    std::uint16_t bid_quantity_validity = 0U;
    std::uint16_t bid_order_count_validity = 0U;
    std::uint16_t ask_price_validity = 0U;
    std::uint16_t ask_quantity_validity = 0U;
    std::uint16_t ask_order_count_validity = 0U;
    CanonicalTradingPhaseV1 phase = CanonicalTradingPhaseV1::kUnknown;
    CanonicalQuantityUnitV1 quantity_unit =
        CanonicalQuantityUnitV1::kUnknown;
    CanonicalLimitSemanticsV1 high_limit_semantics =
        CanonicalLimitSemanticsV1::kUnknown;
    CanonicalLimitSemanticsV1 low_limit_semantics =
        CanonicalLimitSemanticsV1::kUnknown;
    std::array<std::byte, 520U> reserved{};
};

struct alignas(8) CanonicalSnapshotRecordV1 final {
    CanonicalHeaderV1 header{};
    CanonicalSnapshotPayloadV1 payload{};
};

struct alignas(8) CanonicalQualityPayloadV1 final {
    std::uint64_t expected_sequence = 0U;
    std::uint64_t actual_sequence = 0U;
    std::uint64_t first_bad_origin_wal_end_pos = 0U;
    std::uint64_t scope_id = 0U;
    l2flow::common::Sha256Digest payload_sha256{};
    std::uint32_t related_connection_epoch = 0U;
    CanonicalQualityTypeV1 quality_type =
        CanonicalQualityTypeV1::kUnknown;
    CanonicalQualityScopeV1 scope_type =
        CanonicalQualityScopeV1::kUnknown;
    std::uint16_t detail_code = 0U;
    std::uint16_t human_code_id = 0U;
    std::uint32_t reserved = 0U;
};

struct alignas(8) CanonicalQualityRecordV1 final {
    CanonicalHeaderV1 header{};
    CanonicalQualityPayloadV1 payload{};
};

struct alignas(8) CanonicalControlPayloadV1 final {
    l2flow::common::Sha256Digest response_manifest_sha256{};
    l2flow::common::Sha256Digest address_sha256{};
    l2flow::common::Sha256Digest error_text_sha256{};
    std::uint32_t return_or_error_code = 0U;
    std::uint32_t connection_epoch = 0U;
    std::uint32_t subscription_epoch = 0U;
    std::uint32_t required_count = 0U;
    std::uint32_t required_ok_count = 0U;
    std::uint32_t required_failed_count = 0U;
    std::uint32_t optional_count = 0U;
    std::uint32_t optional_ok_count = 0U;
    std::uint32_t optional_failed_count = 0U;
    std::uint32_t response_entry_count = 0U;
    CanonicalControlTypeV1 control_type =
        CanonicalControlTypeV1::kUnknown;
    std::uint16_t flags = 0U;
    std::uint32_t reserved = 0U;
};

struct alignas(8) CanonicalControlRecordV1 final {
    CanonicalHeaderV1 header{};
    CanonicalControlPayloadV1 payload{};
};

enum class CanonicalValidationErrorV1 : std::uint8_t {
    kNone = 0U,
    kUnsupportedHostEndian,
    kInvalidMagic,
    kUnsupportedSchemaVersion,
    kUnknownEventType,
    kEventTypeMismatch,
    kInvalidRecordSize,
    kInvalidSourceStream,
    kInvalidOriginCursor,
    kInvalidShardEventId,
    kUnknownQualityFlags,
    kUnknownMarket,
    kMissingInstrument,
    kUnknownEnum,
    kUnknownFlags,
    kUnknownValidityBits,
    kNonzeroReserved,
    kInvalidFieldValue,
    kInconsistentFieldValidity,
    kSnapshotQueueTooLong,
    kInconsistentSnapshotDepth,
    kInvalidCounts,
    kHashPresenceMismatch,
};

[[nodiscard]] std::string_view CanonicalValidationErrorNameV1(
    CanonicalValidationErrorV1 error) noexcept;

[[nodiscard]] CanonicalValidationErrorV1 ValidateCanonicalHeaderV1(
    const CanonicalHeaderV1& header) noexcept;
[[nodiscard]] CanonicalValidationErrorV1 ValidateCanonicalTickRecordV1(
    const CanonicalTickRecordV1& record) noexcept;
[[nodiscard]] CanonicalValidationErrorV1 ValidateCanonicalSnapshotRecordV1(
    const CanonicalSnapshotRecordV1& record) noexcept;
[[nodiscard]] CanonicalValidationErrorV1 ValidateCanonicalQualityRecordV1(
    const CanonicalQualityRecordV1& record) noexcept;
[[nodiscard]] CanonicalValidationErrorV1 ValidateCanonicalControlRecordV1(
    const CanonicalControlRecordV1& record) noexcept;

// Stable ASCII descriptors are the hash inputs. The schema descriptor freezes
// wire enums, masks and semantic validation; the dtype descriptor freezes the
// NumPy-visible field names, primitive widths, shapes and byte offsets.
[[nodiscard]] std::string_view CanonicalSchemaDescriptorV1() noexcept;
[[nodiscard]] l2flow::common::Sha256Digest
CanonicalSchemaDescriptorSha256V1() noexcept;
[[nodiscard]] std::string_view CanonicalDtypeDescriptorV1() noexcept;
[[nodiscard]] l2flow::common::Sha256Digest
CanonicalDtypeDescriptorSha256V1() noexcept;

static_assert(sizeof(ClockEpochIdentityV1) == 48U);
static_assert(alignof(ClockEpochIdentityV1) == 8U);
static_assert(offsetof(ClockEpochIdentityV1, algorithm) == 0U);
static_assert(offsetof(ClockEpochIdentityV1, digest) == 4U);
static_assert(offsetof(ClockEpochIdentityV1, label) == 40U);

static_assert(sizeof(CanonicalHeaderV1) == kCanonicalHeaderBytesV1);
static_assert(alignof(CanonicalHeaderV1) == 8U);
static_assert(offsetof(CanonicalHeaderV1, magic) == 0U);
static_assert(offsetof(CanonicalHeaderV1, schema_version) == 4U);
static_assert(offsetof(CanonicalHeaderV1, event_type) == 6U);
static_assert(offsetof(CanonicalHeaderV1, record_size) == 8U);
static_assert(offsetof(CanonicalHeaderV1, source_stream_id) == 12U);
static_assert(offsetof(CanonicalHeaderV1, connection_epoch) == 16U);
static_assert(offsetof(CanonicalHeaderV1, trade_date) == 20U);
static_assert(offsetof(CanonicalHeaderV1, quality_flags) == 24U);
static_assert(offsetof(CanonicalHeaderV1, shard_event_id) == 32U);
static_assert(offsetof(CanonicalHeaderV1, origin_ingress_sequence) == 40U);
static_assert(offsetof(CanonicalHeaderV1, origin_wal_end_pos) == 48U);
static_assert(offsetof(CanonicalHeaderV1, vendor_sequence_id) == 56U);
static_assert(offsetof(CanonicalHeaderV1, exchange_sequence) == 64U);
static_assert(offsetof(CanonicalHeaderV1, exchange_time_ns) == 72U);
static_assert(offsetof(CanonicalHeaderV1, recv_realtime_ns) == 80U);
static_assert(offsetof(CanonicalHeaderV1, recv_monotonic_ns) == 88U);
static_assert(offsetof(CanonicalHeaderV1, instrument_id) == 96U);
static_assert(offsetof(CanonicalHeaderV1, channel) == 100U);
static_assert(offsetof(CanonicalHeaderV1, market) == 104U);
static_assert(offsetof(CanonicalHeaderV1, origin_service_version) == 106U);
static_assert(offsetof(CanonicalHeaderV1, origin_message_id) == 108U);
static_assert(offsetof(CanonicalHeaderV1, origin_service_id) == 110U);
static_assert(offsetof(CanonicalHeaderV1, sub_index) == 111U);

static_assert(sizeof(CanonicalTickPayloadV1) ==
              kCanonicalTickPayloadBytesV1);
static_assert(alignof(CanonicalTickPayloadV1) == 8U);
static_assert(offsetof(CanonicalTickPayloadV1, price_p6) == 0U);
static_assert(offsetof(CanonicalTickPayloadV1, quantity_native) == 8U);
static_assert(offsetof(CanonicalTickPayloadV1, trade_amount_p6) == 16U);
static_assert(offsetof(CanonicalTickPayloadV1,
                       matched_quantity_native) == 24U);
static_assert(offsetof(CanonicalTickPayloadV1, primary_order_id) == 32U);
static_assert(offsetof(CanonicalTickPayloadV1, buy_order_id) == 40U);
static_assert(offsetof(CanonicalTickPayloadV1, sell_order_id) == 48U);
static_assert(offsetof(CanonicalTickPayloadV1, validity_bitmap) == 56U);
static_assert(offsetof(CanonicalTickPayloadV1, business_flags) == 60U);
static_assert(offsetof(CanonicalTickPayloadV1, source_enum_bits) == 64U);
static_assert(offsetof(CanonicalTickPayloadV1, action) == 72U);
static_assert(offsetof(CanonicalTickPayloadV1, side) == 73U);
static_assert(offsetof(CanonicalTickPayloadV1, order_type) == 74U);
static_assert(offsetof(CanonicalTickPayloadV1, aggressor) == 75U);
static_assert(offsetof(CanonicalTickPayloadV1, quantity_unit) == 76U);
static_assert(offsetof(CanonicalTickPayloadV1, phase) == 77U);
static_assert(offsetof(CanonicalTickPayloadV1, reserved) == 78U);
static_assert(sizeof(CanonicalTickRecordV1) == kCanonicalTickRecordBytesV1);
static_assert(alignof(CanonicalTickRecordV1) == 8U);
static_assert(offsetof(CanonicalTickRecordV1, header) == 0U);
static_assert(offsetof(CanonicalTickRecordV1, payload) == 112U);

static_assert(sizeof(CanonicalSnapshotPayloadV1) ==
              kCanonicalSnapshotPayloadBytesV1);
static_assert(alignof(CanonicalSnapshotPayloadV1) == 8U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       pre_close_price_p6) == 0U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, open_price_p6) == 8U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, high_price_p6) == 16U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, low_price_p6) == 24U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, last_price_p6) == 32U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, close_price_p6) == 40U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, volume_native) == 48U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, turnover_p6) == 56U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, trade_count) == 64U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       total_bid_quantity_native) == 72U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       total_ask_quantity_native) == 80U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       weighted_bid_price_p6) == 88U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       weighted_ask_price_p6) == 96U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       high_limit_price_p6) == 104U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       low_limit_price_p6) == 112U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, iopv_p6) == 120U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, bid_price_p6) == 128U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       bid_quantity_native) == 208U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, ask_price_p6) == 288U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       ask_quantity_native) == 368U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       bid1_queue_quantity_native) == 448U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       ask1_queue_quantity_native) == 848U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       bid_order_count) == 1248U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       ask_order_count) == 1288U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, raw_phase_bits) == 1328U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, status_code_bits) == 1336U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, scalar_validity) == 1344U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       bid_queue_validity) == 1352U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       ask_queue_validity) == 1360U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, snapshot_flags) == 1368U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, image_status) == 1372U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, actual_bid_depth) == 1376U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, actual_ask_depth) == 1380U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       bid1_total_order_count) == 1384U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       bid1_revealed_count) == 1388U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       ask1_total_order_count) == 1392U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       ask1_revealed_count) == 1396U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       bid_price_validity) == 1400U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       bid_quantity_validity) == 1402U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       bid_order_count_validity) == 1404U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       ask_price_validity) == 1406U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       ask_quantity_validity) == 1408U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       ask_order_count_validity) == 1410U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, phase) == 1412U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, quantity_unit) == 1413U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       high_limit_semantics) == 1414U);
static_assert(offsetof(CanonicalSnapshotPayloadV1,
                       low_limit_semantics) == 1415U);
static_assert(offsetof(CanonicalSnapshotPayloadV1, reserved) == 1416U);
static_assert(sizeof(CanonicalSnapshotRecordV1) ==
              kCanonicalSnapshotRecordBytesV1);
static_assert(alignof(CanonicalSnapshotRecordV1) == 8U);
static_assert(offsetof(CanonicalSnapshotRecordV1, header) == 0U);
static_assert(offsetof(CanonicalSnapshotRecordV1, payload) == 112U);

static_assert(sizeof(CanonicalQualityPayloadV1) ==
              kCanonicalQualityPayloadBytesV1);
static_assert(alignof(CanonicalQualityPayloadV1) == 8U);
static_assert(offsetof(CanonicalQualityPayloadV1, expected_sequence) == 0U);
static_assert(offsetof(CanonicalQualityPayloadV1, actual_sequence) == 8U);
static_assert(offsetof(CanonicalQualityPayloadV1,
                       first_bad_origin_wal_end_pos) == 16U);
static_assert(offsetof(CanonicalQualityPayloadV1, scope_id) == 24U);
static_assert(offsetof(CanonicalQualityPayloadV1, payload_sha256) == 32U);
static_assert(offsetof(CanonicalQualityPayloadV1,
                       related_connection_epoch) == 64U);
static_assert(offsetof(CanonicalQualityPayloadV1, quality_type) == 68U);
static_assert(offsetof(CanonicalQualityPayloadV1, scope_type) == 70U);
static_assert(offsetof(CanonicalQualityPayloadV1, detail_code) == 72U);
static_assert(offsetof(CanonicalQualityPayloadV1, human_code_id) == 74U);
static_assert(offsetof(CanonicalQualityPayloadV1, reserved) == 76U);
static_assert(sizeof(CanonicalQualityRecordV1) ==
              kCanonicalQualityRecordBytesV1);
static_assert(alignof(CanonicalQualityRecordV1) == 8U);
static_assert(offsetof(CanonicalQualityRecordV1, header) == 0U);
static_assert(offsetof(CanonicalQualityRecordV1, payload) == 112U);

static_assert(sizeof(CanonicalControlPayloadV1) ==
              kCanonicalControlPayloadBytesV1);
static_assert(alignof(CanonicalControlPayloadV1) == 8U);
static_assert(offsetof(CanonicalControlPayloadV1,
                       response_manifest_sha256) == 0U);
static_assert(offsetof(CanonicalControlPayloadV1, address_sha256) == 32U);
static_assert(offsetof(CanonicalControlPayloadV1,
                       error_text_sha256) == 64U);
static_assert(offsetof(CanonicalControlPayloadV1,
                       return_or_error_code) == 96U);
static_assert(offsetof(CanonicalControlPayloadV1, connection_epoch) == 100U);
static_assert(offsetof(CanonicalControlPayloadV1,
                       subscription_epoch) == 104U);
static_assert(offsetof(CanonicalControlPayloadV1, required_count) == 108U);
static_assert(offsetof(CanonicalControlPayloadV1,
                       required_ok_count) == 112U);
static_assert(offsetof(CanonicalControlPayloadV1,
                       required_failed_count) == 116U);
static_assert(offsetof(CanonicalControlPayloadV1, optional_count) == 120U);
static_assert(offsetof(CanonicalControlPayloadV1,
                       optional_ok_count) == 124U);
static_assert(offsetof(CanonicalControlPayloadV1,
                       optional_failed_count) == 128U);
static_assert(offsetof(CanonicalControlPayloadV1,
                       response_entry_count) == 132U);
static_assert(offsetof(CanonicalControlPayloadV1, control_type) == 136U);
static_assert(offsetof(CanonicalControlPayloadV1, flags) == 138U);
static_assert(offsetof(CanonicalControlPayloadV1, reserved) == 140U);
static_assert(sizeof(CanonicalControlRecordV1) ==
              kCanonicalControlRecordBytesV1);
static_assert(alignof(CanonicalControlRecordV1) == 8U);
static_assert(offsetof(CanonicalControlRecordV1, header) == 0U);
static_assert(offsetof(CanonicalControlRecordV1, payload) == 112U);

static_assert(std::is_standard_layout_v<ClockEpochIdentityV1>);
static_assert(std::is_trivially_copyable_v<ClockEpochIdentityV1>);
static_assert(std::is_standard_layout_v<CanonicalHeaderV1>);
static_assert(std::is_trivially_copyable_v<CanonicalHeaderV1>);
static_assert(std::is_standard_layout_v<CanonicalTickRecordV1>);
static_assert(std::is_trivially_copyable_v<CanonicalTickRecordV1>);
static_assert(std::is_standard_layout_v<CanonicalSnapshotRecordV1>);
static_assert(std::is_trivially_copyable_v<CanonicalSnapshotRecordV1>);
static_assert(std::is_standard_layout_v<CanonicalQualityRecordV1>);
static_assert(std::is_trivially_copyable_v<CanonicalQualityRecordV1>);
static_assert(std::is_standard_layout_v<CanonicalControlRecordV1>);
static_assert(std::is_trivially_copyable_v<CanonicalControlRecordV1>);

}  // namespace l2flow::canonical
