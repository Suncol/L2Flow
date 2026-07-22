#include "l2flow/canonical/canonical_schema_v1.h"
#include "l2flow/common/sha256.h"
#include "l2flow/control/quality_flags_v1.h"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <span>
#include <string>
#include <string_view>

namespace canonical = l2flow::canonical;
namespace common = l2flow::common;
namespace control = l2flow::control;

namespace {

// Independent ABI expectations: these literals intentionally do not use the
// production offset or size constants.
static_assert(sizeof(canonical::CanonicalHeaderV1) == 112U);
static_assert(alignof(canonical::CanonicalHeaderV1) == 8U);
static_assert(offsetof(canonical::CanonicalHeaderV1, quality_flags) == 24U);
static_assert(offsetof(canonical::CanonicalHeaderV1,
                       origin_wal_end_pos) == 48U);
static_assert(offsetof(canonical::CanonicalHeaderV1, instrument_id) == 96U);
static_assert(offsetof(canonical::CanonicalHeaderV1, sub_index) == 111U);
static_assert(sizeof(canonical::CanonicalTickPayloadV1) == 80U);
static_assert(sizeof(canonical::CanonicalTickRecordV1) == 192U);
static_assert(offsetof(canonical::CanonicalTickRecordV1, payload) == 112U);
static_assert(offsetof(canonical::CanonicalTickPayloadV1,
                       validity_bitmap) == 56U);
static_assert(offsetof(canonical::CanonicalTickPayloadV1, action) == 72U);
static_assert(sizeof(canonical::CanonicalSnapshotPayloadV1) == 1936U);
static_assert(sizeof(canonical::CanonicalSnapshotRecordV1) == 2048U);
static_assert(offsetof(canonical::CanonicalSnapshotPayloadV1,
                       bid1_queue_quantity_native) == 448U);
static_assert(offsetof(canonical::CanonicalSnapshotPayloadV1,
                       ask1_queue_quantity_native) == 848U);
static_assert(offsetof(canonical::CanonicalSnapshotPayloadV1,
                       scalar_validity) == 1344U);
static_assert(offsetof(canonical::CanonicalSnapshotPayloadV1,
                       reserved) == 1416U);
static_assert(sizeof(canonical::CanonicalQualityPayloadV1) == 80U);
static_assert(sizeof(canonical::CanonicalQualityRecordV1) == 192U);
static_assert(sizeof(canonical::CanonicalControlPayloadV1) == 144U);
static_assert(sizeof(canonical::CanonicalControlRecordV1) == 256U);
static_assert(offsetof(canonical::CanonicalControlPayloadV1,
                       response_entry_count) == 132U);
static_assert(offsetof(canonical::CanonicalControlPayloadV1,
                       reserved) == 140U);
static_assert(
    static_cast<std::uint16_t>(
        canonical::CanonicalEventTypeV1::kSnapshot) == 1U);
static_assert(
    static_cast<std::uint16_t>(canonical::CanonicalEventTypeV1::kControl) ==
    4U);
static_assert(
    static_cast<std::uint8_t>(canonical::CanonicalTickActionV1::kStatus) ==
    4U);
static_assert(
    static_cast<std::uint8_t>(canonical::CanonicalSideV1::kLend) == 4U);
static_assert(
    static_cast<std::uint16_t>(
        canonical::CanonicalQualityTypeV1::kInstrumentUnknown) == 13U);
static_assert(
    static_cast<std::uint16_t>(
        canonical::CanonicalQualityTypeV1::kSequenceCapacityExhausted) ==
    14U);
static_assert(
    static_cast<std::uint16_t>(
        canonical::CanonicalQualityTypeV1::kNormalizationRejected) == 15U);
static_assert(
    static_cast<std::uint16_t>(
        canonical::CanonicalQualityTypeV1::kExchangeSequenceDuplicate) ==
    16U);
static_assert(
    static_cast<std::uint16_t>(
        canonical::CanonicalControlTypeV1::kDecodeError) == 10U);

class TestContext final {
public:
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    void ExpectHex(const common::Sha256Digest& digest,
                   std::string_view expected,
                   std::string_view description) {
        const std::string actual = common::Sha256Hex(digest);
        if (actual != expected) {
            ++failures_;
            std::cerr << "FAIL: " << description << "\n  actual: "
                      << actual << "\nexpected: " << expected << '\n';
        }
    }

    [[nodiscard]] int failures() const noexcept {
        return failures_;
    }

private:
    int failures_ = 0;
};

canonical::CanonicalHeaderV1 MakeHeader(
    canonical::CanonicalEventTypeV1 event_type,
    std::uint32_t record_size) {
    canonical::CanonicalHeaderV1 header;
    header.event_type = event_type;
    header.record_size = record_size;
    header.source_stream_id = 1001U;
    header.connection_epoch = 1U;
    header.trade_date = 20260722U;
    header.shard_event_id = 1U;
    header.origin_ingress_sequence = 2U;
    header.origin_wal_end_pos = 8192U;
    header.vendor_sequence_id = 3U;
    header.recv_realtime_ns = 4;
    header.recv_monotonic_ns = 5;
    header.origin_service_version = 101U;
    header.origin_message_id = 1U;
    header.origin_service_id = 4U;
    if (event_type == canonical::CanonicalEventTypeV1::kQuality) {
        header.sub_index = 2U;
    }
    if (event_type == canonical::CanonicalEventTypeV1::kSnapshot ||
        event_type == canonical::CanonicalEventTypeV1::kTick) {
        header.instrument_id = 101U;
        header.market = canonical::CanonicalMarketV1::kShanghai;
    }
    if (event_type == canonical::CanonicalEventTypeV1::kControl) {
        header.origin_service_id = 1U;
        header.vendor_sequence_id = 0U;
    }
    return header;
}

canonical::CanonicalTickRecordV1 MakeValidTick() {
    canonical::CanonicalTickRecordV1 record;
    record.header = MakeHeader(
        canonical::CanonicalEventTypeV1::kTick, 192U);
    record.header.origin_message_id = 24U;
    record.header.exchange_sequence = 51U;
    record.header.exchange_time_ns = 1'650'000'000'000'000'000LL;
    record.payload.action = canonical::CanonicalTickActionV1::kTrade;
    record.payload.aggressor = canonical::CanonicalAggressorV1::kBuy;
    record.payload.quantity_unit = canonical::CanonicalQuantityUnitV1::kShare;
    record.payload.price_p6 = 12'345'000;
    record.payload.quantity_native = 100;
    record.payload.trade_amount_p6 = 1'234'500'000;
    record.payload.buy_order_id = 71;
    record.payload.sell_order_id = 72;
    record.payload.source_enum_bits =
        static_cast<std::uint64_t>('T') |
        (static_cast<std::uint64_t>('B') << 16U);
    record.payload.validity_bitmap =
        canonical::CanonicalTickValidityBitV1(
            canonical::CanonicalTickValidityV1::kPrice) |
        canonical::CanonicalTickValidityBitV1(
            canonical::CanonicalTickValidityV1::kQuantity) |
        canonical::CanonicalTickValidityBitV1(
            canonical::CanonicalTickValidityV1::kTradeAmount) |
        canonical::CanonicalTickValidityBitV1(
            canonical::CanonicalTickValidityV1::kBuyOrderId) |
        canonical::CanonicalTickValidityBitV1(
            canonical::CanonicalTickValidityV1::kSellOrderId) |
        canonical::CanonicalTickValidityBitV1(
            canonical::CanonicalTickValidityV1::kExchangeTime) |
        canonical::CanonicalTickValidityBitV1(
            canonical::CanonicalTickValidityV1::kAggressor);
    return record;
}

canonical::CanonicalSnapshotRecordV1 MakeValidSnapshot() {
    canonical::CanonicalSnapshotRecordV1 record;
    record.header = MakeHeader(
        canonical::CanonicalEventTypeV1::kSnapshot, 2048U);
    record.header.origin_message_id = 4U;
    record.payload.actual_bid_depth = 12U;
    record.payload.actual_ask_depth = 2U;
    record.payload.bid1_revealed_count = 2U;
    record.payload.ask1_revealed_count = 1U;
    record.payload.bid1_total_order_count = 2U;
    record.payload.ask1_total_order_count = 1U;
    record.payload.scalar_validity =
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::kActualBidDepth) |
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::kActualAskDepth) |
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::
                kBid1RevealedCount) |
        canonical::CanonicalSnapshotValidityBitV1(
                canonical::CanonicalSnapshotScalarValidityV1::
                    kAsk1RevealedCount) |
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::
                kBid1TotalOrderCount) |
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::
                kAsk1TotalOrderCount);
    record.payload.snapshot_flags =
        canonical::CanonicalSnapshotFlagBitV1(
            canonical::CanonicalSnapshotFlagV1::kBidDepthTruncatedTo10);
    record.payload.bid_price_validity = 0x03ffU;
    record.payload.bid_quantity_validity = 0x03ffU;
    record.payload.ask_price_validity = 0x0003U;
    record.payload.ask_quantity_validity = 0x0003U;
    record.payload.bid_order_count_validity = 0x03ffU;
    record.payload.ask_order_count_validity = 0x0003U;
    record.payload.bid_order_count[0] = 2U;
    record.payload.ask_order_count[0] = 1U;
    for (std::size_t index = 0U; index < 10U; ++index) {
        record.payload.bid_price_p6[index] =
            10'000'000 - static_cast<std::int64_t>(index);
        record.payload.bid_quantity_native[index] =
            100 + static_cast<std::int64_t>(index);
    }
    for (std::size_t index = 0U; index < 2U; ++index) {
        record.payload.ask_price_p6[index] =
            10'000'001 + static_cast<std::int64_t>(index);
        record.payload.ask_quantity_native[index] =
            200 + static_cast<std::int64_t>(index);
    }
    record.payload.bid_queue_validity = 0x3U;
    record.payload.ask_queue_validity = 0x1U;
    record.payload.bid1_queue_quantity_native[0] = 10;
    record.payload.bid1_queue_quantity_native[1] = 20;
    record.payload.ask1_queue_quantity_native[0] = 30;
    return record;
}

canonical::CanonicalQualityRecordV1 MakeValidQuality() {
    canonical::CanonicalQualityRecordV1 record;
    record.header = MakeHeader(
        canonical::CanonicalEventTypeV1::kQuality, 192U);
    record.header.market = canonical::CanonicalMarketV1::kShanghai;
    record.header.origin_message_id = 24U;
    record.header.channel = 7U;
    record.header.exchange_sequence = 12U;
    record.header.quality_flags = control::QualityBit(
        control::QualityFlagV1::kExchangeSequenceGap);
    record.payload.expected_sequence = 10U;
    record.payload.actual_sequence = 12U;
    record.payload.first_bad_origin_wal_end_pos = 8192U;
    record.payload.scope_id = canonical::CanonicalChannelScopeIdV1(
        record.header.market, record.header.channel);
    record.payload.payload_sha256[0] = std::byte{1U};
    record.payload.related_connection_epoch = 1U;
    record.payload.quality_type =
        canonical::CanonicalQualityTypeV1::kExchangeSequenceGap;
    record.payload.scope_type =
        canonical::CanonicalQualityScopeV1::kChannel;
    record.payload.detail_code = 3U;
    return record;
}

void SetVendorQualityScope(
    canonical::CanonicalQualityRecordV1* record) {
    record->header.market = canonical::CanonicalMarketV1::kUnknown;
    record->header.instrument_id = 0U;
    record->header.channel = 0U;
    record->header.exchange_sequence = 0U;
    record->header.exchange_time_ns = 0;
    record->header.vendor_sequence_id = record->payload.actual_sequence;
    record->header.sub_index = 1U;
    record->payload.scope_type =
        canonical::CanonicalQualityScopeV1::kVendorMessage;
    record->payload.scope_id = canonical::CanonicalVendorMessageScopeIdV1(
        record->header.origin_service_id,
        record->header.origin_message_id);
}

canonical::CanonicalControlRecordV1 MakeValidControl() {
    canonical::CanonicalControlRecordV1 record;
    record.header = MakeHeader(
        canonical::CanonicalEventTypeV1::kControl, 256U);
    record.payload.connection_epoch = record.header.connection_epoch;
    record.payload.control_type =
        canonical::CanonicalControlTypeV1::kConnecting;
    record.payload.required_count = 1U;
    record.header.quality_flags = control::QualityBit(
        control::QualityFlagV1::kSessionUnknown);
    record.payload.address_sha256[0] = std::byte{0xa5U};
    record.payload.flags = canonical::CanonicalControlFlagBitV1(
        canonical::CanonicalControlFlagV1::kAddressHashPresent);
    return record;
}

void TestClockEpochIdentity(TestContext* test) {
    canonical::ClockEpochIdentityV1 first;
    first.algorithm = 1U;
    first.digest[0] = std::byte{1U};
    first.label = 11U;
    canonical::ClockEpochIdentityV1 relabeled = first;
    relabeled.label = 999U;
    canonical::ClockEpochIdentityV1 different = first;
    different.digest[1] = std::byte{2U};
    canonical::ClockEpochIdentityV1 invalid;

    test->Expect(
        canonical::ClockEpochIdentityV1Valid(first) &&
            first == relabeled && !(first == different) &&
            !canonical::ClockEpochIdentityV1Valid(invalid),
        "clock identity is algorithm+full digest and ignores label");
}

void TestDescriptorHashes(TestContext* test) {
    test->Expect(
        !canonical::CanonicalSchemaDescriptorV1().empty() &&
            !canonical::CanonicalDtypeDescriptorV1().empty(),
        "schema and dtype descriptors are nonempty");
    test->ExpectHex(
        canonical::CanonicalSchemaDescriptorSha256V1(),
        "f66cc65410a5c0862b87a2e467f13b02fe40c0910c17e418be82209b0ddf405e",
        "schema descriptor SHA-256 matches independent golden");
    test->ExpectHex(
        canonical::CanonicalDtypeDescriptorSha256V1(),
        "f92a990e174f4f1aae60244fef447f007c83292f8b8aeb375cc1913f7c8ba5bd",
        "dtype descriptor SHA-256 matches independent golden");
}

void TestTickValidationAndGolden(TestContext* test) {
    const canonical::CanonicalTickRecordV1 valid = MakeValidTick();
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(valid) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "valid tick record passes");

    const std::array<std::byte, 192U> bytes =
        std::bit_cast<std::array<std::byte, 192U>>(valid);
    test->Expect(
        bytes[0] == std::byte{0x4dU} &&
            bytes[1] == std::byte{0x43U} &&
            bytes[2] == std::byte{0x45U} &&
            bytes[3] == std::byte{0x31U} &&
            bytes[6] == std::byte{0x02U} &&
            bytes[8] == std::byte{0xc0U} &&
            bytes[9] == std::byte{0x00U} &&
            bytes[184] == std::byte{0x03U} &&
            bytes[185] == std::byte{0x00U} &&
            bytes[187] == std::byte{0x01U} &&
            bytes[190] == std::byte{0x00U} &&
            bytes[191] == std::byte{0x00U},
        "tick object bytes match independent little-endian offsets");
    test->ExpectHex(
        common::ComputeSha256(std::span<const std::byte>(bytes)),
        "ef332b1d1a6f6868b1586111eee27612555b0d5a660f2a2ed594aee15cc99c97",
        "tick golden bytes have stable SHA-256");

    canonical::CanonicalTickRecordV1 changed = valid;
    changed.header.magic = 0U;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidMagic,
        "bad canonical magic is rejected");
    changed = valid;
    changed.header.schema_version = 2U;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::
                kUnsupportedSchemaVersion,
        "unknown canonical version is rejected");
    changed = valid;
    changed.header.event_type =
        canonical::CanonicalEventTypeV1::kSnapshot;
    changed.header.record_size = 2048U;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kEventTypeMismatch,
        "record validator rejects a mismatched event type");
    changed = valid;
    changed.header.quality_flags = std::uint64_t{1U} << 36U;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kUnknownQualityFlags,
        "Phase-5 flags cannot consume an unknown Phase-3 quality bit");
    changed = valid;
    changed.payload.reserved = 1U;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kNonzeroReserved,
        "tick reserved bytes must remain zero");
    changed = valid;
    changed.payload.business_flags = 1U;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kUnknownFlags,
        "unassigned tick business flags are rejected");
    changed = valid;
    changed.payload.phase =
        static_cast<canonical::CanonicalTradingPhaseV1>(255U);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kUnknownEnum,
        "unknown tick enum is rejected");
    changed = valid;
    changed.payload.primary_order_id = 9;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::
                kInconsistentFieldValidity,
        "invalid tick scalar must be canonical zero");
    changed = valid;
    changed.payload.quantity_native = -1;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "a valid tick quantity cannot be negative");
    changed = valid;
    changed.payload.price_p6 = -1;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "a valid tick price cannot be negative");
    changed = valid;
    changed.payload.trade_amount_p6 = -1;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "a valid tick trade amount cannot be negative");
    changed = valid;
    changed.payload.quantity_native = 0;
    changed.payload.validity_bitmap &=
        ~canonical::CanonicalTickValidityBitV1(
            canonical::CanonicalTickValidityV1::kQuantity);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "trade action cannot omit its required quantity");
    changed = {};
    changed.header = valid.header;
    changed.payload.action = canonical::CanonicalTickActionV1::kAdd;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "add action with an empty validity set is rejected");
    changed = valid;
    changed.header.recv_monotonic_ns = -1;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "negative receive time is rejected by the common header");
    changed = valid;
    changed.payload.side = canonical::CanonicalSideV1::kBuy;
    changed.payload.validity_bitmap |=
        canonical::CanonicalTickValidityBitV1(
            canonical::CanonicalTickValidityV1::kSide);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "trade action cannot reinterpret side as aggressor");
    changed = valid;
    changed.payload.action = canonical::CanonicalTickActionV1::kStatus;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "status action cannot carry trade fields");

    changed = valid;
    changed.header.origin_message_id = 4U;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidOriginCursor,
        "tick cannot claim a snapshot origin tuple");
    changed = valid;
    changed.header.market = canonical::CanonicalMarketV1::kShenzhen;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "Shanghai tick origin cannot claim Shenzhen market");
    changed = valid;
    changed.header.exchange_sequence = 0U;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "tick business sequence must be positive");
    changed = valid;
    changed.header.exchange_sequence =
        std::numeric_limits<std::uint64_t>::max();
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "tick business sequence must fit its signed int64 wire domain");
    changed = valid;
    changed.header.sub_index = 1U;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidOriginCursor,
        "business record sub-index is exactly zero");
    changed = valid;
    changed.header.vendor_sequence_id = 0U;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidOriginCursor,
        "business record vendor sequence is positive");
    changed = valid;
    changed.payload.source_enum_bits |= std::uint64_t{1U} << 32U;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "unused source-enum slots stay zero");
    changed = valid;
    changed.payload.source_enum_bits =
        static_cast<std::uint64_t>('A') |
        (static_cast<std::uint64_t>('B') << 16U);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "raw Shanghai action slot must match normalized action");
    changed = valid;
    changed.header.exchange_time_ns = 0;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "valid exchange-time bit requires a positive projected timestamp");
    changed = valid;
    changed.header.channel = 0xffffffffU;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "Shanghai channel preserves every int32 bit pattern");

    canonical::CanonicalTickRecordV1 sz_order{};
    sz_order.header = valid.header;
    sz_order.header.origin_service_id = 6U;
    sz_order.header.origin_message_id = 33U;
    sz_order.header.market = canonical::CanonicalMarketV1::kShenzhen;
    sz_order.header.channel = 0U;
    sz_order.header.exchange_time_ns = 0;
    sz_order.payload.action = canonical::CanonicalTickActionV1::kAdd;
    sz_order.payload.side = canonical::CanonicalSideV1::kBuy;
    sz_order.payload.order_type = canonical::CanonicalOrderTypeV1::kLimit;
    sz_order.payload.quantity_native = 100;
    sz_order.payload.price_p6 = 10'000'000;
    sz_order.payload.primary_order_id = static_cast<std::int64_t>(
        sz_order.header.exchange_sequence);
    sz_order.payload.source_enum_bits = 49U | (50ULL << 16U);
    sz_order.payload.validity_bitmap =
        canonical::CanonicalTickValidityBitV1(
            canonical::CanonicalTickValidityV1::kPrice) |
        canonical::CanonicalTickValidityBitV1(
            canonical::CanonicalTickValidityV1::kQuantity) |
        canonical::CanonicalTickValidityBitV1(
            canonical::CanonicalTickValidityV1::kPrimaryOrderId) |
        canonical::CanonicalTickValidityBitV1(
            canonical::CanonicalTickValidityV1::kSide) |
        canonical::CanonicalTickValidityBitV1(
            canonical::CanonicalTickValidityV1::kOrderType);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(sz_order) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "Shenzhen order origin and normalized raw-enum projection pass");
    sz_order.payload.side = canonical::CanonicalSideV1::kUnknown;
    sz_order.payload.validity_bitmap &=
        ~canonical::CanonicalTickValidityBitV1(
            canonical::CanonicalTickValidityV1::kSide);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(sz_order) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "known low-16 side code cannot disguise an unavailable raw int32");

    const auto tick_bit = [](canonical::CanonicalTickValidityV1 bit) {
        return canonical::CanonicalTickValidityBitV1(bit);
    };

    canonical::CanonicalTickRecordV1 sh_add{};
    sh_add.header = valid.header;
    sh_add.header.exchange_time_ns = 0;
    sh_add.payload.action = canonical::CanonicalTickActionV1::kAdd;
    sh_add.payload.quantity_native = 100;
    sh_add.payload.source_enum_bits = static_cast<std::uint64_t>('A');
    sh_add.payload.validity_bitmap = tick_bit(
        canonical::CanonicalTickValidityV1::kQuantity);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(sh_add) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "Shanghai add accepts the decoder's minimum producer projection");

    changed = sh_add;
    changed.payload.order_type = canonical::CanonicalOrderTypeV1::kLimit;
    changed.payload.validity_bitmap |= tick_bit(
        canonical::CanonicalTickValidityV1::kOrderType);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "Shanghai add cannot invent a Shenzhen-only order type");

    changed = sh_add;
    changed.payload.primary_order_id = 71;
    changed.payload.validity_bitmap |= tick_bit(
        canonical::CanonicalTickValidityV1::kPrimaryOrderId);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "Shanghai add cannot publish a primary ID without a decoded side");

    changed.payload.source_enum_bits =
        static_cast<std::uint64_t>('A') |
        (static_cast<std::uint64_t>('B') << 16U);
    changed.payload.side = canonical::CanonicalSideV1::kBuy;
    changed.payload.validity_bitmap |= tick_bit(
        canonical::CanonicalTickValidityV1::kSide);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "Shanghai add may publish the positive primary ID selected by B/S");

    canonical::CanonicalTickRecordV1 sh_cancel = sh_add;
    sh_cancel.payload.action = canonical::CanonicalTickActionV1::kCancel;
    sh_cancel.payload.source_enum_bits = static_cast<std::uint64_t>('D');
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(sh_cancel) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "Shanghai cancel accepts the decoder's minimum producer projection");
    changed = sh_cancel;
    changed.payload.buy_order_id = 71;
    changed.payload.validity_bitmap |= tick_bit(
        canonical::CanonicalTickValidityV1::kBuyOrderId);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "Shanghai cancel cannot publish transaction-style buy/sell IDs");

    sz_order.payload.side = canonical::CanonicalSideV1::kBuy;
    sz_order.payload.validity_bitmap |= tick_bit(
        canonical::CanonicalTickValidityV1::kSide);
    changed = sz_order;
    changed.payload.phase = canonical::CanonicalTradingPhaseV1::kContinuous;
    changed.payload.validity_bitmap |= tick_bit(
        canonical::CanonicalTickValidityV1::kPhase);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "Shenzhen order cannot carry Shanghai phase attribution");

    canonical::CanonicalTickRecordV1 sz_market = sz_order;
    sz_market.payload.source_enum_bits = 49U | (49ULL << 16U);
    sz_market.payload.order_type = canonical::CanonicalOrderTypeV1::kMarket;
    sz_market.payload.price_p6 = 0;
    sz_market.payload.validity_bitmap &= ~tick_bit(
        canonical::CanonicalTickValidityV1::kPrice);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(sz_market) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "Shenzhen market order preserves price as invalid");
    changed = sz_market;
    changed.payload.price_p6 = 10'000'000;
    changed.payload.validity_bitmap |= tick_bit(
        canonical::CanonicalTickValidityV1::kPrice);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "Shenzhen non-limit order cannot advertise a factor-safe price");

    canonical::CanonicalTickRecordV1 sz_trade{};
    sz_trade.header = valid.header;
    sz_trade.header.origin_service_id = 6U;
    sz_trade.header.origin_message_id = 36U;
    sz_trade.header.market = canonical::CanonicalMarketV1::kShenzhen;
    sz_trade.header.channel = 7U;
    sz_trade.header.exchange_time_ns = 0;
    sz_trade.payload.action = canonical::CanonicalTickActionV1::kTrade;
    sz_trade.payload.quantity_native = 100;
    sz_trade.payload.source_enum_bits = 70U;
    sz_trade.payload.validity_bitmap = tick_bit(
        canonical::CanonicalTickValidityV1::kQuantity);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(sz_trade) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "Shenzhen trade accepts the decoder's minimum producer projection");
    changed = sz_trade;
    changed.payload.trade_amount_p6 = 1'000'000;
    changed.payload.validity_bitmap |= tick_bit(
        canonical::CanonicalTickValidityV1::kTradeAmount);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "Shenzhen trade cannot invent Shanghai trade-amount semantics");

    canonical::CanonicalTickRecordV1 sz_cancel = sz_trade;
    sz_cancel.payload.action = canonical::CanonicalTickActionV1::kCancel;
    sz_cancel.payload.source_enum_bits = 52U;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(sz_cancel) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "Shenzhen cancel with no positive order references remains ambiguous");

    canonical::CanonicalTickRecordV1 sz_cancel_buy = sz_cancel;
    sz_cancel_buy.payload.buy_order_id = 71;
    sz_cancel_buy.payload.primary_order_id = 71;
    sz_cancel_buy.payload.side = canonical::CanonicalSideV1::kBuy;
    sz_cancel_buy.payload.validity_bitmap |=
        tick_bit(canonical::CanonicalTickValidityV1::kBuyOrderId) |
        tick_bit(canonical::CanonicalTickValidityV1::kPrimaryOrderId) |
        tick_bit(canonical::CanonicalTickValidityV1::kSide);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(sz_cancel_buy) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "Shenzhen cancel infers primary ID and side from exactly one reference");
    changed = sz_cancel_buy;
    changed.payload.primary_order_id = 72;
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "Shenzhen cancel primary ID must equal its sole positive reference");
    changed = sz_cancel_buy;
    changed.payload.primary_order_id = 0;
    changed.payload.side = canonical::CanonicalSideV1::kUnknown;
    changed.payload.validity_bitmap &=
        ~tick_bit(canonical::CanonicalTickValidityV1::kPrimaryOrderId);
    changed.payload.validity_bitmap &=
        ~tick_bit(canonical::CanonicalTickValidityV1::kSide);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "Shenzhen cancel cannot omit inference for one positive reference");

    canonical::CanonicalTickRecordV1 sz_cancel_both = sz_cancel;
    sz_cancel_both.payload.buy_order_id = 71;
    sz_cancel_both.payload.sell_order_id = 72;
    sz_cancel_both.payload.validity_bitmap |=
        tick_bit(canonical::CanonicalTickValidityV1::kBuyOrderId) |
        tick_bit(canonical::CanonicalTickValidityV1::kSellOrderId);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(sz_cancel_both) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "Shenzhen cancel with two references keeps primary ID and side unknown");
    changed = sz_cancel;
    changed.payload.phase = canonical::CanonicalTradingPhaseV1::kContinuous;
    changed.payload.validity_bitmap |= tick_bit(
        canonical::CanonicalTickValidityV1::kPhase);
    test->Expect(
        canonical::ValidateCanonicalTickRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "Shenzhen cancel cannot carry Shanghai phase attribution");
}

void TestSnapshotValidation(TestContext* test) {
    const canonical::CanonicalSnapshotRecordV1 valid = MakeValidSnapshot();
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(valid) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "valid depth-truncated snapshot passes");

    canonical::CanonicalSnapshotRecordV1 changed = valid;
    changed.payload.snapshot_flags = 0U;
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::
                kInconsistentSnapshotDepth,
        "depth above ten requires the Phase-5-local truncation flag");
    changed = valid;
    changed.payload.bid1_revealed_count = 51U;
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kSnapshotQueueTooLong,
        "queue longer than fifty is rejected, not Canonical-truncated");
    changed = valid;
    changed.payload.reserved[519] = std::byte{1U};
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kNonzeroReserved,
        "snapshot reserved tail must remain zero");
    changed = valid;
    changed.payload.bid_price_validity = 0x0400U;
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kUnknownValidityBits,
        "snapshot level bits above ten are rejected");
    changed = valid;
    changed.payload.high_limit_price_p6 = 88;
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::
                kInconsistentFieldValidity,
        "unknown limit semantics cannot carry a factor-safe numeric value");
    changed = valid;
    changed.payload.volume_native = -1;
    changed.payload.scalar_validity |=
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::kVolumeNative);
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "a valid snapshot scalar quantity cannot be negative");
    changed = valid;
    changed.payload.last_price_p6 = -1;
    changed.payload.scalar_validity |=
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::kLastPrice);
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "a valid snapshot scalar price cannot be negative");
    changed = valid;
    changed.payload.turnover_p6 = -1;
    changed.payload.scalar_validity |=
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::kTurnoverP6);
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "a valid snapshot turnover cannot be negative");
    changed = valid;
    changed.payload.trade_count = -1;
    changed.payload.scalar_validity |=
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::kTradeCount);
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "a valid snapshot trade count cannot be negative");
    changed = valid;
    changed.payload.bid_price_p6[0] = -1;
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "a valid depth price cannot be negative");
    changed = valid;
    changed.payload.bid_quantity_native[0] = -1;
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "a valid depth quantity cannot be negative");
    changed = valid;
    changed.payload.ask1_queue_quantity_native[0] = -1;
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "a valid queue quantity cannot be negative");

    changed = valid;
    changed.header.origin_message_id = 24U;
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidOriginCursor,
        "snapshot cannot claim a tick origin tuple");
    changed = valid;
    changed.header.exchange_sequence = 1U;
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "snapshot exchange sequence is exactly zero");
    changed = valid;
    changed.payload.bid1_total_order_count = 1U;
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidCounts,
        "revealed queue count cannot exceed total order count");
    changed = valid;
    changed.payload.bid1_total_order_count = 3U;
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidCounts,
        "best-level total order count matches level-zero order count");
    changed = valid;
    changed.payload.actual_bid_depth = 0U;
    changed.payload.snapshot_flags = 0U;
    changed.payload.bid_price_validity = 0U;
    changed.payload.bid_quantity_validity = 0U;
    changed.payload.bid_order_count_validity = 0U;
    changed.payload.bid_price_p6 = {};
    changed.payload.bid_quantity_native = {};
    changed.payload.bid_order_count = {};
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kInvalidCounts,
        "zero depth cannot carry best-level counts or queue entries");
    changed = valid;
    changed.payload.scalar_validity &=
        ~canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::
                kBid1TotalOrderCount);
    changed.payload.bid1_total_order_count = 0U;
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::
                kInconsistentFieldValidity,
        "snapshot count validity is explicit even for zero counts");

    changed = valid;
    changed.payload.bid_queue_validity = 0x1U;
    changed.payload.bid1_queue_quantity_native[1] = 0;
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "Shanghai nullable queue quantities may leave holes in validity");
    changed.header.origin_service_id = 6U;
    changed.header.origin_message_id = 28U;
    changed.header.market = canonical::CanonicalMarketV1::kShenzhen;
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::
                kInconsistentFieldValidity,
        "Shenzhen non-null queue projection requires the full revealed prefix");
    changed = valid;
    changed.header.origin_service_id = 6U;
    changed.header.origin_message_id = 28U;
    changed.header.market = canonical::CanonicalMarketV1::kShenzhen;
    changed.header.channel = 0U;
    test->Expect(
        canonical::ValidateCanonicalSnapshotRecordV1(changed) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "Shenzhen snapshot channel zero remains an opaque valid code");
}

void TestQualityAndControlValidation(TestContext* test) {
    canonical::CanonicalQualityRecordV1 quality = MakeValidQuality();
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "valid quality record passes");
    quality.payload.first_bad_origin_wal_end_pos = 0U;
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kInvalidOriginCursor,
        "quality first-bad WAL cursor cannot be zero");
    quality = MakeValidQuality();
    quality.payload.related_connection_epoch += 1U;
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "quality related epoch must equal the header epoch");
    quality = MakeValidQuality();
    quality.header.channel = 0U;
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "changing a channel without changing its encoded scope id is rejected");
    quality.payload.scope_id = canonical::CanonicalChannelScopeIdV1(
        quality.header.market, quality.header.channel);
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "opaque upstream channel zero has one tagged Canonical scope id");
    quality.header.market = canonical::CanonicalMarketV1::kUnknown;
    quality.payload.scope_id = 0U;
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "channel scope cannot disguise an unknown market as scope zero");
    quality = MakeValidQuality();
    quality.payload.expected_sequence = quality.payload.actual_sequence;
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "gap quality requires expected sequence below actual sequence");
    quality = MakeValidQuality();
    quality.header.quality_flags = 0U;
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "typed sequence quality carries its required frozen quality bit");
    quality = MakeValidQuality();
    quality.payload.payload_sha256 = {};
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kHashPresenceMismatch,
        "sequence diagnostics retain their exact payload fingerprint");
    quality = MakeValidQuality();
    quality.payload.human_code_id = 1U;
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "unassigned human code IDs remain zero in V1");
    quality = MakeValidQuality();
    quality.payload.quality_type =
        canonical::CanonicalQualityTypeV1::kVendorSequenceConflict;
    SetVendorQualityScope(&quality);
    quality.payload.detail_code = 5U;
    quality.payload.expected_sequence = quality.payload.actual_sequence;
    quality.header.quality_flags = control::QualityBit(
        control::QualityFlagV1::kVendorSequenceConflict);
    quality.payload.payload_sha256 = {};
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kHashPresenceMismatch,
        "duplicate conflict requires a full payload SHA-256");
    quality.payload.payload_sha256[31] = std::byte{1U};
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "conflict quality accepts a nonzero full payload SHA-256");
    quality = MakeValidQuality();
    quality.payload.quality_type =
        canonical::CanonicalQualityTypeV1::kSequenceCapacityExhausted;
    SetVendorQualityScope(&quality);
    quality.payload.detail_code = 8U;
    quality.header.quality_flags = control::QualityBit(
        control::QualityFlagV1::kVendorSequenceConflict);
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "sequence capacity exhaustion has a distinct quality type");
    quality = MakeValidQuality();
    quality.payload.quality_type =
        canonical::CanonicalQualityTypeV1::kExchangeSequenceDuplicate;
    quality.payload.detail_code = 4U;
    quality.payload.expected_sequence = quality.payload.actual_sequence;
    quality.payload.payload_sha256 = {};
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kHashPresenceMismatch,
        "exchange duplicate requires its exact payload fingerprint");
    quality.payload.payload_sha256[0] = std::byte{2U};
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "exchange duplicate is not mislabeled as a vendor duplicate");
    quality.payload.scope_type =
        canonical::CanonicalQualityScopeV1::kVendorMessage;
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "exchange quality cannot claim a vendor-message scope");
    quality = MakeValidQuality();
    quality.payload.quality_type =
        canonical::CanonicalQualityTypeV1::kScopePoisoned;
    quality.payload.detail_code = 7U;
    quality.payload.payload_sha256 = {};
    quality.header.quality_flags = control::QualityBit(
        control::QualityFlagV1::kExchangeSequenceConflict);
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "poisoned scope uses sticky provenance without a current payload hash");
    quality.payload.payload_sha256[0] = std::byte{1U};
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kHashPresenceMismatch,
        "poisoned scope cannot mislabel a current payload as first-bad evidence");
    quality = MakeValidQuality();
    quality.payload.quality_type =
        canonical::CanonicalQualityTypeV1::kSnapshotRejected;
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "snapshot rejection requires the snapshot-family scope");
    quality.payload.scope_type =
        canonical::CanonicalQualityScopeV1::kSnapshotFamily;
    quality.header.market = canonical::CanonicalMarketV1::kUnknown;
    quality.payload.scope_id = 0U;
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "snapshot-family scope cannot use unknown market and scope zero");
    quality = MakeValidQuality();
    quality.header.instrument_id = 0U;
    quality.header.sub_index = 3U;
    quality.header.quality_flags = control::QualityBit(
        control::QualityFlagV1::kInstrumentUnknown);
    quality.payload.quality_type =
        canonical::CanonicalQualityTypeV1::kInstrumentUnknown;
    quality.payload.scope_type =
        canonical::CanonicalQualityScopeV1::kInstrument;
    quality.payload.scope_id = 0U;
    quality.payload.expected_sequence = 0U;
    quality.payload.actual_sequence = 0U;
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "unknown instrument may use an external-key scope without an id");
    quality = MakeValidQuality();
    quality.payload.scope_id = quality.header.channel;
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "channel scope rejects an ambiguous bare-channel scope id");
    quality = MakeValidQuality();
    quality.payload.quality_type =
        canonical::CanonicalQualityTypeV1::kSourceState;
    quality.payload.scope_type =
        canonical::CanonicalQualityScopeV1::kStream;
    quality.payload.scope_id = canonical::CanonicalStreamScopeIdV1(
        quality.header.source_stream_id);
    quality.header.market = canonical::CanonicalMarketV1::kUnknown;
    quality.header.channel = 0U;
    quality.header.exchange_sequence = 0U;
    quality.payload.payload_sha256 = {};
    test->Expect(
        canonical::ValidateCanonicalQualityRecordV1(quality) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "producer-less source-state quality is rejected in frozen V1");

    const canonical::CanonicalControlRecordV1 valid_control =
        MakeValidControl();
    test->Expect(
        canonical::ValidateCanonicalControlRecordV1(valid_control) ==
            canonical::CanonicalValidationErrorV1::kNone,
        "valid Canonical control record passes");
    canonical::CanonicalControlRecordV1 control = valid_control;
    control.header.quality_flags = 0U;
    test->Expect(
        canonical::ValidateCanonicalControlRecordV1(control) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "connecting control requires session-unknown quality");
    control = valid_control;
    control.header.vendor_sequence_id = 1U;
    test->Expect(
        canonical::ValidateCanonicalControlRecordV1(control) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "control cannot invent a vendor sequence ID");
    control = valid_control;
    control.header.sub_index = 1U;
    test->Expect(
        canonical::ValidateCanonicalControlRecordV1(control) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "control occupies sub-index zero");
    control = valid_control;
    control.payload.address_sha256 = {};
    test->Expect(
        canonical::ValidateCanonicalControlRecordV1(control) ==
            canonical::CanonicalValidationErrorV1::kHashPresenceMismatch,
        "control hash presence flag must match digest bytes");
    control = valid_control;
    control.payload.required_count = 1U;
    control.payload.required_failed_count = 2U;
    control.payload.flags |= canonical::CanonicalControlFlagBitV1(
        canonical::CanonicalControlFlagV1::kRequiredFailure);
    test->Expect(
        canonical::ValidateCanonicalControlRecordV1(control) ==
            canonical::CanonicalValidationErrorV1::kInvalidCounts,
        "control response counts are bounded without overflow");
    control = valid_control;
    control.payload.connection_epoch += 1U;
    test->Expect(
        canonical::ValidateCanonicalControlRecordV1(control) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "control payload epoch must match Canonical header epoch");
    control = valid_control;
    control.header.origin_message_id += 1U;
    test->Expect(
        canonical::ValidateCanonicalControlRecordV1(control) ==
            canonical::CanonicalValidationErrorV1::kInvalidFieldValue,
        "control type must match its exact Phase-3 origin identity");
}

}  // namespace

int main() {
    TestContext test;
    TestClockEpochIdentity(&test);
    TestDescriptorHashes(&test);
    TestTickValidationAndGolden(&test);
    TestSnapshotValidation(&test);
    TestQualityAndControlValidation(&test);
    if (test.failures() == 0) {
        std::cout << "phase5 canonical schema tests passed\n";
    }
    return test.failures() == 0 ? 0 : 1;
}
