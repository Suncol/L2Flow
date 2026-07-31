#include "l2flow/control/quality_flags_v1.h"
#include "l2flow/market/market_decoder.h"
#include "l2flow/market/market_types_v1.h"

#include <algorithm>
#include <array>
#include <barrier>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

namespace control = l2flow::control;
namespace market = l2flow::market;

namespace {

constexpr std::uint32_t kTradeDate = 20260722U;
constexpr std::uint32_t kSourceStreamId = 404U;
constexpr std::uint16_t kSupportedVersion = 101U;
constexpr std::uint8_t kShanghaiService = 4U;
constexpr std::uint8_t kShenzhenService = 6U;
constexpr std::uint16_t kShanghaiSnapshotMessage = 4U;
constexpr std::uint16_t kShanghaiTickMessage = 24U;
constexpr std::uint16_t kShenzhenSnapshotMessage = 28U;
constexpr std::uint16_t kShenzhenOrderMessage = 33U;
constexpr std::uint16_t kShenzhenTransactionMessage = 36U;
constexpr std::int64_t kUnresolvedHighLimitRaw =
    std::numeric_limits<std::int64_t>::max();
constexpr std::int64_t kUnresolvedLowLimitRaw =
    std::numeric_limits<std::int64_t>::min() + 1;

// These are deliberately frozen test-oracle ABI values, rather than sizeof
// or offsetof expressions over the vendor structs.
namespace wire_abi {

constexpr std::size_t kStringDescriptorBytes = 6U;
constexpr std::size_t kListDescriptorBytes = 8U;

namespace sh_snapshot {
constexpr std::size_t kFixedBytes = 248U;
constexpr std::size_t kUpdateTime = 0U;
constexpr std::size_t kSecurityId = 4U;
constexpr std::size_t kImageStatus = 10U;
constexpr std::size_t kPreClosePrice = 14U;
constexpr std::size_t kOpenPrice = 18U;
constexpr std::size_t kHighPrice = 22U;
constexpr std::size_t kLowPrice = 26U;
constexpr std::size_t kLastPrice = 30U;
constexpr std::size_t kClosePrice = 34U;
constexpr std::size_t kInstrumentStatus = 38U;
constexpr std::size_t kTradeCount = 44U;
constexpr std::size_t kTradeVolume = 48U;
constexpr std::size_t kTurnover = 56U;
constexpr std::size_t kWeightedAverageBidPrice = 72U;
constexpr std::size_t kAlternateWeightedAverageBidPrice = 76U;
constexpr std::size_t kWeightedAverageAskPrice = 88U;
constexpr std::size_t kAlternateWeightedAverageAskPrice = 92U;
constexpr std::size_t kEtfBuyCount = 96U;
constexpr std::size_t kEtfBuyQuantity = 100U;
constexpr std::size_t kEtfBuyAmount = 108U;
constexpr std::size_t kEtfSellCount = 116U;
constexpr std::size_t kEtfSellQuantity = 120U;
constexpr std::size_t kEtfSellAmount = 128U;
constexpr std::size_t kYieldToMaturity = 136U;
constexpr std::size_t kTotalWarrantExerciseQuantity = 140U;
constexpr std::size_t kVendorWarLower = 148U;
constexpr std::size_t kVendorWarUpper = 156U;
constexpr std::size_t kMaximumBidDuration = 212U;
constexpr std::size_t kMaximumAskDuration = 216U;
constexpr std::size_t kBidCount = 220U;
constexpr std::size_t kAskCount = 224U;
constexpr std::size_t kBidLevels = 228U;
constexpr std::size_t kAskLevels = 236U;
constexpr std::size_t kIopv = 244U;

constexpr std::size_t kLevelBytes = 28U;
constexpr std::size_t kLevelPrice = 4U;
constexpr std::size_t kLevelVolume = 8U;
constexpr std::size_t kLevelOrderCount = 16U;
constexpr std::size_t kLevelQueue = 20U;
constexpr std::size_t kQueueItemBytes = 16U;
constexpr std::size_t kQueueItemQuantity = 8U;
}  // namespace sh_snapshot

namespace sh_tick {
constexpr std::size_t kFixedBytes = 70U;
constexpr std::size_t kBusinessIndex = 0U;
constexpr std::size_t kChannel = 8U;
constexpr std::size_t kSecurityId = 12U;
constexpr std::size_t kTickTime = 18U;
constexpr std::size_t kType = 22U;
constexpr std::size_t kBuyOrderId = 28U;
constexpr std::size_t kSellOrderId = 36U;
constexpr std::size_t kPrice = 44U;
constexpr std::size_t kQuantity = 48U;
constexpr std::size_t kTradeMoney = 56U;
constexpr std::size_t kTickFlag = 64U;
}  // namespace sh_tick

namespace sz_snapshot {
constexpr std::size_t kFixedBytes = 224U;
constexpr std::size_t kUpdateTime = 0U;
constexpr std::size_t kChannel = 4U;
constexpr std::size_t kMdStreamId = 8U;
constexpr std::size_t kSecurityId = 14U;
constexpr std::size_t kSecurityIdSource = 20U;
constexpr std::size_t kTradingPhase = 26U;
constexpr std::size_t kPreClosePrice = 32U;
constexpr std::size_t kTradeCount = 40U;
constexpr std::size_t kVolume = 48U;
constexpr std::size_t kTurnover = 56U;
constexpr std::size_t kLastPrice = 64U;
constexpr std::size_t kOpenPrice = 72U;
constexpr std::size_t kHighPrice = 80U;
constexpr std::size_t kLowPrice = 88U;
constexpr std::size_t kPeRatio1 = 112U;
constexpr std::size_t kPeRatio2 = 120U;
constexpr std::size_t kPreCloseIopv = 128U;
constexpr std::size_t kIopv = 136U;
constexpr std::size_t kWeightedAverageAskPrice = 152U;
constexpr std::size_t kWeightedAverageBidPrice = 168U;
constexpr std::size_t kHighLimitPrice = 176U;
constexpr std::size_t kLowLimitPrice = 184U;
constexpr std::size_t kOpenInterest = 192U;
constexpr std::size_t kVendorOptPremiumRatio = 200U;
constexpr std::size_t kBidLevels = 208U;
constexpr std::size_t kAskLevels = 216U;

constexpr std::size_t kLevelBytes = 28U;
constexpr std::size_t kLevelVolume = 0U;
constexpr std::size_t kLevelPrice = 8U;
constexpr std::size_t kLevelOrderCount = 16U;
constexpr std::size_t kLevelQueue = 20U;
constexpr std::size_t kQueueItemBytes = 8U;
}  // namespace sz_snapshot

namespace sz_order {
constexpr std::size_t kFixedBytes = 58U;
constexpr std::size_t kChannel = 0U;
constexpr std::size_t kApplicationSequence = 4U;
constexpr std::size_t kMdStreamId = 12U;
constexpr std::size_t kSecurityId = 18U;
constexpr std::size_t kSecurityIdSource = 24U;
constexpr std::size_t kPrice = 30U;
constexpr std::size_t kQuantity = 38U;
constexpr std::size_t kSide = 46U;
constexpr std::size_t kTransactTime = 50U;
constexpr std::size_t kOrderType = 54U;
}  // namespace sz_order

namespace sz_transaction {
constexpr std::size_t kFixedBytes = 70U;
constexpr std::size_t kChannel = 0U;
constexpr std::size_t kApplicationSequence = 4U;
constexpr std::size_t kMdStreamId = 12U;
constexpr std::size_t kBidApplicationSequence = 18U;
constexpr std::size_t kOfferApplicationSequence = 26U;
constexpr std::size_t kSecurityId = 34U;
constexpr std::size_t kSecurityIdSource = 40U;
constexpr std::size_t kLastPrice = 46U;
constexpr std::size_t kLastQuantity = 54U;
constexpr std::size_t kExecutionType = 62U;
constexpr std::size_t kTransactTime = 66U;
}  // namespace sz_transaction

}  // namespace wire_abi

struct TestContext final {
    void Expect(bool condition, const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    int failures = 0;
};

// Independent little-endian byte writer used only by this golden oracle.  In
// particular it does not reuse CheckedBodyView, fixed-point, time, or enum
// code from the decoder.
class WireWriter final {
public:
    explicit WireWriter(std::size_t fixed_bytes)
        : bytes_(fixed_bytes, std::byte{0U}) {}

    void StoreU16(std::size_t offset, std::uint16_t value) {
        bytes_.at(offset) =
            static_cast<std::byte>(value & 0xffU);
        bytes_.at(offset + 1U) = static_cast<std::byte>(
            (value >> 8U) & 0xffU);
    }

    void StoreU32(std::size_t offset, std::uint32_t value) {
        for (std::size_t index = 0U; index < 4U; ++index) {
            const unsigned int shift =
                static_cast<unsigned int>(index * 8U);
            bytes_.at(offset + index) = static_cast<std::byte>(
                (value >> shift) & 0xffU);
        }
    }

    void StoreI32(std::size_t offset, std::int32_t value) {
        StoreU32(offset, static_cast<std::uint32_t>(value));
    }

    void StoreU64(std::size_t offset, std::uint64_t value) {
        for (std::size_t index = 0U; index < 8U; ++index) {
            const unsigned int shift =
                static_cast<unsigned int>(index * 8U);
            bytes_.at(offset + index) = static_cast<std::byte>(
                (value >> shift) & 0xffU);
        }
    }

    void StoreI64(std::size_t offset, std::int64_t value) {
        StoreU64(offset, static_cast<std::uint64_t>(value));
    }

    std::size_t AppendZeroes(std::size_t count) {
        const std::size_t start = bytes_.size();
        bytes_.resize(start + count, std::byte{0U});
        return start;
    }

    void StoreString(
        std::size_t descriptor,
        std::string_view value) {
        const std::size_t start = AppendZeroes(value.size());
        for (std::size_t index = 0U; index < value.size(); ++index) {
            bytes_.at(start + index) = static_cast<std::byte>(
                static_cast<unsigned char>(value[index]));
        }
        StoreU16(
            descriptor,
            static_cast<std::uint16_t>(value.size()));
        StoreU32(
            descriptor + 2U,
            static_cast<std::uint32_t>(start - descriptor));
    }

    std::size_t BeginList(
        std::size_t descriptor,
        std::uint32_t count,
        std::size_t item_bytes) {
        const std::size_t start =
            AppendZeroes(static_cast<std::size_t>(count) * item_bytes);
        StoreU32(descriptor, count);
        StoreU32(
            descriptor + 4U,
            static_cast<std::uint32_t>(start - descriptor));
        return start;
    }

    std::vector<std::byte> Take() && {
        return std::move(bytes_);
    }

private:
    std::vector<std::byte> bytes_;
};

void OverwriteU32(
    std::vector<std::byte>* bytes,
    std::size_t offset,
    std::uint32_t value) {
    for (std::size_t index = 0U; index < 4U; ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        bytes->at(offset + index) = static_cast<std::byte>(
            (value >> shift) & 0xffU);
    }
}

void OverwriteI32(
    std::vector<std::byte>* bytes,
    std::size_t offset,
    std::int32_t value) {
    OverwriteU32(bytes, offset, static_cast<std::uint32_t>(value));
}

void OverwriteI64(
    std::vector<std::byte>* bytes,
    std::size_t offset,
    std::int64_t value) {
    const std::uint64_t raw = static_cast<std::uint64_t>(value);
    for (std::size_t index = 0U; index < 8U; ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        bytes->at(offset + index) = static_cast<std::byte>(
            (raw >> shift) & 0xffU);
    }
}

struct ShanghaiTickSpec final {
    std::int64_t business_index = 101;
    std::int32_t channel = 7;
    std::string security_id = "600000";
    std::uint32_t time = 93000123U;
    std::string type = "A";
    std::int64_t buy_order_id = 1101;
    std::int64_t sell_order_id = 2202;
    std::int32_t price_p3 = 12345;
    std::int64_t quantity = 99;
    std::int64_t trade_money_p3 = 7000;
    std::string tick_flag = "B";
};

std::vector<std::byte> MakeShanghaiTickWire(
    const ShanghaiTickSpec& spec) {
    using namespace wire_abi::sh_tick;
    WireWriter writer(kFixedBytes);
    writer.StoreI64(kBusinessIndex, spec.business_index);
    writer.StoreI32(kChannel, spec.channel);
    writer.StoreU32(kTickTime, spec.time);
    writer.StoreI64(kBuyOrderId, spec.buy_order_id);
    writer.StoreI64(kSellOrderId, spec.sell_order_id);
    writer.StoreI32(kPrice, spec.price_p3);
    writer.StoreI64(kQuantity, spec.quantity);
    writer.StoreI64(kTradeMoney, spec.trade_money_p3);
    writer.StoreString(kSecurityId, spec.security_id);
    writer.StoreString(kType, spec.type);
    writer.StoreString(kTickFlag, spec.tick_flag);
    return std::move(writer).Take();
}

struct ShenzhenOrderSpec final {
    std::uint32_t channel = 12U;
    std::int64_t application_sequence = 9001;
    std::string md_stream_id = "010";
    std::string security_id = "000001";
    std::string security_id_source = "102";
    std::int64_t price_p4 = 123456;
    std::int64_t quantity = 700;
    std::int32_t side = 49;
    std::uint32_t time = 93000123U;
    std::int32_t order_type = 50;
};

std::vector<std::byte> MakeShenzhenOrderWire(
    const ShenzhenOrderSpec& spec) {
    using namespace wire_abi::sz_order;
    WireWriter writer(kFixedBytes);
    writer.StoreU32(kChannel, spec.channel);
    writer.StoreI64(kApplicationSequence, spec.application_sequence);
    writer.StoreI64(kPrice, spec.price_p4);
    writer.StoreI64(kQuantity, spec.quantity);
    writer.StoreI32(kSide, spec.side);
    writer.StoreU32(kTransactTime, spec.time);
    writer.StoreI32(kOrderType, spec.order_type);
    writer.StoreString(kMdStreamId, spec.md_stream_id);
    writer.StoreString(kSecurityId, spec.security_id);
    writer.StoreString(kSecurityIdSource, spec.security_id_source);
    return std::move(writer).Take();
}

struct ShenzhenTransactionSpec final {
    std::uint32_t channel = 12U;
    std::int64_t application_sequence = 9101;
    std::string md_stream_id = "010";
    std::int64_t bid_application_sequence = 8101;
    std::int64_t offer_application_sequence = 8202;
    std::string security_id = "000001";
    std::string security_id_source = "102";
    std::int64_t last_price_p4 = 123456;
    std::int64_t last_quantity = 50;
    std::int32_t execution_type = 70;
    std::uint32_t time = 93000123U;
};

std::vector<std::byte> MakeShenzhenTransactionWire(
    const ShenzhenTransactionSpec& spec) {
    using namespace wire_abi::sz_transaction;
    WireWriter writer(kFixedBytes);
    writer.StoreU32(kChannel, spec.channel);
    writer.StoreI64(kApplicationSequence, spec.application_sequence);
    writer.StoreI64(
        kBidApplicationSequence,
        spec.bid_application_sequence);
    writer.StoreI64(
        kOfferApplicationSequence,
        spec.offer_application_sequence);
    writer.StoreI64(kLastPrice, spec.last_price_p4);
    writer.StoreI64(kLastQuantity, spec.last_quantity);
    writer.StoreI32(kExecutionType, spec.execution_type);
    writer.StoreU32(kTransactTime, spec.time);
    writer.StoreString(kMdStreamId, spec.md_stream_id);
    writer.StoreString(kSecurityId, spec.security_id);
    writer.StoreString(
        kSecurityIdSource,
        spec.security_id_source);
    return std::move(writer).Take();
}

std::vector<std::byte> MakeShanghaiSnapshotWire() {
    using namespace wire_abi::sh_snapshot;
    WireWriter writer(kFixedBytes);
    writer.StoreU32(kUpdateTime, 93100123U);
    writer.StoreI32(kImageStatus, 3);
    writer.StoreI32(kPreClosePrice, 10000);
    writer.StoreI32(kOpenPrice, 10100);
    writer.StoreI32(kHighPrice, 10300);
    writer.StoreI32(kLowPrice, 9900);
    writer.StoreI32(kLastPrice, 10200);
    writer.StoreI32(kClosePrice, 10250);
    writer.StoreU32(kTradeCount, 42U);
    writer.StoreI64(kTradeVolume, 123000);
    writer.StoreI64(kTurnover, 45600000);
    writer.StoreI32(kWeightedAverageBidPrice, 10190);
    writer.StoreI32(kAlternateWeightedAverageBidPrice, 10180);
    writer.StoreI32(kWeightedAverageAskPrice, 10210);
    writer.StoreI32(kAlternateWeightedAverageAskPrice, 10220);
    writer.StoreU32(kEtfBuyCount, 17U);
    writer.StoreI64(kEtfBuyQuantity, 21000);
    writer.StoreI64(kEtfBuyAmount, 3'210'000);
    writer.StoreU32(kEtfSellCount, 19U);
    writer.StoreI64(kEtfSellQuantity, 23000);
    writer.StoreI64(kEtfSellAmount, 4'560'000);
    writer.StoreI32(kYieldToMaturity, -125);
    writer.StoreI64(kTotalWarrantExerciseQuantity, 7000);
    writer.StoreI64(kVendorWarLower, 123456);
    writer.StoreI64(kVendorWarUpper, 654321);
    writer.StoreU32(kMaximumBidDuration, 0U);
    writer.StoreU32(
        kMaximumAskDuration,
        std::numeric_limits<std::uint32_t>::max() - 1U);
    // Production continuous-trading messages prove these counters are not
    // the MDLListT lengths.  Keep them deliberately different from the
    // 12/1 dynamic level counts below.
    writer.StoreU32(kBidCount, 43U);
    writer.StoreU32(kAskCount, 74U);
    writer.StoreI32(kIopv, 10123);
    writer.StoreString(kSecurityId, "600000");
    writer.StoreString(kInstrumentStatus, "TRADE");

    constexpr std::uint32_t kBidDepth = 12U;
    const std::size_t bids =
        writer.BeginList(kBidLevels, kBidDepth, kLevelBytes);
    for (std::uint32_t index = 0U; index < kBidDepth; ++index) {
        const std::size_t level =
            bids + static_cast<std::size_t>(index) * kLevelBytes;
        writer.StoreI32(
            level + kLevelPrice,
            10200 - static_cast<std::int32_t>(index));
        writer.StoreI64(
            level + kLevelVolume,
            1000 + static_cast<std::int64_t>(index) * 1000);
        writer.StoreU32(
            level + kLevelOrderCount,
            index == 0U ? 77U : 0U);
    }

    const std::size_t asks =
        writer.BeginList(kAskLevels, 1U, kLevelBytes);
    writer.StoreI32(asks + kLevelPrice, 10201);
    writer.StoreI64(asks + kLevelVolume, 2000);
    writer.StoreU32(asks + kLevelOrderCount, 2U);

    const std::size_t bid_queue = writer.BeginList(
        bids + kLevelQueue,
        52U,
        kQueueItemBytes);
    for (std::uint32_t index = 0U; index < 52U; ++index) {
        const std::size_t item =
            bid_queue +
            static_cast<std::size_t>(index) * kQueueItemBytes;
        writer.StoreI64(
            item + kQueueItemQuantity,
            static_cast<std::int64_t>(index + 1U) * 1000);
    }

    const std::size_t ask_queue = writer.BeginList(
        asks + kLevelQueue,
        2U,
        kQueueItemBytes);
    writer.StoreI64(ask_queue + kQueueItemQuantity, 3000);
    writer.StoreI64(
        ask_queue + kQueueItemBytes + kQueueItemQuantity,
        4000);
    return std::move(writer).Take();
}

std::vector<std::byte> MakeShenzhenSnapshotWire() {
    using namespace wire_abi::sz_snapshot;
    WireWriter writer(kFixedBytes);
    writer.StoreU32(kUpdateTime, 93200123U);
    writer.StoreU32(kChannel, 12U);
    writer.StoreI64(kPreClosePrice, 100000);
    writer.StoreI64(kTradeCount, 31);
    writer.StoreI64(kVolume, 2000);
    writer.StoreI64(kTurnover, 900000);
    writer.StoreI64(kLastPrice, 1012345);
    writer.StoreI64(kOpenPrice, 1'010'000);
    writer.StoreI64(kHighPrice, 1'030'000);
    writer.StoreI64(kLowPrice, 990'000);
    writer.StoreI64(kPeRatio1, -123456);
    writer.StoreI64(kPeRatio2, 654321);
    writer.StoreI64(kPreCloseIopv, 1'000'001);
    writer.StoreI64(kIopv, 1'000'002);
    writer.StoreI64(kWeightedAverageAskPrice, 1'012'350);
    writer.StoreI64(kWeightedAverageBidPrice, 1'012'340);
    writer.StoreI64(kHighLimitPrice, kUnresolvedHighLimitRaw);
    writer.StoreI64(kLowLimitPrice, kUnresolvedLowLimitRaw);
    writer.StoreI64(kOpenInterest, 77);
    writer.StoreI64(kVendorOptPremiumRatio, 88'000);
    writer.StoreString(kMdStreamId, "010");
    writer.StoreString(kSecurityId, "000001");
    writer.StoreString(kSecurityIdSource, "102");
    writer.StoreString(kTradingPhase, "T");

    const std::size_t bids =
        writer.BeginList(kBidLevels, 2U, kLevelBytes);
    writer.StoreI64(bids + kLevelVolume, 101);
    writer.StoreI64(bids + kLevelPrice, 1234567);
    writer.StoreU32(bids + kLevelOrderCount, 9U);
    const std::size_t bid2 = bids + kLevelBytes;
    writer.StoreI64(bid2 + kLevelVolume, 202);
    writer.StoreI64(bid2 + kLevelPrice, 1234560);
    writer.StoreU32(bid2 + kLevelOrderCount, 3U);

    const std::size_t asks =
        writer.BeginList(kAskLevels, 1U, kLevelBytes);
    writer.StoreI64(asks + kLevelVolume, 303);
    writer.StoreI64(asks + kLevelPrice, 1234570);
    writer.StoreU32(asks + kLevelOrderCount, 4U);

    const std::size_t bid_queue = writer.BeginList(
        bids + kLevelQueue,
        2U,
        kQueueItemBytes);
    writer.StoreI64(bid_queue, 111);
    writer.StoreI64(bid_queue + kQueueItemBytes, 222);
    const std::size_t ask_queue = writer.BeginList(
        asks + kLevelQueue,
        1U,
        kQueueItemBytes);
    writer.StoreI64(ask_queue, 333);
    return std::move(writer).Take();
}

market::MarketDecoderV1 MakeDecoder() {
    market::MarketDecoderConfigV1 config;
    config.trade_date = kTradeDate;
    config.source_stream_id = kSourceStreamId;
    return market::MarketDecoderV1(config);
}

market::MarketMessageViewV1 Message(
    std::uint8_t service_id,
    std::uint16_t message_id,
    std::span<const std::byte> body,
    std::uint64_t source_sequence,
    std::uint16_t service_version = kSupportedVersion,
    std::uint32_t vendor_local_time = 93000000U) {
    market::MarketMessageViewV1 input;
    input.source_stream_id = kSourceStreamId;
    input.trade_date = kTradeDate;
    input.source_sequence = source_sequence;
    input.service_id = service_id;
    input.service_version = service_version;
    input.message_id = message_id;
    input.message_encoding = 1U;
    input.vendor_local_time_raw = vendor_local_time;
    input.vendor_sequence_id = 100000U + source_sequence;
    input.recv_realtime_ns = 2000000 +
        static_cast<std::int64_t>(source_sequence);
    input.recv_monotonic_ns = 3000000 +
        static_cast<std::int64_t>(source_sequence);
    input.body = body;
    return input;
}

bool HasQuality(
    const market::DecodedMarketCommonV1& common,
    control::QualityFlagV1 flag) {
    return (common.quality_flags & control::QualityBit(flag)) != 0U;
}

bool HasNotice(
    const market::DecodedMarketCommonV1& common,
    market::MarketNoticeV1 notice) {
    return (common.market_notices & market::MarketNoticeBitV1(notice)) !=
           0U;
}

market::DecodedMarketEventV1 SentinelOutput() {
    market::ShenzhenOrderV1 sentinel;
    sentinel.channel = 0xdec0deU;
    sentinel.application_sequence = 0x12345678;
    sentinel.common.security_id = "sentinel";
    return sentinel;
}

bool IsSentinel(const market::DecodedMarketEventV1& event) {
    const auto* const value =
        std::get_if<market::ShenzhenOrderV1>(&event);
    return value != nullptr && value->channel == 0xdec0deU &&
           value->application_sequence == 0x12345678 &&
           value->common.security_id == "sentinel";
}

bool SameTime(
    const market::TimeValueV1& left,
    const market::TimeValueV1& right) {
    return left.raw_hhmmssmmm == right.raw_hhmmssmmm &&
           left.nanoseconds_since_midnight ==
               right.nanoseconds_since_midnight &&
           left.unix_nanoseconds == right.unix_nanoseconds &&
           left.valid == right.valid && left.is_null == right.is_null &&
           left.unix_nanoseconds_valid == right.unix_nanoseconds_valid;
}

bool SameDecimal(
    const market::DecimalValueV1& left,
    const market::DecimalValueV1& right) {
    return left.raw == right.raw &&
           left.normalized_p6 == right.normalized_p6 &&
           left.scale == right.scale &&
           left.valid == right.valid && left.is_null == right.is_null;
}

bool SameQuantity(
    const market::QuantityValueV1& left,
    const market::QuantityValueV1& right) {
    return left.raw == right.raw && left.scale == right.scale &&
           left.valid == right.valid && left.is_null == right.is_null;
}

bool SameOrigin(
    const market::MarketMessageViewV1& left,
    const market::MarketMessageViewV1& right) {
    return left.source_stream_id == right.source_stream_id &&
           left.trade_date == right.trade_date &&
           left.source_sequence == right.source_sequence &&
           left.service_id == right.service_id &&
           left.service_version == right.service_version &&
           left.message_id == right.message_id &&
           left.message_encoding == right.message_encoding &&
           left.vendor_local_time_raw == right.vendor_local_time_raw &&
           left.vendor_sequence_id == right.vendor_sequence_id &&
           left.recv_realtime_ns == right.recv_realtime_ns &&
           left.recv_monotonic_ns == right.recv_monotonic_ns &&
           left.body.empty() && right.body.empty();
}

bool SameCommon(
    const market::DecodedMarketCommonV1& left,
    const market::DecodedMarketCommonV1& right) {
    return left.kind == right.kind && left.market == right.market &&
           SameOrigin(left.origin, right.origin) &&
           SameTime(left.exchange_time, right.exchange_time) &&
           SameTime(left.vendor_local_time, right.vendor_local_time) &&
           left.security_id == right.security_id &&
           left.security_id_source == right.security_id_source &&
           left.md_stream_id == right.md_stream_id &&
           left.security_id_valid == right.security_id_valid &&
           left.security_id_source_valid ==
               right.security_id_source_valid &&
           left.md_stream_id_valid == right.md_stream_id_valid &&
           left.instrument_id == right.instrument_id &&
           left.ordinal == right.ordinal &&
           left.quantity_unit == right.quantity_unit &&
           left.security_type == right.security_type &&
           left.asset_scope == right.asset_scope &&
           left.quality_flags == right.quality_flags &&
           left.market_notices == right.market_notices;
}

bool SameTickFields(
    const market::TickFieldsV1& left,
    const market::TickFieldsV1& right) {
    return left.action == right.action && left.side == right.side &&
           left.order_type == right.order_type &&
           left.aggressor == right.aggressor &&
           left.phase == right.phase &&
           SameDecimal(left.price, right.price) &&
           SameQuantity(left.quantity, right.quantity) &&
           SameDecimal(left.trade_amount, right.trade_amount) &&
           SameQuantity(left.matched_quantity, right.matched_quantity) &&
           left.primary_order_id == right.primary_order_id &&
           left.buy_order_id == right.buy_order_id &&
           left.sell_order_id == right.sell_order_id &&
           left.validity_bitmap == right.validity_bitmap;
}

bool SameShanghaiTick(
    const market::DecodedMarketEventV1& left,
    const market::DecodedMarketEventV1& right) {
    const auto* const left_tick =
        std::get_if<market::ShanghaiTickV1>(&left);
    const auto* const right_tick =
        std::get_if<market::ShanghaiTickV1>(&right);
    return left_tick != nullptr && right_tick != nullptr &&
           SameCommon(left_tick->common, right_tick->common) &&
           left_tick->business_index == right_tick->business_index &&
           left_tick->channel == right_tick->channel &&
           left_tick->raw_type == right_tick->raw_type &&
           left_tick->raw_tick_flag == right_tick->raw_tick_flag &&
           left_tick->raw_type_valid == right_tick->raw_type_valid &&
           left_tick->raw_tick_flag_valid ==
               right_tick->raw_tick_flag_valid &&
           SameTickFields(left_tick->fields, right_tick->fields);
}

void TestFixedLowerBoundsAndSchemaGate(TestContext* context) {
    struct Case final {
        std::uint8_t service_id;
        std::uint16_t message_id;
        std::size_t fixed_bytes;
        std::size_t variant_index;
        const char* label;
    };
    const std::vector<Case> cases = {
        {kShanghaiService,
         kShanghaiSnapshotMessage,
         wire_abi::sh_snapshot::kFixedBytes,
         0U,
         "SH snapshot"},
        {kShanghaiService,
         kShanghaiTickMessage,
         wire_abi::sh_tick::kFixedBytes,
         1U,
         "SH tick"},
        {kShenzhenService,
         kShenzhenSnapshotMessage,
         wire_abi::sz_snapshot::kFixedBytes,
         2U,
         "SZ snapshot"},
        {kShenzhenService,
         kShenzhenOrderMessage,
         wire_abi::sz_order::kFixedBytes,
         3U,
         "SZ order"},
        {kShenzhenService,
         kShenzhenTransactionMessage,
         wire_abi::sz_transaction::kFixedBytes,
         4U,
         "SZ transaction"},
    };

    market::MarketDecoderV1 decoder = MakeDecoder();
    context->Expect(
        decoder.configuration_valid(),
        "valid market decoder configuration is accepted");
    std::uint64_t sequence = 1U;
    for (const Case& test_case : cases) {
        std::vector<std::byte> exact(
            test_case.fixed_bytes,
            std::byte{0U});
        market::DecodedMarketEventV1 decoded = SentinelOutput();
        const market::MarketDecodeErrorV1 exact_error = decoder.Decode(
            Message(
                test_case.service_id,
                test_case.message_id,
                exact,
                sequence++),
            &decoded);
        context->Expect(
            exact_error == market::MarketDecodeErrorV1::kNone,
            std::string(test_case.label) +
                " exact fixed lower bound decodes");
        context->Expect(
            decoded.index() == test_case.variant_index,
            std::string(test_case.label) +
                " selects the expected variant");
        if (exact_error == market::MarketDecodeErrorV1::kNone) {
            const market::DecodedMarketCommonV1& common =
                market::MarketCommonV1(decoded);
            context->Expect(
                common.origin.body.empty() &&
                    common.origin.body.data() == nullptr &&
                    common.ordinal ==
                        std::numeric_limits<std::size_t>::max(),
                std::string(test_case.label) +
                    " output retains neither body span nor registry route");
            context->Expect(
                HasQuality(
                    common,
                    control::QualityFlagV1::kInstrumentUnknown) &&
                    HasQuality(
                        common,
                        control::QualityFlagV1::kQtyUnitUnknown),
                std::string(test_case.label) +
                    " nullptr registry yields instrument/unit unknown quality");
        }

        std::vector<std::byte> truncated(
            test_case.fixed_bytes - 1U,
            std::byte{0U});
        market::DecodedMarketEventV1 unchanged = SentinelOutput();
        const market::MarketDecodeErrorV1 truncated_error =
            decoder.Decode(
                Message(
                    test_case.service_id,
                    test_case.message_id,
                    truncated,
                    sequence++),
                &unchanged);
        context->Expect(
            truncated_error == market::MarketDecodeErrorV1::kTruncated,
            std::string(test_case.label) +
                " rejects fixed lower bound minus one");
        context->Expect(
            IsSentinel(unchanged),
            std::string(test_case.label) +
                " failed decode leaves output unchanged");

        unchanged = SentinelOutput();
        const market::MarketDecodeErrorV1 version_error = decoder.Decode(
            Message(
                test_case.service_id,
                test_case.message_id,
                exact,
                sequence++,
                102U),
            &unchanged);
        context->Expect(
            version_error ==
                market::MarketDecodeErrorV1::kUnsupportedServiceVersion,
            std::string(test_case.label) +
                " rejects unknown service version");
        context->Expect(
            IsSentinel(unchanged),
            std::string(test_case.label) +
                " version failure leaves output unchanged");
    }
}

void TestShanghaiTickValidityMatrix(TestContext* context) {
    market::MarketDecoderV1 decoder = MakeDecoder();
    std::uint64_t sequence = 100U;

    ShanghaiTickSpec spec;
    spec.type = "A";
    spec.tick_flag = "B";
    spec.trade_money_p3 = 7000;
    std::vector<std::byte> body = MakeShanghaiTickWire(spec);
    market::DecodedMarketEventV1 event;
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH A divisible matched quantity decodes");
    const auto* tick = std::get_if<market::ShanghaiTickV1>(&event);
    context->Expect(tick != nullptr, "SH A produces ShanghaiTickV1");
    if (tick != nullptr) {
        const std::uint32_t expected =
            market::kTickPriceValidV1 |
            market::kTickQuantityValidV1 |
            market::kTickMatchedQuantityValidV1 |
            market::kTickPrimaryOrderIdValidV1 |
            market::kTickExchangeTimeValidV1 |
            market::kTickSideValidV1;
        context->Expect(
            tick->fields.action == market::TickActionV1::kAdd &&
                tick->fields.side == market::SideV1::kBuy &&
                tick->fields.validity_bitmap == expected,
            "SH A validity bitmap contains only documented A fields");
        context->Expect(
            tick->fields.price.valid &&
                tick->fields.price.raw == 12345 &&
                tick->fields.price.scale == 3U &&
                tick->fields.price.normalized_p6 == 12345000,
            "SH A price is exact p3 to p6");
        context->Expect(
            tick->fields.matched_quantity.valid &&
                tick->fields.matched_quantity.raw == 7 &&
                tick->fields.matched_quantity.scale == 0U &&
                !tick->fields.trade_amount.valid,
            "SH A converts matched quantity without exposing TradeMoney as amount");
        context->Expect(
            tick->fields.primary_order_id == spec.buy_order_id,
            "SH A buy chooses buy order number as primary ID");
    }

    spec.trade_money_p3 = 7001;
    body = MakeShanghaiTickWire(spec);
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH A non-integral matched quantity remains a decoded event");
    tick = std::get_if<market::ShanghaiTickV1>(&event);
    if (tick != nullptr) {
        context->Expect(
            !tick->fields.matched_quantity.valid &&
                (tick->fields.validity_bitmap &
                 market::kTickMatchedQuantityValidV1) == 0U,
            "SH A non-integral matched quantity is invalid, not rounded");
        context->Expect(
            HasQuality(
                tick->common,
                control::QualityFlagV1::kNonIntegralMatchedQty),
            "SH A non-integral matched quantity sets quality");
    }

    spec.type = "D";
    spec.tick_flag = "S";
    spec.price_p3 = 999999;
    spec.quantity = 45;
    spec.trade_money_p3 = 888000;
    body = MakeShanghaiTickWire(spec);
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH D decodes");
    tick = std::get_if<market::ShanghaiTickV1>(&event);
    if (tick != nullptr) {
        const std::uint32_t expected =
            market::kTickQuantityValidV1 |
            market::kTickPrimaryOrderIdValidV1 |
            market::kTickExchangeTimeValidV1 |
            market::kTickSideValidV1;
        context->Expect(
            tick->fields.action == market::TickActionV1::kCancel &&
                tick->fields.side == market::SideV1::kSell &&
                tick->fields.primary_order_id == spec.sell_order_id &&
                tick->fields.validity_bitmap == expected,
            "SH D exposes quantity/side/selected ID only");
        context->Expect(
            !tick->fields.price.valid &&
                !tick->fields.trade_amount.valid &&
                !tick->fields.matched_quantity.valid,
            "SH D ignores price and both TradeMoney interpretations");
    }

    spec.type = "T";
    spec.tick_flag = "N";
    spec.price_p3 = 12345;
    spec.quantity = 60;
    spec.trade_money_p3 = 740700;
    body = MakeShanghaiTickWire(spec);
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH T decodes");
    tick = std::get_if<market::ShanghaiTickV1>(&event);
    if (tick != nullptr) {
        const std::uint32_t expected =
            market::kTickPriceValidV1 |
            market::kTickQuantityValidV1 |
            market::kTickTradeAmountValidV1 |
            market::kTickBuyOrderIdValidV1 |
            market::kTickSellOrderIdValidV1 |
            market::kTickExchangeTimeValidV1 |
            market::kTickAggressorValidV1;
        context->Expect(
            tick->fields.action == market::TickActionV1::kTrade &&
                tick->fields.aggressor == market::AggressorV1::kNeutral &&
                tick->fields.validity_bitmap == expected,
            "SH T exposes price/qty/amount/two IDs/aggressor");
        context->Expect(
            tick->fields.trade_amount.valid &&
                tick->fields.trade_amount.raw == 740700 &&
                tick->fields.trade_amount.scale == 3U &&
                !tick->fields.matched_quantity.valid,
            "SH T interprets TradeMoney as amount, not matched quantity");
    }

    spec.type = "S";
    spec.tick_flag = "TRADE";
    spec.price_p3 = 777777;
    spec.quantity = 888;
    spec.trade_money_p3 = 999999;
    body = MakeShanghaiTickWire(spec);
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH S status decodes");
    tick = std::get_if<market::ShanghaiTickV1>(&event);
    if (tick != nullptr) {
        const std::uint32_t expected =
            market::kTickExchangeTimeValidV1 |
            market::kTickPhaseValidV1;
        context->Expect(
            tick->fields.action == market::TickActionV1::kStatus &&
                tick->fields.phase == market::TradingPhaseV1::kContinuous &&
                tick->fields.validity_bitmap == expected,
            "SH S exposes status phase and time only");
        context->Expect(
            !tick->fields.price.valid &&
                !tick->fields.quantity.valid &&
                !tick->fields.trade_amount.valid &&
                !tick->fields.matched_quantity.valid,
            "SH S does not expose economic fields");
    }

    spec.type = "A";
    spec.tick_flag = "B";
    spec.trade_money_p3 = 0;
    body = MakeShanghaiTickWire(spec);
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH A following status decodes");
    tick = std::get_if<market::ShanghaiTickV1>(&event);
    if (tick != nullptr) {
        context->Expect(
            tick->fields.phase == market::TradingPhaseV1::kContinuous &&
                (tick->fields.validity_bitmap &
                 market::kTickPhaseValidV1) != 0U,
            "SH normalized phase persists for later ticks of the security");
    }
}

void TestShenzhenOrderEnumsAndPriceValidity(TestContext* context) {
    struct Case final {
        std::int32_t raw_side;
        std::int32_t raw_order_type;
        market::SideV1 side;
        market::OrderTypeV1 order_type;
        bool price_valid;
        bool unknown_enum;
        const char* label;
    };
    const std::vector<Case> cases = {
        {49,
         49,
         market::SideV1::kBuy,
         market::OrderTypeV1::kMarket,
         false,
         false,
         "buy market"},
        {50,
         50,
         market::SideV1::kSell,
         market::OrderTypeV1::kLimit,
         true,
         false,
         "sell limit"},
        {70,
         85,
         market::SideV1::kLend,
         market::OrderTypeV1::kSameSideBest,
         false,
         false,
         "lend own-best"},
        {71,
         50,
         market::SideV1::kBorrow,
         market::OrderTypeV1::kLimit,
         true,
         false,
         "borrow limit"},
        {49,
         999,
         market::SideV1::kBuy,
         market::OrderTypeV1::kUnknown,
         false,
         true,
         "unknown order type"},
        {999,
         50,
         market::SideV1::kUnknown,
         market::OrderTypeV1::kLimit,
         true,
         true,
         "unknown side limit"},
    };

    market::MarketDecoderV1 decoder = MakeDecoder();
    std::uint64_t sequence = 200U;
    for (const Case& test_case : cases) {
        ShenzhenOrderSpec spec;
        spec.application_sequence =
            9000 + static_cast<std::int64_t>(sequence);
        spec.side = test_case.raw_side;
        spec.order_type = test_case.raw_order_type;
        std::vector<std::byte> body = MakeShenzhenOrderWire(spec);
        market::DecodedMarketEventV1 event;
        const market::MarketDecodeErrorV1 error = decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenOrderMessage,
                body,
                sequence++),
            &event);
        context->Expect(
            error == market::MarketDecodeErrorV1::kNone,
            std::string("SZ order ") + test_case.label + " decodes");
        const auto* const order =
            std::get_if<market::ShenzhenOrderV1>(&event);
        context->Expect(
            order != nullptr,
            std::string("SZ order ") + test_case.label +
                " selects order variant");
        if (order == nullptr) {
            continue;
        }
        context->Expect(
            order->raw_side == test_case.raw_side &&
                order->raw_order_type == test_case.raw_order_type &&
                order->fields.side == test_case.side &&
                order->fields.order_type == test_case.order_type,
            std::string("SZ order ") + test_case.label +
                " maps 32-bit ASCII enums exactly");
        context->Expect(
            order->fields.price.valid == test_case.price_valid &&
                (((order->fields.validity_bitmap &
                   market::kTickPriceValidV1) != 0U) ==
                 test_case.price_valid),
            std::string("SZ order ") + test_case.label +
                " applies limit-only price validity");
        context->Expect(
            order->fields.action == market::TickActionV1::kAdd &&
                order->fields.quantity.valid &&
                order->fields.quantity.raw == spec.quantity &&
                order->fields.primary_order_id ==
                    spec.application_sequence &&
                (order->fields.validity_bitmap &
                 market::kTickPrimaryOrderIdValidV1) != 0U,
            std::string("SZ order ") + test_case.label +
                " preserves quantity and ApplSeqNum primary ID");
        context->Expect(
            HasQuality(
                order->common,
                control::QualityFlagV1::kUnknownEnum) ==
                test_case.unknown_enum,
            std::string("SZ order ") + test_case.label +
                " unknown-enum quality is exact");

        if (test_case.raw_side == 49 &&
            test_case.raw_order_type == 49) {
            std::fill(body.begin(), body.end(), std::byte{0x58U});
            context->Expect(
                order->common.security_id == "000001" &&
                    order->common.security_id_source == "102" &&
                    order->common.md_stream_id == "010" &&
                    order->common.origin.body.empty() &&
                    order->common.origin.body.data() == nullptr,
                "decoded output owns text and retains no body span");
        }
    }
}

void TestShenzhenTransactionMatrix(TestContext* context) {
    struct CancelCase final {
        std::int64_t bid_id;
        std::int64_t offer_id;
        market::SideV1 side;
        bool primary_valid;
        std::int64_t primary_id;
        bool ambiguous;
        const char* label;
    };
    const std::vector<CancelCase> cancel_cases = {
        {0,
         0,
         market::SideV1::kUnknown,
         false,
         0,
         true,
         "00"},
        {0,
         2202,
         market::SideV1::kSell,
         true,
         2202,
         false,
         "01"},
        {1101,
         0,
         market::SideV1::kBuy,
         true,
         1101,
         false,
         "10"},
        {1101,
         2202,
         market::SideV1::kUnknown,
         false,
         0,
         true,
         "11"},
    };

    market::MarketDecoderV1 decoder = MakeDecoder();
    std::uint64_t sequence = 300U;
    for (const CancelCase& test_case : cancel_cases) {
        ShenzhenTransactionSpec spec;
        spec.application_sequence =
            20000 + static_cast<std::int64_t>(sequence);
        spec.bid_application_sequence = test_case.bid_id;
        spec.offer_application_sequence = test_case.offer_id;
        spec.last_price_p4 = 987654;
        spec.last_quantity = 33;
        spec.execution_type = 52;
        std::vector<std::byte> body =
            MakeShenzhenTransactionWire(spec);
        market::DecodedMarketEventV1 event;
        const market::MarketDecodeErrorV1 error = decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenTransactionMessage,
                body,
                sequence++),
            &event);
        context->Expect(
            error == market::MarketDecodeErrorV1::kNone,
            std::string("SZ cancel ") + test_case.label + " decodes");
        const auto* const transaction =
            std::get_if<market::ShenzhenTransactionV1>(&event);
        if (transaction == nullptr) {
            context->Expect(
                false,
                std::string("SZ cancel ") + test_case.label +
                    " selects transaction variant");
            continue;
        }
        context->Expect(
            transaction->fields.action ==
                    market::TickActionV1::kCancel &&
                transaction->raw_execution_type == 52 &&
                !transaction->fields.price.valid &&
                (transaction->fields.validity_bitmap &
                 market::kTickPriceValidV1) == 0U &&
                transaction->fields.quantity.valid &&
                transaction->fields.quantity.raw == 33,
            std::string("SZ cancel ") + test_case.label +
                " keeps cancel price invalid even when wire price is nonzero");
        context->Expect(
            transaction->fields.side == test_case.side &&
                (((transaction->fields.validity_bitmap &
                   market::kTickPrimaryOrderIdValidV1) != 0U) ==
                 test_case.primary_valid) &&
                transaction->fields.primary_order_id ==
                    test_case.primary_id,
            std::string("SZ cancel ") + test_case.label +
                " applies the documented ID/side rule");
        context->Expect(
            HasQuality(
                transaction->common,
                control::QualityFlagV1::kAmbiguousOrderReference) ==
                test_case.ambiguous,
            std::string("SZ cancel ") + test_case.label +
                " ambiguity quality is exact");
    }

    ShenzhenTransactionSpec trade_spec;
    trade_spec.execution_type = 70;
    trade_spec.bid_application_sequence = 1101;
    trade_spec.offer_application_sequence = 2202;
    std::vector<std::byte> trade_body =
        MakeShenzhenTransactionWire(trade_spec);
    market::DecodedMarketEventV1 trade_event;
    context->Expect(
        decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenTransactionMessage,
                trade_body,
                sequence++),
            &trade_event) == market::MarketDecodeErrorV1::kNone,
        "SZ ExecType 70 trade decodes");
    const auto* const trade =
        std::get_if<market::ShenzhenTransactionV1>(&trade_event);
    if (trade != nullptr) {
        context->Expect(
            trade->fields.action == market::TickActionV1::kTrade &&
                trade->fields.price.valid &&
                trade->fields.price.raw == trade_spec.last_price_p4 &&
                trade->fields.quantity.valid &&
                trade->fields.buy_order_id == 1101 &&
                trade->fields.sell_order_id == 2202 &&
                (trade->fields.validity_bitmap &
                 market::kTickBuyOrderIdValidV1) != 0U &&
                (trade->fields.validity_bitmap &
                 market::kTickSellOrderIdValidV1) != 0U &&
                (trade->fields.validity_bitmap &
                 market::kTickPrimaryOrderIdValidV1) == 0U,
            "SZ trade exposes price/qty/two IDs without inventing primary ID");
    }

    ShenzhenTransactionSpec unknown_spec;
    unknown_spec.execution_type = 99;
    std::vector<std::byte> unknown_body =
        MakeShenzhenTransactionWire(unknown_spec);
    market::DecodedMarketEventV1 unknown_event;
    context->Expect(
        decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenTransactionMessage,
                unknown_body,
                sequence++),
            &unknown_event) == market::MarketDecodeErrorV1::kNone,
        "unknown SZ ExecType remains an owned decoded event");
    const auto* const unknown =
        std::get_if<market::ShenzhenTransactionV1>(&unknown_event);
    if (unknown != nullptr) {
        context->Expect(
            unknown->fields.action == market::TickActionV1::kUnknown &&
                HasQuality(
                    unknown->common,
                    control::QualityFlagV1::kUnknownEnum),
            "unknown SZ ExecType fails closed with unknown-enum quality");
        context->Expect(
            unknown->fields.buy_order_id ==
                    unknown_spec.bid_application_sequence &&
                unknown->fields.sell_order_id ==
                    unknown_spec.offer_application_sequence &&
                (unknown->fields.validity_bitmap &
                 (market::kTickBuyOrderIdValidV1 |
                  market::kTickSellOrderIdValidV1)) == 0U,
            "unknown SZ ExecType retains raw IDs without publishing validity");
    }
}

void TestIgnoredNumericOverflowAndPhaseHistory(TestContext* context) {
    constexpr std::int64_t kNonNullI64Maximum =
        std::numeric_limits<std::int64_t>::max();
    constexpr std::int32_t kNonNullI32Maximum =
        std::numeric_limits<std::int32_t>::max();
    constexpr std::int64_t kLargestIntegralMatchedP3 =
        (kNonNullI64Maximum / 1000) * 1000;
    constexpr std::int64_t kExpectedMatchedNative =
        kLargestIntegralMatchedP3 / 1000;

    market::MarketDecoderV1 sh_decoder = MakeDecoder();
    std::uint64_t sequence = 360U;
    ShanghaiTickSpec sh_spec;
    sh_spec.price_p3 = kNonNullI32Maximum;
    sh_spec.trade_money_p3 = kNonNullI64Maximum;
    sh_spec.type = "D";
    sh_spec.tick_flag = "B";
    std::vector<std::byte> body = MakeShanghaiTickWire(sh_spec);
    market::DecodedMarketEventV1 event;
    context->Expect(
        sh_decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH D ignores non-null extreme placeholders before p6 normalization");
    const auto* sh_tick = std::get_if<market::ShanghaiTickV1>(&event);
    if (sh_tick != nullptr) {
        context->Expect(
            sh_tick->fields.action == market::TickActionV1::kCancel &&
                !sh_tick->fields.price.valid &&
                !sh_tick->fields.trade_amount.valid &&
                (sh_tick->fields.validity_bitmap &
                 (market::kTickPriceValidV1 |
                  market::kTickTradeAmountValidV1)) == 0U,
            "SH D extreme Price/TradeMoney remain semantically invalid");
    }

    // INT32_MAX p3 itself fits p6; the paired non-null INT64_MAX p3
    // TradeMoney does not.  Both fields are meaningless for STATUS and must
    // therefore be gated before normalization.
    sh_spec.type = "S";
    sh_spec.tick_flag = "TRADE";
    body = MakeShanghaiTickWire(sh_spec);
    context->Expect(
        sh_decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH known STATUS ignores p6-overflowing TradeMoney placeholder");
    sh_tick = std::get_if<market::ShanghaiTickV1>(&event);
    if (sh_tick != nullptr) {
        context->Expect(
            sh_tick->fields.phase == market::TradingPhaseV1::kContinuous &&
                !sh_tick->fields.price.valid &&
                !sh_tick->fields.trade_amount.valid,
            "SH known STATUS records phase without exposing extreme fields");
    }

    sh_spec.tick_flag = "FUTURE_PHASE";
    body = MakeShanghaiTickWire(sh_spec);
    context->Expect(
        sh_decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "printable unknown SH STATUS remains a decoded event");
    sh_tick = std::get_if<market::ShanghaiTickV1>(&event);
    if (sh_tick != nullptr) {
        context->Expect(
            sh_tick->fields.phase == market::TradingPhaseV1::kUnknown &&
                (sh_tick->fields.validity_bitmap &
                 market::kTickPhaseValidV1) == 0U &&
                HasQuality(
                    sh_tick->common,
                    control::QualityFlagV1::kUnknownEnum) &&
                !HasQuality(
                    sh_tick->common,
                    control::QualityFlagV1::kDecodeTextInvalid),
            "printable unknown SH STATUS is unknown enum, not invalid text");
    }

    sh_spec.type = "A";
    sh_spec.tick_flag = "B";
    sh_spec.price_p3 = 12345;
    sh_spec.trade_money_p3 = kLargestIntegralMatchedP3;
    body = MakeShanghaiTickWire(sh_spec);
    context->Expect(
        sh_decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH A handles large divisible matched p3 without amount p6 overflow");
    sh_tick = std::get_if<market::ShanghaiTickV1>(&event);
    if (sh_tick != nullptr) {
        context->Expect(
            sh_tick->fields.matched_quantity.valid &&
                sh_tick->fields.matched_quantity.raw ==
                    kExpectedMatchedNative &&
                sh_tick->fields.matched_quantity.scale == 0U &&
                !sh_tick->fields.trade_amount.valid &&
                !HasQuality(
                    sh_tick->common,
                    control::QualityFlagV1::kNonIntegralMatchedQty),
            "SH A divides large matched p3 exactly instead of normalizing it as amount");
        context->Expect(
            sh_tick->fields.phase == market::TradingPhaseV1::kContinuous &&
                (sh_tick->fields.validity_bitmap &
                 market::kTickPhaseValidV1) != 0U,
            "unknown printable STATUS does not overwrite known phase history");
    }

    struct SzOrderCase final {
        std::int32_t order_type;
        const char* label;
    };
    const std::vector<SzOrderCase> order_cases = {
        {49, "market"},
        {85, "own-best"},
        {999, "unknown"},
    };
    market::MarketDecoderV1 sz_order_decoder = MakeDecoder();
    for (const SzOrderCase& test_case : order_cases) {
        ShenzhenOrderSpec order_spec;
        order_spec.application_sequence =
            30000 + static_cast<std::int64_t>(sequence);
        order_spec.price_p4 = kNonNullI64Maximum;
        order_spec.order_type = test_case.order_type;
        body = MakeShenzhenOrderWire(order_spec);
        const market::MarketDecodeErrorV1 error = sz_order_decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenOrderMessage,
                body,
                sequence++),
            &event);
        context->Expect(
            error == market::MarketDecodeErrorV1::kNone,
            std::string("SZ ") + test_case.label +
                " order ignores p6-overflowing Price");
        const auto* const order =
            std::get_if<market::ShenzhenOrderV1>(&event);
        if (order != nullptr) {
            context->Expect(
                !order->fields.price.valid &&
                    (order->fields.validity_bitmap &
                     market::kTickPriceValidV1) == 0U,
                std::string("SZ ") + test_case.label +
                    " extreme Price remains invalid");
        }
    }

    struct SzExecutionCase final {
        std::int32_t execution_type;
        market::TickActionV1 action;
        const char* label;
    };
    const std::vector<SzExecutionCase> execution_cases = {
        {52, market::TickActionV1::kCancel, "cancel"},
        {99, market::TickActionV1::kUnknown, "unknown ExecType"},
    };
    market::MarketDecoderV1 sz_transaction_decoder = MakeDecoder();
    for (const SzExecutionCase& test_case : execution_cases) {
        ShenzhenTransactionSpec transaction_spec;
        transaction_spec.application_sequence =
            40000 + static_cast<std::int64_t>(sequence);
        transaction_spec.bid_application_sequence = 1101;
        transaction_spec.offer_application_sequence = 0;
        transaction_spec.last_price_p4 = kNonNullI64Maximum;
        transaction_spec.execution_type = test_case.execution_type;
        body = MakeShenzhenTransactionWire(transaction_spec);
        const market::MarketDecodeErrorV1 error =
            sz_transaction_decoder.Decode(
                Message(
                    kShenzhenService,
                    kShenzhenTransactionMessage,
                    body,
                    sequence++),
                &event);
        context->Expect(
            error == market::MarketDecodeErrorV1::kNone,
            std::string("SZ ") + test_case.label +
                " ignores p6-overflowing LastPx");
        const auto* const transaction =
            std::get_if<market::ShenzhenTransactionV1>(&event);
        if (transaction != nullptr) {
            context->Expect(
                transaction->fields.action == test_case.action &&
                    !transaction->fields.price.valid &&
                    (transaction->fields.validity_bitmap &
                     market::kTickPriceValidV1) == 0U,
                std::string("SZ ") + test_case.label +
                    " extreme LastPx remains invalid");
        }
    }
}

void TestMatchedQuantityDomainAndFailureAtomicity(TestContext* context) {
    market::MarketDecoderV1 decoder = MakeDecoder();
    std::uint64_t sequence = 440U;
    ShanghaiTickSpec sh_spec;
    sh_spec.type = "A";
    sh_spec.tick_flag = "B";
    sh_spec.trade_money_p3 =
        std::numeric_limits<std::int64_t>::min();
    std::vector<std::byte> body = MakeShanghaiTickWire(sh_spec);
    market::DecodedMarketEventV1 event;
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH A null matched-quantity sentinel decodes");
    const auto* tick = std::get_if<market::ShanghaiTickV1>(&event);
    if (tick != nullptr) {
        context->Expect(
            tick->fields.trade_amount.is_null &&
                !tick->fields.trade_amount.valid &&
                !tick->fields.matched_quantity.valid &&
                (tick->fields.validity_bitmap &
                 market::kTickMatchedQuantityValidV1) == 0U &&
                HasQuality(
                    tick->common,
                    control::QualityFlagV1::kNullValuePresent) &&
                !HasQuality(
                    tick->common,
                    control::QualityFlagV1::kNonIntegralMatchedQty) &&
                !HasNotice(
                    tick->common,
                    market::MarketNoticeV1::
                        kMatchedQuantityDomainInvalid),
            "SH A null is null-only, not non-integral or negative-domain");
    }

    sh_spec.trade_money_p3 = -7000;
    body = MakeShanghaiTickWire(sh_spec);
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH A negative divisible matched quantity decodes fail-closed");
    tick = std::get_if<market::ShanghaiTickV1>(&event);
    if (tick != nullptr) {
        context->Expect(
            !tick->fields.trade_amount.valid &&
                !tick->fields.matched_quantity.valid &&
                (tick->fields.validity_bitmap &
                 market::kTickMatchedQuantityValidV1) == 0U &&
                HasNotice(
                    tick->common,
                    market::MarketNoticeV1::
                        kMatchedQuantityDomainInvalid) &&
                !HasQuality(
                    tick->common,
                    control::QualityFlagV1::kNonIntegralMatchedQty) &&
                !HasQuality(
                    tick->common,
                    control::QualityFlagV1::kNullValuePresent),
            "SH A negative divisible raw is domain-invalid, not non-integral");
    }

    sh_spec.type = "T";
    sh_spec.tick_flag = "B";
    sh_spec.trade_money_p3 =
        std::numeric_limits<std::int64_t>::min() + 1;
    body = MakeShanghaiTickWire(sh_spec);
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH T negative extreme amount remains an auditable record");
    tick = std::get_if<market::ShanghaiTickV1>(&event);
    if (tick != nullptr) {
        context->Expect(
            tick->fields.trade_amount.raw ==
                    std::numeric_limits<std::int64_t>::min() + 1 &&
                tick->fields.trade_amount.normalized_p6 == 0 &&
                !tick->fields.trade_amount.valid &&
                !tick->fields.trade_amount.is_null &&
                (tick->fields.validity_bitmap &
                 market::kTickTradeAmountValidV1) == 0U &&
                HasNotice(
                    tick->common,
                    market::MarketNoticeV1::
                        kTradeAmountDomainInvalid) &&
                !HasQuality(
                    tick->common,
                    control::QualityFlagV1::kNullValuePresent),
            "SH T negative amount retains raw but is never advertised valid");
    }

    market::MarketDecoderConfigV1 too_small_config;
    too_small_config.trade_date = kTradeDate;
    too_small_config.source_stream_id = kSourceStreamId;
    too_small_config.limits.maximum_body_bytes = 224U;
    market::MarketDecoderV1 too_small_decoder(too_small_config);
    context->Expect(
        !too_small_decoder.configuration_valid(),
        "maximum_body_bytes 224 cannot hold the 248-byte SH fixed body");
    market::DecodedMarketEventV1 unchanged = SentinelOutput();
    context->Expect(
        too_small_decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                sequence++),
            &unchanged) ==
                market::MarketDecodeErrorV1::kInvalidConfiguration &&
            IsSentinel(unchanged),
        "invalid maximum-body configuration leaves output unchanged");

    ShenzhenOrderSpec overflow_order;
    overflow_order.order_type = 50;
    overflow_order.price_p4 =
        std::numeric_limits<std::int64_t>::max();
    body = MakeShenzhenOrderWire(overflow_order);
    unchanged = SentinelOutput();
    context->Expect(
        decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenOrderMessage,
                body,
                sequence++),
            &unchanged) ==
                market::MarketDecodeErrorV1::kFixedPointOverflow &&
            IsSentinel(unchanged),
        "fixed-point overflow leaves output unchanged");

    body = MakeShenzhenSnapshotWire();
    // The four golden strings occupy 3+6+3+1 bytes after the 224-byte
    // fixed body.  The first bid therefore starts at 237, and its nested
    // list descriptor's relative-offset word is at 237+20+4=261.
    constexpr std::size_t kFirstBidNestedOffsetWord = 261U;
    OverwriteU32(&body, kFirstBidNestedOffsetWord, 0U);
    unchanged = SentinelOutput();
    context->Expect(
        decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenSnapshotMessage,
                body,
                sequence++),
            &unchanged) == market::MarketDecodeErrorV1::kOffsetInvalid &&
            IsSentinel(unchanged),
        "invalid nested-list offset leaves output unchanged");

    market::MarketDecoderConfigV1 count_config;
    count_config.trade_date = kTradeDate;
    count_config.source_stream_id = kSourceStreamId;
    count_config.limits.maximum_depth_items = 1U;
    market::MarketDecoderV1 count_decoder(count_config);
    context->Expect(
        count_decoder.configuration_valid(),
        "depth-one limit is a valid decoder configuration");
    body = MakeShenzhenSnapshotWire();
    unchanged = SentinelOutput();
    context->Expect(
        count_decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenSnapshotMessage,
                body,
                sequence++),
            &unchanged) == market::MarketDecodeErrorV1::kCountExceeded &&
            market::MarketDecodeQualityFlagsV1(
                market::MarketDecodeErrorV1::kCountExceeded) == 0U &&
            market::MarketDecodeQualityFlagsV1(
                market::MarketDecodeErrorV1::kCountMismatch) == 0U &&
            IsSentinel(unchanged),
        "configured depth ceiling fails without fake offset quality");

    ShenzhenOrderSpec valid_order;
    body = MakeShenzhenOrderWire(valid_order);
    market::MarketMessageViewV1 invalid_input = Message(
        kShenzhenService,
        kShenzhenOrderMessage,
        body,
        sequence++);
    invalid_input.source_sequence = 0U;
    unchanged = SentinelOutput();
    context->Expect(
        decoder.Decode(invalid_input, &unchanged) ==
                market::MarketDecodeErrorV1::kInvalidInput &&
            IsSentinel(unchanged),
        "invalid input leaves output unchanged");

    invalid_input = Message(
        kShenzhenService,
        kShenzhenOrderMessage,
        body,
        sequence++);
    invalid_input.message_encoding = 2U;
    unchanged = SentinelOutput();
    context->Expect(
        decoder.Decode(invalid_input, &unchanged) ==
                market::MarketDecodeErrorV1::kInvalidInput &&
            IsSentinel(unchanged),
        "non-binary body never enters the binary decoder");
}

void TestNegativeQuantityDomains(TestContext* context) {
    market::MarketDecoderV1 decoder = MakeDecoder();
    std::uint64_t sequence = 470U;

    struct ShanghaiCase final {
        std::string type;
        const char* label;
    };
    const std::vector<ShanghaiCase> shanghai_cases = {
        {"A", "SH add"},
        {"D", "SH cancel"},
        {"T", "SH trade"},
    };
    for (const ShanghaiCase& test_case : shanghai_cases) {
        ShanghaiTickSpec spec;
        spec.type = test_case.type;
        spec.quantity = -7;
        const std::vector<std::byte> body = MakeShanghaiTickWire(spec);
        market::DecodedMarketEventV1 event;
        const market::MarketDecodeErrorV1 error = decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                sequence++),
            &event);
        context->Expect(
            error == market::MarketDecodeErrorV1::kNone,
            std::string(test_case.label) +
                " with negative quantity decodes for audit retention");
        const auto* const tick =
            std::get_if<market::ShanghaiTickV1>(&event);
        context->Expect(
            tick != nullptr && tick->fields.quantity.raw == -7 &&
                !tick->fields.quantity.valid &&
                (tick->fields.validity_bitmap &
                 market::kTickQuantityValidV1) == 0U &&
                HasNotice(
                    tick->common,
                    market::MarketNoticeV1::kQuantityDomainInvalid),
            std::string(test_case.label) +
                " retains raw negative quantity without publishing validity");
    }

    ShenzhenOrderSpec order_spec;
    order_spec.quantity = -11;
    std::vector<std::byte> body = MakeShenzhenOrderWire(order_spec);
    market::DecodedMarketEventV1 event;
    context->Expect(
        decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenOrderMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SZ order with negative quantity decodes for audit retention");
    const auto* const order =
        std::get_if<market::ShenzhenOrderV1>(&event);
    context->Expect(
        order != nullptr && order->fields.quantity.raw == -11 &&
            !order->fields.quantity.valid &&
            (order->fields.validity_bitmap &
             market::kTickQuantityValidV1) == 0U &&
            HasNotice(
                order->common,
                market::MarketNoticeV1::kQuantityDomainInvalid),
        "SZ order retains raw negative quantity without publishing validity");

    struct ShenzhenTransactionCase final {
        std::int32_t execution_type;
        const char* label;
    };
    const std::vector<ShenzhenTransactionCase> transaction_cases = {
        {70, "SZ trade"},
        {52, "SZ cancel"},
        {99, "SZ unknown execution"},
    };
    for (const ShenzhenTransactionCase& test_case : transaction_cases) {
        ShenzhenTransactionSpec spec;
        spec.execution_type = test_case.execution_type;
        spec.last_quantity = -13;
        body = MakeShenzhenTransactionWire(spec);
        event = SentinelOutput();
        const market::MarketDecodeErrorV1 error = decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenTransactionMessage,
                body,
                sequence++),
            &event);
        context->Expect(
            error == market::MarketDecodeErrorV1::kNone,
            std::string(test_case.label) +
                " with negative quantity decodes for audit retention");
        const auto* const transaction =
            std::get_if<market::ShenzhenTransactionV1>(&event);
        context->Expect(
            transaction != nullptr &&
                transaction->fields.quantity.raw == -13 &&
                !transaction->fields.quantity.valid &&
                (transaction->fields.validity_bitmap &
                 market::kTickQuantityValidV1) == 0U &&
                HasNotice(
                    transaction->common,
                    market::MarketNoticeV1::kQuantityDomainInvalid),
            std::string(test_case.label) +
                " retains raw negative quantity without publishing validity");
    }

    // Golden SH snapshot construction: fixed body 248 + 6-byte SecurityID +
    // 5-byte status => first bid at 259; 12 bids and one ask end at 623,
    // where the first bid-one queue item begins.
    constexpr std::size_t kFirstShanghaiBidQuantity = 267U;
    constexpr std::size_t kFirstShanghaiBidQueueQuantity = 631U;
    body = MakeShanghaiSnapshotWire();
    OverwriteI64(&body, wire_abi::sh_snapshot::kTradeVolume, -17);
    OverwriteI64(&body, kFirstShanghaiBidQuantity, -18);
    OverwriteI64(&body, kFirstShanghaiBidQueueQuantity, -19);
    event = SentinelOutput();
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiSnapshotMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH snapshot aggregate/book/queue negative quantities remain auditable");
    const auto* const sh_snapshot =
        std::get_if<market::ShanghaiSnapshotV1>(&event);
    context->Expect(
        sh_snapshot != nullptr && sh_snapshot->trade_volume.raw == -17 &&
            !sh_snapshot->trade_volume.valid &&
            sh_snapshot->book.bids[0U].quantity.raw == -18 &&
            !sh_snapshot->book.bids[0U].quantity.valid &&
            sh_snapshot->book.bid1_queue.quantities[0U].raw == -19 &&
            !sh_snapshot->book.bid1_queue.quantities[0U].valid &&
            HasNotice(
                sh_snapshot->common,
                market::MarketNoticeV1::kQuantityDomainInvalid),
        "SH nested quantity paths share the negative-domain contract");

    body = MakeShanghaiSnapshotWire();
    OverwriteI64(
        &body,
        wire_abi::sh_snapshot::kTradeVolume,
        std::numeric_limits<std::int64_t>::min());
    event = SentinelOutput();
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiSnapshotMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH nullable quantity sentinel remains decodable");
    const auto* const sh_null_snapshot =
        std::get_if<market::ShanghaiSnapshotV1>(&event);
    context->Expect(
        sh_null_snapshot != nullptr &&
            sh_null_snapshot->trade_volume.is_null &&
            !sh_null_snapshot->trade_volume.valid &&
            HasQuality(
                sh_null_snapshot->common,
                control::QualityFlagV1::kNullValuePresent) &&
            !HasNotice(
                sh_null_snapshot->common,
                market::MarketNoticeV1::kQuantityDomainInvalid),
        "SH null quantity is distinct from a negative-domain value");

    // Golden SZ snapshot construction: four strings end at 237, two bid items
    // and one ask item end at 321, where the bid-one queue begins.
    constexpr std::size_t kFirstShenzhenBidQuantity = 237U;
    constexpr std::size_t kFirstShenzhenBidQueueQuantity = 321U;
    body = MakeShenzhenSnapshotWire();
    OverwriteI64(&body, wire_abi::sz_snapshot::kVolume, -27);
    OverwriteI64(&body, kFirstShenzhenBidQuantity, -28);
    OverwriteI64(&body, kFirstShenzhenBidQueueQuantity, -29);
    event = SentinelOutput();
    context->Expect(
        decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenSnapshotMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SZ snapshot aggregate/book/queue negative quantities remain auditable");
    const auto* const sz_snapshot =
        std::get_if<market::ShenzhenSnapshotV1>(&event);
    context->Expect(
        sz_snapshot != nullptr && sz_snapshot->volume.raw == -27 &&
            !sz_snapshot->volume.valid &&
            sz_snapshot->book.bids[0U].quantity.raw == -28 &&
            !sz_snapshot->book.bids[0U].quantity.valid &&
            sz_snapshot->book.bid1_queue.quantities[0U].raw == -29 &&
            !sz_snapshot->book.bid1_queue.quantities[0U].valid &&
            HasNotice(
                sz_snapshot->common,
                market::MarketNoticeV1::kQuantityDomainInvalid),
        "SZ nested quantity paths share the negative-domain contract");
}

void TestOrderReferenceDomains(TestContext* context) {
    struct ShSelectedCase final {
        std::string type;
        std::string flag;
        std::int64_t selected_order_id;
        market::TickActionV1 action;
        market::SideV1 side;
        bool domain_notice;
        const char* label;
    };
    const std::vector<ShSelectedCase> selected_cases = {
        {"A",
         "B",
         0,
         market::TickActionV1::kAdd,
         market::SideV1::kBuy,
         false,
         "SH A zero buy reference"},
        {"A",
         "B",
         -101,
         market::TickActionV1::kAdd,
         market::SideV1::kBuy,
         true,
         "SH A negative buy reference"},
        {"D",
         "S",
         0,
         market::TickActionV1::kCancel,
         market::SideV1::kSell,
         false,
         "SH D zero sell reference"},
        {"D",
         "S",
         -202,
         market::TickActionV1::kCancel,
         market::SideV1::kSell,
         true,
         "SH D negative sell reference"},
    };

    market::MarketDecoderV1 sh_decoder = MakeDecoder();
    std::uint64_t sequence = 470U;
    for (const ShSelectedCase& test_case : selected_cases) {
        ShanghaiTickSpec spec;
        spec.type = test_case.type;
        spec.tick_flag = test_case.flag;
        spec.trade_money_p3 = 0;
        if (test_case.flag == "B") {
            spec.buy_order_id = test_case.selected_order_id;
        } else {
            spec.sell_order_id = test_case.selected_order_id;
        }
        std::vector<std::byte> body = MakeShanghaiTickWire(spec);
        market::DecodedMarketEventV1 event;
        const market::MarketDecodeErrorV1 error = sh_decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                sequence++),
            &event);
        context->Expect(
            error == market::MarketDecodeErrorV1::kNone,
            std::string(test_case.label) + " decodes");
        const auto* const tick =
            std::get_if<market::ShanghaiTickV1>(&event);
        if (tick != nullptr) {
            context->Expect(
                tick->fields.action == test_case.action &&
                    tick->fields.side == test_case.side &&
                    (tick->fields.validity_bitmap &
                     market::kTickSideValidV1) != 0U &&
                    (tick->fields.validity_bitmap &
                     market::kTickPrimaryOrderIdValidV1) == 0U,
                std::string(test_case.label) +
                    " keeps side valid but clears primary-ID validity");
            context->Expect(
                HasNotice(
                    tick->common,
                    market::MarketNoticeV1::
                        kOrderReferenceDomainInvalid) ==
                    test_case.domain_notice,
                std::string(test_case.label) +
                    " classifies only negative reference as domain-invalid");
        }
    }

    struct ShTradeCase final {
        std::int64_t buy_order_id;
        std::int64_t sell_order_id;
        bool buy_valid;
        bool sell_valid;
        const char* label;
    };
    const std::vector<ShTradeCase> sh_trade_cases = {
        {-101, 202, false, true, "SH T negative buy reference"},
        {101, -202, true, false, "SH T negative sell reference"},
    };
    for (const ShTradeCase& test_case : sh_trade_cases) {
        ShanghaiTickSpec spec;
        spec.type = "T";
        spec.tick_flag = "N";
        spec.buy_order_id = test_case.buy_order_id;
        spec.sell_order_id = test_case.sell_order_id;
        std::vector<std::byte> body = MakeShanghaiTickWire(spec);
        market::DecodedMarketEventV1 event;
        const market::MarketDecodeErrorV1 error = sh_decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                sequence++),
            &event);
        context->Expect(
            error == market::MarketDecodeErrorV1::kNone,
            std::string(test_case.label) + " decodes");
        const auto* const tick =
            std::get_if<market::ShanghaiTickV1>(&event);
        if (tick != nullptr) {
            context->Expect(
                (((tick->fields.validity_bitmap &
                   market::kTickBuyOrderIdValidV1) != 0U) ==
                 test_case.buy_valid) &&
                    (((tick->fields.validity_bitmap &
                       market::kTickSellOrderIdValidV1) != 0U) ==
                     test_case.sell_valid) &&
                    HasNotice(
                        tick->common,
                        market::MarketNoticeV1::
                            kOrderReferenceDomainInvalid),
                std::string(test_case.label) +
                    " publishes only the positive trade reference");
        }
    }

    struct SzOrderCase final {
        std::int64_t application_sequence;
        bool domain_notice;
        const char* label;
    };
    const std::vector<SzOrderCase> sz_order_cases = {
        {0, false, "SZ order zero ApplSeqNum"},
        {-303, true, "SZ order negative ApplSeqNum"},
    };
    market::MarketDecoderV1 sz_order_decoder = MakeDecoder();
    for (const SzOrderCase& test_case : sz_order_cases) {
        ShenzhenOrderSpec spec;
        spec.application_sequence = test_case.application_sequence;
        std::vector<std::byte> body = MakeShenzhenOrderWire(spec);
        market::DecodedMarketEventV1 event;
        const market::MarketDecodeErrorV1 error = sz_order_decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenOrderMessage,
                body,
                sequence++),
            &event);
        context->Expect(
            error == market::MarketDecodeErrorV1::kNone,
            std::string(test_case.label) + " decodes");
        const auto* const order =
            std::get_if<market::ShenzhenOrderV1>(&event);
        if (order != nullptr) {
            context->Expect(
                order->application_sequence ==
                        test_case.application_sequence &&
                    (order->fields.validity_bitmap &
                     market::kTickPrimaryOrderIdValidV1) == 0U,
                std::string(test_case.label) +
                    " does not publish primary-ID validity");
            context->Expect(
                !test_case.domain_notice ||
                    HasNotice(
                        order->common,
                        market::MarketNoticeV1::
                            kEventSequenceDomainInvalid),
                std::string(test_case.label) +
                    " emits sequence-domain notice when negative");
        }
    }

    struct SzCancelReferenceCase final {
        std::int64_t bid_id;
        std::int64_t offer_id;
        bool buy_valid;
        bool sell_valid;
        bool primary_valid;
        std::int64_t primary_id;
        market::SideV1 side;
        const char* label;
    };
    const std::vector<SzCancelReferenceCase> sz_cancel_cases = {
        {-401,
         0,
         false,
         false,
         false,
         0,
         market::SideV1::kUnknown,
         "SZ cancel negative bid and absent offer"},
        {-401,
         402,
         false,
         true,
         true,
         402,
         market::SideV1::kSell,
         "SZ cancel negative bid and valid offer"},
        {401,
         -402,
         true,
         false,
         true,
         401,
         market::SideV1::kBuy,
         "SZ cancel valid bid and negative offer"},
        {-401,
         -402,
         false,
         false,
         false,
         0,
         market::SideV1::kUnknown,
         "SZ cancel two negative references"},
    };
    market::MarketDecoderV1 sz_transaction_decoder = MakeDecoder();
    for (const SzCancelReferenceCase& test_case : sz_cancel_cases) {
        ShenzhenTransactionSpec spec;
        spec.execution_type = 52;
        spec.bid_application_sequence = test_case.bid_id;
        spec.offer_application_sequence = test_case.offer_id;
        std::vector<std::byte> body =
            MakeShenzhenTransactionWire(spec);
        market::DecodedMarketEventV1 event;
        const market::MarketDecodeErrorV1 error =
            sz_transaction_decoder.Decode(
                Message(
                    kShenzhenService,
                    kShenzhenTransactionMessage,
                    body,
                    sequence++),
                &event);
        context->Expect(
            error == market::MarketDecodeErrorV1::kNone,
            std::string(test_case.label) + " decodes");
        const auto* const transaction =
            std::get_if<market::ShenzhenTransactionV1>(&event);
        if (transaction != nullptr) {
            context->Expect(
                (((transaction->fields.validity_bitmap &
                   market::kTickBuyOrderIdValidV1) != 0U) ==
                 test_case.buy_valid) &&
                    (((transaction->fields.validity_bitmap &
                       market::kTickSellOrderIdValidV1) != 0U) ==
                     test_case.sell_valid) &&
                    (((transaction->fields.validity_bitmap &
                       market::kTickPrimaryOrderIdValidV1) != 0U) ==
                     test_case.primary_valid) &&
                    transaction->fields.primary_order_id ==
                        test_case.primary_id &&
                    transaction->fields.side == test_case.side &&
                    HasNotice(
                        transaction->common,
                        market::MarketNoticeV1::
                            kOrderReferenceDomainInvalid),
                std::string(test_case.label) +
                    " ignores negative references for cancel-side inference");
        }
    }

    ShenzhenTransactionSpec trade_spec;
    trade_spec.execution_type = 70;
    trade_spec.bid_application_sequence = -501;
    trade_spec.offer_application_sequence = 502;
    std::vector<std::byte> trade_body =
        MakeShenzhenTransactionWire(trade_spec);
    market::DecodedMarketEventV1 trade_event;
    context->Expect(
        sz_transaction_decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenTransactionMessage,
                trade_body,
                sequence++),
            &trade_event) == market::MarketDecodeErrorV1::kNone,
        "SZ trade with negative bid reference decodes");
    const auto* const transaction =
        std::get_if<market::ShenzhenTransactionV1>(&trade_event);
    if (transaction != nullptr) {
        context->Expect(
            (transaction->fields.validity_bitmap &
             market::kTickBuyOrderIdValidV1) == 0U &&
                (transaction->fields.validity_bitmap &
                 market::kTickSellOrderIdValidV1) != 0U &&
                HasNotice(
                    transaction->common,
                    market::MarketNoticeV1::
                        kOrderReferenceDomainInvalid),
            "SZ trade retains negative raw bid without publishing validity");
    }
}

void TestPhaseProductLimit(TestContext* context) {
    market::MarketDecoderConfigV1 config;
    config.trade_date = kTradeDate;
    config.source_stream_id = kSourceStreamId;
    config.limits.maximum_phase_products = 1U;
    market::MarketDecoderV1 decoder(config);
    context->Expect(
        decoder.configuration_valid(),
        "one-product phase-map limit is a valid configuration");

    ShanghaiTickSpec status;
    status.type = "S";
    status.tick_flag = "TRADE";
    status.security_id = "600000";
    std::vector<std::byte> body = MakeShanghaiTickWire(status);
    market::DecodedMarketEventV1 event;
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                570U),
            &event) == market::MarketDecodeErrorV1::kNone,
        "first SH product occupies the sole phase-map slot");

    status.security_id = "600001";
    body = MakeShanghaiTickWire(status);
    market::DecodedMarketEventV1 unchanged = SentinelOutput();
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                571U),
            &unchanged) == market::MarketDecodeErrorV1::
                               kPhaseProductLimitExceeded &&
            IsSentinel(unchanged) &&
            market::MarketDecodeQualityFlagsV1(
                market::MarketDecodeErrorV1::
                    kPhaseProductLimitExceeded) == 0U,
        "phase-map capacity fails atomically without fake offset quality");

    status.security_id = "600000";
    status.tick_flag = "SUSP";
    body = MakeShanghaiTickWire(status);
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                572U),
            &event) == market::MarketDecodeErrorV1::kNone,
        "existing SH product phase update remains allowed at capacity");
    const auto* const tick =
        std::get_if<market::ShanghaiTickV1>(&event);
    if (tick != nullptr) {
        context->Expect(
            tick->fields.phase == market::TradingPhaseV1::kSuspended &&
                (tick->fields.validity_bitmap &
                 market::kTickPhaseValidV1) != 0U,
            "existing phase-map entry updates to the new known phase");
    }
}

void TestStatelessOrderedFinalizeEquivalence(TestContext* context) {
    market::MarketDecoderV1 legacy = MakeDecoder();
    market::MarketDecoderV1 split = MakeDecoder();
    struct Step final {
        const char* type;
        const char* tick_flag;
    };
    const std::array<Step, 9U> steps{{
        {"S", "TRADE"},
        {"A", "B"},
        {"D", "S"},
        {"T", "N"},
        {"X", "?"},
        {"S", "UNKNOWN"},
        {"A", "B"},
        {"S", "SUSP"},
        {"A", "S"},
    }};

    std::uint64_t sequence = 3'000U;
    for (const Step& step : steps) {
        ShanghaiTickSpec spec;
        spec.type = step.type;
        spec.tick_flag = step.tick_flag;
        const std::vector<std::byte> body = MakeShanghaiTickWire(spec);
        const market::MarketMessageViewV1 input = Message(
            kShanghaiService,
            kShanghaiTickMessage,
            body,
            sequence++);

        market::DecodedMarketEventV1 legacy_event = SentinelOutput();
        market::DecodedMarketEventV1 split_event = SentinelOutput();
        const market::MarketDecodeErrorV1 legacy_error =
            legacy.Decode(input, &legacy_event);
        const market::MarketDecodeErrorV1 parse_error =
            split.DecodeStateless(input, &split_event);
        context->Expect(
            legacy_error == market::MarketDecodeErrorV1::kNone &&
                parse_error == legacy_error,
            "split SH tick stateless parse matches legacy success");
        if (parse_error != market::MarketDecodeErrorV1::kNone) {
            continue;
        }

        const auto* const parsed_tick =
            std::get_if<market::ShanghaiTickV1>(&split_event);
        if (parsed_tick != nullptr && parsed_tick->raw_type != "S") {
            context->Expect(
                parsed_tick->fields.phase ==
                    market::TradingPhaseV1::kUnknown &&
                    (parsed_tick->fields.validity_bitmap &
                     market::kTickPhaseValidV1) == 0U,
                "stateless non-status SH tick does not read phase history");
        }

        const market::MarketDecodeErrorV1 finalize_error =
            split.FinalizeInSourceOrder(&split_event);
        context->Expect(
            finalize_error == legacy_error &&
                SameShanghaiTick(legacy_event, split_event),
            "stateless parse plus ordered finalize is field-exact legacy SH decode");
    }

    market::MarketDecoderConfigV1 limited_config;
    limited_config.trade_date = kTradeDate;
    limited_config.source_stream_id = kSourceStreamId;
    limited_config.limits.maximum_phase_products = 1U;
    market::MarketDecoderV1 limited(limited_config);

    ShanghaiTickSpec status;
    status.type = "S";
    status.tick_flag = "TRADE";
    status.security_id = "600000";
    std::vector<std::byte> body = MakeShanghaiTickWire(status);
    market::DecodedMarketEventV1 event = SentinelOutput();
    context->Expect(
        limited.DecodeStateless(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                4'000U),
            &event) == market::MarketDecodeErrorV1::kNone &&
            limited.FinalizeInSourceOrder(&event) ==
                market::MarketDecodeErrorV1::kNone,
        "split decoder seeds one ordered SH phase product");

    status.security_id = "600001";
    body = MakeShanghaiTickWire(status);
    context->Expect(
        limited.DecodeStateless(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                body,
                4'001U),
            &event) == market::MarketDecodeErrorV1::kNone,
        "stateless parse is independent of ordered phase capacity");
    const market::DecodedMarketEventV1 before_finalize = event;
    context->Expect(
        limited.FinalizeInSourceOrder(&event) ==
                market::MarketDecodeErrorV1::
                    kPhaseProductLimitExceeded &&
            SameShanghaiTick(before_finalize, event),
        "ordered phase-capacity failure leaves split event unchanged");
}

void TestConcurrentStatelessDecode(TestContext* context) {
    constexpr std::size_t kWorkerCount = 8U;
    constexpr std::size_t kIterations = 200U;

    market::MarketDecoderConfigV1 config;
    config.trade_date = kTradeDate;
    config.source_stream_id = kSourceStreamId;
    config.limits.maximum_phase_products = 1U;
    market::MarketDecoderV1 decoder(config);

    ShanghaiTickSpec seed;
    seed.type = "S";
    seed.tick_flag = "TRADE";
    seed.security_id = "600000";
    std::vector<std::byte> seed_body = MakeShanghaiTickWire(seed);
    market::DecodedMarketEventV1 seed_event = SentinelOutput();
    context->Expect(
        decoder.DecodeStateless(
            Message(
                kShanghaiService,
                kShanghaiTickMessage,
                seed_body,
                1U),
            &seed_event) == market::MarketDecodeErrorV1::kNone &&
            decoder.FinalizeInSourceOrder(&seed_event) ==
                market::MarketDecodeErrorV1::kNone,
        "concurrent stateless test seeds ordered phase history");

    std::array<std::vector<std::byte>, kWorkerCount> status_bodies;
    std::array<std::vector<std::byte>, kWorkerCount> add_bodies;
    for (std::size_t worker = 0U; worker < kWorkerCount; ++worker) {
        ShanghaiTickSpec status;
        status.type = "S";
        status.tick_flag = "SUSP";
        status.security_id =
            std::to_string(610000U + static_cast<unsigned int>(worker));
        status_bodies[worker] = MakeShanghaiTickWire(status);

        ShanghaiTickSpec add;
        add.type = "A";
        add.tick_flag = "B";
        add.security_id = "600000";
        add_bodies[worker] = MakeShanghaiTickWire(add);
    }

    std::array<std::uint32_t, kWorkerCount> failures{};
    std::array<market::DecodedMarketEventV1, kWorkerCount> retained;
    std::barrier start(static_cast<std::ptrdiff_t>(kWorkerCount));
    std::vector<std::thread> workers;
    workers.reserve(kWorkerCount);
    for (std::size_t worker = 0U; worker < kWorkerCount; ++worker) {
        workers.emplace_back([&, worker]() {
            start.arrive_and_wait();
            for (std::size_t iteration = 0U;
                 iteration < kIterations;
                 ++iteration) {
                const bool parse_status = (iteration % 2U) == 0U;
                const std::vector<std::byte>& body = parse_status
                    ? status_bodies[worker]
                    : add_bodies[worker];
                const std::uint64_t source_sequence =
                    10'000U +
                    static_cast<std::uint64_t>(iteration) *
                        static_cast<std::uint64_t>(kWorkerCount) +
                    static_cast<std::uint64_t>(worker);
                market::DecodedMarketEventV1 parsed{
                    market::ShanghaiTickV1{}};
                if (decoder.DecodeStateless(
                        Message(
                            kShanghaiService,
                            kShanghaiTickMessage,
                            body,
                            source_sequence),
                        &parsed) != market::MarketDecodeErrorV1::kNone) {
                    ++failures[worker];
                    continue;
                }
                const auto* const tick =
                    std::get_if<market::ShanghaiTickV1>(&parsed);
                if (tick == nullptr ||
                    (parse_status &&
                     (tick->fields.phase !=
                          market::TradingPhaseV1::kSuspended ||
                      (tick->fields.validity_bitmap &
                       market::kTickPhaseValidV1) == 0U)) ||
                    (!parse_status &&
                     (tick->fields.phase !=
                          market::TradingPhaseV1::kUnknown ||
                      (tick->fields.validity_bitmap &
                       market::kTickPhaseValidV1) != 0U))) {
                    ++failures[worker];
                }
            }

            market::DecodedMarketEventV1 parsed{
                market::ShanghaiTickV1{}};
            const market::MarketDecodeErrorV1 error =
                decoder.DecodeStateless(
                    Message(
                        kShanghaiService,
                        kShanghaiTickMessage,
                        add_bodies[worker],
                        2U + static_cast<std::uint64_t>(worker)),
                    &parsed);
            if (error != market::MarketDecodeErrorV1::kNone) {
                ++failures[worker];
            }
            retained[worker] = std::move(parsed);
        });
    }
    for (std::thread& worker : workers) {
        worker.join();
    }

    for (std::size_t worker = 0U; worker < kWorkerCount; ++worker) {
        context->Expect(
            failures[worker] == 0U,
            "same decoder instance supports concurrent stateless SH parses");
        context->Expect(
            decoder.FinalizeInSourceOrder(&retained[worker]) ==
                market::MarketDecodeErrorV1::kNone,
            "concurrently parsed SH tick finalizes in source order");
        const auto* const tick =
            std::get_if<market::ShanghaiTickV1>(&retained[worker]);
        context->Expect(
            tick != nullptr &&
                tick->fields.phase ==
                    market::TradingPhaseV1::kContinuous &&
                (tick->fields.validity_bitmap &
                 market::kTickPhaseValidV1) != 0U,
            "ordered finalizer alone applies stored SH phase");
    }
}

void TestTimeNullInvalidAndBoundaries(TestContext* context) {
    market::MarketDecoderV1 decoder = MakeDecoder();
    std::uint64_t sequence = 400U;

    ShenzhenOrderSpec spec;
    spec.time = 1000000000U;
    std::vector<std::byte> body = MakeShenzhenOrderWire(spec);
    market::DecodedMarketEventV1 event;
    context->Expect(
        decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenOrderMessage,
                body,
                sequence++,
                kSupportedVersion,
                1000000000U),
            &event) == market::MarketDecodeErrorV1::kNone,
        "null exchange/vendor times do not reject the message");
    const auto* order = std::get_if<market::ShenzhenOrderV1>(&event);
    if (order != nullptr) {
        context->Expect(
            order->common.exchange_time.is_null &&
                !order->common.exchange_time.valid &&
                order->common.vendor_local_time.is_null &&
                !order->common.vendor_local_time.valid &&
                HasQuality(
                    order->common,
                    control::QualityFlagV1::kNullValuePresent),
            "time null sentinel is distinct from zero and sets null quality");
    }

    spec.time = 240000000U;
    body = MakeShenzhenOrderWire(spec);
    context->Expect(
        decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenOrderMessage,
                body,
                sequence++,
                kSupportedVersion,
                126000000U),
            &event) == market::MarketDecodeErrorV1::kNone,
        "invalid exchange/vendor times do not reject the message");
    order = std::get_if<market::ShenzhenOrderV1>(&event);
    if (order != nullptr) {
        context->Expect(
            !order->common.exchange_time.valid &&
                !order->common.exchange_time.is_null &&
                !order->common.vendor_local_time.valid &&
                !order->common.vendor_local_time.is_null &&
                HasNotice(
                    order->common,
                    market::MarketNoticeV1::kExchangeTimeInvalid) &&
                HasNotice(
                    order->common,
                    market::MarketNoticeV1::kVendorLocalTimeInvalid),
            "invalid time components set the two explicit notices");
    }

    spec.time = 0U;
    body = MakeShenzhenOrderWire(spec);
    context->Expect(
        decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenOrderMessage,
                body,
                sequence++,
                kSupportedVersion,
                0U),
            &event) == market::MarketDecodeErrorV1::kNone,
        "00:00:00.000 boundary decodes");
    order = std::get_if<market::ShenzhenOrderV1>(&event);
    if (order != nullptr) {
        context->Expect(
            order->common.exchange_time.valid &&
                order->common.exchange_time.nanoseconds_since_midnight ==
                    0U &&
                order->common.exchange_time.unix_nanoseconds_valid &&
                order->common.exchange_time.unix_nanoseconds ==
                    1784649600000000000LL,
            "lower time boundary maps to exact UTC+08 Unix nanoseconds");
    }

    spec.time = 235959999U;
    body = MakeShenzhenOrderWire(spec);
    context->Expect(
        decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenOrderMessage,
                body,
                sequence++,
                kSupportedVersion,
                235959999U),
            &event) == market::MarketDecodeErrorV1::kNone,
        "23:59:59.999 boundary decodes");
    order = std::get_if<market::ShenzhenOrderV1>(&event);
    if (order != nullptr) {
        context->Expect(
            order->common.exchange_time.valid &&
                order->common.exchange_time.nanoseconds_since_midnight ==
                    86399999000000ULL &&
                order->common.exchange_time.unix_nanoseconds_valid &&
                order->common.exchange_time.unix_nanoseconds ==
                    1784735999999000000LL,
            "upper time boundary maps exactly without millisecond loss");
        context->Expect(
            order->common.vendor_local_time.valid &&
                order->common.vendor_local_time.nanoseconds_since_midnight ==
                    86399999000000ULL &&
                !order->common.vendor_local_time.unix_nanoseconds_valid,
            "vendor local time is valid only as time-of-day, not a dated instant");
    }
}

void TestAbsolutePriceDomains(TestContext* context) {
    market::MarketDecoderV1 decoder = MakeDecoder();
    std::uint64_t sequence = 600U;
    market::DecodedMarketEventV1 event;

    std::vector<std::byte> body = MakeShanghaiSnapshotWire();
    OverwriteI32(&body, wire_abi::sh_snapshot::kLastPrice, 0);
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiSnapshotMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH snapshot zero last price remains a decoded audit record");
    const auto* sh_snapshot =
        std::get_if<market::ShanghaiSnapshotV1>(&event);
    context->Expect(
        sh_snapshot != nullptr && sh_snapshot->last_price.raw == 0 &&
            sh_snapshot->last_price.scale == 3U &&
            sh_snapshot->last_price.normalized_p6 == 0 &&
            !sh_snapshot->last_price.valid &&
            !sh_snapshot->last_price.is_null &&
            HasNotice(
                sh_snapshot->common,
                market::MarketNoticeV1::kAbsolutePriceDomainInvalid),
        "SH zero last price is retained but never advertised as a formed price");

    body = MakeShanghaiSnapshotWire();
    OverwriteI32(
        &body,
        wire_abi::sh_snapshot::kLastPrice,
        std::numeric_limits<std::int32_t>::min());
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiSnapshotMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH snapshot null last price remains a decoded audit record");
    sh_snapshot = std::get_if<market::ShanghaiSnapshotV1>(&event);
    context->Expect(
        sh_snapshot != nullptr && sh_snapshot->last_price.is_null &&
            !sh_snapshot->last_price.valid &&
            HasQuality(
                sh_snapshot->common,
                control::QualityFlagV1::kNullValuePresent) &&
            !HasNotice(
                sh_snapshot->common,
                market::MarketNoticeV1::kAbsolutePriceDomainInvalid),
        "SH null price is distinct from a nonpositive price-domain failure");

    body = MakeShenzhenSnapshotWire();
    const std::int64_t negative_extreme =
        std::numeric_limits<std::int64_t>::min() + 1;
    OverwriteI64(
        &body,
        wire_abi::sz_snapshot::kPreClosePrice,
        negative_extreme);
    context->Expect(
        decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenSnapshotMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SZ negative extreme scale-4 price is domain-invalid, not overflow-fatal");
    const auto* sz_snapshot =
        std::get_if<market::ShenzhenSnapshotV1>(&event);
    context->Expect(
        sz_snapshot != nullptr &&
            sz_snapshot->pre_close_price.raw == negative_extreme &&
            sz_snapshot->pre_close_price.scale == 4U &&
            sz_snapshot->pre_close_price.normalized_p6 == 0 &&
            !sz_snapshot->pre_close_price.valid &&
            HasNotice(
                sz_snapshot->common,
                market::MarketNoticeV1::kAbsolutePriceDomainInvalid),
        "SZ invalid negative extreme is retained before p6 multiplication");

    // Golden construction order puts the first SH bid at fixed body + the
    // 6-byte SecurityID and 5-byte status strings; price is item offset +4.
    constexpr std::size_t kFirstShanghaiBidPrice = 263U;
    body = MakeShanghaiSnapshotWire();
    OverwriteI32(&body, kFirstShanghaiBidPrice, -1);
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiSnapshotMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH book level with negative price remains decodable");
    sh_snapshot = std::get_if<market::ShanghaiSnapshotV1>(&event);
    context->Expect(
        sh_snapshot != nullptr &&
            sh_snapshot->book.bids[0U].price.raw == -1 &&
            !sh_snapshot->book.bids[0U].price.valid &&
            HasNotice(
                sh_snapshot->common,
                market::MarketNoticeV1::kAbsolutePriceDomainInvalid),
        "SH book price uses the same strict positive domain contract");

    // The existing nested-offset oracle establishes the first SZ bid at 237;
    // its price is the int64 at item offset +8.
    constexpr std::size_t kFirstShenzhenBidPrice = 245U;
    body = MakeShenzhenSnapshotWire();
    OverwriteI64(&body, kFirstShenzhenBidPrice, 0);
    context->Expect(
        decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenSnapshotMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SZ book level with zero price remains decodable");
    sz_snapshot = std::get_if<market::ShenzhenSnapshotV1>(&event);
    context->Expect(
        sz_snapshot != nullptr &&
            sz_snapshot->book.bids[0U].price.raw == 0 &&
            !sz_snapshot->book.bids[0U].price.valid &&
            HasNotice(
                sz_snapshot->common,
                market::MarketNoticeV1::kAbsolutePriceDomainInvalid),
        "SZ book zero price is retained but invalid");

    struct ShanghaiTickPriceCase final {
        std::string type;
        std::int32_t price;
        bool expect_domain_notice;
        bool expect_null;
        const char* label;
    };
    const std::vector<ShanghaiTickPriceCase> sh_tick_cases = {
        {"A", 0, true, false, "SH add zero price"},
        {"T", -1, true, false, "SH trade negative price"},
        {"A",
         std::numeric_limits<std::int32_t>::min(),
         false,
         true,
         "SH add null price"},
        {"D", -1, false, false, "SH cancel ignored negative price"},
        {"S", -1, false, false, "SH status ignored negative price"},
    };
    for (const ShanghaiTickPriceCase& test_case : sh_tick_cases) {
        ShanghaiTickSpec spec;
        spec.type = test_case.type;
        spec.price_p3 = test_case.price;
        body = MakeShanghaiTickWire(spec);
        context->Expect(
            decoder.Decode(
                Message(
                    kShanghaiService,
                    kShanghaiTickMessage,
                    body,
                    sequence++),
                &event) == market::MarketDecodeErrorV1::kNone,
            std::string(test_case.label) + " decodes");
        const auto* const tick =
            std::get_if<market::ShanghaiTickV1>(&event);
        context->Expect(
            tick != nullptr && !tick->fields.price.valid &&
                (tick->fields.validity_bitmap &
                 market::kTickPriceValidV1) == 0U &&
                tick->fields.price.is_null == test_case.expect_null &&
                HasNotice(
                    tick->common,
                    market::MarketNoticeV1::
                        kAbsolutePriceDomainInvalid) ==
                    test_case.expect_domain_notice,
            std::string(test_case.label) +
                " keeps value validity, bitmap, and notice consistent");
    }

    ShenzhenOrderSpec limit;
    limit.order_type = 50;
    limit.price_p4 = negative_extreme;
    body = MakeShenzhenOrderWire(limit);
    context->Expect(
        decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenOrderMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SZ limit negative extreme price is domain-invalid, not overflow-fatal");
    const auto* order = std::get_if<market::ShenzhenOrderV1>(&event);
    context->Expect(
        order != nullptr && order->fields.price.raw == negative_extreme &&
            !order->fields.price.valid &&
            (order->fields.validity_bitmap &
             market::kTickPriceValidV1) == 0U &&
            HasNotice(
                order->common,
                market::MarketNoticeV1::kAbsolutePriceDomainInvalid),
        "SZ limit price gate precedes p6 normalization");

    ShenzhenOrderSpec market_order;
    market_order.order_type = 49;
    market_order.price_p4 = negative_extreme;
    body = MakeShenzhenOrderWire(market_order);
    context->Expect(
        decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenOrderMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SZ market-order ignored negative price does not overflow");
    order = std::get_if<market::ShenzhenOrderV1>(&event);
    context->Expect(
        order != nullptr && !order->fields.price.valid &&
            !HasNotice(
                order->common,
                market::MarketNoticeV1::kAbsolutePriceDomainInvalid),
        "SZ market-order price placeholder does not emit a false domain notice");

    ShenzhenTransactionSpec trade;
    trade.execution_type = 70;
    trade.last_price_p4 = 0;
    body = MakeShenzhenTransactionWire(trade);
    context->Expect(
        decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenTransactionMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SZ trade zero price remains a decoded audit record");
    const auto* transaction =
        std::get_if<market::ShenzhenTransactionV1>(&event);
    context->Expect(
        transaction != nullptr && !transaction->fields.price.valid &&
            (transaction->fields.validity_bitmap &
             market::kTickPriceValidV1) == 0U &&
            HasNotice(
                transaction->common,
                market::MarketNoticeV1::kAbsolutePriceDomainInvalid),
        "SZ trade zero price is retained but invalid");

    trade.last_price_p4 = std::numeric_limits<std::int64_t>::min();
    body = MakeShenzhenTransactionWire(trade);
    context->Expect(
        decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenTransactionMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SZ trade null price remains a decoded audit record");
    transaction = std::get_if<market::ShenzhenTransactionV1>(&event);
    context->Expect(
        transaction != nullptr && transaction->fields.price.is_null &&
            !transaction->fields.price.valid &&
            !HasNotice(
                transaction->common,
                market::MarketNoticeV1::kAbsolutePriceDomainInvalid),
        "SZ trade null price is not misreported as a price-domain failure");

    ShenzhenTransactionSpec cancel;
    cancel.execution_type = 52;
    cancel.last_price_p4 = negative_extreme;
    body = MakeShenzhenTransactionWire(cancel);
    context->Expect(
        decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenTransactionMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SZ cancel ignored negative price remains decodable");
    transaction = std::get_if<market::ShenzhenTransactionV1>(&event);
    context->Expect(
        transaction != nullptr && !transaction->fields.price.valid &&
            !HasNotice(
                transaction->common,
                market::MarketNoticeV1::kAbsolutePriceDomainInvalid),
        "SZ cancel price placeholder does not emit a false domain notice");
}

void TestMaximumDurationSentinel(TestContext* context) {
    market::MarketDecoderV1 decoder = MakeDecoder();
    std::uint64_t sequence = 700U;
    market::DecodedMarketEventV1 event;

    std::vector<std::byte> body = MakeShanghaiSnapshotWire();
    OverwriteU32(
        &body,
        wire_abi::sh_snapshot::kMaximumBidDuration,
        std::numeric_limits<std::uint32_t>::max());
    OverwriteU32(
        &body,
        wire_abi::sh_snapshot::kMaximumAskDuration,
        7U);
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiSnapshotMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH bid duration sentinel remains a decoded audit record");
    const auto* snapshot =
        std::get_if<market::ShanghaiSnapshotV1>(&event);
    context->Expect(
        snapshot != nullptr &&
            snapshot->maximum_bid_duration.raw ==
                std::numeric_limits<std::uint32_t>::max() &&
            !snapshot->maximum_bid_duration.valid &&
            snapshot->maximum_ask_duration.raw == 7U &&
            snapshot->maximum_ask_duration.valid &&
            HasNotice(
                snapshot->common,
                market::MarketNoticeV1::kMaximumDurationUnavailable),
        "SH bid and ask duration validity is independent");

    body = MakeShanghaiSnapshotWire();
    OverwriteU32(
        &body,
        wire_abi::sh_snapshot::kMaximumBidDuration,
        8U);
    OverwriteU32(
        &body,
        wire_abi::sh_snapshot::kMaximumAskDuration,
        std::numeric_limits<std::uint32_t>::max());
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiSnapshotMessage,
                body,
                sequence++),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH ask duration sentinel remains a decoded audit record");
    snapshot = std::get_if<market::ShanghaiSnapshotV1>(&event);
    context->Expect(
        snapshot != nullptr && snapshot->maximum_bid_duration.raw == 8U &&
            snapshot->maximum_bid_duration.valid &&
            snapshot->maximum_ask_duration.raw ==
                std::numeric_limits<std::uint32_t>::max() &&
            !snapshot->maximum_ask_duration.valid &&
            HasNotice(
                snapshot->common,
                market::MarketNoticeV1::kMaximumDurationUnavailable),
        "SH ask sentinel does not invalidate the independent bid duration");
}

void TestSnapshotNestedListsAndPublicCaps(TestContext* context) {
    market::MarketDecoderV1 decoder = MakeDecoder();
    std::vector<std::byte> sh_body = MakeShanghaiSnapshotWire();
    market::DecodedMarketEventV1 event;
    context->Expect(
        decoder.Decode(
            Message(
                kShanghaiService,
                kShanghaiSnapshotMessage,
                sh_body,
                500U),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SH snapshot with nested descriptor-relative queue decodes");
    const auto* const sh_snapshot =
        std::get_if<market::ShanghaiSnapshotV1>(&event);
    if (sh_snapshot != nullptr) {
        const market::SnapshotBookV1& book = sh_snapshot->book;
        context->Expect(
            sh_snapshot->vendor_war_lower_value.raw == 123456 &&
                sh_snapshot->vendor_war_lower_value.scale == 3U &&
                !sh_snapshot->vendor_war_lower_value.valid &&
                HasNotice(
                    sh_snapshot->common,
                    market::MarketNoticeV1::
                        kVendorWarLowerSemanticsUnknown) &&
                sh_snapshot->vendor_war_upper_value.raw == 654321 &&
                sh_snapshot->vendor_war_upper_value.scale == 5U &&
                !sh_snapshot->vendor_war_upper_value.valid &&
                HasNotice(
                    sh_snapshot->common,
                    market::MarketNoticeV1::
                        kVendorWarUpperSemanticsUnknown),
            "SH vendor War fields retain raw values without guessed semantics");
        context->Expect(
            sh_snapshot->vendor_etf_buy_count.raw == 17U &&
                !sh_snapshot->vendor_etf_buy_count.valid &&
                sh_snapshot->vendor_etf_buy_quantity.raw == 21000 &&
                sh_snapshot->vendor_etf_buy_quantity.scale == 3U &&
                !sh_snapshot->vendor_etf_buy_quantity.valid &&
                sh_snapshot->vendor_etf_buy_amount.raw == 3'210'000 &&
                sh_snapshot->vendor_etf_buy_amount.scale == 5U &&
                !sh_snapshot->vendor_etf_buy_amount.valid &&
                sh_snapshot->vendor_etf_buy_amount.normalized_p6 == 0 &&
                sh_snapshot->vendor_etf_sell_count.raw == 19U &&
                !sh_snapshot->vendor_etf_sell_count.valid &&
                sh_snapshot->vendor_etf_sell_quantity.raw == 23000 &&
                sh_snapshot->vendor_etf_sell_quantity.scale == 3U &&
                !sh_snapshot->vendor_etf_sell_quantity.valid &&
                sh_snapshot->vendor_etf_sell_amount.raw == 4'560'000 &&
                sh_snapshot->vendor_etf_sell_amount.scale == 5U &&
                !sh_snapshot->vendor_etf_sell_amount.valid &&
                sh_snapshot->yield_to_maturity.raw == -125 &&
                sh_snapshot->yield_to_maturity.scale == 4U &&
                !sh_snapshot->yield_to_maturity.valid &&
                sh_snapshot->total_warrant_exercise_quantity.raw == 7000 &&
                sh_snapshot->total_warrant_exercise_quantity.scale == 3U &&
                !sh_snapshot->total_warrant_exercise_quantity.valid &&
                sh_snapshot->iopv.raw == 10123 &&
                sh_snapshot->iopv.scale == 3U &&
                !sh_snapshot->iopv.valid &&
                HasNotice(
                    sh_snapshot->common,
                    market::MarketNoticeV1::kProductApplicabilityUnknown),
            "SH product-specific fields retain nonzero raw values without inferred applicability");
        context->Expect(
            sh_snapshot->maximum_bid_duration.raw == 0U &&
                sh_snapshot->maximum_bid_duration.valid &&
                sh_snapshot->maximum_ask_duration.raw ==
                    std::numeric_limits<std::uint32_t>::max() - 1U &&
                sh_snapshot->maximum_ask_duration.valid &&
                !HasNotice(
                    sh_snapshot->common,
                    market::MarketNoticeV1::kMaximumDurationUnavailable),
            "SH duration zero and UINT32_MAX-1 remain independent valid raw values");
        context->Expect(
            book.actual_bid_depth == 12U &&
                book.retained_bid_depth == 10U &&
                book.actual_ask_depth == 1U &&
                book.retained_ask_depth == 1U &&
                HasNotice(
                    sh_snapshot->common,
                    market::MarketNoticeV1::kSnapshotDepthTruncatedTo10),
            "SH depth >10 is counted, capped, and explicitly noticed");
        context->Expect(
            book.bids[0].price.valid &&
                book.bids[0].price.raw == 10200 &&
                book.bids[9].price.raw == 10191 &&
                book.bids[0].order_count_valid &&
                book.bids[0].order_count == 77U,
            "SH retained ten levels preserve order and fixed fields");
        context->Expect(
            book.bid1_queue.total_order_count == 77U &&
                book.bid1_queue.actual_revealed_count == 52U &&
                book.bid1_queue.retained_count == 50U &&
                book.bid1_queue.quantities[0].raw == 1000 &&
                book.bid1_queue.quantities[49].raw == 50000 &&
                HasQuality(
                    sh_snapshot->common,
                    control::QualityFlagV1::kQueueTruncatedTo50),
            "SH nested queue >50 is retained to 50 with quality");
        context->Expect(
            book.ask1_queue.total_order_count == 2U &&
                book.ask1_queue.actual_revealed_count == 2U &&
                book.ask1_queue.retained_count == 2U &&
                book.ask1_queue.quantities[0].raw == 3000 &&
                book.ask1_queue.quantities[1].raw == 4000,
            "SH ask-one nested queue uses its own descriptor base");
    }

    std::vector<std::byte> sz_body = MakeShenzhenSnapshotWire();
    context->Expect(
        decoder.Decode(
            Message(
                kShenzhenService,
                kShenzhenSnapshotMessage,
                sz_body,
                501U),
            &event) == market::MarketDecodeErrorV1::kNone,
        "SZ snapshot with two nested queues decodes");
    const auto* const sz_snapshot =
        std::get_if<market::ShenzhenSnapshotV1>(&event);
    if (sz_snapshot != nullptr) {
        const market::SnapshotBookV1& book = sz_snapshot->book;
        context->Expect(
            sz_snapshot->high_limit_price.raw ==
                    kUnresolvedHighLimitRaw &&
                sz_snapshot->high_limit_price.scale == 6U &&
                !sz_snapshot->high_limit_price.valid &&
                !sz_snapshot->high_limit_price.is_null &&
                sz_snapshot->low_limit_price.raw ==
                    kUnresolvedLowLimitRaw &&
                sz_snapshot->low_limit_price.scale == 6U &&
                !sz_snapshot->low_limit_price.valid &&
                !sz_snapshot->low_limit_price.is_null &&
                sz_snapshot->high_limit_semantics ==
                    market::LimitPriceSemanticsV1::kUnknown &&
                sz_snapshot->low_limit_semantics ==
                    market::LimitPriceSemanticsV1::kUnknown &&
                HasNotice(
                    sz_snapshot->common,
                    market::MarketNoticeV1::
                        kLimitPriceSemanticsUnknown),
            "SZ unresolved limit raw extremes are retained but not factor-safe");
        context->Expect(
            sz_snapshot->pe_ratio_1.raw == -123456 &&
                sz_snapshot->pe_ratio_1.scale == 6U &&
                !sz_snapshot->pe_ratio_1.valid &&
                sz_snapshot->pe_ratio_1.normalized_p6 == 0 &&
                sz_snapshot->pe_ratio_2.raw == 654321 &&
                sz_snapshot->pe_ratio_2.scale == 6U &&
                !sz_snapshot->pe_ratio_2.valid &&
                sz_snapshot->pre_close_iopv.raw == 1'000'001 &&
                sz_snapshot->pre_close_iopv.scale == 6U &&
                !sz_snapshot->pre_close_iopv.valid &&
                sz_snapshot->iopv.raw == 1'000'002 &&
                sz_snapshot->iopv.scale == 6U &&
                !sz_snapshot->iopv.valid &&
                sz_snapshot->open_interest.raw == 77 &&
                sz_snapshot->open_interest.scale == 0U &&
                !sz_snapshot->open_interest.valid &&
                sz_snapshot->vendor_opt_premium_ratio.raw == 88'000 &&
                sz_snapshot->vendor_opt_premium_ratio.scale == 6U &&
                !sz_snapshot->vendor_opt_premium_ratio.valid &&
                HasNotice(
                    sz_snapshot->common,
                    market::MarketNoticeV1::kProductApplicabilityUnknown) &&
                HasNotice(
                    sz_snapshot->common,
                    market::MarketNoticeV1::
                        kVendorOptPremiumRatioSemanticsUnknown),
            "SZ product-specific fields retain nonzero raw values without inferred applicability");
        context->Expect(
            book.actual_bid_depth == 2U &&
                book.retained_bid_depth == 2U &&
                book.actual_ask_depth == 1U &&
                book.retained_ask_depth == 1U,
            "SZ nested lists retain their actual depths");
        context->Expect(
            book.bids[0].quantity.raw == 101 &&
                book.bids[0].price.raw == 1234567 &&
                book.bids[1].quantity.raw == 202 &&
                book.asks[0].price.raw == 1234570,
            "SZ bid/ask items retain descriptor-relative list order");
        context->Expect(
            book.bid1_queue.total_order_count == 9U &&
                book.bid1_queue.actual_revealed_count == 2U &&
                book.bid1_queue.retained_count == 2U &&
                book.bid1_queue.quantities[0].raw == 111 &&
                book.bid1_queue.quantities[1].raw == 222 &&
                book.ask1_queue.total_order_count == 4U &&
                book.ask1_queue.quantities[0].raw == 333,
            "SZ bid-one and ask-one nested queue offsets use each item base");
    }
}

void TestDecoderTradeDateDomain(TestContext* context) {
    const auto valid = [](std::uint32_t trade_date) {
        market::MarketDecoderConfigV1 config{};
        config.trade_date = trade_date;
        config.source_stream_id = kSourceStreamId;
        return market::MarketDecoderV1(config).configuration_valid();
    };
    context->Expect(
        valid(19920101U) && valid(19920229U) && valid(20000229U) &&
            valid(22001231U),
        "decoder accepts documented endpoints and Gregorian leap dates");
    context->Expect(
        !valid(19911231U) && !valid(19930229U) && !valid(21000229U) &&
            !valid(22010101U),
        "decoder rejects out-of-domain and non-leap trade dates");
}

}  // namespace

int main() {
    TestContext context;
    TestFixedLowerBoundsAndSchemaGate(&context);
    TestShanghaiTickValidityMatrix(&context);
    TestShenzhenOrderEnumsAndPriceValidity(&context);
    TestShenzhenTransactionMatrix(&context);
    TestIgnoredNumericOverflowAndPhaseHistory(&context);
    TestMatchedQuantityDomainAndFailureAtomicity(&context);
    TestNegativeQuantityDomains(&context);
    TestOrderReferenceDomains(&context);
    TestPhaseProductLimit(&context);
    TestStatelessOrderedFinalizeEquivalence(&context);
    TestConcurrentStatelessDecode(&context);
    TestTimeNullInvalidAndBoundaries(&context);
    TestAbsolutePriceDomains(&context);
    TestMaximumDurationSentinel(&context);
    TestSnapshotNestedListsAndPublicCaps(&context);
    TestDecoderTradeDateDomain(&context);

    if (context.failures != 0) {
        std::cerr << context.failures
                  << " market decoder test(s) failed\n";
        return 1;
    }
    std::cout << "Market decoder tests passed\n";
    return 0;
}
