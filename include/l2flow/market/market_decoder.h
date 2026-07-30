#pragma once

#include "l2flow/market/market_types_v1.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <span>
#include <string>

namespace l2flow::market {

// Borrowed exact identity extracted from one of the five production message
// bodies. The spans point into MarketMessageViewV1::body and are valid only
// while that body remains alive. Extraction performs no allocation and is
// used by callback admission for immutable daily-catalog lookup before the
// pooled message is published to its source decoder lane.
struct ExactInstrumentKeyViewV2 final {
    MarketV1 market = MarketV1::kUnknown;
    std::span<const std::byte> security_id_source{};
    std::span<const std::byte> security_id{};
};

struct DailyInstrumentIdentityViewV2 final {
    ExactInstrumentKeyViewV2 key{};
    std::uint32_t instrument_id = 0U;
    std::size_t ordinal = std::numeric_limits<std::size_t>::max();
    QuantityUnitV1 quantity_unit = QuantityUnitV1::kUnknown;
    SecurityTypeV1 security_type = SecurityTypeV1::kUnknown;
    AssetScopeV1 asset_scope = AssetScopeV1::kUnknown;
};

enum class MarketDecodeErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kInvalidInput,
    kUnsupportedMessage,
    kUnsupportedServiceVersion,
    kTruncated,
    kOffsetInvalid,
    kRangeOverlap,
    kCountExceeded,
    kCountMismatch,
    kTextInvalid,
    kFixedPointOverflow,
    kPhaseProductLimitExceeded,
    kResourceExhausted,
    kUnexpectedFailure,
};

struct MarketDecoderLimitsV1 final {
    std::size_t maximum_body_bytes = 16U * 1024U * 1024U;
    std::size_t maximum_text_bytes = 4096U;
    std::size_t maximum_depth_items = 4096U;
    std::size_t maximum_queue_items = 1'000'000U;
    // Bounds the stateful SH product-phase map, which is outside retained
    // session-event accounting.  Exhaustion fails closed; entries are never
    // evicted within a decoder's one-day/source lifetime.
    std::size_t maximum_phase_products = 100'000U;
};

struct MarketDecoderConfigV1 final {
    // V1 accepts valid calendar dates from 1992-01-01 through 2200-12-31.
    // Its Unix-time
    // projection is fixed UTC+08:00 and deliberately does not pretend to
    // model older Asia/Shanghai DST history.
    std::uint32_t trade_date = 0U;
    std::uint32_t source_stream_id = 0U;
    MarketDecoderLimitsV1 limits{};
};

// Stateful only for the documented SH 4.24 product-phase attribution.  One
// decoder instance represents exactly one trade-date/source session and is a
// single-writer object.  The caller must invoke Decode in authoritative,
// strictly increasing owned-ingress source_sequence order; this object
// neither reorders nor validates sequence monotonicity on its own. Decoded
// events own every published string/array and never retain
// MarketMessageViewV1::body.
class MarketDecoderV1 final {
public:
    explicit MarketDecoderV1(MarketDecoderConfigV1 config) noexcept;

    MarketDecoderV1(const MarketDecoderV1&) = delete;
    MarketDecoderV1& operator=(const MarketDecoderV1&) = delete;
    MarketDecoderV1(MarketDecoderV1&&) = delete;
    MarketDecoderV1& operator=(MarketDecoderV1&&) = delete;
    ~MarketDecoderV1() = default;

    // kUnsupportedMessage means the tuple is outside this five-message
    // decoder.  Only a caller that has classified it as optional/irrelevant
    // may ignore it; a required unknown tuple must be failed closed upstream.
    [[nodiscard]] MarketDecodeErrorV1 Decode(
        const MarketMessageViewV1& input,
        DecodedMarketEventV1* output) noexcept;

    [[nodiscard]] const MarketDecoderConfigV1& config()
        const noexcept {
        return config_;
    }
    [[nodiscard]] bool configuration_valid() const noexcept {
        return configuration_valid_;
    }

private:
    MarketDecoderConfigV1 config_{};
    bool configuration_valid_ = false;
    std::map<std::string, TradingPhaseV1> sh_phases_;
};

[[nodiscard]] std::uint64_t MarketDecodeQualityFlagsV1(
    MarketDecodeErrorV1 error) noexcept;

// This parses only the exact security key descriptors. It deliberately does
// not perform a second full market decode and therefore keeps callback
// admission bounded. The source decoder remains the single authority for the
// complete body/schema validation.
[[nodiscard]] MarketDecodeErrorV1 ExtractExactInstrumentKeyV2(
    const MarketMessageViewV1& input,
    std::size_t maximum_text_bytes,
    ExactInstrumentKeyViewV2* output) noexcept;

// Applies the immutable daily-catalog identity selected by callback admission
// to the decoded event. It verifies that full decode produced the same exact
// key; no second catalog lookup occurs on a decoder lane.
[[nodiscard]] bool ApplyDailyInstrumentIdentityV2(
    const DailyInstrumentIdentityViewV2& identity,
    DecodedMarketEventV1* event) noexcept;

// Narrow source-compatibility aliases for callers migrating from the former
// observed-universe terminology. New pipeline code uses the daily/exact
// names above; these declarations do not restore dynamic identity binding.
using ObservedInstrumentKeyViewV2 = ExactInstrumentKeyViewV2;
using ObservedInstrumentIdentityViewV2 = DailyInstrumentIdentityViewV2;

[[nodiscard]] inline MarketDecodeErrorV1
ExtractObservedInstrumentKeyV2(
    const MarketMessageViewV1& input,
    std::size_t maximum_text_bytes,
    ObservedInstrumentKeyViewV2* output) noexcept {
    return ExtractExactInstrumentKeyV2(
        input, maximum_text_bytes, output);
}

[[nodiscard]] inline bool ApplyObservedInstrumentIdentityV2(
    const ObservedInstrumentIdentityViewV2& identity,
    DecodedMarketEventV1* event) noexcept {
    return ApplyDailyInstrumentIdentityV2(identity, event);
}

}  // namespace l2flow::market
