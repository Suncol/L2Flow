#pragma once

#include "l2flow/market/market_types_v1.h"

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>

namespace l2flow::market {

class InstrumentRegistryV1;

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
    // Borrowed immutable registry. When non-null, it must outlive this
    // decoder.
    const InstrumentRegistryV1* instrument_registry = nullptr;
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

}  // namespace l2flow::market
