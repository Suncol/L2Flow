#pragma once

#include "l2flow/ingress/raw_live_tail.h"
#include "l2flow/ingress/raw_reader.h"
#include "l2flow/market/market_types_v1.h"

#include <cstdint>

namespace l2flow::market {

enum class RawMarketAdapterErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidTradeDate,
    kNamespaceMismatch,
    kInvalidIngressSequence,
    kReceiveTimeOutOfRange,
    kBodySizeMismatch,
};

[[nodiscard]] const char* RawMarketAdapterErrorNameV1(
    RawMarketAdapterErrorV1 error) noexcept;

// Converts one already-validated Raw record into the borrowed market decoder
// view. The returned body span is owned by `record`; callers must complete
// Decode/Retain before that RawRecordView is released. trade_date is an
// explicit business partition and is never inferred from capture_date.
[[nodiscard]] RawMarketAdapterErrorV1 MakeMarketMessageViewFromRawV1(
    const l2flow::ingress::RawRecordView& record,
    std::uint32_t expected_source_stream_id,
    std::uint32_t expected_capture_date,
    std::uint32_t trade_date,
    MarketMessageViewV1* output) noexcept;

// Live records additionally bind the validated segment namespace. This
// overload rejects a record/segment mismatch before delegating to the pure
// RawRecordView mapping above.
[[nodiscard]] RawMarketAdapterErrorV1 MakeMarketMessageViewFromRawV1(
    const l2flow::ingress::RawLiveRecord& record,
    std::uint32_t trade_date,
    MarketMessageViewV1* output) noexcept;

}  // namespace l2flow::market
