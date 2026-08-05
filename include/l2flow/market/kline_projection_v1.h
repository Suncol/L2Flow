#pragma once

#include "l2flow/market/kline_types_v1.h"

#include <cstdint>

namespace l2flow::market {

// Converts only valid Shanghai Tick trades and Shenzhen Transaction trades.
// It never consults receive time, SDK local time, or a process clock.
[[nodiscard]] KLineTradeProjectionV1 ProjectKLineTradeV1(
    const DecodedMarketEventV1& event,
    std::uint64_t arrival_id,
    KLineTradeV1* output) noexcept;

}  // namespace l2flow::market
