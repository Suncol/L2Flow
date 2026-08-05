#include "l2flow/market/market_types_v1.h"

#include <variant>

namespace l2flow::market {
const DecodedMarketCommonV1& MarketCommonV1(
    const DecodedMarketEventV1& event) noexcept {
    return std::visit(
        [](const auto& value) -> const DecodedMarketCommonV1& {
            return value.common;
        },
        event);
}

DecodedMarketCommonV1& MarketCommonV1(
    DecodedMarketEventV1& event) noexcept {
    return std::visit(
        [](auto& value) -> DecodedMarketCommonV1& {
            return value.common;
        },
        event);
}

}  // namespace l2flow::market
