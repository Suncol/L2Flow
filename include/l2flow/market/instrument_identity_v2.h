#pragma once

#include "l2flow/market/market_types_v1.h"

#include <cstddef>
#include <span>
#include <vector>

namespace l2flow::market {

// Exact opaque SDK identity. No component is trimmed, case-folded,
// transcoded, or inferred from another component.
struct InstrumentKeyV1 final {
    MarketV1 market = MarketV1::kUnknown;
    std::vector<std::byte> security_id_source;
    std::vector<std::byte> security_id;
};

struct InstrumentKeyViewV1 final {
    MarketV1 market = MarketV1::kUnknown;
    std::span<const std::byte> security_id_source;
    std::span<const std::byte> security_id;
};

}  // namespace l2flow::market
