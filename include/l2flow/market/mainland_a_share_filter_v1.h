#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace l2flow::market {

// This exchange identity is deliberately separate from MarketV1.  MarketV1
// describes the exchanges currently supported by the decoder, whereas this
// filter must also be able to classify Beijing Stock Exchange security IDs
// before Beijing decoding is added.
enum class MainlandExchangeV1 : std::uint8_t {
    kUnknown = 0U,
    kShanghai = 1U,
    kShenzhen = 2U,
    kBeijing = 3U,
};

// Rule baseline: 2026-07-30 (SSE 2026 second revision, SZSE 2026-03,
// and the current BSE 920 allocation). Returns true only when security_id is
// exactly six ASCII decimal digits and belongs to an A-share code range for
// the exchange supplied by the caller:
//
//   Shanghai: 600xxx, 601xxx, 603xxx, 605xxx, 688xxx
//   Shenzhen: 000001-000999, 001200-004999, 300000-309799
//   Beijing:  920000-920999
//
// Exchange identity must come from the trusted message source/tuple.  It is
// not inferred from security_id: the same six-digit text may have a different
// meaning on another exchange.  The function performs no allocation.
[[nodiscard]] bool IsMainlandAShareSecurityIdV1(
    MainlandExchangeV1 exchange,
    std::span<const std::byte> security_id) noexcept;

}  // namespace l2flow::market
