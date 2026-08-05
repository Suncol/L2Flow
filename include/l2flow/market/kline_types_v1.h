#pragma once

#include "l2flow/market/market_types_v1.h"

#include <cstddef>
#include <cstdint>

namespace l2flow::market {

inline constexpr std::uint64_t kKLineNanosecondsPerMillisecondV1 =
    1'000'000ULL;
inline constexpr std::uint64_t kKLineNanosecondsPerSecondV1 =
    1'000'000'000ULL;
inline constexpr std::uint64_t kKLineNanosecondsPerDayV1 =
    86'400ULL * kKLineNanosecondsPerSecondV1;
inline constexpr std::size_t kKLineMaximumWindowsV1 = 32U;

// Windows are aligned to exchange-local midnight. A duration need not divide
// 24 hours; the final non-empty window may end after midnight in Unix time,
// while its start always belongs to trade_date.
struct KLineWindowSpecV1 final {
    std::uint32_t window_id = 0U;
    std::uint64_t duration_ns = 0U;
};

// Stable total order used only when selecting open/close. Exchange event time
// is authoritative; equal timestamps use channel-local BusinessSequence and
// then process source/arrival identities as deterministic tie-breakers. No
// ordering claim is made between different channels.
struct KLineEventOrderV1 final {
    std::uint64_t event_time_ns_since_midnight = 0U;
    // Positive native BizIndex/ApplSeqNum within channel.
    std::uint64_t event_sequence = 0U;
    std::uint64_t source_sequence = 0U;
    std::uint64_t ingress_sequence = 0U;
    // Business sequence is comparable only inside this channel. Channel is
    // therefore part of the deterministic equal-time tie-break rather than
    // being silently discarded.
    std::int32_t channel = 0;
};

struct KLineTradeV1 final {
    std::uint32_t trade_date = 0U;
    std::uint32_t instrument_id = 0U;
    std::size_t ordinal = 0U;
    std::uint64_t event_time_ns_since_midnight = 0U;
    std::int64_t event_time_unix_ns = 0;
    std::int64_t price_p6 = 0;
    std::uint64_t quantity_raw = 0U;
    std::uint8_t quantity_scale = 0U;
    QuantityUnitV1 quantity_unit = QuantityUnitV1::kUnknown;
    std::uint64_t event_sequence = 0U;
    std::uint64_t source_sequence = 0U;
    std::uint64_t ingress_sequence = 0U;
    std::int32_t channel = 0;
};

// Only non-empty bars are materialized. A bar is one stable revision;
// "window_end" does not imply that a future late trade cannot publish a newer
// revision for the same stable bar key.
struct KLineBarV1 final {
    std::uint32_t trade_date = 0U;
    std::uint32_t instrument_id = 0U;
    std::uint32_t window_id = 0U;
    std::uint64_t window_duration_ns = 0U;
    std::uint64_t window_start_ns_since_midnight = 0U;
    std::uint64_t window_end_ns_since_midnight = 0U;
    std::int64_t window_start_unix_ns = 0;
    std::int64_t window_end_unix_ns = 0;
    std::int64_t open_price_p6 = 0;
    std::int64_t high_price_p6 = 0;
    std::int64_t low_price_p6 = 0;
    std::int64_t close_price_p6 = 0;
    std::uint64_t volume_raw = 0U;
    std::uint8_t volume_scale = 0U;
    QuantityUnitV1 quantity_unit = QuantityUnitV1::kUnknown;
    std::uint64_t trade_count = 0U;
    std::uint64_t revision = 0U;
    KLineEventOrderV1 first_trade{};
    KLineEventOrderV1 last_trade{};
};

enum class KLineTradeProjectionV1 : std::uint8_t {
    kNotTrade = 0U,
    kTrade,
    kInvalidTrade,
};

}  // namespace l2flow::market
