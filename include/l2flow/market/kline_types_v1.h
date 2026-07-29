#pragma once

#include "l2flow/market/market_types_v1.h"

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace l2flow::market {

inline constexpr std::uint64_t kKLineNanosecondsPerMillisecondV1 =
    1'000'000ULL;
inline constexpr std::uint64_t kKLineNanosecondsPerSecondV1 =
    1'000'000'000ULL;
inline constexpr std::uint64_t kKLineNanosecondsPerDayV1 =
    86'400ULL * kKLineNanosecondsPerSecondV1;
inline constexpr std::size_t kKLineMaximumWindowsV1 = 32U;
inline constexpr std::size_t kKLineMaximumBarsPerReadV1 =
    1024U * 1024U;

// Windows are aligned to exchange-local midnight. A duration need not divide
// 24 hours; the final non-empty window may end after midnight in Unix time,
// while its start always belongs to trade_date.
struct KLineWindowSpecV1 final {
    std::uint32_t window_id = 0U;
    std::uint64_t duration_ns = 0U;
};

struct KLineAggregatorConfigV1 final {
    // This is the process trade date supplied by the server/operator. It
    // anchors fixed-UTC+08 Unix boundaries and rejects cross-date input;
    // bucket selection still uses the exchange time carried by the message.
    std::uint32_t trade_date = 0U;
    std::vector<KLineWindowSpecV1> windows;
    // Per owner-worker bound. The history runtime derives a safe value from
    // its already-required retained-record bound when this is zero.
    std::uint64_t maximum_bars = 0U;
    // Optional dense owner-local row count. When nonzero, the history runtime
    // supplies worker_local_row to make the per-trade series lookup O(1).
    // Standalone users may leave it zero and use the ID-indexed overload.
    std::size_t instrument_capacity = 0U;
    std::size_t bars_per_chunk = 256U;
    std::size_t maximum_bars_per_read = 64U * 1024U;

    [[nodiscard]] bool enabled() const noexcept {
        return !windows.empty();
    }
};

// Stable total order used only when selecting open/close. Exchange event time
// is authoritative; a positive native event sequence is preferred for equal
// timestamps, followed by process source/ingress sequence tie-breakers.
struct KLineEventOrderV1 final {
    std::uint64_t event_time_ns_since_midnight = 0U;
    // Native business/application sequence when positive, otherwise the
    // process source sequence.
    std::uint64_t event_sequence = 0U;
    std::uint64_t source_sequence = 0U;
    std::uint64_t ingress_sequence = 0U;
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
};

// Only non-empty bars are materialized. A bar is the exact result for its
// immutable ingress generation; "window_end" does not imply that future late
// messages cannot revise the same window in a later generation.
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

enum class KLineCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kResourceExhausted,
};

enum class KLineAppendErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidTrade,
    kBarCapacity,
    kNumericOverflow,
    kResourceExhausted,
    kFailed,
};

enum class KLineCaptureErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kResourceExhausted,
    kFailed,
};

enum class KLineQueryErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidArgument,
    kNotFound,
    kBatchLimitExceeded,
    kResourceExhausted,
};

[[nodiscard]] std::string_view KLineCreateErrorNameV1(
    KLineCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view KLineAppendErrorNameV1(
    KLineAppendErrorV1 error) noexcept;
[[nodiscard]] std::string_view KLineCaptureErrorNameV1(
    KLineCaptureErrorV1 error) noexcept;
[[nodiscard]] std::string_view KLineQueryErrorNameV1(
    KLineQueryErrorV1 error) noexcept;

}  // namespace l2flow::market
