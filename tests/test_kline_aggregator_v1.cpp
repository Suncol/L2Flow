#include "l2flow/market/kline_aggregator_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace {

namespace market = l2flow::market;

constexpr std::int64_t kTradeDateMidnightUnixNs =
    1'784'822'400'000'000'000LL;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

constexpr std::uint64_t TimeNs(
    std::uint64_t hour,
    std::uint64_t minute,
    std::uint64_t second,
    std::uint64_t millisecond) noexcept {
    return ((hour * 60U + minute) * 60U + second) *
               market::kKLineNanosecondsPerSecondV1 +
           millisecond *
               market::kKLineNanosecondsPerMillisecondV1;
}

market::KLineTradeV1 Trade(
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence,
    std::uint64_t event_time,
    std::int64_t price,
    std::uint64_t quantity) {
    market::KLineTradeV1 trade{};
    trade.trade_date = 20260724U;
    trade.instrument_id = 9U;
    trade.registry_ordinal = 0U;
    trade.event_time_ns_since_midnight = event_time;
    trade.event_time_unix_ns =
        kTradeDateMidnightUnixNs +
        static_cast<std::int64_t>(event_time);
    trade.price_p6 = price;
    trade.quantity_raw = quantity;
    trade.quantity_scale = 0U;
    trade.quantity_unit = market::QuantityUnitV1::kShare;
    trade.source_sequence = source_sequence;
    trade.ingress_sequence = ingress_sequence;
    return trade;
}

std::vector<market::KLineBarV1> Read(
    const market::KLineAggregatorSnapshotV1& snapshot,
    std::uint32_t window_id,
    bool* ok) {
    std::unique_ptr<market::KLineCursorV1> cursor;
    *ok &= Expect(
        snapshot.OpenInstrumentCursor(9U, window_id, &cursor) ==
                market::KLineQueryErrorV1::kNone &&
            cursor != nullptr,
        "open KLine cursor");
    std::vector<market::KLineBarV1> result;
    if (cursor == nullptr) {
        return result;
    }
    std::array<market::KLineBarV1, 2U> batch{};
    for (;;) {
        std::size_t written = 0U;
        const market::KLineQueryErrorV1 error =
            cursor->ReadBatch(batch, &written);
        *ok &= Expect(
            error == market::KLineQueryErrorV1::kNone,
            "read KLine cursor");
        if (error != market::KLineQueryErrorV1::kNone) {
            return result;
        }
        result.insert(
            result.end(), batch.begin(), batch.begin() +
                static_cast<std::ptrdiff_t>(written));
        if (written == 0U) {
            *ok &= Expect(cursor->done(), "KLine cursor done");
            return result;
        }
    }
}

}  // namespace

int main() {
    bool ok = true;

    market::ShanghaiTickV1 projected_tick{};
    projected_tick.common.origin.trade_date = 20260724U;
    projected_tick.common.origin.source_sequence = 7U;
    projected_tick.common.origin.recv_realtime_ns = -99;
    projected_tick.common.origin.recv_monotonic_ns = 8;
    projected_tick.common.instrument_id = 9U;
    projected_tick.common.registry_ordinal = 0U;
    projected_tick.common.quantity_unit =
        market::QuantityUnitV1::kShare;
    projected_tick.common.exchange_time.valid = true;
    projected_tick.common.exchange_time.unix_nanoseconds_valid = true;
    projected_tick.common.exchange_time.nanoseconds_since_midnight =
        TimeNs(9U, 30U, 0U, 800U);
    projected_tick.common.exchange_time.unix_nanoseconds =
        kTradeDateMidnightUnixNs +
        static_cast<std::int64_t>(
            projected_tick.common.exchange_time
                .nanoseconds_since_midnight);
    projected_tick.fields.action = market::TickActionV1::kTrade;
    projected_tick.fields.price.valid = true;
    projected_tick.fields.price.normalized_p6 = 10'000'000;
    projected_tick.fields.quantity.valid = true;
    projected_tick.fields.quantity.raw = 2;
    projected_tick.fields.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickExchangeTimeValidV1;
    market::DecodedMarketEventV1 decoded(std::move(projected_tick));
    market::KLineTradeV1 projected{};
    ok &= Expect(
        market::ProjectKLineTradeV1(decoded, 3U, &projected) ==
                market::KLineTradeProjectionV1::kTrade &&
            projected.event_time_ns_since_midnight ==
                TimeNs(9U, 30U, 0U, 800U) &&
            projected.ingress_sequence == 3U,
        "projection uses decoded exchange time");

    market::KLineAggregatorConfigV1 config{};
    config.trade_date = 20260724U;
    config.windows = {
        {1U, market::kKLineNanosecondsPerSecondV1},
        {2U, 5U * market::kKLineNanosecondsPerSecondV1}};
    config.maximum_bars = 32U;
    config.bars_per_chunk = 2U;
    config.maximum_bars_per_read = 2U;
    std::unique_ptr<market::KLineAggregatorV1> aggregator;
    ok &= Expect(
        market::KLineAggregatorV1::Create(config, &aggregator) ==
                market::KLineCreateErrorV1::kNone &&
            aggregator != nullptr,
        "create KLine aggregator");
    if (aggregator == nullptr) {
        return 1;
    }

    ok &= Expect(
        aggregator->Append(
            Trade(
                1U,
                1U,
                TimeNs(9U, 30U, 0U, 800U),
                10'000'000,
                2U)) == market::KLineAppendErrorV1::kNone,
        "append first trade");
    std::shared_ptr<const market::KLineAggregatorSnapshotV1>
        first_snapshot;
    ok &= Expect(
        aggregator->Capture(&first_snapshot) ==
                market::KLineCaptureErrorV1::kNone &&
            first_snapshot != nullptr,
        "capture first immutable snapshot");

    const std::array<market::KLineTradeV1, 5U> later{{
        Trade(
            2U,
            2U,
            TimeNs(9U, 30U, 0U, 100U),
            9'000'000,
            1U),
        Trade(
            3U,
            3U,
            TimeNs(9U, 30U, 0U, 500U),
            12'000'000,
            3U),
        Trade(
            4U,
            4U,
            TimeNs(9U, 30U, 0U, 999U),
            11'000'000,
            4U),
        Trade(
            5U,
            5U,
            TimeNs(9U, 30U, 1U, 0U),
            8'000'000,
            5U),
        Trade(
            6U,
            6U,
            TimeNs(14U, 59U, 59U, 900U),
            13'000'000,
            6U),
    }};
    for (const market::KLineTradeV1& trade : later) {
        ok &= Expect(
            aggregator->Append(trade) ==
                market::KLineAppendErrorV1::kNone,
            "append out-of-order/boundary trade");
    }

    std::shared_ptr<const market::KLineAggregatorSnapshotV1>
        second_snapshot;
    ok &= Expect(
        aggregator->Capture(&second_snapshot) ==
                market::KLineCaptureErrorV1::kNone &&
            second_snapshot != nullptr &&
            second_snapshot->bar_count() == 5U,
        "capture multi-window snapshot");
    if (first_snapshot == nullptr || second_snapshot == nullptr) {
        return 1;
    }

    const std::vector<market::KLineBarV1> old_bars =
        Read(*first_snapshot, 1U, &ok);
    ok &= Expect(
        old_bars.size() == 1U &&
            old_bars[0U].open_price_p6 == 10'000'000 &&
            old_bars[0U].volume_raw == 2U &&
            old_bars[0U].trade_count == 1U,
        "old snapshot remains unchanged after late trade");

    const std::vector<market::KLineBarV1> one_second =
        Read(*second_snapshot, 1U, &ok);
    ok &= Expect(
        one_second.size() == 3U &&
            one_second[0U].open_price_p6 == 9'000'000 &&
            one_second[0U].high_price_p6 == 12'000'000 &&
            one_second[0U].low_price_p6 == 9'000'000 &&
            one_second[0U].close_price_p6 == 11'000'000 &&
            one_second[0U].volume_raw == 10U &&
            one_second[0U].trade_count == 4U &&
            one_second[1U].window_start_ns_since_midnight ==
                TimeNs(9U, 30U, 1U, 0U) &&
            one_second[1U].open_price_p6 == 8'000'000 &&
            one_second[2U].window_start_ns_since_midnight ==
                TimeNs(14U, 59U, 59U, 0U),
        "1-second OHLCV uses event time and exact boundary");

    const std::vector<market::KLineBarV1> five_second =
        Read(*second_snapshot, 2U, &ok);
    ok &= Expect(
        five_second.size() == 2U &&
            five_second[0U].window_start_ns_since_midnight ==
                TimeNs(9U, 30U, 0U, 0U) &&
            five_second[0U].open_price_p6 == 9'000'000 &&
            five_second[0U].high_price_p6 == 12'000'000 &&
            five_second[0U].low_price_p6 == 8'000'000 &&
            five_second[0U].close_price_p6 == 8'000'000 &&
            five_second[0U].volume_raw == 15U &&
            five_second[0U].trade_count == 5U &&
            five_second[1U].window_start_ns_since_midnight ==
                TimeNs(14U, 59U, 55U, 0U),
        "5-second fan-out is sorted and complete");

    market::KLineAggregatorConfigV1 tie_config{};
    tie_config.trade_date = 20260724U;
    tie_config.windows = {
        {1U, market::kKLineNanosecondsPerSecondV1}};
    tie_config.maximum_bars = 4U;
    std::unique_ptr<market::KLineAggregatorV1> tie_aggregator;
    ok &= Expect(
        market::KLineAggregatorV1::Create(
            tie_config, &tie_aggregator) ==
                market::KLineCreateErrorV1::kNone &&
            tie_aggregator != nullptr,
        "create equal-time tie aggregator");
    if (tie_aggregator != nullptr) {
        market::KLineTradeV1 later_native = Trade(
            1U,
            1U,
            TimeNs(10U, 0U, 0U, 100U),
            20'000'000,
            1U);
        later_native.event_sequence = 20U;
        market::KLineTradeV1 earlier_native = Trade(
            2U,
            2U,
            TimeNs(10U, 0U, 0U, 100U),
            10'000'000,
            1U);
        earlier_native.event_sequence = 10U;
        std::shared_ptr<const market::KLineAggregatorSnapshotV1>
            tie_snapshot;
        ok &= Expect(
            tie_aggregator->Append(later_native) ==
                    market::KLineAppendErrorV1::kNone &&
                tie_aggregator->Append(earlier_native) ==
                    market::KLineAppendErrorV1::kNone &&
                tie_aggregator->Capture(&tie_snapshot) ==
                    market::KLineCaptureErrorV1::kNone &&
                tie_snapshot != nullptr,
            "append equal-time native sequences");
        if (tie_snapshot != nullptr) {
            const std::vector<market::KLineBarV1> tie_bars =
                Read(*tie_snapshot, 1U, &ok);
            ok &= Expect(
                tie_bars.size() == 1U &&
                    tie_bars[0U].open_price_p6 == 10'000'000 &&
                    tie_bars[0U].close_price_p6 == 20'000'000,
                "native event sequence orders equal exchange timestamps");
        }
    }

    return ok ? 0 : 1;
}
