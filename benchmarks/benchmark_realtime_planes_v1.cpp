#include "l2flow/runtime/realtime_planes_v1.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

namespace market = l2flow::market;
namespace runtime = l2flow::runtime;

constexpr std::uint32_t kTradeDate = 20260805U;
constexpr std::uint64_t kSecond = 1'000'000'000U;

struct Options final {
    std::size_t records = 100'000U;
    std::uint32_t disorder_basis_points = 0U;
    bool earliest_late = false;
};

template <typename Unsigned>
bool ParseUnsigned(std::string_view text, Unsigned* output) noexcept {
    Unsigned value = 0U;
    const auto parsed = std::from_chars(
        text.data(), text.data() + text.size(), value);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != text.data() + text.size()) {
        return false;
    }
    *output = value;
    return true;
}

bool Parse(int argc, char** argv, Options* output) noexcept {
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument == "--earliest-late") {
            output->earliest_late = true;
            continue;
        }
        if (index + 1 >= argc) {
            return false;
        }
        const std::string_view value(argv[++index]);
        if (argument == "--records") {
            if (!ParseUnsigned(value, &output->records)) {
                return false;
            }
        } else if (argument == "--disorder-bps") {
            if (!ParseUnsigned(value, &output->disorder_basis_points)) {
                return false;
            }
        } else {
            return false;
        }
    }
    return output->records >= 2U &&
           output->records <=
               (std::numeric_limits<std::size_t>::max() - 1024U) / 4U &&
           output->records <
               static_cast<std::size_t>(
                   std::numeric_limits<std::int64_t>::max()) &&
           output->disorder_basis_points <= 10'000U;
}

l2flow::common::Identity128 SessionId() {
    l2flow::common::Identity128 result{};
    result[0U] = std::byte{0xb1U};
    result[15U] = std::byte{0x3aU};
    return result;
}

runtime::RealtimePlanesConfigV1 Config(std::size_t records) {
    runtime::RealtimePlanesConfigV1 result{};
    result.fast.session_id = SessionId();
    result.fast.trade_date = kTradeDate;
    result.fast.instrument_count = 1U;
    result.fast.worker_count = 1U;
    result.fast.maximum_session_records = records;
    result.fast.records_per_chunk = 1024U;
    result.fast.maximum_records_per_read = 64U * 1024U;
    result.fast.coverage_from_open = true;
    result.fast.tick_routes = {0U};

    result.event.session_id = SessionId();
    result.event.trade_date = kTradeDate;
    result.event.instrument_count = 1U;
    result.event.worker_count = 1U;
    result.event.maximum_order_states_per_instrument = 16U;
    result.event.maximum_inputs_per_instrument = records;
    result.event.maximum_events_per_instrument = records;
    result.event.input_block_records = 256U;
    result.event.event_block_records = 1024U;
    result.event.cdc_range_chunk_records = 4096U;
    result.event.maximum_change_records_per_instrument =
        records * 4U + 1024U;
    result.event.maximum_changes_per_read = 64U * 1024U;
    result.event.event_routes = {0U};

    result.kline.session_id = SessionId();
    result.kline.trade_date = kTradeDate;
    result.kline.instrument_count = 1U;
    result.kline.worker_count = 1U;
    result.kline.windows = {{1U, kSecond}};
    result.kline.maximum_trades_per_instrument = records;
    result.kline.maximum_bars_per_instrument = 4U;
    result.kline.cdc_range_chunk_bars = 4U;
    result.kline.maximum_change_records_per_instrument =
        records * 2U + 1024U;
    result.kline.maximum_changes_per_read = 64U * 1024U;
    result.kline.kline_routes = {0U};

    result.tick_queue_capacity_per_source_worker = records + 1U;
    result.event_queue_capacity_per_source_worker = records + 1U;
    result.kline_queue_capacity_per_source_worker = records + 1U;
    result.live_batch_budget = 256U;
    return result;
}

std::vector<std::int64_t> BusinessSequences(const Options& options) {
    std::vector<std::int64_t> result(options.records);
    for (std::size_t index = 0U; index < options.records; ++index) {
        result[index] = static_cast<std::int64_t>(index + 1U);
    }
    if (options.earliest_late) {
        const std::int64_t first = result.front();
        result.erase(result.begin());
        result.push_back(first);
        return result;
    }
    if (options.disorder_basis_points == 0U) {
        return result;
    }
    const std::size_t interval = std::max<std::size_t>(
        2U,
        10'000U / options.disorder_basis_points);
    for (std::size_t index = interval - 1U;
         index + 1U < result.size(); index += interval) {
        std::swap(result[index], result[index + 1U]);
    }
    return result;
}

struct TickPair final {
    market::CompactFastTickV1 compact{};
    market::DecodedFastTickV1 owned{};
};

TickPair Trade(
    std::int64_t business_sequence,
    std::uint64_t arrival_id) {
    market::ShanghaiTickV1 tick{};
    tick.common.kind = market::MarketEventKindV1::kShanghaiTick;
    tick.common.market = market::MarketV1::kShanghai;
    tick.common.origin.trade_date = kTradeDate;
    tick.common.origin.source_stream_id = 1U;
    tick.common.origin.source_sequence = arrival_id;
    tick.common.origin.vendor_sequence_id =
        static_cast<std::uint64_t>(business_sequence);
    tick.common.origin.recv_realtime_ns =
        static_cast<std::int64_t>(arrival_id);
    tick.common.origin.recv_monotonic_ns =
        static_cast<std::int64_t>(arrival_id);
    tick.common.instrument_id = 1U;
    tick.common.ordinal = 0U;
    tick.common.exchange_time.valid = true;
    tick.common.exchange_time.unix_nanoseconds_valid = true;
    tick.common.exchange_time.nanoseconds_since_midnight =
        34'200U * kSecond + arrival_id;
    tick.common.exchange_time.unix_nanoseconds =
        1'785'859'200'000'000'000LL +
        static_cast<std::int64_t>(
            tick.common.exchange_time.nanoseconds_since_midnight);
    tick.channel = 3;
    tick.business_index = business_sequence;
    tick.fields.action = market::TickActionV1::kTrade;
    tick.fields.aggressor = market::AggressorV1::kNeutral;
    tick.fields.price.valid = true;
    tick.fields.price.normalized_p6 = 10'000'000;
    tick.fields.quantity.valid = true;
    tick.fields.quantity.raw = 1;
    tick.fields.buy_order_id =
        1'000'000 + business_sequence * 2;
    tick.fields.sell_order_id = tick.fields.buy_order_id + 1;
    tick.fields.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickBuyOrderIdValidV1 |
        market::kTickSellOrderIdValidV1 |
        market::kTickAggressorValidV1 |
        market::kTickExchangeTimeValidV1;

    TickPair result{};
    market::DecodedMarketEventV1 decoded(std::move(tick));
    const auto projected = market::ProjectFastTickV1(
        std::move(decoded),
        market::FastTickSourceV1::kShanghaiTick,
        arrival_id,
        &result.owned,
        &result.compact);
    if (projected != market::FastTickProjectionErrorV1::kNone) {
        std::terminate();
    }
    return result;
}

template <typename Predicate>
bool WaitUntil(Predicate predicate, std::chrono::seconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    return predicate();
}

std::uint64_t Percentile(
    const std::vector<std::uint64_t>& sorted,
    std::uint64_t numerator,
    std::uint64_t denominator) {
    const std::uint64_t count = sorted.size();
    const std::uint64_t rank =
        (count * numerator + denominator - 1U) / denominator;
    return sorted[static_cast<std::size_t>(std::max<std::uint64_t>(
        1U, rank) - 1U)];
}

}  // namespace

int main(int argc, char** argv) {
    Options options{};
    if (!Parse(argc, argv, &options)) {
        std::cerr << "usage: benchmark-realtime-planes [--records N] "
                     "[--disorder-bps 0..10000] [--earliest-late]\n";
        return 2;
    }

    std::unique_ptr<runtime::RealtimePlanesV1> planes;
    std::string detail;
    const auto created = runtime::RealtimePlanesV1::Create(
        Config(options.records), &planes, &detail);
    if (created != runtime::RealtimePlanesCreateErrorV1::kNone ||
        planes == nullptr) {
        std::cerr << "create failed: "
                  << runtime::RealtimePlanesCreateErrorNameV1(created)
                  << ' ' << detail << '\n';
        return 1;
    }

    const auto sequences = BusinessSequences(options);
    std::vector<std::uint64_t> route_latency_ns;
    route_latency_ns.reserve(options.records);
    const auto begin = std::chrono::steady_clock::now();
    for (std::size_t index = 0U; index < sequences.size(); ++index) {
        TickPair tick = Trade(sequences[index], index + 1U);
        const auto route_begin = std::chrono::steady_clock::now();
        const auto routed = planes->RouteDecoded(
            tick.compact, std::move(tick.owned));
        const auto route_end = std::chrono::steady_clock::now();
        if (routed.error != runtime::RealtimeRouteErrorV1::kNone) {
            std::cerr << "route failed at " << index << ": "
                      << runtime::RealtimeRouteErrorNameV1(routed.error)
                      << '\n';
            planes->StopAndDrain();
            return 1;
        }
        route_latency_ns.push_back(static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                route_end - route_begin)
                .count()));
    }
    const auto route_done = std::chrono::steady_clock::now();
    if (!planes->WaitFastPublished(
            1U, options.records, std::chrono::seconds(30))) {
        std::cerr << "FAST did not publish the offered tail\n";
        planes->StopAndDrain();
        return 1;
    }
    const auto fast_done = std::chrono::steady_clock::now();
    const bool derived_ready = WaitUntil(
        [&] {
            const auto progress = planes->Snapshot();
            if ((progress.event_applied != options.records &&
                 progress.event_rebuild_attempts == 0U) ||
                progress.kline_applied != options.records ||
                planes->event_history().RepairState(1U) !=
                    market::EventRepairStateV1::kLive ||
                planes->kline_history().RepairState(1U) !=
                    market::EventRepairStateV1::kLive) {
                return false;
            }
            market::EventStableSnapshotV1 current_events{};
            market::KLineStableSnapshotV1 current_bars{};
            return planes->event_history().AcquireStable(
                       1U, &current_events) ==
                       market::OrderedEventHistoryErrorV1::kNone &&
                   planes->kline_history().AcquireStable(
                       1U, &current_bars) ==
                       market::MutableKLineHistoryErrorV1::kNone &&
                   current_events.root != nullptr &&
                   current_bars.root != nullptr &&
                   current_events.root->row_count() == options.records &&
                   current_bars.root->bar_count() == 1U;
        },
        std::chrono::seconds(60));
    market::EventStableSnapshotV1 events{};
    market::KLineStableSnapshotV1 bars{};
    const bool roots_ready = derived_ready &&
        planes->event_history().AcquireStable(1U, &events) ==
            market::OrderedEventHistoryErrorV1::kNone &&
        planes->kline_history().AcquireStable(1U, &bars) ==
            market::MutableKLineHistoryErrorV1::kNone &&
        events.root != nullptr && bars.root != nullptr &&
        events.root->row_count() == options.records &&
        bars.root->bar_count() == 1U;
    const auto end = std::chrono::steady_clock::now();
    planes->StopAndDrain();
    if (!roots_ready) {
        std::cerr << "derived planes did not catch up\n";
        return 1;
    }

    std::sort(route_latency_ns.begin(), route_latency_ns.end());
    const double seconds = std::chrono::duration<double>(end - begin).count();
    const double throughput =
        static_cast<double>(options.records) / seconds;
    const double route_seconds =
        std::chrono::duration<double>(route_done - begin).count();
    const double fast_wait_seconds =
        std::chrono::duration<double>(fast_done - route_done).count();
    const double derived_wait_seconds =
        std::chrono::duration<double>(end - fast_done).count();
    const auto snapshot = planes->Snapshot();
    const auto event_stats = planes->event_history().Stats();
    std::cout
        << "{\"records\":" << options.records
        << ",\"disorder_basis_points\":"
        << options.disorder_basis_points
        << ",\"earliest_late\":"
        << (options.earliest_late ? "true" : "false")
        << ",\"throughput_records_per_second\":" << throughput
        << ",\"route_seconds\":" << route_seconds
        << ",\"fast_wait_seconds\":" << fast_wait_seconds
        << ",\"derived_wait_seconds\":" << derived_wait_seconds
        << ",\"route_p50_ns\":"
        << Percentile(route_latency_ns, 50U, 100U)
        << ",\"route_p99_ns\":"
        << Percentile(route_latency_ns, 99U, 100U)
        << ",\"route_p999_ns\":"
        << Percentile(route_latency_ns, 999U, 1000U)
        << ",\"event_rebuild_attempts\":"
        << snapshot.event_rebuild_attempts
        << ",\"kline_rebuild_attempts\":"
        << snapshot.kline_rebuild_attempts
        << ",\"event_full_comparison_sort_calls\":"
        << event_stats.full_comparison_sort_calls
        << "}\n";
    return 0;
}
