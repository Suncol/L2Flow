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
constexpr std::int32_t kChannel = 3;
constexpr std::uint64_t kSecond = 1'000'000'000U;

struct Options final {
    std::size_t records = 100'000U;
    std::uint32_t disorder_basis_points = 0U;
    bool earliest_late = false;
    std::string_view exchange = "shanghai";
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
        } else if (argument == "--exchange") {
            output->exchange = value;
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
           output->disorder_basis_points <= 10'000U &&
           (output->exchange == "shanghai" ||
            output->exchange == "shenzhen");
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

    result.event_queue_capacity_per_tick_worker = records + 1U;
    result.kline_queue_capacity_per_tick_worker = records + 1U;
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

TickPair ShanghaiTrade(
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
    tick.channel = kChannel;
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

TickPair ShenzhenTrade(
    std::int64_t business_sequence,
    std::uint64_t arrival_id) {
    market::ShenzhenTransactionV1 tick{};
    tick.common.kind = market::MarketEventKindV1::kShenzhenTransaction;
    tick.common.market = market::MarketV1::kShenzhen;
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
    tick.channel = static_cast<std::uint32_t>(kChannel);
    tick.application_sequence = business_sequence;
    tick.fields.action = market::TickActionV1::kTrade;
    tick.fields.price.valid = true;
    tick.fields.price.normalized_p6 = 10'000'000;
    tick.fields.quantity.valid = true;
    tick.fields.quantity.raw = 1;
    // Shenzhen 6.36 defines zero independently as no corresponding order.
    tick.fields.buy_order_id = 0;
    tick.fields.sell_order_id = 0;
    tick.fields.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickExchangeTimeValidV1;

    TickPair result{};
    market::DecodedMarketEventV1 decoded(std::move(tick));
    const auto projected = market::ProjectFastTickV1(
        std::move(decoded),
        market::FastTickSourceV1::kShenzhenTick,
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
                     "[--disorder-bps 0..10000] [--earliest-late] "
                     "[--exchange shanghai|shenzhen]\n";
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
    std::vector<std::uint64_t> publish_latency_ns;
    publish_latency_ns.reserve(options.records);
    const auto begin = std::chrono::steady_clock::now();
    for (std::size_t index = 0U; index < sequences.size(); ++index) {
        TickPair tick = options.exchange == "shanghai"
            ? ShanghaiTrade(sequences[index], index + 1U)
            : ShenzhenTrade(sequences[index], index + 1U);
        const auto publish_begin = std::chrono::steady_clock::now();
        const auto published = planes->PublishDecoded(
            0U, tick.compact, std::move(tick.owned));
        const auto publish_end = std::chrono::steady_clock::now();
        if (published.error != runtime::RealtimePublishErrorV1::kNone) {
            std::cerr << "publish failed at " << index << ": "
                      << runtime::RealtimePublishErrorNameV1(
                             published.error)
                      << '\n';
            planes->StopAndDrain();
            return 1;
        }
        publish_latency_ns.push_back(static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                publish_end - publish_begin)
                .count()));
    }
    const auto publish_done = std::chrono::steady_clock::now();
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
                   current_events.root->strictly_ordered() &&
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
        events.root->strictly_ordered() &&
        bars.root->bar_count() == 1U;
    const auto end = std::chrono::steady_clock::now();
    if (!roots_ready) {
        const auto failed_progress = planes->Snapshot();
        market::EventStableSnapshotV1 failed_events{};
        market::KLineStableSnapshotV1 failed_bars{};
        static_cast<void>(planes->event_history().AcquireStable(
            1U, &failed_events));
        static_cast<void>(planes->kline_history().AcquireStable(
            1U, &failed_bars));
        std::vector<market::OrderedDerivedEventV1> failed_rows;
        if (failed_events.root != nullptr) {
            static_cast<void>(failed_events.root->CopyRows(&failed_rows));
        }
        std::int64_t first_missing = 0;
        std::int64_t expected_sequence = 1;
        for (const auto& row : failed_rows) {
            if (row.order_key.business_sequence != expected_sequence) {
                first_missing = expected_sequence;
                break;
            }
            ++expected_sequence;
        }
        if (first_missing == 0 &&
            expected_sequence <= static_cast<std::int64_t>(options.records)) {
            first_missing = expected_sequence;
        }
        std::cerr
            << "derived planes did not catch up: event_state="
            << static_cast<int>(planes->event_history().RepairState(1U))
            << " event_rows="
            << (failed_events.root == nullptr
                    ? 0U
                    : failed_events.root->row_count())
            << " kline_state="
            << static_cast<int>(planes->kline_history().RepairState(1U))
            << " bars="
            << (failed_bars.root == nullptr
                    ? 0U
                    : failed_bars.root->bar_count())
            << " event_repairs="
            << failed_progress.event_rebuild_attempts
            << " kline_repairs="
            << failed_progress.kline_rebuild_attempts
            << " first_missing=" << first_missing << '\n';
        planes->StopAndDrain();
        return 1;
    }

    constexpr std::size_t kStableReadSamples = 10'000U;
    constexpr std::size_t kFullReadSamples = 100U;
    std::vector<std::uint64_t> stable_read_latency_ns;
    stable_read_latency_ns.reserve(kStableReadSamples);
    std::uint64_t observed_row_counts = 0U;
    for (std::size_t sample = 0U; sample < kStableReadSamples; ++sample) {
        market::EventStableSnapshotV1 read{};
        const auto read_begin = std::chrono::steady_clock::now();
        const auto read_error = planes->event_history().AcquireStable(
            1U, &read);
        const auto read_end = std::chrono::steady_clock::now();
        if (read_error != market::OrderedEventHistoryErrorV1::kNone ||
            read.root == nullptr) {
            std::cerr << "stable Event read failed\n";
            planes->StopAndDrain();
            return 1;
        }
        observed_row_counts += read.root->row_count();
        stable_read_latency_ns.push_back(static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                read_end - read_begin)
                .count()));
    }
    std::vector<std::uint64_t> full_read_latency_ns;
    full_read_latency_ns.reserve(kFullReadSamples);
    std::vector<market::OrderedDerivedEventV1> read_rows;
    for (std::size_t sample = 0U; sample < kFullReadSamples; ++sample) {
        const auto read_begin = std::chrono::steady_clock::now();
        const bool copied = events.root->CopyRows(&read_rows);
        const auto read_end = std::chrono::steady_clock::now();
        if (!copied || read_rows.size() != options.records) {
            std::cerr << "full Event read failed\n";
            planes->StopAndDrain();
            return 1;
        }
        full_read_latency_ns.push_back(static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                read_end - read_begin)
                .count()));
    }
    for (std::size_t index = 0U; index < read_rows.size(); ++index) {
        if (read_rows[index].order_key.channel != kChannel ||
            read_rows[index].order_key.business_sequence !=
                static_cast<std::int64_t>(index + 1U)) {
            std::cerr << "Event root contains a missing or unordered "
                         "business sequence at row "
                      << index << '\n';
            planes->StopAndDrain();
            return 1;
        }
    }
    planes->StopAndDrain();

    std::sort(publish_latency_ns.begin(), publish_latency_ns.end());
    std::sort(
        stable_read_latency_ns.begin(), stable_read_latency_ns.end());
    std::sort(full_read_latency_ns.begin(), full_read_latency_ns.end());
    const double seconds = std::chrono::duration<double>(end - begin).count();
    const double throughput =
        static_cast<double>(options.records) / seconds;
    const double publish_seconds =
        std::chrono::duration<double>(publish_done - begin).count();
    const double fast_wait_seconds =
        std::chrono::duration<double>(fast_done - publish_done).count();
    const double derived_wait_seconds =
        std::chrono::duration<double>(end - fast_done).count();
    const auto snapshot = planes->Snapshot();
    const auto event_stats = planes->event_history().Stats();
    std::cout
        << "{\"records\":" << options.records
        << ",\"exchange\":\"" << options.exchange << "\""
        << ",\"disorder_basis_points\":"
        << options.disorder_basis_points
        << ",\"earliest_late\":"
        << (options.earliest_late ? "true" : "false")
        << ",\"throughput_records_per_second\":" << throughput
        << ",\"publish_seconds\":" << publish_seconds
        << ",\"fast_wait_seconds\":" << fast_wait_seconds
        << ",\"derived_wait_seconds\":" << derived_wait_seconds
        << ",\"publish_p50_ns\":"
        << Percentile(publish_latency_ns, 50U, 100U)
        << ",\"publish_p99_ns\":"
        << Percentile(publish_latency_ns, 99U, 100U)
        << ",\"publish_p999_ns\":"
        << Percentile(publish_latency_ns, 999U, 1000U)
        << ",\"stable_read_samples\":" << kStableReadSamples
        << ",\"stable_read_p50_ns\":"
        << Percentile(stable_read_latency_ns, 50U, 100U)
        << ",\"stable_read_p99_ns\":"
        << Percentile(stable_read_latency_ns, 99U, 100U)
        << ",\"full_read_samples\":" << kFullReadSamples
        << ",\"full_read_p50_ns\":"
        << Percentile(full_read_latency_ns, 50U, 100U)
        << ",\"full_read_p99_ns\":"
        << Percentile(full_read_latency_ns, 99U, 100U)
        << ",\"observed_row_count_checksum\":"
        << observed_row_counts
        << ",\"event_rebuild_attempts\":"
        << snapshot.event_rebuild_attempts
        << ",\"kline_rebuild_attempts\":"
        << snapshot.kline_rebuild_attempts
        << ",\"event_mutable_tail_repairs\":"
        << event_stats.mutable_tail_repairs
        << ",\"event_deep_suffix_repairs\":"
        << event_stats.deep_suffix_repairs
        << ",\"event_dirty_replay_inputs\":"
        << event_stats.dirty_replay_inputs
        << ",\"event_cold_fast_rebuilds\":"
        << event_stats.cold_fast_rebuilds
        << ",\"event_full_comparison_sort_calls\":"
        << event_stats.full_comparison_sort_calls
        << "}\n";
    return 0;
}
