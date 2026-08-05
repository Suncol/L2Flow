#include "l2flow/runtime/realtime_planes_v1.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

namespace market = l2flow::market;
namespace runtime = l2flow::runtime;

constexpr std::uint32_t kTradeDate = 20260805U;
constexpr std::uint64_t kSecond = 1'000'000'000U;
constexpr std::size_t kHotRecords = 8192U;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

l2flow::common::Identity128 SessionId() {
    l2flow::common::Identity128 result{};
    result[0U] = std::byte{0x7aU};
    result[15U] = std::byte{0xa7U};
    return result;
}

runtime::RealtimePlanesConfigV1 Config() {
    runtime::RealtimePlanesConfigV1 result{};
    result.fast.session_id = SessionId();
    result.fast.trade_date = kTradeDate;
    result.fast.instrument_count = 2U;
    result.fast.worker_count = 1U;
    result.fast.maximum_session_records = kHotRecords + 2U;
    result.fast.instrument_record_capacities = {
        kHotRecords + 1U, 1U};
    result.fast.records_per_chunk = 256U;
    result.fast.maximum_records_per_read = 4096U;
    result.fast.coverage_from_open = true;
    result.fast.tick_routes = {0U, 0U};

    result.event.session_id = SessionId();
    result.event.trade_date = kTradeDate;
    result.event.instrument_count = 2U;
    result.event.worker_count = 1U;
    result.event.maximum_order_states_per_instrument = 16U;
    result.event.maximum_inputs_per_instrument = kHotRecords + 1U;
    result.event.maximum_events_per_instrument = kHotRecords + 1U;
    result.event.input_block_records = 128U;
    result.event.event_block_records = 256U;
    result.event.cdc_range_chunk_records = 512U;
    result.event.maximum_change_records_per_instrument =
        kHotRecords * 4U;
    result.event.maximum_changes_per_read = 4096U;
    result.event.repair_replay_record_budget = 1U;
    result.event.event_routes = {0U, 0U};

    result.kline.session_id = SessionId();
    result.kline.trade_date = kTradeDate;
    result.kline.instrument_count = 2U;
    result.kline.worker_count = 1U;
    result.kline.windows = {{1U, kSecond}};
    result.kline.maximum_trades_per_instrument = kHotRecords + 1U;
    result.kline.maximum_bars_per_instrument = 4U;
    result.kline.stable_block_bars = 2U;
    result.kline.cdc_range_chunk_bars = 2U;
    result.kline.maximum_change_records_per_instrument =
        kHotRecords * 2U;
    result.kline.maximum_changes_per_read = 4096U;
    result.kline.repair_replay_record_budget = 1U;
    result.kline.kline_routes = {0U, 0U};

    // FAST can accept the complete test prefix. Tiny independent derived
    // queues make their overload path deterministic without slowing FAST.
    result.event_queue_capacity_per_tick_worker = 1U;
    result.kline_queue_capacity_per_tick_worker = 1U;
    result.live_batch_budget = 1U;
    return result;
}

struct TickPair final {
    market::CompactFastTickV1 compact{};
    market::DecodedFastTickV1 owned{};
};

TickPair Trade(
    std::uint32_t instrument_id,
    std::size_t ordinal,
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
    tick.common.instrument_id = instrument_id;
    tick.common.ordinal = ordinal;
    tick.common.exchange_time.valid = true;
    tick.common.exchange_time.unix_nanoseconds_valid = true;
    tick.common.exchange_time.nanoseconds_since_midnight =
        34'200U * kSecond +
        static_cast<std::uint64_t>(business_sequence);
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
        static_cast<std::int64_t>(instrument_id) * 1'000'000 +
        business_sequence * 2;
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
    if (market::ProjectFastTickV1(
            std::move(decoded),
            market::FastTickSourceV1::kShanghaiTick,
            arrival_id,
            &result.owned,
            &result.compact) !=
        market::FastTickProjectionErrorV1::kNone) {
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

bool BarHasTradeCount(
    const runtime::RealtimePlanesV1& planes,
    std::uint32_t instrument_id,
    std::uint64_t expected) {
    market::KLineStableSnapshotV1 stable{};
    if (planes.kline_history().AcquireStable(instrument_id, &stable) !=
            market::MutableKLineHistoryErrorV1::kNone ||
        stable.root == nullptr) {
        return false;
    }
    const market::KLineBarKeyV1 key{
        instrument_id, 1U, 34'200U * kSecond};
    market::KLineBarV1 bar{};
    return stable.root->Find(key, &bar) &&
           bar.trade_count == expected && bar.volume_raw == expected;
}

}  // namespace

int main() {
    bool ok = true;
    std::unique_ptr<runtime::RealtimePlanesV1> planes;
    std::string detail;
    ok &= Expect(
        runtime::RealtimePlanesV1::Create(
            Config(), &planes, &detail) ==
                runtime::RealtimePlanesCreateErrorV1::kNone &&
            planes != nullptr,
        "create bounded three-plane overload fixture");
    if (planes == nullptr) {
        return 1;
    }

    std::uint64_t arrival_id = 0U;
    std::uint64_t event_repair_registrations = 0U;
    std::uint64_t kline_repair_registrations = 0U;
    for (std::size_t index = 0U; index < kHotRecords; ++index) {
        ++arrival_id;
        TickPair tick = Trade(
            1U,
            0U,
            10'000 + static_cast<std::int64_t>(index),
            arrival_id);
        const auto routed = planes->PublishDecoded(
            0U, tick.compact, std::move(tick.owned));
        ok &= Expect(
            routed.error == runtime::RealtimePublishErrorV1::kNone &&
                routed.fast_published,
            "derived overload never rejects the FAST route");
        event_repair_registrations +=
            routed.event_repair_registered ? 1U : 0U;
        kline_repair_registrations +=
            routed.kline_repair_registered ? 1U : 0U;
    }
    const std::uint64_t ordered_hot_arrival = arrival_id;
    ok &= Expect(
        planes->WaitFastPublished(
            1U, ordered_hot_arrival, std::chrono::seconds(30)),
        "FAST publishes every hot-instrument row during derived overload");

    const bool first_recovery = WaitUntil(
        [&] {
            market::EventStableSnapshotV1 events{};
            return planes->event_history().RepairState(1U) ==
                       market::EventRepairStateV1::kLive &&
                   planes->kline_history().RepairState(1U) ==
                       market::EventRepairStateV1::kLive &&
                   planes->event_history().AcquireStable(1U, &events) ==
                       market::OrderedEventHistoryErrorV1::kNone &&
                   events.root != nullptr &&
                   events.root->row_count() == kHotRecords &&
                   events.root->strictly_ordered() &&
                   BarHasTradeCount(*planes, 1U, kHotRecords);
        },
        std::chrono::seconds(60));
    ok &= Expect(
        first_recovery,
        "Event and KLine recover the complete ordered prefix from FAST");

    const auto overloaded = planes->Snapshot();
    ok &= Expect(
        overloaded.fast_append_failures == 0U &&
            overloaded.fast_applied == kHotRecords &&
            overloaded.event_queue_failures > 0U &&
            overloaded.kline_queue_failures > 0U &&
            overloaded.event_rebuild_attempts > 0U &&
            overloaded.kline_rebuild_attempts > 0U &&
            event_repair_registrations > 0U &&
            kline_repair_registrations > 0U,
        "independent queue failures coalesce into FAST-based repairs");

    market::EventStableSnapshotV1 pinned_hot{};
    ok &= Expect(
        planes->event_history().AcquireStable(1U, &pinned_hot) ==
                market::OrderedEventHistoryErrorV1::kNone &&
            pinned_hot.root != nullptr &&
            pinned_hot.root->row_count() == kHotRecords,
        "pin the complete hot Event root before an early late input");

    ++arrival_id;
    const std::uint64_t late_arrival = arrival_id;
    TickPair late = Trade(1U, 0U, 1, late_arrival);
    const auto late_route = planes->PublishDecoded(
        0U, late.compact, std::move(late.owned));
    ok &= Expect(
        late_route.error == runtime::RealtimePublishErrorV1::kNone &&
            late_route.fast_published,
        "an early late input is recorded by FAST immediately");
    ok &= Expect(
        planes->WaitFastPublished(
            1U, late_arrival, std::chrono::seconds(30)),
        "FAST publishes the early late input before Event repair");

    const bool observed_repair = WaitUntil(
        [&] {
            return planes->event_history().RepairState(1U) !=
                   market::EventRepairStateV1::kLive;
        },
        std::chrono::seconds(10));
    market::EventStableSnapshotV1 during_repair{};
    ok &= Expect(
        observed_repair &&
            planes->event_history().AcquireStable(
                1U, &during_repair) ==
                market::OrderedEventHistoryErrorV1::kNone &&
            during_repair.root == pinned_hot.root &&
            during_repair.root->row_count() == kHotRecords,
        "repairing Event readers retain the prior complete immutable root");

    ok &= Expect(
        WaitUntil(
            [&] {
                return BarHasTradeCount(
                    *planes, 1U, kHotRecords + 1U);
            },
            std::chrono::seconds(10)),
        "the independent KLine plane applies the late trade");

    ++arrival_id;
    const std::uint64_t healthy_arrival = arrival_id;
    TickPair healthy = Trade(2U, 1U, 500, healthy_arrival);
    const auto healthy_route = planes->PublishDecoded(
        0U, healthy.compact, std::move(healthy.owned));
    ok &= Expect(
        healthy_route.error == runtime::RealtimePublishErrorV1::kNone &&
            healthy_route.fast_published && healthy_route.event_enqueued &&
            healthy_route.kline_enqueued,
        "a healthy instrument on the same workers remains live during repair");

    bool healthy_event_ready = false;
    const bool healthy_before_hot_commit = WaitUntil(
        [&] {
            market::EventStableSnapshotV1 stable{};
            healthy_event_ready =
                planes->event_history().AcquireStable(2U, &stable) ==
                    market::OrderedEventHistoryErrorV1::kNone &&
                stable.root != nullptr && stable.root->row_count() == 1U;
            return healthy_event_ready ||
                   planes->event_history().RepairState(1U) ==
                       market::EventRepairStateV1::kLive;
        },
        std::chrono::seconds(10));
    ok &= Expect(
        healthy_before_hot_commit && healthy_event_ready &&
            planes->event_history().RepairState(1U) !=
                market::EventRepairStateV1::kLive,
        "repair replay yields so a same-worker healthy Event can publish");

    const bool complete = WaitUntil(
        [&] {
            market::EventStableSnapshotV1 hot{};
            market::EventStableSnapshotV1 healthy_events{};
            return planes->event_history().RepairState(1U) ==
                       market::EventRepairStateV1::kLive &&
                   planes->event_history().AcquireStable(1U, &hot) ==
                       market::OrderedEventHistoryErrorV1::kNone &&
                   planes->event_history().AcquireStable(
                       2U, &healthy_events) ==
                       market::OrderedEventHistoryErrorV1::kNone &&
                   hot.root != nullptr &&
                   hot.root->row_count() == kHotRecords + 1U &&
                   hot.root->strictly_ordered() &&
                   healthy_events.root != nullptr &&
                   healthy_events.root->row_count() == 1U &&
                   BarHasTradeCount(*planes, 2U, 1U);
        },
        std::chrono::seconds(60));
    ok &= Expect(
        complete,
        "both instruments converge without a global catch-up barrier");

    market::EventStableSnapshotV1 repaired_hot{};
    std::vector<market::OrderedDerivedEventV1> repaired_rows;
    ok &= Expect(
        planes->event_history().AcquireStable(1U, &repaired_hot) ==
                market::OrderedEventHistoryErrorV1::kNone &&
            repaired_hot.root != nullptr &&
            repaired_hot.root != pinned_hot.root &&
            repaired_hot.root->CopyRows(&repaired_rows) &&
            repaired_rows.size() == kHotRecords + 1U &&
            repaired_rows.front().order_key.business_sequence == 1 &&
            repaired_rows.back().order_key.business_sequence ==
                10'000 + static_cast<std::int64_t>(kHotRecords - 1U) &&
            BarHasTradeCount(*planes, 1U, kHotRecords + 1U),
        "the atomic replacement matches the complete FAST-derived suffix");

    market::FastTickInstrumentStatusV1 hot_fast{};
    market::FastTickInstrumentStatusV1 healthy_fast{};
    ok &= Expect(
        planes->fast_store().Status(1U, &hot_fast) ==
                market::FastTickStoreQueryErrorV1::kNone &&
            planes->fast_store().Status(2U, &healthy_fast) ==
                market::FastTickStoreQueryErrorV1::kNone &&
            hot_fast.published_tail == kHotRecords + 1U &&
            healthy_fast.published_tail == 1U &&
            hot_fast.coverage_complete && healthy_fast.coverage_complete,
        "FAST coverage and tails remain independent and complete");

    planes->StopAndDrain();
    return ok ? 0 : 1;
}
