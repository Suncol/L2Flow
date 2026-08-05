#include "l2flow/market/fast_tick_store_v1.h"
#include "l2flow/market/kline_projection_v1.h"
#include "l2flow/market/mutable_kline_history_v1.h"
#include "l2flow/market/ordered_event_history_v1.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

namespace market = l2flow::market;

constexpr std::uint32_t kTradeDate = 20260805U;
constexpr std::uint64_t kSecond = 1'000'000'000U;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

l2flow::common::Identity128 SessionId() {
    l2flow::common::Identity128 result{};
    result[0U] = std::byte{0x5aU};
    result[15U] = std::byte{0xa5U};
    return result;
}

struct TickPair final {
    market::CompactFastTickV1 compact{};
    market::DecodedFastTickV1 owned{};
};

TickPair ShanghaiTick(
    std::int64_t business_sequence,
    std::uint64_t arrival_id,
    market::TickActionV1 action,
    std::uint64_t event_time,
    std::int64_t price,
    std::int64_t quantity,
    std::int64_t order_id,
    std::uint32_t instrument_id = 1U,
    std::size_t ordinal = 0U,
    std::int32_t channel = 3) {
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
    tick.common.exchange_time.nanoseconds_since_midnight = event_time;
    tick.common.exchange_time.unix_nanoseconds =
        1'786'000'000'000'000'000LL +
        static_cast<std::int64_t>(event_time);
    tick.business_index = business_sequence;
    tick.channel = channel;
    tick.fields.action = action;
    tick.fields.side = market::SideV1::kBuy;
    tick.fields.price.valid = true;
    tick.fields.price.normalized_p6 = price;
    tick.fields.quantity.valid = true;
    tick.fields.quantity.raw = quantity;
    tick.fields.primary_order_id = order_id;
    tick.fields.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickPrimaryOrderIdValidV1 |
        market::kTickSideValidV1 |
        market::kTickExchangeTimeValidV1;
    if (action == market::TickActionV1::kTrade) {
        tick.fields.buy_order_id = order_id;
        tick.fields.sell_order_id = order_id + 10'000;
        tick.fields.aggressor = market::AggressorV1::kBuy;
        tick.fields.validity_bitmap |=
            market::kTickBuyOrderIdValidV1 |
            market::kTickSellOrderIdValidV1 |
            market::kTickAggressorValidV1;
    }

    TickPair result{};
    market::DecodedMarketEventV1 decoded(std::move(tick));
    const auto projection = market::ProjectFastTickV1(
        std::move(decoded),
        market::FastTickSourceV1::kShanghaiTick,
        arrival_id,
        &result.owned,
        &result.compact);
    if (projection != market::FastTickProjectionErrorV1::kNone) {
        std::cerr << "FAIL: project test Tick: "
                  << market::FastTickProjectionErrorNameV1(projection)
                  << '\n';
    }
    return result;
}

std::unique_ptr<market::FastTickStoreV1> MakeFastStore(bool* ok) {
    market::FastTickStoreConfigV1 config{};
    config.session_id = SessionId();
    config.trade_date = kTradeDate;
    config.instrument_count = 1U;
    config.worker_count = 1U;
    config.maximum_session_records = 128U;
    config.records_per_chunk = 4U;
    config.maximum_records_per_read = 16U;
    config.coverage_from_open = true;
    config.tick_routes = {0U};
    std::unique_ptr<market::FastTickStoreV1> result;
    *ok &= Expect(
        market::FastTickStoreV1::Create(config, &result) ==
                market::FastTickStoreCreateErrorV1::kNone &&
            result != nullptr,
        "create per-instrument FAST store");
    return result;
}

bool Append(
    market::FastTickStoreV1* store,
    const market::FastTickRouteTokenV1& route,
    TickPair* tick) {
    return Expect(
        store->Append(
            0U,
            route,
            tick->compact,
            std::move(tick->owned)) ==
            market::FastTickStoreAppendErrorV1::kNone,
        "append FAST arrival ledger row");
}

void TestBusinessDuplicateIdentity(bool* ok) {
    TickPair first = ShanghaiTick(
        77,
        1U,
        market::TickActionV1::kAdd,
        9U * 3'600U * kSecond,
        10'000'000,
        100,
        77);
    TickPair retransmitted = ShanghaiTick(
        77,
        2U,
        market::TickActionV1::kAdd,
        9U * 3'600U * kSecond,
        10'000'000,
        100,
        77);
    retransmitted.compact.vendor_sequence_id = 9002U;
    *ok &= Expect(
        market::SameFastTickPayloadV1(
            first.compact, retransmitted.compact),
        "duplicate identity ignores arrival, receive, source and vendor transport anchors");
    retransmitted.compact.price_p6 += 1;
    *ok &= Expect(
        !market::SameFastTickPayloadV1(
            first.compact, retransmitted.compact),
        "same BusinessSequence with a changed exchange payload is a conflict");
}

void TestFastRejectsForgedProjection(bool* ok) {
    auto fast = MakeFastStore(ok);
    if (fast == nullptr) {
        return;
    }
    market::FastTickRouteTokenV1 route{};
    *ok &= Expect(
        fast->ResolveRoute(0U, 1U, &route) ==
            market::FastTickStoreQueryErrorV1::kNone,
        "resolve FAST projection-validation route");
    TickPair forged = ShanghaiTick(
        78,
        1U,
        market::TickActionV1::kTrade,
        9U * 3'600U * kSecond,
        10'000'000,
        1,
        78);
    ++forged.compact.price_p6;
    *ok &= Expect(
        fast->Append(
            0U,
            route,
            forged.compact,
            std::move(forged.owned)) ==
            market::FastTickStoreAppendErrorV1::kInvalidInput,
        "FAST rejects a compact projection that disagrees with its owned Tick");

    TickPair invalid_source = ShanghaiTick(
        79,
        2U,
        market::TickActionV1::kTrade,
        9U * 3'600U * kSecond + 1U,
        10'000'000,
        1,
        79);
    invalid_source.compact.source =
        static_cast<market::FastTickSourceV1>(99U);
    *ok &= Expect(
        fast->Append(
            0U,
            route,
            invalid_source.compact,
            std::move(invalid_source.owned)) ==
            market::FastTickStoreAppendErrorV1::kInvalidInput,
        "FAST rejects an out-of-domain source enum");
}

void TestFastStatusIsCoherentDuringPublication(bool* ok) {
    constexpr std::size_t records = 4096U;
    market::FastTickStoreConfigV1 config{};
    config.session_id = SessionId();
    config.trade_date = kTradeDate;
    config.instrument_count = 1U;
    config.worker_count = 1U;
    config.maximum_session_records = records;
    config.records_per_chunk = 64U;
    config.maximum_records_per_read = records;
    config.coverage_from_open = true;
    config.tick_routes = {0U};
    std::unique_ptr<market::FastTickStoreV1> fast;
    *ok &= Expect(
        market::FastTickStoreV1::Create(config, &fast) ==
                market::FastTickStoreCreateErrorV1::kNone &&
            fast != nullptr,
        "create FAST publication-coherence fixture");
    if (fast == nullptr) {
        return;
    }
    market::FastTickRouteTokenV1 route{};
    *ok &= Expect(
        fast->ResolveRoute(0U, 1U, &route) ==
            market::FastTickStoreQueryErrorV1::kNone,
        "resolve FAST publication-coherence route");

    std::atomic<bool> writer_failed{false};
    std::atomic<bool> writer_done{false};
    std::thread writer([&] {
        for (std::size_t index = 0U; index < records; ++index) {
            const std::uint64_t sequence = index + 1U;
            TickPair tick = ShanghaiTick(
                static_cast<std::int64_t>(sequence),
                sequence,
                market::TickActionV1::kTrade,
                9U * 3'600U * kSecond + sequence,
                10'000'000,
                1,
                static_cast<std::int64_t>(sequence));
            if (fast->Append(
                    0U,
                    route,
                    tick.compact,
                    std::move(tick.owned)) !=
                market::FastTickStoreAppendErrorV1::kNone) {
                writer_failed.store(true, std::memory_order_release);
                break;
            }
        }
        writer_done.store(true, std::memory_order_release);
    });

    bool coherent = true;
    while (!writer_done.load(std::memory_order_acquire)) {
        market::FastTickInstrumentStatusV1 status{};
        if (fast->Status(1U, &status) !=
                market::FastTickStoreQueryErrorV1::kNone ||
            status.published_tail != status.latest_arrival_id ||
            !status.coverage_complete) {
            coherent = false;
            break;
        }
        std::this_thread::yield();
    }
    writer.join();
    market::FastTickInstrumentStatusV1 final{};
    *ok &= Expect(
        coherent && !writer_failed.load(std::memory_order_acquire) &&
            fast->Status(1U, &final) ==
                market::FastTickStoreQueryErrorV1::kNone &&
            final.published_tail == records &&
            final.latest_arrival_id == records &&
            final.coverage_complete,
        "FAST status never combines a new latest pointer with an older tail");
}

void TestKLineTradeProjection(bool* ok) {
    TickPair trade = ShanghaiTick(
        91,
        7U,
        market::TickActionV1::kTrade,
        9U * 3'600U * kSecond + 123U,
        10'250'000,
        300,
        91);
    market::KLineTradeV1 projected{};
    *ok &= Expect(
        market::ProjectKLineTradeV1(
            trade.owned, trade.compact.arrival_id, &projected) ==
                market::KLineTradeProjectionV1::kTrade,
        "project valid Shanghai trade for KLine");
    *ok &= Expect(
        projected.event_sequence == 91U &&
            projected.channel == 3 &&
            projected.ingress_sequence == 7U &&
            projected.event_time_ns_since_midnight ==
                9U * 3'600U * kSecond + 123U &&
            projected.price_p6 == 10'250'000 &&
            projected.quantity_raw == 300U,
        "KLine projection uses event time and native BusinessSequence");

    TickPair order = ShanghaiTick(
        92,
        8U,
        market::TickActionV1::kAdd,
        9U * 3'600U * kSecond + 124U,
        10'250'000,
        300,
        92);
    *ok &= Expect(
        market::ProjectKLineTradeV1(
            order.owned, order.compact.arrival_id, &projected) ==
                market::KLineTradeProjectionV1::kNotTrade,
        "KLine projection ignores non-trade Tick input");

    TickPair invalid_price = ShanghaiTick(
        93,
        9U,
        market::TickActionV1::kTrade,
        9U * 3'600U * kSecond + 125U,
        0,
        1,
        93);
    *ok &= Expect(
        market::ProjectKLineTradeV1(
            invalid_price.owned,
            invalid_price.compact.arrival_id,
            &projected) == market::KLineTradeProjectionV1::kInvalidTrade &&
            !market::IsKLineTradeV1(invalid_price.compact),
        "KLine consistently rejects a non-positive trade price");

    auto forged_source = trade.compact;
    forged_source.source = market::FastTickSourceV1::kShenzhenTick;
    *ok &= Expect(
        !market::IsKLineTradeV1(forged_source),
        "KLine trade classification validates source, market and kind");

    market::ShenzhenOrderV1 shenzhen_order{};
    shenzhen_order.common.kind =
        market::MarketEventKindV1::kShenzhenOrder;
    shenzhen_order.common.market = market::MarketV1::kShenzhen;
    shenzhen_order.channel = 4U;
    shenzhen_order.application_sequence = 93;
    market::DecodedMarketEventV1 decoded_order(
        std::move(shenzhen_order));
    *ok &= Expect(
        market::ProjectKLineTradeV1(
            decoded_order, 9U, &projected) ==
                market::KLineTradeProjectionV1::kNotTrade,
        "KLine projection never treats Shenzhen Order as a trade");
}

std::vector<market::OrderedDerivedEventV1> EventRows(
    const market::EventStableSnapshotV1& snapshot,
    bool* ok) {
    std::vector<market::OrderedDerivedEventV1> rows;
    *ok &= Expect(
        snapshot.root != nullptr && snapshot.root->CopyRows(&rows),
        "copy immutable Event root");
    return rows;
}

void TestFastAndEventRepair(bool* ok) {
    auto fast = MakeFastStore(ok);
    if (fast == nullptr) {
        return;
    }
    market::FastTickRouteTokenV1 fast_route{};
    *ok &= Expect(
        fast->ResolveRoute(0U, 1U, &fast_route) ==
            market::FastTickStoreQueryErrorV1::kNone,
        "resolve FAST route");

    market::OrderedEventHistoryConfigV1 event_config{};
    event_config.session_id = SessionId();
    event_config.trade_date = kTradeDate;
    event_config.instrument_count = 1U;
    event_config.worker_count = 1U;
    event_config.maximum_order_states_per_instrument = 64U;
    event_config.maximum_inputs_per_instrument = 128U;
    event_config.maximum_events_per_instrument = 256U;
    event_config.input_block_records = 4U;
    event_config.event_block_records = 4U;
    event_config.cdc_range_chunk_records = 2U;
    event_config.maximum_change_records_per_instrument = 512U;
    event_config.maximum_changes_per_read = 32U;
    event_config.event_routes = {0U};
    std::unique_ptr<market::OrderedEventHistoryV1> events;
    *ok &= Expect(
        market::OrderedEventHistoryV1::Create(
            event_config, &events) ==
                market::OrderedEventHistoryCreateErrorV1::kNone &&
            events != nullptr,
        "create ordered Event history");
    if (events == nullptr) {
        return;
    }
    market::EventRouteTokenV1 event_route{};
    *ok &= Expect(
        events->ResolveRoute(0U, 1U, &event_route) ==
            market::OrderedEventHistoryErrorV1::kNone,
        "resolve Event route");

    TickPair first = ShanghaiTick(
        100,
        1U,
        market::TickActionV1::kAdd,
        9U * 3'600U * kSecond,
        10'000'000,
        100,
        100);
    TickPair gap = ShanghaiTick(
        102,
        2U,
        market::TickActionV1::kAdd,
        9U * 3'600U * kSecond + 2U,
        10'200'000,
        100,
        102);
    TickPair late = ShanghaiTick(
        101,
        3U,
        market::TickActionV1::kAdd,
        9U * 3'600U * kSecond + 1U,
        10'100'000,
        100,
        101);

    *ok &= Append(fast.get(), fast_route, &first);
    const auto first_apply = events->ApplyLive(
        0U, event_route, first.compact);
    *ok &= Expect(
        first_apply.error ==
                market::OrderedEventHistoryErrorV1::kNone &&
            first_apply.disposition ==
                market::EventInputDispositionV1::kPublished,
        "publish first monotonic Event input");
    *ok &= Append(fast.get(), fast_route, &gap);
    const auto gap_apply = events->ApplyLive(0U, event_route, gap.compact);
    *ok &= Expect(
        gap_apply.error == market::OrderedEventHistoryErrorV1::kNone &&
            gap_apply.disposition ==
                market::EventInputDispositionV1::kPublished,
        "BizIndex numeric gap remains a valid O(1) append");

    market::EventStableSnapshotV1 before{};
    *ok &= Expect(
        events->AcquireStable(1U, &before) ==
                market::OrderedEventHistoryErrorV1::kNone &&
            before.root != nullptr && before.root->strictly_ordered(),
        "pin stable Event root before late input");
    const auto before_rows = EventRows(before, ok);
    *ok &= Expect(
        before_rows.size() == 2U &&
            before_rows[0U].order_key.business_sequence == 100 &&
            before_rows[1U].order_key.business_sequence == 102,
        "pre-repair stable root contains the prior complete view");

    *ok &= Append(fast.get(), fast_route, &late);
    const auto late_apply = events->ApplyLive(
        0U, event_route, late.compact);
    *ok &= Expect(
        late_apply.disposition ==
                market::EventInputDispositionV1::kRepairRegistered &&
            events->RepairState(1U) ==
                market::EventRepairStateV1::kRepairRequired,
        "late BizIndex immediately registers instrument repair");
    const auto still_before = EventRows(before, ok);
    *ok &= Expect(
        still_before.size() == 2U &&
            still_before[1U].order_key.business_sequence == 102,
        "pinned root is not modified while repair is pending");

    const auto rebuild = events->RebuildFromFast(
        0U, event_route, *fast);
    *ok &= Expect(
        rebuild.error == market::OrderedEventHistoryErrorV1::kNone &&
            rebuild.complete &&
            rebuild.captured_fast_tail == 3U &&
            events->RepairState(1U) == market::EventRepairStateV1::kLive,
        "rebuild Event exclusively from captured FAST history");
    market::EventStableSnapshotV1 after{};
    *ok &= Expect(
        events->AcquireStable(1U, &after) ==
                market::OrderedEventHistoryErrorV1::kNone &&
            after.root != before.root && after.root->strictly_ordered(),
        "atomically publish replacement Event root");
    const auto after_rows = EventRows(after, ok);
    *ok &= Expect(
        after_rows.size() == 3U &&
            after_rows[0U].order_key.business_sequence == 100 &&
            after_rows[1U].order_key.business_sequence == 101 &&
            after_rows[2U].order_key.business_sequence == 102,
        "repaired Event view is strictly BusinessSequence ordered");

    std::array<market::EventMutationV1, 8U> changes{};
    std::size_t written = 0U;
    market::EventChangeCursorV1 cursor = before.next_changes;
    *ok &= Expect(
        events->ReadChanges(&cursor, changes, &written) ==
                market::OrderedEventHistoryErrorV1::kNone &&
            written >= 3U &&
            changes[0U].kind ==
                market::EventMutationKindV1::kRangeReplaceBegin &&
            changes[0U].replace_entire_instrument &&
            changes[written - 1U].kind ==
                market::EventMutationKindV1::kRangeReplaceCommit &&
            changes[0U].transaction_id ==
                changes[written - 1U].transaction_id,
        "Event CDC publishes a complete range transaction");
    const auto stats = events->Stats();
    *ok &= Expect(
        stats.monotonic_fast_path_inputs == 2U &&
            stats.late_inputs == 1U &&
            stats.full_comparison_sort_calls == 0U &&
            stats.rebuild_natural_run_merge == 1U,
        "ordered fast path and adaptive recovery counters are exact");

    std::unique_ptr<market::FastTickCursorV1> ledger;
    *ok &= Expect(
        fast->OpenCursor(1U, 1U, &ledger) ==
                market::FastTickStoreQueryErrorV1::kNone &&
            ledger != nullptr,
        "open instrument-local FAST cursor");
    if (ledger != nullptr) {
        std::array<const market::FastTickRecordV1*, 4U> records{};
        std::size_t count = 0U;
        *ok &= Expect(
            ledger->ReadBatch(records, &count) ==
                    market::FastTickStoreQueryErrorV1::kNone &&
                count == 3U &&
                records[0U]->compact().business_sequence.value == 100 &&
                records[1U]->compact().business_sequence.value == 102 &&
                records[2U]->compact().business_sequence.value == 101 &&
                records[2U]->instrument_tick_sequence() == 3U,
            "FAST remains append-only in physical arrival order");
    }
}

void TestKLineRebuildAndRevision(bool* ok) {
    auto fast = MakeFastStore(ok);
    if (fast == nullptr) {
        return;
    }
    market::FastTickRouteTokenV1 fast_route{};
    *ok &= Expect(
        fast->ResolveRoute(0U, 1U, &fast_route) ==
            market::FastTickStoreQueryErrorV1::kNone,
        "resolve KLine test FAST route");

    market::MutableKLineHistoryConfigV1 config{};
    config.session_id = SessionId();
    config.trade_date = kTradeDate;
    config.instrument_count = 1U;
    config.worker_count = 1U;
    config.windows = {{1U, kSecond}};
    config.maximum_trades_per_instrument = 128U;
    config.maximum_bars_per_instrument = 128U;
    config.cdc_range_chunk_bars = 2U;
    config.maximum_change_records_per_instrument = 256U;
    config.maximum_changes_per_read = 16U;
    config.kline_routes = {0U};
    std::unique_ptr<market::MutableKLineHistoryV1> history;
    *ok &= Expect(
        market::MutableKLineHistoryV1::Create(config, &history) ==
                market::MutableKLineHistoryCreateErrorV1::kNone &&
            history != nullptr,
        "create mutable KLine history");
    if (history == nullptr) {
        return;
    }
    market::KLineRouteTokenV1 route{};
    *ok &= Expect(
        history->ResolveRoute(0U, 1U, &route) ==
            market::MutableKLineHistoryErrorV1::kNone,
        "resolve KLine route");

    constexpr std::uint64_t window =
        (9U * 3'600U + 30U * 60U) * kSecond;
    TickPair first = ShanghaiTick(
        200,
        1U,
        market::TickActionV1::kTrade,
        window + 800'000'000U,
        10'000'000,
        2,
        200);
    TickPair missed = ShanghaiTick(
        201,
        2U,
        market::TickActionV1::kTrade,
        window + 100'000'000U,
        9'000'000,
        1,
        201);
    *ok &= Append(fast.get(), fast_route, &first);
    const auto applied = history->ApplyLive(0U, route, first.compact);
    *ok &= Expect(
        applied.error == market::MutableKLineHistoryErrorV1::kNone &&
            applied.upserted_bars == 1U,
        "apply first KLine trade independently");
    market::KLineStableSnapshotV1 before{};
    *ok &= Expect(
        history->AcquireStable(1U, &before) ==
                market::MutableKLineHistoryErrorV1::kNone,
        "pin KLine root before repair");

    *ok &= Append(fast.get(), fast_route, &missed);
    history->MarkRepairRequired(1U, missed.compact.arrival_id);
    const auto rebuild = history->RebuildFromFast(0U, route, *fast);
    *ok &= Expect(
        rebuild.error == market::MutableKLineHistoryErrorV1::kNone &&
            rebuild.complete &&
            rebuild.rebuilt_trades == 2U &&
            rebuild.upserted_bars == 1U &&
            history->RepairState(1U) == market::EventRepairStateV1::kLive,
        "rebuild KLine from FAST after a missed projection");

    const market::KLineBarKeyV1 key{1U, 1U, window};
    market::KLineBarV1 old_bar{};
    market::KLineBarV1 repaired{};
    market::KLineStableSnapshotV1 after{};
    *ok &= Expect(
        before.root != nullptr && before.root->Find(key, &old_bar) &&
            history->AcquireStable(1U, &after) ==
                market::MutableKLineHistoryErrorV1::kNone &&
            after.root != nullptr && after.root != before.root &&
            after.root->Find(key, &repaired),
        "atomically replace KLine root while old reader stays pinned");
    *ok &= Expect(
        old_bar.revision == 1U && old_bar.volume_raw == 2U &&
            repaired.revision == 2U && repaired.volume_raw == 3U &&
            repaired.trade_count == 2U &&
            repaired.open_price_p6 == 9'000'000 &&
            repaired.close_price_p6 == 10'000'000 &&
            repaired.high_price_p6 == 10'000'000 &&
            repaired.low_price_p6 == 9'000'000,
        "historical KLine upsert uses event-time order and increments revision");

    std::array<market::KLineMutationV1, 4U> mutations{};
    std::size_t written = 0U;
    market::KLineChangeCursorV1 cursor = before.next_changes;
    *ok &= Expect(
        history->ReadChanges(&cursor, mutations, &written) ==
                market::MutableKLineHistoryErrorV1::kNone &&
            written == 3U &&
            mutations[0U].kind ==
                market::KLineMutationKindV1::kRangeReplaceBegin &&
            mutations[0U].replace_entire_instrument &&
            mutations[1U].kind ==
                market::KLineMutationKindV1::kRangeReplaceChunk &&
            mutations[1U].replacement_bars.size() == 1U &&
            mutations[1U].replacement_bars[0U].revision == 2U &&
            mutations[2U].kind ==
                market::KLineMutationKindV1::kRangeReplaceCommit &&
            mutations[0U].transaction_id ==
                mutations[2U].transaction_id,
        "KLine repair CDC is one committed range transaction");

    TickPair conflict = ShanghaiTick(
        200,
        3U,
        market::TickActionV1::kTrade,
        window + 800'000'000U,
        12'000'000,
        2,
        200);
    *ok &= Append(fast.get(), fast_route, &conflict);
    const auto conflict_apply = history->ApplyLive(
        0U, route, conflict.compact);
    *ok &= Expect(
        conflict_apply.error ==
                market::MutableKLineHistoryErrorV1::kSourceConflict &&
            history->RepairState(1U) ==
                market::EventRepairStateV1::kRepairRequired,
        "conflicting TradeUid requests FAST recovery without double counting");
    const auto conflict_rebuild = history->RebuildFromFast(
        0U, route, *fast);
    market::KLineStableSnapshotV1 isolated{};
    market::KLineBarV1 isolated_bar{};
    *ok &= Expect(
        conflict_rebuild.error ==
                market::MutableKLineHistoryErrorV1::kSourceConflict &&
            history->RepairState(1U) ==
                market::EventRepairStateV1::kSourceConflict &&
            history->AcquireStable(1U, &isolated) ==
                market::MutableKLineHistoryErrorV1::kNone &&
            isolated.root != nullptr &&
            isolated.root->Find(key, &isolated_bar) &&
            isolated_bar.volume_raw == repaired.volume_raw,
        "FAST rebuild isolates irreconcilable TradeUid payloads and preserves the last stable bar");
}

void TestPersistentEventChannels(bool* ok) {
    market::OrderedEventHistoryConfigV1 config{};
    config.session_id = SessionId();
    config.trade_date = kTradeDate;
    config.instrument_count = 1U;
    config.worker_count = 1U;
    config.maximum_order_states_per_instrument = 32U;
    config.maximum_inputs_per_instrument = 32U;
    config.maximum_events_per_instrument = 32U;
    config.input_block_records = 2U;
    config.event_block_records = 2U;
    config.cdc_range_chunk_records = 2U;
    config.maximum_change_records_per_instrument = 128U;
    config.maximum_changes_per_read = 32U;
    config.event_routes = {0U};
    std::unique_ptr<market::OrderedEventHistoryV1> history;
    *ok &= Expect(
        market::OrderedEventHistoryV1::Create(config, &history) ==
                market::OrderedEventHistoryCreateErrorV1::kNone &&
            history != nullptr,
        "create persistent per-channel Event root");
    if (history == nullptr) {
        return;
    }
    market::EventRouteTokenV1 route{};
    *ok &= Expect(
        history->ResolveRoute(0U, 1U, &route) ==
            market::OrderedEventHistoryErrorV1::kNone,
        "resolve persistent Event route");

    std::uint64_t arrival = 1U;
    market::EventStableSnapshotV1 pinned{};
    for (std::int64_t sequence : {100, 101, 102}) {
        TickPair tick = ShanghaiTick(
            sequence,
            arrival++,
            market::TickActionV1::kAdd,
            9U * 3'600U * kSecond +
                static_cast<std::uint64_t>(sequence),
            10'000'000,
            1,
            10'000 + sequence,
            1U,
            0U,
            5);
        *ok &= Expect(
            history->ApplyLive(0U, route, tick.compact).disposition ==
                market::EventInputDispositionV1::kPublished,
            "append channel-five Event node");
    }
    *ok &= Expect(
        history->AcquireStable(1U, &pinned) ==
                market::OrderedEventHistoryErrorV1::kNone &&
            pinned.root != nullptr && pinned.root->row_count() == 3U,
        "pin immutable root before inserting a lower channel");
    for (std::int64_t sequence : {10, 11, 12}) {
        TickPair tick = ShanghaiTick(
            sequence,
            arrival++,
            market::TickActionV1::kAdd,
            9U * 3'600U * kSecond +
                static_cast<std::uint64_t>(sequence),
            11'000'000,
            1,
            20'000 + sequence,
            1U,
            0U,
            2);
        *ok &= Expect(
            history->ApplyLive(0U, route, tick.compact).disposition ==
                market::EventInputDispositionV1::kPublished,
            "append channel-two Event node");
    }
    market::EventStableSnapshotV1 current{};
    *ok &= Expect(
        history->AcquireStable(1U, &current) ==
                market::OrderedEventHistoryErrorV1::kNone &&
            current.root != nullptr && current.root->strictly_ordered(),
        "persistent Event root remains strictly ordered across channels");
    const auto rows = EventRows(current, ok);
    *ok &= Expect(
        rows.size() == 6U && rows[0U].order_key.channel == 2 &&
            rows[0U].order_key.business_sequence == 10 &&
            rows[2U].order_key.business_sequence == 12 &&
            rows[3U].order_key.channel == 5 &&
            rows[3U].order_key.business_sequence == 100 &&
            rows[5U].order_key.business_sequence == 102,
        "presentation order is channel then BusinessSequence");
    const auto pinned_rows = EventRows(pinned, ok);
    *ok &= Expect(
        pinned_rows.size() == 3U &&
            pinned_rows[0U].order_key.channel == 5,
        "lower-channel appends do not mutate a pinned older root");
}

void TestPersistentKLineBlocks(bool* ok) {
    market::MutableKLineHistoryConfigV1 config{};
    config.session_id = SessionId();
    config.trade_date = kTradeDate;
    config.instrument_count = 1U;
    config.worker_count = 1U;
    config.windows = {{1U, kSecond}};
    config.maximum_trades_per_instrument = 32U;
    config.maximum_bars_per_instrument = 32U;
    config.stable_block_bars = 2U;
    config.cdc_range_chunk_bars = 2U;
    config.maximum_change_records_per_instrument = 128U;
    config.maximum_changes_per_read = 32U;
    config.kline_routes = {0U};
    std::unique_ptr<market::MutableKLineHistoryV1> history;
    *ok &= Expect(
        market::MutableKLineHistoryV1::Create(config, &history) ==
                market::MutableKLineHistoryCreateErrorV1::kNone &&
            history != nullptr,
        "create persistent KLine block root");
    if (history == nullptr) {
        return;
    }
    market::KLineRouteTokenV1 route{};
    *ok &= Expect(
        history->ResolveRoute(0U, 1U, &route) ==
            market::MutableKLineHistoryErrorV1::kNone,
        "resolve persistent KLine route");
    constexpr std::uint64_t base = 9U * 3'600U * kSecond;
    market::KLineStableSnapshotV1 pinned{};
    for (std::uint64_t index = 0U; index < 5U; ++index) {
        TickPair tick = ShanghaiTick(
            static_cast<std::int64_t>(index + 1U),
            index + 1U,
            market::TickActionV1::kTrade,
            base + index * kSecond + 500'000'000U,
            10'000'000 + static_cast<std::int64_t>(index),
            1,
            100 + static_cast<std::int64_t>(index));
        *ok &= Expect(
            history->ApplyLive(0U, route, tick.compact).disposition ==
                market::KLineInputDispositionV1::kUpserted,
            "insert bar through persistent KLine block directory");
        if (index == 3U) {
            *ok &= Expect(
                history->AcquireStable(1U, &pinned) ==
                        market::MutableKLineHistoryErrorV1::kNone &&
                    pinned.root != nullptr &&
                    pinned.root->bar_count() == 4U,
                "pin KLine root before a block split and historical update");
        }
    }
    TickPair historical = ShanghaiTick(
        6,
        6U,
        market::TickActionV1::kTrade,
        base + 100'000'000U,
        9'000'000,
        2,
        106);
    *ok &= Expect(
        history->ApplyLive(0U, route, historical.compact).disposition ==
            market::KLineInputDispositionV1::kUpserted,
        "update a historical bar through copy-on-write block replacement");
    market::KLineStableSnapshotV1 current{};
    std::vector<market::KLineBarV1> bars;
    const market::KLineBarKeyV1 first_key{1U, 1U, base};
    market::KLineBarV1 first{};
    market::KLineBarV1 old_first{};
    *ok &= Expect(
        history->AcquireStable(1U, &current) ==
                market::MutableKLineHistoryErrorV1::kNone &&
            current.root != nullptr && current.root->bar_count() == 5U &&
            current.root->CopyBars(&bars) && bars.size() == 5U &&
            current.root->Find(first_key, &first) &&
            pinned.root->Find(first_key, &old_first),
        "read ordered KLine blocks after split and point replacement");
    *ok &= Expect(
        first.revision == 2U && first.open_price_p6 == 9'000'000 &&
            first.volume_raw == 3U && old_first.revision == 1U &&
            old_first.open_price_p6 == 10'000'000,
        "KLine point replacement preserves the pinned prior block");
    for (std::size_t index = 1U; index < bars.size(); ++index) {
        *ok &= Expect(
            market::KLineBarKeyLessV1(
                market::KLineBarKeyV1{
                    bars[index - 1U].instrument_id,
                    bars[index - 1U].window_id,
                    bars[index - 1U].window_start_ns_since_midnight},
                market::KLineBarKeyV1{
                    bars[index].instrument_id,
                    bars[index].window_id,
                    bars[index].window_start_ns_since_midnight}),
            "KLine block directory returns strictly key-ordered bars");
    }
}

void TestDeterministicDerivedFailuresFailClosed(bool* ok) {
    market::OrderedEventHistoryConfigV1 event_config{};
    event_config.session_id = SessionId();
    event_config.trade_date = kTradeDate;
    event_config.instrument_count = 1U;
    event_config.worker_count = 1U;
    event_config.maximum_order_states_per_instrument = 16U;
    event_config.maximum_inputs_per_instrument = 16U;
    event_config.maximum_events_per_instrument = 32U;
    event_config.input_block_records = 2U;
    event_config.event_block_records = 2U;
    event_config.cdc_range_chunk_records = 2U;
    event_config.maximum_change_records_per_instrument = 64U;
    event_config.maximum_changes_per_read = 16U;
    event_config.event_routes = {0U};
    std::unique_ptr<market::OrderedEventHistoryV1> events;
    *ok &= Expect(
        market::OrderedEventHistoryV1::Create(
            event_config, &events) ==
                market::OrderedEventHistoryCreateErrorV1::kNone &&
            events != nullptr,
        "create Event history for deterministic failure isolation");
    if (events != nullptr) {
        market::EventRouteTokenV1 route{};
        *ok &= Expect(
            events->ResolveRoute(0U, 1U, &route) ==
                market::OrderedEventHistoryErrorV1::kNone,
            "resolve Event failure-isolation route");
        TickPair valid = ShanghaiTick(
            1,
            1U,
            market::TickActionV1::kAdd,
            9U * 3'600U * kSecond,
            10'000'000,
            1,
            1,
            1U,
            0U,
            7);
        *ok &= Expect(
            events->ApplyLive(0U, route, valid.compact).error ==
                market::OrderedEventHistoryErrorV1::kNone,
            "publish valid Event before malformed channel input");
        auto forged_event = valid.compact;
        forged_event.ordinal = std::numeric_limits<std::size_t>::max();
        *ok &= Expect(
            events->ApplyLive(0U, route, forged_event).error ==
                    market::OrderedEventHistoryErrorV1::kInvalidInput &&
                events->RepairState(1U) ==
                    market::EventRepairStateV1::kLive,
            "forged Event ordinal is rejected before route-table indexing");
        TickPair malformed = ShanghaiTick(
            2,
            2U,
            market::TickActionV1::kAdd,
            9U * 3'600U * kSecond + 1U,
            10'000'001,
            1,
            2,
            1U,
            0U,
            7);
        malformed.compact.market = market::MarketV1::kShenzhen;
        malformed.compact.kind =
            market::MarketEventKindV1::kShenzhenOrder;
        malformed.compact.source =
            market::FastTickSourceV1::kShenzhenTick;
        const auto rejected = events->ApplyLive(
            0U, route, malformed.compact);
        *ok &= Expect(
            rejected.error ==
                    market::OrderedEventHistoryErrorV1::kInvalidInput &&
                events->RepairState(1U) ==
                    market::EventRepairStateV1::kUnrecoverable,
            "deterministic Event core mismatch cannot remain LIVE or spin repair");
    }

    market::MutableKLineHistoryConfigV1 kline_config{};
    kline_config.session_id = SessionId();
    kline_config.trade_date = kTradeDate;
    kline_config.instrument_count = 1U;
    kline_config.worker_count = 1U;
    kline_config.windows = {{1U, kSecond}};
    kline_config.maximum_trades_per_instrument = 4U;
    kline_config.maximum_bars_per_instrument = 1U;
    kline_config.stable_block_bars = 2U;
    kline_config.cdc_range_chunk_bars = 2U;
    kline_config.maximum_change_records_per_instrument = 16U;
    kline_config.maximum_changes_per_read = 8U;
    kline_config.kline_routes = {0U};
    std::unique_ptr<market::MutableKLineHistoryV1> klines;
    *ok &= Expect(
        market::MutableKLineHistoryV1::Create(
            kline_config, &klines) ==
                market::MutableKLineHistoryCreateErrorV1::kNone &&
            klines != nullptr,
        "create capacity-bounded KLine history");
    if (klines == nullptr) {
        return;
    }
    market::KLineRouteTokenV1 route{};
    *ok &= Expect(
        klines->ResolveRoute(0U, 1U, &route) ==
            market::MutableKLineHistoryErrorV1::kNone,
        "resolve KLine failure-isolation route");
    constexpr std::uint64_t base = 9U * 3'600U * kSecond;
    TickPair first = ShanghaiTick(
        10,
        1U,
        market::TickActionV1::kTrade,
        base + 1U,
        10'000'000,
        1,
        10);
    TickPair second = ShanghaiTick(
        11,
        2U,
        market::TickActionV1::kTrade,
        base + kSecond + 1U,
        10'000'001,
        1,
        11);
    *ok &= Expect(
        klines->ApplyLive(0U, route, first.compact).error ==
            market::MutableKLineHistoryErrorV1::kNone,
        "publish first bounded KLine bar");
    auto mismatched_source = first.compact;
    mismatched_source.source =
        market::FastTickSourceV1::kShenzhenTick;
    *ok &= Expect(
        klines->ApplyLive(0U, route, mismatched_source).error ==
                market::MutableKLineHistoryErrorV1::kInvalidInput &&
            klines->RepairState(1U) ==
                market::EventRepairStateV1::kLive,
        "KLine rejects a forged source/market envelope without mutating state");
    const auto exhausted = klines->ApplyLive(
        0U, route, second.compact);
    *ok &= Expect(
        exhausted.error ==
                market::MutableKLineHistoryErrorV1::kBarCapacity &&
            klines->RepairState(1U) ==
                market::EventRepairStateV1::kUnrecoverable,
        "deterministic KLine capacity exhaustion fails closed");

    market::KLineRouteTokenV1 forged = route;
    forged.ordinal = std::numeric_limits<std::size_t>::max();
    TickPair forged_tick = ShanghaiTick(
        12,
        3U,
        market::TickActionV1::kTrade,
        base + 2U,
        10'000'002,
        1,
        12);
    forged_tick.compact.ordinal = forged.ordinal;
    *ok &= Expect(
        klines->ApplyLive(0U, forged, forged_tick.compact).error ==
            market::MutableKLineHistoryErrorV1::kInvalidInput,
        "forged KLine route is rejected before route-table indexing");
}

void TestCdcCapacityFailsClosedWithoutPartialPublication(bool* ok) {
    market::OrderedEventHistoryConfigV1 event_config{};
    event_config.session_id = SessionId();
    event_config.trade_date = kTradeDate;
    event_config.instrument_count = 1U;
    event_config.worker_count = 1U;
    event_config.maximum_order_states_per_instrument = 16U;
    event_config.maximum_inputs_per_instrument = 16U;
    event_config.maximum_events_per_instrument = 16U;
    event_config.input_block_records = 2U;
    event_config.event_block_records = 2U;
    event_config.cdc_range_chunk_records = 2U;
    event_config.maximum_change_records_per_instrument = 1U;
    event_config.maximum_changes_per_read = 2U;
    event_config.event_routes = {0U};
    std::unique_ptr<market::OrderedEventHistoryV1> events;
    *ok &= Expect(
        market::OrderedEventHistoryV1::Create(event_config, &events) ==
                market::OrderedEventHistoryCreateErrorV1::kNone &&
            events != nullptr,
        "create Event history with a one-record CDC bound");
    if (events != nullptr) {
        market::EventRouteTokenV1 route{};
        *ok &= Expect(
            events->ResolveRoute(0U, 1U, &route) ==
                market::OrderedEventHistoryErrorV1::kNone,
            "resolve CDC-bounded Event route");
        TickPair first = ShanghaiTick(
            1,
            1U,
            market::TickActionV1::kAdd,
            9U * 3'600U * kSecond,
            10'000'000,
            1,
            1);
        TickPair second = ShanghaiTick(
            2,
            2U,
            market::TickActionV1::kAdd,
            9U * 3'600U * kSecond + 1U,
            10'000'001,
            1,
            2);
        *ok &= Expect(
            events->ApplyLive(0U, route, first.compact).error ==
                market::OrderedEventHistoryErrorV1::kNone,
            "fill the Event CDC bound");
        const auto exhausted = events->ApplyLive(
            0U, route, second.compact);
        market::EventStableSnapshotV1 stable{};
        *ok &= Expect(
            exhausted.error ==
                    market::OrderedEventHistoryErrorV1::kChangeCapacity &&
                events->RepairState(1U) ==
                    market::EventRepairStateV1::kUnrecoverable &&
                events->AcquireStable(1U, &stable) ==
                    market::OrderedEventHistoryErrorV1::kNone &&
                stable.root != nullptr && stable.root->row_count() == 1U &&
                stable.root->included_change_sequence() == 1U,
            "Event CDC exhaustion preserves the prior complete root and fails closed");
    }

    market::MutableKLineHistoryConfigV1 kline_config{};
    kline_config.session_id = SessionId();
    kline_config.trade_date = kTradeDate;
    kline_config.instrument_count = 1U;
    kline_config.worker_count = 1U;
    kline_config.windows = {{1U, kSecond}};
    kline_config.maximum_trades_per_instrument = 4U;
    kline_config.maximum_bars_per_instrument = 4U;
    kline_config.stable_block_bars = 2U;
    kline_config.cdc_range_chunk_bars = 2U;
    kline_config.maximum_change_records_per_instrument = 1U;
    kline_config.maximum_changes_per_read = 2U;
    kline_config.kline_routes = {0U};
    std::unique_ptr<market::MutableKLineHistoryV1> klines;
    *ok &= Expect(
        market::MutableKLineHistoryV1::Create(
            kline_config, &klines) ==
                market::MutableKLineHistoryCreateErrorV1::kNone &&
            klines != nullptr,
        "create KLine history with a one-record CDC bound");
    if (klines == nullptr) {
        return;
    }
    market::KLineRouteTokenV1 route{};
    *ok &= Expect(
        klines->ResolveRoute(0U, 1U, &route) ==
            market::MutableKLineHistoryErrorV1::kNone,
        "resolve CDC-bounded KLine route");
    constexpr std::uint64_t window_start = 9U * 3'600U * kSecond;
    TickPair first = ShanghaiTick(
        10,
        1U,
        market::TickActionV1::kTrade,
        window_start + 1U,
        10'000'000,
        1,
        10);
    TickPair second = ShanghaiTick(
        11,
        2U,
        market::TickActionV1::kTrade,
        window_start + 2U,
        11'000'000,
        2,
        11);
    *ok &= Expect(
        klines->ApplyLive(0U, route, first.compact).error ==
            market::MutableKLineHistoryErrorV1::kNone,
        "fill the KLine CDC bound");
    const auto exhausted = klines->ApplyLive(
        0U, route, second.compact);
    market::KLineStableSnapshotV1 stable{};
    market::KLineBarV1 bar{};
    *ok &= Expect(
        exhausted.error ==
                market::MutableKLineHistoryErrorV1::kChangeCapacity &&
            klines->RepairState(1U) ==
                market::EventRepairStateV1::kUnrecoverable &&
            klines->AcquireStable(1U, &stable) ==
                market::MutableKLineHistoryErrorV1::kNone &&
            stable.root != nullptr &&
            stable.root->Find(
                market::KLineBarKeyV1{1U, 1U, window_start}, &bar) &&
            bar.revision == 1U && bar.volume_raw == 1U,
        "KLine CDC exhaustion rolls back working bars and preserves the prior root");
}

void TestFastCapacityIsInstrumentLocal(bool* ok) {
    constexpr std::size_t instruments = 8U;
    market::FastTickStoreConfigV1 config{};
    config.session_id = SessionId();
    config.trade_date = kTradeDate;
    config.instrument_count = instruments;
    config.worker_count = 1U;
    config.maximum_session_records = instruments;
    // Deliberately larger than every instrument's one-row partition. A
    // worker-global page pool would let the first instrument strand seven
    // advertised slots in its page and exhaust immediately.
    config.records_per_chunk = instruments;
    config.maximum_records_per_read = instruments;
    config.coverage_from_open = true;
    config.tick_routes.assign(instruments, 0U);

    std::unique_ptr<market::FastTickStoreV1> store;
    *ok &= Expect(
        market::FastTickStoreV1::Create(config, &store) ==
                market::FastTickStoreCreateErrorV1::kNone &&
            store != nullptr,
        "create instrument-partitioned FAST capacity");
    if (store == nullptr) {
        return;
    }

    for (std::size_t ordinal = 0U; ordinal < instruments; ++ordinal) {
        const std::uint32_t instrument_id =
            static_cast<std::uint32_t>(ordinal + 1U);
        market::FastTickRouteTokenV1 route{};
        *ok &= Expect(
            store->ResolveRoute(ordinal, instrument_id, &route) ==
                market::FastTickStoreQueryErrorV1::kNone,
            "resolve sparse FAST route");
        TickPair tick = ShanghaiTick(
            1'000 + static_cast<std::int64_t>(ordinal),
            ordinal + 1U,
            market::TickActionV1::kAdd,
            9U * 3'600U * kSecond + ordinal,
            10'000'000,
            1,
            1'000 + static_cast<std::int64_t>(ordinal),
            instrument_id,
            ordinal);
        *ok &= Expect(
            store->Append(
                0U, route, tick.compact, std::move(tick.owned)) ==
                market::FastTickStoreAppendErrorV1::kNone,
            "every sparse instrument receives its advertised FAST slot");
        market::FastTickInstrumentStatusV1 status{};
        *ok &= Expect(
            store->Status(instrument_id, &status) ==
                    market::FastTickStoreQueryErrorV1::kNone &&
                status.record_capacity == 1U &&
                status.published_tail == 1U && status.coverage_complete,
            "FAST status reports the local capacity and tail");
    }

    market::FastTickRouteTokenV1 first_route{};
    *ok &= Expect(
        store->ResolveRoute(0U, 1U, &first_route) ==
            market::FastTickStoreQueryErrorV1::kNone,
        "resolve exhausted FAST instrument");
    TickPair overflow = ShanghaiTick(
        2'000,
        instruments + 1U,
        market::TickActionV1::kAdd,
        9U * 3'600U * kSecond + instruments,
        10'000'000,
        1,
        2'000);
    *ok &= Expect(
        store->Append(
            0U,
            first_route,
            overflow.compact,
            std::move(overflow.owned)) ==
            market::FastTickStoreAppendErrorV1::kRecordCapacity,
        "FAST capacity exhaustion is attributed to the target instrument");
    market::FastTickInstrumentStatusV1 unaffected{};
    *ok &= Expect(
        store->Status(2U, &unaffected) ==
                market::FastTickStoreQueryErrorV1::kNone &&
            unaffected.coverage_complete,
        "one instrument's capacity failure does not poison another");
}

void TestTerminalRepairStatesAreSticky(bool* ok) {
    constexpr std::size_t records = 4096U;
    constexpr std::size_t fast_records = records + 1U;
    market::FastTickStoreConfigV1 fast_config{};
    fast_config.session_id = SessionId();
    fast_config.trade_date = kTradeDate;
    fast_config.instrument_count = 1U;
    fast_config.worker_count = 1U;
    fast_config.maximum_session_records = fast_records;
    fast_config.records_per_chunk = 64U;
    fast_config.maximum_records_per_read = fast_records;
    fast_config.coverage_from_open = true;
    fast_config.tick_routes = {0U};
    std::unique_ptr<market::FastTickStoreV1> fast;
    *ok &= Expect(
        market::FastTickStoreV1::Create(fast_config, &fast) ==
                market::FastTickStoreCreateErrorV1::kNone &&
            fast != nullptr,
        "create FAST fixture for terminal-state race");
    if (fast == nullptr) {
        return;
    }
    market::FastTickRouteTokenV1 fast_route{};
    *ok &= Expect(
        fast->ResolveRoute(0U, 1U, &fast_route) ==
            market::FastTickStoreQueryErrorV1::kNone,
        "resolve terminal-state FAST route");
    for (std::size_t index = 0U; index < records; ++index) {
        const std::uint64_t sequence = index + 1U;
        TickPair tick = ShanghaiTick(
            static_cast<std::int64_t>(sequence),
            sequence,
            market::TickActionV1::kTrade,
            9U * 3'600U * kSecond + sequence,
            10'000'000,
            1,
            static_cast<std::int64_t>(sequence));
        if (!Append(fast.get(), fast_route, &tick)) {
            *ok = false;
            return;
        }
    }
    // Put a conflicting duplicate at the end of the durable FAST prefix.
    // The main thread marks coverage terminal while the rebuild is still
    // scanning the long valid prefix; conflict discovery must not downgrade
    // UNRECOVERABLE back to SOURCE_CONFLICT.
    TickPair conflicting = ShanghaiTick(
        static_cast<std::int64_t>(records),
        fast_records,
        market::TickActionV1::kTrade,
        9U * 3'600U * kSecond + records,
        11'000'000,
        1,
        static_cast<std::int64_t>(records));
    if (!Append(fast.get(), fast_route, &conflicting)) {
        *ok = false;
        return;
    }

    market::OrderedEventHistoryConfigV1 event_config{};
    event_config.session_id = SessionId();
    event_config.trade_date = kTradeDate;
    event_config.instrument_count = 1U;
    event_config.worker_count = 1U;
    event_config.maximum_order_states_per_instrument = records * 3U;
    event_config.maximum_inputs_per_instrument = fast_records;
    event_config.maximum_events_per_instrument = records * 4U;
    event_config.input_block_records = 64U;
    event_config.event_block_records = 64U;
    event_config.cdc_range_chunk_records = 256U;
    event_config.maximum_change_records_per_instrument = records;
    event_config.maximum_changes_per_read = 64U;
    event_config.repair_replay_record_budget = 1U;
    event_config.event_routes = {0U};
    std::unique_ptr<market::OrderedEventHistoryV1> events;
    *ok &= Expect(
        market::OrderedEventHistoryV1::Create(event_config, &events) ==
                market::OrderedEventHistoryCreateErrorV1::kNone &&
            events != nullptr,
        "create Event terminal-state fixture");
    if (events != nullptr) {
        market::EventRouteTokenV1 route{};
        *ok &= Expect(
            events->ResolveRoute(0U, 1U, &route) ==
                market::OrderedEventHistoryErrorV1::kNone,
            "resolve Event terminal-state route");
        events->MarkRepairRequired(1U, fast_records);
        std::atomic<bool> done{false};
        std::thread repair([&] {
            static_cast<void>(events->RebuildFromFast(0U, route, *fast));
            done.store(true, std::memory_order_release);
        });
        bool observed_repair = false;
        while (!done.load(std::memory_order_acquire)) {
            const auto state = events->RepairState(1U);
            if (state == market::EventRepairStateV1::kRebuilding ||
                state == market::EventRepairStateV1::kCatchingUp) {
                observed_repair = true;
                events->MarkUnrecoverable(1U);
                break;
            }
            std::this_thread::yield();
        }
        if (!observed_repair) {
            events->MarkUnrecoverable(1U);
        }
        repair.join();
        *ok &= Expect(
            observed_repair &&
                events->RepairState(1U) ==
                    market::EventRepairStateV1::kUnrecoverable,
            "Event rebuild cannot resurrect a concurrent terminal state");
    }

    market::MutableKLineHistoryConfigV1 kline_config{};
    kline_config.session_id = SessionId();
    kline_config.trade_date = kTradeDate;
    kline_config.instrument_count = 1U;
    kline_config.worker_count = 1U;
    kline_config.windows = {{1U, kSecond}};
    kline_config.maximum_trades_per_instrument = fast_records;
    kline_config.maximum_bars_per_instrument = records;
    kline_config.stable_block_bars = 64U;
    kline_config.cdc_range_chunk_bars = 64U;
    kline_config.maximum_change_records_per_instrument = records;
    kline_config.maximum_changes_per_read = 64U;
    kline_config.repair_replay_record_budget = 1U;
    kline_config.kline_routes = {0U};
    std::unique_ptr<market::MutableKLineHistoryV1> klines;
    *ok &= Expect(
        market::MutableKLineHistoryV1::Create(
            kline_config, &klines) ==
                market::MutableKLineHistoryCreateErrorV1::kNone &&
            klines != nullptr,
        "create KLine terminal-state fixture");
    if (klines != nullptr) {
        market::KLineRouteTokenV1 route{};
        *ok &= Expect(
            klines->ResolveRoute(0U, 1U, &route) ==
                market::MutableKLineHistoryErrorV1::kNone,
            "resolve KLine terminal-state route");
        klines->MarkRepairRequired(1U, fast_records);
        std::atomic<bool> done{false};
        std::thread repair([&] {
            static_cast<void>(klines->RebuildFromFast(0U, route, *fast));
            done.store(true, std::memory_order_release);
        });
        bool observed_repair = false;
        while (!done.load(std::memory_order_acquire)) {
            const auto state = klines->RepairState(1U);
            if (state == market::EventRepairStateV1::kRebuilding ||
                state == market::EventRepairStateV1::kCatchingUp) {
                observed_repair = true;
                klines->MarkUnrecoverable(1U);
                break;
            }
            std::this_thread::yield();
        }
        if (!observed_repair) {
            klines->MarkUnrecoverable(1U);
        }
        repair.join();
        *ok &= Expect(
            observed_repair &&
                klines->RepairState(1U) ==
                    market::EventRepairStateV1::kUnrecoverable,
            "KLine rebuild cannot resurrect a concurrent terminal state");
    }
}

}  // namespace

int main() {
    bool ok = true;
    TestBusinessDuplicateIdentity(&ok);
    TestFastRejectsForgedProjection(&ok);
    TestFastStatusIsCoherentDuringPublication(&ok);
    TestKLineTradeProjection(&ok);
    TestFastAndEventRepair(&ok);
    TestKLineRebuildAndRevision(&ok);
    TestPersistentEventChannels(&ok);
    TestPersistentKLineBlocks(&ok);
    TestDeterministicDerivedFailuresFailClosed(&ok);
    TestCdcCapacityFailsClosedWithoutPartialPublication(&ok);
    TestFastCapacityIsInstrumentLocal(&ok);
    TestTerminalRepairStatesAreSticky(&ok);
    return ok ? 0 : 1;
}
