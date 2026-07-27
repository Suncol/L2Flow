#include "l2flow/market/realtime_history_v1.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace {

namespace common = l2flow::common;
namespace market = l2flow::market;

std::vector<std::byte> Bytes(std::string_view text) {
    const auto bytes = std::as_bytes(std::span(text));
    return {bytes.begin(), bytes.end()};
}

std::unique_ptr<market::InstrumentRegistryV1> MakeRegistry() {
    std::vector<market::InstrumentRegistryEntryV1> entries;
    for (std::uint32_t instrument_id : {1U, 2U, 9U}) {
        market::InstrumentRegistryEntryV1 entry{};
        entry.instrument_id = instrument_id;
        entry.key.market = market::MarketV1::kShanghai;
        entry.key.security_id_source = Bytes("101");
        entry.key.security_id = Bytes(
            instrument_id == 1U
                ? "600001"
                : instrument_id == 2U ? "600002" : "600009");
        entry.quantity_unit = market::QuantityUnitV1::kShare;
        entry.security_type = market::SecurityTypeV1::kEquity;
        entry.asset_scope = market::AssetScopeV1::kDocumentedCore;
        entries.push_back(std::move(entry));
    }
    std::unique_ptr<market::InstrumentRegistryV1> registry;
    if (market::InstrumentRegistryV1::Create(
            7U, entries, &registry) !=
        market::InstrumentRegistryCreateErrorV1::kNone) {
        return nullptr;
    }
    return registry;
}

std::optional<market::RealtimeHistoryEventInputV1> MakeRecord(
    const market::InstrumentRegistryV1& registry,
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence,
    std::uint32_t instrument_id,
    std::int64_t price) {
    market::ShanghaiSnapshotV1 snapshot{};
    snapshot.common.kind = market::MarketEventKindV1::kShanghaiSnapshot;
    snapshot.common.market = market::MarketV1::kShanghai;
    snapshot.common.origin.source_stream_id = 11U;
    snapshot.common.origin.trade_date = 20260724U;
    snapshot.common.origin.source_sequence = source_sequence;
    snapshot.common.instrument_id = instrument_id;
    const auto lookup = registry.LookupById(instrument_id);
    if (!lookup.known()) {
        return std::nullopt;
    }
    snapshot.common.registry_ordinal = lookup.registry_ordinal;
    snapshot.last_price.valid = true;
    snapshot.last_price.raw = price;
    snapshot.last_price.normalized_p6 = price;
    snapshot.last_price.scale = 6U;

    market::DecodedMarketEventV1 decoded(std::move(snapshot));
    return market::RealtimeHistoryEventInputV1::Create(
        0U, ingress_sequence, std::move(decoded));
}

std::optional<market::RealtimeHistoryEventInputV1> MakeTickRecord(
    const market::InstrumentRegistryV1& registry,
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence,
    std::uint32_t instrument_id,
    std::int64_t price) {
    market::ShanghaiTickV1 tick{};
    tick.common.kind = market::MarketEventKindV1::kShanghaiTick;
    tick.common.market = market::MarketV1::kShanghai;
    tick.common.origin.source_stream_id = 12U;
    tick.common.origin.trade_date = 20260724U;
    tick.common.origin.source_sequence = source_sequence;
    tick.common.instrument_id = instrument_id;
    const auto lookup = registry.LookupById(instrument_id);
    if (!lookup.known()) {
        return std::nullopt;
    }
    tick.common.registry_ordinal = lookup.registry_ordinal;
    tick.fields.price.valid = true;
    tick.fields.price.raw = price;
    tick.fields.price.normalized_p6 = price;
    tick.fields.price.scale = 6U;

    market::DecodedMarketEventV1 decoded(std::move(tick));
    return market::RealtimeHistoryEventInputV1::Create(
        1U, ingress_sequence, std::move(decoded));
}

constexpr std::int64_t kTradeDateMidnightUnixNs =
    1'784'822'400'000'000'000LL;

constexpr std::uint64_t TimeSinceMidnightNs(
    std::uint32_t hour,
    std::uint32_t minute,
    std::uint32_t second,
    std::uint32_t millisecond) {
    return (((static_cast<std::uint64_t>(hour) * 60U + minute) * 60U +
             second) *
                market::kKLineNanosecondsPerSecondV1) +
           static_cast<std::uint64_t>(millisecond) *
               market::kKLineNanosecondsPerMillisecondV1;
}

std::optional<market::RealtimeHistoryEventInputV1> MakeKLineTradeRecord(
    const market::InstrumentRegistryV1& registry,
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence,
    std::uint32_t instrument_id,
    std::uint32_t raw_exchange_time,
    std::uint64_t exchange_time_ns_since_midnight,
    std::int64_t price_p6,
    std::int64_t quantity,
    std::int64_t recv_realtime_ns,
    std::int64_t recv_monotonic_ns) {
    market::ShanghaiTickV1 tick{};
    tick.common.kind = market::MarketEventKindV1::kShanghaiTick;
    tick.common.market = market::MarketV1::kShanghai;
    tick.common.origin.source_stream_id = 12U;
    tick.common.origin.trade_date = 20260724U;
    tick.common.origin.source_sequence = source_sequence;
    tick.common.origin.recv_realtime_ns = recv_realtime_ns;
    tick.common.origin.recv_monotonic_ns = recv_monotonic_ns;
    tick.common.instrument_id = instrument_id;
    const auto lookup = registry.LookupById(instrument_id);
    if (!lookup.known() ||
        exchange_time_ns_since_midnight >
            static_cast<std::uint64_t>(
                std::numeric_limits<std::int64_t>::max()) ||
        exchange_time_ns_since_midnight >=
            market::kKLineNanosecondsPerDayV1) {
        return std::nullopt;
    }
    tick.common.registry_ordinal = lookup.registry_ordinal;
    tick.common.quantity_unit = lookup.quantity_unit;
    tick.common.security_type = lookup.security_type;
    tick.common.asset_scope = lookup.asset_scope;
    tick.common.exchange_time.raw_hhmmssmmm = raw_exchange_time;
    tick.common.exchange_time.nanoseconds_since_midnight =
        exchange_time_ns_since_midnight;
    tick.common.exchange_time.unix_nanoseconds =
        kTradeDateMidnightUnixNs +
        static_cast<std::int64_t>(exchange_time_ns_since_midnight);
    tick.common.exchange_time.valid = true;
    tick.common.exchange_time.unix_nanoseconds_valid = true;
    tick.fields.action = market::TickActionV1::kTrade;
    tick.fields.price.valid = true;
    tick.fields.price.raw = price_p6;
    tick.fields.price.normalized_p6 = price_p6;
    tick.fields.price.scale = 6U;
    tick.fields.quantity.valid = true;
    tick.fields.quantity.raw = quantity;
    tick.fields.quantity.scale = 0U;
    tick.fields.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickExchangeTimeValidV1;

    market::DecodedMarketEventV1 decoded(std::move(tick));
    return market::RealtimeHistoryEventInputV1::Create(
        1U, ingress_sequence, std::move(decoded));
}

market::RealtimeHistorySubmitErrorV1 Submit(
    market::RealtimeHistoryRuntimeV1* runtime,
    std::optional<market::RealtimeHistoryEventInputV1> input) {
    if (runtime == nullptr || !input.has_value()) {
        return market::RealtimeHistorySubmitErrorV1::kInvalidRecord;
    }
    return runtime->TrySubmit(std::move(*input));
}

market::RealtimeHistoryWatermarkV1 MakeWatermark(
    const market::InstrumentRegistryV1& registry,
    std::uint64_t generation,
    std::uint64_t ingress_exclusive,
    std::uint64_t sh_snapshot_exclusive,
    std::uint64_t sh_tick_exclusive) {
    common::Identity128 run_id{};
    run_id[0] = std::byte{0x42U};
    const std::array<market::RealtimeSourceWatermarkV1, 4U> sources{{
        {11U, sh_snapshot_exclusive},
        {12U, sh_tick_exclusive},
        {13U, 1U},
        {14U, 1U},
    }};
    market::RealtimeHistoryWatermarkV1 watermark{};
    if (market::BuildRealtimeHistoryWatermarkV1(
            run_id,
            generation,
            20260724U,
            ingress_exclusive,
            1000U + generation,
            registry,
            sources,
            &watermark) !=
        market::RealtimeHistoryWatermarkErrorV1::kNone) {
        return {};
    }
    return watermark;
}

bool Expect(bool condition, std::string_view message);

std::int64_t LastPrice(
    const market::IntradayInstrumentSummaryV1& summary) {
    if (summary.latest_snapshot == nullptr) {
        return -1;
    }
    const auto* snapshot = market::StoredMarketEventGetV1<
        market::ShanghaiSnapshotV1>(summary.latest_snapshot->event());
    return snapshot == nullptr ? -1 : snapshot->last_price.normalized_p6;
}

std::vector<std::uint64_t> TailIngressSequences(
    const market::IntradayInstrumentStoreGenerationV1& generation,
    std::uint32_t instrument_id,
    std::uint64_t count,
    bool* ok) {
    std::unique_ptr<market::IntradayInstrumentCursorV1> cursor;
    *ok &= Expect(
        generation.OpenTailCursor(instrument_id, count, &cursor) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            cursor != nullptr,
        "open store tail cursor");
    if (cursor == nullptr) {
        return {};
    }
    std::vector<std::uint64_t> result;
    std::array<const market::RealtimeHistoryRecordV1*, 4U> batch{};
    for (;;) {
        std::size_t written = 0U;
        const auto error = cursor->ReadBatch(batch, &written);
        *ok &= Expect(
            error == market::IntradayInstrumentStoreQueryErrorV1::kNone,
            "read store tail cursor");
        if (error != market::IntradayInstrumentStoreQueryErrorV1::kNone) {
            return result;
        }
        for (std::size_t index = 0U; index < written; ++index) {
            result.push_back(batch[index]->ingress_sequence());
        }
        if (written == 0U) {
            *ok &= Expect(cursor->done(), "tail cursor reaches end");
            return result;
        }
    }
}

std::vector<market::KLineBarV1> ReadKLines(
    const market::RealtimeKLineGenerationV1& generation,
    std::uint32_t instrument_id,
    std::uint32_t window_id,
    bool* ok) {
    std::unique_ptr<market::KLineCursorV1> cursor;
    *ok &= Expect(
        generation.OpenInstrumentCursor(
            instrument_id, window_id, &cursor) ==
                market::KLineQueryErrorV1::kNone &&
            cursor != nullptr,
        "open KLine instrument/window cursor");
    if (cursor == nullptr) {
        return {};
    }

    std::vector<market::KLineBarV1> result;
    std::array<market::KLineBarV1, 2U> batch{};
    for (;;) {
        std::size_t written = 0U;
        const auto error = cursor->ReadBatch(batch, &written);
        *ok &= Expect(
            error == market::KLineQueryErrorV1::kNone,
            "read KLine cursor batch");
        if (error != market::KLineQueryErrorV1::kNone) {
            return result;
        }
        result.insert(
            result.end(), batch.begin(), batch.begin() + written);
        if (written == 0U) {
            *ok &= Expect(cursor->done(), "KLine cursor reaches end");
            break;
        }
    }
    for (std::size_t index = 1U; index < result.size(); ++index) {
        *ok &= Expect(
            result[index - 1U].window_start_ns_since_midnight <
                result[index].window_start_ns_since_midnight,
            "KLine cursor is strictly ascending by window start");
    }
    return result;
}

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

}  // namespace

int main() {
    std::unique_ptr<market::InstrumentRegistryV1> registry = MakeRegistry();
    if (!Expect(registry != nullptr, "registry creation")) {
        return 1;
    }

    market::RealtimeHistoryRuntimeConfigV1 config{};
    config.source_stream_ids = {11U, 12U, 13U, 14U};
    config.worker_count = 2U;
    config.queue_capacity_per_source_worker = 32U;
    config.intraday_store.segment_target_bytes =
        market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
    config.intraday_store.maximum_session_records = 32U;
    config.intraday_store.maximum_session_accounted_bytes = 1U << 20U;
    config.intraday_store.maximum_records_per_batch = 4U;
    config.intraday_store.coverage_from_open = true;
    config.registry = registry.get();
    std::unique_ptr<market::RealtimeHistoryRuntimeV1> runtime;
    if (!Expect(
            market::RealtimeHistoryRuntimeV1::Create(config, &runtime) ==
                market::RealtimeHistoryCreateErrorV1::kNone,
            "history runtime creation")) {
        return 1;
    }
    bool ok = true;
    ok &= Expect(runtime->WorkerForInstrument(1U) == 1U,
                 "instrument 1 has fixed worker 1");
    ok &= Expect(runtime->WorkerForInstrument(2U) == 0U,
                 "instrument 2 has fixed worker 0");
    ok &= Expect(
        MakeWatermark(*registry, 99U, 4U, 2U, 2U).generation == 0U,
        "watermark rejects contradictory global/source prefix counts");
    ok &= Expect(
        MakeWatermark(
            *registry,
            99U,
            std::numeric_limits<std::uint64_t>::max(),
            std::numeric_limits<std::uint64_t>::max(),
            1U).generation == 99U,
        "UINT64_MAX remains representable as an exclusive cut");
    ok &= Expect(
        !MakeRecord(
            *registry,
            std::numeric_limits<std::uint64_t>::max(),
            1U,
            1U,
            1'000'000).has_value() &&
            !MakeRecord(
                *registry,
                1U,
                std::numeric_limits<std::uint64_t>::max(),
                1U,
                1'000'000).has_value(),
        "message records reject the reserved sequence sentinel");

    const auto generation1 = MakeWatermark(*registry, 1U, 4U, 3U, 2U);
    ok &= Expect(
        runtime->BeginGeneration(generation1) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "begin generation 1");
    ok &= Expect(
        Submit(
            runtime.get(),
            MakeRecord(*registry, 1U, 2U, 1U, 1'000'001)) ==
            market::RealtimeHistorySubmitErrorV1::kNone,
        "submit later snapshot to generation-1 worker 1");
    ok &= Expect(
        Submit(
            runtime.get(),
            MakeRecord(*registry, 2U, 3U, 2U, 1'000'002)) ==
            market::RealtimeHistorySubmitErrorV1::kNone,
        "submit generation-1 worker-0 record");
    // Source decoders may reach a worker in a different order from the
    // serialized callback. This earlier global record is deliberately
    // submitted after the later snapshot above.
    ok &= Expect(
        Submit(
            runtime.get(),
            MakeTickRecord(*registry, 1U, 1U, 1U, 900'001)) ==
            market::RealtimeHistorySubmitErrorV1::kNone,
        "submit earlier cross-source tick after later snapshot");

    // Source 0 is fenced first. Its next record is legal realtime work, but
    // every worker must park it until all four generation-1 fences arrive.
    ok &= Expect(
        runtime->SealSource(0U, 1U) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "seal source 0 generation 1");
    ok &= Expect(
        Submit(
            runtime.get(),
            MakeRecord(*registry, 3U, 4U, 1U, 2'000'001)) ==
            market::RealtimeHistorySubmitErrorV1::kNone,
        "submit post-fence source record");
    for (std::uint8_t source = 1U; source < 4U; ++source) {
        ok &= Expect(
            runtime->SealSource(source, 1U) ==
                market::RealtimeHistoryGenerationErrorV1::kNone,
            "seal idle source generation 1");
    }

    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1> first;
    ok &= Expect(
        runtime->WaitForGeneration(
            1U, std::chrono::seconds(2), &first) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "wait generation 1");
    ok &= Expect(first != nullptr && first->instrument_count() == 3U,
                 "generation 1 has exact fixed universe");
    if (first != nullptr) {
        const std::array<std::uint32_t, 3U> expected_ids{1U, 2U, 9U};
        for (std::size_t ordinal = 0U; ordinal < expected_ids.size();
             ++ordinal) {
            market::IntradayInstrumentSummaryV1 ordinal_summary{};
            ok &= Expect(
                first->SummaryAt(ordinal, &ordinal_summary) ==
                        market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                    ordinal_summary.instrument_id == expected_ids[ordinal],
                "SummaryAt exposes the sorted fixed universe");
        }
        market::IntradayInstrumentSummaryV1 first_summary{};
        ok &= Expect(
            first->Find(1U, &first_summary) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                LastPrice(first_summary) == 1'000'001,
            "post-fence update excluded from generation 1");
        ok &= Expect(
            TailIngressSequences(*first, 1U, 3U, &ok) ==
                std::vector<std::uint64_t>{2U, 1U},
            "cross-source tail is merged by process ingress, not worker arrival");
        market::IntradayInstrumentSummaryV1 other_worker{};
        ok &= Expect(
            first->Find(2U, &other_worker) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                LastPrice(other_worker) == 1'000'002,
            "other worker is same generation");
        market::IntradayInstrumentSummaryV1 empty{};
        ok &= Expect(
            first->Find(9U, &empty) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                empty.latest_snapshot == nullptr &&
                empty.latest_tick == nullptr && empty.record_count == 0U,
            "unobserved fixed-universe instrument remains empty");
        ok &= Expect(
            first->SummaryAt(first->instrument_count(), &empty) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNotFound,
            "SummaryAt rejects an ordinal outside the fixed universe");
    }

    const auto generation2 = MakeWatermark(*registry, 2U, 5U, 4U, 2U);
    ok &= Expect(
        runtime->BeginGeneration(generation2) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "begin generation 2");
    for (std::uint8_t source = 0U; source < 4U; ++source) {
        ok &= Expect(
            runtime->SealSource(source, 2U) ==
                market::RealtimeHistoryGenerationErrorV1::kNone,
            "seal source generation 2");
    }
    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1> second;
    ok &= Expect(
        runtime->WaitForGeneration(
            2U, std::chrono::seconds(2), &second) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "wait generation 2");
    if (second != nullptr) {
        market::IntradayInstrumentSummaryV1 second_summary{};
        ok &= Expect(
            second->Find(1U, &second_summary) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                LastPrice(second_summary) == 2'000'001,
            "parked update appears only in generation 2");
        ok &= Expect(
            TailIngressSequences(*second, 1U, 4U, &ok) ==
                std::vector<std::uint64_t>{4U, 2U, 1U},
            "next store generation exposes the complete newest-first tail");
    }
    if (first != nullptr) {
        market::IntradayInstrumentSummaryV1 old_summary{};
        ok &= Expect(
            first->Find(1U, &old_summary) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                LastPrice(old_summary) == 1'000'001 &&
                TailIngressSequences(*first, 1U, 4U, &ok) ==
                    std::vector<std::uint64_t>{2U, 1U},
            "old generation remains immutable while the session grows");
    }
    ok &= Expect(runtime->AcquireLatestGeneration().get() == second.get(),
                 "whole generation atomically published");
    ok &= Expect(!runtime->IsGenerationCurrentAndHealthy(first) &&
                     runtime->IsGenerationCurrentAndHealthy(second),
                 "only latest complete generation is current");

    runtime->StopAndDrain();

    // A separate runtime exercises event-time KLine aggregation without
    // changing any of the store-only generation assertions above.
    market::RealtimeHistoryRuntimeConfigV1 kline_config{};
    kline_config.source_stream_ids = {11U, 12U, 13U, 14U};
    kline_config.worker_count = 2U;
    kline_config.queue_capacity_per_source_worker = 32U;
    kline_config.intraday_store.segment_target_bytes =
        market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
    kline_config.intraday_store.maximum_session_records = 16U;
    kline_config.intraday_store.maximum_session_accounted_bytes =
        1U << 20U;
    kline_config.intraday_store.maximum_records_per_batch = 4U;
    kline_config.intraday_store.coverage_from_open = true;
    kline_config.kline.trade_date = 20260724U;
    kline_config.kline.windows = {
        {1U, market::kKLineNanosecondsPerSecondV1},
        {2U, 5U * market::kKLineNanosecondsPerSecondV1},
    };
    // Leave maximum_bars at zero so history derives the bounded capacity
    // from its retained-record limit and the two configured windows.
    kline_config.registry = registry.get();

    std::unique_ptr<market::RealtimeHistoryRuntimeV1> kline_runtime;
    ok &= Expect(
        market::RealtimeHistoryRuntimeV1::Create(
            kline_config, &kline_runtime) ==
                market::RealtimeHistoryCreateErrorV1::kNone &&
            kline_runtime != nullptr,
        "KLine history runtime creation");
    if (kline_runtime == nullptr) {
        return 1;
    }

    constexpr std::uint64_t k093000100 =
        TimeSinceMidnightNs(9U, 30U, 0U, 100U);
    constexpr std::uint64_t k093000500 =
        TimeSinceMidnightNs(9U, 30U, 0U, 500U);
    constexpr std::uint64_t k093000800 =
        TimeSinceMidnightNs(9U, 30U, 0U, 800U);
    constexpr std::uint64_t k093000999 =
        TimeSinceMidnightNs(9U, 30U, 0U, 999U);
    constexpr std::uint64_t k093001000 =
        TimeSinceMidnightNs(9U, 30U, 1U, 0U);
    constexpr std::uint64_t k145959900 =
        TimeSinceMidnightNs(14U, 59U, 59U, 900U);
    constexpr std::uint64_t k093000Window =
        TimeSinceMidnightNs(9U, 30U, 0U, 0U);
    constexpr std::uint64_t k093001Window =
        TimeSinceMidnightNs(9U, 30U, 1U, 0U);
    constexpr std::uint64_t k145955Window =
        TimeSinceMidnightNs(14U, 59U, 55U, 0U);
    constexpr std::uint64_t k145959Window =
        TimeSinceMidnightNs(14U, 59U, 59U, 0U);

    const auto kline_generation1_watermark =
        MakeWatermark(*registry, 1U, 6U, 1U, 6U);
    ok &= Expect(
        kline_runtime->BeginGeneration(
            kline_generation1_watermark) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "begin KLine generation 1");

    struct TradeInput final {
        std::uint32_t raw_exchange_time;
        std::uint64_t exchange_time_ns_since_midnight;
        std::int64_t price_p6;
        std::int64_t quantity;
        std::int64_t recv_realtime_ns;
    };
    const std::array<TradeInput, 5U> generation1_trades{{
        {93'000'800U,
         k093000800,
         10'000'000,
         2,
         kTradeDateMidnightUnixNs + 15LL * 60LL * 60LL *
             static_cast<std::int64_t>(
                 market::kKLineNanosecondsPerSecondV1)},
        {93'000'500U,
         k093000500,
         12'000'000,
         3,
         kTradeDateMidnightUnixNs + 8LL * 60LL * 60LL *
             static_cast<std::int64_t>(
                 market::kKLineNanosecondsPerSecondV1)},
        {93'000'999U,
         k093000999,
         11'000'000,
         4,
         kTradeDateMidnightUnixNs + 20LL * 60LL * 60LL *
             static_cast<std::int64_t>(
                 market::kKLineNanosecondsPerSecondV1)},
        {93'001'000U,
         k093001000,
         8'000'000,
         5,
         kTradeDateMidnightUnixNs + 1LL * 60LL * 60LL *
             static_cast<std::int64_t>(
                 market::kKLineNanosecondsPerSecondV1)},
        {145'959'900U,
         k145959900,
         13'000'000,
         6,
         kTradeDateMidnightUnixNs + 9LL * 60LL * 60LL *
             static_cast<std::int64_t>(
                 market::kKLineNanosecondsPerSecondV1)},
    }};
    for (std::size_t index = 0U;
         index < generation1_trades.size();
         ++index) {
        const TradeInput& trade = generation1_trades[index];
        const std::uint64_t sequence =
            static_cast<std::uint64_t>(index) + 1U;
        ok &= Expect(
            Submit(
                kline_runtime.get(),
                MakeKLineTradeRecord(
                    *registry,
                    sequence,
                    sequence,
                    1U,
                    trade.raw_exchange_time,
                    trade.exchange_time_ns_since_midnight,
                    trade.price_p6,
                    trade.quantity,
                    trade.recv_realtime_ns,
                    static_cast<std::int64_t>(sequence))) ==
                market::RealtimeHistorySubmitErrorV1::kNone,
            "submit generation-1 KLine trade");
    }

    // This source's next record is admitted after its generation-1 fence.
    // Its earlier exchange timestamp must revise generation 2 only.
    ok &= Expect(
        kline_runtime->SealSource(1U, 1U) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "seal KLine trade source for generation 1");
    ok &= Expect(
        Submit(
            kline_runtime.get(),
            MakeKLineTradeRecord(
                *registry,
                6U,
                6U,
                1U,
                93'000'100U,
                k093000100,
                9'000'000,
                1,
                kTradeDateMidnightUnixNs + 23LL * 60LL * 60LL *
                    static_cast<std::int64_t>(
                        market::kKLineNanosecondsPerSecondV1),
                6)) ==
            market::RealtimeHistorySubmitErrorV1::kNone,
        "submit post-fence late KLine trade");
    constexpr std::array<std::uint8_t, 3U> kIdleSources{
        0U, 2U, 3U};
    for (std::uint8_t source : kIdleSources) {
        ok &= Expect(
            kline_runtime->SealSource(source, 1U) ==
                market::RealtimeHistoryGenerationErrorV1::kNone,
            "seal idle KLine source for generation 1");
    }

    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        kline_store1;
    std::shared_ptr<const market::RealtimeKLineGenerationV1>
        kline_generation1;
    ok &= Expect(
        kline_runtime->WaitForGeneration(
            1U,
            std::chrono::seconds(2),
            &kline_store1,
            &kline_generation1) ==
                market::RealtimeHistoryGenerationErrorV1::kNone &&
            kline_store1 != nullptr &&
            kline_generation1 != nullptr,
        "wait KLine generation 1");

    std::vector<market::KLineBarV1> first_1s;
    std::vector<market::KLineBarV1> first_5s;
    if (kline_generation1 != nullptr) {
        ok &= Expect(
            kline_generation1->coverage_from_open() &&
                kline_generation1->input_store().get() ==
                    kline_store1.get() &&
                kline_generation1->bar_count() == 5U,
            "KLine generation 1 retains the complete from-open store prefix");
        first_1s =
            ReadKLines(*kline_generation1, 1U, 1U, &ok);
        first_5s =
            ReadKLines(*kline_generation1, 1U, 2U, &ok);
        ok &= Expect(
            first_1s.size() == 3U &&
                first_1s[0U].window_start_ns_since_midnight ==
                    k093000Window &&
                first_1s[0U].open_price_p6 == 12'000'000 &&
                first_1s[0U].high_price_p6 == 12'000'000 &&
                first_1s[0U].low_price_p6 == 10'000'000 &&
                first_1s[0U].close_price_p6 == 11'000'000 &&
                first_1s[0U].volume_raw == 9U &&
                first_1s[0U].trade_count == 3U &&
                first_1s[0U].revision == 3U &&
                first_1s[0U].first_trade
                        .event_time_ns_since_midnight ==
                    k093000500 &&
                first_1s[0U].last_trade
                        .event_time_ns_since_midnight ==
                    k093000999 &&
                first_1s[1U].window_start_ns_since_midnight ==
                    k093001Window &&
                first_1s[1U].open_price_p6 == 8'000'000 &&
                first_1s[1U].volume_raw == 5U &&
                first_1s[1U].trade_count == 1U &&
                first_1s[2U].window_start_ns_since_midnight ==
                    k145959Window &&
                first_1s[2U].open_price_p6 == 13'000'000 &&
                first_1s[2U].volume_raw == 6U &&
                first_1s[2U].trade_count == 1U,
            "1-second KLines use exchange time for OHLC and expose the full day");
        ok &= Expect(
            first_5s.size() == 2U &&
                first_5s[0U].window_start_ns_since_midnight ==
                    k093000Window &&
                first_5s[0U].open_price_p6 == 12'000'000 &&
                first_5s[0U].high_price_p6 == 12'000'000 &&
                first_5s[0U].low_price_p6 == 8'000'000 &&
                first_5s[0U].close_price_p6 == 8'000'000 &&
                first_5s[0U].volume_raw == 14U &&
                first_5s[0U].trade_count == 4U &&
                first_5s[0U].revision == 4U &&
                first_5s[1U].window_start_ns_since_midnight ==
                    k145955Window &&
                first_5s[1U].open_price_p6 == 13'000'000 &&
                first_5s[1U].volume_raw == 6U &&
                first_5s[1U].trade_count == 1U,
            "5-second KLines aggregate the same trades in a second window");
    }

    const auto kline_generation2_watermark =
        MakeWatermark(*registry, 2U, 7U, 1U, 7U);
    ok &= Expect(
        kline_runtime->BeginGeneration(
            kline_generation2_watermark) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "begin KLine generation 2");
    for (std::uint8_t source = 0U; source < 4U; ++source) {
        ok &= Expect(
            kline_runtime->SealSource(source, 2U) ==
                market::RealtimeHistoryGenerationErrorV1::kNone,
            "seal source for KLine generation 2");
    }

    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        kline_store2;
    std::shared_ptr<const market::RealtimeKLineGenerationV1>
        kline_generation2;
    ok &= Expect(
        kline_runtime->WaitForGeneration(
            2U,
            std::chrono::seconds(2),
            &kline_store2,
            &kline_generation2) ==
                market::RealtimeHistoryGenerationErrorV1::kNone &&
            kline_store2 != nullptr &&
            kline_generation2 != nullptr,
        "wait KLine generation 2");
    if (kline_generation2 != nullptr) {
        const std::vector<market::KLineBarV1> second_1s =
            ReadKLines(*kline_generation2, 1U, 1U, &ok);
        const std::vector<market::KLineBarV1> second_5s =
            ReadKLines(*kline_generation2, 1U, 2U, &ok);
        ok &= Expect(
            kline_generation2->coverage_from_open() &&
                kline_generation2->input_store().get() ==
                    kline_store2.get() &&
                kline_generation2->bar_count() == 5U &&
                second_1s.size() == 3U &&
                second_1s[0U].open_price_p6 == 9'000'000 &&
                second_1s[0U].high_price_p6 == 12'000'000 &&
                second_1s[0U].low_price_p6 == 9'000'000 &&
                second_1s[0U].close_price_p6 == 11'000'000 &&
                second_1s[0U].volume_raw == 10U &&
                second_1s[0U].trade_count == 4U &&
                second_1s[0U].revision == 4U &&
                second_1s[0U].first_trade
                        .event_time_ns_since_midnight ==
                    k093000100 &&
                second_5s.size() == 2U &&
                second_5s[0U].open_price_p6 == 9'000'000 &&
                second_5s[0U].high_price_p6 == 12'000'000 &&
                second_5s[0U].low_price_p6 == 8'000'000 &&
                second_5s[0U].close_price_p6 == 8'000'000 &&
                second_5s[0U].volume_raw == 15U &&
                second_5s[0U].trade_count == 5U &&
                second_5s[0U].revision == 5U,
            "late exchange-time trade revises both windows in generation 2");
        ok &= Expect(
            kline_runtime->AcquireLatestKLineGeneration().get() ==
                kline_generation2.get(),
            "latest KLine generation is atomically published");
    }

    if (kline_generation1 != nullptr) {
        const std::vector<market::KLineBarV1> old_1s =
            ReadKLines(*kline_generation1, 1U, 1U, &ok);
        const std::vector<market::KLineBarV1> old_5s =
            ReadKLines(*kline_generation1, 1U, 2U, &ok);
        ok &= Expect(
            old_1s.size() == 3U &&
                old_1s[0U].open_price_p6 == 12'000'000 &&
                old_1s[0U].low_price_p6 == 10'000'000 &&
                old_1s[0U].volume_raw == 9U &&
                old_1s[0U].trade_count == 3U &&
                old_1s[0U].revision == 3U &&
                old_5s.size() == 2U &&
                old_5s[0U].open_price_p6 == 12'000'000 &&
                old_5s[0U].volume_raw == 14U &&
                old_5s[0U].trade_count == 4U &&
                old_5s[0U].revision == 4U,
            "older KLine generation remains immutable after late revision");
    }

    kline_runtime->StopAndDrain();
    return ok ? 0 : 1;
}
