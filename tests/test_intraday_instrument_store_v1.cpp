#include "l2flow/market/daily_instrument_catalog_v2.h"
#include "l2flow/market/intraday_instrument_store_v1.h"
#include "l2flow/market/instrument_runtime_state_v2.h"
#include "l2flow/market/realtime_history_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

namespace common = l2flow::common;
namespace market = l2flow::market;

constexpr std::array<std::uint32_t, 4U> kSourceStreamIds{
    11U, 12U, 13U, 14U};
constexpr std::uint32_t kTradeDate = 20260724U;

static_assert(
    static_cast<std::uint8_t>(
        market::RealtimeHistoryCreateErrorV1::kResourceExhausted) == 3U);
static_assert(
    static_cast<std::uint8_t>(
        market::RealtimeHistoryCreateErrorV1::kThreadStartFailed) == 4U);
static_assert(
    static_cast<std::uint8_t>(
        market::RealtimeHistoryGenerationErrorV1::kResourceExhausted) ==
    10U);

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

std::vector<std::byte> Bytes(std::string_view text) {
    const auto bytes = std::as_bytes(std::span(text));
    return {bytes.begin(), bytes.end()};
}

struct DailyRuntimeFixture final {
    std::unique_ptr<market::DailyInstrumentCatalogV2> catalog;
    std::unique_ptr<market::InstrumentRuntimeStateV2> runtime_state;

    [[nodiscard]] explicit operator bool() const noexcept {
        return catalog != nullptr && runtime_state != nullptr;
    }
};

DailyRuntimeFixture MakeDailyRuntimeFixture(
    std::size_t instrument_count = 9U,
    std::uint64_t session_epoch = 17U) {
    DailyRuntimeFixture fixture{};
    if (instrument_count == 0U || instrument_count > 9U ||
        session_epoch == 0U) {
        return fixture;
    }
    constexpr std::array<std::string_view, 9U> security_ids{
        "600001",
        "600002",
        "600003",
        "600004",
        "600005",
        "600006",
        "600007",
        "600008",
        "600009"};
    std::vector<market::DailyInstrumentSourceEntryV2> entries;
    entries.reserve(instrument_count);
    for (std::size_t ordinal = 0U;
         ordinal < instrument_count;
         ++ordinal) {
        market::DailyInstrumentSourceEntryV2 entry{};
        entry.key.market = market::MarketV1::kShanghai;
        entry.key.security_id_source = {};
        entry.key.security_id = Bytes(security_ids[ordinal]);
        entry.metadata.quantity_unit =
            market::QuantityUnitV1::kShare;
        entry.metadata.security_type =
            market::SecurityTypeV1::kEquity;
        entry.metadata.asset_scope =
            market::AssetScopeV1::kDocumentedCore;
        entries.push_back(std::move(entry));
    }
    market::DailyInstrumentCatalogConfigV2 catalog_config{};
    catalog_config.trade_date = kTradeDate;
    catalog_config.catalog_version = session_epoch;
    catalog_config.session_epoch = session_epoch;
    catalog_config.market_scope =
        market::kDailyCatalogMainlandScopeV2;
    catalog_config.coverage_complete = true;
    if (market::DailyInstrumentCatalogV2::Create(
            catalog_config,
            entries,
            &fixture.catalog) !=
            market::DailyInstrumentCatalogCreateErrorV2::kNone ||
        market::InstrumentRuntimeStateV2::Create(
            *fixture.catalog,
            &fixture.runtime_state) !=
            market::InstrumentRuntimeStateErrorV2::kNone) {
        return {};
    }
    return fixture;
}

void FillCommon(
    const market::InstrumentRuntimeStateV2& directory,
    market::DecodedMarketCommonV1* common,
    market::MarketEventKindV1 kind,
    market::MarketV1 venue,
    std::uint8_t source_slot,
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence,
    std::uint32_t instrument_id) {
    common->kind = kind;
    common->market = venue;
    common->origin.source_stream_id = kSourceStreamIds[source_slot];
    common->origin.trade_date = kTradeDate;
    common->origin.source_sequence = source_sequence;
    common->origin.recv_realtime_ns =
        static_cast<std::int64_t>(ingress_sequence * 100U);
    common->origin.recv_monotonic_ns =
        static_cast<std::int64_t>(ingress_sequence * 10U);
    common->instrument_id = instrument_id;
    market::InstrumentRuntimeEntryViewV2 lookup{};
    if (directory.LookupById(instrument_id, &lookup) ==
        market::InstrumentRuntimeStateErrorV2::kNone) {
        common->ordinal = lookup.ordinal;
    }
}

template <typename Event>
std::unique_ptr<market::RealtimeHistoryEventInputV1> OwnInput(
    std::uint8_t source_slot,
    std::uint64_t ingress_sequence,
    Event event) {
    market::DecodedMarketEventV1 decoded(std::move(event));
    auto input = market::RealtimeHistoryEventInputV1::Create(
        source_slot,
        ingress_sequence,
        std::move(decoded),
        source_slot == 1U || source_slot == 3U
            ? ingress_sequence
            : 0U);
    if (!input.has_value()) {
        return nullptr;
    }
    return std::make_unique<market::RealtimeHistoryEventInputV1>(
        std::move(*input));
}

std::unique_ptr<market::RealtimeHistoryEventInputV1> MakeInput(
    const market::InstrumentRuntimeStateV2& registry,
    std::uint8_t source_slot,
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence,
    std::uint32_t instrument_id) {
    const std::int64_t price =
        static_cast<std::int64_t>(ingress_sequence * 1'000'000U);
    switch (source_slot) {
        case 0U: {
            market::ShanghaiSnapshotV1 event{};
            FillCommon(
                registry,
                &event.common,
                market::MarketEventKindV1::kShanghaiSnapshot,
                market::MarketV1::kShanghai,
                source_slot,
                source_sequence,
                ingress_sequence,
                instrument_id);
            event.last_price.valid = true;
            event.last_price.raw = price;
            event.last_price.normalized_p6 = price;
            event.last_price.scale = 6U;
            return OwnInput(
                source_slot, ingress_sequence, std::move(event));
        }
        case 1U: {
            market::ShanghaiTickV1 event{};
            FillCommon(
                registry,
                &event.common,
                market::MarketEventKindV1::kShanghaiTick,
                market::MarketV1::kShanghai,
                source_slot,
                source_sequence,
                ingress_sequence,
                instrument_id);
            event.fields.price.valid = true;
            event.fields.price.raw = price;
            event.fields.price.normalized_p6 = price;
            event.fields.price.scale = 6U;
            return OwnInput(
                source_slot, ingress_sequence, std::move(event));
        }
        case 2U: {
            market::ShenzhenSnapshotV1 event{};
            FillCommon(
                registry,
                &event.common,
                market::MarketEventKindV1::kShenzhenSnapshot,
                market::MarketV1::kShenzhen,
                source_slot,
                source_sequence,
                ingress_sequence,
                instrument_id);
            event.last_price.valid = true;
            event.last_price.raw = price;
            event.last_price.normalized_p6 = price;
            event.last_price.scale = 6U;
            return OwnInput(
                source_slot, ingress_sequence, std::move(event));
        }
        case 3U: {
            market::ShenzhenOrderV1 event{};
            FillCommon(
                registry,
                &event.common,
                market::MarketEventKindV1::kShenzhenOrder,
                market::MarketV1::kShenzhen,
                source_slot,
                source_sequence,
                ingress_sequence,
                instrument_id);
            event.fields.price.valid = true;
            event.fields.price.raw = price;
            event.fields.price.normalized_p6 = price;
            event.fields.price.scale = 6U;
            return OwnInput(
                source_slot, ingress_sequence, std::move(event));
        }
        default:
            return nullptr;
    }
}

market::IntradayInstrumentStoreAppendErrorV1 AppendInput(
    market::IntradayInstrumentStoreV1* store,
    std::uint32_t worker,
    market::RealtimeHistoryEventInputV1* input) {
    if (store == nullptr || input == nullptr || !input->valid()) {
        return market::IntradayInstrumentStoreAppendErrorV1::
            kInvalidRecord;
    }
    market::InstrumentRouteTokenV1 route{};
    if (store->ResolveRouteToken(
            input->ordinal(),
            input->instrument_id(),
            &route) !=
        market::IntradayInstrumentStoreQueryErrorV1::kNone) {
        return market::IntradayInstrumentStoreAppendErrorV1::
            kInvalidRecord;
    }
    return store->Append(worker, route, std::move(*input));
}

market::RealtimeHistoryWatermarkV1 MakeWatermarkForSnapshot(
    std::shared_ptr<
        const market::DailyInstrumentCatalogSnapshotV2>
        catalog_snapshot,
    std::uint64_t generation,
    std::array<std::uint64_t, 4U> source_sequence_exclusive) {
    common::Identity128 run_id{};
    run_id[0U] = std::byte{0x54U};
    run_id[1U] = std::byte{0x31U};

    std::array<market::RealtimeSourceWatermarkV1, 4U> sources{};
    std::uint64_t ingress_sequence_exclusive = 1U;
    for (std::size_t source_slot = 0U;
         source_slot < sources.size();
         ++source_slot) {
        sources[source_slot].source_stream_id =
            kSourceStreamIds[source_slot];
        sources[source_slot].sequence_exclusive =
            source_sequence_exclusive[source_slot];
        ingress_sequence_exclusive +=
            source_sequence_exclusive[source_slot] - 1U;
    }

    market::RealtimeHistoryWatermarkV1 watermark{};
    if (catalog_snapshot == nullptr) {
        return {};
    }
    l2flow::realtime::ProcessingProgressV2 progress{};
    progress.accepted_sequence = ingress_sequence_exclusive - 1U;
    progress.applied_sequence = ingress_sequence_exclusive - 1U;
    if (market::BuildRealtimeHistoryWatermarkV1(
            run_id,
            generation,
            kTradeDate,
            ingress_sequence_exclusive,
            10'000U + generation,
            std::move(catalog_snapshot),
            progress,
            sources,
            &watermark) !=
        market::RealtimeHistoryWatermarkErrorV1::kNone) {
        return {};
    }
    return watermark;
}

market::RealtimeHistoryWatermarkV1 MakeWatermark(
    const market::InstrumentRuntimeStateV2& registry,
    std::uint64_t generation,
    std::array<std::uint64_t, 4U> source_sequence_exclusive) {
    std::shared_ptr<
        const market::DailyInstrumentCatalogSnapshotV2>
        catalog_snapshot;
    if (registry.AcquireSnapshot(&catalog_snapshot) !=
            market::InstrumentRuntimeStateErrorV2::kNone ||
        catalog_snapshot == nullptr) {
        return {};
    }
    return MakeWatermarkForSnapshot(
        std::move(catalog_snapshot),
        generation,
        source_sequence_exclusive);
}

market::IntradayInstrumentStoreConfigV1 StoreConfig(
    std::uint64_t maximum_records = 100U,
    std::uint64_t maximum_bytes = 1U << 30U) {
    market::IntradayInstrumentStoreConfigV1 config{};
    config.segment_target_bytes =
        market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
    config.maximum_session_records = maximum_records;
    config.maximum_session_accounted_bytes = maximum_bytes;
    config.maximum_records_per_batch = 2U;
    config.coverage_from_open = true;
    return config;
}

std::unique_ptr<market::IntradayInstrumentStoreV1> CreateStore(
    const market::InstrumentRuntimeStateV2& registry,
    std::uint32_t worker_count,
    market::IntradayInstrumentStoreConfigV1 config,
    std::string_view message,
    bool* ok) {
    std::unique_ptr<market::IntradayInstrumentStoreV1> store;
    const auto error = market::IntradayInstrumentStoreV1::Create(
        config, worker_count, kSourceStreamIds, &registry, &store);
    *ok &= Expect(
        error == market::IntradayInstrumentStoreCreateErrorV1::kNone &&
            store != nullptr,
        message);
    return store;
}

std::shared_ptr<const market::IntradayInstrumentStoreGenerationV1>
BuildStoreGeneration(
    market::IntradayInstrumentStoreV1* store,
    const market::InstrumentRuntimeStateV2& registry,
    std::uint32_t worker_count,
    std::uint64_t generation,
    std::array<std::uint64_t, 4U> source_sequence_exclusive,
    bool* ok) {
    const market::RealtimeHistoryWatermarkV1 watermark =
        MakeWatermark(registry, generation, source_sequence_exclusive);
    *ok &= Expect(
        watermark.generation == generation,
        "build direct-store watermark");
    if (watermark.generation != generation) {
        return nullptr;
    }

    std::vector<
        std::unique_ptr<market::IntradayInstrumentStoreWorkerSliceV1>>
        slices(worker_count);
    for (std::uint32_t worker = 0U; worker < worker_count; ++worker) {
        const auto error =
            store->CaptureWorker(
                worker,
                generation,
                watermark.catalog_snapshot,
                &slices[worker]);
        *ok &= Expect(
            error ==
                    market::IntradayInstrumentStoreGenerationErrorV1::
                        kNone &&
                slices[worker] != nullptr,
            "capture direct-store worker slice");
        if (error !=
                market::IntradayInstrumentStoreGenerationErrorV1::kNone ||
            slices[worker] == nullptr) {
            return nullptr;
        }
    }

    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        result;
    const std::uint64_t previously_published =
        store->Snapshot().latest_generation;
    const auto error = store->BuildGeneration(
        watermark, std::move(slices), &result);
    *ok &= Expect(
        error ==
                market::IntradayInstrumentStoreGenerationErrorV1::kNone &&
            result != nullptr,
        "build direct-store generation");
    *ok &= Expect(
        store->Snapshot().latest_generation == previously_published,
        "BuildGeneration is publication-free");
    if (error ==
            market::IntradayInstrumentStoreGenerationErrorV1::kNone &&
        result != nullptr) {
        *ok &= Expect(
            store->PublishGeneration(result) ==
                    market::IntradayInstrumentStoreGenerationErrorV1::
                        kNone &&
                store->Snapshot().latest_generation == generation,
            "explicitly publish direct-store generation");
    }
    return result;
}

template <typename Cursor>
std::vector<const market::RealtimeHistoryRecordV1*> DrainCursor(
    Cursor* cursor,
    std::size_t batch_capacity,
    bool* ok) {
    std::vector<const market::RealtimeHistoryRecordV1*> records;
    std::vector<const market::RealtimeHistoryRecordV1*> batch(
        batch_capacity);
    for (std::size_t iteration = 0U; iteration < 100U; ++iteration) {
        std::size_t written = 0U;
        const auto error = cursor->ReadBatch(batch, &written);
        *ok &= Expect(
            error == market::IntradayInstrumentStoreQueryErrorV1::kNone,
            "cursor batch read");
        *ok &= Expect(
            written <= batch.size(), "cursor respects caller batch");
        if (error !=
                market::IntradayInstrumentStoreQueryErrorV1::kNone ||
            written > batch.size()) {
            return records;
        }
        records.insert(
            records.end(), batch.begin(), batch.begin() + written);
        if (written == 0U) {
            *ok &= Expect(cursor->done(), "zero batch is end-of-stream");
            return records;
        }
    }
    *ok &= Expect(false, "cursor terminates in bounded iterations");
    return records;
}

bool CheckFrozenDailyCatalogAndRouting() {
    bool ok = true;
    auto fixture = MakeDailyRuntimeFixture(3U, 71U);
    ok &= Expect(
        static_cast<bool>(fixture) &&
            fixture.catalog->instrument_count() == 3U &&
            fixture.runtime_state->capacity() == 3U,
        "create frozen three-instrument daily catalog and runtime state");
    if (!fixture) {
        return false;
    }
    market::InstrumentRuntimeStateV2& runtime_state =
        *fixture.runtime_state;
    auto store = CreateStore(
        runtime_state,
        3U,
        StoreConfig(),
        "create daily-catalog-backed Store",
        &ok);
    if (store == nullptr) {
        return false;
    }

    market::InstrumentRouteTokenV1 route{};
    ok &= Expect(
        store->ResolveRouteToken(0U, 1U, &route) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            route.ordinal == 0U && route.instrument_id == 1U &&
            route.worker == 0U &&
            store->ResolveRouteToken(1U, 2U, &route) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            route.ordinal == 1U && route.worker == 1U &&
            store->ResolveRouteToken(2U, 3U, &route) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            route.ordinal == 2U && route.worker == 2U,
        "daily catalog IDs resolve by exact dense ordinal and owner");
    ok &= Expect(
        store->ResolveRouteToken(3U, 4U, &route) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNotFound &&
            store->ResolveRouteToken(0U, 2U, &route) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNotFound,
        "route resolution rejects out-of-catalog and mismatched identities");

    std::shared_ptr<
        const market::DailyInstrumentCatalogSnapshotV2>
        first_catalog;
    ok &= Expect(
        runtime_state.AcquireSnapshot(&first_catalog) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            first_catalog != nullptr &&
            first_catalog->capacity() == 3U &&
            first_catalog->bound_count() == 3U &&
            first_catalog->catalog_generation() == 1U &&
            first_catalog->catalog_scope() ==
                market::InstrumentCatalogScopeV2::
                    kDeclaredDailyAShare &&
            first_catalog->coverage_complete() &&
            first_catalog->trade_date() == kTradeDate &&
            first_catalog->catalog_digest() ==
                fixture.catalog->catalog_digest(),
        "snapshot carries the complete frozen daily catalog identity");
    if (first_catalog == nullptr) {
        return false;
    }

    ok &= Expect(
        store->ResolveRouteToken(3U, 4U, &route) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNotFound,
        "frozen runtime exposes no post-connect identity mutation");

    const market::RealtimeHistoryWatermarkV1 first_watermark =
        MakeWatermarkForSnapshot(
            first_catalog, 1U, {1U, 1U, 1U, 1U});
    std::vector<
        std::unique_ptr<market::IntradayInstrumentStoreWorkerSliceV1>>
        first_slices(3U);
    for (std::uint32_t worker = 0U; worker < 3U; ++worker) {
        ok &= Expect(
            store->CaptureWorker(
                worker,
                1U,
                first_catalog,
                &first_slices[worker]) ==
                market::IntradayInstrumentStoreGenerationErrorV1::kNone,
            "capture capacity-backed worker slice");
    }
    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        first_generation;
    ok &= Expect(
        store->BuildGeneration(
            first_watermark,
            std::move(first_slices),
            &first_generation) ==
                market::IntradayInstrumentStoreGenerationErrorV1::kNone &&
            first_generation != nullptr &&
            first_generation->catalog_snapshot() == first_catalog &&
            first_generation->watermark().catalog_snapshot ==
                first_catalog &&
            first_generation->instrument_count() == 3U,
        "generation retains the exact frozen catalog snapshot");
    if (first_generation == nullptr) {
        return false;
    }
    for (std::size_t ordinal = 0U; ordinal < 3U; ++ordinal) {
        market::IntradayInstrumentSummaryV1 summary{};
        ok &= Expect(
            first_generation->SummaryAt(ordinal, &summary) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                summary.instrument_id == ordinal + 1U &&
                summary.record_count == 0U &&
                summary.latest_snapshot == nullptr &&
                summary.latest_tick == nullptr,
            "bound-no-data identities remain present in the cut");
    }
    market::IntradayInstrumentSummaryV1 outside_cut{};
    ok &= Expect(
        first_generation->SummaryAt(3U, &outside_cut) ==
            market::IntradayInstrumentStoreQueryErrorV1::kNotFound,
        "identity outside the declared daily catalog is absent");

    ok &= Expect(
        store->PublishGeneration(first_generation) ==
            market::IntradayInstrumentStoreGenerationErrorV1::kNone,
        "publish first frozen-catalog generation");
    std::shared_ptr<
        const market::DailyInstrumentCatalogSnapshotV2>
        second_catalog;
    ok &= Expect(
        runtime_state.AcquireSnapshot(&second_catalog) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            second_catalog != nullptr &&
            second_catalog->capacity() == 3U &&
            second_catalog->bound_count() == 3U &&
            second_catalog->catalog_generation() == 1U &&
            second_catalog->catalog_digest() ==
                first_catalog->catalog_digest(),
        "successive cuts retain one immutable catalog identity");
    std::vector<
        std::unique_ptr<market::IntradayInstrumentStoreWorkerSliceV1>>
        second_slices(3U);
    for (std::uint32_t worker = 0U; worker < 3U; ++worker) {
        ok &= Expect(
            store->CaptureWorker(
                worker,
                2U,
                second_catalog,
                &second_slices[worker]) ==
                market::IntradayInstrumentStoreGenerationErrorV1::kNone,
            "capture second frozen-catalog worker slice");
    }
    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        second_generation;
    const market::RealtimeHistoryWatermarkV1 second_watermark =
        MakeWatermarkForSnapshot(
            second_catalog, 2U, {1U, 1U, 1U, 1U});
    ok &= Expect(
        store->BuildGeneration(
            second_watermark,
            std::move(second_slices),
            &second_generation) ==
                market::IntradayInstrumentStoreGenerationErrorV1::kNone &&
            second_generation != nullptr &&
            second_generation->catalog_snapshot() == second_catalog &&
            second_generation->instrument_count() == 3U &&
            second_generation->watermark().catalog_snapshot ==
                second_catalog,
        "next generation reuses the complete frozen daily catalog");
    return ok;
}

bool CheckGenerationCaptureUsesFrozenCatalog() {
    bool ok = true;
    constexpr std::size_t kInstrumentCount = 3U;
    constexpr std::uint32_t kWorkerCount = 8U;
    auto fixture =
        MakeDailyRuntimeFixture(kInstrumentCount, 72U);
    ok &= Expect(
        static_cast<bool>(fixture) &&
            fixture.runtime_state->capacity() == kInstrumentCount,
        "create three-instrument frozen daily runtime state");
    if (!fixture) {
        return false;
    }
    market::InstrumentRuntimeStateV2& runtime_state =
        *fixture.runtime_state;
    auto store = CreateStore(
        runtime_state,
        kWorkerCount,
        StoreConfig(),
        "create frozen-catalog Store",
        &ok);
    if (store == nullptr) {
        return false;
    }

    std::shared_ptr<
        const market::DailyInstrumentCatalogSnapshotV2>
        catalog_snapshot;
    ok &= Expect(
        runtime_state.AcquireSnapshot(&catalog_snapshot) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            catalog_snapshot != nullptr &&
            catalog_snapshot->capacity() == kInstrumentCount &&
            catalog_snapshot->bound_count() == kInstrumentCount &&
            catalog_snapshot->catalog_generation() == 1U &&
            catalog_snapshot->coverage_complete(),
        "capture complete three-instrument daily catalog");
    if (catalog_snapshot == nullptr) {
        return false;
    }
    const market::RealtimeHistoryWatermarkV1 watermark =
        MakeWatermarkForSnapshot(
            catalog_snapshot, 1U, {1U, 1U, 1U, 1U});
    ok &= Expect(
        watermark.generation == 1U,
        "build frozen-catalog generation watermark");

    std::vector<
        std::unique_ptr<market::IntradayInstrumentStoreWorkerSliceV1>>
        slices(kWorkerCount);
    std::size_t captured_instrument_count = 0U;
    for (std::uint32_t worker = 0U;
         worker < kWorkerCount;
         ++worker) {
        const auto error = store->CaptureWorker(
            worker,
            watermark.generation,
            catalog_snapshot,
            &slices[worker]);
        ok &= Expect(
            error ==
                    market::IntradayInstrumentStoreGenerationErrorV1::
                        kNone &&
                slices[worker] != nullptr,
            "capture frozen-catalog worker slice");
        if (slices[worker] == nullptr) {
            return false;
        }
        const std::size_t expected =
            worker < kInstrumentCount ? 1U : 0U;
        ok &= Expect(
            slices[worker]->captured_instrument_count() == expected,
            "worker slice token represents its owned catalog rows");
        captured_instrument_count +=
            slices[worker]->captured_instrument_count();
    }
    ok &= Expect(
        captured_instrument_count == kInstrumentCount,
        "worker tokens cover each daily catalog row exactly once");

    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        generation;
    ok &= Expect(
        store->BuildGeneration(
            watermark, std::move(slices), &generation) ==
                market::IntradayInstrumentStoreGenerationErrorV1::kNone &&
            generation != nullptr &&
            generation->instrument_count() == kInstrumentCount &&
            generation->catalog_snapshot() == catalog_snapshot,
        "generation outputs exactly the frozen daily catalog");
    if (generation != nullptr) {
        for (std::size_t ordinal = 0U;
             ordinal < kInstrumentCount;
             ++ordinal) {
            market::IntradayInstrumentSummaryV1 summary{};
            ok &= Expect(
                generation->SummaryAt(ordinal, &summary) ==
                        market::IntradayInstrumentStoreQueryErrorV1::
                            kNone &&
                    summary.instrument_id == ordinal + 1U,
                "generation SummaryAt preserves dense catalog order");
        }
        market::IntradayInstrumentSummaryV1 outside_catalog{};
        ok &= Expect(
            generation->SummaryAt(
                kInstrumentCount, &outside_catalog) ==
                    market::IntradayInstrumentStoreQueryErrorV1::
                        kNotFound &&
                outside_catalog.instrument_id == 0U,
            "generation excludes rows outside the daily catalog");
    }
    return ok;
}

bool CheckLazyPostCutRowFreeze() {
    bool ok = true;
    constexpr std::uint32_t kWorkerCount = 4U;
    auto fixture = MakeDailyRuntimeFixture(1U, 73U);
    ok &= Expect(
        static_cast<bool>(fixture),
        "create one-instrument frozen daily runtime state");
    if (!fixture) {
        return false;
    }
    market::InstrumentRuntimeStateV2& runtime_state =
        *fixture.runtime_state;
    auto store = CreateStore(
        runtime_state,
        kWorkerCount,
        StoreConfig(),
        "create daily-catalog lazy-freeze Store",
        &ok);
    if (store == nullptr) {
        return false;
    }

    auto first = MakeInput(runtime_state, 0U, 1U, 1U, 1U);
    ok &= Expect(
        first != nullptr &&
            AppendInput(store.get(), 0U, first.get()) ==
                market::IntradayInstrumentStoreAppendErrorV1::kNone,
        "append record before lazy generation fence");

    const market::RealtimeHistoryWatermarkV1 first_watermark =
        MakeWatermark(
            runtime_state, 1U, {2U, 1U, 1U, 1U});
    std::vector<
        std::unique_ptr<market::IntradayInstrumentStoreWorkerSliceV1>>
        first_slices(kWorkerCount);
    for (std::uint32_t worker = 0U;
         worker < kWorkerCount;
         ++worker) {
        ok &= Expect(
            store->CaptureWorker(
                worker,
                1U,
                first_watermark.catalog_snapshot,
                &first_slices[worker]) ==
                    market::IntradayInstrumentStoreGenerationErrorV1::
                        kNone &&
                first_slices[worker] != nullptr,
            "publish constant-work generation token");
    }

    // This append deliberately occurs before BuildGeneration. Its row owner
    // must preserve generation 1 lazily while the live latest path advances.
    auto second = MakeInput(runtime_state, 0U, 2U, 2U, 1U);
    ok &= Expect(
        second != nullptr &&
            AppendInput(store.get(), 0U, second.get()) ==
                market::IntradayInstrumentStoreAppendErrorV1::kNone,
        "post-cut append proceeds before background materialization");

    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        generation_one;
    ok &= Expect(
        store->BuildGeneration(
            first_watermark,
            std::move(first_slices),
            &generation_one) ==
                market::IntradayInstrumentStoreGenerationErrorV1::kNone &&
            generation_one != nullptr &&
            generation_one->record_count() == 1U,
        "background builder uses the lazily frozen pre-cut endpoint");
    market::IntradayInstrumentSummaryV1 first_summary{};
    ok &= Expect(
        generation_one != nullptr &&
            generation_one->Find(1U, &first_summary) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            first_summary.record_count == 1U &&
            first_summary.latest_snapshot != nullptr &&
            first_summary.latest_snapshot->ingress_sequence() == 1U,
        "generation one excludes the already-visible post-cut record");

    const auto generation_two = BuildStoreGeneration(
        store.get(),
        runtime_state,
        kWorkerCount,
        2U,
        {3U, 1U, 1U, 1U},
        &ok);
    market::IntradayInstrumentSummaryV1 second_summary{};
    ok &= Expect(
        generation_two != nullptr &&
            generation_two->Find(1U, &second_summary) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            generation_two->record_count() == 2U &&
            second_summary.record_count == 2U &&
            second_summary.latest_snapshot != nullptr &&
            second_summary.latest_snapshot->ingress_sequence() == 2U,
        "next generation includes the post-cut record exactly once");
    return ok;
}

std::vector<std::uint64_t> IngressSequences(
    const std::vector<const market::RealtimeHistoryRecordV1*>& records) {
    std::vector<std::uint64_t> result;
    result.reserve(records.size());
    for (const market::RealtimeHistoryRecordV1* record : records) {
        result.push_back(
            record == nullptr ? 0U : record->ingress_sequence());
    }
    return result;
}

struct CursorFingerprint final {
    std::uint64_t count = 0U;
    std::uint64_t ingress_sum = 0U;
    std::uint64_t ingress_xor = 0U;
    std::array<std::uint64_t, 4U> source_counts{};
    std::array<std::uint64_t, 5U> kind_counts{};
    std::uint32_t last_instrument_id = 0U;
    std::uint64_t last_ingress_sequence = 0U;

    bool operator==(const CursorFingerprint&) const = default;
};

CursorFingerprint Fingerprint(
    const std::vector<const market::RealtimeHistoryRecordV1*>& records) {
    CursorFingerprint result{};
    for (const market::RealtimeHistoryRecordV1* record : records) {
        if (record == nullptr) {
            continue;
        }
        ++result.count;
        result.ingress_sum += record->ingress_sequence();
        result.ingress_xor ^= record->ingress_sequence();
        const std::size_t source =
            static_cast<std::size_t>(record->source_slot());
        if (source < result.source_counts.size()) {
            ++result.source_counts[source];
        }
        const std::uint8_t raw_kind =
            static_cast<std::uint8_t>(record->kind());
        if (raw_kind != 0U &&
            static_cast<std::size_t>(raw_kind) <=
                result.kind_counts.size()) {
            ++result.kind_counts[
                static_cast<std::size_t>(raw_kind - 1U)];
        }
        result.last_instrument_id = record->instrument_id();
        result.last_ingress_sequence =
            record->ingress_sequence();
    }
    return result;
}

struct ConcurrentRangeDrainResult final {
    market::IntradayInstrumentStoreQueryErrorV1 error =
        market::IntradayInstrumentStoreQueryErrorV1::kNone;
    std::vector<const market::RealtimeHistoryRecordV1*> records;
    bool terminal_page = false;
    bool unexpected_failure = false;
};

std::vector<ConcurrentRangeDrainResult> DrainRangesConcurrently(
    const market::IntradayInstrumentStoreGenerationV1& generation,
    std::size_t range_count,
    std::size_t batch_capacity) {
    std::vector<ConcurrentRangeDrainResult> results(range_count);
    if (range_count == 0U || batch_capacity == 0U) {
        return results;
    }
    std::vector<std::array<std::size_t, 2U>> ranges(range_count);
    const std::size_t quotient =
        generation.instrument_count() / range_count;
    const std::size_t remainder =
        generation.instrument_count() % range_count;
    std::size_t next_ordinal = 0U;
    for (std::size_t index = 0U; index < range_count; ++index) {
        const std::size_t width =
            quotient + (index < remainder ? 1U : 0U);
        ranges[index] = {next_ordinal, next_ordinal + width};
        next_ordinal += width;
    }

    std::vector<std::jthread> readers;
    readers.reserve(range_count);
    std::mutex start_mutex;
    std::condition_variable start_cv;
    std::size_t ready_readers = 0U;
    bool start_readers = false;
    try {
        for (std::size_t index = 0U; index < range_count; ++index) {
            readers.emplace_back(
                [&generation,
                 &ranges,
                 &results,
                 &start_mutex,
                 &start_cv,
                 &ready_readers,
                 &start_readers,
                 batch_capacity,
                 index]() noexcept {
                    {
                        std::unique_lock<std::mutex> lock(start_mutex);
                        ++ready_readers;
                        start_cv.notify_all();
                        start_cv.wait(lock, [&start_readers]() {
                            return start_readers;
                        });
                    }
                    ConcurrentRangeDrainResult& result =
                        results[index];
                    try {
                        std::unique_ptr<
                            market::IntradayUniverseCursorV1>
                            cursor;
                        result.error =
                            generation.OpenUniverseRangeCursor(
                                ranges[index][0U],
                                ranges[index][1U],
                                {},
                                &cursor);
                        if (result.error !=
                                market::
                                    IntradayInstrumentStoreQueryErrorV1::
                                        kNone ||
                            cursor == nullptr) {
                            return;
                        }
                        std::vector<
                            const market::RealtimeHistoryRecordV1*>
                            batch(batch_capacity);
                        std::uint64_t remaining_pages =
                            generation.record_count();
                        if (remaining_pages !=
                            std::numeric_limits<std::uint64_t>::max()) {
                            ++remaining_pages;
                        }
                        while (remaining_pages != 0U) {
                            --remaining_pages;
                            std::size_t written =
                                std::numeric_limits<std::size_t>::max();
                            result.error =
                                cursor->ReadBatch(batch, &written);
                            if (result.error !=
                                    market::
                                        IntradayInstrumentStoreQueryErrorV1::
                                            kNone ||
                                written > batch.size()) {
                                return;
                            }
                            result.records.insert(
                                result.records.end(),
                                batch.begin(),
                                batch.begin() + written);
                            if (written == 0U) {
                                result.terminal_page = cursor->done();
                                return;
                            }
                        }
                        result.unexpected_failure = true;
                    } catch (...) {
                        result.unexpected_failure = true;
                    }
                });
        }
    } catch (...) {
        {
            std::lock_guard<std::mutex> lock(start_mutex);
            start_readers = true;
        }
        start_cv.notify_all();
        throw;
    }
    {
        std::unique_lock<std::mutex> lock(start_mutex);
        start_cv.wait(lock, [&ready_readers, range_count]() {
            return ready_readers == range_count;
        });
        start_readers = true;
    }
    start_cv.notify_all();
    readers.clear();
    return results;
}

std::vector<std::uint32_t> InstrumentIds(
    const std::vector<const market::RealtimeHistoryRecordV1*>& records) {
    std::vector<std::uint32_t> result;
    result.reserve(records.size());
    for (const market::RealtimeHistoryRecordV1* record : records) {
        result.push_back(
            record == nullptr ? 0U : record->instrument_id());
    }
    return result;
}

bool CheckCreationAndInvalidConfiguration(
    const market::InstrumentRuntimeStateV2& registry) {
    bool ok = true;
    const auto valid = StoreConfig();
    ok &= Expect(
        market::IntradayInstrumentStoreV1::Create(
            valid, 2U, kSourceStreamIds, &registry, nullptr) ==
            market::IntradayInstrumentStoreCreateErrorV1::kNullOutput,
        "store rejects null output");

    std::unique_ptr<market::IntradayInstrumentStoreV1> store;
    auto invalid = valid;
    ok &= Expect(
        market::IntradayInstrumentStoreV1::Create(
            valid, 0U, kSourceStreamIds, &registry, &store) ==
            market::IntradayInstrumentStoreCreateErrorV1::
                kInvalidConfiguration,
        "store rejects zero workers");
    ok &= Expect(
        market::IntradayInstrumentStoreV1::Create(
            valid, 2U, kSourceStreamIds, nullptr, &store) ==
            market::IntradayInstrumentStoreCreateErrorV1::
                kInvalidConfiguration,
        "store rejects null directory");
    auto invalid_source_ids = kSourceStreamIds;
    invalid_source_ids[2U] = 0U;
    ok &= Expect(
        market::IntradayInstrumentStoreV1::Create(
            valid, 2U, invalid_source_ids, &registry, &store) ==
            market::IntradayInstrumentStoreCreateErrorV1::
                kInvalidConfiguration,
        "store rejects a zero fixed source stream identity");
    invalid_source_ids = kSourceStreamIds;
    invalid_source_ids[3U] = invalid_source_ids[2U];
    ok &= Expect(
        market::IntradayInstrumentStoreV1::Create(
            valid, 2U, invalid_source_ids, &registry, &store) ==
            market::IntradayInstrumentStoreCreateErrorV1::
                kInvalidConfiguration,
        "store rejects duplicate fixed source stream identities");

    invalid = valid;
    invalid.segment_target_bytes =
        market::kIntradayInstrumentStoreMinimumSegmentBytesV1 - 1U;
    ok &= Expect(
        market::IntradayInstrumentStoreV1::Create(
            invalid, 2U, kSourceStreamIds, &registry, &store) ==
            market::IntradayInstrumentStoreCreateErrorV1::
                kInvalidConfiguration,
        "store rejects undersized arena segment target");
    invalid = valid;
    invalid.segment_target_bytes =
        market::kIntradayInstrumentStoreMaximumSegmentBytesV1 + 1U;
    ok &= Expect(
        market::IntradayInstrumentStoreV1::Create(
            invalid, 2U, kSourceStreamIds, &registry, &store) ==
            market::IntradayInstrumentStoreCreateErrorV1::
                kInvalidConfiguration,
        "store rejects oversized arena segment target");
    invalid = valid;
    invalid.maximum_session_records = 0U;
    ok &= Expect(
        market::IntradayInstrumentStoreV1::Create(
            invalid, 2U, kSourceStreamIds, &registry, &store) ==
            market::IntradayInstrumentStoreCreateErrorV1::
                kInvalidConfiguration,
        "store requires record hard cap");
    invalid = valid;
    invalid.maximum_session_accounted_bytes = 0U;
    ok &= Expect(
        market::IntradayInstrumentStoreV1::Create(
            invalid, 2U, kSourceStreamIds, &registry, &store) ==
            market::IntradayInstrumentStoreCreateErrorV1::
                kInvalidConfiguration,
        "store requires byte hard cap");
    invalid = valid;
    invalid.maximum_records_per_batch = 0U;
    ok &= Expect(
        market::IntradayInstrumentStoreV1::Create(
            invalid, 2U, kSourceStreamIds, &registry, &store) ==
            market::IntradayInstrumentStoreCreateErrorV1::
                kInvalidConfiguration,
        "store rejects zero batch boundary");
    invalid = valid;
    invalid.maximum_records_per_batch =
        market::kIntradayInstrumentStoreMaximumBatchRecordsV1 + 1U;
    ok &= Expect(
        market::IntradayInstrumentStoreV1::Create(
            invalid, 2U, kSourceStreamIds, &registry, &store) ==
            market::IntradayInstrumentStoreCreateErrorV1::
                kInvalidConfiguration,
        "store rejects unbounded per-call query work");

    store = CreateStore(
        registry, 2U, valid, "valid direct store creation", &ok);
    if (store != nullptr) {
        const auto snapshot = store->Snapshot();
        ok &= Expect(
            snapshot.maximum_session_records ==
                    valid.maximum_session_records &&
                snapshot.maximum_session_accounted_bytes ==
                    valid.maximum_session_accounted_bytes &&
                snapshot.coverage_from_open &&
                !snapshot.coverage_lost &&
                snapshot.appended_records == 0U,
            "new store exposes configured healthy coverage");
        ok &= Expect(
            store->WorkerForInstrument(2U) == 1U &&
                store->WorkerForInstrument(5U) == 0U,
            "store uses permanent ordinal-modulo ownership");
    }
    return ok;
}

bool CheckStoreSessionProvenance(
    const market::InstrumentRuntimeStateV2& registry) {
    bool ok = true;
    auto first_store = CreateStore(
        registry,
        2U,
        StoreConfig(),
        "first provenance-test store creation",
        &ok);
    auto second_store = CreateStore(
        registry,
        2U,
        StoreConfig(),
        "second provenance-test store creation",
        &ok);
    if (first_store == nullptr || second_store == nullptr) {
        return false;
    }

    const auto first_generation = BuildStoreGeneration(
        first_store.get(), registry, 2U, 1U, {1U, 1U, 1U, 1U}, &ok);
    const auto second_generation = BuildStoreGeneration(
        second_store.get(), registry, 2U, 1U, {1U, 1U, 1U, 1U}, &ok);
    if (first_generation == nullptr || second_generation == nullptr) {
        return false;
    }
    ok &= Expect(
        first_generation->store_session_epoch() != 0U &&
            second_generation->store_session_epoch() != 0U,
        "successful Store sessions expose nonzero provenance");
    ok &= Expect(
        first_generation->store_session_epoch() !=
            second_generation->store_session_epoch(),
        "distinct Store sessions have process-unique provenance");
    return ok;
}

bool CheckGenerationQueriesAndLifetime(
    const market::InstrumentRuntimeStateV2& registry) {
    bool ok = true;
    auto store = CreateStore(
        registry,
        2U,
        StoreConfig(),
        "query-test store creation",
        &ok);
    if (store == nullptr) {
        return false;
    }

    const auto ingress1 = MakeInput(registry, 1U, 1U, 1U, 5U);
    const auto ingress2 = MakeInput(registry, 0U, 1U, 2U, 5U);
    const auto ingress3 = MakeInput(registry, 2U, 1U, 3U, 5U);
    const auto ingress4 = MakeInput(registry, 3U, 1U, 4U, 5U);
    const auto ingress5 = MakeInput(registry, 1U, 2U, 5U, 5U);
    const auto ingress6 = MakeInput(registry, 1U, 3U, 6U, 5U);
    const auto ingress7 = MakeInput(registry, 0U, 2U, 7U, 2U);
    const auto ingress8 = MakeInput(registry, 3U, 2U, 8U, 9U);
    ok &= Expect(
        ingress1 != nullptr && ingress2 != nullptr &&
            ingress3 != nullptr && ingress4 != nullptr &&
            ingress5 != nullptr && ingress6 != nullptr &&
            ingress7 != nullptr && ingress8 != nullptr,
        "construct direct-store records");
    if (!ok) {
        return false;
    }

    const auto append = [&ok, &store](
                            const std::unique_ptr<
                                market::RealtimeHistoryEventInputV1>&
                                input,
                            std::string_view message) {
        const std::uint32_t worker =
            store->WorkerForInstrument(input->instrument_id());
        ok &= Expect(
            AppendInput(store.get(), worker, input.get()) ==
                market::IntradayInstrumentStoreAppendErrorV1::kNone,
            message);
    };

    // Source-local order is increasing, while the four sources deliberately
    // reach the owner in a different order from global process ingress.
    append(ingress4, "append source 3 before earlier global records");
    append(ingress3, "append source 2 before earlier global records");
    append(ingress1, "append source 1 first record");
    append(ingress2, "append source 0 first record");
    append(ingress5, "append source 1 second record");
    append(ingress6, "append source 1 third record");
    append(ingress7, "append second instrument");
    append(ingress8, "append third instrument");
    if (!ok) {
        return false;
    }

    const auto first = BuildStoreGeneration(
        store.get(), registry, 2U, 1U, {3U, 4U, 2U, 3U}, &ok);
    if (first == nullptr) {
        return false;
    }
    ok &= Expect(
            first->watermark().generation == 1U &&
            first->watermark().ingress_sequence_exclusive == 9U &&
            first->instrument_count() == registry.capacity() &&
            first->catalog_snapshot() ==
                first->watermark().catalog_snapshot &&
            first->record_count() == 8U &&
            first->coverage_from_open() &&
            first->store_session_epoch() != 0U,
        "generation carries exact fixed universe and global cut");

    const std::array<std::uint32_t, 9U> expected_instrument_ids{
        1U, 2U, 3U, 4U, 5U, 6U, 7U, 8U, 9U};
    for (std::size_t ordinal = 0U;
         ordinal < expected_instrument_ids.size();
         ++ordinal) {
        market::IntradayInstrumentSummaryV1 ordinal_summary{};
        ok &= Expect(
            first->SummaryAt(ordinal, &ordinal_summary) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                ordinal_summary.instrument_id ==
                    expected_instrument_ids[ordinal],
            "SummaryAt exposes the complete dense catalog in ID order");
    }
    market::IntradayInstrumentSummaryV1 invalid_ordinal_summary{};
    invalid_ordinal_summary.instrument_id = 123U;
    ok &= Expect(
        first->SummaryAt(
            expected_instrument_ids.size(), &invalid_ordinal_summary) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNotFound &&
            invalid_ordinal_summary.instrument_id == 0U,
        "SummaryAt rejects and clears an out-of-range ordinal");
    ok &= Expect(
        first->SummaryAt(0U, nullptr) ==
            market::IntradayInstrumentStoreQueryErrorV1::kNullOutput,
        "SummaryAt rejects null output");

    market::IntradayInstrumentSummaryV1 summary{};
    ok &= Expect(
        first->Find(5U, &summary) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            summary.instrument_id == 5U &&
            summary.record_count == 6U &&
            summary.source_record_counts ==
                std::array<std::uint64_t, 4U>{1U, 3U, 1U, 1U} &&
            summary.latest_snapshot != nullptr &&
            summary.latest_snapshot->ingress_sequence() == 3U &&
            summary.latest_tick != nullptr &&
            summary.latest_tick->ingress_sequence() == 6U,
        "Find exposes per-source counts and latest snapshot/tick");
    if (summary.latest_snapshot != nullptr &&
        summary.latest_tick != nullptr) {
        const auto snapshot_event = summary.latest_snapshot->event();
        const auto tick_event = summary.latest_tick->event();
        const auto* snapshot =
            market::StoredMarketEventGetV1<
                market::ShenzhenSnapshotV1>(snapshot_event);
        const auto* tick =
            market::StoredMarketEventGetV1<
                market::ShanghaiTickV1>(tick_event);
        ok &= Expect(
            snapshot != nullptr && tick != nullptr &&
                snapshot->last_price.normalized_p6 == 3'000'000 &&
                tick->fields.price.normalized_p6 == 6'000'000,
            "arena headers resolve exact typed payloads without copied variants");
    }

    market::IntradayInstrumentSummaryV1 empty_summary{};
    ok &= Expect(
        first->Find(7U, &empty_summary) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            empty_summary.instrument_id == 7U &&
            empty_summary.record_count == 0U &&
            empty_summary.latest_snapshot == nullptr &&
            empty_summary.latest_tick == nullptr,
        "Find retains a no-data daily-catalog instrument");
    ok &= Expect(
        first->Find(99U, &empty_summary) ==
            market::IntradayInstrumentStoreQueryErrorV1::kNotFound,
        "Find rejects instrument outside the bound catalog");

    market::IntradayInstrumentScanOptionsV1
        empty_instrument_options{};
    empty_instrument_options.ingress_sequence_begin_inclusive =
        first->watermark().ingress_sequence_exclusive;
    empty_instrument_options.ingress_sequence_end_exclusive =
        first->watermark().ingress_sequence_exclusive;
    std::unique_ptr<market::IntradayInstrumentCursorV1>
        empty_range_cursor;
    ok &= Expect(
        first->OpenInstrumentCursor(
            7U,
            empty_instrument_options,
            &empty_range_cursor) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            empty_range_cursor != nullptr &&
            empty_range_cursor->done(),
        "equal half-open bounds open an empty instrument cursor");
    if (empty_range_cursor != nullptr) {
        std::array<const market::RealtimeHistoryRecordV1*, 1U> batch{};
        std::size_t written = std::numeric_limits<std::size_t>::max();
        ok &= Expect(
            empty_range_cursor->ReadBatch(batch, &written) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                written == 0U,
            "empty instrument cursor returns an explicit terminal batch");
    }

    std::unique_ptr<market::IntradayInstrumentCursorV1> all_cursor;
    ok &= Expect(
        first->OpenInstrumentCursor(5U, {}, &all_cursor) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            all_cursor != nullptr,
        "open full instrument cursor");
    if (all_cursor != nullptr) {
        const auto records = DrainCursor(all_cursor.get(), 2U, &ok);
        ok &= Expect(
            IngressSequences(records) ==
                std::vector<std::uint64_t>{1U, 2U, 3U, 4U, 5U, 6U},
            "four source lanes merge by global ingress sequence");
    }

    std::unique_ptr<market::IntradayInstrumentTickDeltaCursorV1>
        tick_delta;
    ok &= Expect(
        first->OpenInstrumentTickDeltaCursor(
            5U, 2U, &tick_delta) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            tick_delta != nullptr,
        "open tick-only instrument delta");
    if (tick_delta != nullptr) {
        const auto& delta_summary = tick_delta->summary();
        ok &= Expect(
            delta_summary.instrument_id == 5U &&
                delta_summary.selected_source_mask ==
                    std::array<std::uint8_t, 4U>{0U, 1U, 0U, 1U} &&
                delta_summary.ingress_sequence_begin_inclusive == 2U &&
                delta_summary.ingress_sequence_end_exclusive == 9U &&
                delta_summary.base_tick_source_record_counts ==
                    std::array<std::uint64_t, 4U>{0U, 1U, 0U, 0U} &&
                delta_summary.target_tick_source_record_counts ==
                    std::array<std::uint64_t, 4U>{0U, 3U, 0U, 1U} &&
                delta_summary.delta_tick_source_record_counts ==
                    std::array<std::uint64_t, 4U>{0U, 2U, 0U, 1U} &&
                delta_summary.delta_tick_record_count == 3U,
            "tick delta summary exposes exact selected/base/target/delta counts");
        const auto records = DrainCursor(tick_delta.get(), 2U, &ok);
        ok &= Expect(
            IngressSequences(records) ==
                    std::vector<std::uint64_t>{4U, 5U, 6U} &&
                std::all_of(
                    records.begin(),
                    records.end(),
                    [](const market::RealtimeHistoryRecordV1* record) {
                        return record != nullptr &&
                               (record->source_slot() == 1U ||
                                record->source_slot() == 3U) &&
                               market::IsTickEventKindV1(record->kind());
                    }),
            "tick delta excludes both snapshot lanes and merges tick lanes by ingress");

        market::IntradayInstrumentScanOptionsV1 oracle_options{};
        oracle_options.ingress_sequence_begin_inclusive = 2U;
        oracle_options.ingress_sequence_end_exclusive =
            first->watermark().ingress_sequence_exclusive;
        std::unique_ptr<market::IntradayInstrumentCursorV1>
            oracle_cursor;
        ok &= Expect(
            first->OpenInstrumentCursor(
                5U, oracle_options, &oracle_cursor) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                oracle_cursor != nullptr,
            "open legacy range cursor as tick-delta oracle");
        if (oracle_cursor != nullptr) {
            const auto mixed =
                DrainCursor(oracle_cursor.get(), 2U, &ok);
            std::vector<const market::RealtimeHistoryRecordV1*>
                oracle;
            for (const market::RealtimeHistoryRecordV1* record : mixed) {
                if (record != nullptr &&
                    (record->source_slot() == 1U ||
                     record->source_slot() == 3U)) {
                    oracle.push_back(record);
                }
            }
            ok &= Expect(
                records == oracle,
                "tail-located tick delta exactly matches legacy full-range oracle");
        }
    }

    std::unique_ptr<market::IntradayInstrumentTickDeltaCursorV1>
        bootstrap_delta;
    ok &= Expect(
        first->OpenInstrumentTickDeltaCursor(
            5U, 1U, &bootstrap_delta) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            bootstrap_delta != nullptr,
        "open begin-one tick bootstrap");
    if (bootstrap_delta != nullptr) {
        const auto records =
            DrainCursor(bootstrap_delta.get(), 2U, &ok);
        ok &= Expect(
            IngressSequences(records) ==
                    std::vector<std::uint64_t>{1U, 4U, 5U, 6U} &&
                bootstrap_delta->summary()
                        .base_tick_source_record_counts ==
                    std::array<std::uint64_t, 4U>{},
            "begin-one bootstrap emits every retained tick and has zero base counts");
    }

    std::unique_ptr<market::IntradayInstrumentTickDeltaCursorV1>
        empty_tick_delta;
    ok &= Expect(
        first->OpenInstrumentTickDeltaCursor(
            5U,
            first->watermark().ingress_sequence_exclusive,
            &empty_tick_delta) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            empty_tick_delta != nullptr && empty_tick_delta->done() &&
            empty_tick_delta->summary().delta_tick_record_count == 0U &&
            empty_tick_delta->summary()
                    .base_tick_source_record_counts ==
                empty_tick_delta->summary()
                    .target_tick_source_record_counts,
        "begin equal to generation end is a valid empty tick delta");
    if (empty_tick_delta != nullptr) {
        std::array<const market::RealtimeHistoryRecordV1*, 1U> batch{};
        std::size_t written = std::numeric_limits<std::size_t>::max();
        ok &= Expect(
            empty_tick_delta->ReadBatch(batch, &written) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                written == 0U,
            "empty tick delta returns an explicit zero-sized terminal batch");
    }

    ok &= Expect(
        first->OpenInstrumentTickDeltaCursor(
            5U, 0U, &empty_tick_delta) ==
                market::IntradayInstrumentStoreQueryErrorV1::
                    kInvalidArgument &&
            empty_tick_delta == nullptr,
        "tick delta rejects begin zero");
    ok &= Expect(
        first->OpenInstrumentTickDeltaCursor(
            5U,
            first->watermark().ingress_sequence_exclusive + 1U,
            &empty_tick_delta) ==
                market::IntradayInstrumentStoreQueryErrorV1::
                    kInvalidArgument &&
            empty_tick_delta == nullptr,
        "tick delta rejects begin beyond immutable generation end");
    ok &= Expect(
        first->OpenInstrumentTickDeltaCursor(
            99U, 1U, &empty_tick_delta) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNotFound &&
            empty_tick_delta == nullptr,
        "tick delta rejects an instrument outside the bound catalog");
    ok &= Expect(
        first->OpenInstrumentTickDeltaCursor(5U, 1U, nullptr) ==
            market::IntradayInstrumentStoreQueryErrorV1::kNullOutput,
        "tick delta rejects a null cursor output");

    market::IntradayInstrumentScanOptionsV1 range{};
    range.ingress_sequence_begin_inclusive = 2U;
    range.ingress_sequence_end_exclusive = 6U;
    std::unique_ptr<market::IntradayInstrumentCursorV1> range_cursor;
    ok &= Expect(
        first->OpenInstrumentCursor(5U, range, &range_cursor) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            range_cursor != nullptr,
        "open half-open range cursor");
    if (range_cursor != nullptr) {
        std::array<const market::RealtimeHistoryRecordV1*, 2U> batch{};
        std::size_t written = 0U;
        ok &= Expect(
            range_cursor->ReadBatch(batch, &written) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                written == 2U &&
                batch[0U]->ingress_sequence() == 2U &&
                batch[1U]->ingress_sequence() == 3U,
            "range cursor first page");
        ok &= Expect(
            range_cursor->ReadBatch(batch, &written) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                written == 2U &&
                batch[0U]->ingress_sequence() == 4U &&
                batch[1U]->ingress_sequence() == 5U,
            "range cursor second page");
        ok &= Expect(
            range_cursor->ReadBatch(batch, &written) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                written == 0U && range_cursor->done(),
            "range cursor reports zero-sized terminal page");
    }

    market::IntradayInstrumentScanOptionsV1 reverse_range = range;
    reverse_range.direction =
        market::IntradayInstrumentScanDirectionV1::kNewestFirst;
    std::unique_ptr<market::IntradayInstrumentCursorV1>
        reverse_range_cursor;
    ok &= Expect(
        first->OpenInstrumentCursor(
            5U, reverse_range, &reverse_range_cursor) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            reverse_range_cursor != nullptr,
        "open reverse half-open range cursor");
    if (reverse_range_cursor != nullptr) {
        const auto records =
            DrainCursor(reverse_range_cursor.get(), 2U, &ok);
        ok &= Expect(
            IngressSequences(records) ==
                std::vector<std::uint64_t>{5U, 4U, 3U, 2U},
            "reverse range preserves half-open bounds across chunks");
    }

    std::unique_ptr<market::IntradayInstrumentCursorV1> limited_cursor;
    ok &= Expect(
        first->OpenInstrumentCursor(5U, {}, &limited_cursor) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            limited_cursor != nullptr,
        "open cursor for batch-limit check");
    if (limited_cursor != nullptr) {
        std::array<const market::RealtimeHistoryRecordV1*, 3U> too_large{};
        std::size_t written = std::numeric_limits<std::size_t>::max();
        ok &= Expect(
            limited_cursor->ReadBatch(too_large, &written) ==
                    market::IntradayInstrumentStoreQueryErrorV1::
                        kBatchLimitExceeded &&
                written == 0U,
            "ReadBatch enforces configured allocation boundary");
    }

    std::unique_ptr<market::IntradayInstrumentCursorV1> tail_cursor;
    ok &= Expect(
        first->OpenTailCursor(5U, 3U, &tail_cursor) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            tail_cursor != nullptr,
        "open newest-first tail cursor");
    if (tail_cursor != nullptr) {
        const auto records = DrainCursor(tail_cursor.get(), 2U, &ok);
        ok &= Expect(
            IngressSequences(records) ==
                std::vector<std::uint64_t>{6U, 5U, 4U},
            "tail emits exactly newest N records newest-first");
    }

    std::vector<const market::RealtimeHistoryRecordV1*>
        full_universe_records;
    std::unique_ptr<market::IntradayUniverseCursorV1> universe_cursor;
    ok &= Expect(
        first->OpenUniverseCursor({}, &universe_cursor) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            universe_cursor != nullptr,
        "open universe cursor");
    if (universe_cursor != nullptr) {
        full_universe_records =
            DrainCursor(universe_cursor.get(), 2U, &ok);
        ok &= Expect(
            InstrumentIds(full_universe_records) ==
                std::vector<std::uint32_t>{
                    2U, 5U, 5U, 5U, 5U, 5U, 5U, 9U},
            "universe cursor is instrument-id ordered");
        ok &= Expect(
            IngressSequences(full_universe_records) ==
                std::vector<std::uint64_t>{
                    7U, 1U, 2U, 3U, 4U, 5U, 6U, 8U},
            "universe cursor is ingress ordered within instrument");
    }

    std::vector<const market::RealtimeHistoryRecordV1*>
        ranged_universe_records;
    const std::array<std::array<std::size_t, 2U>, 3U> ranges{{
        {0U, 3U},
        {3U, 6U},
        {6U, 9U},
    }};
    for (const auto& range_ordinals : ranges) {
        std::unique_ptr<market::IntradayUniverseCursorV1> range_universe;
        ok &= Expect(
            first->OpenUniverseRangeCursor(
                range_ordinals[0U],
                range_ordinals[1U],
                {},
                &range_universe) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                range_universe != nullptr,
            "open independent universe ordinal-range cursor");
        if (range_universe != nullptr) {
            const auto range_records =
                DrainCursor(range_universe.get(), 2U, &ok);
            ranged_universe_records.insert(
                ranged_universe_records.end(),
                range_records.begin(),
                range_records.end());
        }
    }
    ok &= Expect(
        ranged_universe_records == full_universe_records &&
            ranged_universe_records.size() ==
                static_cast<std::size_t>(first->record_count()),
        "concatenated disjoint ranges exactly equal the full universe cursor");

    for (const std::size_t concurrent_ranges : {4U, 8U}) {
        const auto concurrent =
            DrainRangesConcurrently(*first, concurrent_ranges, 1U);
        std::vector<const market::RealtimeHistoryRecordV1*>
            concurrent_records;
        bool concurrent_ok =
            concurrent.size() == concurrent_ranges;
        for (const ConcurrentRangeDrainResult& result : concurrent) {
            concurrent_ok =
                concurrent_ok &&
                result.error ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                result.terminal_page &&
                !result.unexpected_failure;
            concurrent_records.insert(
                concurrent_records.end(),
                result.records.begin(),
                result.records.end());
        }
        ok &= Expect(
            concurrent_ok &&
                concurrent_records == full_universe_records,
            concurrent_ranges == 4U
                ? "four concurrent ordinal ranges equal the full cursor"
                : "eight concurrent ordinal ranges, including empty "
                  "ranges, equal the full cursor");
        ok &= Expect(
            Fingerprint(concurrent_records) ==
                Fingerprint(full_universe_records),
            concurrent_ranges == 4U
                ? "four-range count/source/kind/sum/xor/last fingerprint"
                : "eight-range count/source/kind/sum/xor/last fingerprint");
    }

    market::IntradayInstrumentScanOptionsV1 limited_options{};
    limited_options.maximum_records = 1U;
    std::unique_ptr<market::IntradayUniverseCursorV1> limited_full;
    std::unique_ptr<market::IntradayUniverseCursorV1> limited_first_range;
    std::unique_ptr<market::IntradayUniverseCursorV1> limited_second_range;
    ok &= Expect(
        first->OpenUniverseCursor(limited_options, &limited_full) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            first->OpenUniverseRangeCursor(
                0U, 2U, limited_options, &limited_first_range) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            first->OpenUniverseRangeCursor(
                2U,
                first->instrument_count(),
                limited_options,
                &limited_second_range) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone,
        "open full and ranged cursors with finite record budgets");
    if (limited_full != nullptr && limited_first_range != nullptr &&
        limited_second_range != nullptr) {
        const auto full_limited_records =
            DrainCursor(limited_full.get(), 2U, &ok);
        const auto first_limited_records =
            DrainCursor(limited_first_range.get(), 2U, &ok);
        const auto second_limited_records =
            DrainCursor(limited_second_range.get(), 2U, &ok);
        ok &= Expect(
            full_limited_records.size() == 1U &&
                first_limited_records.size() == 1U &&
                second_limited_records.size() == 1U &&
                first_limited_records[0U]->instrument_id() == 2U &&
                second_limited_records[0U]->instrument_id() == 5U,
            "maximum_records is an independent total budget for each range cursor");
    }

    std::unique_ptr<market::IntradayUniverseCursorV1> empty_range;
    ok &= Expect(
        first->OpenUniverseRangeCursor(2U, 2U, {}, &empty_range) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            empty_range != nullptr && empty_range->done(),
        "empty ordinal range returns an immediately done cursor");
    if (empty_range != nullptr) {
        std::array<const market::RealtimeHistoryRecordV1*, 1U> batch{};
        std::size_t written = std::numeric_limits<std::size_t>::max();
        ok &= Expect(
            empty_range->ReadBatch(batch, &written) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                written == 0U && empty_range->done(),
            "empty ordinal-range cursor has a terminal zero-sized page");
    }

    std::unique_ptr<market::IntradayUniverseCursorV1> invalid_range;
    ok &= Expect(
        first->OpenUniverseRangeCursor(3U, 2U, {}, &invalid_range) ==
                market::IntradayInstrumentStoreQueryErrorV1::
                    kInvalidArgument &&
            invalid_range == nullptr,
        "universe range rejects a reversed half-open interval");
    ok &= Expect(
        first->OpenUniverseRangeCursor(
            0U, first->instrument_count() + 1U, {}, &invalid_range) ==
                market::IntradayInstrumentStoreQueryErrorV1::
                    kInvalidArgument &&
            invalid_range == nullptr,
        "universe range rejects an ordinal beyond the fixed universe");
    market::IntradayInstrumentScanOptionsV1 newest_first{};
    newest_first.direction =
        market::IntradayInstrumentScanDirectionV1::kNewestFirst;
    ok &= Expect(
        first->OpenUniverseRangeCursor(
            0U, first->instrument_count(), newest_first, &invalid_range) ==
                market::IntradayInstrumentStoreQueryErrorV1::
                    kInvalidArgument &&
            invalid_range == nullptr,
        "universe range preserves oldest-first ordering contract");
    ok &= Expect(
        first->OpenUniverseRangeCursor(0U, 1U, {}, nullptr) ==
            market::IntradayInstrumentStoreQueryErrorV1::kNullOutput,
        "universe range rejects null cursor output");

    const auto ingress9 = MakeInput(registry, 0U, 3U, 9U, 5U);
    ok &= Expect(
        ingress9 != nullptr &&
            AppendInput(store.get(), 0U, ingress9.get()) ==
                market::IntradayInstrumentStoreAppendErrorV1::kNone,
        "append record after generation-1 cut");
    const auto second = BuildStoreGeneration(
        store.get(), registry, 2U, 2U, {4U, 4U, 2U, 3U}, &ok);
    if (second == nullptr) {
        return false;
    }
    ok &= Expect(
        second->store_session_epoch() ==
            first->store_session_epoch(),
        "successive generations preserve Store-session provenance");

    market::IntradayInstrumentSummaryV1 first_after_append{};
    market::IntradayInstrumentSummaryV1 second_summary{};
    ok &= Expect(
        first->Find(5U, &first_after_append) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            first_after_append.record_count == 6U &&
            first_after_append.latest_snapshot != nullptr &&
            first_after_append.latest_snapshot->ingress_sequence() == 3U,
        "post-cut append cannot mutate older generation");
    ok &= Expect(
        second->Find(5U, &second_summary) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            second->record_count() == 9U &&
            second_summary.record_count == 7U &&
            second_summary.latest_snapshot != nullptr &&
            second_summary.latest_snapshot->ingress_sequence() == 9U,
        "next generation includes post-cut append");

    std::unique_ptr<market::IntradayInstrumentTickDeltaCursorV1>
        snapshot_only_delta;
    ok &= Expect(
        second->OpenInstrumentTickDeltaCursor(
            5U, 9U, &snapshot_only_delta) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            snapshot_only_delta != nullptr &&
            snapshot_only_delta->done() &&
            snapshot_only_delta->summary().delta_tick_record_count ==
                0U &&
            snapshot_only_delta->summary()
                    .base_tick_source_record_counts ==
                snapshot_only_delta->summary()
                    .target_tick_source_record_counts,
        "snapshot-only suffix produces an empty tick delta");

    std::unique_ptr<market::IntradayInstrumentCursorV1>
        cursor_surviving_store;
    ok &= Expect(
        first->OpenInstrumentCursor(
            5U, {}, &cursor_surviving_store) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            cursor_surviving_store != nullptr,
        "open cursor before destroying its mutable store");
    store.reset();
    if (cursor_surviving_store != nullptr) {
        const auto records =
            DrainCursor(cursor_surviving_store.get(), 2U, &ok);
        ok &= Expect(
            IngressSequences(records) ==
                std::vector<std::uint64_t>{
                    1U, 2U, 3U, 4U, 5U, 6U},
            "cursor keeps generation and session arena alive after store destruction");
    }
    market::IntradayInstrumentSummaryV1 after_store_destroy{};
    ok &= Expect(
        first->Find(5U, &after_store_destroy) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            after_store_destroy.record_count == 6U,
        "generation remains readable after store destruction");
    std::unique_ptr<market::IntradayInstrumentCursorV1>
        after_destroy_cursor;
    ok &= Expect(
        second->OpenTailCursor(5U, 1U, &after_destroy_cursor) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            after_destroy_cursor != nullptr,
        "generation can open cursor after store destruction");
    if (after_destroy_cursor != nullptr) {
        const auto records =
            DrainCursor(after_destroy_cursor.get(), 1U, &ok);
        ok &= Expect(
            IngressSequences(records) == std::vector<std::uint64_t>{9U},
            "generation owns records independently of mutable store");
    }
    return ok;
}

bool CheckRolloverCapsAndWorkerOwnership(
    const market::InstrumentRuntimeStateV2& registry) {
    bool ok = true;
    constexpr std::uint64_t kMaximumFixtureRecords = 64U;

    auto rollover = CreateStore(
        registry,
        1U,
        StoreConfig(kMaximumFixtureRecords),
        "rollover store creation",
        &ok);
    if (rollover == nullptr) {
        return false;
    }
    std::uint64_t first_segment_record_count = 0U;
    for (std::uint64_t sequence = 1U;
         sequence <= kMaximumFixtureRecords;
         ++sequence) {
        const auto input =
            MakeInput(registry, 1U, sequence, sequence, 5U);
        ok &= Expect(
            input != nullptr &&
                AppendInput(rollover.get(), 0U, input.get()) ==
                    market::IntradayInstrumentStoreAppendErrorV1::kNone,
            "append same-lane arena record");
        if (!ok) {
            return false;
        }
        const auto snapshot = rollover->Snapshot();
        if (snapshot.allocated_segments == 1U) {
            first_segment_record_count = snapshot.appended_records;
        } else if (snapshot.allocated_segments >= 2U) {
            break;
        }
    }
    ok &= Expect(
        first_segment_record_count > 0U &&
            first_segment_record_count < kMaximumFixtureRecords &&
            rollover->Snapshot().allocated_segments >= 2U,
        "4 KiB lane arena rolls into a second segment at a measured boundary");
    const std::uint64_t rollover_record_count =
        rollover->Snapshot().appended_records;
    const auto rollover_generation = BuildStoreGeneration(
        rollover.get(),
        registry,
        1U,
        1U,
        {1U, rollover_record_count + 1U, 1U, 1U},
        &ok);
    if (rollover_generation != nullptr) {
        std::unique_ptr<market::IntradayInstrumentTickDeltaCursorV1>
            cross_segment_delta;
        ok &= Expect(
            rollover_generation->OpenInstrumentTickDeltaCursor(
                5U,
                first_segment_record_count,
                &cross_segment_delta) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                cross_segment_delta != nullptr &&
                cross_segment_delta->summary()
                        .delta_tick_record_count == 2U &&
                cross_segment_delta->summary()
                        .base_tick_source_record_counts[1U] ==
                    first_segment_record_count - 1U &&
                cross_segment_delta->summary()
                        .target_tick_source_record_counts[1U] ==
                    rollover_record_count,
            "tick delta locates a boundary in the prior segment from the target tail");
        if (cross_segment_delta != nullptr) {
            const auto records =
                DrainCursor(cross_segment_delta.get(), 1U, &ok);
            ok &= Expect(
                IngressSequences(records) ==
                    std::vector<std::uint64_t>{
                        first_segment_record_count,
                        rollover_record_count},
                "tick delta traverses forward across the arena segment boundary");
        }
    }

    auto wrong_worker = CreateStore(
        registry,
        2U,
        StoreConfig(),
        "wrong-worker store creation",
        &ok);
    const auto wrong_worker_record = MakeInput(registry, 0U, 1U, 1U, 5U);
    if (wrong_worker != nullptr && wrong_worker_record != nullptr) {
        ok &= Expect(
            AppendInput(
                wrong_worker.get(), 1U, wrong_worker_record.get()) ==
                market::IntradayInstrumentStoreAppendErrorV1::kWrongWorker,
            "append rejects non-owner worker");
    } else {
        ok &= Expect(false, "construct wrong-worker fixture");
    }

    auto foreign_route_store = CreateStore(
        registry,
        2U,
        StoreConfig(),
        "foreign-route store creation",
        &ok);
    auto foreign_route_input =
        MakeInput(registry, 0U, 1U, 1U, 5U);
    market::InstrumentRouteTokenV1 foreign_route{};
    if (wrong_worker != nullptr && foreign_route_store != nullptr &&
        foreign_route_input != nullptr) {
        ok &= Expect(
            wrong_worker->ResolveRouteToken(
                foreign_route_input->ordinal(),
                foreign_route_input->instrument_id(),
                &foreign_route) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone,
            "resolve route in the original store session");
        ok &= Expect(
            foreign_route_store->Append(
                1U,
                foreign_route,
                std::move(*foreign_route_input)) ==
                market::IntradayInstrumentStoreAppendErrorV1::
                    kInvalidRecord,
            "route token cannot cross store session identity");
    } else {
        ok &= Expect(false, "construct foreign-route fixture");
    }

    auto record_capped = CreateStore(
        registry,
        1U,
        StoreConfig(1U),
        "record-cap store creation",
        &ok);
    const auto record_cap_first = MakeInput(registry, 0U, 1U, 1U, 5U);
    const auto record_cap_second = MakeInput(registry, 0U, 2U, 2U, 5U);
    if (record_capped != nullptr && record_cap_first != nullptr &&
        record_cap_second != nullptr) {
        ok &= Expect(
            AppendInput(
                record_capped.get(), 0U, record_cap_first.get()) ==
                market::IntradayInstrumentStoreAppendErrorV1::kNone,
            "record-cap store accepts record at limit");
        ok &= Expect(
            AppendInput(
                record_capped.get(), 0U, record_cap_second.get()) ==
                market::IntradayInstrumentStoreAppendErrorV1::
                    kRecordCapacity,
            "record hard cap fails closed without eviction");
        const auto snapshot = record_capped->Snapshot();
        ok &= Expect(
            snapshot.appended_records == 1U &&
                snapshot.failed_appends == 1U,
            "record-cap failure leaves accepted prefix intact");
    } else {
        ok &= Expect(false, "construct record-cap fixture");
    }

    auto byte_sizing = CreateStore(
        registry,
        1U,
        StoreConfig(),
        "byte-cap sizing store creation",
        &ok);
    std::uint64_t one_record_total_bytes = 0U;
    std::uint64_t one_record_bytes = 0U;
    auto byte_sizing_input = MakeInput(registry, 1U, 1U, 1U, 5U);
    if (byte_sizing != nullptr && byte_sizing_input != nullptr) {
        one_record_bytes =
            byte_sizing_input->accounted_record_bytes();
        ok &= Expect(
            AppendInput(
                byte_sizing.get(), 0U, byte_sizing_input.get()) ==
                market::IntradayInstrumentStoreAppendErrorV1::kNone,
            "sizing store appends one record");
        const auto sizing_snapshot = byte_sizing->Snapshot();
        if (sizing_snapshot.accounted_record_bytes <=
            std::numeric_limits<std::uint64_t>::max() -
                sizing_snapshot.allocated_index_bytes) {
            one_record_total_bytes =
                sizing_snapshot.accounted_record_bytes +
                sizing_snapshot.allocated_index_bytes;
        }
        ok &= Expect(
            one_record_bytes > 0U && one_record_total_bytes != 0U &&
                sizing_snapshot.allocated_segments == 1U,
            "total byte budget includes the exact record and first arena segment");
    }
    auto byte_capped = CreateStore(
        registry,
        1U,
        StoreConfig(10U, one_record_total_bytes),
        "byte-cap store creation",
        &ok);
    auto byte_cap_first = MakeInput(registry, 1U, 1U, 1U, 5U);
    auto byte_cap_second = MakeInput(registry, 1U, 2U, 2U, 5U);
    if (byte_capped != nullptr && byte_cap_first != nullptr &&
        byte_cap_second != nullptr) {
        ok &= Expect(
            AppendInput(
                byte_capped.get(), 0U, byte_cap_first.get()) ==
                market::IntradayInstrumentStoreAppendErrorV1::kNone,
            "byte-cap store accepts exactly one accounted record");
        ok &= Expect(
            byte_capped->Snapshot().accounted_record_bytes ==
                one_record_bytes,
            "snapshot uses conservative record accounting");
        const auto one_record_snapshot = byte_capped->Snapshot();
        ok &= Expect(
            one_record_snapshot.accounted_record_bytes <=
                    one_record_total_bytes -
                        one_record_snapshot.allocated_index_bytes &&
                one_record_snapshot.accounted_record_bytes +
                        one_record_snapshot.allocated_index_bytes ==
                    one_record_total_bytes,
            "record and index accounting share the exact total byte cap");
        ok &= Expect(
            AppendInput(
                byte_capped.get(), 0U, byte_cap_second.get()) ==
                market::IntradayInstrumentStoreAppendErrorV1::
                    kByteCapacity,
            "byte hard cap fails closed without eviction");
    } else {
        ok &= Expect(false, "construct byte-cap fixture");
    }

    auto rollover_sizing = CreateStore(
        registry,
        1U,
        StoreConfig(kMaximumFixtureRecords),
        "segment-cap sizing store creation",
        &ok);
    std::uint64_t full_segment_total_bytes = 0U;
    if (rollover_sizing != nullptr) {
        for (std::uint64_t sequence = 1U;
             sequence <= first_segment_record_count;
             ++sequence) {
            auto input =
                MakeInput(registry, 1U, sequence, sequence, 5U);
            ok &= Expect(
                input != nullptr &&
                    AppendInput(
                        rollover_sizing.get(), 0U, input.get()) ==
                        market::IntradayInstrumentStoreAppendErrorV1::
                            kNone,
                "reproduce measured first-segment occupancy");
        }
        const auto sizing_snapshot = rollover_sizing->Snapshot();
        if (sizing_snapshot.accounted_record_bytes <=
            std::numeric_limits<std::uint64_t>::max() -
                sizing_snapshot.allocated_index_bytes) {
            full_segment_total_bytes =
                sizing_snapshot.accounted_record_bytes +
                sizing_snapshot.allocated_index_bytes;
        }
        ok &= Expect(
            sizing_snapshot.allocated_segments == 1U &&
                sizing_snapshot.appended_records ==
                    first_segment_record_count,
            "measured first-segment fixture remains in one segment");
    }
    auto rollover_capped = CreateStore(
        registry,
        1U,
        StoreConfig(
            kMaximumFixtureRecords, full_segment_total_bytes),
        "segment-rollover-cap store creation",
        &ok);
    if (rollover_capped != nullptr) {
        for (std::uint64_t sequence = 1U;
             sequence <= first_segment_record_count;
             ++sequence) {
            auto input =
                MakeInput(registry, 1U, sequence, sequence, 5U);
            ok &= Expect(
                input != nullptr &&
                    AppendInput(
                        rollover_capped.get(), 0U, input.get()) ==
                        market::IntradayInstrumentStoreAppendErrorV1::
                            kNone,
                "fill byte-capped arena segment exactly");
        }
        auto rollover_input = MakeInput(
            registry,
            1U,
            first_segment_record_count + 1U,
            first_segment_record_count + 1U,
            5U);
        ok &= Expect(
            rollover_input != nullptr &&
                AppendInput(
                    rollover_capped.get(),
                    0U,
                    rollover_input.get()) ==
                    market::IntradayInstrumentStoreAppendErrorV1::
                        kByteCapacity,
            "next append reserves a complete arena segment before allocation");
        const auto rollover_snapshot = rollover_capped->Snapshot();
        ok &= Expect(
            rollover_snapshot.appended_records ==
                    first_segment_record_count &&
                rollover_snapshot.allocated_segments == 1U &&
                rollover_snapshot.accounted_record_bytes +
                        rollover_snapshot.allocated_index_bytes ==
                    full_segment_total_bytes,
            "failed segment rollover leaves the prior arena and budget intact");
    } else {
        ok &= Expect(false, "construct segment-rollover-cap fixture");
    }

    // The first worker receives a quota block larger than the tiny cap. The
    // second worker must reclaim the unused credits, so a two-record hard
    // limit remains exactly usable instead of becoming worker-local waste.
    auto cross_worker_record_cap = CreateStore(
        registry,
        2U,
        StoreConfig(2U),
        "cross-worker record-credit store creation",
        &ok);
    auto worker0_first = MakeInput(registry, 0U, 1U, 1U, 2U);
    auto worker1_first = MakeInput(registry, 1U, 1U, 2U, 5U);
    auto worker0_over_cap = MakeInput(registry, 0U, 2U, 3U, 2U);
    if (cross_worker_record_cap != nullptr &&
        worker0_first != nullptr && worker1_first != nullptr &&
        worker0_over_cap != nullptr) {
        ok &= Expect(
            AppendInput(
                cross_worker_record_cap.get(),
                1U,
                worker0_first.get()) ==
                    market::IntradayInstrumentStoreAppendErrorV1::kNone &&
                AppendInput(
                    cross_worker_record_cap.get(),
                    0U,
                    worker1_first.get()) ==
                    market::IntradayInstrumentStoreAppendErrorV1::kNone,
            "second owner reclaims record credits and uses the exact small cap");
        ok &= Expect(
            AppendInput(
                cross_worker_record_cap.get(),
                1U,
                worker0_over_cap.get()) ==
                    market::IntradayInstrumentStoreAppendErrorV1::
                        kRecordCapacity,
            "cross-worker record quota never exceeds its hard cap");
        const auto snapshot = cross_worker_record_cap->Snapshot();
        ok &= Expect(
            snapshot.appended_records == 2U &&
                snapshot.failed_appends == 1U,
            "record credit reclaim preserves exact accepted accounting");
    } else {
        ok &= Expect(false, "construct cross-worker record-credit fixture");
    }

    // Repeat the exact-budget experiment for byte credits. Both workers need
    // their own first segment, making cross-worker reclaim observable.
    auto cross_worker_byte_sizing = CreateStore(
        registry,
        2U,
        StoreConfig(10U),
        "cross-worker byte-credit sizing store creation",
        &ok);
    std::uint64_t two_worker_total_bytes = 0U;
    if (cross_worker_byte_sizing != nullptr) {
        auto worker0 = MakeInput(registry, 0U, 1U, 1U, 2U);
        auto worker1 = MakeInput(registry, 1U, 1U, 2U, 5U);
        ok &= Expect(
            worker0 != nullptr && worker1 != nullptr &&
                AppendInput(
                    cross_worker_byte_sizing.get(),
                    1U,
                    worker0.get()) ==
                    market::IntradayInstrumentStoreAppendErrorV1::kNone &&
                AppendInput(
                    cross_worker_byte_sizing.get(),
                    0U,
                    worker1.get()) ==
                    market::IntradayInstrumentStoreAppendErrorV1::kNone,
            "measure two-worker byte budget");
        const auto snapshot = cross_worker_byte_sizing->Snapshot();
        if (snapshot.accounted_record_bytes <=
            std::numeric_limits<std::uint64_t>::max() -
                snapshot.allocated_index_bytes) {
            two_worker_total_bytes =
                snapshot.accounted_record_bytes +
                snapshot.allocated_index_bytes;
        }
        ok &= Expect(
            snapshot.allocated_segments == 2U &&
                two_worker_total_bytes != 0U,
            "two-worker sizing owns one arena segment per worker");
    }
    auto cross_worker_byte_cap = CreateStore(
        registry,
        2U,
        StoreConfig(10U, two_worker_total_bytes),
        "cross-worker byte-credit store creation",
        &ok);
    if (cross_worker_byte_cap != nullptr) {
        auto worker0 = MakeInput(registry, 0U, 1U, 1U, 2U);
        auto worker1 = MakeInput(registry, 1U, 1U, 2U, 5U);
        auto over_cap = MakeInput(registry, 0U, 2U, 3U, 2U);
        ok &= Expect(
            worker0 != nullptr && worker1 != nullptr &&
                over_cap != nullptr &&
                AppendInput(
                    cross_worker_byte_cap.get(),
                    1U,
                    worker0.get()) ==
                    market::IntradayInstrumentStoreAppendErrorV1::kNone &&
                AppendInput(
                    cross_worker_byte_cap.get(),
                    0U,
                    worker1.get()) ==
                    market::IntradayInstrumentStoreAppendErrorV1::kNone,
            "second owner reclaims byte credits and uses the exact small cap");
        ok &= Expect(
            AppendInput(
                cross_worker_byte_cap.get(),
                1U,
                over_cap.get()) ==
                market::IntradayInstrumentStoreAppendErrorV1::
                    kByteCapacity,
            "cross-worker byte quota never exceeds its hard cap");
        const auto snapshot = cross_worker_byte_cap->Snapshot();
        ok &= Expect(
            snapshot.appended_records == 2U &&
                snapshot.accounted_record_bytes +
                        snapshot.allocated_index_bytes ==
                    two_worker_total_bytes,
            "byte credit reclaim consumes the exact configured budget");
    }
    return ok;
}

bool CheckLiveTailGenerationIsolation(
    const market::InstrumentRuntimeStateV2& registry) {
    bool ok = true;
    constexpr std::uint64_t final_sequence = 5'000U;
    std::uint64_t full_tail_records = 0U;
    auto boundary_probe = CreateStore(
        registry,
        1U,
        StoreConfig(128U),
        "live-tail boundary probe store creation",
        &ok);
    if (boundary_probe == nullptr) {
        return false;
    }
    for (std::uint64_t sequence = 1U; sequence <= 128U; ++sequence) {
        auto input = MakeInput(registry, 1U, sequence, sequence, 5U);
        ok &= Expect(
            input != nullptr &&
                AppendInput(
                    boundary_probe.get(), 0U, input.get()) ==
                    market::IntradayInstrumentStoreAppendErrorV1::kNone,
            "probe exact full-tail arena boundary");
        if (!ok) {
            return false;
        }
        if (boundary_probe->Snapshot().allocated_segments == 2U) {
            full_tail_records = sequence - 1U;
            break;
        }
    }
    ok &= Expect(
        full_tail_records > 1U,
        "minimum segment holds a nontrivial partial and full tick tail");
    if (!ok) {
        return false;
    }
    boundary_probe.reset();

    const auto run_case =
        [&registry, &ok](
            std::uint64_t initial_records,
            std::string_view creation_message,
            std::string_view isolation_message) {
            auto config = StoreConfig(10'000U);
            config.segment_target_bytes =
                market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
            auto store = CreateStore(
                registry,
                1U,
                config,
                creation_message,
                &ok);
            if (store == nullptr) {
                return;
            }
            for (std::uint64_t sequence = 1U;
                 sequence <= initial_records;
                 ++sequence) {
                const auto record =
                    MakeInput(registry, 1U, sequence, sequence, 5U);
                ok &= Expect(
                    record != nullptr &&
                        AppendInput(
                            store.get(), 0U, record.get()) ==
                            market::
                                IntradayInstrumentStoreAppendErrorV1::
                                    kNone,
                    "append live-tail initial record");
            }
            const auto old_generation = BuildStoreGeneration(
                store.get(),
                registry,
                1U,
                1U,
                {1U, initial_records + 1U, 1U, 1U},
                &ok);
            if (old_generation == nullptr) {
                return;
            }

            std::atomic<bool> go{false};
            std::atomic<bool> writer_done{false};
            std::atomic<bool> writer_failed{false};
            std::thread writer([&store,
                                &registry,
                                &go,
                                &writer_done,
                                &writer_failed,
                                initial_records] {
                while (!go.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                for (std::uint64_t sequence = initial_records + 1U;
                     sequence <= final_sequence;
                     ++sequence) {
                    const auto record =
                        MakeInput(registry, 1U, sequence, sequence, 5U);
                    if (record == nullptr ||
                        AppendInput(
                            store.get(), 0U, record.get()) !=
                            market::
                                IntradayInstrumentStoreAppendErrorV1::
                                    kNone) {
                        writer_failed.store(
                            true, std::memory_order_release);
                        break;
                    }
                }
                writer_done.store(true, std::memory_order_release);
            });

            go.store(true, std::memory_order_release);
            std::size_t completed_reads = 0U;
            do {
                std::unique_ptr<market::IntradayInstrumentCursorV1>
                    cursor;
                ok &= Expect(
                    old_generation->OpenInstrumentCursor(
                        5U, {}, &cursor) ==
                            market::
                                IntradayInstrumentStoreQueryErrorV1::
                                    kNone &&
                        cursor != nullptr,
                    "open cursor while live tail grows");
                if (cursor != nullptr) {
                    const auto records =
                        DrainCursor(cursor.get(), 2U, &ok);
                    ok &= Expect(
                        records.size() ==
                            static_cast<std::size_t>(
                                initial_records),
                        isolation_message);
                    for (std::size_t index = 0U;
                         index < records.size();
                         ++index) {
                        ok &= Expect(
                            records[index] != nullptr &&
                                records[index]->
                                        ingress_sequence() ==
                                    static_cast<std::uint64_t>(
                                        index + 1U),
                            isolation_message);
                    }
                }
                ++completed_reads;
            } while (
                !writer_done.load(std::memory_order_acquire) ||
                completed_reads < 16U);
            writer.join();
            const auto snapshot = store->Snapshot();
            ok &= Expect(
                !writer_failed.load(std::memory_order_acquire) &&
                    snapshot.appended_records == final_sequence &&
                    !snapshot.coverage_lost,
                "post-cut writer completes while old generation is read");
        };

    run_case(
        1U,
        "partial-tail isolation store creation",
        "partial captured tail never exposes post-cut slot writes");
    run_case(
        full_tail_records,
        "full-tail isolation store creation",
        "full captured tail never follows post-cut owned_next");
    return ok;
}

bool CheckRuntimeStoreGenerationPublication(
    market::InstrumentRuntimeStateV2& registry) {
    bool ok = true;
    market::RealtimeHistoryRuntimeConfigV1 config{};
    config.source_stream_ids = kSourceStreamIds;
    config.worker_count = 2U;
    config.queue_capacity_per_source_worker = 32U;
    config.intraday_store = StoreConfig(32U, 1U << 20U);
    config.runtime_state = &registry;

    std::unique_ptr<market::RealtimeHistoryRuntimeV1> runtime;
    ok &= Expect(
        market::RealtimeHistoryRuntimeV1::Create(config, &runtime) ==
                market::RealtimeHistoryCreateErrorV1::kNone &&
            runtime != nullptr,
        "store-backed history runtime creation");
    if (runtime == nullptr) {
        return false;
    }

    const auto watermark =
        MakeWatermark(registry, 1U, {2U, 2U, 2U, 2U});
    ok &= Expect(
        runtime->BeginGeneration(watermark) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "begin store generation");

    std::array<
        std::unique_ptr<market::RealtimeHistoryEventInputV1>,
        4U> records{
        MakeInput(registry, 0U, 1U, 1U, 5U),
        MakeInput(registry, 1U, 1U, 2U, 5U),
        MakeInput(registry, 2U, 1U, 3U, 5U),
        MakeInput(registry, 3U, 1U, 4U, 5U),
    };
    for (std::size_t index : {3U, 2U, 0U, 1U}) {
        ok &= Expect(
            records[index] != nullptr &&
                runtime->TrySubmit(std::move(*records[index])) ==
                    market::RealtimeHistorySubmitErrorV1::kNone,
            "submit cross-source store record");
    }
    for (std::uint8_t source_slot = 0U; source_slot < 4U;
         ++source_slot) {
        ok &= Expect(
            runtime->SealSource(source_slot, 1U) ==
                market::RealtimeHistoryGenerationErrorV1::kNone,
            "seal store source fence");
    }

    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1> generation;
    ok &= Expect(
        runtime->WaitForGeneration(
            1U, std::chrono::seconds(2), &generation) ==
                market::RealtimeHistoryGenerationErrorV1::kNone &&
            generation != nullptr,
        "wait directly published store generation");
    if (generation != nullptr) {
        const auto latest = runtime->AcquireLatestGeneration();
        ok &= Expect(
            latest.get() == generation.get(),
            "runtime atomically publishes the exact store generation handle");
        ok &= Expect(
            generation->watermark().generation == 1U &&
                generation->watermark().ingress_sequence_exclusive == 5U &&
                generation->record_count() == 4U &&
                generation->instrument_count() ==
                    registry.capacity() &&
                generation->coverage_from_open(),
            "store generation carries the exact joint fence identity");

        const std::array<std::uint32_t, 9U> expected_ids{
            1U, 2U, 3U, 4U, 5U, 6U, 7U, 8U, 9U};
        for (std::size_t ordinal = 0U; ordinal < expected_ids.size();
             ++ordinal) {
            market::IntradayInstrumentSummaryV1 ordinal_summary{};
            ok &= Expect(
                generation->SummaryAt(ordinal, &ordinal_summary) ==
                        market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                    ordinal_summary.instrument_id == expected_ids[ordinal],
                "runtime generation preserves the fixed sorted universe");
        }

        market::IntradayInstrumentSummaryV1 summary{};
        ok &= Expect(
            generation->Find(5U, &summary) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                summary.record_count == 4U &&
                summary.latest_snapshot != nullptr &&
                summary.latest_snapshot->ingress_sequence() == 3U &&
                summary.latest_tick != nullptr &&
                summary.latest_tick->ingress_sequence() == 4U,
            "runtime generation exposes latest records from all sources");

        std::unique_ptr<market::IntradayInstrumentCursorV1> tail;
        ok &= Expect(
            generation->OpenTailCursor(5U, 4U, &tail) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone &&
                tail != nullptr,
            "open runtime generation tail");
        if (tail != nullptr) {
            const auto tail_records = DrainCursor(tail.get(), 2U, &ok);
            ok &= Expect(
                IngressSequences(tail_records) ==
                    std::vector<std::uint64_t>{4U, 3U, 2U, 1U},
                "runtime generation tail merges four sources by ingress");
        }
    }
    const auto snapshot = runtime->StoreSnapshot();
    ok &= Expect(
        snapshot.appended_records == 4U &&
            snapshot.latest_generation == 1U &&
            snapshot.coverage_from_open && !snapshot.coverage_lost,
        "runtime reports healthy mandatory store publication");

    runtime->StopAndDrain();
    return ok;
}

bool CheckRuntimeStoreFailureFailClosed(
    market::InstrumentRuntimeStateV2& registry) {
    bool ok = true;
    market::RealtimeHistoryRuntimeConfigV1 config{};
    config.source_stream_ids = kSourceStreamIds;
    config.worker_count = 1U;
    config.queue_capacity_per_source_worker = 16U;
    config.intraday_store = StoreConfig(1U, 1U << 20U);
    config.runtime_state = &registry;
    const market::RealtimeHistoryWatermarkV1 watermark =
        MakeWatermark(registry, 1U, {3U, 1U, 1U, 1U});

    std::unique_ptr<market::RealtimeHistoryRuntimeV1> runtime;
    ok &= Expect(
        market::RealtimeHistoryRuntimeV1::Create(
            config, &runtime) ==
                market::RealtimeHistoryCreateErrorV1::kNone &&
            runtime != nullptr,
        "capacity-failure runtime creation");
    if (runtime != nullptr) {
        ok &= Expect(
            runtime->BeginGeneration(watermark) ==
                market::RealtimeHistoryGenerationErrorV1::kNone,
            "begin capacity-failure generation");
        const auto first = MakeInput(registry, 0U, 1U, 1U, 5U);
        const auto second = MakeInput(registry, 0U, 2U, 2U, 5U);
        ok &= Expect(
            first != nullptr && second != nullptr &&
                runtime->TrySubmit(std::move(*first)) ==
                    market::RealtimeHistorySubmitErrorV1::kNone &&
                runtime->TrySubmit(std::move(*second)) ==
                    market::RealtimeHistorySubmitErrorV1::kNone,
            "submit records beyond the sole store cap");
        for (std::uint8_t source = 0U; source < 4U; ++source) {
            const auto seal = runtime->SealSource(source, 1U);
            ok &= Expect(
                seal ==
                        market::RealtimeHistoryGenerationErrorV1::
                            kNone ||
                    seal ==
                        market::RealtimeHistoryGenerationErrorV1::
                            kFatal ||
                    seal ==
                        market::RealtimeHistoryGenerationErrorV1::
                            kStoreFailed,
                "source fence races only with the fail-closed transition");
        }
        std::shared_ptr<
            const market::IntradayInstrumentStoreGenerationV1> generation;
        ok &= Expect(
            runtime->WaitForGeneration(
                1U, std::chrono::seconds(2), &generation) ==
                    market::RealtimeHistoryGenerationErrorV1::
                        kStoreFailed &&
                generation == nullptr && runtime->fatal() &&
                runtime->AcquireLatestGeneration() == nullptr,
            "store append failure prevents every generation publication");
        const auto snapshot = runtime->StoreSnapshot();
        ok &= Expect(
            snapshot.appended_records == 1U &&
                snapshot.failed_appends == 1U &&
                snapshot.coverage_lost &&
                !snapshot.coverage_from_open,
            "fail-closed snapshot reports retained prefix and lost coverage");
        runtime->StopAndDrain();
    }
    return ok;
}

}  // namespace

int main() {
    DailyRuntimeFixture fixture = MakeDailyRuntimeFixture();
    if (!Expect(
            static_cast<bool>(fixture),
            "daily catalog and runtime-state creation")) {
        return 1;
    }
    market::InstrumentRuntimeStateV2& registry =
        *fixture.runtime_state;

    bool ok = true;
    ok &= CheckFrozenDailyCatalogAndRouting();
    ok &= CheckGenerationCaptureUsesFrozenCatalog();
    ok &= CheckLazyPostCutRowFreeze();
    ok &= CheckCreationAndInvalidConfiguration(registry);
    ok &= CheckStoreSessionProvenance(registry);
    ok &= CheckGenerationQueriesAndLifetime(registry);
    ok &= CheckRolloverCapsAndWorkerOwnership(registry);
    ok &= CheckLiveTailGenerationIsolation(registry);
    ok &= CheckRuntimeStoreGenerationPublication(registry);
    ok &= CheckRuntimeStoreFailureFailClosed(registry);
    return ok ? 0 : 1;
}
