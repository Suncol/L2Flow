#include "l2flow/market/intraday_instrument_store_v1.h"
#include "l2flow/market/daily_instrument_catalog_v2.h"
#include "l2flow/market/instrument_runtime_state_v2.h"
#include "l2flow/market/market_types_v1.h"
#include "l2flow/market/realtime_history_v1.h"
#include "l2flow/market/realtime_latest_read_model_v1.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <span>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

namespace market = l2flow::market;

constexpr std::uint32_t kTradeDate = 20260727U;
constexpr std::array<std::uint32_t, 4U> kSourceStreamIds{
    7101U, 7102U, 7103U, 7104U};
constexpr std::size_t kCatalogCapacity = 5U;
constexpr std::uint64_t kCatalogSessionEpoch = 27U;
constexpr std::uint32_t kShanghaiInstrumentId = 1U;
constexpr std::uint32_t kCatalogOnlyInstrumentId = 2U;
constexpr std::uint32_t kNoDataInstrumentId = 3U;
constexpr std::uint32_t kShenzhenInstrumentId = 4U;
constexpr std::uint32_t kOutOfCapacityInstrumentId = 6U;

bool Expect(bool condition, std::string_view detail) {
    if (!condition) {
        std::cerr << "FAIL: " << detail << '\n';
    }
    return condition;
}

std::vector<std::byte> Bytes(std::string_view value) {
    const auto* const begin =
        reinterpret_cast<const std::byte*>(value.data());
    return std::vector<std::byte>(begin, begin + value.size());
}

market::InstrumentKeyV1 MakeKey(
    market::MarketV1 venue,
    std::string_view security_id_source,
    std::string_view security_id) {
    market::InstrumentKeyV1 key{};
    key.market = venue;
    key.security_id_source = Bytes(security_id_source);
    key.security_id = Bytes(security_id);
    return key;
}

market::InstrumentRuntimeMetadataV2 EquityMetadata() {
    market::InstrumentRuntimeMetadataV2 metadata{};
    metadata.quantity_unit = market::QuantityUnitV1::kShare;
    metadata.security_type = market::SecurityTypeV1::kEquity;
    metadata.asset_scope = market::AssetScopeV1::kDocumentedCore;
    return metadata;
}

market::DailyInstrumentSourceEntryV2 CatalogEntry(
    const market::InstrumentKeyV1& key,
    market::InstrumentRuntimeMetadataV2 metadata = EquityMetadata()) {
    market::DailyInstrumentSourceEntryV2 result{};
    result.key = key;
    result.metadata = metadata;
    return result;
}

std::unique_ptr<market::InstrumentRuntimeStateV2> MakeRuntimeState() {
    const std::array<market::DailyInstrumentSourceEntryV2,
                     kCatalogCapacity>
        entries{
            CatalogEntry(MakeKey(
                market::MarketV1::kShanghai, "", "600007")),
            CatalogEntry(MakeKey(
                market::MarketV1::kShanghai, "", "600033")),
            CatalogEntry(MakeKey(
                market::MarketV1::kShanghai, "", "600099")),
            CatalogEntry(MakeKey(
                market::MarketV1::kShenzhen, "102", "000018")),
            CatalogEntry(MakeKey(
                market::MarketV1::kShenzhen, "102", "000099")),
        };
    market::DailyInstrumentCatalogConfigV2 config{};
    config.trade_date = kTradeDate;
    config.catalog_version = 1U;
    config.session_epoch = kCatalogSessionEpoch;
    config.market_scope = market::kDailyCatalogMainlandScopeV2;
    config.coverage_complete = true;
    std::unique_ptr<market::DailyInstrumentCatalogV2> catalog;
    if (market::DailyInstrumentCatalogV2::Create(
            config, entries, &catalog) !=
            market::DailyInstrumentCatalogCreateErrorV2::kNone ||
        catalog == nullptr) {
        return nullptr;
    }
    std::unique_ptr<market::InstrumentRuntimeStateV2> runtime_state;
    if (market::InstrumentRuntimeStateV2::Create(
            *catalog, &runtime_state) !=
            market::InstrumentRuntimeStateErrorV2::kNone) {
        return nullptr;
    }
    return runtime_state;
}

void FillCommon(
    const market::InstrumentRuntimeStateV2& runtime_state,
    market::DecodedMarketCommonV1* common,
    market::MarketEventKindV1 kind,
    market::MarketV1 venue,
    std::uint8_t source_slot,
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence,
    std::uint32_t instrument_id) {
    *common = market::DecodedMarketCommonV1{};
    common->kind = kind;
    common->market = venue;
    common->origin.source_stream_id =
        kSourceStreamIds[source_slot];
    common->origin.trade_date = kTradeDate;
    common->origin.source_sequence = source_sequence;
    common->origin.recv_realtime_ns =
        static_cast<std::int64_t>(ingress_sequence * 100U);
    common->origin.recv_monotonic_ns =
        static_cast<std::int64_t>(ingress_sequence * 10U);
    common->instrument_id = instrument_id;
    market::InstrumentRuntimeEntryViewV2 identity{};
    if (runtime_state.LookupById(instrument_id, &identity) ==
            market::InstrumentRuntimeStateErrorV2::kNone &&
        identity.bound()) {
        common->ordinal = identity.ordinal;
    }
}

market::DecodedMarketEventV1 MakeShanghaiSnapshot(
    const market::InstrumentRuntimeStateV2& runtime_state,
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence,
    std::int64_t price_p6) {
    market::ShanghaiSnapshotV1 event{};
    FillCommon(
        runtime_state,
        &event.common,
        market::MarketEventKindV1::kShanghaiSnapshot,
        market::MarketV1::kShanghai,
        0U,
        source_sequence,
        ingress_sequence,
        kShanghaiInstrumentId);
    event.last_price.raw = price_p6;
    event.last_price.normalized_p6 = price_p6;
    event.last_price.scale = 6U;
    event.last_price.valid = true;
    return event;
}

market::DecodedMarketEventV1 MakeShenzhenSnapshot(
    const market::InstrumentRuntimeStateV2& runtime_state,
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence,
    std::int64_t price_p6) {
    market::ShenzhenSnapshotV1 event{};
    FillCommon(
        runtime_state,
        &event.common,
        market::MarketEventKindV1::kShenzhenSnapshot,
        market::MarketV1::kShenzhen,
        2U,
        source_sequence,
        ingress_sequence,
        kShenzhenInstrumentId);
    event.last_price.raw = price_p6;
    event.last_price.normalized_p6 = price_p6;
    event.last_price.scale = 6U;
    event.last_price.valid = true;
    return event;
}

market::DecodedMarketEventV1 MakeShanghaiTick(
    const market::InstrumentRuntimeStateV2& runtime_state,
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence) {
    market::ShanghaiTickV1 event{};
    FillCommon(
        runtime_state,
        &event.common,
        market::MarketEventKindV1::kShanghaiTick,
        market::MarketV1::kShanghai,
        1U,
        source_sequence,
        ingress_sequence,
        kShanghaiInstrumentId);
    event.fields.action = market::TickActionV1::kStatus;
    return event;
}

market::DecodedMarketEventV1 MakeShenzhenOrder(
    const market::InstrumentRuntimeStateV2& runtime_state,
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence) {
    market::ShenzhenOrderV1 event{};
    FillCommon(
        runtime_state,
        &event.common,
        market::MarketEventKindV1::kShenzhenOrder,
        market::MarketV1::kShenzhen,
        3U,
        source_sequence,
        ingress_sequence,
        kShenzhenInstrumentId);
    event.fields.action = market::TickActionV1::kAdd;
    return event;
}

market::DecodedMarketEventV1 MakeCatalogOnlySnapshot(
    const market::InstrumentRuntimeStateV2& runtime_state,
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence,
    std::int64_t price_p6) {
    market::ShanghaiSnapshotV1 event{};
    FillCommon(
        runtime_state,
        &event.common,
        market::MarketEventKindV1::kShanghaiSnapshot,
        market::MarketV1::kShanghai,
        0U,
        source_sequence,
        ingress_sequence,
        kCatalogOnlyInstrumentId);
    event.last_price.raw = price_p6;
    event.last_price.normalized_p6 = price_p6;
    event.last_price.scale = 6U;
    event.last_price.valid = true;
    return event;
}

const market::RealtimeHistoryRecordV1* Append(
    market::IntradayInstrumentStoreV1* store,
    std::uint8_t source_slot,
    std::uint64_t ingress_sequence,
    market::DecodedMarketEventV1 event,
    bool* ok) {
    const std::uint64_t tick_stream_sequence =
        source_slot == 0U || source_slot == 2U
            ? 0U
            : ingress_sequence;
    auto input = market::RealtimeHistoryEventInputV1::Create(
        source_slot,
        ingress_sequence,
        std::move(event),
        tick_stream_sequence);
    *ok &= Expect(input.has_value(), "construct history input");
    if (!input.has_value()) {
        return nullptr;
    }
    market::InstrumentRouteTokenV1 route{};
    *ok &= Expect(
        store->ResolveRouteToken(
            input->ordinal(),
            input->instrument_id(),
            &route) ==
            market::IntradayInstrumentStoreQueryErrorV1::kNone,
        "resolve Store route");
    const market::RealtimeHistoryRecordV1* record = nullptr;
    const market::IntradayInstrumentStoreAppendErrorV1 error =
        store->Append(
            route.worker, route, std::move(*input), &record);
    if (error !=
        market::IntradayInstrumentStoreAppendErrorV1::kNone) {
        std::cerr << "Store append error: "
                  << market::IntradayInstrumentStoreAppendErrorNameV1(
                         error)
                  << '\n';
    }
    *ok &= Expect(
        error == market::IntradayInstrumentStoreAppendErrorV1::kNone &&
            record != nullptr,
        "append immutable Store record and return receipt");
    return record;
}

bool Run() {
    bool ok = true;

    ok &= Expect(
        market::RealtimeLatestReadModelV1::Create(nullptr, nullptr) ==
            market::RealtimeLatestReadModelCreateErrorV1::kNullOutput,
        "Create rejects a null output before configuration");
    std::unique_ptr<market::RealtimeLatestReadModelV1> invalid;
    ok &= Expect(
        market::RealtimeLatestReadModelV1::Create(
            nullptr, &invalid) ==
                market::RealtimeLatestReadModelCreateErrorV1::
                    kInvalidConfiguration &&
            invalid == nullptr,
        "Create rejects a null runtime state");

    std::unique_ptr<market::InstrumentRuntimeStateV2> runtime_state =
        MakeRuntimeState();
    ok &= Expect(
        runtime_state != nullptr,
        "create frozen daily instrument runtime state");
    if (runtime_state == nullptr) {
        return false;
    }

    market::IntradayInstrumentStoreConfigV1 store_config{};
    store_config.segment_target_bytes = 16U * 1024U;
    store_config.maximum_session_records = 128U;
    store_config.maximum_session_accounted_bytes =
        16U * 1024U * 1024U;
    store_config.maximum_records_per_batch = 128U;
    store_config.coverage_from_open = true;
    std::unique_ptr<market::IntradayInstrumentStoreV1> store;
    ok &= Expect(
        market::IntradayInstrumentStoreV1::Create(
            store_config,
            2U,
            kSourceStreamIds,
            runtime_state.get(),
            &store) ==
                market::IntradayInstrumentStoreCreateErrorV1::kNone &&
            store != nullptr,
        "create required Store");
    if (store == nullptr) {
        return false;
    }

    std::unique_ptr<market::RealtimeLatestReadModelV1> model;
    ok &= Expect(
        market::RealtimeLatestReadModelV1::Create(
            runtime_state.get(), &model) ==
                market::RealtimeLatestReadModelCreateErrorV1::kNone &&
            model != nullptr,
        "create latest read model");
    if (model == nullptr) {
        return false;
    }

    market::RealtimeLatestRecordViewV1 view{};
    ok &= Expect(
        model->GetLatestSnapshot(kShanghaiInstrumentId, &view) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            view.instrument_id == kShanghaiInstrumentId &&
            view.status ==
                market::RealtimeLatestRecordStatusV1::kBoundNoTypeData &&
            view.record == nullptr,
        "bound instrument starts without requested-type data");
    ok &= Expect(
        model->GetLatestTick(kNoDataInstrumentId, &view) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            view.instrument_id == kNoDataInstrumentId &&
            view.status ==
                market::RealtimeLatestRecordStatusV1::
                    kBoundNoTypeData &&
            view.record == nullptr,
        "catalog identity without data reports bound-no-type-data");
    ok &= Expect(
        model->GetLatestTick(kOutOfCapacityInstrumentId, &view) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            view.instrument_id == kOutOfCapacityInstrumentId &&
            view.status ==
                market::RealtimeLatestRecordStatusV1::
                    kInvalidInstrumentId &&
            view.record == nullptr,
        "instrument ID above capacity is invalid");
    ok &= Expect(
        model->GetLatestSnapshot(0U, &view) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            view.status ==
                market::RealtimeLatestRecordStatusV1::
                    kInvalidInstrumentId,
        "zero instrument ID has an item-level status");
    ok &= Expect(
        model->GetLatestSnapshot(kShanghaiInstrumentId, nullptr) ==
            market::RealtimeLatestQueryErrorV1::kNullOutput,
        "single query rejects null output");

    market::InstrumentRuntimeEntryViewV2 catalog_only{};
    ok &= Expect(
        runtime_state->LookupById(
            kCatalogOnlyInstrumentId, &catalog_only) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            model->GetLatestSnapshot(
                kCatalogOnlyInstrumentId, &view) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            view.instrument_id == kCatalogOnlyInstrumentId &&
            view.status ==
                market::RealtimeLatestRecordStatusV1::kBoundNoTypeData &&
            view.record == nullptr,
        "every frozen catalog identity is queryable at model creation");

    const market::RealtimeHistoryRecordV1* const sh_snapshot_1 =
        Append(
            store.get(),
            0U,
            1U,
            MakeShanghaiSnapshot(*runtime_state, 1U, 1U, 7'100'000),
            &ok);
    const market::RealtimeHistoryRecordV1* const sz_snapshot =
        Append(
            store.get(),
            2U,
            2U,
            MakeShenzhenSnapshot(
                *runtime_state, 1U, 2U, 18'200'000),
            &ok);
    const market::RealtimeHistoryRecordV1* const sh_tick =
        Append(
            store.get(),
            1U,
            3U,
            MakeShanghaiTick(*runtime_state, 1U, 3U),
            &ok);
    const market::RealtimeHistoryRecordV1* const sz_order =
        Append(
            store.get(),
            3U,
            4U,
            MakeShenzhenOrder(*runtime_state, 1U, 4U),
            &ok);
    const market::RealtimeHistoryRecordV1* const sh_snapshot_5 =
        Append(
            store.get(),
            0U,
            5U,
            MakeShanghaiSnapshot(*runtime_state, 2U, 5U, 7'500'000),
            &ok);
    const market::RealtimeHistoryRecordV1* const sh_snapshot_10 =
        Append(
            store.get(),
            0U,
            10U,
            MakeShanghaiSnapshot(
                *runtime_state, 3U, 10U, 7'900'000),
            &ok);
    const market::RealtimeHistoryRecordV1* const catalog_only_snapshot =
        Append(
            store.get(),
            0U,
            6U,
            MakeCatalogOnlySnapshot(
                *runtime_state, 1U, 6U, 33'600'000),
            &ok);
    if (sh_snapshot_1 == nullptr || sz_snapshot == nullptr ||
        sh_tick == nullptr || sz_order == nullptr ||
        sh_snapshot_5 == nullptr || sh_snapshot_10 == nullptr ||
        catalog_only_snapshot == nullptr) {
        return false;
    }

    market::InstrumentRuntimeEntryViewV2 sh_lookup{};
    market::InstrumentRuntimeEntryViewV2 sz_lookup{};
    ok &= Expect(
        runtime_state->LookupById(kShanghaiInstrumentId, &sh_lookup) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            runtime_state->LookupById(
                kShenzhenInstrumentId, &sz_lookup) ==
                market::InstrumentRuntimeStateErrorV2::kNone,
        "resolve frozen catalog identities");
    bool updated = true;
    ok &= Expect(
        model->PublishApplied(
            sh_lookup.ordinal, nullptr, &updated) ==
                market::RealtimeLatestPublishErrorV1::kNullRecord &&
            !updated,
        "publish clears updated and rejects null record");
    ok &= Expect(
        model->PublishApplied(
            runtime_state->capacity(), sh_snapshot_1, &updated) ==
                market::RealtimeLatestPublishErrorV1::kInvalidOrdinal &&
            !updated,
        "publish rejects an invalid ordinal");
    ok &= Expect(
        model->PublishApplied(
            sz_lookup.ordinal, sh_snapshot_1, &updated) ==
                market::RealtimeLatestPublishErrorV1::
                    kInstrumentMismatch &&
            !updated,
        "publish validates ordinal-to-instrument identity");

    ok &= Expect(
        model->PublishApplied(
            sh_lookup.ordinal, sh_snapshot_1, &updated) ==
                market::RealtimeLatestPublishErrorV1::kNone &&
            updated,
        "publish Shanghai snapshot");
    ok &= Expect(
        model->PublishApplied(
            sz_lookup.ordinal, sz_snapshot, &updated) ==
                market::RealtimeLatestPublishErrorV1::kNone &&
            updated,
        "publish Shenzhen snapshot");
    ok &= Expect(
        model->PublishApplied(
            sh_lookup.ordinal, sh_tick, &updated) ==
                market::RealtimeLatestPublishErrorV1::kNone &&
            updated,
        "publish Shanghai mixed tick");
    ok &= Expect(
        model->PublishApplied(
            sz_lookup.ordinal, sz_order, &updated) ==
                market::RealtimeLatestPublishErrorV1::kNone &&
            updated,
        "publish Shenzhen order as mixed tick");
    ok &= Expect(
        model->PublishApplied(
            catalog_only.ordinal, catalog_only_snapshot, &updated) ==
                market::RealtimeLatestPublishErrorV1::kNone &&
            updated,
        "catalog-only identity publishes through its preallocated slot");
    ok &= Expect(
        model->GetLatestSnapshot(kCatalogOnlyInstrumentId, &view) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            view.available() && view.record == catalog_only_snapshot,
        "catalog identity is readable after publication");
    ok &= Expect(
        model->GetLatestTick(kCatalogOnlyInstrumentId, &view) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            view.status ==
                market::RealtimeLatestRecordStatusV1::kBoundNoTypeData &&
            view.record == nullptr,
        "bound instrument with another type remains no-data for this type");

    ok &= Expect(
        model->GetLatestSnapshot(kShanghaiInstrumentId, &view) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            view.available() && view.record == sh_snapshot_1 &&
            view.record->kind() ==
                market::MarketEventKindV1::kShanghaiSnapshot,
        "single query returns exact Shanghai snapshot receipt");
    const auto* const sh_payload =
        market::StoredMarketEventGetV1<market::ShanghaiSnapshotV1>(
            view.record->event());
    ok &= Expect(
        sh_payload != nullptr &&
            sh_payload->last_price.normalized_p6 == 7'100'000,
        "Shanghai snapshot retains exact typed payload");

    ok &= Expect(
        model->GetLatestSnapshot(kShenzhenInstrumentId, &view) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            view.available() && view.record == sz_snapshot &&
            market::StoredMarketEventGetV1<
                market::ShenzhenSnapshotV1>(
                view.record->event()) != nullptr,
        "single query returns exact Shenzhen snapshot payload");
    ok &= Expect(
        model->GetLatestTick(kShanghaiInstrumentId, &view) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            view.available() && view.record == sh_tick,
        "latest tick retains Shanghai status event");
    ok &= Expect(
        model->GetLatestTick(kShenzhenInstrumentId, &view) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            view.available() && view.record == sz_order &&
            view.record->kind() ==
                market::MarketEventKindV1::kShenzhenOrder,
        "latest tick retains Shenzhen order without claiming trade");

    const std::array<std::uint32_t, 7U> ids{
        kShenzhenInstrumentId,
        kShanghaiInstrumentId,
        kCatalogOnlyInstrumentId,
        kNoDataInstrumentId,
        0U,
        kShanghaiInstrumentId,
        kOutOfCapacityInstrumentId};
    std::array<market::RealtimeLatestRecordViewV1, ids.size()>
        snapshots{};
    ok &= Expect(
        model->GetLatestSnapshots(ids, snapshots) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            snapshots[0U].record == sz_snapshot &&
            snapshots[1U].record == sh_snapshot_1 &&
            snapshots[2U].record == catalog_only_snapshot &&
            snapshots[3U].status ==
                market::RealtimeLatestRecordStatusV1::
                    kBoundNoTypeData &&
            snapshots[4U].status ==
                market::RealtimeLatestRecordStatusV1::
                    kInvalidInstrumentId &&
            snapshots[5U].record == sh_snapshot_1 &&
            snapshots[6U].status ==
                market::RealtimeLatestRecordStatusV1::
                    kInvalidInstrumentId,
        "batch preserves order, duplicates, and item-level status");

    std::array<market::RealtimeLatestRecordViewV1, 1U> wrong_size{
        market::RealtimeLatestRecordViewV1{
            kOutOfCapacityInstrumentId,
            market::RealtimeLatestRecordStatusV1::kAvailable,
            sh_snapshot_1}};
    ok &= Expect(
        model->GetLatestSnapshots(ids, wrong_size) ==
                market::RealtimeLatestQueryErrorV1::kInvalidArgument &&
            wrong_size[0U].record == nullptr,
        "batch rejects shape mismatch and clears output");
    ok &= Expect(
        model->GetLatest(
            static_cast<market::RealtimeLatestRecordKindV1>(99U),
            kShanghaiInstrumentId,
            &view) ==
                market::RealtimeLatestQueryErrorV1::kInvalidArgument &&
            view.record == nullptr,
        "general selector rejects an unknown kind");

    ok &= Expect(
        model->PublishApplied(
            sh_lookup.ordinal, sh_snapshot_10, &updated) ==
                market::RealtimeLatestPublishErrorV1::kNone &&
            updated,
        "publish newer ingress snapshot");
    ok &= Expect(
        model->PublishApplied(
            sh_lookup.ordinal, sh_snapshot_5, &updated) ==
                market::RealtimeLatestPublishErrorV1::kNone &&
            !updated,
        "defensive stale publication is ignored without regression");
    ok &= Expect(
        model->GetLatestSnapshot(kShanghaiInstrumentId, &view) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            view.record == sh_snapshot_10 &&
            view.record->ingress_sequence() == 10U,
        "latest snapshot remains the largest ingress sequence");

    std::vector<const market::RealtimeHistoryRecordV1*>
        sequential_snapshots;
    sequential_snapshots.reserve(34U);
    sequential_snapshots.push_back(sh_snapshot_5);
    sequential_snapshots.push_back(sh_snapshot_10);
    for (std::uint64_t source_sequence = 4U;
         source_sequence < 36U;
         ++source_sequence) {
        const std::uint64_t ingress_sequence =
            source_sequence + 7U;
        const market::RealtimeHistoryRecordV1* const record =
            Append(
                store.get(),
                0U,
                ingress_sequence,
                MakeShanghaiSnapshot(
                    *runtime_state,
                    source_sequence,
                    ingress_sequence,
                    7'900'000 +
                        static_cast<std::int64_t>(
                            source_sequence)),
                &ok);
        if (record == nullptr) {
            return false;
        }
        sequential_snapshots.push_back(record);
    }

    std::unique_ptr<market::RealtimeLatestReadModelV1>
        concurrent_model;
    ok &= Expect(
        market::RealtimeLatestReadModelV1::Create(
            runtime_state.get(), &concurrent_model) ==
                market::RealtimeLatestReadModelCreateErrorV1::kNone &&
            concurrent_model != nullptr,
        "create concurrent-read model");
    if (concurrent_model != nullptr) {
        ok &= Expect(
            concurrent_model->PublishApplied(
                sh_lookup.ordinal, sh_snapshot_1) ==
                market::RealtimeLatestPublishErrorV1::kNone,
            "seed concurrent-read model");
        std::atomic<bool> reader_ready{false};
        std::atomic<bool> publication_done{false};
        std::atomic<bool> reader_ok{true};
        std::thread reader([&] {
            std::uint64_t last_ingress = 0U;
            reader_ready.store(true, std::memory_order_release);
            do {
                market::RealtimeLatestRecordViewV1 observed{};
                if (concurrent_model->GetLatestSnapshot(
                        kShanghaiInstrumentId, &observed) !=
                        market::RealtimeLatestQueryErrorV1::kNone ||
                    !observed.available() ||
                    observed.record->instrument_id() !=
                        kShanghaiInstrumentId ||
                    observed.record->kind() !=
                        market::MarketEventKindV1::
                            kShanghaiSnapshot ||
                    observed.record->ingress_sequence() <
                        last_ingress) {
                    reader_ok.store(false, std::memory_order_release);
                    return;
                }
                last_ingress =
                    observed.record->ingress_sequence();
            } while (!publication_done.load(
                std::memory_order_acquire));
        });
        while (!reader_ready.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (const market::RealtimeHistoryRecordV1* candidate :
             sequential_snapshots) {
            if (concurrent_model->PublishApplied(
                    sh_lookup.ordinal, candidate) !=
                market::RealtimeLatestPublishErrorV1::kNone) {
                ok = false;
                break;
            }
        }
        publication_done.store(true, std::memory_order_release);
        reader.join();
        ok &= Expect(
            reader_ok.load(std::memory_order_acquire),
            "concurrent acquire readers see complete monotonic records");
        ok &= Expect(
            concurrent_model->GetLatestSnapshot(
                kShanghaiInstrumentId, &view) ==
                    market::RealtimeLatestQueryErrorV1::kNone &&
                view.record == sequential_snapshots.back(),
            "single owner continuously publishes while readers poll");

        std::atomic<bool> healthy_read_complete{false};
        std::atomic<bool> coverage_mark_complete{false};
        std::atomic<bool> coverage_reader_ok{true};
        std::thread coverage_reader([&] {
            market::RealtimeLatestRecordViewV1 observed{};
            if (concurrent_model->GetLatestSnapshot(
                    kShanghaiInstrumentId, &observed) !=
                    market::RealtimeLatestQueryErrorV1::kNone ||
                !observed.available()) {
                coverage_reader_ok.store(
                    false, std::memory_order_release);
            }
            healthy_read_complete.store(
                true, std::memory_order_release);
            while (!coverage_mark_complete.load(
                std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            observed = market::RealtimeLatestRecordViewV1{};
            const market::RealtimeLatestQueryErrorV1 error =
                concurrent_model->GetLatestSnapshot(
                    kShanghaiInstrumentId, &observed);
            coverage_reader_ok.store(
                error ==
                        market::RealtimeLatestQueryErrorV1::
                            kCoverageLost &&
                    observed.record == nullptr &&
                    !observed.available(),
                std::memory_order_release);
        });
        while (!healthy_read_complete.load(
            std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        concurrent_model->MarkCoverageLost();
        coverage_mark_complete.store(
            true, std::memory_order_release);
        coverage_reader.join();
        ok &= Expect(
            coverage_reader_ok.load(std::memory_order_acquire),
            "cross-thread sticky coverage transition fails closed");
    }

    auto rejected_input = market::RealtimeHistoryEventInputV1::Create(
        0U,
        99U,
        MakeShanghaiSnapshot(
            *runtime_state, 99U, 99U, 7'099'000));
    market::InstrumentRouteTokenV1 rejected_route{};
    ok &= Expect(
        rejected_input.has_value() &&
            store->ResolveRouteToken(
                rejected_input->ordinal(),
                rejected_input->instrument_id(),
                &rejected_route) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone,
        "prepare rejected Store append receipt test");
    const market::RealtimeHistoryRecordV1* rejected_receipt =
        sequential_snapshots.back();
    if (rejected_input.has_value()) {
        ok &= Expect(
            store->Append(
                (rejected_route.worker + 1U) % 2U,
                rejected_route,
                std::move(*rejected_input),
                &rejected_receipt) ==
                    market::IntradayInstrumentStoreAppendErrorV1::
                        kWrongWorker &&
                rejected_receipt == nullptr,
            "failed Store append always clears its record receipt");
    }

    model->MarkCoverageLost();
    model->MarkCoverageLost();
    ok &= Expect(
        model->coverage_lost(),
        "coverage loss is sticky and idempotent");
    view = market::RealtimeLatestRecordViewV1{
        kShanghaiInstrumentId,
        market::RealtimeLatestRecordStatusV1::kAvailable,
        sh_snapshot_10};
    ok &= Expect(
        model->GetLatestSnapshot(kShanghaiInstrumentId, &view) ==
                market::RealtimeLatestQueryErrorV1::kCoverageLost &&
            view.record == nullptr,
        "query fails closed and clears output after coverage loss");
    updated = true;
    ok &= Expect(
        model->PublishApplied(
            sh_lookup.ordinal, sh_snapshot_10, &updated) ==
                market::RealtimeLatestPublishErrorV1::kCoverageLost &&
            !updated,
        "publication is rejected after coverage loss");

    ok &= Expect(
        market::RealtimeLatestReadModelCreateErrorNameV1(
            market::RealtimeLatestReadModelCreateErrorV1::kNone) ==
                "none" &&
            market::RealtimeLatestPublishErrorNameV1(
                market::RealtimeLatestPublishErrorV1::
                    kIngressConflict) == "ingress_conflict" &&
            market::RealtimeLatestPublishErrorNameV1(
                market::RealtimeLatestPublishErrorV1::
                    kUnboundInstrument) == "unbound_instrument" &&
            market::RealtimeLatestQueryErrorNameV1(
                market::RealtimeLatestQueryErrorV1::kUnavailable) ==
                "unavailable",
        "public error names are stable");
    return ok;
}

}  // namespace

int main() {
    if (!Run()) {
        std::cerr
            << "realtime latest read model regression test failed\n";
        return 1;
    }
    std::cout
        << "realtime latest read model regression test passed\n";
    return 0;
}
