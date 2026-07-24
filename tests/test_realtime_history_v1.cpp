#include "l2flow/market/realtime_history_v1.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
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

market::RealtimeHistoryRecordHandleV1 MakeRecord(
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
    snapshot.last_price.valid = true;
    snapshot.last_price.raw = price;
    snapshot.last_price.normalized_p6 = price;
    snapshot.last_price.scale = 6U;

    market::RetainedMarketEventV1 retained(
        std::in_place_type<
            std::unique_ptr<const market::ShanghaiSnapshotV1>>,
        std::make_unique<const market::ShanghaiSnapshotV1>(
            std::move(snapshot)));
    market::RealtimeHistoryRecordHandleV1 record;
    if (!market::RealtimeHistoryRecordV1::Create(
            0U, ingress_sequence, std::move(retained), &record)) {
        return nullptr;
    }
    return record;
}

market::RealtimeHistoryRecordHandleV1 MakeTickRecord(
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
    tick.fields.price.valid = true;
    tick.fields.price.raw = price;
    tick.fields.price.normalized_p6 = price;
    tick.fields.price.scale = 6U;

    market::RetainedMarketEventV1 retained(
        std::in_place_type<
            std::unique_ptr<const market::ShanghaiTickV1>>,
        std::make_unique<const market::ShanghaiTickV1>(std::move(tick)));
    market::RealtimeHistoryRecordHandleV1 record;
    if (!market::RealtimeHistoryRecordV1::Create(
            1U, ingress_sequence, std::move(retained), &record)) {
        return nullptr;
    }
    return record;
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

std::int64_t LastPrice(
    const market::RealtimeInstrumentGenerationV1* row) {
    if (row == nullptr || row->latest_snapshot == nullptr) {
        return -1;
    }
    const auto* snapshot = market::RetainedMarketEventGetV1<
        market::ShanghaiSnapshotV1>(row->latest_snapshot->event());
    return snapshot == nullptr ? -1 : snapshot->last_price.normalized_p6;
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
    config.maximum_records_per_instrument = 2U;
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
        MakeRecord(
            std::numeric_limits<std::uint64_t>::max(),
            1U,
            1U,
            1'000'000) == nullptr &&
            MakeRecord(
                1U,
                std::numeric_limits<std::uint64_t>::max(),
                1U,
                1'000'000) == nullptr,
        "message records reject the reserved sequence sentinel");

    const auto generation1 = MakeWatermark(*registry, 1U, 4U, 3U, 2U);
    ok &= Expect(
        runtime->BeginGeneration(generation1) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "begin generation 1");
    ok &= Expect(
        runtime->TrySubmit(MakeRecord(1U, 2U, 1U, 1'000'001)) ==
            market::RealtimeHistorySubmitErrorV1::kNone,
        "submit later snapshot to generation-1 worker 1");
    ok &= Expect(
        runtime->TrySubmit(MakeRecord(2U, 3U, 2U, 1'000'002)) ==
            market::RealtimeHistorySubmitErrorV1::kNone,
        "submit generation-1 worker-0 record");
    // Source decoders may reach a worker in a different order from the
    // serialized callback. This earlier global record is deliberately
    // submitted after the later snapshot above.
    ok &= Expect(
        runtime->TrySubmit(MakeTickRecord(1U, 1U, 1U, 900'001)) ==
            market::RealtimeHistorySubmitErrorV1::kNone,
        "submit earlier cross-source tick after later snapshot");

    // Source 0 is fenced first. Its next record is legal realtime work, but
    // every worker must park it until all four generation-1 fences arrive.
    ok &= Expect(
        runtime->SealSource(0U, 1U) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "seal source 0 generation 1");
    ok &= Expect(
        runtime->TrySubmit(MakeRecord(3U, 4U, 1U, 2'000'001)) ==
            market::RealtimeHistorySubmitErrorV1::kNone,
        "submit post-fence source record");
    for (std::uint8_t source = 1U; source < 4U; ++source) {
        ok &= Expect(
            runtime->SealSource(source, 1U) ==
                market::RealtimeHistoryGenerationErrorV1::kNone,
            "seal idle source generation 1");
    }

    std::shared_ptr<const market::RealtimeHistoryGenerationV1> first;
    ok &= Expect(
        runtime->WaitForGeneration(
            1U, std::chrono::seconds(2), &first) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "wait generation 1");
    ok &= Expect(first != nullptr && first->instruments().size() == 3U,
                 "generation 1 has exact fixed universe");
    ok &= Expect(LastPrice(first->Find(1U)) == 1'000'001,
                 "post-fence update excluded from generation 1");
    const market::RealtimeInstrumentGenerationV1* first_row =
        first->Find(1U);
    ok &= Expect(
        first_row != nullptr && first_row->history.size() == 2U &&
            first_row->history[0U]->ingress_sequence() == 1U &&
            first_row->history[1U]->ingress_sequence() == 2U,
        "cross-source history is ordered by process ingress, not worker arrival");
    ok &= Expect(LastPrice(first->Find(2U)) == 1'000'002,
                 "other worker is same generation");
    ok &= Expect(first->Find(9U) != nullptr &&
                     first->Find(9U)->latest_snapshot == nullptr &&
                     first->Find(9U)->history.empty(),
                 "unobserved fixed-universe instrument remains empty");

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
    std::shared_ptr<const market::RealtimeHistoryGenerationV1> second;
    ok &= Expect(
        runtime->WaitForGeneration(
            2U, std::chrono::seconds(2), &second) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "wait generation 2");
    ok &= Expect(LastPrice(second->Find(1U)) == 2'000'001,
                 "parked update appears only in generation 2");
    const market::RealtimeInstrumentGenerationV1* second_row =
        second->Find(1U);
    ok &= Expect(
        second_row != nullptr && second_row->history.size() == 2U &&
            second_row->history[0U]->ingress_sequence() == 2U &&
            second_row->history[1U]->ingress_sequence() == 4U,
        "bounded history retains the newest ingress suffix");
    ok &= Expect(LastPrice(first->Find(1U)) == 1'000'001,
                 "old generation handle remains immutable and alive");
    ok &= Expect(runtime->AcquireLatestGeneration().get() == second.get(),
                 "whole generation atomically published");
    ok &= Expect(!runtime->IsGenerationCurrentAndHealthy(first) &&
                     runtime->IsGenerationCurrentAndHealthy(second),
                 "only latest complete generation is current");

    runtime->StopAndDrain();
    return ok ? 0 : 1;
}
