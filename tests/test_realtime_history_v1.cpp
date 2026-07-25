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
    return ok ? 0 : 1;
}
