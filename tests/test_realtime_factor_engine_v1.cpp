#include "l2flow/factor/realtime_factor_engine_v1.h"

#include "l2flow/common/identity128.h"
#include "l2flow/market/instrument_registry.h"
#include "l2flow/market/market_types_v1.h"
#include "l2flow/market/realtime_history_v1.h"

#include <array>
#include <chrono>
#include <cmath>
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

namespace factor = l2flow::factor;
namespace market = l2flow::market;

namespace {

struct TestContext final {
    int failures = 0;

    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }
};

std::vector<std::byte> Bytes(std::string_view text) {
    const std::span<const char> characters(text.data(), text.size());
    const std::span<const std::byte> bytes = std::as_bytes(characters);
    return {bytes.begin(), bytes.end()};
}

market::InstrumentRegistryEntryV1 RegistryEntry(
    std::uint32_t instrument_id,
    std::string_view security_id) {
    market::InstrumentRegistryEntryV1 entry{};
    entry.instrument_id = instrument_id;
    entry.key.market = market::MarketV1::kShanghai;
    entry.key.security_id_source = Bytes("101");
    entry.key.security_id = Bytes(security_id);
    entry.quantity_unit = market::QuantityUnitV1::kShare;
    entry.security_type = market::SecurityTypeV1::kEquity;
    entry.asset_scope = market::AssetScopeV1::kDocumentedCore;
    return entry;
}

market::InstrumentRegistryEntryV1 ShenzhenRegistryEntry(
    std::uint32_t instrument_id,
    std::string_view security_id) {
    market::InstrumentRegistryEntryV1 entry{};
    entry.instrument_id = instrument_id;
    entry.key.market = market::MarketV1::kShenzhen;
    // Production publishes this opaque source identifier as four exact bytes;
    // it must not be trimmed to "102" in registry fixtures.
    entry.key.security_id_source = Bytes("102 ");
    entry.key.security_id = Bytes(security_id);
    entry.quantity_unit = market::QuantityUnitV1::kShare;
    entry.security_type = market::SecurityTypeV1::kEquity;
    entry.asset_scope = market::AssetScopeV1::kDocumentedCore;
    return entry;
}

std::unique_ptr<market::InstrumentRegistryV1> MakeRegistry(
    TestContext* test,
    std::span<const market::InstrumentRegistryEntryV1> entries) {
    std::unique_ptr<market::InstrumentRegistryV1> registry;
    const auto error = market::InstrumentRegistryV1::Create(
        17U, entries, &registry);
    test->Expect(
        error == market::InstrumentRegistryCreateErrorV1::kNone &&
            registry != nullptr,
        "test instrument registry creates");
    return registry;
}

std::unique_ptr<market::RealtimeHistoryRuntimeV1> MakeRuntime(
    TestContext* test,
    const market::InstrumentRegistryV1* registry,
    std::uint64_t maximum_session_records = 1024U) {
    market::RealtimeHistoryRuntimeConfigV1 config{};
    config.source_stream_ids = {101U, 102U, 103U, 104U};
    config.worker_count = 2U;
    config.queue_capacity_per_source_worker = 64U;
    config.registry = registry;
    config.intraday_store.segment_target_bytes =
        market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
    config.intraday_store.maximum_session_records =
        maximum_session_records;
    config.intraday_store.maximum_session_accounted_bytes = 1U << 30U;
    config.intraday_store.maximum_records_per_batch = 64U;
    config.intraday_store.coverage_from_open = true;
    std::unique_ptr<market::RealtimeHistoryRuntimeV1> runtime;
    const auto error =
        market::RealtimeHistoryRuntimeV1::Create(config, &runtime);
    test->Expect(
        error == market::RealtimeHistoryCreateErrorV1::kNone &&
            runtime != nullptr,
        "test history runtime creates");
    return runtime;
}

l2flow::common::Identity128 RunId() {
    l2flow::common::Identity128 run_id{};
    for (std::size_t index = 0U; index < run_id.size(); ++index) {
        run_id[index] = static_cast<std::byte>(index + 1U);
    }
    return run_id;
}

std::shared_ptr<const market::IntradayInstrumentStoreGenerationV1>
PublishStoreGeneration(
    TestContext* test,
    market::RealtimeHistoryRuntimeV1* runtime,
    const market::InstrumentRegistryV1& registry,
    std::uint64_t generation,
    std::uint64_t ingress_sequence_exclusive,
    std::array<std::uint64_t, market::kRealtimeHistorySourceCountV1>
        source_sequence_exclusive) {
    std::array<market::RealtimeSourceWatermarkV1,
               market::kRealtimeHistorySourceCountV1>
        source_watermarks{};
    const auto source_ids = runtime->config().source_stream_ids;
    for (std::size_t source = 0U; source < source_watermarks.size(); ++source) {
        source_watermarks[source].source_stream_id = source_ids[source];
        source_watermarks[source].sequence_exclusive =
            source_sequence_exclusive[source];
    }

    market::RealtimeHistoryWatermarkV1 watermark{};
    test->Expect(
        market::BuildRealtimeHistoryWatermarkV1(
            RunId(),
            generation,
            20260724U,
            ingress_sequence_exclusive,
            1000U + generation,
            registry,
            source_watermarks,
            &watermark) ==
            market::RealtimeHistoryWatermarkErrorV1::kNone,
        "store watermark builds");
    test->Expect(
        runtime->BeginGeneration(watermark) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "store generation begins");
    for (std::uint8_t source = 0U;
         source < market::kRealtimeHistorySourceCountV1;
         ++source) {
        test->Expect(
            runtime->SealSource(source, generation) ==
                market::RealtimeHistoryGenerationErrorV1::kNone,
            "store source fence seals");
    }

    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        result;
    test->Expect(
        runtime->WaitForGeneration(
            generation, std::chrono::seconds(5), &result) ==
                market::RealtimeHistoryGenerationErrorV1::kNone &&
            result != nullptr,
        "complete store generation publishes");
    return result;
}

bool SubmitShanghaiSnapshot(
    market::RealtimeHistoryRuntimeV1* runtime,
    std::uint32_t instrument_id,
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence,
    std::int64_t normalized_last_price_p6) {
    market::ShanghaiSnapshotV1 snapshot{};
    snapshot.common.kind = market::MarketEventKindV1::kShanghaiSnapshot;
    snapshot.common.market = market::MarketV1::kShanghai;
    snapshot.common.origin.source_stream_id = 101U;
    snapshot.common.origin.source_sequence = source_sequence;
    snapshot.common.instrument_id = instrument_id;
    if (runtime == nullptr || runtime->config().registry == nullptr) {
        return false;
    }
    const auto lookup =
        runtime->config().registry->LookupById(instrument_id);
    if (!lookup.known()) {
        return false;
    }
    snapshot.common.registry_ordinal = lookup.registry_ordinal;
    snapshot.common.origin.recv_realtime_ns = 100;
    snapshot.common.origin.recv_monotonic_ns = 90;
    snapshot.last_price.raw = normalized_last_price_p6 / 1000;
    snapshot.last_price.normalized_p6 = normalized_last_price_p6;
    snapshot.last_price.scale = 3U;
    snapshot.last_price.valid = true;

    market::DecodedMarketEventV1 decoded(std::move(snapshot));
    auto input = market::RealtimeHistoryEventInputV1::Create(
        0U, ingress_sequence, std::move(decoded));
    if (!input.has_value()) {
        return false;
    }
    return runtime->TrySubmit(std::move(*input)) ==
           market::RealtimeHistorySubmitErrorV1::kNone;
}

bool SubmitShenzhenSnapshot(
    market::RealtimeHistoryRuntimeV1* runtime,
    std::uint32_t instrument_id,
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence,
    std::int64_t normalized_last_price_p6) {
    market::ShenzhenSnapshotV1 snapshot{};
    snapshot.common.kind = market::MarketEventKindV1::kShenzhenSnapshot;
    snapshot.common.market = market::MarketV1::kShenzhen;
    snapshot.common.origin.source_stream_id = 103U;
    snapshot.common.origin.source_sequence = source_sequence;
    snapshot.common.instrument_id = instrument_id;
    if (runtime == nullptr || runtime->config().registry == nullptr) {
        return false;
    }
    const auto lookup =
        runtime->config().registry->LookupById(instrument_id);
    if (!lookup.known()) {
        return false;
    }
    snapshot.common.registry_ordinal = lookup.registry_ordinal;
    snapshot.common.origin.recv_realtime_ns = 200;
    snapshot.common.origin.recv_monotonic_ns = 190;
    snapshot.last_price.raw = normalized_last_price_p6 / 1000;
    snapshot.last_price.normalized_p6 = normalized_last_price_p6;
    snapshot.last_price.scale = 3U;
    snapshot.last_price.valid = true;

    market::DecodedMarketEventV1 decoded(std::move(snapshot));
    auto input = market::RealtimeHistoryEventInputV1::Create(
        2U, ingress_sequence, std::move(decoded));
    if (!input.has_value()) {
        return false;
    }
    return runtime->TrySubmit(std::move(*input)) ==
           market::RealtimeHistorySubmitErrorV1::kNone;
}

bool WatermarkExactEqual(
    const market::RealtimeHistoryWatermarkV1& left,
    const market::RealtimeHistoryWatermarkV1& right) {
    if (left.run_id != right.run_id ||
        left.generation != right.generation ||
        left.trade_date != right.trade_date ||
        left.ingress_sequence_exclusive !=
            right.ingress_sequence_exclusive ||
        left.recv_monotonic_cut_ns != right.recv_monotonic_cut_ns ||
        left.registry_version != right.registry_version ||
        left.registry_sha256 != right.registry_sha256 ||
        left.input_identity_sha256 != right.input_identity_sha256) {
        return false;
    }
    for (std::size_t index = 0U; index < left.sources.size(); ++index) {
        if (left.sources[index].source_stream_id !=
                right.sources[index].source_stream_id ||
            left.sources[index].sequence_exclusive !=
                right.sources[index].sequence_exclusive) {
            return false;
        }
    }
    return true;
}

enum class BadOutputMode : std::uint8_t {
    kGood = 0U,
    kMissingRow,
    kReversedRows,
    kNan,
    kInfinity,
    kInvalidNonzero,
    kInvalidNegativeZero,
    kWrongColumnCount,
};

class TestCalculator final : public factor::RealtimeFactorCalculatorV1 {
public:
    explicit TestCalculator(BadOutputMode mode) : mode_(mode) {}

    std::span<const factor::RealtimeFactorDefinitionV1> definitions()
        const noexcept override {
        return definitions_;
    }

    factor::RealtimeFactorCalculatorErrorV1 Calculate(
        const market::IntradayInstrumentStoreGenerationV1& store,
        std::vector<factor::RealtimeFactorPointV1>* output)
        const noexcept override {
        if (output == nullptr) {
            return factor::RealtimeFactorCalculatorErrorV1::kNullOutput;
        }
        try {
            std::vector<factor::RealtimeFactorPointV1> points;
            points.reserve(store.instrument_count());
            for (std::size_t ordinal = 0U;
                 ordinal < store.instrument_count();
                 ++ordinal) {
                market::IntradayInstrumentSummaryV1 instrument{};
                if (store.SummaryAt(ordinal, &instrument) !=
                    market::IntradayInstrumentStoreQueryErrorV1::kNone) {
                    return factor::RealtimeFactorCalculatorErrorV1::
                        kInvalidStore;
                }
                factor::RealtimeFactorPointV1 point{};
                point.instrument_id = instrument.instrument_id;
                point.values.push_back(factor::RealtimeFactorValueV1{
                    static_cast<double>(instrument.instrument_id), true});
                points.push_back(std::move(point));
            }
            if (mode_ == BadOutputMode::kMissingRow && !points.empty()) {
                points.pop_back();
            } else if (mode_ == BadOutputMode::kReversedRows &&
                       points.size() > 1U) {
                std::swap(points.front(), points.back());
            } else if (mode_ == BadOutputMode::kNan && !points.empty()) {
                points.front().values.front().value =
                    std::numeric_limits<double>::quiet_NaN();
            } else if (mode_ == BadOutputMode::kInfinity &&
                       !points.empty()) {
                points.front().values.front().value =
                    std::numeric_limits<double>::infinity();
            } else if (mode_ == BadOutputMode::kInvalidNonzero &&
                       !points.empty()) {
                points.front().values.front().valid = false;
            } else if (mode_ == BadOutputMode::kInvalidNegativeZero &&
                       !points.empty()) {
                points.front().values.front() =
                    factor::RealtimeFactorValueV1{-0.0, false};
            } else if (mode_ == BadOutputMode::kWrongColumnCount &&
                       !points.empty()) {
                points.front().values.clear();
            }
            *output = std::move(points);
            return factor::RealtimeFactorCalculatorErrorV1::kNone;
        } catch (...) {
            return factor::RealtimeFactorCalculatorErrorV1::
                kResourceExhausted;
        }
    }

private:
    std::array<factor::RealtimeFactorDefinitionV1, 1U> definitions_{{
        {"test_projection", "v1", "test-only instrument-id projection"}}};
    BadOutputMode mode_ = BadOutputMode::kGood;
};

class BlockingCalculator final : public factor::RealtimeFactorCalculatorV1 {
public:
    std::span<const factor::RealtimeFactorDefinitionV1> definitions()
        const noexcept override {
        return definitions_;
    }

    factor::RealtimeFactorCalculatorErrorV1 Calculate(
        const market::IntradayInstrumentStoreGenerationV1& store,
        std::vector<factor::RealtimeFactorPointV1>* output)
        const noexcept override {
        if (output == nullptr) {
            return factor::RealtimeFactorCalculatorErrorV1::kNullOutput;
        }
        {
            std::unique_lock<std::mutex> lock(mutex_);
            entered_ = true;
            condition_.notify_all();
            condition_.wait(lock, [this] { return released_; });
        }
        try {
            std::vector<factor::RealtimeFactorPointV1> points;
            points.reserve(store.instrument_count());
            for (std::size_t ordinal = 0U;
                 ordinal < store.instrument_count();
                 ++ordinal) {
                market::IntradayInstrumentSummaryV1 instrument{};
                if (store.SummaryAt(ordinal, &instrument) !=
                    market::IntradayInstrumentStoreQueryErrorV1::kNone) {
                    return factor::RealtimeFactorCalculatorErrorV1::
                        kInvalidStore;
                }
                factor::RealtimeFactorPointV1 point{};
                point.instrument_id = instrument.instrument_id;
                point.values.push_back(
                    factor::RealtimeFactorValueV1{1.0, true});
                points.push_back(std::move(point));
            }
            *output = std::move(points);
            return factor::RealtimeFactorCalculatorErrorV1::kNone;
        } catch (...) {
            return factor::RealtimeFactorCalculatorErrorV1::
                kResourceExhausted;
        }
    }

    bool WaitUntilEntered(std::chrono::seconds timeout) const {
        std::unique_lock<std::mutex> lock(mutex_);
        return condition_.wait_for(
            lock, timeout, [this] { return entered_; });
    }

    void Release() const {
        std::lock_guard<std::mutex> lock(mutex_);
        released_ = true;
        condition_.notify_all();
    }

private:
    std::array<factor::RealtimeFactorDefinitionV1, 1U> definitions_{{
        {"blocking_test_projection", "v1", "test-only constant projection"}}};
    mutable std::mutex mutex_;
    mutable std::condition_variable condition_;
    mutable bool entered_ = false;
    mutable bool released_ = false;
};

std::unique_ptr<factor::RealtimeFactorEngineV1> MakeFactorEngine(
    TestContext* test,
    const market::InstrumentRegistryV1* registry,
    const market::RealtimeHistoryRuntimeV1* runtime,
    std::shared_ptr<const factor::RealtimeFactorCalculatorV1> calculator) {
    factor::RealtimeFactorEngineConfigV1 config{};
    config.registry = registry;
    config.generation_runtime = runtime;
    config.calculator = std::move(calculator);
    std::unique_ptr<factor::RealtimeFactorEngineV1> engine;
    test->Expect(
        factor::RealtimeFactorEngineV1::Create(
            std::move(config), &engine) ==
                factor::RealtimeFactorEngineCreateErrorV1::kNone &&
            engine != nullptr,
        "factor engine creates");
    return engine;
}

void CheckProjectionAndWholeGenerationLifetime(TestContext* test) {
    const std::array<market::InstrumentRegistryEntryV1, 2U> entries{
        RegistryEntry(20U, "600020"),
        RegistryEntry(3U, "600003")};
    auto registry = MakeRegistry(test, entries);
    if (registry == nullptr) {
        return;
    }
    auto runtime = MakeRuntime(test, registry.get());
    if (runtime == nullptr) {
        return;
    }
    test->Expect(
        SubmitShanghaiSnapshot(runtime.get(), 3U, 1U, 1U, 12'345'600),
        "valid snapshot reaches store worker");
    auto store1 = PublishStoreGeneration(
        test, runtime.get(), *registry, 1U, 2U, {2U, 1U, 1U, 1U});
    if (store1 == nullptr) {
        return;
    }

    auto calculator =
        std::make_shared<factor::SnapshotLastPriceProjectionV1>();
    auto engine = MakeFactorEngine(test, registry.get(), runtime.get(), calculator);
    if (engine == nullptr) {
        return;
    }
    const factor::RealtimeFactorPublishResultV1 first =
        engine->CalculateAndPublish(store1);
    test->Expect(first.published(), "complete factor generation publishes");
    if (!first.published()) {
        return;
    }
    test->Expect(
        WatermarkExactEqual(
            first.generation->watermark(), store1->watermark()) &&
            first.generation->input_store().get() == store1.get() &&
            !first.generation->input_store().owner_before(store1) &&
            !store1.owner_before(first.generation->input_store()),
        "factor publication retains the exact store watermark and handle");
    test->Expect(
        first.generation->points().size() == 2U &&
            first.generation->points()[0U].instrument_id == 3U &&
            first.generation->points()[1U].instrument_id == 20U,
        "factor rows use the exact ascending fixed universe");
    const auto* observed = first.generation->Find(3U);
    test->Expect(
        observed != nullptr && observed->values.size() == 1U &&
            observed->values[0U].valid &&
            std::abs(observed->values[0U].value - 12.3456) < 1.0e-12,
        "snapshot calculator literally projects normalized p6 price");
    observed = first.generation->Find(20U);
    test->Expect(
        observed != nullptr && !observed->values[0U].valid &&
            observed->values[0U].value == 0.0 &&
            !std::signbit(observed->values[0U].value),
        "missing snapshot is explicit canonical invalid, not NaN");

    const auto old_generation = first.generation;
    auto store2 = PublishStoreGeneration(
        test, runtime.get(), *registry, 2U, 2U, {2U, 1U, 1U, 1U});
    if (store2 == nullptr) {
        return;
    }
    const auto second = engine->CalculateAndPublish(store2);
    test->Expect(
        second.published() &&
            engine->AcquireLatestGeneration().get() ==
                second.generation.get(),
        "one atomic handle replaces the whole factor generation");
    test->Expect(
        old_generation->watermark().generation == 1U &&
            old_generation->Find(3U) != nullptr &&
            old_generation->Find(3U)->values[0U].valid &&
            old_generation.get() != second.generation.get(),
        "reader-held old generation survives a newer publication intact");
}

void CheckDefaultProjectionEconomicDomain(TestContext* test) {
    constexpr std::uint32_t shanghai_instrument = 30U;
    constexpr std::uint32_t shenzhen_instrument = 4U;
    const std::array<market::InstrumentRegistryEntryV1, 2U> entries{
        RegistryEntry(shanghai_instrument, "600030"),
        ShenzhenRegistryEntry(shenzhen_instrument, "000004")};
    auto registry = MakeRegistry(test, entries);
    if (registry == nullptr) {
        return;
    }
    auto runtime = MakeRuntime(test, registry.get());
    if (runtime == nullptr) {
        return;
    }
    test->Expect(
        SubmitShanghaiSnapshot(
            runtime.get(), shanghai_instrument, 1U, 1U, 0),
        "valid zero Shanghai snapshot reaches store");
    test->Expect(
        SubmitShenzhenSnapshot(
            runtime.get(), shenzhen_instrument, 1U, 2U, -1'000'000),
        "valid negative Shenzhen snapshot reaches store");
    auto store = PublishStoreGeneration(
        test, runtime.get(), *registry, 1U, 3U, {2U, 1U, 2U, 1U});
    if (store == nullptr) {
        return;
    }

    market::IntradayInstrumentSummaryV1 shanghai_row{};
    market::IntradayInstrumentSummaryV1 shenzhen_row{};
    test->Expect(
        store->Find(shanghai_instrument, &shanghai_row) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            store->Find(shenzhen_instrument, &shenzhen_row) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone,
        "store summaries expose both snapshot instruments");
    const auto* shanghai_snapshot =
        shanghai_row.latest_snapshot == nullptr
            ? nullptr
            : market::StoredMarketEventGetV1<
                  market::ShanghaiSnapshotV1>(
                  shanghai_row.latest_snapshot->event());
    const auto* shenzhen_snapshot =
        shenzhen_row.latest_snapshot == nullptr
            ? nullptr
            : market::StoredMarketEventGetV1<
                  market::ShenzhenSnapshotV1>(
                  shenzhen_row.latest_snapshot->event());
    test->Expect(
        shanghai_snapshot != nullptr &&
            shanghai_snapshot->last_price.valid &&
            shanghai_snapshot->last_price.normalized_p6 == 0,
        "Shanghai economic-domain input is a present valid zero price");
    test->Expect(
        shenzhen_snapshot != nullptr &&
            shenzhen_snapshot->last_price.valid &&
            shenzhen_snapshot->last_price.normalized_p6 < 0,
        "Shenzhen economic-domain input is a present valid negative price");

    auto calculator =
        std::make_shared<factor::SnapshotLastPriceProjectionV1>();
    auto engine =
        MakeFactorEngine(test, registry.get(), runtime.get(), calculator);
    if (engine == nullptr) {
        return;
    }
    const factor::RealtimeFactorPublishResultV1 result =
        engine->CalculateAndPublish(store);
    test->Expect(
        result.published(),
        "non-positive snapshot domain publishes a complete generation");
    if (!result.published()) {
        return;
    }

    const auto canonical_invalid =
        [](const factor::RealtimeFactorPointV1* point) {
            return point != nullptr && point->values.size() == 1U &&
                   !point->values[0U].valid &&
                   point->values[0U].value == 0.0 &&
                   !std::signbit(point->values[0U].value);
        };
    test->Expect(
        canonical_invalid(result.generation->Find(shanghai_instrument)),
        "zero Shanghai price publishes canonical {+0.0,false}");
    test->Expect(
        canonical_invalid(result.generation->Find(shenzhen_instrument)),
        "negative Shenzhen price publishes canonical {+0.0,false}");
}

void CheckInvalidOutputDenied(TestContext* test) {
    const std::array<market::InstrumentRegistryEntryV1, 2U> entries{
        RegistryEntry(9U, "600009"),
        RegistryEntry(2U, "600002")};
    auto registry = MakeRegistry(test, entries);
    if (registry == nullptr) {
        return;
    }
    auto runtime = MakeRuntime(test, registry.get());
    if (runtime == nullptr) {
        return;
    }
    auto store = PublishStoreGeneration(
        test, runtime.get(), *registry, 1U, 1U, {1U, 1U, 1U, 1U});
    if (store == nullptr) {
        return;
    }

    constexpr std::array<BadOutputMode, 7U> bad_modes{
        BadOutputMode::kMissingRow,
        BadOutputMode::kReversedRows,
        BadOutputMode::kNan,
        BadOutputMode::kInfinity,
        BadOutputMode::kInvalidNonzero,
        BadOutputMode::kInvalidNegativeZero,
        BadOutputMode::kWrongColumnCount};
    for (BadOutputMode mode : bad_modes) {
        auto calculator = std::make_shared<TestCalculator>(mode);
        auto engine =
            MakeFactorEngine(test, registry.get(), runtime.get(), calculator);
        if (engine == nullptr) {
            continue;
        }
        const auto result = engine->CalculateAndPublish(store);
        test->Expect(
            result.error ==
                    factor::RealtimeFactorPublishErrorV1::kInvalidFactorOutput &&
                engine->AcquireLatestGeneration() == nullptr,
            "partial, reordered, nonfinite, wrong-shape, and noncanonical invalid output fail closed");
    }
}

void CheckForgedStoreOwnerDenied(TestContext* test) {
    const std::array<market::InstrumentRegistryEntryV1, 1U> entries{
        RegistryEntry(13U, "600013")};
    auto registry = MakeRegistry(test, entries);
    if (registry == nullptr) {
        return;
    }
    auto runtime = MakeRuntime(test, registry.get());
    if (runtime == nullptr) {
        return;
    }
    auto store = PublishStoreGeneration(
        test, runtime.get(), *registry, 1U, 1U, {1U, 1U, 1U, 1U});
    if (store == nullptr) {
        return;
    }

    const std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        forged_owner(
            store.get(),
            [](const market::IntradayInstrumentStoreGenerationV1*) noexcept {
            });
    test->Expect(
        forged_owner.get() == store.get() &&
            (forged_owner.owner_before(store) ||
             store.owner_before(forged_owner)) &&
            !runtime->IsGenerationCurrentAndHealthy(forged_owner),
        "same raw pointer with a different control block is not the current store owner");
    bool commit_action_invoked = false;
    test->Expect(
        !runtime->CommitIfCurrentAndHealthy(
            forged_owner,
            [](void* context) noexcept {
                *static_cast<bool*>(context) = true;
            },
            &commit_action_invoked) &&
            !commit_action_invoked,
        "commit guard rejects a forged owner without invoking its action");

    auto calculator = std::make_shared<TestCalculator>(BadOutputMode::kGood);
    auto engine =
        MakeFactorEngine(test, registry.get(), runtime.get(), calculator);
    if (engine == nullptr) {
        return;
    }
    const factor::RealtimeFactorPublishResultV1 result =
        engine->CalculateAndPublish(forged_owner);
    test->Expect(
        result.error ==
                factor::RealtimeFactorPublishErrorV1::
                    kStoreNotCurrentOrHealthy &&
            result.generation == nullptr &&
            engine->AcquireLatestGeneration() == nullptr,
        "factor engine rejects a forged shared owner before dereferencing it");
}

void CheckGenerationChangeDuringCalculationDenied(TestContext* test) {
    const std::array<market::InstrumentRegistryEntryV1, 1U> entries{
        RegistryEntry(7U, "600007")};
    auto registry = MakeRegistry(test, entries);
    if (registry == nullptr) {
        return;
    }
    auto runtime = MakeRuntime(test, registry.get());
    if (runtime == nullptr) {
        return;
    }
    auto store1 = PublishStoreGeneration(
        test, runtime.get(), *registry, 1U, 1U, {1U, 1U, 1U, 1U});
    if (store1 == nullptr) {
        return;
    }

    auto calculator = std::make_shared<BlockingCalculator>();
    auto engine = MakeFactorEngine(test, registry.get(), runtime.get(), calculator);
    if (engine == nullptr) {
        return;
    }
    factor::RealtimeFactorPublishResultV1 result{};
    std::thread calculation([&] {
        result = engine->CalculateAndPublish(store1);
    });
    const bool entered = calculator->WaitUntilEntered(std::chrono::seconds(5));
    test->Expect(entered, "blocking calculator entered");
    if (entered) {
        const auto store2 = PublishStoreGeneration(
            test, runtime.get(), *registry, 2U, 1U, {1U, 1U, 1U, 1U});
        test->Expect(
            store2 != nullptr,
            "new store generation supersedes factor input");
    }
    calculator->Release();
    calculation.join();
    test->Expect(
        result.error == factor::RealtimeFactorPublishErrorV1::
                            kStoreNotCurrentOrHealthy &&
            engine->AcquireLatestGeneration() == nullptr,
        "generation change during factor calculation denies commit");
}

void CheckFatalDuringCalculationDenied(TestContext* test) {
    const std::array<market::InstrumentRegistryEntryV1, 1U> entries{
        RegistryEntry(11U, "600011")};
    auto registry = MakeRegistry(test, entries);
    if (registry == nullptr) {
        return;
    }
    auto runtime = MakeRuntime(test, registry.get());
    if (runtime == nullptr) {
        return;
    }
    auto store = PublishStoreGeneration(
        test, runtime.get(), *registry, 1U, 1U, {1U, 1U, 1U, 1U});
    if (store == nullptr) {
        return;
    }

    auto calculator = std::make_shared<BlockingCalculator>();
    auto engine = MakeFactorEngine(test, registry.get(), runtime.get(), calculator);
    if (engine == nullptr) {
        return;
    }
    factor::RealtimeFactorPublishResultV1 result{};
    std::thread calculation([&] {
        result = engine->CalculateAndPublish(store);
    });
    const bool entered = calculator->WaitUntilEntered(std::chrono::seconds(5));
    test->Expect(entered, "fatal test calculator entered");
    if (entered) {
        runtime->MarkFatal();
    }
    calculator->Release();
    calculation.join();
    test->Expect(
        result.error == factor::RealtimeFactorPublishErrorV1::
                            kStoreNotCurrentOrHealthy &&
            engine->AcquireLatestGeneration() == nullptr,
        "store fatal transition during calculation denies commit");
}

void CheckStoreFailureDuringCalculationDenied(TestContext* test) {
    const std::array<market::InstrumentRegistryEntryV1, 1U> entries{
        RegistryEntry(17U, "600017")};
    auto registry = MakeRegistry(test, entries);
    if (registry == nullptr) {
        return;
    }
    auto runtime = MakeRuntime(test, registry.get(), 1U);
    if (runtime == nullptr) {
        return;
    }
    test->Expect(
        SubmitShanghaiSnapshot(runtime.get(), 17U, 1U, 1U, 1'000'000),
        "capacity test first record reaches the store");
    auto store = PublishStoreGeneration(
        test, runtime.get(), *registry, 1U, 2U, {2U, 1U, 1U, 1U});
    if (store == nullptr) {
        return;
    }

    auto calculator = std::make_shared<BlockingCalculator>();
    auto engine =
        MakeFactorEngine(test, registry.get(), runtime.get(), calculator);
    if (engine == nullptr) {
        return;
    }
    factor::RealtimeFactorPublishResultV1 result{};
    std::thread calculation([&] {
        result = engine->CalculateAndPublish(store);
    });
    const bool entered = calculator->WaitUntilEntered(std::chrono::seconds(5));
    test->Expect(entered, "store-failure test calculator entered");
    if (entered) {
        test->Expect(
            SubmitShanghaiSnapshot(
                runtime.get(), 17U, 2U, 2U, 2'000'000),
            "post-cut record is admitted before the store capacity failure");
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!runtime->StoreSnapshot().coverage_lost &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        test->Expect(
            runtime->StoreSnapshot().coverage_lost,
            "post-cut append makes store coverage loss observable");
    }
    calculator->Release();
    calculation.join();
    test->Expect(
        result.error == factor::RealtimeFactorPublishErrorV1::
                            kStoreNotCurrentOrHealthy &&
            engine->AcquireLatestGeneration() == nullptr,
        "observable post-cut store failure denies the pending factor commit");
}

}  // namespace

int main() {
    TestContext test;
    CheckProjectionAndWholeGenerationLifetime(&test);
    CheckDefaultProjectionEconomicDomain(&test);
    CheckInvalidOutputDenied(&test);
    CheckForgedStoreOwnerDenied(&test);
    CheckGenerationChangeDuringCalculationDenied(&test);
    CheckFatalDuringCalculationDenied(&test);
    CheckStoreFailureDuringCalculationDenied(&test);
    if (test.failures != 0) {
        std::cerr << test.failures << " realtime factor test(s) failed\n";
        return 1;
    }
    std::cout << "realtime factor engine tests passed\n";
    return 0;
}
