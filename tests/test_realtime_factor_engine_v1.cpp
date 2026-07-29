#include "l2flow/factor/realtime_factor_engine_v1.h"

#include "l2flow/common/identity128.h"
#include "l2flow/market/market_types_v1.h"
#include "l2flow/market/observed_instrument_directory_v2.h"
#include "l2flow/market/realtime_history_v1.h"

#include <algorithm>
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

using namespace std::chrono_literals;

constexpr std::array<std::uint32_t,
                     market::kRealtimeHistorySourceCountV1>
    kSourceStreamIds{{101U, 102U, 103U, 104U}};
constexpr std::uint32_t kTradeDate = 20260729U;

struct TestContext final {
    int failures = 0;

    void Expect(bool condition, std::string_view message) {
        if (condition) {
            return;
        }
        ++failures;
        std::cerr << "FAIL: " << message << '\n';
    }
};

[[nodiscard]] std::vector<std::byte> Bytes(std::string_view text) {
    const std::span<const char> characters(text.data(), text.size());
    const std::span<const std::byte> bytes =
        std::as_bytes(characters);
    return {bytes.begin(), bytes.end()};
}

[[nodiscard]] market::InstrumentKeyV1 InstrumentKey(
    market::MarketV1 venue,
    std::string_view security_id_source,
    std::string_view security_id) {
    market::InstrumentKeyV1 result{};
    result.market = venue;
    result.security_id_source = Bytes(security_id_source);
    result.security_id = Bytes(security_id);
    return result;
}

[[nodiscard]] market::ObservedInstrumentMetadataV2 EquityMetadata() {
    return {
        market::QuantityUnitV1::kShare,
        market::SecurityTypeV1::kEquity,
        market::AssetScopeV1::kDocumentedCore};
}

[[nodiscard]] std::unique_ptr<
    market::ObservedInstrumentDirectoryV2>
MakeDirectory(
    TestContext* test,
    std::size_t capacity,
    std::uint64_t session_epoch) {
    market::ObservedInstrumentDirectoryConfigV2 config{};
    config.capacity = capacity;
    config.session_epoch = session_epoch;
    std::unique_ptr<market::ObservedInstrumentDirectoryV2> result;
    const auto error =
        market::ObservedInstrumentDirectoryV2::Create(
            config, &result);
    test->Expect(
        error ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            result != nullptr,
        "observed instrument directory creates");
    return result;
}

[[nodiscard]] std::uint32_t BindInstrument(
    TestContext* test,
    market::ObservedInstrumentDirectoryV2* directory,
    const market::InstrumentKeyV1& key,
    std::uint64_t capture_sequence) {
    market::ObservedInstrumentBindResultV2 result{};
    const auto error = directory == nullptr
                           ? market::ObservedInstrumentDirectoryErrorV2::
                                 kNullOutput
                           : directory->BindOrGet(
                                 key,
                                 EquityMetadata(),
                                 capture_sequence,
                                 &result);
    test->Expect(
        error ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            result.newly_bound && result.entry.bound() &&
            result.entry.instrument_id ==
                static_cast<std::uint32_t>(
                    result.entry.ordinal + 1U),
        "observed instrument binds to its capture-ordered ID");
    return error ==
                       market::ObservedInstrumentDirectoryErrorV2::
                           kNone
               ? result.entry.instrument_id
               : 0U;
}

[[nodiscard]] std::unique_ptr<market::RealtimeHistoryRuntimeV1>
MakeRuntime(
    TestContext* test,
    market::ObservedInstrumentDirectoryV2* directory,
    std::uint64_t maximum_session_records = 1024U) {
    market::RealtimeHistoryRuntimeConfigV1 config{};
    config.source_stream_ids = kSourceStreamIds;
    config.worker_count = 2U;
    config.queue_capacity_per_source_worker = 64U;
    config.directory = directory;
    config.intraday_store.segment_target_bytes =
        market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
    config.intraday_store.maximum_session_records =
        maximum_session_records;
    config.intraday_store.maximum_session_accounted_bytes = 1U << 30U;
    config.intraday_store.maximum_records_per_batch = 64U;
    config.intraday_store.coverage_from_open = true;
    std::unique_ptr<market::RealtimeHistoryRuntimeV1> result;
    const auto error =
        market::RealtimeHistoryRuntimeV1::Create(config, &result);
    test->Expect(
        error == market::RealtimeHistoryCreateErrorV1::kNone &&
            result != nullptr,
        "observed-universe history runtime creates");
    return result;
}

[[nodiscard]] l2flow::common::Identity128 RunId() {
    l2flow::common::Identity128 result{};
    for (std::size_t index = 0U; index < result.size(); ++index) {
        result[index] = static_cast<std::byte>(index + 1U);
    }
    return result;
}

void FillCommon(
    const market::ObservedInstrumentDirectoryV2& directory,
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
    market::ObservedInstrumentEntryViewV2 identity{};
    if (directory.LookupById(instrument_id, &identity) ==
            market::ObservedInstrumentDirectoryErrorV2::kNone &&
        identity.bound()) {
        common->ordinal = identity.ordinal;
    }
}

struct SnapshotPrice final {
    std::int64_t normalized_p6 = 0;
    bool valid = false;
    bool is_null = false;
};

[[nodiscard]] bool SubmitSnapshot(
    market::RealtimeHistoryRuntimeV1* runtime,
    const market::ObservedInstrumentDirectoryV2& directory,
    market::MarketV1 venue,
    std::uint32_t instrument_id,
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence,
    SnapshotPrice price) {
    const std::uint8_t source_slot =
        venue == market::MarketV1::kShanghai ? 0U : 2U;
    const auto fill = [&](auto* event) {
        FillCommon(
            directory,
            &event->common,
            venue == market::MarketV1::kShanghai
                ? market::MarketEventKindV1::kShanghaiSnapshot
                : market::MarketEventKindV1::kShenzhenSnapshot,
            venue,
            source_slot,
            source_sequence,
            ingress_sequence,
            instrument_id);
        event->last_price.raw = price.normalized_p6;
        event->last_price.normalized_p6 = price.normalized_p6;
        event->last_price.scale = 6U;
        event->last_price.valid = price.valid;
        event->last_price.is_null = price.is_null;
    };
    market::DecodedMarketEventV1 decoded =
        [&]() -> market::DecodedMarketEventV1 {
        if (venue == market::MarketV1::kShanghai) {
            market::ShanghaiSnapshotV1 event{};
            fill(&event);
            return market::DecodedMarketEventV1(std::move(event));
        }
        market::ShenzhenSnapshotV1 event{};
        fill(&event);
        return market::DecodedMarketEventV1(std::move(event));
    }();
    auto input = market::RealtimeHistoryEventInputV1::Create(
        source_slot, ingress_sequence, std::move(decoded));
    return runtime != nullptr && input.has_value() &&
           runtime->TrySubmit(std::move(*input)) ==
               market::RealtimeHistorySubmitErrorV1::kNone;
}

[[nodiscard]] bool SubmitShanghaiTick(
    market::RealtimeHistoryRuntimeV1* runtime,
    const market::ObservedInstrumentDirectoryV2& directory,
    std::uint32_t instrument_id,
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence,
    std::uint64_t tick_stream_sequence) {
    market::ShanghaiTickV1 tick{};
    FillCommon(
        directory,
        &tick.common,
        market::MarketEventKindV1::kShanghaiTick,
        market::MarketV1::kShanghai,
        1U,
        source_sequence,
        ingress_sequence,
        instrument_id);
    tick.fields.action = market::TickActionV1::kStatus;
    market::DecodedMarketEventV1 decoded(std::move(tick));
    auto input = market::RealtimeHistoryEventInputV1::Create(
        1U,
        ingress_sequence,
        std::move(decoded),
        tick_stream_sequence);
    return runtime != nullptr && input.has_value() &&
           runtime->TrySubmit(std::move(*input)) ==
               market::RealtimeHistorySubmitErrorV1::kNone;
}

[[nodiscard]] bool WaitUntilApplied(
    const market::ObservedInstrumentDirectoryV2& directory,
    std::uint32_t instrument_id,
    std::uint64_t ingress_sequence,
    bool factor_eligible,
    bool has_tick = false,
    std::chrono::seconds timeout = 5s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    do {
        market::ObservedInstrumentEntryViewV2 entry{};
        if (directory.LookupById(instrument_id, &entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            entry.last_ingress_sequence >= ingress_sequence &&
            entry.has_snapshot &&
            entry.has_tick == has_tick &&
            entry.factor_eligible == factor_eligible) {
            return true;
        }
        std::this_thread::yield();
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
}

[[nodiscard]] std::shared_ptr<
    const market::IntradayInstrumentStoreGenerationV1>
PublishStoreGeneration(
    TestContext* test,
    market::RealtimeHistoryRuntimeV1* runtime,
    const market::ObservedInstrumentDirectoryV2& directory,
    std::uint64_t generation,
    std::array<std::uint64_t,
               market::kRealtimeHistorySourceCountV1>
        source_sequence_exclusive,
    std::uint64_t durable_sequence) {
    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2>
        catalog_snapshot;
    test->Expect(
        directory.AcquireSnapshot(&catalog_snapshot) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            catalog_snapshot != nullptr,
        "exact observed CatalogSnapshot freezes");
    if (catalog_snapshot == nullptr || runtime == nullptr) {
        return nullptr;
    }

    std::array<market::RealtimeSourceWatermarkV1,
               market::kRealtimeHistorySourceCountV1>
        source_watermarks{};
    std::uint64_t ingress_sequence_exclusive = 1U;
    for (std::size_t source = 0U;
         source < source_watermarks.size();
         ++source) {
        source_watermarks[source].source_stream_id =
            kSourceStreamIds[source];
        source_watermarks[source].sequence_exclusive =
            source_sequence_exclusive[source];
        ingress_sequence_exclusive +=
            source_sequence_exclusive[source] - 1U;
    }
    const std::uint64_t applied_sequence =
        ingress_sequence_exclusive - 1U;
    l2flow::realtime::ProcessingProgressV2 progress{};
    progress.applied_sequence = applied_sequence;
    progress.durable_sequence =
        std::max(durable_sequence, applied_sequence);
    progress.accepted_sequence = progress.durable_sequence;

    market::RealtimeHistoryWatermarkV1 watermark{};
    test->Expect(
        market::BuildRealtimeHistoryWatermarkV1(
            RunId(),
            generation,
            kTradeDate,
            ingress_sequence_exclusive,
            1000U + generation,
            catalog_snapshot,
            progress,
            source_watermarks,
            &watermark) ==
            market::RealtimeHistoryWatermarkErrorV1::kNone,
        "observed store watermark builds");
    if (watermark.generation == 0U) {
        return nullptr;
    }
    test->Expect(
        runtime->BeginGeneration(watermark) ==
            market::RealtimeHistoryGenerationErrorV1::kNone,
        "observed store generation begins");
    for (std::uint8_t source = 0U;
         source < market::kRealtimeHistorySourceCountV1;
         ++source) {
        test->Expect(
            runtime->SealSource(source, generation) ==
                market::RealtimeHistoryGenerationErrorV1::kNone,
            "observed store source fence seals");
    }

    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        result;
    test->Expect(
        runtime->WaitForGeneration(generation, 5s, &result) ==
                market::RealtimeHistoryGenerationErrorV1::kNone &&
            result != nullptr,
        "complete observed store generation publishes");
    return result;
}

template <typename Value>
[[nodiscard]] bool SameSharedOwnerAndPointer(
    const std::shared_ptr<const Value>& left,
    const std::shared_ptr<const Value>& right) noexcept {
    return left.get() == right.get() &&
           !left.owner_before(right) &&
           !right.owner_before(left);
}

[[nodiscard]] bool WatermarkExactEqual(
    const market::RealtimeHistoryWatermarkV1& left,
    const market::RealtimeHistoryWatermarkV1& right) {
    if (left.run_id != right.run_id ||
        left.generation != right.generation ||
        left.trade_date != right.trade_date ||
        left.ingress_sequence_exclusive !=
            right.ingress_sequence_exclusive ||
        left.recv_monotonic_cut_ns != right.recv_monotonic_cut_ns ||
        !SameSharedOwnerAndPointer(
            left.catalog_snapshot, right.catalog_snapshot) ||
        left.processing_progress.accepted_sequence !=
            right.processing_progress.accepted_sequence ||
        left.processing_progress.durable_sequence !=
            right.processing_progress.durable_sequence ||
        left.processing_progress.applied_sequence !=
            right.processing_progress.applied_sequence ||
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

[[nodiscard]] std::vector<std::uint32_t> EligibleIds(
    const market::IntradayInstrumentStoreGenerationV1& store) {
    std::vector<std::uint32_t> result;
    const auto& catalog = store.catalog_snapshot();
    if (catalog == nullptr) {
        return result;
    }
    result.reserve(catalog->factor_eligible_count());
    for (std::size_t ordinal = 0U;
         ordinal < catalog->bound_count();
         ++ordinal) {
        market::ObservedInstrumentEntryViewV2 entry{};
        if (catalog->EntryAt(ordinal, &entry) !=
            market::ObservedInstrumentDirectoryErrorV2::kNone) {
            return {};
        }
        if (entry.factor_eligible) {
            result.push_back(entry.instrument_id);
        }
    }
    return result;
}

enum class BadOutputMode : std::uint8_t {
    kGood = 0U,
    kMissingRow,
    kExtraBoundRow,
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
            const std::vector<std::uint32_t> eligible =
                EligibleIds(store);
            std::vector<factor::RealtimeFactorPointV1> points;
            points.reserve(eligible.size() + 1U);
            for (const std::uint32_t instrument_id : eligible) {
                factor::RealtimeFactorPointV1 point{};
                point.instrument_id = instrument_id;
                point.values.push_back(
                    factor::RealtimeFactorValueV1{
                        static_cast<double>(instrument_id), true});
                points.push_back(std::move(point));
            }
            if (mode_ == BadOutputMode::kMissingRow &&
                !points.empty()) {
                points.pop_back();
            } else if (mode_ == BadOutputMode::kExtraBoundRow) {
                factor::RealtimeFactorPointV1 extra{};
                extra.instrument_id = 2U;
                extra.values.push_back(
                    factor::RealtimeFactorValueV1{2.0, true});
                points.insert(
                    points.begin() +
                        static_cast<std::ptrdiff_t>(
                            std::min<std::size_t>(
                                1U, points.size())),
                    std::move(extra));
            } else if (mode_ == BadOutputMode::kReversedRows &&
                       points.size() > 1U) {
                std::swap(points.front(), points.back());
            } else if (mode_ == BadOutputMode::kNan &&
                       !points.empty()) {
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
        {"test_projection",
         "v2",
         "test-only factor-eligible instrument-id projection"}}};
    BadOutputMode mode_ = BadOutputMode::kGood;
};

class BlockingCalculator final
    : public factor::RealtimeFactorCalculatorV1 {
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
            for (const std::uint32_t instrument_id :
                 EligibleIds(store)) {
                factor::RealtimeFactorPointV1 point{};
                point.instrument_id = instrument_id;
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

    [[nodiscard]] bool WaitUntilEntered(
        std::chrono::seconds timeout) const {
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
        {"blocking_test_projection",
         "v2",
         "test-only factor-eligible constant projection"}}};
    mutable std::mutex mutex_;
    mutable std::condition_variable condition_;
    mutable bool entered_ = false;
    mutable bool released_ = false;
};

[[nodiscard]] std::unique_ptr<factor::RealtimeFactorEngineV1>
MakeFactorEngine(
    TestContext* test,
    const market::RealtimeHistoryRuntimeV1* runtime,
    std::shared_ptr<const factor::RealtimeFactorCalculatorV1>
        calculator) {
    factor::RealtimeFactorEngineConfigV1 config{};
    config.generation_runtime = runtime;
    config.calculator = std::move(calculator);
    std::unique_ptr<factor::RealtimeFactorEngineV1> result;
    test->Expect(
        factor::RealtimeFactorEngineV1::Create(
            std::move(config), &result) ==
                factor::RealtimeFactorEngineCreateErrorV1::kNone &&
            result != nullptr,
        "observed-universe factor engine creates");
    return result;
}

void CheckEligibilityCountsProjectionAndLifetime(TestContext* test) {
    auto directory = MakeDirectory(test, 8U, 71U);
    if (directory == nullptr) {
        return;
    }
    std::array<std::uint32_t, 5U> ids{};
    for (std::size_t index = 0U; index < ids.size(); ++index) {
        const std::string_view security_id =
            std::array<std::string_view, 5U>{{
                "600001", "600002", "600003", "600004", "600005"}}[
                index];
        ids[index] = BindInstrument(
            test,
            directory.get(),
            InstrumentKey(
                market::MarketV1::kShanghai,
                "101",
                security_id),
            static_cast<std::uint64_t>(index + 1U));
    }
    auto runtime = MakeRuntime(test, directory.get());
    if (runtime == nullptr) {
        return;
    }

    const std::array<SnapshotPrice, 5U> prices{{
        {1'000'000, false, false},
        {1'000'000, true, true},
        {0, true, false},
        {-1'000'000, true, false},
        {12'345'600, true, false},
    }};
    for (std::size_t index = 0U; index < ids.size(); ++index) {
        test->Expect(
            SubmitSnapshot(
                runtime.get(),
                *directory,
                market::MarketV1::kShanghai,
                ids[index],
                static_cast<std::uint64_t>(index + 1U),
                static_cast<std::uint64_t>(index + 1U),
                prices[index]),
            "eligibility-domain snapshot reaches Store");
    }
    test->Expect(
        SubmitShanghaiTick(
            runtime.get(), *directory, ids[0U], 1U, 6U, 1U),
        "tick availability input reaches Store");
    for (std::size_t index = 0U; index < ids.size(); ++index) {
        const std::uint64_t expected_ingress =
            index == 0U ? 6U
                        : static_cast<std::uint64_t>(index + 1U);
        test->Expect(
            WaitUntilApplied(
                *directory,
                ids[index],
                expected_ingress,
                index == 4U,
                index == 0U),
            "directory observes each fully applied input");
    }

    auto store1 = PublishStoreGeneration(
        test,
        runtime.get(),
        *directory,
        1U,
        {6U, 2U, 1U, 1U},
        10U);
    if (store1 == nullptr) {
        return;
    }
    auto calculator =
        std::make_shared<factor::SnapshotLastPriceProjectionV1>();
    auto engine =
        MakeFactorEngine(test, runtime.get(), calculator);
    if (engine == nullptr) {
        return;
    }
    const auto first = engine->CalculateAndPublish(store1);
    test->Expect(
        first.published(),
        "eligible-only factor generation publishes");
    if (!first.published()) {
        return;
    }

    const auto& catalog1 = store1->catalog_snapshot();
    test->Expect(
        WatermarkExactEqual(
            first.generation->watermark(), store1->watermark()) &&
            SameSharedOwnerAndPointer(
                first.generation->input_store(), store1) &&
            SameSharedOwnerAndPointer(
                first.generation->catalog_snapshot(), catalog1),
        "factor generation retains the exact Store and CatalogSnapshot");
    test->Expect(
        first.generation->catalog_scope() ==
                market::ObservedInstrumentCatalogScopeV2::
                    kObservedOnly &&
            first.generation->catalog_generation() == 5U &&
            first.generation->catalog_digest() ==
                catalog1->catalog_digest() &&
            first.generation->bound_count() == 5U &&
            first.generation->available_count() == 5U &&
            first.generation->snapshot_available_count() == 5U &&
            first.generation->tick_available_count() == 1U &&
            first.generation->factor_eligible_count() == 1U &&
            first.generation->capture_accepted_sequence() == 10U &&
            first.generation->capture_durable_sequence() == 10U &&
            first.generation->input_applied_sequence() == 6U &&
            first.generation->processing_lag_records() == 4U &&
            first.generation->durability_lag_records() == 0U,
        "factor envelope exposes exact observed counts, identity, and "
        "processing lag");
    test->Expect(
        first.generation->points().size() ==
                first.generation->factor_eligible_count() &&
            first.generation->points()[0U].instrument_id == ids[4U] &&
            first.generation->Find(ids[0U]) == nullptr &&
            first.generation->Find(ids[1U]) == nullptr &&
            first.generation->Find(ids[2U]) == nullptr &&
            first.generation->Find(ids[3U]) == nullptr,
        "invalid, null, zero, and negative latest prices produce no "
        "factor row");
    const factor::RealtimeFactorPointV1* positive =
        first.generation->Find(ids[4U]);
    test->Expect(
        positive != nullptr && positive->values.size() == 1U &&
            positive->values[0U].valid &&
            std::abs(positive->values[0U].value - 12.3456) <
                1.0e-12,
        "positive normalized p6 price is divided by exactly one "
        "million");

    test->Expect(
        SubmitSnapshot(
            runtime.get(),
            *directory,
            market::MarketV1::kShanghai,
            ids[3U],
            6U,
            7U,
            SnapshotPrice{2'500'000, true, false}) &&
            WaitUntilApplied(*directory, ids[3U], 7U, true),
        "new positive latest snapshot reverses eligibility");
    auto store2 = PublishStoreGeneration(
        test,
        runtime.get(),
        *directory,
        2U,
        {7U, 2U, 1U, 1U},
        7U);
    if (store2 == nullptr) {
        return;
    }
    const auto second = engine->CalculateAndPublish(store2);
    test->Expect(
        second.published() &&
            second.generation->points().size() == 2U &&
            second.generation->factor_eligible_count() == 2U &&
            second.generation->points()[0U].instrument_id == ids[3U] &&
            second.generation->points()[1U].instrument_id == ids[4U],
        "new generation emits exactly eligible IDs in ascending order");
    test->Expect(
        first.generation->factor_eligible_count() == 1U &&
            first.generation->points().size() == 1U &&
            first.generation->Find(ids[3U]) == nullptr &&
            engine->AcquireLatestGeneration().get() ==
                second.generation.get(),
        "reader-held old CatalogSnapshot and factor generation stay "
        "immutable after eligibility changes");
}

void CheckEmptyBoundAndEmptyEligibleGenerations(TestContext* test) {
    auto directory = MakeDirectory(test, 4U, 72U);
    auto runtime = MakeRuntime(test, directory.get());
    if (directory == nullptr || runtime == nullptr) {
        return;
    }
    auto calculator =
        std::make_shared<factor::SnapshotLastPriceProjectionV1>();
    auto engine =
        MakeFactorEngine(test, runtime.get(), calculator);
    if (engine == nullptr) {
        return;
    }

    auto empty_store = PublishStoreGeneration(
        test,
        runtime.get(),
        *directory,
        1U,
        {1U, 1U, 1U, 1U},
        0U);
    if (empty_store == nullptr) {
        return;
    }
    const auto empty = engine->CalculateAndPublish(empty_store);
    test->Expect(
        empty.published() && empty.generation->bound_count() == 0U &&
            empty.generation->available_count() == 0U &&
            empty.generation->factor_eligible_count() == 0U &&
            empty.generation->points().empty() &&
            empty.generation->input_applied_sequence() == 0U &&
            empty.generation->capture_durable_sequence() == 0U,
        "an empty observed catalog is a legal factor generation");

    const std::uint32_t first = BindInstrument(
        test,
        directory.get(),
        InstrumentKey(
            market::MarketV1::kShanghai, "101", "601001"),
        1U);
    const std::uint32_t second = BindInstrument(
        test,
        directory.get(),
        InstrumentKey(
            market::MarketV1::kShenzhen, "102 ", "001002"),
        2U);
    auto bound_no_data_store = PublishStoreGeneration(
        test,
        runtime.get(),
        *directory,
        2U,
        {1U, 1U, 1U, 1U},
        0U);
    if (bound_no_data_store == nullptr) {
        return;
    }
    const auto bound_no_data =
        engine->CalculateAndPublish(bound_no_data_store);
    test->Expect(
        first == 1U && second == 2U &&
            bound_no_data.published() &&
            bound_no_data.generation->catalog_generation() == 2U &&
            bound_no_data.generation->bound_count() == 2U &&
            bound_no_data.generation->available_count() == 0U &&
            bound_no_data.generation->snapshot_available_count() ==
                0U &&
            bound_no_data.generation->tick_available_count() == 0U &&
            bound_no_data.generation->factor_eligible_count() == 0U &&
            bound_no_data.generation->points().empty() &&
            bound_no_data.generation->Find(first) == nullptr &&
            bound_no_data.generation->Find(second) == nullptr,
        "bound instruments with no eligible snapshot legally publish "
        "an empty factor batch");
}

void CheckInvalidCalculatorOutputDenied(TestContext* test) {
    auto directory = MakeDirectory(test, 4U, 73U);
    if (directory == nullptr) {
        return;
    }
    const std::uint32_t first = BindInstrument(
        test,
        directory.get(),
        InstrumentKey(
            market::MarketV1::kShanghai, "101", "602001"),
        1U);
    const std::uint32_t ineligible = BindInstrument(
        test,
        directory.get(),
        InstrumentKey(
            market::MarketV1::kShanghai, "101", "602002"),
        2U);
    const std::uint32_t third = BindInstrument(
        test,
        directory.get(),
        InstrumentKey(
            market::MarketV1::kShanghai, "101", "602003"),
        3U);
    auto runtime = MakeRuntime(test, directory.get());
    if (runtime == nullptr) {
        return;
    }
    test->Expect(
        SubmitSnapshot(
            runtime.get(),
            *directory,
            market::MarketV1::kShanghai,
            first,
            1U,
            1U,
            SnapshotPrice{1'000'000, true, false}) &&
            SubmitSnapshot(
                runtime.get(),
                *directory,
                market::MarketV1::kShanghai,
                third,
                2U,
                2U,
                SnapshotPrice{3'000'000, true, false}) &&
            WaitUntilApplied(*directory, first, 1U, true) &&
            WaitUntilApplied(*directory, third, 2U, true),
        "two eligible calculator-validation inputs apply");
    auto store = PublishStoreGeneration(
        test,
        runtime.get(),
        *directory,
        1U,
        {3U, 1U, 1U, 1U},
        2U);
    if (store == nullptr) {
        return;
    }
    test->Expect(
        first == 1U && ineligible == 2U && third == 3U &&
            store->catalog_snapshot()->factor_eligible_count() == 2U,
        "calculator validation fixture has a sparse eligible subset");

    constexpr std::array<BadOutputMode, 8U> bad_modes{{
        BadOutputMode::kMissingRow,
        BadOutputMode::kExtraBoundRow,
        BadOutputMode::kReversedRows,
        BadOutputMode::kNan,
        BadOutputMode::kInfinity,
        BadOutputMode::kInvalidNonzero,
        BadOutputMode::kInvalidNegativeZero,
        BadOutputMode::kWrongColumnCount,
    }};
    for (const BadOutputMode mode : bad_modes) {
        auto calculator = std::make_shared<TestCalculator>(mode);
        auto engine =
            MakeFactorEngine(test, runtime.get(), calculator);
        if (engine == nullptr) {
            continue;
        }
        const auto result = engine->CalculateAndPublish(store);
        test->Expect(
            result.error ==
                    factor::RealtimeFactorPublishErrorV1::
                        kInvalidFactorOutput &&
                result.generation == nullptr &&
                engine->AcquireLatestGeneration() == nullptr,
            "missing, extra-bound, reordered, nonfinite, wrong-shape, "
            "and noncanonical output fail closed");
    }

    auto good_calculator =
        std::make_shared<TestCalculator>(BadOutputMode::kGood);
    auto good_engine =
        MakeFactorEngine(test, runtime.get(), good_calculator);
    if (good_engine != nullptr) {
        const auto good = good_engine->CalculateAndPublish(store);
        test->Expect(
            good.published() &&
                good.generation->points().size() == 2U &&
                good.generation->points()[0U].instrument_id == first &&
                good.generation->points()[1U].instrument_id == third,
            "calculator may publish exactly the sparse eligible IDs");
    }
}

void CheckForgedStoreOwnerDenied(TestContext* test) {
    auto directory = MakeDirectory(test, 2U, 74U);
    auto runtime = MakeRuntime(test, directory.get());
    if (directory == nullptr || runtime == nullptr) {
        return;
    }
    auto store = PublishStoreGeneration(
        test,
        runtime.get(),
        *directory,
        1U,
        {1U, 1U, 1U, 1U},
        0U);
    if (store == nullptr) {
        return;
    }

    const std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        forged(
            store.get(),
            [](const market::IntradayInstrumentStoreGenerationV1*)
                noexcept {});
    test->Expect(
        forged.get() == store.get() &&
            (forged.owner_before(store) ||
             store.owner_before(forged)) &&
            !runtime->IsGenerationCurrentAndHealthy(forged),
        "same raw Store pointer with a forged owner is not current");

    auto calculator =
        std::make_shared<TestCalculator>(BadOutputMode::kGood);
    auto engine =
        MakeFactorEngine(test, runtime.get(), calculator);
    if (engine == nullptr) {
        return;
    }
    const auto result = engine->CalculateAndPublish(forged);
    test->Expect(
        result.error ==
                factor::RealtimeFactorPublishErrorV1::
                    kStoreNotCurrentOrHealthy &&
            result.generation == nullptr &&
            engine->AcquireLatestGeneration() == nullptr,
        "factor engine rejects a forged Store owner before use");
}

void CheckGenerationChangeDuringCalculationDenied(
    TestContext* test) {
    auto directory = MakeDirectory(test, 2U, 75U);
    auto runtime = MakeRuntime(test, directory.get());
    if (directory == nullptr || runtime == nullptr) {
        return;
    }
    auto store1 = PublishStoreGeneration(
        test,
        runtime.get(),
        *directory,
        1U,
        {1U, 1U, 1U, 1U},
        0U);
    if (store1 == nullptr) {
        return;
    }

    auto calculator = std::make_shared<BlockingCalculator>();
    auto engine =
        MakeFactorEngine(test, runtime.get(), calculator);
    if (engine == nullptr) {
        return;
    }
    factor::RealtimeFactorPublishResultV1 result{};
    std::thread calculation([&] {
        result = engine->CalculateAndPublish(store1);
    });
    const bool entered = calculator->WaitUntilEntered(5s);
    test->Expect(entered, "blocking observed calculator enters");
    if (entered) {
        const auto store2 = PublishStoreGeneration(
            test,
            runtime.get(),
            *directory,
            2U,
            {1U, 1U, 1U, 1U},
            0U);
        test->Expect(
            store2 != nullptr,
            "new exact Store generation supersedes factor input");
    }
    calculator->Release();
    calculation.join();
    test->Expect(
        result.error ==
                factor::RealtimeFactorPublishErrorV1::
                    kStoreNotCurrentOrHealthy &&
            result.generation == nullptr &&
            engine->AcquireLatestGeneration() == nullptr,
        "Store generation change during calculation denies commit");
}

void CheckFatalAndStoreFailureDuringCalculationDenied(
    TestContext* test) {
    {
        auto directory = MakeDirectory(test, 2U, 76U);
        auto runtime = MakeRuntime(test, directory.get());
        if (directory == nullptr || runtime == nullptr) {
            return;
        }
        auto store = PublishStoreGeneration(
            test,
            runtime.get(),
            *directory,
            1U,
            {1U, 1U, 1U, 1U},
            0U);
        if (store == nullptr) {
            return;
        }
        auto calculator = std::make_shared<BlockingCalculator>();
        auto engine =
            MakeFactorEngine(test, runtime.get(), calculator);
        if (engine == nullptr) {
            return;
        }
        factor::RealtimeFactorPublishResultV1 result{};
        std::thread calculation([&] {
            result = engine->CalculateAndPublish(store);
        });
        const bool entered = calculator->WaitUntilEntered(5s);
        test->Expect(entered, "fatal test calculator enters");
        if (entered) {
            runtime->MarkFatal();
        }
        calculator->Release();
        calculation.join();
        test->Expect(
            result.error ==
                    factor::RealtimeFactorPublishErrorV1::
                        kStoreNotCurrentOrHealthy &&
                engine->AcquireLatestGeneration() == nullptr,
            "fatal transition during calculation denies commit");
    }

    {
        auto directory = MakeDirectory(test, 2U, 77U);
        if (directory == nullptr) {
            return;
        }
        const std::uint32_t instrument_id = BindInstrument(
            test,
            directory.get(),
            InstrumentKey(
                market::MarketV1::kShanghai,
                "101",
                "603001"),
            1U);
        auto runtime = MakeRuntime(test, directory.get(), 1U);
        if (runtime == nullptr) {
            return;
        }
        test->Expect(
            SubmitSnapshot(
                runtime.get(),
                *directory,
                market::MarketV1::kShanghai,
                instrument_id,
                1U,
                1U,
                SnapshotPrice{1'000'000, true, false}) &&
                WaitUntilApplied(
                    *directory, instrument_id, 1U, true),
            "capacity fixture first eligible snapshot applies");
        auto store = PublishStoreGeneration(
            test,
            runtime.get(),
            *directory,
            1U,
            {2U, 1U, 1U, 1U},
            1U);
        if (store == nullptr) {
            return;
        }
        auto calculator = std::make_shared<BlockingCalculator>();
        auto engine =
            MakeFactorEngine(test, runtime.get(), calculator);
        if (engine == nullptr) {
            return;
        }
        factor::RealtimeFactorPublishResultV1 result{};
        std::thread calculation([&] {
            result = engine->CalculateAndPublish(store);
        });
        const bool entered = calculator->WaitUntilEntered(5s);
        test->Expect(
            entered, "Store-failure test calculator enters");
        if (entered) {
            test->Expect(
                SubmitSnapshot(
                    runtime.get(),
                    *directory,
                    market::MarketV1::kShanghai,
                    instrument_id,
                    2U,
                    2U,
                    SnapshotPrice{2'000'000, true, false}),
                "post-cut record is admitted before Store capacity "
                "failure");
            const auto deadline =
                std::chrono::steady_clock::now() + 5s;
            while (!runtime->StoreSnapshot().coverage_lost &&
                   std::chrono::steady_clock::now() < deadline) {
                std::this_thread::yield();
            }
            test->Expect(
                runtime->StoreSnapshot().coverage_lost,
                "post-cut Store capacity failure becomes observable");
        }
        calculator->Release();
        calculation.join();
        test->Expect(
            result.error ==
                    factor::RealtimeFactorPublishErrorV1::
                        kStoreNotCurrentOrHealthy &&
                engine->AcquireLatestGeneration() == nullptr,
            "Store failure during calculation denies factor commit");
    }
}

}  // namespace

int main() {
    TestContext test;
    CheckEligibilityCountsProjectionAndLifetime(&test);
    CheckEmptyBoundAndEmptyEligibleGenerations(&test);
    CheckInvalidCalculatorOutputDenied(&test);
    CheckForgedStoreOwnerDenied(&test);
    CheckGenerationChangeDuringCalculationDenied(&test);
    CheckFatalAndStoreFailureDuringCalculationDenied(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " realtime factor test(s) failed\n";
        return 1;
    }
    std::cout << "realtime factor engine tests passed\n";
    return 0;
}
