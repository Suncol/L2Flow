#include "l2flow/market/daily_instrument_catalog_v2.h"
#include "l2flow/market/instrument_runtime_state_v2.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <span>
#include <string_view>
#include <thread>
#include <vector>

namespace {

namespace market = l2flow::market;

struct Test final {
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
    const std::span<const std::byte> bytes =
        std::as_bytes(characters);
    return {bytes.begin(), bytes.end()};
}

market::DailyInstrumentSourceEntryV2 Entry(
    market::MarketV1 venue,
    std::string_view source,
    std::string_view security_id) {
    market::DailyInstrumentSourceEntryV2 result{};
    result.key.market = venue;
    result.key.security_id_source = Bytes(source);
    result.key.security_id = Bytes(security_id);
    result.metadata.quantity_unit = market::QuantityUnitV1::kShare;
    result.metadata.security_type = market::SecurityTypeV1::kEquity;
    result.metadata.asset_scope =
        market::AssetScopeV1::kDocumentedCore;
    return result;
}

struct Fixture final {
    std::unique_ptr<market::DailyInstrumentCatalogV2> catalog;
    std::unique_ptr<market::InstrumentRuntimeStateV2> runtime_state;
};

Fixture MakeFixture(Test* test) {
    Fixture fixture{};
    const std::array<market::DailyInstrumentSourceEntryV2, 4U> input{
        Entry(market::MarketV1::kShenzhen, "102", "000002"),
        Entry(market::MarketV1::kShanghai, "", "600002"),
        Entry(market::MarketV1::kShenzhen, "102", "000001"),
        Entry(market::MarketV1::kShanghai, "", "600001"),
    };
    market::DailyInstrumentCatalogConfigV2 config{};
    config.trade_date = 20260730U;
    config.catalog_version = 41U;
    config.session_epoch = 73U;
    config.market_scope = market::kDailyCatalogMainlandScopeV2;
    config.coverage_complete = true;
    test->Expect(
        market::DailyInstrumentCatalogV2::Create(
            config, input, &fixture.catalog) ==
                market::DailyInstrumentCatalogCreateErrorV2::kNone &&
            fixture.catalog != nullptr,
        "daily catalog creates");
    if (fixture.catalog != nullptr) {
        test->Expect(
            market::InstrumentRuntimeStateV2::Create(
                *fixture.catalog, &fixture.runtime_state) ==
                    market::InstrumentRuntimeStateErrorV2::kNone &&
                fixture.runtime_state != nullptr,
            "runtime state creates from frozen catalog");
    }
    return fixture;
}

void CheckInitialIdentity(Test* test) {
    Fixture fixture = MakeFixture(test);
    if (fixture.runtime_state == nullptr ||
        fixture.catalog == nullptr) {
        return;
    }
    market::InstrumentRuntimeStateV2& state =
        *fixture.runtime_state;
    test->Expect(
        state.capacity() == fixture.catalog->instrument_count() &&
            state.session_epoch() ==
                fixture.catalog->session_epoch() &&
            state.trade_date() == fixture.catalog->trade_date() &&
            state.catalog_version() ==
                fixture.catalog->catalog_version(),
        "runtime identity matches catalog session");

    for (std::size_t ordinal = 0U;
         ordinal < state.capacity();
         ++ordinal) {
        market::InstrumentRuntimeEntryViewV2 by_ordinal{};
        market::InstrumentRuntimeEntryViewV2 by_id{};
        const market::DailyInstrumentCatalogEntryV2* source =
            fixture.catalog->EntryAt(ordinal);
        test->Expect(
            source != nullptr &&
                state.LookupByOrdinal(ordinal, &by_ordinal) ==
                    market::InstrumentRuntimeStateErrorV2::kNone &&
                state.LookupById(source->instrument_id, &by_id) ==
                    market::InstrumentRuntimeStateErrorV2::kNone &&
                by_ordinal.bound() && !by_ordinal.available() &&
                by_ordinal.instrument_id == source->instrument_id &&
                by_id.ordinal == ordinal,
            "all catalog identities start bound with no data");
        if (source != nullptr) {
            market::InstrumentRuntimeEntryViewV2 by_key{};
            test->Expect(
                state.LookupByKey(source->key, &by_key) ==
                        market::InstrumentRuntimeStateErrorV2::kNone &&
                    by_key.instrument_id == source->instrument_id,
                "exact key binary lookup resolves dense identity");
        }
    }

    const std::vector<std::byte> missing = Bytes("600999");
    const market::InstrumentKeyViewV1 missing_key{
        market::MarketV1::kShanghai, {}, missing};
    market::InstrumentRuntimeEntryViewV2 view{};
    test->Expect(
        state.LookupByKey(missing_key, &view) ==
            market::InstrumentRuntimeStateErrorV2::kNotFound,
        "unknown daily identity is not dynamically inserted");

    std::shared_ptr<const market::DailyInstrumentCatalogSnapshotV2>
        snapshot;
    test->Expect(
        state.AcquireSnapshot(&snapshot) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            snapshot != nullptr &&
            snapshot->catalog_scope() ==
                market::InstrumentCatalogScopeV2::
                    kDeclaredDailyAShare &&
            snapshot->coverage_complete() &&
            snapshot->catalog_generation() == 1U &&
            snapshot->bound_count() == state.capacity() &&
            snapshot->available_count() == 0U &&
            snapshot->catalog_digest() ==
                fixture.catalog->catalog_digest(),
        "initial immutable snapshot covers the complete daily catalog");
}

void CheckAvailabilityAndRetainedSnapshots(Test* test) {
    Fixture fixture = MakeFixture(test);
    if (fixture.runtime_state == nullptr) {
        return;
    }
    market::InstrumentRuntimeStateV2& state =
        *fixture.runtime_state;
    std::shared_ptr<const market::DailyInstrumentCatalogSnapshotV2>
        before;
    test->Expect(
        state.AcquireSnapshot(&before) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            before != nullptr,
        "pre-mutation snapshot acquired");

    test->Expect(
        state.SetFactorEligible(0U, 1U, true) ==
            market::InstrumentRuntimeStateErrorV2::
                kPrerequisiteUnavailable,
        "factor eligibility requires snapshot data");
    test->Expect(
        state.MarkApplied(
            0U,
            1U,
            market::InstrumentRuntimeDataKindV2::kSnapshot,
            7U) == market::InstrumentRuntimeStateErrorV2::kNone &&
            state.MarkApplied(
                0U,
                1U,
                market::InstrumentRuntimeDataKindV2::kTick,
                3U) == market::InstrumentRuntimeStateErrorV2::kNone &&
            state.MarkApplied(
                0U,
                1U,
                market::InstrumentRuntimeDataKindV2::kTick,
                11U) == market::InstrumentRuntimeStateErrorV2::kNone &&
            state.SetFactorEligible(0U, 1U, true) ==
                market::InstrumentRuntimeStateErrorV2::kNone,
        "availability and factor state mutate by dense identity");

    std::shared_ptr<const market::DailyInstrumentCatalogSnapshotV2>
        after;
    test->Expect(
        state.AcquireSnapshot(&after) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            after != nullptr && after != before &&
            after->available_count() == 1U &&
            after->snapshot_available_count() == 1U &&
            after->tick_available_count() == 1U &&
            after->factor_eligible_count() == 1U &&
            after->data_state_generation() == 3U,
        "new snapshot publishes coherent aggregate state");

    market::InstrumentRuntimeEntryViewV2 old_entry{};
    market::InstrumentRuntimeEntryViewV2 new_entry{};
    test->Expect(
        before->LookupById(1U, &old_entry) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            !old_entry.available() && !old_entry.has_snapshot &&
            !old_entry.has_tick && !old_entry.factor_eligible &&
            after->LookupById(1U, &new_entry) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            new_entry.available() && new_entry.has_snapshot &&
            new_entry.has_tick && new_entry.factor_eligible &&
            new_entry.first_ingress_sequence == 3U &&
            new_entry.last_ingress_sequence == 11U,
        "retained snapshot remains immutable after live mutation");

    fixture.runtime_state.reset();
    market::InstrumentRuntimeEntryViewV2 retained{};
    test->Expect(
        before->LookupById(1U, &retained) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            !retained.available(),
        "snapshot retains stable storage after runtime destruction");
}

void CheckConcurrentPerEntryMutation(Test* test) {
    Fixture fixture = MakeFixture(test);
    if (fixture.runtime_state == nullptr) {
        return;
    }
    constexpr std::size_t kThreadCount = 4U;
    constexpr std::size_t kIterations = 2'000U;
    std::atomic<bool> failed{false};
    std::array<std::thread, kThreadCount> workers;
    for (std::size_t worker = 0U;
         worker < workers.size();
         ++worker) {
        workers[worker] = std::thread([&, worker]() noexcept {
            for (std::size_t index = 0U;
                 index < kIterations;
                 ++index) {
                const std::uint64_t sequence =
                    static_cast<std::uint64_t>(
                        worker * kIterations + index + 1U);
                const market::InstrumentRuntimeDataKindV2 kind =
                    ((worker + index) & 1U) == 0U
                        ? market::InstrumentRuntimeDataKindV2::
                              kSnapshot
                        : market::InstrumentRuntimeDataKindV2::kTick;
                if (fixture.runtime_state->MarkApplied(
                        0U, 1U, kind, sequence) !=
                    market::InstrumentRuntimeStateErrorV2::kNone) {
                    failed.store(true, std::memory_order_relaxed);
                    return;
                }
            }
        });
    }
    for (std::thread& worker : workers) {
        worker.join();
    }

    std::shared_ptr<const market::DailyInstrumentCatalogSnapshotV2>
        snapshot;
    market::InstrumentRuntimeEntryViewV2 entry{};
    test->Expect(
        !failed.load(std::memory_order_relaxed) &&
            fixture.runtime_state->AcquireSnapshot(&snapshot) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            snapshot != nullptr &&
            snapshot->LookupById(1U, &entry) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            entry.has_snapshot && entry.has_tick &&
            entry.first_ingress_sequence == 1U &&
            entry.last_ingress_sequence ==
                kThreadCount * kIterations &&
            snapshot->available_count() == 1U &&
            snapshot->snapshot_available_count() == 1U &&
            snapshot->tick_available_count() == 1U &&
            snapshot->data_state_generation() == 2U,
        "per-entry concurrency preserves min/max and one-time counts");

    market::InstrumentRuntimeSnapshotStorageStatsV2 stats{};
    test->Expect(
        fixture.runtime_state->SnapshotStorageStats(&stats) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            stats.live_snapshot_count >= 1U,
        "snapshot retention diagnostics remain available");
}

}  // namespace

int main() {
    Test test{};
    CheckInitialIdentity(&test);
    CheckAvailabilityAndRetainedSnapshots(&test);
    CheckConcurrentPerEntryMutation(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " instrument runtime state test(s) failed\n";
        return 1;
    }
    std::cout << "instrument runtime state tests passed\n";
    return 0;
}
