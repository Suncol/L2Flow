#include "l2flow/market/observed_instrument_directory_v2.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

namespace common = l2flow::common;
namespace market = l2flow::market;

class TestContext final {
public:
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    [[nodiscard]] int failures() const noexcept {
        return failures_;
    }

private:
    int failures_ = 0;
};

std::vector<std::byte> Bytes(std::string_view value) {
    const std::span<const char> characters(value.data(), value.size());
    const std::span<const std::byte> bytes =
        std::as_bytes(characters);
    return {bytes.begin(), bytes.end()};
}

market::InstrumentKeyV1 Key(
    market::MarketV1 market,
    std::string_view source,
    std::string_view security_id) {
    market::InstrumentKeyV1 result{};
    result.market = market;
    result.security_id_source = Bytes(source);
    result.security_id = Bytes(security_id);
    return result;
}

market::ObservedInstrumentMetadataV2 EquityMetadata() {
    market::ObservedInstrumentMetadataV2 result{};
    result.quantity_unit = market::QuantityUnitV1::kShare;
    result.security_type = market::SecurityTypeV1::kEquity;
    result.asset_scope = market::AssetScopeV1::kDocumentedCore;
    return result;
}

market::ObservedInstrumentDirectoryConfigV2 Config(
    std::size_t capacity,
    std::uint64_t session_epoch) {
    market::ObservedInstrumentDirectoryConfigV2 result{};
    result.capacity = capacity;
    result.session_epoch = session_epoch;
    return result;
}

std::unique_ptr<market::ObservedInstrumentDirectoryV2> MakeDirectory(
    TestContext* test,
    std::size_t capacity,
    std::uint64_t session_epoch,
    std::string_view description) {
    std::unique_ptr<market::ObservedInstrumentDirectoryV2> result;
    const auto error =
        market::ObservedInstrumentDirectoryV2::Create(
            Config(capacity, session_epoch), &result);
    test->Expect(
        error == market::ObservedInstrumentDirectoryErrorV2::kNone &&
            result != nullptr,
        description);
    return result;
}

bool DigestNonzero(const common::Sha256Digest& digest) {
    for (std::byte byte : digest) {
        if (byte != std::byte{0U}) {
            return true;
        }
    }
    return false;
}

bool KeyEquals(
    const market::InstrumentKeyViewV1& view,
    const market::InstrumentKeyV1& expected) {
    return view.market == expected.market &&
           std::vector<std::byte>(
               view.security_id_source.begin(),
               view.security_id_source.end()) ==
               expected.security_id_source &&
           std::vector<std::byte>(
               view.security_id.begin(), view.security_id.end()) ==
               expected.security_id;
}

bool SnapshotInternallyConsistent(
    const market::ObservedInstrumentCatalogSnapshotV2& snapshot) {
    if (snapshot.catalog_scope() !=
            market::ObservedInstrumentCatalogScopeV2::kObservedOnly ||
        snapshot.coverage_complete() ||
        snapshot.factor_eligible_count() >
            snapshot.snapshot_available_count() ||
        snapshot.snapshot_available_count() >
            snapshot.available_count() ||
        snapshot.tick_available_count() >
            snapshot.available_count() ||
        snapshot.available_count() > snapshot.bound_count() ||
        snapshot.bound_count() > snapshot.capacity()) {
        return false;
    }

    std::size_t available = 0U;
    std::size_t snapshots = 0U;
    std::size_t ticks = 0U;
    std::size_t eligible = 0U;
    for (std::size_t ordinal = 0U;
         ordinal < snapshot.bound_count();
         ++ordinal) {
        market::ObservedInstrumentEntryViewV2 entry{};
        if (snapshot.EntryAt(ordinal, &entry) !=
                market::ObservedInstrumentDirectoryErrorV2::kNone ||
            !entry.bound() || entry.ordinal != ordinal ||
            entry.instrument_id != ordinal + 1U) {
            return false;
        }
        if (entry.available()) {
            ++available;
            if ((!entry.has_snapshot && !entry.has_tick) ||
                entry.first_ingress_sequence == 0U ||
                entry.last_ingress_sequence == 0U ||
                entry.first_ingress_sequence >
                    entry.last_ingress_sequence) {
                return false;
            }
        } else if (
            entry.has_snapshot || entry.has_tick ||
            entry.factor_eligible ||
            entry.first_ingress_sequence != 0U ||
            entry.last_ingress_sequence != 0U) {
            return false;
        }
        snapshots += entry.has_snapshot ? 1U : 0U;
        ticks += entry.has_tick ? 1U : 0U;
        eligible += entry.factor_eligible ? 1U : 0U;
        if (entry.factor_eligible && !entry.has_snapshot) {
            return false;
        }
    }
    return available == snapshot.available_count() &&
           snapshots == snapshot.snapshot_available_count() &&
           ticks == snapshot.tick_available_count() &&
           eligible == snapshot.factor_eligible_count();
}

void TestCreationAndEmptySnapshot(TestContext* test) {
    const auto valid = Config(4U, 17U);
    test->Expect(
        market::ObservedInstrumentDirectoryV2::Create(
            valid, nullptr) ==
            market::ObservedInstrumentDirectoryErrorV2::kNullOutput,
        "Create rejects null output");

    std::unique_ptr<market::ObservedInstrumentDirectoryV2> directory;
    test->Expect(
        market::ObservedInstrumentDirectoryV2::Create(
            Config(0U, 17U), &directory) ==
                market::ObservedInstrumentDirectoryErrorV2::
                    kInvalidConfiguration &&
            directory == nullptr,
        "Create rejects zero capacity");
    test->Expect(
        market::ObservedInstrumentDirectoryV2::Create(
            Config(4U, 0U), &directory) ==
                market::ObservedInstrumentDirectoryErrorV2::
                    kInvalidConfiguration &&
            directory == nullptr,
        "Create requires a nonzero session epoch");
    if constexpr (
        sizeof(std::size_t) > sizeof(std::uint32_t)) {
        test->Expect(
            market::ObservedInstrumentDirectoryV2::Create(
                Config(
                    static_cast<std::size_t>(
                        std::numeric_limits<std::uint32_t>::max()) +
                        1U,
                    17U),
                &directory) ==
                    market::ObservedInstrumentDirectoryErrorV2::
                        kInvalidConfiguration,
            "capacity cannot exceed the instrument-id domain");
    }

    market::ObservedInstrumentDirectoryConfigV2 default_config{};
    default_config.session_epoch = 91U;
    test->Expect(
        market::ObservedInstrumentDirectoryV2::Create(
            default_config, &directory) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            directory != nullptr &&
            directory->capacity() ==
                market::
                    kObservedInstrumentDirectoryDefaultCapacityV2 &&
            directory->session_epoch() == 91U,
        "production default reserves exactly 65,536 session slots");
    if (directory == nullptr) {
        return;
    }

    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> snapshot;
    test->Expect(
        directory->AcquireSnapshot(&snapshot) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            snapshot != nullptr,
        "empty observed catalog is a valid snapshot");
    if (snapshot == nullptr) {
        return;
    }
    test->Expect(
        snapshot->session_epoch() == 91U &&
            snapshot->capacity() ==
                market::
                    kObservedInstrumentDirectoryDefaultCapacityV2 &&
            snapshot->catalog_scope() ==
                market::ObservedInstrumentCatalogScopeV2::kObservedOnly &&
            !snapshot->coverage_complete() &&
            snapshot->catalog_generation() == 0U &&
            snapshot->data_state_generation() == 0U &&
            snapshot->bound_count() == 0U &&
            snapshot->available_count() == 0U &&
            snapshot->snapshot_available_count() == 0U &&
            snapshot->tick_available_count() == 0U &&
            snapshot->factor_eligible_count() == 0U &&
            DigestNonzero(snapshot->catalog_digest()) &&
            SnapshotInternallyConsistent(*snapshot),
        "empty snapshot states observed-only semantics without claiming coverage");

    market::ObservedInstrumentEntryViewV2 entry{};
    test->Expect(
        snapshot->EntryAt(0U, &entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNotFound &&
            entry.binding_state ==
                market::ObservedInstrumentBindingStateV2::kUnbound,
        "empty catalog has no published row");
    market::ObservedInstrumentSnapshotStorageStatsV2 stats{};
    test->Expect(
        directory->SnapshotStorageStats(nullptr) ==
                market::ObservedInstrumentDirectoryErrorV2::kNullOutput &&
            directory->SnapshotStorageStats(&stats) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            stats.live_snapshot_count == 1U &&
            stats.allocated_version_node_count == 0U,
        "snapshot storage diagnostics report live readers and MVCC high-water");
}

void TestBindingAndSnapshotLifetime(TestContext* test) {
    auto directory = MakeDirectory(
        test, 4U, 23U, "binding-test directory creates");
    if (directory == nullptr) {
        return;
    }
    const market::InstrumentKeyV1 shanghai =
        Key(market::MarketV1::kShanghai, "", "600001");
    const market::InstrumentKeyV1 shenzhen =
        Key(market::MarketV1::kShenzhen, "102 ", "000001");
    const auto metadata = EquityMetadata();

    market::ObservedInstrumentBindResultV2 first{};
    test->Expect(
        directory->BindOrGet(
            shanghai, metadata, 10U, &first) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            first.newly_bound && first.entry.bound() &&
            !first.entry.available() &&
            first.entry.instrument_id == 1U &&
            first.entry.ordinal == 0U &&
            first.entry.first_capture_sequence == 10U &&
            first.catalog_generation == 1U &&
            first.bound_count == 1U &&
            first.catalog_digest != common::Sha256Digest{} &&
            KeyEquals(first.entry.key, shanghai),
        "first exact key binds ordinal zero and session ID one");
    std::size_t resolved_ordinal =
        std::numeric_limits<std::size_t>::max();
    test->Expect(
        directory->ResolveBoundId(1U, &resolved_ordinal) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            resolved_ordinal == 0U &&
            directory->ResolveBoundId(2U, &resolved_ordinal) ==
                market::ObservedInstrumentDirectoryErrorV2::kNotFound,
        "existing-ID hot path resolves directly and rejects unbound slots");

    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> one;
    test->Expect(
        directory->AcquireSnapshot(&one) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            one != nullptr && one->bound_count() == 1U &&
            one->catalog_generation() == 1U,
        "first binding publishes one immutable catalog prefix");
    if (one == nullptr) {
        return;
    }
    const common::Sha256Digest digest_one = one->catalog_digest();

    market::ObservedInstrumentBindResultV2 duplicate{};
    test->Expect(
        directory->BindOrGet(
            shanghai, metadata, 11U, &duplicate) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            !duplicate.newly_bound &&
            duplicate.entry.instrument_id == 1U &&
            duplicate.entry.ordinal == 0U &&
            duplicate.catalog_generation == 1U &&
            duplicate.bound_count == 1U &&
            duplicate.catalog_digest == digest_one,
        "repeated exact key returns only its original binding");
    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2>
        after_duplicate;
    test->Expect(
        directory->AcquireSnapshot(&after_duplicate) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            after_duplicate->catalog_generation() == 1U &&
            after_duplicate->catalog_digest() == digest_one,
        "duplicate key changes neither catalog generation nor digest");

    auto conflicting = metadata;
    conflicting.security_type = market::SecurityTypeV1::kFund;
    test->Expect(
        directory->BindOrGet(
            shanghai, conflicting, 12U, &duplicate) ==
                market::ObservedInstrumentDirectoryErrorV2::
                    kMetadataConflict,
        "same key cannot silently change immutable metadata");

    market::ObservedInstrumentBindResultV2 second{};
    test->Expect(
        directory->BindOrGet(
            shenzhen, metadata, 12U, &second) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            second.newly_bound &&
            second.entry.instrument_id == 2U &&
            second.entry.ordinal == 1U &&
            second.catalog_generation == 2U &&
            second.bound_count == 2U &&
            KeyEquals(second.entry.key, shenzhen),
        "failed metadata conflict does not consume capture order or a slot");
    test->Expect(
        directory->BindOrGet(
            shanghai, metadata, 12U, &duplicate) ==
            market::ObservedInstrumentDirectoryErrorV2::
                kSequenceNotIncreasing,
        "BindOrGet enforces strictly increasing capture order");

    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> two;
    test->Expect(
        directory->AcquireSnapshot(&two) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            two != nullptr && two->bound_count() == 2U &&
            two->catalog_generation() == 2U &&
            two->catalog_digest() != digest_one,
        "new binding advances the rolling catalog identity exactly once");
    if (two == nullptr) {
        return;
    }
    market::ObservedInstrumentEntryViewV2 entry{};
    test->Expect(
        one->EntryAt(1U, &entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNotFound &&
            one->bound_count() == 1U,
        "older snapshot retains its original bound prefix");
    test->Expect(
        two->LookupById(2U, &entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            entry.ordinal == 1U && KeyEquals(entry.key, shenzhen),
        "snapshot ID lookup is direct and exact");
    test->Expect(
        directory->LookupByKey(shenzhen, &entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            entry.instrument_id == 2U &&
            directory->LookupById(2U, &entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            directory->LookupByOrdinal(1U, &entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone,
        "directory key, ID, and ordinal lookups agree");
    test->Expect(
        directory->LookupByOrdinal(2U, &entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNotFound &&
            entry.binding_state ==
                market::ObservedInstrumentBindingStateV2::kUnbound,
        "unbound capacity slots are never presented as instruments");

    directory.reset();
    test->Expect(
        two->EntryAt(0U, &entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            KeyEquals(entry.key, shanghai) &&
            two->EntryAt(1U, &entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            KeyEquals(entry.key, shenzhen),
        "snapshot-owned stable storage survives directory destruction");
}

void TestValidationAndCapacity(TestContext* test) {
    auto directory = MakeDirectory(
        test, 2U, 31U, "capacity-test directory creates");
    if (directory == nullptr) {
        return;
    }
    const auto metadata = EquityMetadata();
    market::ObservedInstrumentBindResultV2 result{};

    market::InstrumentKeyV1 invalid =
        Key(market::MarketV1::kUnknown, "", "600001");
    test->Expect(
        directory->BindOrGet(invalid, metadata, 1U, &result) ==
            market::ObservedInstrumentDirectoryErrorV2::kInvalidKey,
        "unknown market cannot be bound");
    invalid = Key(market::MarketV1::kShanghai, "", "");
    test->Expect(
        directory->BindOrGet(invalid, metadata, 1U, &result) ==
            market::ObservedInstrumentDirectoryErrorV2::kInvalidKey,
        "empty SecurityID cannot be bound");
    auto invalid_metadata = metadata;
    invalid_metadata.asset_scope =
        static_cast<market::AssetScopeV1>(0xffU);
    test->Expect(
        directory->BindOrGet(
            Key(market::MarketV1::kShanghai, "", "600001"),
            invalid_metadata,
            1U,
            &result) ==
            market::ObservedInstrumentDirectoryErrorV2::
                kInvalidMetadata,
        "invalid metadata enum cannot enter stable storage");
    test->Expect(
        directory->BindOrGet(
            Key(market::MarketV1::kShanghai, "", "600001"),
            metadata,
            0U,
            &result) ==
            market::ObservedInstrumentDirectoryErrorV2::
                kInvalidSequence,
        "zero capture sequence is rejected");

    const auto first =
        Key(market::MarketV1::kShanghai, "", "600001");
    const auto second =
        Key(market::MarketV1::kShanghai, "", "600002");
    const auto overflow =
        Key(market::MarketV1::kShanghai, "", "600003");
    test->Expect(
        directory->BindOrGet(first, metadata, 1U, &result) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            result.entry.instrument_id == 1U &&
            directory->BindOrGet(second, metadata, 2U, &result) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            result.entry.instrument_id == 2U,
        "capacity slots bind once in capture order");
    test->Expect(
        directory->BindOrGet(overflow, metadata, 3U, &result) ==
            market::ObservedInstrumentDirectoryErrorV2::
                kCapacityExhausted,
        "capacity exhaustion cannot overwrite a bound slot");

    market::ObservedInstrumentEntryViewV2 entry{};
    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> snapshot;
    test->Expect(
        directory->LookupById(1U, &entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            KeyEquals(entry.key, first) &&
            directory->LookupById(2U, &entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            KeyEquals(entry.key, second) &&
            directory->AcquireSnapshot(&snapshot) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            snapshot->bound_count() == 2U &&
            snapshot->catalog_generation() == 2U,
        "capacity failure leaves the complete prior prefix unchanged");
}

void TestAvailabilityAndEligibility(TestContext* test) {
    auto directory = MakeDirectory(
        test, 3U, 41U, "availability-test directory creates");
    if (directory == nullptr) {
        return;
    }
    const auto metadata = EquityMetadata();
    const std::array<market::InstrumentKeyV1, 3U> keys{
        Key(market::MarketV1::kShanghai, "", "600001"),
        Key(market::MarketV1::kShanghai, "", "600002"),
        Key(market::MarketV1::kShanghai, "", "600003")};
    market::ObservedInstrumentBindResultV2 bound{};
    for (std::size_t index = 0U; index < keys.size(); ++index) {
        test->Expect(
            directory->BindOrGet(
                keys[index], metadata, index + 1U, &bound) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone,
            "availability fixture binds identity");
    }

    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> before;
    test->Expect(
        directory->AcquireSnapshot(&before) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            before != nullptr && before->bound_count() == 3U &&
            before->available_count() == 0U,
        "bound identities begin without applied data");
    if (before == nullptr) {
        return;
    }
    const common::Sha256Digest catalog_digest =
        before->catalog_digest();

    test->Expect(
        directory->SetFactorEligible(0U, 1U, true) ==
            market::ObservedInstrumentDirectoryErrorV2::
                kPrerequisiteUnavailable,
        "factor eligibility requires an applied snapshot");
    test->Expect(
        directory->MarkApplied(
            0U,
            2U,
            market::ObservedInstrumentDataKindV2::kSnapshot,
            100U) ==
            market::ObservedInstrumentDirectoryErrorV2::
                kIdentityMismatch,
        "applied transition validates the exact ordinal-ID pair");
    test->Expect(
        directory->MarkApplied(
            0U,
            1U,
            static_cast<market::ObservedInstrumentDataKindV2>(255U),
            100U) ==
                market::ObservedInstrumentDirectoryErrorV2::
                    kInvalidDataKind,
        "applied transition rejects an unknown data kind precisely");
    test->Expect(
        directory->MarkApplied(
            0U,
            1U,
            market::ObservedInstrumentDataKindV2::kSnapshot,
            100U) ==
            market::ObservedInstrumentDirectoryErrorV2::kNone,
        "first snapshot transitions bound-no-data to available");

    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> first_data;
    test->Expect(
        directory->AcquireSnapshot(&first_data) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            first_data->available_count() == 1U &&
            first_data->snapshot_available_count() == 1U &&
            first_data->tick_available_count() == 0U &&
            first_data->data_state_generation() == 1U,
        "first applied snapshot increments each relevant count once");

    test->Expect(
        directory->MarkApplied(
            0U,
            1U,
            market::ObservedInstrumentDataKindV2::kSnapshot,
            90U) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            directory->MarkApplied(
                0U,
                1U,
                market::ObservedInstrumentDataKindV2::kSnapshot,
                150U) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone,
        "repeat snapshots update sequence extrema without recounting");
    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> repeated;
    test->Expect(
        directory->AcquireSnapshot(&repeated) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            repeated != nullptr,
        "snapshot repeated applied state");
    if (repeated == nullptr) {
        return;
    }
    market::ObservedInstrumentEntryViewV2 entry{};
    test->Expect(
        repeated->data_state_generation() == 1U &&
            repeated->available_count() == 1U &&
            repeated->snapshot_available_count() == 1U &&
            repeated->EntryAt(0U, &entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            entry.first_ingress_sequence == 90U &&
            entry.last_ingress_sequence == 150U,
        "first/last ingress are min/max, not worker arrival order");
    market::ObservedInstrumentEntryViewV2 first_data_entry{};
    test->Expect(
        first_data->EntryAt(0U, &first_data_entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            first_data_entry.first_ingress_sequence == 100U &&
            first_data_entry.last_ingress_sequence == 100U,
        "older available snapshot freezes ingress extrema");

    test->Expect(
        directory->MarkApplied(
            0U,
            1U,
            market::ObservedInstrumentDataKindV2::kTick,
            120U) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            directory->SetFactorEligible(0U, 1U, true) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone,
        "new tick kind and eligibility each transition once");
    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> eligible;
    test->Expect(
        directory->AcquireSnapshot(&eligible) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            eligible != nullptr,
        "snapshot eligible state");
    if (eligible == nullptr) {
        return;
    }
    const std::uint64_t eligible_generation =
        eligible->data_state_generation();
    test->Expect(
        eligible_generation == 3U &&
            eligible->available_count() == 1U &&
            eligible->snapshot_available_count() == 1U &&
            eligible->tick_available_count() == 1U &&
            eligible->factor_eligible_count() == 1U,
        "data-state generation covers first kind and eligibility transitions");
    test->Expect(
        directory->SetFactorEligible(0U, 1U, true) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            directory->AcquireSnapshot(&eligible) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            eligible->data_state_generation() == eligible_generation,
        "idempotent eligibility does not advance its generation");
    test->Expect(
        directory->SetFactorEligible(0U, 1U, false) ==
            market::ObservedInstrumentDirectoryErrorV2::kNone,
        "factor eligibility can be removed without changing availability");

    test->Expect(
        directory->MarkApplied(
            1U,
            2U,
            market::ObservedInstrumentDataKindV2::kTick,
            200U) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            directory->SetFactorEligible(1U, 2U, true) ==
                market::ObservedInstrumentDirectoryErrorV2::
                    kPrerequisiteUnavailable &&
            directory->MarkApplied(
                1U,
                2U,
                market::ObservedInstrumentDataKindV2::kSnapshot,
                180U) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            directory->SetFactorEligible(1U, 2U, true) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone,
        "tick-only identity remains ineligible until a snapshot arrives");

    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> final;
    test->Expect(
        directory->AcquireSnapshot(&final) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            final != nullptr && SnapshotInternallyConsistent(*final) &&
            final->catalog_generation() == 3U &&
            final->catalog_digest() == catalog_digest &&
            final->data_state_generation() == 7U &&
            final->bound_count() == 3U &&
            final->available_count() == 2U &&
            final->snapshot_available_count() == 2U &&
            final->tick_available_count() == 2U &&
            final->factor_eligible_count() == 1U,
        "final counts satisfy every observed-universe inequality");
    if (final != nullptr) {
        test->Expect(
            final->EntryAt(1U, &entry) ==
                    market::ObservedInstrumentDirectoryErrorV2::kNone &&
                entry.first_ingress_sequence == 180U &&
                entry.last_ingress_sequence == 200U,
            "cross-kind ingress extrema remain exact");
    }

    test->Expect(
        before->available_count() == 0U &&
            before->data_state_generation() == 0U &&
            before->EntryAt(0U, &entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            !entry.available() && !entry.has_snapshot &&
            !entry.has_tick && !entry.factor_eligible,
        "old snapshot freezes data state as well as catalog identity");
}

void TestRollingDigestIdentity(TestContext* test) {
    auto first = MakeDirectory(
        test, 4U, 51U, "first digest directory creates");
    auto same = MakeDirectory(
        test, 4U, 51U, "same digest directory creates");
    auto different_session = MakeDirectory(
        test, 4U, 52U, "different-session directory creates");
    auto reverse = MakeDirectory(
        test, 4U, 51U, "reverse digest directory creates");
    if (first == nullptr || same == nullptr ||
        different_session == nullptr || reverse == nullptr) {
        return;
    }
    const auto metadata = EquityMetadata();
    const auto one =
        Key(market::MarketV1::kShanghai, "", "600001");
    const auto two =
        Key(market::MarketV1::kShenzhen, "102 ", "000001");
    market::ObservedInstrumentBindResultV2 result{};
    bool bindings_ok = true;
    for (market::ObservedInstrumentDirectoryV2* directory :
         {first.get(), same.get(), different_session.get()}) {
        bindings_ok =
            bindings_ok &&
            directory->BindOrGet(one, metadata, 10U, &result) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            directory->BindOrGet(two, metadata, 20U, &result) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone;
    }
    bindings_ok =
        bindings_ok &&
        reverse->BindOrGet(two, metadata, 10U, &result) ==
            market::ObservedInstrumentDirectoryErrorV2::kNone &&
        reverse->BindOrGet(one, metadata, 20U, &result) ==
            market::ObservedInstrumentDirectoryErrorV2::kNone;
    test->Expect(bindings_ok, "digest fixtures bind exact prefixes");
    if (!bindings_ok) {
        return;
    }

    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> first_snapshot;
    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> same_snapshot;
    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> session_snapshot;
    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> reverse_snapshot;
    const bool snapshots_ok =
        first->AcquireSnapshot(&first_snapshot) ==
            market::ObservedInstrumentDirectoryErrorV2::kNone &&
        same->AcquireSnapshot(&same_snapshot) ==
            market::ObservedInstrumentDirectoryErrorV2::kNone &&
        different_session->AcquireSnapshot(&session_snapshot) ==
            market::ObservedInstrumentDirectoryErrorV2::kNone &&
        reverse->AcquireSnapshot(&reverse_snapshot) ==
            market::ObservedInstrumentDirectoryErrorV2::kNone;
    test->Expect(
        snapshots_ok && first_snapshot != nullptr &&
            same_snapshot != nullptr &&
            session_snapshot != nullptr && reverse_snapshot != nullptr &&
            first_snapshot->catalog_digest() ==
                same_snapshot->catalog_digest() &&
            first_snapshot->catalog_digest() !=
                session_snapshot->catalog_digest() &&
            first_snapshot->catalog_digest() !=
                reverse_snapshot->catalog_digest(),
        "rolling digest is deterministic and binds session plus slot order");
}

void TestConcurrentSnapshotsAndAppliedTransitions(TestContext* test) {
    constexpr std::size_t kInstrumentCount = 96U;
    constexpr std::size_t kWorkerCount = 4U;
    auto directory = MakeDirectory(
        test,
        kInstrumentCount,
        61U,
        "concurrency-test directory creates");
    if (directory == nullptr) {
        return;
    }
    const auto metadata = EquityMetadata();
    market::ObservedInstrumentBindResultV2 result{};
    for (std::size_t ordinal = 0U;
         ordinal < kInstrumentCount;
         ++ordinal) {
        const std::string security_id =
            "S" + std::to_string(ordinal);
        if (directory->BindOrGet(
                Key(
                    market::MarketV1::kShanghai,
                    "",
                    security_id),
                metadata,
                ordinal + 1U,
                &result) !=
            market::ObservedInstrumentDirectoryErrorV2::kNone) {
            test->Expect(false, "concurrency fixture binds all slots");
            return;
        }
    }

    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> before;
    test->Expect(
        directory->AcquireSnapshot(&before) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            before != nullptr,
        "snapshot concurrency fixture before transitions");
    if (before == nullptr) {
        return;
    }
    std::atomic<bool> start{false};
    std::atomic<std::size_t> completed{0U};
    std::atomic<bool> failed{false};
    std::vector<std::thread> workers;
    workers.reserve(kWorkerCount);
    for (std::size_t worker = 0U; worker < kWorkerCount; ++worker) {
        workers.emplace_back([&, worker] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (std::size_t ordinal = worker;
                 ordinal < kInstrumentCount;
                 ordinal += kWorkerCount) {
                const std::uint32_t instrument_id =
                    static_cast<std::uint32_t>(ordinal + 1U);
                const std::uint64_t base =
                    1'000U +
                    static_cast<std::uint64_t>(ordinal) * 10U;
                if (directory->MarkApplied(
                        ordinal,
                        instrument_id,
                        market::ObservedInstrumentDataKindV2::kSnapshot,
                        base + 5U) !=
                        market::ObservedInstrumentDirectoryErrorV2::kNone ||
                    directory->MarkApplied(
                        ordinal,
                        instrument_id,
                        market::ObservedInstrumentDataKindV2::kSnapshot,
                        base + 1U) !=
                        market::ObservedInstrumentDirectoryErrorV2::kNone) {
                    failed.store(true, std::memory_order_release);
                    break;
                }
                if (ordinal % 2U == 0U &&
                    directory->MarkApplied(
                        ordinal,
                        instrument_id,
                        market::ObservedInstrumentDataKindV2::kTick,
                        base + 9U) !=
                        market::ObservedInstrumentDirectoryErrorV2::kNone) {
                    failed.store(true, std::memory_order_release);
                    break;
                }
                if (ordinal % 3U == 0U &&
                    directory->SetFactorEligible(
                        ordinal, instrument_id, true) !=
                        market::ObservedInstrumentDirectoryErrorV2::kNone) {
                    failed.store(true, std::memory_order_release);
                    break;
                }
            }
            completed.fetch_add(1U, std::memory_order_release);
        });
    }

    std::thread snapshot_reader([&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        do {
            std::shared_ptr<
                const market::ObservedInstrumentCatalogSnapshotV2>
                snapshot;
            if (directory->AcquireSnapshot(&snapshot) !=
                    market::ObservedInstrumentDirectoryErrorV2::kNone ||
                snapshot == nullptr ||
                !SnapshotInternallyConsistent(*snapshot) ||
                snapshot->bound_count() != kInstrumentCount ||
                snapshot->catalog_generation() != kInstrumentCount) {
                failed.store(true, std::memory_order_release);
                return;
            }
            std::this_thread::yield();
        } while (
            completed.load(std::memory_order_acquire) <
            kWorkerCount);
    });

    start.store(true, std::memory_order_release);
    for (std::thread& worker : workers) {
        worker.join();
    }
    snapshot_reader.join();

    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> final;
    test->Expect(
        !failed.load(std::memory_order_acquire) &&
            directory->AcquireSnapshot(&final) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            final != nullptr && SnapshotInternallyConsistent(*final),
        "concurrent MarkApplied and snapshot acquisition remain coherent");
    if (final == nullptr) {
        return;
    }
    test->Expect(
        final->bound_count() == kInstrumentCount &&
            final->available_count() == kInstrumentCount &&
            final->snapshot_available_count() == kInstrumentCount &&
            final->tick_available_count() ==
                (kInstrumentCount + 1U) / 2U &&
            final->factor_eligible_count() ==
                (kInstrumentCount + 2U) / 3U &&
            final->data_state_generation() ==
                kInstrumentCount +
                    (kInstrumentCount + 1U) / 2U +
                    (kInstrumentCount + 2U) / 3U,
        "concurrent first transitions are counted exactly once");

    market::ObservedInstrumentEntryViewV2 entry{};
    test->Expect(
        before != nullptr && before->available_count() == 0U &&
            before->EntryAt(0U, &entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            !entry.available() && !entry.has_snapshot,
        "pre-transition snapshot stays immutable during concurrent writers");
}

void TestSparseLiveEpochIntervalsAndRecycling(TestContext* test) {
    auto directory = MakeDirectory(
        test, 1U, 67U, "interval directory creates");
    if (directory == nullptr) {
        return;
    }
    market::ObservedInstrumentBindResultV2 binding{};
    if (directory->BindOrGet(
            Key(market::MarketV1::kShanghai, "", "I1"),
            EquityMetadata(),
            1U,
            &binding) !=
        market::ObservedInstrumentDirectoryErrorV2::kNone) {
        test->Expect(false, "interval fixture binds identity");
        return;
    }

    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> first;
    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> middle;
    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> third;
    const auto apply = [&directory](std::uint64_t sequence) {
        return directory->MarkApplied(
            0U,
            1U,
            market::ObservedInstrumentDataKindV2::kSnapshot,
            sequence);
    };
    test->Expect(
        directory->AcquireSnapshot(&first) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            apply(100U) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            directory->AcquireSnapshot(&middle) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            apply(200U) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            directory->AcquireSnapshot(&third) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone,
        "three interval snapshots publish around two mutations");
    if (first == nullptr || middle == nullptr || third == nullptr) {
        return;
    }

    middle.reset();
    test->Expect(
        apply(300U) ==
            market::ObservedInstrumentDirectoryErrorV2::kNone,
        "mutation compacts a dead middle epoch");
    market::ObservedInstrumentEntryViewV2 first_entry{};
    market::ObservedInstrumentEntryViewV2 third_entry{};
    test->Expect(
        first->EntryAt(0U, &first_entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            third->EntryAt(0U, &third_entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            !first_entry.available() &&
            third_entry.first_ingress_sequence == 100U &&
            third_entry.last_ingress_sequence == 200U,
        "live endpoints remain exact after dead interval recycling");

    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> fourth;
    test->Expect(
        directory->AcquireSnapshot(&fourth) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            fourth != nullptr,
        "post-compaction state publishes");
    first.reset();
    third.reset();
    test->Expect(
        apply(400U) ==
            market::ObservedInstrumentDirectoryErrorV2::kNone,
        "only newest retained interval remains");
    market::ObservedInstrumentSnapshotStorageStatsV2 stats{};
    market::ObservedInstrumentEntryViewV2 fourth_entry{};
    test->Expect(
        fourth != nullptr &&
            fourth->EntryAt(0U, &fourth_entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            fourth_entry.first_ingress_sequence == 100U &&
            fourth_entry.last_ingress_sequence == 300U &&
            directory->SnapshotStorageStats(&stats) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            stats.live_snapshot_count == 1U &&
            stats.allocated_version_node_count == 2U,
        "version high-water follows simultaneously retained intervals, "
        "not elapsed cuts");
}

void TestSixtyThousandRowSnapshotPublication(TestContext* test) {
    constexpr std::size_t kInstrumentCount = 60'000U;
    constexpr std::size_t kCachedAcquireCount = 2'048U;
    constexpr auto kMaximumPublicationLatency =
        std::chrono::milliseconds(100);
    auto directory = MakeDirectory(
        test,
        kInstrumentCount,
        71U,
        "60k publication directory creates");
    if (directory == nullptr) {
        return;
    }

    const auto metadata = EquityMetadata();
    market::ObservedInstrumentBindResultV2 binding{};
    for (std::size_t ordinal = 0U;
         ordinal < kInstrumentCount;
         ++ordinal) {
        const std::string security_id =
            "L" + std::to_string(ordinal);
        if (directory->BindOrGet(
                Key(
                    market::MarketV1::kShanghai,
                    "",
                    security_id),
                metadata,
                ordinal + 1U,
                &binding) !=
            market::ObservedInstrumentDirectoryErrorV2::kNone) {
            test->Expect(false, "60k fixture binds every identity");
            return;
        }
    }

    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> before;
    const auto first_begin = std::chrono::steady_clock::now();
    const auto first_error = directory->AcquireSnapshot(&before);
    const auto first_elapsed =
        std::chrono::steady_clock::now() - first_begin;
    test->Expect(
        first_error ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            before != nullptr &&
            before->bound_count() == kInstrumentCount &&
            before->catalog_generation() == kInstrumentCount &&
            before->available_count() == 0U &&
            first_elapsed < kMaximumPublicationLatency,
        "60k dirty snapshot publication is bounded and coherent");
    if (before == nullptr) {
        return;
    }

    bool rows_valid = true;
    for (std::size_t ordinal = 0U;
         ordinal < kInstrumentCount;
         ++ordinal) {
        market::ObservedInstrumentEntryViewV2 entry{};
        if (before->EntryAt(ordinal, &entry) !=
                market::ObservedInstrumentDirectoryErrorV2::kNone ||
            entry.ordinal != ordinal ||
            entry.instrument_id != ordinal + 1U ||
            entry.available()) {
            rows_valid = false;
            break;
        }
    }
    test->Expect(
        rows_valid,
        "60k snapshot rows match its generation, count and bound prefix");

    bool cached_pointer_stable = true;
    const auto cached_begin = std::chrono::steady_clock::now();
    for (std::size_t attempt = 0U;
         attempt < kCachedAcquireCount;
         ++attempt) {
        std::shared_ptr<
            const market::ObservedInstrumentCatalogSnapshotV2> cached;
        if (directory->AcquireSnapshot(&cached) !=
                market::ObservedInstrumentDirectoryErrorV2::kNone ||
            cached.get() != before.get()) {
            cached_pointer_stable = false;
            break;
        }
    }
    const auto cached_elapsed =
        std::chrono::steady_clock::now() - cached_begin;
    test->Expect(
        cached_pointer_stable &&
            cached_elapsed < kMaximumPublicationLatency,
        "unchanged 60k catalog reuses one O(1) published snapshot");

    const std::size_t changed_ordinal = kInstrumentCount - 1U;
    const std::uint32_t changed_id =
        static_cast<std::uint32_t>(kInstrumentCount);
    test->Expect(
        directory->MarkApplied(
            changed_ordinal,
            changed_id,
            market::ObservedInstrumentDataKindV2::kSnapshot,
            80'000U) ==
            market::ObservedInstrumentDirectoryErrorV2::kNone,
        "60k fixture mutates one row after the first publication");

    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> after;
    const auto second_begin = std::chrono::steady_clock::now();
    const auto second_error = directory->AcquireSnapshot(&after);
    const auto second_elapsed =
        std::chrono::steady_clock::now() - second_begin;
    market::ObservedInstrumentEntryViewV2 before_entry{};
    market::ObservedInstrumentEntryViewV2 after_entry{};
    test->Expect(
        second_error ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            after != nullptr && after.get() != before.get() &&
            after->catalog_generation() ==
                before->catalog_generation() &&
            after->catalog_digest() == before->catalog_digest() &&
            after->bound_count() == kInstrumentCount &&
            after->available_count() == 1U &&
            after->snapshot_available_count() == 1U &&
            before->EntryAt(changed_ordinal, &before_entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            after->EntryAt(changed_ordinal, &after_entry) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            !before_entry.available() &&
            after_entry.available() && after_entry.has_snapshot &&
            after_entry.first_ingress_sequence == 80'000U &&
            after_entry.last_ingress_sequence == 80'000U &&
            second_elapsed < kMaximumPublicationLatency,
        "one-row MVCC update preserves the old 60k cut and publishes the "
        "new cut without a prefix scan");

    before.reset();
    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2> retained =
        std::move(after);
    bool bounded_versions = retained != nullptr;
    constexpr std::size_t kRetentionRounds = 8U;
    std::chrono::nanoseconds first_post_cut_elapsed{0};
    std::chrono::nanoseconds same_cut_repeat_elapsed{0};
    for (std::size_t round = 0U;
         bounded_versions && round < kRetentionRounds;
         ++round) {
        const std::uint64_t ingress_sequence =
            100'000U + static_cast<std::uint64_t>(round);
        const auto post_cut_begin = std::chrono::steady_clock::now();
        for (std::size_t ordinal = 0U;
             ordinal < kInstrumentCount;
             ++ordinal) {
            if (directory->MarkApplied(
                    ordinal,
                    static_cast<std::uint32_t>(ordinal + 1U),
                    market::ObservedInstrumentDataKindV2::kSnapshot,
                    ingress_sequence) !=
                market::ObservedInstrumentDirectoryErrorV2::kNone) {
                bounded_versions = false;
                break;
            }
        }
        const auto post_cut_end = std::chrono::steady_clock::now();
        if (round == 1U) {
            first_post_cut_elapsed =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    post_cut_end - post_cut_begin);
            const auto repeat_begin =
                std::chrono::steady_clock::now();
            for (std::size_t ordinal = 0U;
                 ordinal < kInstrumentCount;
                 ++ordinal) {
                if (directory->MarkApplied(
                        ordinal,
                        static_cast<std::uint32_t>(ordinal + 1U),
                        market::ObservedInstrumentDataKindV2::kSnapshot,
                        200'000U +
                            static_cast<std::uint64_t>(round)) !=
                    market::ObservedInstrumentDirectoryErrorV2::kNone) {
                    bounded_versions = false;
                    break;
                }
            }
            same_cut_repeat_elapsed =
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() -
                    repeat_begin);
        }
        std::shared_ptr<
            const market::ObservedInstrumentCatalogSnapshotV2> next;
        if (bounded_versions &&
            (directory->AcquireSnapshot(&next) !=
                 market::ObservedInstrumentDirectoryErrorV2::kNone ||
             next == nullptr ||
             next->bound_count() != kInstrumentCount ||
             next->available_count() != kInstrumentCount ||
             next->snapshot_available_count() !=
                 kInstrumentCount)) {
            bounded_versions = false;
        }
        retained.reset();
        retained = std::move(next);
    }
    market::ObservedInstrumentSnapshotStorageStatsV2 storage_stats{};
    test->Expect(
        bounded_versions &&
            directory->SnapshotStorageStats(&storage_stats) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            storage_stats.live_snapshot_count == 1U &&
            storage_stats.allocated_version_node_count <=
                kInstrumentCount,
        "60k repeated cuts recycle per-ordinal MVCC nodes after old "
        "snapshots are released");

    std::cout
        << "60k catalog publication: first_ns="
        << std::chrono::duration_cast<std::chrono::nanoseconds>(
               first_elapsed)
               .count()
        << " second_ns="
        << std::chrono::duration_cast<std::chrono::nanoseconds>(
               second_elapsed)
               .count()
        << " cached_2048_ns="
        << std::chrono::duration_cast<std::chrono::nanoseconds>(
               cached_elapsed)
               .count()
        << " mvcc_node_high_water="
        << storage_stats.allocated_version_node_count
        << " first_post_cut_mark_mean_ns="
        << (first_post_cut_elapsed.count() /
            static_cast<std::int64_t>(kInstrumentCount))
        << " same_cut_mark_mean_ns="
        << (same_cut_repeat_elapsed.count() /
            static_cast<std::int64_t>(kInstrumentCount))
        << '\n';
}

}  // namespace

int main() {
    TestContext test;
    TestCreationAndEmptySnapshot(&test);
    TestBindingAndSnapshotLifetime(&test);
    TestValidationAndCapacity(&test);
    TestAvailabilityAndEligibility(&test);
    TestRollingDigestIdentity(&test);
    TestConcurrentSnapshotsAndAppliedTransitions(&test);
    TestSparseLiveEpochIntervalsAndRecycling(&test);
    TestSixtyThousandRowSnapshotPublication(&test);
    if (test.failures() == 0) {
        std::cout
            << "observed instrument directory v2 tests passed\n";
    }
    return test.failures() == 0 ? 0 : 1;
}
