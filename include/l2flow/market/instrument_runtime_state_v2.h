#pragma once

#include "l2flow/common/sha256.h"
#include "l2flow/market/daily_instrument_catalog_v2.h"
#include "l2flow/market/instrument_identity_v2.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string_view>

namespace l2flow::market {

// Runtime identity is the complete, immutable daily A-share catalog. Only
// availability, factor eligibility, and applied-sequence coverage mutate.
enum class InstrumentCatalogScopeV2 : std::uint8_t {
    kDeclaredDailyAShare = 2U,
};

enum class InstrumentRuntimeDataStateV2 : std::uint8_t {
    kBoundNoData = 0U,
    kAvailable,
};

enum class InstrumentRuntimeDataKindV2 : std::uint8_t {
    kSnapshot = 0U,
    kTick,
};

using InstrumentRuntimeMetadataV2 = InstrumentMetadataV2;

enum class InstrumentRuntimeStateErrorV2 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kInvalidDataKind,
    kInvalidKey,
    kInvalidSequence,
    kNotFound,
    kIdentityMismatch,
    kPrerequisiteUnavailable,
    kGenerationExhausted,
    kResourceExhausted,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view
InstrumentRuntimeStateErrorNameV2(
    InstrumentRuntimeStateErrorV2 error) noexcept;

// Key spans borrow immutable catalog storage. Runtime lookups remain valid
// while InstrumentRuntimeStateV2 lives; snapshot lookups remain valid while
// the snapshot lives.
struct InstrumentRuntimeEntryViewV2 final {
    std::uint32_t instrument_id = 0U;
    std::size_t ordinal = std::numeric_limits<std::size_t>::max();
    InstrumentKeyViewV1 key{};
    InstrumentRuntimeMetadataV2 metadata{};
    InstrumentRuntimeDataStateV2 data_state =
        InstrumentRuntimeDataStateV2::kBoundNoData;
    bool has_snapshot = false;
    bool has_tick = false;
    bool factor_eligible = false;
    std::uint64_t first_ingress_sequence = 0U;
    std::uint64_t last_ingress_sequence = 0U;

    [[nodiscard]] bool bound() const noexcept {
        return instrument_id != 0U &&
               ordinal != std::numeric_limits<std::size_t>::max() &&
               (data_state ==
                    InstrumentRuntimeDataStateV2::kBoundNoData ||
                data_state ==
                    InstrumentRuntimeDataStateV2::kAvailable);
    }

    [[nodiscard]] bool available() const noexcept {
        return bound() &&
               data_state ==
                   InstrumentRuntimeDataStateV2::kAvailable;
    }
};

struct InstrumentRuntimeSnapshotStorageStatsV2 final {
    // Count of immutable daily-catalog snapshots that can still be read.
    std::size_t live_snapshot_count = 0U;
    // Session high-water mark. Recycled per-ordinal MVCC nodes do not
    // increase it, so this exposes retention mistakes without scanning rows.
    std::size_t allocated_version_node_count = 0U;
};

class DailyInstrumentCatalogSnapshotV2 final {
public:
    DailyInstrumentCatalogSnapshotV2(
        const DailyInstrumentCatalogSnapshotV2&) = delete;
    DailyInstrumentCatalogSnapshotV2& operator=(
        const DailyInstrumentCatalogSnapshotV2&) = delete;
    DailyInstrumentCatalogSnapshotV2(
        DailyInstrumentCatalogSnapshotV2&&) = delete;
    DailyInstrumentCatalogSnapshotV2& operator=(
        DailyInstrumentCatalogSnapshotV2&&) = delete;
    ~DailyInstrumentCatalogSnapshotV2();

    [[nodiscard]] std::uint64_t session_epoch() const noexcept;
    [[nodiscard]] std::uint32_t trade_date() const noexcept;
    [[nodiscard]] std::uint64_t catalog_version() const noexcept;
    [[nodiscard]] std::size_t capacity() const noexcept;
    [[nodiscard]] InstrumentCatalogScopeV2 catalog_scope()
        const noexcept;
    [[nodiscard]] bool coverage_complete() const noexcept;
    [[nodiscard]] std::uint64_t catalog_generation() const noexcept;
    [[nodiscard]] std::uint64_t data_state_generation() const noexcept;
    [[nodiscard]] const l2flow::common::Sha256Digest& catalog_digest()
        const noexcept;

    [[nodiscard]] std::size_t bound_count() const noexcept;
    [[nodiscard]] std::size_t available_count() const noexcept;
    [[nodiscard]] std::size_t snapshot_available_count() const noexcept;
    [[nodiscard]] std::size_t tick_available_count() const noexcept;
    [[nodiscard]] std::size_t factor_eligible_count() const noexcept;

    // The snapshot contains the complete immutable daily catalog.
    // Lookups are allocation-free. The current published
    // snapshot is O(1); a retained older snapshot walks only this ordinal's
    // sparse post-snapshot versions, never the catalog prefix.
    [[nodiscard]] InstrumentRuntimeStateErrorV2 EntryAt(
        std::size_t ordinal,
        InstrumentRuntimeEntryViewV2* output) const noexcept;
    [[nodiscard]] InstrumentRuntimeStateErrorV2 LookupById(
        std::uint32_t instrument_id,
        InstrumentRuntimeEntryViewV2* output) const noexcept;

private:
    class Impl;
    explicit DailyInstrumentCatalogSnapshotV2(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;

    friend class InstrumentRuntimeStateV2;
};

class InstrumentRuntimeStateV2 final {
public:
    InstrumentRuntimeStateV2(
        const InstrumentRuntimeStateV2&) = delete;
    InstrumentRuntimeStateV2& operator=(
        const InstrumentRuntimeStateV2&) = delete;
    InstrumentRuntimeStateV2(
        InstrumentRuntimeStateV2&&) = delete;
    InstrumentRuntimeStateV2& operator=(
        InstrumentRuntimeStateV2&&) = delete;
    ~InstrumentRuntimeStateV2();

    // Copies the canonical dense identity table once before SDK Connect.
    // The returned object cannot add, delete, or renumber identities.
    [[nodiscard]] static InstrumentRuntimeStateErrorV2 Create(
        const DailyInstrumentCatalogV2& catalog,
        std::unique_ptr<InstrumentRuntimeStateV2>* output) noexcept;

    [[nodiscard]] InstrumentRuntimeStateErrorV2 LookupByKey(
        const InstrumentKeyViewV1& key,
        InstrumentRuntimeEntryViewV2* output) const noexcept;
    [[nodiscard]] InstrumentRuntimeStateErrorV2 LookupByKey(
        const InstrumentKeyV1& key,
        InstrumentRuntimeEntryViewV2* output) const noexcept;
    // Both identity lookups use direct slot arithmetic and are fixed O(1).
    [[nodiscard]] InstrumentRuntimeStateErrorV2 LookupById(
        std::uint32_t instrument_id,
        InstrumentRuntimeEntryViewV2* output) const noexcept;
    [[nodiscard]] InstrumentRuntimeStateErrorV2 LookupByOrdinal(
        std::size_t ordinal,
        InstrumentRuntimeEntryViewV2* output) const noexcept;
    // Minimal existing-ID hot path: one direct ordinal calculation and an
    // immutable dense-identity check. It copies no key bytes.
    [[nodiscard]] InstrumentRuntimeStateErrorV2 ResolveBoundId(
        std::uint32_t instrument_id,
        std::size_t* ordinal) const noexcept;

    // A successful call records one applied event. First/last ingress are the
    // numeric minimum/maximum, because different source lanes may complete out
    // of global ingress order. Availability and per-kind counts transition
    // only once.
    [[nodiscard]] InstrumentRuntimeStateErrorV2 MarkApplied(
        std::size_t ordinal,
        std::uint32_t instrument_id,
        InstrumentRuntimeDataKindV2 kind,
        std::uint64_t ingress_sequence) noexcept;

    // Eligibility is reversible. Enabling it requires an available snapshot,
    // which makes factor_eligible_count <= snapshot_available_count an enforced
    // runtime-state invariant.
    [[nodiscard]] InstrumentRuntimeStateErrorV2 SetFactorEligible(
        std::size_t ordinal,
        std::uint32_t instrument_id,
        bool eligible) noexcept;

    // Publishes generations, digest, counts, and each catalog entry's data
    // state in O(1) catalog work: it never scans/copies identity
    // rows. Sparse MVCC preserves one old row on that ordinal's first mutation
    // after a publication; unchanged state reuses the same immutable snapshot.
    // Nodes whose state interval contains no live snapshot epoch are recycled
    // on that ordinal's next mutation, so storage is proportional to retained
    // snapshot versions rather than session duration. The snapshot shares
    // stable slot/key storage and remains usable after runtime destruction.
    [[nodiscard]] InstrumentRuntimeStateErrorV2 AcquireSnapshot(
        std::shared_ptr<const DailyInstrumentCatalogSnapshotV2>* output)
        const noexcept;

    // O(1) operational diagnostics for detecting readers that retain old
    // generations and the resulting snapshot-version memory high-water mark.
    [[nodiscard]] InstrumentRuntimeStateErrorV2
    SnapshotStorageStats(
        InstrumentRuntimeSnapshotStorageStatsV2* output)
        const noexcept;

    [[nodiscard]] std::uint64_t session_epoch() const noexcept;
    [[nodiscard]] std::uint32_t trade_date() const noexcept;
    [[nodiscard]] std::uint64_t catalog_version() const noexcept;
    [[nodiscard]] std::size_t capacity() const noexcept;
private:
    class Impl;
    explicit InstrumentRuntimeStateV2(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

// Minimal source-compatibility aliases for downstream C++ readers. Dynamic
// observed-universe construction and binding APIs intentionally have no
// compatibility declarations.
using ObservedInstrumentCatalogScopeV2 = InstrumentCatalogScopeV2;
using ObservedInstrumentBindingStateV2 = InstrumentRuntimeDataStateV2;
using ObservedInstrumentDataKindV2 = InstrumentRuntimeDataKindV2;
using ObservedInstrumentMetadataV2 = InstrumentRuntimeMetadataV2;
using ObservedInstrumentDirectoryErrorV2 = InstrumentRuntimeStateErrorV2;
using ObservedInstrumentEntryViewV2 = InstrumentRuntimeEntryViewV2;
using ObservedInstrumentSnapshotStorageStatsV2 =
    InstrumentRuntimeSnapshotStorageStatsV2;
using ObservedInstrumentCatalogSnapshotV2 =
    DailyInstrumentCatalogSnapshotV2;
using ObservedInstrumentDirectoryV2 = InstrumentRuntimeStateV2;

[[nodiscard]] inline std::string_view
ObservedInstrumentDirectoryErrorNameV2(
    ObservedInstrumentDirectoryErrorV2 error) noexcept {
    return InstrumentRuntimeStateErrorNameV2(error);
}

}  // namespace l2flow::market
