#pragma once

#include "l2flow/common/sha256.h"
#include "l2flow/market/instrument_identity_v2.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string_view>

namespace l2flow::market {

inline constexpr std::size_t
    kObservedInstrumentDirectoryDefaultCapacityV2 = 65'536U;

// This directory never claims that its bound prefix is an authoritative
// exchange universe. It contains only identities observed by this process in
// capture order.
enum class ObservedInstrumentCatalogScopeV2 : std::uint8_t {
    kObservedOnly = 1U,
};

enum class ObservedInstrumentBindingStateV2 : std::uint8_t {
    kUnbound = 0U,
    kBinding,
    kBoundNoData,
    kAvailable,
};

enum class ObservedInstrumentDataKindV2 : std::uint8_t {
    kSnapshot = 0U,
    kTick,
};

struct ObservedInstrumentMetadataV2 final {
    QuantityUnitV1 quantity_unit = QuantityUnitV1::kUnknown;
    SecurityTypeV1 security_type = SecurityTypeV1::kUnknown;
    AssetScopeV1 asset_scope = AssetScopeV1::kUnknown;
};

struct ObservedInstrumentDirectoryConfigV2 final {
    // Production uses the default. A smaller value is supported so boundary
    // behavior can be tested without allocating a production-sized directory.
    std::size_t capacity =
        kObservedInstrumentDirectoryDefaultCapacityV2;
    // instrument_id is session-scoped. Callers must pair it with this nonzero
    // epoch and must not carry an ID into another directory session.
    std::uint64_t session_epoch = 0U;
};

enum class ObservedInstrumentDirectoryErrorV2 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kInvalidDataKind,
    kInvalidKey,
    kInvalidMetadata,
    kInvalidSequence,
    kSequenceNotIncreasing,
    kNotFound,
    kBindingInProgress,
    kMetadataConflict,
    kIdentityMismatch,
    kCapacityExhausted,
    kPrerequisiteUnavailable,
    kGenerationExhausted,
    kHashFailure,
    kResourceExhausted,
    kUnexpectedFailure,
};

[[nodiscard]] std::string_view
ObservedInstrumentDirectoryErrorNameV2(
    ObservedInstrumentDirectoryErrorV2 error) noexcept;

// Key spans borrow immutable slot storage. For a directory lookup they remain
// valid while the directory lives. For a CatalogSnapshot lookup they remain
// valid while that snapshot lives.
struct ObservedInstrumentEntryViewV2 final {
    std::uint32_t instrument_id = 0U;
    std::size_t ordinal = std::numeric_limits<std::size_t>::max();
    InstrumentKeyViewV1 key{};
    ObservedInstrumentMetadataV2 metadata{};
    std::uint64_t first_capture_sequence = 0U;
    ObservedInstrumentBindingStateV2 binding_state =
        ObservedInstrumentBindingStateV2::kUnbound;
    bool has_snapshot = false;
    bool has_tick = false;
    bool factor_eligible = false;
    std::uint64_t first_ingress_sequence = 0U;
    std::uint64_t last_ingress_sequence = 0U;

    [[nodiscard]] bool bound() const noexcept {
        return instrument_id != 0U &&
               ordinal != std::numeric_limits<std::size_t>::max() &&
               (binding_state ==
                    ObservedInstrumentBindingStateV2::kBoundNoData ||
                binding_state ==
                    ObservedInstrumentBindingStateV2::kAvailable);
    }

    [[nodiscard]] bool available() const noexcept {
        return bound() &&
               binding_state ==
                   ObservedInstrumentBindingStateV2::kAvailable;
    }
};

struct ObservedInstrumentBindResultV2 final {
    ObservedInstrumentEntryViewV2 entry{};
    bool newly_bound = false;
    // Coherent identity status at the BindOrGet linearization point. This
    // lets the IPC writer publish one new row and its rolling catalog identity
    // without allocating or rescanning the bound prefix on the router path.
    std::uint64_t catalog_generation = 0U;
    std::size_t bound_count = 0U;
    l2flow::common::Sha256Digest catalog_digest{};
};

struct ObservedInstrumentSnapshotStorageStatsV2 final {
    // Count of immutable CatalogSnapshots that can still be read.
    std::size_t live_snapshot_count = 0U;
    // Session high-water mark. Recycled per-ordinal MVCC nodes do not
    // increase it, so this exposes retention mistakes without scanning rows.
    std::size_t allocated_version_node_count = 0U;
};

class ObservedInstrumentBindingSinkV2 {
public:
    virtual ~ObservedInstrumentBindingSinkV2() = default;

    // Called only for newly_bound results, on the single capture-ordered
    // in-memory dispatcher. Implementations must be bounded, noexcept, and
    // must publish the immutable row before advancing visible bound_count.
    [[nodiscard]] virtual bool PublishObservedInstrumentBinding(
        const ObservedInstrumentBindResultV2& binding) noexcept = 0;
};

class ObservedInstrumentCatalogSnapshotV2 final {
public:
    ObservedInstrumentCatalogSnapshotV2(
        const ObservedInstrumentCatalogSnapshotV2&) = delete;
    ObservedInstrumentCatalogSnapshotV2& operator=(
        const ObservedInstrumentCatalogSnapshotV2&) = delete;
    ObservedInstrumentCatalogSnapshotV2(
        ObservedInstrumentCatalogSnapshotV2&&) = delete;
    ObservedInstrumentCatalogSnapshotV2& operator=(
        ObservedInstrumentCatalogSnapshotV2&&) = delete;
    ~ObservedInstrumentCatalogSnapshotV2();

    [[nodiscard]] std::uint64_t session_epoch() const noexcept;
    [[nodiscard]] std::size_t capacity() const noexcept;
    [[nodiscard]] ObservedInstrumentCatalogScopeV2 catalog_scope()
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

    // The snapshot contains exactly the immutable bound ordinal prefix
    // [0, bound_count()). Lookups are allocation-free. The current published
    // snapshot is O(1); a retained older snapshot walks only this ordinal's
    // sparse post-snapshot versions, never the catalog prefix.
    [[nodiscard]] ObservedInstrumentDirectoryErrorV2 EntryAt(
        std::size_t ordinal,
        ObservedInstrumentEntryViewV2* output) const noexcept;
    [[nodiscard]] ObservedInstrumentDirectoryErrorV2 LookupById(
        std::uint32_t instrument_id,
        ObservedInstrumentEntryViewV2* output) const noexcept;

private:
    class Impl;
    explicit ObservedInstrumentCatalogSnapshotV2(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;

    friend class ObservedInstrumentDirectoryV2;
};

class ObservedInstrumentDirectoryV2 final {
public:
    ObservedInstrumentDirectoryV2(
        const ObservedInstrumentDirectoryV2&) = delete;
    ObservedInstrumentDirectoryV2& operator=(
        const ObservedInstrumentDirectoryV2&) = delete;
    ObservedInstrumentDirectoryV2(
        ObservedInstrumentDirectoryV2&&) = delete;
    ObservedInstrumentDirectoryV2& operator=(
        ObservedInstrumentDirectoryV2&&) = delete;
    ~ObservedInstrumentDirectoryV2();

    [[nodiscard]] static ObservedInstrumentDirectoryErrorV2 Create(
        ObservedInstrumentDirectoryConfigV2 config,
        std::unique_ptr<ObservedInstrumentDirectoryV2>* output) noexcept;

    // BindOrGet is a single-writer API. Calls must follow strictly increasing
    // capture sequence order. The first new key receives ordinal=bound_count
    // and instrument_id=ordinal+1. A repeated exact key returns the original
    // binding without changing either generation; conflicting metadata fails.
    [[nodiscard]] ObservedInstrumentDirectoryErrorV2 BindOrGet(
        const InstrumentKeyViewV1& key,
        ObservedInstrumentMetadataV2 metadata,
        std::uint64_t first_capture_sequence,
        ObservedInstrumentBindResultV2* output) noexcept;
    [[nodiscard]] ObservedInstrumentDirectoryErrorV2 BindOrGet(
        const InstrumentKeyV1& key,
        ObservedInstrumentMetadataV2 metadata,
        std::uint64_t first_capture_sequence,
        ObservedInstrumentBindResultV2* output) noexcept;

    [[nodiscard]] ObservedInstrumentDirectoryErrorV2 LookupByKey(
        const InstrumentKeyViewV1& key,
        ObservedInstrumentEntryViewV2* output) const noexcept;
    [[nodiscard]] ObservedInstrumentDirectoryErrorV2 LookupByKey(
        const InstrumentKeyV1& key,
        ObservedInstrumentEntryViewV2* output) const noexcept;
    // Both identity lookups use direct slot arithmetic and are fixed O(1).
    [[nodiscard]] ObservedInstrumentDirectoryErrorV2 LookupById(
        std::uint32_t instrument_id,
        ObservedInstrumentEntryViewV2* output) const noexcept;
    [[nodiscard]] ObservedInstrumentDirectoryErrorV2 LookupByOrdinal(
        std::size_t ordinal,
        ObservedInstrumentEntryViewV2* output) const noexcept;
    // Minimal existing-ID hot path: one direct ordinal calculation and one
    // acquire load of the immutable slot's binding state. It does not touch
    // the key index, copy key views, or observe catalog generation.
    [[nodiscard]] ObservedInstrumentDirectoryErrorV2 ResolveBoundId(
        std::uint32_t instrument_id,
        std::size_t* ordinal) const noexcept;

    // A successful call records one applied event. First/last ingress are the
    // numeric minimum/maximum, because different source lanes may complete out
    // of global ingress order. Availability and per-kind counts transition
    // only once.
    [[nodiscard]] ObservedInstrumentDirectoryErrorV2 MarkApplied(
        std::size_t ordinal,
        std::uint32_t instrument_id,
        ObservedInstrumentDataKindV2 kind,
        std::uint64_t ingress_sequence) noexcept;

    // Eligibility is reversible. Enabling it requires an available snapshot,
    // which makes factor_eligible_count <= snapshot_available_count an enforced
    // directory invariant.
    [[nodiscard]] ObservedInstrumentDirectoryErrorV2 SetFactorEligible(
        std::size_t ordinal,
        std::uint32_t instrument_id,
        bool eligible) noexcept;

    // Publishes generations, digest, counts, the bound prefix, and each bound
    // entry's data state in O(1) catalog work: it never scans/copies bound
    // rows. Sparse MVCC preserves one old row on that ordinal's first mutation
    // after a publication; unchanged state reuses the same immutable snapshot.
    // Nodes whose state interval contains no live snapshot epoch are recycled
    // on that ordinal's next mutation, so storage is proportional to retained
    // snapshot versions rather than session duration. The snapshot shares
    // stable slot/key storage and remains usable after directory destruction.
    [[nodiscard]] ObservedInstrumentDirectoryErrorV2 AcquireSnapshot(
        std::shared_ptr<const ObservedInstrumentCatalogSnapshotV2>* output)
        const noexcept;

    // O(1) operational diagnostics for detecting readers that retain old
    // generations and the resulting snapshot-version memory high-water mark.
    [[nodiscard]] ObservedInstrumentDirectoryErrorV2
    SnapshotStorageStats(
        ObservedInstrumentSnapshotStorageStatsV2* output)
        const noexcept;

    [[nodiscard]] std::uint64_t session_epoch() const noexcept;
    [[nodiscard]] std::size_t capacity() const noexcept;

private:
    class Impl;
    explicit ObservedInstrumentDirectoryV2(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::market
