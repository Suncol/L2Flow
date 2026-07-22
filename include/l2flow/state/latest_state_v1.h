#pragma once

#include "l2flow/canonical/canonical_schema_v1.h"
#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/state/latest_state_c_api_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

namespace l2flow::state {

inline constexpr std::uint32_t kLatestStateSlotMagicV1 =
    0x3154534cU;  // "LST1" as little-endian bytes.
inline constexpr std::uint16_t kLatestStateSlotSchemaVersionV1 = 1U;
inline constexpr std::size_t kLatestStateSlotBytesV1 = 4096U;
inline constexpr std::size_t kLatestStateSlotAlignmentV1 = 64U;
inline constexpr std::size_t kLatestStatePhaseCountV1 = 8U;

// The shared-memory ABI is deliberately opaque.  Concurrent access must go
// through the helpers in this module; in particular, no process may reinterpret
// any byte as std::atomic<T>.  The frozen byte layout is described by
// LatestStateSchemaDescriptorV1().
struct alignas(kLatestStateSlotAlignmentV1) LatestStateSlotV1 final {
    std::array<std::byte, kLatestStateSlotBytesV1> bytes{};
};

static_assert(sizeof(LatestStateSlotV1) == 4096U);
static_assert(alignof(LatestStateSlotV1) == 64U);

// Identity shared by every slot in one state-table generation.  The caller
// must exclusively lease state_writer_instance; the embedded value fences a
// stale owner but cannot make two processes that reuse the same token valid.
// A new registry, schema, dtype, shard topology, or state generation requires
// a new table generation; it is never changed in-place in an initialized slot.
struct LatestStateConfigV1 final {
    std::uint64_t state_generation = 0U;
    l2flow::common::Identity128 state_writer_instance{};
    std::uint32_t shard_id = 0U;
    std::uint32_t shard_count = 0U;
    l2flow::common::Sha256Digest canonical_schema_sha256{};
    l2flow::common::Sha256Digest canonical_dtype_sha256{};
    std::uint64_t registry_version = 0U;
    l2flow::common::Sha256Digest registry_sha256{};

    [[nodiscard]] friend constexpr bool operator==(
        const LatestStateConfigV1&,
        const LatestStateConfigV1&) noexcept = default;
};

// Exact immutable context from the Canonical segment/frontier that supplied a
// SnapshotRecord.  These facts are not all present in CanonicalHeaderV1, so a
// writer must receive them explicitly instead of filling them with defaults.
struct LatestStateCanonicalOriginV1 final {
    std::uint32_t capture_date = 0U;
    std::uint32_t source_stream_id = 0U;
    l2flow::common::Identity128 stream_day_id{};
    l2flow::common::Identity128 source_writer_instance{};
    std::uint64_t source_generation = 0U;
    std::uint64_t canonical_generation = 0U;
    l2flow::canonical::ClockEpochIdentityV1 clock_epoch{};
    l2flow::common::Sha256Digest canonical_schema_sha256{};
    l2flow::common::Sha256Digest canonical_dtype_sha256{};
    std::uint64_t registry_version = 0U;
    l2flow::common::Sha256Digest registry_sha256{};

    [[nodiscard]] friend constexpr bool operator==(
        const LatestStateCanonicalOriginV1&,
        const LatestStateCanonicalOriginV1&) noexcept = default;
};

using LatestStateSnapshotOriginV1 = LatestStateCanonicalOriginV1;

struct LatestStateTickQualityUpdateV1 final {
    std::uint32_t instrument_id = 0U;
    LatestStateCanonicalOriginV1 origin{};
    std::uint64_t shard_event_id = 0U;
    std::uint64_t origin_ingress_sequence = 0U;
    std::uint64_t origin_wal_end_pos = 0U;
    std::int64_t recv_monotonic_ns = 0;
    std::uint64_t quality_flags = 0U;
};

// Stable decoded value of one slot.  snapshot.header.quality_flags is the
// snapshot quality.  Tick/reconstruction quality is intentionally separate and
// PublishLatestTickQualityV1 never changes snapshot or its quality.
struct LatestStateValueV1 final {
    std::uint64_t slot_sequence = 0U;
    LatestStateConfigV1 config{};
    LatestStateSnapshotOriginV1 origin{};
    l2flow::canonical::CanonicalSnapshotRecordV1 snapshot{};
    bool tick_quality_initialized = false;
    LatestStateCanonicalOriginV1 tick_origin{};
    std::uint64_t tick_shard_event_id = 0U;
    std::uint64_t tick_origin_ingress_sequence = 0U;
    std::uint64_t tick_origin_wal_end_pos = 0U;
    std::int64_t tick_recv_monotonic_ns = 0;
    std::uint64_t tick_quality_flags = 0U;
};

// The caller supplies every threshold.  This library assigns no market-phase
// duration.  Indexes are the frozen numeric values of CanonicalTradingPhaseV1;
// stale means age_ns is strictly greater than the selected max_age_ns.
struct LatestStateStalePolicyV1 final {
    std::array<std::int64_t, kLatestStatePhaseCountV1>
        max_age_ns_by_phase{};
};

static_assert(
    static_cast<std::uint8_t>(
        l2flow::canonical::CanonicalTradingPhaseV1::kEnd) + 1U ==
    kLatestStatePhaseCountV1);

struct LatestStateReadOptionsV1 final {
    std::int64_t now_monotonic_ns = 0;
    l2flow::canonical::ClockEpochIdentityV1 now_clock_epoch{};
    LatestStateStalePolicyV1 stale_policy{};
};

struct LatestStateReadResultV1 final {
    LatestStateValueV1 value{};
    std::int64_t age_ns = 0;
    std::uint64_t effective_snapshot_quality_flags = 0U;
    bool snapshot_stale = false;
};

enum class LatestStateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullArgument,
    kUnsupportedHost,
    kInvalidConfiguration,
    kInvalidOrigin,
    kInvalidRecord,
    kInvalidQualityFlags,
    kUninitialized,
    kCorruptSlot,
    kIdentityMismatch,
    kInstrumentMismatch,
    kStaleCursor,
    kConflictingDuplicate,
    kWriterBusy,
    kSequenceOverflow,
    kReadBusy,
    kClockEpochMismatch,
    kClockRegression,
    kInvalidStalePolicy,
    kNotFound,
    kOutputSizeMismatch,
    kDuplicateInstrument,
    kAllocationFailed,
};

[[nodiscard]] std::string_view LatestStateErrorNameV1(
    LatestStateErrorV1 error) noexcept;

enum class LatestStateUpdateDispositionV1 : std::uint8_t {
    kPublished = 0U,
    kIdempotent = 1U,
};

[[nodiscard]] bool LatestStateHostSupportedV1() noexcept;
[[nodiscard]] bool LatestStateConfigValidV1(
    const LatestStateConfigV1& config) noexcept;
[[nodiscard]] bool LatestStateOriginValidV1(
    const LatestStateCanonicalOriginV1& origin) noexcept;

[[nodiscard]] std::string_view LatestStateSchemaDescriptorV1() noexcept;
[[nodiscard]] l2flow::common::Sha256Digest
LatestStateSchemaDescriptorSha256V1() noexcept;

// Reads and validates one stable initialized slot.  A persistently odd or
// rapidly changing seqlock produces kReadBusy rather than returning torn data.
[[nodiscard]] LatestStateErrorV1 ReadLatestStateSlotV1(
    const LatestStateSlotV1& slot,
    LatestStateValueV1* output) noexcept;

// Validates the complete Canonical SnapshotRecord and exact segment origin,
// then publishes the entire payload in one seqlock transaction.  The caller
// must obtain the record through the Phase-5 committed-reader gate; this local
// function cannot authenticate a Raw envelope or observe a later live-frontier
// FATAL.  A later FATAL requires caller-owned discard of this state generation.
// An exact duplicate is a no-write idempotent success.  Cursors from the same
// immutable source/canonical generation must not regress.
[[nodiscard]] LatestStateErrorV1 PublishLatestSnapshotV1(
    const LatestStateConfigV1& config,
    const LatestStateSnapshotOriginV1& origin,
    const l2flow::canonical::CanonicalSnapshotRecordV1& snapshot,
    LatestStateSlotV1* slot,
    LatestStateUpdateDispositionV1* disposition = nullptr) noexcept;

// Updates only the separate tick-quality lineage/word.  snapshot and
// snapshot.header.quality_flags remain byte-for-byte unchanged.  The same
// committed-input and later-FATAL generation-discard requirement applies.
[[nodiscard]] LatestStateErrorV1 PublishLatestTickQualityV1(
    const LatestStateConfigV1& config,
    const LatestStateTickQualityUpdateV1& update,
    LatestStateSlotV1* slot) noexcept;

// Adds SNAPSHOT_STALE only to the returned effective quality.  The persisted
// snapshot quality is immutable until another SnapshotRecord is published.
[[nodiscard]] LatestStateErrorV1 ReadLatestStateV1(
    const LatestStateSlotV1& slot,
    const LatestStateReadOptionsV1& options,
    LatestStateReadResultV1* output) noexcept;

struct LatestStateBatchRequestV1 final {
    std::uint32_t instrument_id = 0U;
    const LatestStateSlotV1* slot = nullptr;
};

struct LatestStateBatchResultV1 final {
    std::uint32_t instrument_id = 0U;
    LatestStateErrorV1 error = LatestStateErrorV1::kNotFound;
    LatestStateReadResultV1 latest{};
};

[[nodiscard]] LatestStateErrorV1 BatchReadLatestStateV1(
    std::span<const LatestStateBatchRequestV1> requests,
    const LatestStateReadOptionsV1& options,
    std::span<LatestStateBatchResultV1> output) noexcept;

struct LatestStateQueryEntryV1 final {
    std::uint32_t instrument_id = 0U;
    const LatestStateSlotV1* slot = nullptr;
};

// A local, read-only directory over caller-owned SHM slots.  It copies and
// sorts only the (instrument_id, pointer) directory; mappings and slots remain
// caller-owned and must outlive the query object.
class LatestStateLocalQueryV1 final {
public:
    ~LatestStateLocalQueryV1();

    LatestStateLocalQueryV1(const LatestStateLocalQueryV1&) = delete;
    LatestStateLocalQueryV1& operator=(const LatestStateLocalQueryV1&) = delete;
    LatestStateLocalQueryV1(LatestStateLocalQueryV1&&) = delete;
    LatestStateLocalQueryV1& operator=(LatestStateLocalQueryV1&&) = delete;

    [[nodiscard]] static LatestStateErrorV1 Create(
        std::span<const LatestStateQueryEntryV1> entries,
        std::unique_ptr<LatestStateLocalQueryV1>* query);

    [[nodiscard]] LatestStateErrorV1 GetLatest(
        std::uint32_t instrument_id,
        const LatestStateReadOptionsV1& options,
        LatestStateReadResultV1* output) const noexcept;

    [[nodiscard]] LatestStateErrorV1 BatchGetLatest(
        std::span<const std::uint32_t> instrument_ids,
        const LatestStateReadOptionsV1& options,
        std::span<LatestStateBatchResultV1> output) const noexcept;

    // Writes up to output.size() matching entries in increasing instrument ID
    // order and returns kOutputSizeMismatch if additional matches exist.
    [[nodiscard]] LatestStateErrorV1 ScanMarket(
        l2flow::canonical::CanonicalMarketV1 market,
        const LatestStateReadOptionsV1& options,
        std::span<LatestStateBatchResultV1> output,
        std::size_t* written) const noexcept;

private:
    class Impl;
    explicit LatestStateLocalQueryV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::state
