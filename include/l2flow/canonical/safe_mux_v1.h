#pragma once

#include "l2flow/canonical/canonical_schema_v1.h"
#include "l2flow/canonical/source_frontier_v1.h"

#include <cstddef>
#include <cstdint>
#include <span>

namespace l2flow::canonical {

// Stable deterministic receive-time key.  It is not an exchange-causality
// claim; it is only a reproducible order after every required source has
// proved that no smaller key can still appear in the same full clock epoch.
struct CanonicalEventKeyV1 final {
    std::int64_t recv_monotonic_ns = 0;
    std::uint32_t source_stream_id = 0U;
    std::uint64_t origin_ingress_sequence = 0U;
    std::uint8_t sub_index = 0U;

    [[nodiscard]] friend constexpr bool operator==(
        const CanonicalEventKeyV1&,
        const CanonicalEventKeyV1&) noexcept = default;
};

[[nodiscard]] constexpr bool CanonicalEventKeyLessV1(
    const CanonicalEventKeyV1& left,
    const CanonicalEventKeyV1& right) noexcept {
    if (left.recv_monotonic_ns != right.recv_monotonic_ns) {
        return left.recv_monotonic_ns < right.recv_monotonic_ns;
    }
    if (left.source_stream_id != right.source_stream_id) {
        return left.source_stream_id < right.source_stream_id;
    }
    if (left.origin_ingress_sequence !=
        right.origin_ingress_sequence) {
        return left.origin_ingress_sequence <
               right.origin_ingress_sequence;
    }
    return left.sub_index < right.sub_index;
}

struct SafeMuxInputV1 final {
    bool required = true;
    bool has_next = false;
    CanonicalEventKeyV1 next_key{};
    std::uint64_t next_origin_wal_end_pos = 0U;
    ClockEpochIdentityV1 next_clock_epoch{};
    // Namespace carried by the reader batch that supplied next_key.  It must
    // match the exact frontier generation, not merely source_stream_id.
    std::uint32_t next_capture_date = 0U;
    l2flow::common::Identity128 next_stream_day_id{};
    l2flow::common::Identity128 next_writer_instance{};
    std::uint64_t next_generation = 0U;
    // Borrowed live revocation/progress authority.  It is mandatory for every
    // required input, including one with a queued next record.  The mux reads
    // the page at decision time; a caller-saved SourceFrontier snapshot is not
    // a sufficient generation-health proof because FATAL may be latched after
    // that snapshot was taken.  The page must outlive this input and the call.
    const SourceFrontierPageV1* frontier_page = nullptr;
};

enum class SafeMuxDecisionV1 : std::uint8_t {
    kReady = 0U,
    kBlockedNoCandidate,
    kBlockedFrontier,
    kBlockedUnhealthy,
    kClockEpochBarrier,
    kInvalidInput,
};

struct SafeMuxSelectionV1 final {
    SafeMuxDecisionV1 decision =
        SafeMuxDecisionV1::kBlockedNoCandidate;
    std::size_t input_index = 0U;
    CanonicalEventKeyV1 key{};
    ClockEpochIdentityV1 clock_epoch{};

    [[nodiscard]] bool ready() const noexcept {
        return decision == SafeMuxDecisionV1::kReady;
    }
};

[[nodiscard]] SafeMuxSelectionV1 SelectSafeMuxCandidateV1(
    std::span<const SafeMuxInputV1> inputs) noexcept;

// kReady is a point-in-time proof, not an irrevocable lease.  Consumers must
// tag derived state with every participating source generation and discard
// that state if a live page later becomes FATAL; retaining an old selection
// does not bypass the next live-page check.

enum class SnapshotAsofProofV1 : std::uint8_t {
    kReady = 0U,
    kSnapshotPending,
    kBlockedFrontier,
    kBlockedUnhealthy,
    kClockEpochBarrier,
    kInvalidInput,
};

// Proves that all snapshot events with receive time <= tick time have been
// consumed.  Equality is intentionally not enough: an unseen/eagerly queued
// snapshot at exactly tick time belongs in the historical as-of result.
[[nodiscard]] SnapshotAsofProofV1 ProveSnapshotAsofTickV1(
    const CanonicalEventKeyV1& tick,
    const ClockEpochIdentityV1& tick_clock_epoch,
    const SafeMuxInputV1& snapshot_input) noexcept;

struct SnapshotAsofSelectionV1 final {
    bool found = false;
    std::size_t index = 0U;
};

// `consumed_snapshots` must be in CanonicalEventKey order and in the tick's
// already-proved clock epoch.  Returns the last record whose receive time is
// <= tick time; the caller must obtain a kReady proof before using it.
[[nodiscard]] SnapshotAsofSelectionV1 SelectLatestSnapshotAsofV1(
    std::span<const CanonicalEventKeyV1> consumed_snapshots,
    const CanonicalEventKeyV1& tick) noexcept;

}  // namespace l2flow::canonical
