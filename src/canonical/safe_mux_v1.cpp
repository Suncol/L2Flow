#include "l2flow/canonical/safe_mux_v1.h"

#include <algorithm>

namespace l2flow::canonical {
namespace {

[[nodiscard]] bool ValidKey(
    const CanonicalEventKeyV1& key) noexcept {
    return key.recv_monotonic_ns >= 0 &&
           key.source_stream_id != 0U &&
           key.origin_ingress_sequence != 0U;
}

[[nodiscard]] bool ValidClock(
    const ClockEpochIdentityV1& clock) noexcept {
    if (clock.algorithm == 0U) {
        return false;
    }
    return std::any_of(
        clock.digest.begin(), clock.digest.end(),
        [](std::byte value) { return value != std::byte{0}; });
}

[[nodiscard]] bool SameClock(
    const ClockEpochIdentityV1& left,
    const ClockEpochIdentityV1& right) noexcept {
    return left == right;
}

[[nodiscard]] bool ValidState(SourceStateV1 state) noexcept {
    switch (state) {
        case SourceStateV1::kRecovering:
        case SourceStateV1::kHealthy:
        case SourceStateV1::kDisconnected:
        case SourceStateV1::kFatal:
            return true;
    }
    return false;
}

[[nodiscard]] bool ValidFrontier(
    const SourceFrontierV1& frontier) noexcept {
    return frontier.source_stream_id != 0U &&
           frontier.capture_date != 0U &&
           !l2flow::common::IsZeroIdentity(frontier.stream_day_id) &&
           !l2flow::common::IsZeroIdentity(frontier.writer_instance) &&
           frontier.generation != 0U &&
           ValidClock(frontier.clock_epoch) &&
           ValidState(frontier.source_state) &&
           frontier.captured_ingress_sequence >=
               frontier.append_ingress_sequence &&
           frontier.append_ingress_sequence >=
               frontier.processed_ingress_sequence &&
           frontier.append_global_wal_pos >=
               frontier.processed_global_wal_pos &&
           (frontier.append_ingress_sequence ==
                frontier.processed_ingress_sequence ||
            frontier.append_global_wal_pos >
                frontier.processed_global_wal_pos) &&
           (frontier.append_ingress_sequence == 0U ||
            frontier.append_global_wal_pos != 0U) &&
           (frontier.processed_ingress_sequence == 0U ||
            frontier.processed_global_wal_pos != 0U) &&
           frontier.last_appended_recv_monotonic_ns >= 0 &&
           frontier.safe_processed_frontier_ns >= 0 &&
           (frontier.append_ingress_sequence ==
                frontier.processed_ingress_sequence ||
            frontier.last_appended_recv_monotonic_ns >=
                frontier.safe_processed_frontier_ns) &&
           (frontier.quality_flags &
            ~kCanonicalQualityFlagsMaskV1) == 0U;
}

[[nodiscard]] bool NextOriginCovered(
    const SafeMuxInputV1& input,
    const SourceFrontierV1& frontier) noexcept {
    if (input.next_origin_wal_end_pos == 0U ||
        frontier.processed_ingress_sequence == 0U ||
        frontier.processed_global_wal_pos == 0U) {
        return false;
    }
    if (input.next_key.origin_ingress_sequence ==
        frontier.processed_ingress_sequence) {
        return input.next_origin_wal_end_pos <=
               frontier.processed_global_wal_pos;
    }
    return input.next_key.origin_ingress_sequence <
               frontier.processed_ingress_sequence &&
           input.next_origin_wal_end_pos <
               frontier.processed_global_wal_pos;
}

[[nodiscard]] bool NextMatchesFrontier(
    const SafeMuxInputV1& input,
    const SourceFrontierV1& frontier) noexcept {
    return ValidFrontier(frontier) &&
           frontier.source_state != SourceStateV1::kFatal &&
           ValidKey(input.next_key) &&
           ValidClock(input.next_clock_epoch) &&
           input.next_key.source_stream_id ==
               frontier.source_stream_id &&
           input.next_capture_date == frontier.capture_date &&
           input.next_stream_day_id == frontier.stream_day_id &&
           input.next_writer_instance == frontier.writer_instance &&
           input.next_generation == frontier.generation &&
           SameClock(input.next_clock_epoch, frontier.clock_epoch) &&
           NextOriginCovered(input, frontier);
}

[[nodiscard]] bool ReadLiveFrontier(
    const SafeMuxInputV1& input,
    SourceFrontierV1* frontier) noexcept {
    return frontier != nullptr && input.frontier_page != nullptr &&
           ReadSourceFrontierV1(*input.frontier_page, frontier) ==
               SourceFrontierErrorV1::kNone &&
           ValidFrontier(*frontier);
}

}  // namespace

SafeMuxSelectionV1 SelectSafeMuxCandidateV1(
    std::span<const SafeMuxInputV1> inputs) noexcept {
    SafeMuxSelectionV1 result;
    if (inputs.empty()) {
        result.decision = SafeMuxDecisionV1::kInvalidInput;
        return result;
    }

    bool have_required = false;
    bool have_candidate = false;
    for (std::size_t index = 0U; index < inputs.size(); ++index) {
        const SafeMuxInputV1& input = inputs[index];
        if (!input.required) {
            continue;
        }
        have_required = true;
        SourceFrontierV1 frontier{};
        if (!ReadLiveFrontier(input, &frontier)) {
            result.decision = SafeMuxDecisionV1::kInvalidInput;
            return result;
        }
        if (frontier.source_state == SourceStateV1::kFatal) {
            result.decision = SafeMuxDecisionV1::kBlockedUnhealthy;
            return result;
        }
        if (!input.has_next) {
            continue;
        }
        if (!NextMatchesFrontier(input, frontier)) {
            result.decision = SafeMuxDecisionV1::kInvalidInput;
            return result;
        }
        for (std::size_t prior = 0U; prior < index; ++prior) {
            if (inputs[prior].required && inputs[prior].has_next &&
                inputs[prior].next_key == input.next_key) {
                result.decision = SafeMuxDecisionV1::kInvalidInput;
                return result;
            }
        }
        if (!have_candidate || CanonicalEventKeyLessV1(
                                   input.next_key, result.key)) {
            have_candidate = true;
            result.input_index = index;
            result.key = input.next_key;
            result.clock_epoch = input.next_clock_epoch;
        }
    }
    if (!have_required) {
        result.decision = SafeMuxDecisionV1::kInvalidInput;
        return result;
    }
    if (!have_candidate) {
        result.decision = SafeMuxDecisionV1::kBlockedNoCandidate;
        return result;
    }

    for (std::size_t index = 0U; index < inputs.size(); ++index) {
        const SafeMuxInputV1& input = inputs[index];
        if (!input.required) {
            continue;
        }
        // Re-read every required page after candidate selection.  This closes
        // the saved-snapshot hole and makes the result conservative when a
        // generation changes state during selection.
        SourceFrontierV1 frontier{};
        if (!ReadLiveFrontier(input, &frontier)) {
            result.decision = SafeMuxDecisionV1::kInvalidInput;
            return result;
        }
        if (frontier.source_state == SourceStateV1::kFatal) {
            result.decision = SafeMuxDecisionV1::kBlockedUnhealthy;
            return result;
        }
        if (input.has_next) {
            if (!NextMatchesFrontier(input, frontier)) {
                result.decision = SafeMuxDecisionV1::kInvalidInput;
                return result;
            }
            if (index == result.input_index) {
                continue;
            }
            if (!SameClock(input.next_clock_epoch, result.clock_epoch)) {
                result.decision =
                    SafeMuxDecisionV1::kClockEpochBarrier;
                return result;
            }
            // The candidate was selected by the complete stable key, so a
            // known next event cannot precede it here.
            continue;
        }
        if (!SameClock(frontier.clock_epoch, result.clock_epoch)) {
            result.decision = SafeMuxDecisionV1::kClockEpochBarrier;
            return result;
        }
        if (frontier.source_state != SourceStateV1::kHealthy) {
            result.decision = SafeMuxDecisionV1::kBlockedUnhealthy;
            return result;
        }
        if (frontier.safe_processed_frontier_ns <=
            result.key.recv_monotonic_ns) {
            result.decision = SafeMuxDecisionV1::kBlockedFrontier;
            return result;
        }
    }

    // All ordering/frontier predicates above are monotonic for this immutable
    // page generation.  Re-read every live revocation anchor once more after
    // those predicates have been established.  If a source became FATAL
    // between its proof read and a later source's frontier advance, the mux
    // must not combine those observations into a READY result that never
    // existed for one healthy generation set.
    for (const SafeMuxInputV1& input : inputs) {
        if (!input.required) {
            continue;
        }
        SourceFrontierV1 frontier{};
        if (!ReadLiveFrontier(input, &frontier)) {
            result.decision = SafeMuxDecisionV1::kInvalidInput;
            return result;
        }
        if (frontier.source_state == SourceStateV1::kFatal) {
            result.decision = SafeMuxDecisionV1::kBlockedUnhealthy;
            return result;
        }
        if (input.has_next &&
            !NextMatchesFrontier(input, frontier)) {
            result.decision = SafeMuxDecisionV1::kInvalidInput;
            return result;
        }
    }
    result.decision = SafeMuxDecisionV1::kReady;
    return result;
}

SnapshotAsofProofV1 ProveSnapshotAsofTickV1(
    const CanonicalEventKeyV1& tick,
    const ClockEpochIdentityV1& tick_clock_epoch,
    const SafeMuxInputV1& snapshot_input) noexcept {
    if (!ValidKey(tick) || !ValidClock(tick_clock_epoch) ||
        !snapshot_input.required) {
        return SnapshotAsofProofV1::kInvalidInput;
    }
    SourceFrontierV1 frontier{};
    if (!ReadLiveFrontier(snapshot_input, &frontier)) {
        return SnapshotAsofProofV1::kInvalidInput;
    }
    if (frontier.source_state == SourceStateV1::kFatal) {
        return SnapshotAsofProofV1::kBlockedUnhealthy;
    }
    if (snapshot_input.has_next) {
        if (!NextMatchesFrontier(snapshot_input, frontier)) {
            return SnapshotAsofProofV1::kInvalidInput;
        }
        if (!SameClock(
                snapshot_input.next_clock_epoch,
                tick_clock_epoch)) {
            return SnapshotAsofProofV1::kClockEpochBarrier;
        }
        return snapshot_input.next_key.recv_monotonic_ns <=
                       tick.recv_monotonic_ns
                   ? SnapshotAsofProofV1::kSnapshotPending
                   : SnapshotAsofProofV1::kReady;
    }
    if (!SameClock(
            frontier.clock_epoch,
            tick_clock_epoch)) {
        return SnapshotAsofProofV1::kClockEpochBarrier;
    }
    if (frontier.source_state != SourceStateV1::kHealthy) {
        return SnapshotAsofProofV1::kBlockedUnhealthy;
    }
    return frontier.safe_processed_frontier_ns >
                   tick.recv_monotonic_ns
               ? SnapshotAsofProofV1::kReady
               : SnapshotAsofProofV1::kBlockedFrontier;
}

SnapshotAsofSelectionV1 SelectLatestSnapshotAsofV1(
    std::span<const CanonicalEventKeyV1> consumed_snapshots,
    const CanonicalEventKeyV1& tick) noexcept {
    SnapshotAsofSelectionV1 result;
    if (!ValidKey(tick)) {
        return result;
    }
    for (std::size_t index = 0U; index < consumed_snapshots.size();
         ++index) {
        const CanonicalEventKeyV1& current = consumed_snapshots[index];
        if (!ValidKey(current) ||
            (index != 0U && CanonicalEventKeyLessV1(
                                current,
                                consumed_snapshots[index - 1U]))) {
            return SnapshotAsofSelectionV1{};
        }
        if (current.recv_monotonic_ns > tick.recv_monotonic_ns) {
            break;
        }
        result.found = true;
        result.index = index;
    }
    return result;
}

}  // namespace l2flow::canonical
