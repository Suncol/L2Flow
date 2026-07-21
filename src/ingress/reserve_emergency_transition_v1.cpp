#include "l2flow/ingress/reserve_emergency_transition_v1.h"

#include <cstddef>
#include <cstdint>
#include <limits>

namespace l2flow::ingress {
namespace {

struct CandidateContextV1 final {
    ReserveStateV1SlotWire before_wire{};
    ReserveStateSlotV1 candidate{};
};

[[nodiscard]] bool IsZero(
    const ReserveStateV1Identity& value) noexcept {
    return value == ReserveStateV1Identity{};
}

[[nodiscard]] bool IsZero(
    const ReserveStateV1Digest& value) noexcept {
    return value == ReserveStateV1Digest{};
}

[[nodiscard]] bool CheckedAdd(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* output) noexcept {
    if (right >
        std::numeric_limits<std::uint64_t>::max() - left) {
        return false;
    }
    *output = left + right;
    return true;
}

[[nodiscard]] bool CheckedMul(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* output) noexcept {
    if (left != 0U &&
        right >
            std::numeric_limits<std::uint64_t>::max() / left) {
        return false;
    }
    *output = left * right;
    return true;
}

[[nodiscard]] ReserveStateV1Error PrepareCandidate(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    ReserveCoordinatorPhaseV1 required_phase,
    CandidateContextV1* context) noexcept {
    const ReserveStateV1Error before_error =
        EncodeReserveStateSlotV1(
            header, before, &context->before_wire);
    if (before_error != ReserveStateV1Error::kNone) {
        return before_error;
    }
    if (before.coordinator_state != required_phase) {
        return ReserveStateV1Error::kInvalidState;
    }
    if (before.generation ==
        std::numeric_limits<std::uint64_t>::max()) {
        return ReserveStateV1Error::kInvalidGeneration;
    }
    context->candidate = before;
    ++context->candidate.generation;
    return ReserveStateV1Error::kNone;
}

[[nodiscard]] ReserveStateV1Error CommitCandidate(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const CandidateContextV1& context,
    ReserveStateSlotV1* output) noexcept {
    ReserveStateV1SlotWire after_wire{};
    const ReserveStateV1Error after_error =
        EncodeReserveStateSlotV1(
            header, context.candidate, &after_wire);
    if (after_error != ReserveStateV1Error::kNone) {
        return after_error;
    }
    const ReserveStateV1Error pair_error =
        ValidateReserveStateSlotPairV1(
            header,
            before,
            context.before_wire,
            context.candidate,
            after_wire);
    if (pair_error != ReserveStateV1Error::kNone) {
        return pair_error;
    }
    *output = context.candidate;
    return ReserveStateV1Error::kNone;
}

[[nodiscard]] ReserveStateV1Error DerivePreparedAggregates(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveReleasePreparedV1& prepared,
    std::uint64_t* byte_total,
    std::uint64_t* inode_total) noexcept {
    *byte_total = 0U;
    *inode_total = 0U;
    for (const auto& entry : prepared.grant_entries) {
        std::uint64_t action_byte_total = 0U;
        std::uint64_t action_inode_total = 0U;
        for (const auto& action : entry.actions) {
            if (action.action_kind ==
                FinalizationActionKindV1::kUnused) {
                continue;
            }
            std::uint64_t action_bytes = 0U;
            if (!CheckedMul(
                    static_cast<std::uint64_t>(
                        action.byte_cap_quanta),
                    header.allocation_quantum_bytes,
                    &action_bytes) ||
                !CheckedAdd(
                    action_byte_total,
                    action_bytes,
                    &action_byte_total) ||
                !CheckedAdd(
                    action_inode_total,
                    static_cast<std::uint64_t>(
                        action.inode_cap),
                    &action_inode_total)) {
                return ReserveStateV1Error::
                    kArithmeticOverflow;
            }
        }
        if (action_byte_total != entry.grant_bytes) {
            return ReserveStateV1Error::kInvalidAggregate;
        }
        if (!CheckedAdd(
                *byte_total, entry.grant_bytes, byte_total) ||
            !CheckedAdd(
                *inode_total,
                action_inode_total,
                inode_total)) {
            return ReserveStateV1Error::kArithmeticOverflow;
        }
    }
    return ReserveStateV1Error::kNone;
}

[[nodiscard]] ReserveStateV1Error FindGrant(
    const ReserveStateSlotV1& slot,
    const ReserveFinalizationGrantKeyV1& key,
    std::size_t* output_index) noexcept {
    if (slot.finalization_cycle_id !=
        key.finalization_cycle_id) {
        return ReserveStateV1Error::kImmutableFactChanged;
    }
    for (std::size_t index = 0U;
         index < slot.entry_count;
         ++index) {
        const auto& entry = slot.entries[index];
        if (entry.source_stream_id != key.source_stream_id ||
            entry.capture_date != key.capture_date) {
            continue;
        }
        if (entry.stream_day_id != key.stream_day_id ||
            entry.ack_status != key.ack_status ||
            entry.writer_instance != key.ack_writer_instance ||
            entry.safe_stop_template_id !=
                key.safe_stop_template_id) {
            return ReserveStateV1Error::
                kImmutableFactChanged;
        }
        *output_index = index;
        return ReserveStateV1Error::kNone;
    }
    return ReserveStateV1Error::kInvalidEntry;
}

[[nodiscard]] ReserveStateV1Error FindActiveGrant(
    const ReserveStateSlotV1& slot,
    const ReserveFinalizationGrantKeyV1& key,
    std::size_t* output_index) noexcept {
    std::size_t index = 0U;
    const ReserveStateV1Error find_error =
        FindGrant(slot, key, &index);
    if (find_error != ReserveStateV1Error::kNone) {
        return find_error;
    }
    if (slot.active_entry_index != index ||
        slot.entries[index].grant_status !=
            ReserveGrantStatusV1::kActive) {
        return ReserveStateV1Error::kInvalidTransition;
    }
    *output_index = index;
    return ReserveStateV1Error::kNone;
}

[[nodiscard]] ReserveStateV1Error ValidateActionKey(
    const ReserveStateEntryV1& entry,
    const ReserveFinalizationActionKeyV1& key,
    std::size_t* output_index) noexcept {
    const std::size_t index = key.action_id;
    if (index >= kReserveStateV1ActionCapacity) {
        return ReserveStateV1Error::kInvalidReceipt;
    }
    const auto& action = entry.actions[index];
    if (action.action_kind ==
        FinalizationActionKindV1::kUnused) {
        return ReserveStateV1Error::kInvalidTransition;
    }
    if (action.action_id != key.action_id ||
        action.action_kind != key.action_kind ||
        action.object_plan_sha256 !=
            key.object_plan_sha256) {
        return ReserveStateV1Error::kImmutableFactChanged;
    }
    *output_index = index;
    return ReserveStateV1Error::kNone;
}

[[nodiscard]] std::size_t FindFirstPendingGrant(
    const ReserveStateSlotV1& slot) noexcept {
    for (std::size_t index = 0U;
         index < slot.entry_count;
         ++index) {
        if (slot.entries[index].grant_status ==
            ReserveGrantStatusV1::kPending) {
            return index;
        }
    }
    return kReserveStateV1EntryCapacity;
}

[[nodiscard]] bool HasFailedGrant(
    const ReserveStateSlotV1& slot) noexcept {
    for (std::size_t index = 0U;
         index < slot.entry_count;
         ++index) {
        if (slot.entries[index].grant_status ==
            ReserveGrantStatusV1::kFailed) {
            return true;
        }
    }
    return false;
}

[[nodiscard]] std::size_t FindFirstPendingAction(
    const ReserveStateEntryV1& entry) noexcept {
    for (std::size_t index = 0U;
         index < kReserveStateV1ActionCapacity;
         ++index) {
        const auto state = entry.actions[index].action_state;
        if (state == FinalizationActionStateV1::kUnused) {
            break;
        }
        if (state == FinalizationActionStateV1::kPending) {
            return index;
        }
    }
    return kReserveStateV1ActionCapacity;
}

[[nodiscard]] std::size_t FindDebitedAction(
    const ReserveStateEntryV1& entry) noexcept {
    for (std::size_t index = 0U;
         index < kReserveStateV1ActionCapacity;
         ++index) {
        const auto state = entry.actions[index].action_state;
        if (state == FinalizationActionStateV1::kUnused) {
            break;
        }
        if (state == FinalizationActionStateV1::kDebited) {
            return index;
        }
    }
    return kReserveStateV1ActionCapacity;
}

struct ActionCompletionFactsV1 final {
    std::size_t used_count = 0U;
    std::size_t complete_count = 0U;
    bool all_complete = false;
};

[[nodiscard]] ActionCompletionFactsV1 GetCompletionFacts(
    const ReserveStateEntryV1& entry) noexcept {
    ActionCompletionFactsV1 result{};
    for (std::size_t index = 0U;
         index < kReserveStateV1ActionCapacity;
         ++index) {
        const auto state = entry.actions[index].action_state;
        if (state == FinalizationActionStateV1::kUnused) {
            break;
        }
        ++result.used_count;
        if (state == FinalizationActionStateV1::kComplete) {
            ++result.complete_count;
        }
    }
    result.all_complete =
        result.used_count != 0U &&
        result.complete_count == result.used_count;
    return result;
}

void MarkGrantTerminal(
    ReserveStateSlotV1* slot,
    std::size_t entry_index,
    ReserveGrantStatusV1 status) noexcept {
    slot->entries[entry_index].grant_status = status;
    slot->active_entry_index = kReserveStateV1NoActiveEntry;
    slot->active_fs_free_inode_baseline = 0U;
    slot->active_quota_free_inode_baseline = 0U;
    if (status == ReserveGrantStatusV1::kDone) {
        slot->completed_bitmap |= static_cast<std::uint16_t>(
            std::uint16_t{1U} << entry_index);
    }
}

}  // namespace

ReserveStateV1Error BuildReserveReleasingIntentV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveReleaseIntentV1& intent,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    const ReserveStateV1Error prepare_error =
        PrepareCandidate(
            header,
            before,
            ReserveCoordinatorPhaseV1::kProvisioned,
            &context);
    if (prepare_error != ReserveStateV1Error::kNone) {
        return prepare_error;
    }
    if (intent.reason == ReserveReleaseReasonV1::kNone ||
        intent.trigger == ReserveReleaseTriggerV1::kNone) {
        return ReserveStateV1Error::kInvalidState;
    }
    if (IsZero(intent.writer_set_sha256)) {
        return ReserveStateV1Error::kInvalidIdentity;
    }
    context.candidate.coordinator_state =
        ReserveCoordinatorPhaseV1::kReleasingIntent;
    context.candidate.reason = intent.reason;
    context.candidate.trigger = intent.trigger;
    context.candidate.writer_set_sha256 =
        intent.writer_set_sha256;
    return CommitCandidate(
        header, before, context, output);
}

ReserveStateV1Error BuildReserveReleasingPreparedV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveReleasePreparedV1& prepared,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    const ReserveStateV1Error prepare_error =
        PrepareCandidate(
            header,
            before,
            ReserveCoordinatorPhaseV1::kReleasingIntent,
            &context);
    if (prepare_error != ReserveStateV1Error::kNone) {
        return prepare_error;
    }
    if (prepared.grant_entries.size() != before.entry_count) {
        return ReserveStateV1Error::kInvalidCount;
    }
    if (IsZero(prepared.finalization_cycle_id)) {
        return ReserveStateV1Error::kInvalidIdentity;
    }

    std::uint64_t aggregate_bytes = 0U;
    std::uint64_t aggregate_inodes = 0U;
    const ReserveStateV1Error aggregate_error =
        DerivePreparedAggregates(
            header,
            prepared,
            &aggregate_bytes,
            &aggregate_inodes);
    if (aggregate_error != ReserveStateV1Error::kNone) {
        return aggregate_error;
    }

    ReserveStateSlotV1 candidate{};
    candidate.coordinator_state =
        ReserveCoordinatorPhaseV1::kReleasingPrepared;
    candidate.generation = context.candidate.generation;
    candidate.reserve_state_uuid = before.reserve_state_uuid;
    candidate.reason = before.reason;
    candidate.trigger = before.trigger;
    candidate.entry_count = before.entry_count;
    candidate.finalization_cycle_id =
        prepared.finalization_cycle_id;
    candidate.writer_set_sha256 = before.writer_set_sha256;
    candidate.aggregate_grant_bytes = aggregate_bytes;
    candidate.aggregate_inode_grant = aggregate_inodes;
    candidate.pre_release_fs_free_bytes =
        prepared.pre_release_fs_free_bytes;
    candidate.pre_release_quota_free_bytes =
        prepared.pre_release_quota_free_bytes;
    candidate.pre_release_fs_free_inodes =
        prepared.pre_release_fs_free_inodes;
    candidate.pre_release_quota_free_inodes =
        prepared.pre_release_quota_free_inodes;
    candidate.expected_release_fs_bytes =
        prepared.expected_release_fs_bytes;
    candidate.expected_release_quota_bytes =
        prepared.expected_release_quota_bytes;
    candidate.expected_release_fs_inodes =
        prepared.expected_release_fs_inodes;
    candidate.expected_release_quota_inodes =
        prepared.expected_release_quota_inodes;
    candidate.reserved_margin_bytes =
        prepared.reserved_margin_bytes;
    candidate.reserved_margin_inodes =
        prepared.reserved_margin_inodes;
    candidate.effective_min_fs_free_bytes =
        prepared.effective_min_fs_free_bytes;
    candidate.effective_min_quota_free_bytes =
        prepared.effective_min_quota_free_bytes;
    candidate.effective_min_fs_free_inodes =
        prepared.effective_min_fs_free_inodes;
    candidate.effective_min_quota_free_inodes =
        prepared.effective_min_quota_free_inodes;
    for (std::size_t index = 0U;
         index < prepared.grant_entries.size();
         ++index) {
        candidate.entries[index] =
            prepared.grant_entries[index];
    }
    context.candidate = candidate;
    return CommitCandidate(
        header, before, context, output);
}

ReserveStateV1Error BuildReserveConsumedV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    const ReserveStateV1Error prepare_error =
        PrepareCandidate(
            header,
            before,
            ReserveCoordinatorPhaseV1::kReleasingPrepared,
            &context);
    if (prepare_error != ReserveStateV1Error::kNone) {
        return prepare_error;
    }
    context.candidate.coordinator_state =
        ReserveCoordinatorPhaseV1::kConsumed;
    return CommitCandidate(
        header, before, context, output);
}

ReserveStateV1Error BuildReserveActivateNextGrantV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveGrantActivationV1& activation,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    const ReserveStateV1Error prepare_error =
        PrepareCandidate(
            header,
            before,
            ReserveCoordinatorPhaseV1::kConsumed,
            &context);
    if (prepare_error != ReserveStateV1Error::kNone) {
        return prepare_error;
    }
    if (IsZero(activation.executor_instance)) {
        return ReserveStateV1Error::kInvalidIdentity;
    }
    if (activation.filesystem_free_byte_baseline == 0U ||
        activation.quota_free_byte_baseline == 0U ||
        activation.filesystem_free_inode_baseline == 0U ||
        activation.quota_free_inode_baseline == 0U) {
        return ReserveStateV1Error::kInvalidActivation;
    }
    if (before.active_entry_index !=
            kReserveStateV1NoActiveEntry ||
        HasFailedGrant(before)) {
        return ReserveStateV1Error::kInvalidTransition;
    }

    std::size_t entry_index = 0U;
    const ReserveStateV1Error find_error =
        FindGrant(before, activation.grant, &entry_index);
    if (find_error != ReserveStateV1Error::kNone) {
        return find_error;
    }
    if (before.entries[entry_index].grant_status !=
        ReserveGrantStatusV1::kPending) {
        return ReserveStateV1Error::kInvalidTransition;
    }
    if (entry_index != FindFirstPendingGrant(before)) {
        return ReserveStateV1Error::kInvalidOrdering;
    }
    const auto& before_entry = before.entries[entry_index];
    if (before_entry.ack_status ==
            ReserveAckStatusV1::kAcked &&
        activation.executor_instance !=
            before_entry.writer_instance) {
        return ReserveStateV1Error::kImmutableFactChanged;
    }

    auto& entry = context.candidate.entries[entry_index];
    entry.grant_status = ReserveGrantStatusV1::kActive;
    entry.executor_or_recovery_attempt =
        activation.executor_instance;
    entry.activation_fs_free_baseline =
        activation.filesystem_free_byte_baseline;
    entry.activation_quota_free_baseline =
        activation.quota_free_byte_baseline;
    entry.activation_remaining_cap = entry.grant_bytes;
    entry.precharged_bytes = entry.grant_bytes;
    context.candidate.active_entry_index =
        static_cast<std::uint16_t>(entry_index);
    context.candidate.active_fs_free_inode_baseline =
        activation.filesystem_free_inode_baseline;
    context.candidate.active_quota_free_inode_baseline =
        activation.quota_free_inode_baseline;
    return CommitCandidate(
        header, before, context, output);
}

ReserveStateV1Error BuildReserveDebitActionV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveFinalizationActionKeyV1& action,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    const ReserveStateV1Error prepare_error =
        PrepareCandidate(
            header,
            before,
            ReserveCoordinatorPhaseV1::kConsumed,
            &context);
    if (prepare_error != ReserveStateV1Error::kNone) {
        return prepare_error;
    }
    std::size_t entry_index = 0U;
    ReserveStateV1Error error =
        FindActiveGrant(before, action.grant, &entry_index);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    std::size_t action_index = 0U;
    error = ValidateActionKey(
        before.entries[entry_index], action, &action_index);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    if (FindDebitedAction(before.entries[entry_index]) !=
        kReserveStateV1ActionCapacity) {
        return ReserveStateV1Error::kInvalidTransition;
    }
    if (action_index !=
        FindFirstPendingAction(before.entries[entry_index])) {
        return ReserveStateV1Error::kInvalidOrdering;
    }

    const auto& before_action =
        before.entries[entry_index].actions[action_index];
    std::uint64_t byte_cap = 0U;
    if (!CheckedMul(
            static_cast<std::uint64_t>(
                before_action.byte_cap_quanta),
            header.allocation_quantum_bytes,
            &byte_cap)) {
        return ReserveStateV1Error::kArithmeticOverflow;
    }
    auto& entry = context.candidate.entries[entry_index];
    if (byte_cap > entry.activation_remaining_cap) {
        return ReserveStateV1Error::kInvalidAggregate;
    }
    auto& candidate_action = entry.actions[action_index];
    candidate_action.action_state =
        FinalizationActionStateV1::kDebited;
    candidate_action.debit_generation =
        context.candidate.generation;
    entry.activation_remaining_cap -= byte_cap;
    return CommitCandidate(
        header, before, context, output);
}

ReserveStateV1Error BuildReserveCompleteActionV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveFinalizationActionKeyV1& action,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    const ReserveStateV1Error prepare_error =
        PrepareCandidate(
            header,
            before,
            ReserveCoordinatorPhaseV1::kConsumed,
            &context);
    if (prepare_error != ReserveStateV1Error::kNone) {
        return prepare_error;
    }
    std::size_t entry_index = 0U;
    ReserveStateV1Error error =
        FindActiveGrant(before, action.grant, &entry_index);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    std::size_t action_index = 0U;
    error = ValidateActionKey(
        before.entries[entry_index], action, &action_index);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    if (FindDebitedAction(before.entries[entry_index]) !=
            action_index ||
        before.entries[entry_index]
                .actions[action_index]
                .action_state !=
            FinalizationActionStateV1::kDebited) {
        return ReserveStateV1Error::kInvalidTransition;
    }
    context.candidate.entries[entry_index]
        .actions[action_index]
        .action_state =
        FinalizationActionStateV1::kComplete;
    return CommitCandidate(
        header, before, context, output);
}

ReserveStateV1Error BuildReserveFailDebitedActionV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveFinalizationActionKeyV1& action,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    const ReserveStateV1Error prepare_error =
        PrepareCandidate(
            header,
            before,
            ReserveCoordinatorPhaseV1::kConsumed,
            &context);
    if (prepare_error != ReserveStateV1Error::kNone) {
        return prepare_error;
    }
    std::size_t entry_index = 0U;
    ReserveStateV1Error error =
        FindActiveGrant(before, action.grant, &entry_index);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    std::size_t action_index = 0U;
    error = ValidateActionKey(
        before.entries[entry_index], action, &action_index);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    if (FindDebitedAction(before.entries[entry_index]) !=
            action_index ||
        before.entries[entry_index]
                .actions[action_index]
                .action_state !=
            FinalizationActionStateV1::kDebited) {
        return ReserveStateV1Error::kInvalidTransition;
    }

    auto& entry = context.candidate.entries[entry_index];
    entry.actions[action_index].action_state =
        FinalizationActionStateV1::kFailed;
    MarkGrantTerminal(
        &context.candidate,
        entry_index,
        ReserveGrantStatusV1::kFailed);
    return CommitCandidate(
        header, before, context, output);
}

ReserveStateV1Error BuildReserveFailActiveGrantV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveGrantFailureV1& failure,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    const ReserveStateV1Error prepare_error =
        PrepareCandidate(
            header,
            before,
            ReserveCoordinatorPhaseV1::kConsumed,
            &context);
    if (prepare_error != ReserveStateV1Error::kNone) {
        return prepare_error;
    }
    std::size_t entry_index = 0U;
    const ReserveStateV1Error find_error =
        FindActiveGrant(before, failure.grant, &entry_index);
    if (find_error != ReserveStateV1Error::kNone) {
        return find_error;
    }
    if (FindDebitedAction(before.entries[entry_index]) !=
        kReserveStateV1ActionCapacity) {
        return ReserveStateV1Error::kInvalidTransition;
    }
    const ActionCompletionFactsV1 facts =
        GetCompletionFacts(before.entries[entry_index]);
    if (facts.complete_count == 0U ||
        (!IsZero(failure.maintenance_report_sha256) &&
         !facts.all_complete)) {
        return ReserveStateV1Error::kInvalidTransition;
    }

    context.candidate.entries[entry_index]
        .maintenance_report_sha256 =
        failure.maintenance_report_sha256;
    MarkGrantTerminal(
        &context.candidate,
        entry_index,
        ReserveGrantStatusV1::kFailed);
    return CommitCandidate(
        header, before, context, output);
}

ReserveStateV1Error BuildReserveMarkGrantDoneV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveGrantTerminalReportV1& report,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    const ReserveStateV1Error prepare_error =
        PrepareCandidate(
            header,
            before,
            ReserveCoordinatorPhaseV1::kConsumed,
            &context);
    if (prepare_error != ReserveStateV1Error::kNone) {
        return prepare_error;
    }
    if (IsZero(report.maintenance_report_sha256)) {
        return ReserveStateV1Error::kInvalidIdentity;
    }
    std::size_t entry_index = 0U;
    const ReserveStateV1Error find_error =
        FindActiveGrant(before, report.grant, &entry_index);
    if (find_error != ReserveStateV1Error::kNone) {
        return find_error;
    }
    if (!GetCompletionFacts(
             before.entries[entry_index])
             .all_complete) {
        return ReserveStateV1Error::kInvalidTransition;
    }
    context.candidate.entries[entry_index]
        .maintenance_report_sha256 =
        report.maintenance_report_sha256;
    MarkGrantTerminal(
        &context.candidate,
        entry_index,
        ReserveGrantStatusV1::kDone);
    return CommitCandidate(
        header, before, context, output);
}

ReserveStateV1Error
BuildReserveCompleteReportAndMarkGrantDoneV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveAtomicReportCompletionV1& report,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    const ReserveStateV1Error prepare_error =
        PrepareCandidate(
            header,
            before,
            ReserveCoordinatorPhaseV1::kConsumed,
            &context);
    if (prepare_error != ReserveStateV1Error::kNone) {
        return prepare_error;
    }
    if (IsZero(report.maintenance_report_sha256)) {
        return ReserveStateV1Error::kInvalidIdentity;
    }
    std::size_t entry_index = 0U;
    ReserveStateV1Error error = FindActiveGrant(
        before,
        report.report_action.grant,
        &entry_index);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    std::size_t action_index = 0U;
    error = ValidateActionKey(
        before.entries[entry_index],
        report.report_action,
        &action_index);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    if (report.report_action.action_kind !=
            FinalizationActionKindV1::kFinalizationReport ||
        FindDebitedAction(before.entries[entry_index]) !=
            action_index ||
        before.entries[entry_index]
                .actions[action_index]
                .action_state !=
            FinalizationActionStateV1::kDebited) {
        return ReserveStateV1Error::kInvalidTransition;
    }

    auto& entry = context.candidate.entries[entry_index];
    entry.actions[action_index].action_state =
        FinalizationActionStateV1::kComplete;
    entry.maintenance_report_sha256 =
        report.maintenance_report_sha256;
    MarkGrantTerminal(
        &context.candidate,
        entry_index,
        ReserveGrantStatusV1::kDone);
    return CommitCandidate(
        header, before, context, output);
}

}  // namespace l2flow::ingress
