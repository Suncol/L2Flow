#include "l2flow/ingress/raw_phase2_startup_plan.h"

#include <algorithm>
#include <limits>

namespace l2flow::ingress {
namespace {

[[nodiscard]] bool IsZero(
    const ReserveStateV1Identity& value) noexcept {
    return std::all_of(
        value.begin(),
        value.end(),
        [](std::byte byte) {
            return byte == std::byte{0};
        });
}

[[nodiscard]] std::size_t UsedActionCount(
    const ReserveStateEntryV1& entry) noexcept {
    std::size_t count = 0U;
    while (count < entry.actions.size() &&
           entry.actions[count].action_kind !=
               FinalizationActionKindV1::kUnused) {
        ++count;
    }
    return count;
}

void SetActionKey(
    const FinalizationActionReceiptV1& receipt,
    RawPhase2StartupPlanV1* plan) noexcept {
    plan->has_action = true;
    plan->action.grant = plan->grant;
    plan->action.action_id = receipt.action_id;
    plan->action.action_kind = receipt.action_kind;
    plan->action.object_plan_sha256 =
        receipt.object_plan_sha256;
    plan->action_state = receipt.action_state;
    plan->action_debit_generation =
        receipt.debit_generation;
}

[[nodiscard]] RawPhase2StartupPlanErrorV1
SetGrantKey(
    const ReserveCoordinatorStateV1& state,
    const ReserveStateSlotV1& slot,
    std::size_t entry_index,
    RawPhase2StartupPlanV1* plan) noexcept {
    if (entry_index >=
            static_cast<std::size_t>(
                slot.entry_count) ||
        entry_index >= slot.entries.size() ||
        entry_index >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint16_t>::max())) {
        plan->disposition =
            RawPhase2StartupDispositionV1::kP0;
        plan->p0_reason =
            RawPhase2StartupP0ReasonV1::
                kInvalidConsumedTopology;
        return RawPhase2StartupPlanErrorV1::
            kInvalidConsumedTopology;
    }
    ReserveStateV1Digest immutable_grant{};
    const ReserveStateV1Error hash_error =
        ComputeImmutableFinalizationGrantSha256V1(
            state.header,
            slot,
            entry_index,
            &immutable_grant);
    if (hash_error != ReserveStateV1Error::kNone) {
        plan->disposition =
            RawPhase2StartupDispositionV1::kP0;
        plan->p0_reason =
            RawPhase2StartupP0ReasonV1::
                kImmutableGrantHashRejected;
        plan->codec_error = hash_error;
        return RawPhase2StartupPlanErrorV1::
            kImmutableGrantHashRejected;
    }

    const ReserveStateEntryV1& entry =
        slot.entries[entry_index];
    plan->has_grant = true;
    plan->grant_entry_index =
        static_cast<std::uint16_t>(entry_index);
    plan->grant.finalization_cycle_id =
        slot.finalization_cycle_id;
    plan->grant.source_stream_id =
        entry.source_stream_id;
    plan->grant.capture_date = entry.capture_date;
    plan->grant.stream_day_id =
        entry.stream_day_id;
    plan->grant.ack_status = entry.ack_status;
    plan->grant.ack_writer_instance =
        entry.writer_instance;
    plan->grant.safe_stop_template_id =
        entry.safe_stop_template_id;
    plan->grant_flags = entry.grant_flags;
    plan->immutable_grant_sha256 =
        immutable_grant;
    plan->grant_status = entry.grant_status;
    plan->executor_instance =
        entry.executor_or_recovery_attempt;
    return RawPhase2StartupPlanErrorV1::kNone;
}

[[nodiscard]] RawPhase2StartupPlanErrorV1
InvalidConsumedTopology(
    RawPhase2StartupPlanV1* plan) noexcept {
    plan->has_grant = false;
    plan->grant = {};
    plan->grant_entry_index = 0U;
    plan->grant_flags = 0U;
    plan->immutable_grant_sha256 = {};
    plan->grant_status = ReserveGrantStatusV1::kUnused;
    plan->executor_instance = {};
    plan->has_action = false;
    plan->action = {};
    plan->action_state =
        FinalizationActionStateV1::kUnused;
    plan->action_debit_generation = 0U;
    plan->disposition =
        RawPhase2StartupDispositionV1::kP0;
    plan->p0_reason =
        RawPhase2StartupP0ReasonV1::
            kInvalidConsumedTopology;
    return RawPhase2StartupPlanErrorV1::
        kInvalidConsumedTopology;
}

[[nodiscard]] RawPhase2StartupPlanErrorV1
PlanConsumed(
    const ReserveCoordinatorStateV1& state,
    const ReserveStateSlotV1& slot,
    RawPhase2StartupPlanV1* plan) noexcept {
    std::size_t active_count = 0U;
    std::size_t active_index = 0U;
    std::size_t first_pending_index =
        slot.entries.size();
    std::size_t first_failed_index =
        slot.entries.size();
    for (std::size_t index = 0U;
         index < static_cast<std::size_t>(slot.entry_count);
         ++index) {
        const ReserveStateEntryV1& entry =
            slot.entries[index];
        switch (entry.grant_status) {
            case ReserveGrantStatusV1::kActive:
                ++active_count;
                active_index = index;
                break;
            case ReserveGrantStatusV1::kPending:
                if (first_pending_index ==
                    slot.entries.size()) {
                    first_pending_index = index;
                }
                break;
            case ReserveGrantStatusV1::kFailed:
                if (first_failed_index ==
                    slot.entries.size()) {
                    first_failed_index = index;
                }
                break;
            case ReserveGrantStatusV1::kDone:
                break;
            case ReserveGrantStatusV1::kUnused:
                return InvalidConsumedTopology(plan);
        }
    }

    if (active_count > 1U ||
        (active_count == 0U &&
         slot.active_entry_index !=
             kReserveStateV1NoActiveEntry) ||
        (active_count == 1U &&
         (slot.active_entry_index != active_index ||
          active_index >=
              static_cast<std::size_t>(
                  slot.entry_count)))) {
        return InvalidConsumedTopology(plan);
    }

    if (first_failed_index != slot.entries.size()) {
        plan->disposition =
            RawPhase2StartupDispositionV1::kP0;
        const RawPhase2StartupPlanErrorV1 key_error =
            SetGrantKey(
                state, slot, first_failed_index, plan);
        if (key_error !=
            RawPhase2StartupPlanErrorV1::kNone) {
            return key_error;
        }
        const ReserveStateEntryV1& failed =
            slot.entries[first_failed_index];
        const std::size_t used =
            UsedActionCount(failed);
        for (std::size_t index = 0U;
             index < used;
             ++index) {
            if (failed.actions[index].action_state ==
                FinalizationActionStateV1::kFailed) {
                SetActionKey(
                    failed.actions[index], plan);
                plan->p0_reason =
                    RawPhase2StartupP0ReasonV1::
                        kDurableActionFailed;
                return RawPhase2StartupPlanErrorV1::kNone;
            }
        }
        plan->p0_reason =
            RawPhase2StartupP0ReasonV1::
                kDurableGrantFailed;
        return RawPhase2StartupPlanErrorV1::kNone;
    }

    if (active_count == 0U) {
        if (first_pending_index !=
            slot.entries.size()) {
            const RawPhase2StartupPlanErrorV1 key_error =
                SetGrantKey(
                    state,
                    slot,
                    first_pending_index,
                    plan);
            if (key_error !=
                RawPhase2StartupPlanErrorV1::kNone) {
                return key_error;
            }
            if (!IsZero(plan->executor_instance)) {
                return InvalidConsumedTopology(plan);
            }
            plan->disposition =
                RawPhase2StartupDispositionV1::
                    kActivateGrant;
            return RawPhase2StartupPlanErrorV1::kNone;
        }
        plan->disposition =
            RawPhase2StartupDispositionV1::
                kArchiveReprovision;
        return RawPhase2StartupPlanErrorV1::kNone;
    }

    const RawPhase2StartupPlanErrorV1 key_error =
        SetGrantKey(
            state, slot, active_index, plan);
    if (key_error !=
        RawPhase2StartupPlanErrorV1::kNone) {
        return key_error;
    }
    if (IsZero(plan->executor_instance)) {
        return InvalidConsumedTopology(plan);
    }

    const ReserveStateEntryV1& active =
        slot.entries[active_index];
    const std::size_t used = UsedActionCount(active);
    if (used == 0U) {
        return InvalidConsumedTopology(plan);
    }
    std::size_t debited_count = 0U;
    std::size_t debited_index = 0U;
    std::size_t first_pending_action = used;
    for (std::size_t index = 0U; index < used; ++index) {
        switch (active.actions[index].action_state) {
            case FinalizationActionStateV1::kDebited:
                ++debited_count;
                debited_index = index;
                break;
            case FinalizationActionStateV1::kPending:
                if (first_pending_action == used) {
                    first_pending_action = index;
                }
                break;
            case FinalizationActionStateV1::kComplete:
                break;
            case FinalizationActionStateV1::kFailed:
                SetActionKey(
                    active.actions[index], plan);
                plan->disposition =
                    RawPhase2StartupDispositionV1::kP0;
                plan->p0_reason =
                    RawPhase2StartupP0ReasonV1::
                        kDurableActionFailed;
                return RawPhase2StartupPlanErrorV1::kNone;
            case FinalizationActionStateV1::kUnused:
                return InvalidConsumedTopology(plan);
        }
    }
    if (debited_count > 1U ||
        (debited_count != 0U &&
         first_pending_action < debited_index)) {
        return InvalidConsumedTopology(plan);
    }
    if (debited_count == 1U) {
        SetActionKey(
            active.actions[debited_index], plan);
        plan->disposition =
            RawPhase2StartupDispositionV1::
                kResumeDebited;
        return RawPhase2StartupPlanErrorV1::kNone;
    }
    if (first_pending_action != used) {
        for (std::size_t index = 0U;
             index < first_pending_action;
             ++index) {
            if (active.actions[index].action_state !=
                FinalizationActionStateV1::kComplete) {
                return InvalidConsumedTopology(plan);
            }
        }
        SetActionKey(
            active.actions[first_pending_action], plan);
        plan->disposition =
            RawPhase2StartupDispositionV1::
                kDebitNext;
        return RawPhase2StartupPlanErrorV1::kNone;
    }
    for (std::size_t index = 0U; index < used; ++index) {
        if (active.actions[index].action_state !=
            FinalizationActionStateV1::kComplete) {
            return InvalidConsumedTopology(plan);
        }
    }
    const FinalizationActionReceiptV1& report =
        active.actions[used - 1U];
    if (report.action_kind !=
        FinalizationActionKindV1::kFinalizationReport) {
        return InvalidConsumedTopology(plan);
    }
    SetActionKey(report, plan);
    plan->disposition =
        RawPhase2StartupDispositionV1::
            kValidateReportAndDone;
    return RawPhase2StartupPlanErrorV1::kNone;
}

}  // namespace

std::string_view
RawPhase2StartupDispositionV1Name(
    RawPhase2StartupDispositionV1 value) noexcept {
    switch (value) {
        case RawPhase2StartupDispositionV1::kP0:
            return "P0";
        case RawPhase2StartupDispositionV1::
            kNormalRouteDiscovery:
            return "NORMAL_ROUTE_DISCOVERY";
        case RawPhase2StartupDispositionV1::
            kResumeReleasingIntent:
            return "RESUME_RELEASING_INTENT";
        case RawPhase2StartupDispositionV1::
            kResumeReleasingPrepared:
            return "RESUME_RELEASING_PREPARED";
        case RawPhase2StartupDispositionV1::kActivateGrant:
            return "ACTIVATE_GRANT";
        case RawPhase2StartupDispositionV1::kResumeDebited:
            return "RESUME_DEBITED";
        case RawPhase2StartupDispositionV1::kDebitNext:
            return "DEBIT_NEXT";
        case RawPhase2StartupDispositionV1::
            kValidateReportAndDone:
            return "VALIDATE_REPORT_AND_DONE";
        case RawPhase2StartupDispositionV1::
            kArchiveReprovision:
            return "ARCHIVE_REPROVISION";
    }
    return "UNKNOWN";
}

std::string_view
RawPhase2StartupPlanErrorV1Name(
    RawPhase2StartupPlanErrorV1 value) noexcept {
    switch (value) {
        case RawPhase2StartupPlanErrorV1::kNone:
            return "none";
        case RawPhase2StartupPlanErrorV1::kNullOutput:
            return "null output";
        case RawPhase2StartupPlanErrorV1::
            kStateEncodeRejected:
            return "state encode rejected";
        case RawPhase2StartupPlanErrorV1::
            kStateDecodeRejected:
            return "state decode rejected";
        case RawPhase2StartupPlanErrorV1::
            kCanonicalRoundTripMismatch:
            return "canonical round trip mismatch";
        case RawPhase2StartupPlanErrorV1::
            kInvalidConsumedTopology:
            return "invalid consumed topology";
        case RawPhase2StartupPlanErrorV1::
            kImmutableGrantHashRejected:
            return "immutable grant hash rejected";
    }
    return "unknown";
}

RawPhase2StartupPlanErrorV1
BuildRawPhase2StartupPlanV1(
    const ReserveCoordinatorStateV1& untrusted_state,
    RawPhase2StartupPlanV1* output) noexcept {
    if (output == nullptr) {
        return RawPhase2StartupPlanErrorV1::kNullOutput;
    }
    RawPhase2StartupPlanV1 plan{};
    ReserveStateV1FileWire frozen_wire{};
    const ReserveStateV1Error encode_error =
        EncodeReserveCoordinatorStateV1(
            untrusted_state, &frozen_wire);
    if (encode_error != ReserveStateV1Error::kNone) {
        plan.p0_reason =
            RawPhase2StartupP0ReasonV1::kCodecRejected;
        plan.codec_error = encode_error;
        *output = plan;
        return RawPhase2StartupPlanErrorV1::
            kStateEncodeRejected;
    }

    ReserveCoordinatorStateV1 decoded{};
    const ReserveStateV1Error decode_error =
        DecodeAndSelectReserveCoordinatorStateV1(
            frozen_wire, &decoded);
    if (decode_error != ReserveStateV1Error::kNone) {
        plan.p0_reason =
            RawPhase2StartupP0ReasonV1::kCodecRejected;
        plan.codec_error = decode_error;
        *output = plan;
        return RawPhase2StartupPlanErrorV1::
            kStateDecodeRejected;
    }
    ReserveStateV1FileWire canonical_wire{};
    const ReserveStateV1Error canonical_error =
        EncodeReserveCoordinatorStateV1(
            decoded, &canonical_wire);
    if (canonical_error != ReserveStateV1Error::kNone ||
        canonical_wire != frozen_wire) {
        plan.p0_reason =
            RawPhase2StartupP0ReasonV1::
                kCanonicalRoundTripMismatch;
        plan.codec_error = canonical_error;
        *output = plan;
        return RawPhase2StartupPlanErrorV1::
            kCanonicalRoundTripMismatch;
    }
    if (decoded.selected_slot >= decoded.slots.size()) {
        plan.p0_reason =
            RawPhase2StartupP0ReasonV1::
                kCanonicalRoundTripMismatch;
        *output = plan;
        return RawPhase2StartupPlanErrorV1::
            kCanonicalRoundTripMismatch;
    }

    const ReserveStateSlotV1& slot =
        decoded.slots[decoded.selected_slot];
    plan.durable_state_verified = true;
    plan.durable_phase = slot.coordinator_state;
    plan.selected_slot = decoded.selected_slot;
    plan.generation = slot.generation;
    plan.reserve_state_uuid =
        decoded.header.reserve_state_uuid;
    plan.finalization_cycle_id =
        slot.finalization_cycle_id;

    RawPhase2StartupPlanErrorV1 result =
        RawPhase2StartupPlanErrorV1::kNone;
    switch (slot.coordinator_state) {
        case ReserveCoordinatorPhaseV1::kProvisioned:
            plan.disposition =
                RawPhase2StartupDispositionV1::
                    kNormalRouteDiscovery;
            plan.normal_route_pipeline_allowed = true;
            break;
        case ReserveCoordinatorPhaseV1::kReleasingIntent:
            plan.disposition =
                RawPhase2StartupDispositionV1::
                    kResumeReleasingIntent;
            break;
        case ReserveCoordinatorPhaseV1::
            kReleasingPrepared:
            plan.disposition =
                RawPhase2StartupDispositionV1::
                    kResumeReleasingPrepared;
            break;
        case ReserveCoordinatorPhaseV1::kConsumed:
            result = PlanConsumed(
                decoded, slot, &plan);
            break;
    }
    // Intentionally never changed by a successful branch.
    plan.connect_allowed = false;
    *output = plan;
    return result;
}

}  // namespace l2flow::ingress
