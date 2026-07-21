#include "l2flow/ingress/raw_phase2_startup_plan.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <string_view>

namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }

    int failures = 0;
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t seed) {
    std::array<std::byte, Size> result{};
    for (std::size_t index = 0U; index < Size; ++index) {
        result[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(seed + index));
    }
    return result;
}

bool IsZero(
    const ingress::ReserveStateV1Identity& value) {
    return std::all_of(
        value.begin(),
        value.end(),
        [](std::byte byte) {
            return byte == std::byte{0};
        });
}

ingress::ReserveCoordinatorHeaderV1 MakeHeader() {
    ingress::ReserveCoordinatorHeaderV1 header{};
    header.reserve_state_uuid = Pattern<16U>(0x10U);
    header.schema_sha256 =
        ingress::kReserveStateV1SchemaSha256;
    header.quota_identity_sha256 =
        Pattern<32U>(0x20U);
    header.mount_identity_sha256 =
        Pattern<32U>(0x40U);
    header.device_id = UINT64_C(0x0102030405060708);
    header.declared_releasable_bytes =
        UINT64_C(1) << 30U;
    header.allocation_quantum_bytes = 4096U;
    header.declared_inode_reserve_count = 1000U;
    header.byte_probe_version = 1U;
    header.inode_probe_version = 1U;
    header.inode_inventory_sha256 =
        Pattern<32U>(0x60U);
    header.safe_stop_catalog_sha256 =
        Pattern<32U>(0x80U);
    return header;
}

ingress::ReserveStateEntryV1 MakeRegistryEntry() {
    ingress::ReserveStateEntryV1 entry{};
    entry.source_stream_id = 7U;
    entry.capture_date = 20260719U;
    entry.stream_day_id = Pattern<16U>(0x97U);
    entry.registry_status =
        ingress::ReserveRegistryStatusV1::kActive;
    entry.recovery_origin =
        ingress::ReserveRecoveryOriginV1::kFreshInit;
    entry.recovery_intent =
        ingress::ReserveRecoveryIntentV1::kResumeConnect;
    entry.writer_instance = Pattern<16U>(0xa0U);
    entry.executor_or_recovery_attempt =
        Pattern<16U>(0xb0U);
    entry.safe_stop_template_id =
        UINT64_C(0x0102030405060708);
    return entry;
}

ingress::ReserveStateSlotV1 MakeRegistrySlot(
    const ingress::ReserveCoordinatorHeaderV1& header,
    std::uint64_t generation,
    ingress::ReserveCoordinatorPhaseV1 phase) {
    ingress::ReserveStateSlotV1 slot{};
    slot.coordinator_state = phase;
    slot.generation = generation;
    slot.reserve_state_uuid =
        header.reserve_state_uuid;
    slot.entry_count = 1U;
    slot.entries[0U] = MakeRegistryEntry();
    if (phase ==
        ingress::ReserveCoordinatorPhaseV1::
            kReleasingIntent) {
        slot.reason =
            ingress::ReserveReleaseReasonV1::
                kLowWatermark;
        slot.trigger =
            ingress::ReserveReleaseTriggerV1::
                kFilesystemBytes;
        slot.writer_set_sha256 =
            Pattern<32U>(0xc0U);
    }
    return slot;
}

void SetAction(
    ingress::ReserveStateEntryV1* entry,
    std::size_t action_id,
    ingress::FinalizationActionKindV1 kind,
    std::uint32_t byte_cap_quanta,
    std::uint32_t inode_cap,
    std::uint8_t digest_seed) {
    auto& receipt = entry->actions[action_id];
    receipt.action_id =
        static_cast<std::uint16_t>(action_id);
    receipt.action_kind = kind;
    receipt.action_state =
        ingress::FinalizationActionStateV1::kPending;
    receipt.byte_cap_quanta = byte_cap_quanta;
    receipt.inode_cap = inode_cap;
    receipt.object_plan_sha256 =
        Pattern<32U>(digest_seed);

    auto& plan = entry->plans[action_id];
    plan.plan_version = 1U;
    plan.object_type = kind;
    plan.object_sequence =
        static_cast<std::uint32_t>(action_id + 1U);
    plan.range_start =
        static_cast<std::uint64_t>(action_id * 100U);
    plan.range_end_or_size =
        static_cast<std::uint64_t>(
            (action_id + 1U) * 100U);
    plan.causal_id = Pattern<16U>(
        static_cast<std::uint8_t>(
            digest_seed + 1U));
}

ingress::ReserveStateEntryV1
MakePendingGrantEntry() {
    ingress::ReserveStateEntryV1 entry{};
    entry.source_stream_id = 7U;
    entry.capture_date = 20260719U;
    entry.stream_day_id = Pattern<16U>(0x97U);
    entry.ack_status =
        ingress::ReserveAckStatusV1::kAcked;
    entry.counter_validity =
        ingress::kReserveCounterValidityMask;
    entry.grant_status =
        ingress::ReserveGrantStatusV1::kPending;
    entry.grant_flags =
        ingress::kReserveGrantRawFinalization;
    entry.writer_instance = Pattern<16U>(0xa0U);
    entry.raw_counters.callback_published_records =
        50U;
    entry.raw_counters
        .callback_published_vendor_bytes = 5000U;
    entry.raw_counters.append_global_wal_pos = 4000U;
    entry.raw_counters.append_ingress_sequence = 40U;
    entry.raw_counters.durable_global_wal_pos = 3000U;
    entry.raw_counters.durable_ingress_sequence = 30U;
    entry.raw_counters.queued_record_count = 10U;
    entry.raw_counters.queued_framed_wal_bytes = 1000U;
    entry.safe_stop_template_id =
        UINT64_C(0x0102030405060708);
    SetAction(
        &entry,
        0U,
        ingress::FinalizationActionKindV1::
            kCurrentSegmentDrain,
        2U,
        1U,
        0xd0U);
    SetAction(
        &entry,
        1U,
        ingress::FinalizationActionKindV1::
            kFinalizationReport,
        3U,
        2U,
        0xe0U);
    entry.grant_bytes = 5U * 4096U;
    return entry;
}

ingress::ReserveStateSlotV1 MakePreparedSlot(
    const ingress::ReserveCoordinatorHeaderV1& header,
    std::uint64_t generation) {
    ingress::ReserveStateSlotV1 slot{};
    slot.coordinator_state =
        ingress::ReserveCoordinatorPhaseV1::
            kReleasingPrepared;
    slot.generation = generation;
    slot.reserve_state_uuid =
        header.reserve_state_uuid;
    slot.reason =
        ingress::ReserveReleaseReasonV1::kLowWatermark;
    slot.trigger =
        ingress::ReserveReleaseTriggerV1::
            kFilesystemBytes;
    slot.entry_count = 1U;
    slot.finalization_cycle_id =
        Pattern<16U>(0x30U);
    slot.writer_set_sha256 = Pattern<32U>(0xc0U);
    slot.aggregate_grant_bytes = 5U * 4096U;
    slot.aggregate_inode_grant = 3U;
    slot.pre_release_fs_free_bytes = 1000000U;
    slot.pre_release_quota_free_bytes = 900000U;
    slot.pre_release_fs_free_inodes = 500U;
    slot.pre_release_quota_free_inodes = 400U;
    slot.expected_release_fs_bytes =
        header.declared_releasable_bytes;
    slot.expected_release_quota_bytes =
        header.declared_releasable_bytes;
    slot.expected_release_fs_inodes =
        header.declared_inode_reserve_count + 1U;
    slot.expected_release_quota_inodes =
        header.declared_inode_reserve_count + 1U;
    slot.reserved_margin_bytes = 4096U;
    slot.reserved_margin_inodes = 1U;
    slot.effective_min_fs_free_bytes =
        header.declared_releasable_bytes;
    slot.effective_min_quota_free_bytes =
        header.declared_releasable_bytes;
    slot.effective_min_fs_free_inodes =
        header.declared_inode_reserve_count;
    slot.effective_min_quota_free_inodes =
        header.declared_inode_reserve_count;
    slot.entries[0U] = MakePendingGrantEntry();
    return slot;
}

ingress::ReserveStateSlotV1 MakeConsumedPending(
    const ingress::ReserveCoordinatorHeaderV1& header,
    std::uint64_t generation) {
    auto slot = MakePreparedSlot(header, generation);
    slot.coordinator_state =
        ingress::ReserveCoordinatorPhaseV1::kConsumed;
    return slot;
}

ingress::ReserveStateSlotV1 Activate(
    ingress::ReserveStateSlotV1 slot,
    std::uint64_t generation) {
    slot.generation = generation;
    auto& entry = slot.entries[0U];
    entry.grant_status =
        ingress::ReserveGrantStatusV1::kActive;
    entry.executor_or_recovery_attempt =
        entry.writer_instance;
    entry.activation_fs_free_baseline = 900000U;
    entry.activation_quota_free_baseline = 800000U;
    entry.activation_remaining_cap =
        entry.grant_bytes;
    entry.precharged_bytes = entry.grant_bytes;
    slot.active_entry_index = 0U;
    slot.active_fs_free_inode_baseline = 800U;
    slot.active_quota_free_inode_baseline = 700U;
    return slot;
}

ingress::ReserveStateSlotV1 Debit(
    ingress::ReserveStateSlotV1 slot,
    std::size_t action_index,
    std::uint64_t generation) {
    slot.generation = generation;
    auto& action =
        slot.entries[0U].actions[action_index];
    action.action_state =
        ingress::FinalizationActionStateV1::kDebited;
    action.debit_generation = generation;
    slot.entries[0U].activation_remaining_cap -=
        static_cast<std::uint64_t>(
            action.byte_cap_quanta) *
        4096U;
    return slot;
}

ingress::ReserveStateSlotV1 Complete(
    ingress::ReserveStateSlotV1 slot,
    std::size_t action_index,
    std::uint64_t generation) {
    slot.generation = generation;
    slot.entries[0U]
        .actions[action_index]
        .action_state =
        ingress::FinalizationActionStateV1::kComplete;
    return slot;
}

ingress::ReserveStateSlotV1 Done(
    ingress::ReserveStateSlotV1 slot,
    std::uint64_t generation) {
    slot.generation = generation;
    auto& entry = slot.entries[0U];
    entry.grant_status =
        ingress::ReserveGrantStatusV1::kDone;
    entry.maintenance_report_sha256 =
        Pattern<32U>(0x55U);
    slot.active_entry_index =
        ingress::kReserveStateV1NoActiveEntry;
    slot.active_fs_free_inode_baseline = 0U;
    slot.active_quota_free_inode_baseline = 0U;
    slot.completed_bitmap = 1U;
    return slot;
}

ingress::ReserveStateSlotV1 FailAction(
    ingress::ReserveStateSlotV1 slot,
    std::size_t action_index,
    std::uint64_t generation) {
    slot.generation = generation;
    auto& entry = slot.entries[0U];
    entry.actions[action_index].action_state =
        ingress::FinalizationActionStateV1::kFailed;
    entry.grant_status =
        ingress::ReserveGrantStatusV1::kFailed;
    slot.active_entry_index =
        ingress::kReserveStateV1NoActiveEntry;
    slot.active_fs_free_inode_baseline = 0U;
    slot.active_quota_free_inode_baseline = 0U;
    return slot;
}

ingress::ReserveStateSlotV1 FailGrantAfterComplete(
    ingress::ReserveStateSlotV1 slot,
    std::uint64_t generation) {
    slot.generation = generation;
    auto& entry = slot.entries[0U];
    entry.grant_status =
        ingress::ReserveGrantStatusV1::kFailed;
    entry.maintenance_report_sha256 =
        Pattern<32U>(0x66U);
    slot.active_entry_index =
        ingress::kReserveStateV1NoActiveEntry;
    slot.active_fs_free_inode_baseline = 0U;
    slot.active_quota_free_inode_baseline = 0U;
    return slot;
}

ingress::ReserveCoordinatorStateV1 MakePair(
    const ingress::ReserveCoordinatorHeaderV1& header,
    const ingress::ReserveStateSlotV1& older,
    const ingress::ReserveStateSlotV1& newer) {
    ingress::ReserveCoordinatorStateV1 state{};
    state.header = header;
    const std::size_t newer_index =
        (newer.generation & 1U) == 0U ? 1U : 0U;
    state.slots[newer_index] = newer;
    state.slots[1U - newer_index] = older;
    // Deliberately not authoritative. The planner must reselect from wire.
    state.selected_slot = 1U - newer_index;
    return state;
}

ingress::RawPhase2StartupPlanV1 Plan(
    const ingress::ReserveCoordinatorStateV1& state,
    ingress::RawPhase2StartupPlanErrorV1* error) {
    ingress::RawPhase2StartupPlanV1 plan{};
    *error = ingress::BuildRawPhase2StartupPlanV1(
        state, &plan);
    return plan;
}

void ExpectClosed(
    TestContext* test,
    const ingress::RawPhase2StartupPlanV1& plan,
    std::string_view label) {
    test->Expect(
        !plan.connect_allowed,
        std::string(label) +
            " never authorizes SDK Connect");
    if (plan.durable_state_verified &&
        plan.durable_phase !=
            ingress::ReserveCoordinatorPhaseV1::
                kProvisioned) {
        test->Expect(
            !plan.normal_route_pipeline_allowed,
            std::string(label) +
                " cannot enter the normal route pipeline");
    }
}

void TestPhaseRouting(TestContext* test) {
    const auto header = MakeHeader();

    const auto provisioned1 = MakeRegistrySlot(
        header,
        1U,
        ingress::ReserveCoordinatorPhaseV1::kProvisioned);
    auto provisioned2 = provisioned1;
    provisioned2.generation = 2U;
    auto state = MakePair(
        header, provisioned1, provisioned2);
    state.selected_slot = 0U;
    ingress::RawPhase2StartupPlanErrorV1 error{};
    auto plan = Plan(state, &error);
    test->Expect(
        error ==
                ingress::RawPhase2StartupPlanErrorV1::kNone &&
            plan.disposition ==
                ingress::RawPhase2StartupDispositionV1::
                    kNormalRouteDiscovery &&
            plan.durable_state_verified &&
            plan.normal_route_pipeline_allowed &&
            plan.selected_slot == 1U &&
            plan.generation == 2U &&
            !plan.has_grant &&
            !plan.has_action,
        "PROVISIONED is reselected from frozen bytes and only enters normal route discovery");
    ExpectClosed(test, plan, "PROVISIONED");

    const auto intent = MakeRegistrySlot(
        header,
        2U,
        ingress::ReserveCoordinatorPhaseV1::
            kReleasingIntent);
    state = MakePair(header, provisioned1, intent);
    plan = Plan(state, &error);
    test->Expect(
        error ==
                ingress::RawPhase2StartupPlanErrorV1::kNone &&
            plan.disposition ==
                ingress::RawPhase2StartupDispositionV1::
                    kResumeReleasingIntent &&
            plan.generation == 2U &&
            IsZero(plan.finalization_cycle_id) &&
            !plan.has_grant &&
            !plan.has_action &&
            IsZero(plan.executor_instance) &&
            !plan.normal_route_pipeline_allowed,
        "RELEASING_INTENT resumes release with explicit absent grant/action");
    ExpectClosed(test, plan, "RELEASING_INTENT");

    const auto prepared =
        MakePreparedSlot(header, 3U);
    state = MakePair(header, intent, prepared);
    plan = Plan(state, &error);
    test->Expect(
        error ==
                ingress::RawPhase2StartupPlanErrorV1::kNone &&
            plan.disposition ==
                ingress::RawPhase2StartupDispositionV1::
                    kResumeReleasingPrepared &&
            plan.finalization_cycle_id ==
                prepared.finalization_cycle_id &&
            !plan.has_grant &&
            !plan.has_action &&
            IsZero(plan.executor_instance),
        "RELEASING_PREPARED resumes release without prematurely selecting a grant");
    ExpectClosed(test, plan, "RELEASING_PREPARED");
}

void TestConsumedPlans(TestContext* test) {
    const auto header = MakeHeader();
    const auto prepared = MakePreparedSlot(header, 8U);
    const auto pending = MakeConsumedPending(header, 9U);
    const auto active = Activate(pending, 10U);
    const auto debited0 = Debit(active, 0U, 11U);
    const auto complete0 = Complete(debited0, 0U, 12U);
    const auto debited_report =
        Debit(complete0, 1U, 13U);
    const auto all_complete =
        Complete(debited_report, 1U, 14U);
    const auto done = Done(all_complete, 15U);
    const auto failed =
        FailAction(debited0, 0U, 12U);
    const auto grant_failed =
        FailGrantAfterComplete(all_complete, 15U);

    ingress::RawPhase2StartupPlanErrorV1 error{};
    auto plan = Plan(
        MakePair(header, prepared, pending),
        &error);
    ingress::ReserveStateV1Digest pending_hash{};
    const auto hash_error =
        ingress::
            ComputeImmutableFinalizationGrantSha256V1(
                header,
                pending,
                0U,
                &pending_hash);
    test->Expect(
        error ==
                ingress::RawPhase2StartupPlanErrorV1::kNone &&
            hash_error ==
                ingress::ReserveStateV1Error::kNone &&
            plan.disposition ==
                ingress::RawPhase2StartupDispositionV1::
                    kActivateGrant &&
            plan.has_grant &&
            !plan.has_action &&
            plan.generation == pending.generation &&
            plan.grant_entry_index == 0U &&
            plan.grant.source_stream_id == 7U &&
            plan.grant.capture_date == 20260719U &&
            plan.grant.stream_day_id ==
                pending.entries[0U].stream_day_id &&
            plan.grant.finalization_cycle_id ==
                pending.finalization_cycle_id &&
            plan.grant.safe_stop_template_id ==
                pending.entries[0U]
                    .safe_stop_template_id &&
            plan.immutable_grant_sha256 ==
                pending_hash &&
            plan.grant_status ==
                ingress::ReserveGrantStatusV1::kPending &&
            IsZero(plan.executor_instance),
        "CONSUMED PENDING returns the exact first grant activation key");
    ExpectClosed(test, plan, "CONSUMED/PENDING");

    plan = Plan(MakePair(header, pending, active), &error);
    test->Expect(
        error ==
                ingress::RawPhase2StartupPlanErrorV1::kNone &&
            plan.disposition ==
                ingress::RawPhase2StartupDispositionV1::
                    kDebitNext &&
            plan.has_grant &&
            plan.has_action &&
            plan.action.action_id == 0U &&
            plan.action.grant == plan.grant &&
            plan.action_state ==
                ingress::FinalizationActionStateV1::kPending &&
            plan.executor_instance ==
                active.entries[0U].writer_instance,
        "newly ACTIVE grant selects its first PENDING action");
    ExpectClosed(test, plan, "CONSUMED/ACTIVE");

    plan = Plan(
        MakePair(header, active, debited0),
        &error);
    test->Expect(
        error ==
                ingress::RawPhase2StartupPlanErrorV1::kNone &&
            plan.disposition ==
                ingress::RawPhase2StartupDispositionV1::
                    kResumeDebited &&
            plan.action.action_id == 0U &&
            plan.action.grant == plan.grant &&
            plan.action_state ==
                ingress::FinalizationActionStateV1::kDebited &&
            plan.action_debit_generation == 11U &&
            plan.action.object_plan_sha256 ==
                debited0.entries[0U]
                    .actions[0U]
                    .object_plan_sha256,
        "ACTIVE with one DEBITED action resumes its exact receipt");
    ExpectClosed(test, plan, "CONSUMED/DEBITED");

    plan = Plan(
        MakePair(header, debited0, complete0),
        &error);
    test->Expect(
        error ==
                ingress::RawPhase2StartupPlanErrorV1::kNone &&
            plan.disposition ==
                ingress::RawPhase2StartupDispositionV1::
                    kDebitNext &&
            plan.action.action_id == 1U &&
            plan.action.action_kind ==
                ingress::FinalizationActionKindV1::
                    kFinalizationReport &&
            plan.action.grant == plan.grant &&
            plan.action_state ==
                ingress::FinalizationActionStateV1::kPending,
        "COMPLETE prefix advances only to the next canonical action");
    ExpectClosed(
        test, plan, "CONSUMED/next-PENDING");

    plan = Plan(
        MakePair(
            header, complete0, debited_report),
        &error);
    test->Expect(
        error ==
                ingress::RawPhase2StartupPlanErrorV1::kNone &&
            plan.disposition ==
                ingress::RawPhase2StartupDispositionV1::
                    kResumeDebited &&
            plan.action.action_id == 1U &&
            plan.action.action_kind ==
                ingress::FinalizationActionKindV1::
                    kFinalizationReport &&
            plan.action.grant == plan.grant &&
            plan.action_state ==
                ingress::FinalizationActionStateV1::kDebited,
        "DEBITED terminal report is resumed without guessing filesystem publication state");
    ExpectClosed(
        test, plan, "CONSUMED/report-DEBITED");

    plan = Plan(
        MakePair(
            header, debited_report, all_complete),
        &error);
    test->Expect(
        error ==
                ingress::RawPhase2StartupPlanErrorV1::kNone &&
            plan.disposition ==
                ingress::RawPhase2StartupDispositionV1::
                    kValidateReportAndDone &&
            plan.has_grant &&
            plan.has_action &&
            plan.action.action_id == 1U &&
            plan.action.grant == plan.grant &&
            plan.action_state ==
                ingress::FinalizationActionStateV1::kComplete,
        "ACTIVE all-COMPLETE grant validates the exact terminal report before DONE");
    ExpectClosed(
        test, plan, "CONSUMED/all-COMPLETE");

    plan = Plan(
        MakePair(header, all_complete, done),
        &error);
    test->Expect(
        error ==
                ingress::RawPhase2StartupPlanErrorV1::kNone &&
            plan.disposition ==
                ingress::RawPhase2StartupDispositionV1::
                    kArchiveReprovision &&
            plan.finalization_cycle_id ==
                done.finalization_cycle_id &&
            plan.generation == done.generation &&
            !plan.has_grant &&
            !plan.has_action &&
            IsZero(plan.executor_instance),
        "all-DONE CONSUMED state retains cycle but exposes no executable grant");
    ExpectClosed(test, plan, "CONSUMED/all-DONE");

    plan = Plan(
        MakePair(header, debited0, failed),
        &error);
    test->Expect(
        error ==
                ingress::RawPhase2StartupPlanErrorV1::kNone &&
            plan.disposition ==
                ingress::RawPhase2StartupDispositionV1::kP0 &&
            plan.p0_reason ==
                ingress::RawPhase2StartupP0ReasonV1::
                    kDurableActionFailed &&
            plan.has_grant &&
            plan.has_action &&
            plan.action.action_id == 0U &&
            plan.action.grant == plan.grant &&
            plan.action_state ==
                ingress::FinalizationActionStateV1::kFailed,
        "durable FAILED action returns P0 with its exact evidence key");
    ExpectClosed(
        test, plan, "CONSUMED/action-FAILED");

    plan = Plan(
        MakePair(
            header, all_complete, grant_failed),
        &error);
    test->Expect(
        error ==
                ingress::RawPhase2StartupPlanErrorV1::kNone &&
            plan.disposition ==
                ingress::RawPhase2StartupDispositionV1::kP0 &&
            plan.p0_reason ==
                ingress::RawPhase2StartupP0ReasonV1::
                    kDurableGrantFailed &&
            plan.has_grant &&
            !plan.has_action,
        "semantic FAILED grant with COMPLETE receipts remains P0");
    ExpectClosed(
        test, plan, "CONSUMED/grant-FAILED");
}

void TestInvalidStateFailsClosed(TestContext* test) {
    const auto header = MakeHeader();
    const auto pending = MakeConsumedPending(header, 9U);
    const auto active = Activate(pending, 10U);

    auto selector_mismatch = active;
    selector_mismatch.active_entry_index =
        ingress::kReserveStateV1NoActiveEntry;
    ingress::RawPhase2StartupPlanErrorV1 error{};
    auto plan = Plan(
        MakePair(
            header, pending, selector_mismatch),
        &error);
    test->Expect(
        error ==
                ingress::RawPhase2StartupPlanErrorV1::
                    kStateEncodeRejected &&
            plan.disposition ==
                ingress::RawPhase2StartupDispositionV1::kP0 &&
            plan.p0_reason ==
                ingress::RawPhase2StartupP0ReasonV1::
                    kCodecRejected &&
            !plan.durable_state_verified &&
            !plan.connect_allowed &&
            !plan.normal_route_pipeline_allowed &&
            !plan.has_grant &&
            !plan.has_action,
        "active selector mismatch is rejected by the frozen codec before planning");

    auto two_debited = active;
    two_debited.generation = 11U;
    two_debited.entries[0U]
        .actions[0U]
        .action_state =
        ingress::FinalizationActionStateV1::kDebited;
    two_debited.entries[0U]
        .actions[0U]
        .debit_generation = 11U;
    two_debited.entries[0U]
        .actions[1U]
        .action_state =
        ingress::FinalizationActionStateV1::kDebited;
    two_debited.entries[0U]
        .actions[1U]
        .debit_generation = 11U;
    two_debited.entries[0U]
        .activation_remaining_cap = 0U;
    plan = Plan(
        MakePair(header, active, two_debited),
        &error);
    test->Expect(
        error ==
                ingress::RawPhase2StartupPlanErrorV1::
                    kStateEncodeRejected &&
            plan.disposition ==
                ingress::RawPhase2StartupDispositionV1::kP0 &&
            !plan.durable_state_verified &&
            !plan.connect_allowed &&
            !plan.normal_route_pipeline_allowed &&
            !plan.has_grant &&
            !plan.has_action,
        "multiple DEBITED receipts fail closed before a key can escape");

    auto unknown = MakePair(header, pending, active);
    const std::size_t newer =
        (active.generation & 1U) == 0U ? 1U : 0U;
    unknown.slots[newer].coordinator_state =
        static_cast<
            ingress::ReserveCoordinatorPhaseV1>(0xffU);
    plan = Plan(unknown, &error);
    test->Expect(
        error ==
                ingress::RawPhase2StartupPlanErrorV1::
                    kStateEncodeRejected &&
            plan.codec_error ==
                ingress::ReserveStateV1Error::kUnknownEnum &&
            plan.disposition ==
                ingress::RawPhase2StartupDispositionV1::kP0 &&
            !plan.durable_state_verified &&
            !plan.connect_allowed &&
            !plan.normal_route_pipeline_allowed,
        "caller-injected enum is never trusted outside frozen codec validation");

    test->Expect(
        ingress::RawPhase2StartupDispositionV1Name(
            ingress::RawPhase2StartupDispositionV1::
                kActivateGrant) == "ACTIVATE_GRANT" &&
            ingress::RawPhase2StartupDispositionV1Name(
                ingress::RawPhase2StartupDispositionV1::
                    kResumeDebited) == "RESUME_DEBITED" &&
            ingress::RawPhase2StartupDispositionV1Name(
                ingress::RawPhase2StartupDispositionV1::
                    kDebitNext) == "DEBIT_NEXT" &&
            ingress::RawPhase2StartupDispositionV1Name(
                ingress::RawPhase2StartupDispositionV1::
                    kValidateReportAndDone) ==
                "VALIDATE_REPORT_AND_DONE" &&
            ingress::RawPhase2StartupDispositionV1Name(
                ingress::RawPhase2StartupDispositionV1::
                    kArchiveReprovision) ==
                "ARCHIVE_REPROVISION",
        "CONSUMED startup disposition names are stable");

    test->Expect(
        ingress::BuildRawPhase2StartupPlanV1(
            unknown, nullptr) ==
            ingress::RawPhase2StartupPlanErrorV1::kNullOutput,
        "null planner output is rejected");
}

}  // namespace

int main() {
    TestContext test;
    TestPhaseRouting(&test);
    TestConsumedPlans(&test);
    TestInvalidStateFailsClosed(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase-2 startup planner test(s) failed\n";
        return 1;
    }
    std::cout
        << "Phase 2 state-first startup planner tests passed\n";
    return 0;
}
