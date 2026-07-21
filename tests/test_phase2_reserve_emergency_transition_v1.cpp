#include "l2flow/ingress/reserve_emergency_transition_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <span>
#include <string>

namespace ingress = l2flow::ingress;

namespace {

struct TestContext final {
    void Expect(bool condition, const std::string& message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }

    void ExpectError(
        ingress::ReserveStateV1Error actual,
        ingress::ReserveStateV1Error expected,
        const std::string& message) {
        if (actual != expected) {
            ++failures;
            std::cerr
                << "FAIL: " << message << " (expected "
                << ingress::ReserveStateV1ErrorName(expected)
                << ", got "
                << ingress::ReserveStateV1ErrorName(actual)
                << ")\n";
        }
    }

    int failures = 0;
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t first) {
    std::array<std::byte, Size> result{};
    for (std::size_t index = 0U; index < Size; ++index) {
        result[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                static_cast<unsigned int>(first) +
                static_cast<unsigned int>(index)));
    }
    return result;
}

ingress::ReserveCoordinatorHeaderV1 MakeHeader() {
    ingress::ReserveCoordinatorHeaderV1 header{};
    header.reserve_state_uuid = Pattern<16U>(0x10U);
    header.schema_sha256 =
        ingress::kReserveStateV1SchemaSha256;
    header.quota_identity_sha256 = Pattern<32U>(0x20U);
    header.mount_identity_sha256 = Pattern<32U>(0x40U);
    header.device_id = 0x0102030405060708ULL;
    header.declared_releasable_bytes = 1ULL << 30U;
    header.allocation_quantum_bytes = 4096U;
    header.declared_inode_reserve_count = 1000U;
    header.byte_probe_version = 1U;
    header.inode_probe_version = 1U;
    header.inode_inventory_sha256 = Pattern<32U>(0x60U);
    header.safe_stop_catalog_sha256 = Pattern<32U>(0x80U);
    return header;
}

ingress::ReserveStateEntryV1 MakeRegistryEntry(
    std::uint32_t source_stream_id,
    std::uint8_t seed) {
    ingress::ReserveStateEntryV1 entry{};
    entry.source_stream_id = source_stream_id;
    entry.capture_date = 20260719U;
    entry.stream_day_id = Pattern<16U>(seed);
    entry.registry_status =
        ingress::ReserveRegistryStatusV1::kActive;
    entry.recovery_origin =
        ingress::ReserveRecoveryOriginV1::kFreshInit;
    entry.recovery_intent =
        ingress::ReserveRecoveryIntentV1::kResumeConnect;
    entry.writer_instance =
        Pattern<16U>(static_cast<std::uint8_t>(seed + 0x10U));
    entry.executor_or_recovery_attempt =
        Pattern<16U>(static_cast<std::uint8_t>(seed + 0x20U));
    entry.safe_stop_template_id =
        1000U + static_cast<std::uint64_t>(source_stream_id);
    return entry;
}

ingress::ReserveStateSlotV1 MakeProvisioned(
    const ingress::ReserveCoordinatorHeaderV1& header,
    std::uint64_t generation) {
    ingress::ReserveStateSlotV1 slot{};
    slot.coordinator_state =
        ingress::ReserveCoordinatorPhaseV1::kProvisioned;
    slot.generation = generation;
    slot.reserve_state_uuid = header.reserve_state_uuid;
    slot.entry_count = 3U;
    slot.entries[0U] = MakeRegistryEntry(7U, 0x90U);
    slot.entries[1U] = MakeRegistryEntry(8U, 0xa0U);
    slot.entries[2U] = MakeRegistryEntry(9U, 0xb0U);
    return slot;
}

void SetAction(
    ingress::ReserveStateEntryV1* entry,
    std::size_t action_id,
    ingress::FinalizationActionKindV1 kind,
    std::uint32_t byte_cap_quanta,
    std::uint32_t inode_cap,
    std::uint8_t seed) {
    auto& receipt = entry->actions[action_id];
    receipt.action_id =
        static_cast<std::uint16_t>(action_id);
    receipt.action_kind = kind;
    receipt.action_state =
        ingress::FinalizationActionStateV1::kPending;
    receipt.byte_cap_quanta = byte_cap_quanta;
    receipt.inode_cap = inode_cap;
    receipt.object_plan_sha256 = Pattern<32U>(seed);

    auto& plan = entry->plans[action_id];
    plan.plan_version = 1U;
    plan.object_type = kind;
    plan.object_sequence =
        static_cast<std::uint32_t>(action_id + 1U);
    plan.range_start =
        static_cast<std::uint64_t>(action_id * 100U);
    plan.range_end_or_size =
        static_cast<std::uint64_t>((action_id + 1U) * 100U);
    plan.causal_id =
        Pattern<16U>(static_cast<std::uint8_t>(seed + 1U));
}

ingress::ReserveStateEntryV1 MakeGrant(
    const ingress::ReserveStateEntryV1& registry,
    const ingress::ReserveCoordinatorHeaderV1& header,
    std::uint8_t seed) {
    ingress::ReserveStateEntryV1 entry{};
    entry.source_stream_id = registry.source_stream_id;
    entry.capture_date = registry.capture_date;
    entry.stream_day_id = registry.stream_day_id;
    entry.ack_status = ingress::ReserveAckStatusV1::kAcked;
    entry.counter_validity =
        ingress::kReserveCounterValidityMask;
    entry.grant_status =
        ingress::ReserveGrantStatusV1::kPending;
    entry.grant_flags =
        ingress::kReserveGrantRawFinalization;
    entry.writer_instance = registry.writer_instance;
    entry.raw_counters.callback_published_records = 50U;
    entry.raw_counters.callback_published_vendor_bytes =
        5000U;
    entry.raw_counters.append_global_wal_pos = 4000U;
    entry.raw_counters.append_ingress_sequence = 40U;
    entry.raw_counters.durable_global_wal_pos = 3000U;
    entry.raw_counters.durable_ingress_sequence = 30U;
    entry.raw_counters.queued_record_count = 10U;
    entry.raw_counters.queued_framed_wal_bytes = 1000U;
    entry.safe_stop_template_id =
        registry.safe_stop_template_id;
    SetAction(
        &entry,
        0U,
        ingress::FinalizationActionKindV1::
            kCurrentSegmentDrain,
        2U,
        1U,
        seed);
    SetAction(
        &entry,
        1U,
        ingress::FinalizationActionKindV1::
            kFinalizationReport,
        3U,
        2U,
        static_cast<std::uint8_t>(seed + 0x10U));
    entry.grant_bytes =
        5U * header.allocation_quantum_bytes;
    return entry;
}

ingress::ReserveReleaseIntentV1 MakeIntentRequest() {
    ingress::ReserveReleaseIntentV1 request{};
    request.reason =
        ingress::ReserveReleaseReasonV1::kLowWatermark;
    request.trigger =
        ingress::ReserveReleaseTriggerV1::kFilesystemBytes;
    request.writer_set_sha256 = Pattern<32U>(0xc0U);
    return request;
}

ingress::ReserveReleasePreparedV1 MakePreparedRequest(
    const ingress::ReserveCoordinatorHeaderV1& header,
    std::span<const ingress::ReserveStateEntryV1> grants) {
    ingress::ReserveReleasePreparedV1 request{};
    request.finalization_cycle_id = Pattern<16U>(0x30U);
    request.grant_entries = grants;
    request.pre_release_fs_free_bytes = 1000000U;
    request.pre_release_quota_free_bytes = 900000U;
    request.pre_release_fs_free_inodes = 500U;
    request.pre_release_quota_free_inodes = 400U;
    request.expected_release_fs_bytes =
        header.declared_releasable_bytes;
    request.expected_release_quota_bytes =
        header.declared_releasable_bytes;
    request.expected_release_fs_inodes =
        header.declared_inode_reserve_count + 1U;
    request.expected_release_quota_inodes =
        header.declared_inode_reserve_count + 1U;
    request.reserved_margin_bytes =
        header.allocation_quantum_bytes;
    request.reserved_margin_inodes = 1U;
    request.effective_min_fs_free_bytes =
        header.declared_releasable_bytes;
    request.effective_min_quota_free_bytes =
        header.declared_releasable_bytes;
    request.effective_min_fs_free_inodes =
        header.declared_inode_reserve_count;
    request.effective_min_quota_free_inodes =
        header.declared_inode_reserve_count;
    return request;
}

ingress::ReserveFinalizationGrantKeyV1 MakeGrantKey(
    const ingress::ReserveStateSlotV1& slot,
    std::size_t index) {
    const auto& entry = slot.entries[index];
    ingress::ReserveFinalizationGrantKeyV1 key{};
    key.finalization_cycle_id = slot.finalization_cycle_id;
    key.source_stream_id = entry.source_stream_id;
    key.capture_date = entry.capture_date;
    key.stream_day_id = entry.stream_day_id;
    key.ack_status = entry.ack_status;
    key.ack_writer_instance = entry.writer_instance;
    key.safe_stop_template_id = entry.safe_stop_template_id;
    return key;
}

ingress::ReserveFinalizationActionKeyV1 MakeActionKey(
    const ingress::ReserveStateSlotV1& slot,
    std::size_t entry_index,
    std::size_t action_index) {
    const auto& action =
        slot.entries[entry_index].actions[action_index];
    ingress::ReserveFinalizationActionKeyV1 key{};
    key.grant = MakeGrantKey(slot, entry_index);
    key.action_id =
        static_cast<std::uint16_t>(action_index);
    key.action_kind = action.action_kind;
    key.object_plan_sha256 = action.object_plan_sha256;
    return key;
}

ingress::ReserveGrantActivationV1 MakeActivation(
    const ingress::ReserveStateSlotV1& slot,
    std::size_t entry_index) {
    ingress::ReserveGrantActivationV1 activation{};
    activation.grant = MakeGrantKey(slot, entry_index);
    activation.executor_instance =
        slot.entries[entry_index].writer_instance;
    activation.filesystem_free_byte_baseline = 800000U;
    activation.quota_free_byte_baseline = 700000U;
    activation.filesystem_free_inode_baseline = 300U;
    activation.quota_free_inode_baseline = 200U;
    return activation;
}

void ExpectPair(
    TestContext* test,
    const ingress::ReserveCoordinatorHeaderV1& header,
    const ingress::ReserveStateSlotV1& before,
    const ingress::ReserveStateSlotV1& after,
    const std::string& message) {
    ingress::ReserveStateV1SlotWire before_wire{};
    ingress::ReserveStateV1SlotWire after_wire{};
    const auto before_error =
        ingress::EncodeReserveStateSlotV1(
            header, before, &before_wire);
    const auto after_error =
        ingress::EncodeReserveStateSlotV1(
            header, after, &after_wire);
    test->ExpectError(
        before_error,
        ingress::ReserveStateV1Error::kNone,
        message + " encodes before");
    test->ExpectError(
        after_error,
        ingress::ReserveStateV1Error::kNone,
        message + " encodes after");
    if (before_error == ingress::ReserveStateV1Error::kNone &&
        after_error == ingress::ReserveStateV1Error::kNone) {
        test->ExpectError(
            ingress::ValidateReserveStateSlotPairV1(
                header,
                before,
                before_wire,
                after,
                after_wire),
            ingress::ReserveStateV1Error::kNone,
            message + " validates pair");
    }
}

void ExpectUnchanged(
    TestContext* test,
    const ingress::ReserveStateSlotV1& actual,
    const ingress::ReserveStateSlotV1& sentinel,
    const std::string& message) {
    test->Expect(actual == sentinel, message);
}

struct ReleaseChain final {
    ingress::ReserveCoordinatorHeaderV1 header{};
    ingress::ReserveStateSlotV1 provisioned{};
    ingress::ReserveStateSlotV1 intent{};
    ingress::ReserveStateSlotV1 prepared{};
    ingress::ReserveStateSlotV1 consumed{};
    std::array<
        ingress::ReserveStateEntryV1,
        3U>
        grants{};
};

bool BuildReleaseChain(
    TestContext* test,
    ReleaseChain* chain) {
    chain->header = MakeHeader();
    chain->provisioned =
        MakeProvisioned(chain->header, 10U);
    for (std::size_t index = 0U;
         index < chain->grants.size();
         ++index) {
        chain->grants[index] = MakeGrant(
            chain->provisioned.entries[index],
            chain->header,
            static_cast<std::uint8_t>(0xd0U + index * 8U));
    }

    auto error = ingress::BuildReserveReleasingIntentV1(
        chain->header,
        chain->provisioned,
        MakeIntentRequest(),
        &chain->intent);
    test->ExpectError(
        error,
        ingress::ReserveStateV1Error::kNone,
        "build release INTENT");
    if (error != ingress::ReserveStateV1Error::kNone) {
        return false;
    }
    const auto prepared_request = MakePreparedRequest(
        chain->header, chain->grants);
    error = ingress::BuildReserveReleasingPreparedV1(
        chain->header,
        chain->intent,
        prepared_request,
        &chain->prepared);
    test->ExpectError(
        error,
        ingress::ReserveStateV1Error::kNone,
        "build release PREPARED");
    if (error != ingress::ReserveStateV1Error::kNone) {
        return false;
    }
    error = ingress::BuildReserveConsumedV1(
        chain->header,
        chain->prepared,
        &chain->consumed);
    test->ExpectError(
        error,
        ingress::ReserveStateV1Error::kNone,
        "build release CONSUMED");
    return error == ingress::ReserveStateV1Error::kNone;
}

void TestReleaseChainAndBoundaries(TestContext* test) {
    ReleaseChain chain{};
    if (!BuildReleaseChain(test, &chain)) {
        return;
    }

    test->Expect(
        chain.intent.generation ==
                chain.provisioned.generation + 1U &&
            chain.intent.coordinator_state ==
                ingress::ReserveCoordinatorPhaseV1::
                    kReleasingIntent,
        "INTENT advances exactly one generation and phase");
    test->Expect(
        chain.intent.entry_count ==
                chain.provisioned.entry_count &&
            chain.intent.entries == chain.provisioned.entries,
        "INTENT copies the frozen registry entries exactly");
    ExpectPair(
        test,
        chain.header,
        chain.provisioned,
        chain.intent,
        "PROVISIONED to INTENT");

    const std::uint64_t per_grant =
        5U * chain.header.allocation_quantum_bytes;
    test->Expect(
        chain.prepared.aggregate_grant_bytes ==
                3U * per_grant &&
            chain.prepared.aggregate_inode_grant == 9U &&
            chain.prepared.writer_set_sha256 ==
                chain.intent.writer_set_sha256,
        "PREPARED derives aggregate grants and preserves writer set");
    ExpectPair(
        test,
        chain.header,
        chain.intent,
        chain.prepared,
        "INTENT to PREPARED");
    test->ExpectError(
        ingress::ValidateImmutableFinalizationFactsV1(
            chain.prepared, chain.consumed),
        ingress::ReserveStateV1Error::kNone,
        "CONSUMED preserves all immutable finalization facts");
    ExpectPair(
        test,
        chain.header,
        chain.prepared,
        chain.consumed,
        "PREPARED to CONSUMED");

    test->ExpectError(
        ingress::BuildReserveConsumedV1(
            chain.header, chain.prepared, nullptr),
        ingress::ReserveStateV1Error::kNullOutput,
        "null output is rejected");

    ingress::ReserveStateSlotV1 sentinel{};
    sentinel.generation = 77U;
    auto output = sentinel;
    test->ExpectError(
        ingress::BuildReserveConsumedV1(
            chain.header, chain.intent, &output),
        ingress::ReserveStateV1Error::kInvalidState,
        "phase skipping INTENT to CONSUMED is rejected");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "phase rejection leaves output unchanged");

    auto overflow = chain.provisioned;
    overflow.generation =
        std::numeric_limits<std::uint64_t>::max();
    output = sentinel;
    test->ExpectError(
        ingress::BuildReserveReleasingIntentV1(
            chain.header,
            overflow,
            MakeIntentRequest(),
            &output),
        ingress::ReserveStateV1Error::kInvalidGeneration,
        "generation wrap is rejected before mutation");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "generation overflow leaves output unchanged");

    auto bad_intent = MakeIntentRequest();
    bad_intent.writer_set_sha256 = {};
    output = sentinel;
    test->ExpectError(
        ingress::BuildReserveReleasingIntentV1(
            chain.header,
            chain.provisioned,
            bad_intent,
            &output),
        ingress::ReserveStateV1Error::kInvalidIdentity,
        "zero writer-set commitment is rejected");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "bad INTENT leaves output unchanged");

    bad_intent = MakeIntentRequest();
    bad_intent.reason =
        static_cast<ingress::ReserveReleaseReasonV1>(255U);
    output = sentinel;
    test->ExpectError(
        ingress::BuildReserveReleasingIntentV1(
            chain.header,
            chain.provisioned,
            bad_intent,
            &output),
        ingress::ReserveStateV1Error::kUnknownEnum,
        "unknown INTENT reason is rejected by codec validation");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "unknown INTENT enum leaves output unchanged");

    auto count_mismatch = MakePreparedRequest(
        chain.header,
        std::span<const ingress::ReserveStateEntryV1>(
            chain.grants.data(), 2U));
    output = sentinel;
    test->ExpectError(
        ingress::BuildReserveReleasingPreparedV1(
            chain.header,
            chain.intent,
            count_mismatch,
            &output),
        ingress::ReserveStateV1Error::kInvalidCount,
        "PREPARED cannot omit a frozen registry entry");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "count mismatch leaves output unchanged");

    auto understated_release = MakePreparedRequest(
        chain.header, chain.grants);
    understated_release.expected_release_fs_inodes =
        chain.header.declared_inode_reserve_count;
    output = sentinel;
    test->ExpectError(
        ingress::BuildReserveReleasingPreparedV1(
            chain.header,
            chain.intent,
            understated_release,
            &output),
        ingress::ReserveStateV1Error::kInvalidAggregate,
        "PREPARED cannot omit the data-reserve inode from expected release charge");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "understated physical release charge leaves output unchanged");

    auto wrong_identity_grants = chain.grants;
    wrong_identity_grants[1U].stream_day_id =
        Pattern<16U>(0x01U);
    const auto wrong_identity = MakePreparedRequest(
        chain.header, wrong_identity_grants);
    output = sentinel;
    test->ExpectError(
        ingress::BuildReserveReleasingPreparedV1(
            chain.header,
            chain.intent,
            wrong_identity,
            &output),
        ingress::ReserveStateV1Error::kImmutableFactChanged,
        "PREPARED cannot replace a frozen namespace identity");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "identity mismatch leaves output unchanged");

    auto huge_header = chain.header;
    huge_header.allocation_quantum_bytes =
        std::numeric_limits<std::uint64_t>::max();
    huge_header.declared_releasable_bytes =
        std::numeric_limits<std::uint64_t>::max();
    const auto huge_provisioned =
        MakeProvisioned(huge_header, 10U);
    ingress::ReserveStateSlotV1 huge_intent{};
    test->ExpectError(
        ingress::BuildReserveReleasingIntentV1(
            huge_header,
            huge_provisioned,
            MakeIntentRequest(),
            &huge_intent),
        ingress::ReserveStateV1Error::kNone,
        "large valid header reaches PREPARED arithmetic gate");
    auto overflow_grants = chain.grants;
    overflow_grants[0U].actions[0U].byte_cap_quanta = 2U;
    const auto overflow_prepared = MakePreparedRequest(
        huge_header, overflow_grants);
    output = sentinel;
    test->ExpectError(
        ingress::BuildReserveReleasingPreparedV1(
            huge_header,
            huge_intent,
            overflow_prepared,
            &output),
        ingress::ReserveStateV1Error::kArithmeticOverflow,
        "PREPARED action-byte multiplication is checked");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "cap overflow leaves output unchanged");
}

void TestGrantActionSuccessAndFailure(TestContext* test) {
    ReleaseChain chain{};
    if (!BuildReleaseChain(test, &chain)) {
        return;
    }
    ingress::ReserveStateSlotV1 sentinel{};
    sentinel.generation = 91U;
    auto output = sentinel;

    auto second_activation = MakeActivation(chain.consumed, 1U);
    test->ExpectError(
        ingress::BuildReserveActivateNextGrantV1(
            chain.header,
            chain.consumed,
            second_activation,
            &output),
        ingress::ReserveStateV1Error::kInvalidOrdering,
        "only the first PENDING grant can activate");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "out-of-order activation leaves output unchanged");

    auto bad_baseline = MakeActivation(chain.consumed, 0U);
    bad_baseline.filesystem_free_inode_baseline = 0U;
    output = sentinel;
    test->ExpectError(
        ingress::BuildReserveActivateNextGrantV1(
            chain.header,
            chain.consumed,
            bad_baseline,
            &output),
        ingress::ReserveStateV1Error::kInvalidActivation,
        "activation requires all four capacity baselines");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "bad activation baseline leaves output unchanged");

    auto wrong_executor = MakeActivation(chain.consumed, 0U);
    wrong_executor.executor_instance = Pattern<16U>(0x01U);
    output = sentinel;
    test->ExpectError(
        ingress::BuildReserveActivateNextGrantV1(
            chain.header,
            chain.consumed,
            wrong_executor,
            &output),
        ingress::ReserveStateV1Error::kImmutableFactChanged,
        "ACKED activation executor must equal ACK writer");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "wrong executor leaves output unchanged");

    ingress::ReserveStateSlotV1 active{};
    test->ExpectError(
        ingress::BuildReserveActivateNextGrantV1(
            chain.header,
            chain.consumed,
            MakeActivation(chain.consumed, 0U),
            &active),
        ingress::ReserveStateV1Error::kNone,
        "activate first grant");
    ExpectPair(
        test,
        chain.header,
        chain.consumed,
        active,
        "PENDING grant to ACTIVE");
    test->Expect(
        active.entries[0U].precharged_bytes ==
                active.entries[0U].grant_bytes &&
            active.entries[0U].activation_remaining_cap ==
                active.entries[0U].grant_bytes &&
            active.active_entry_index == 0U,
        "activation atomically precharges the full grant once");

    output = sentinel;
    test->ExpectError(
        ingress::BuildReserveDebitActionV1(
            chain.header,
            active,
            MakeActionKey(active, 0U, 1U),
            &output),
        ingress::ReserveStateV1Error::kInvalidOrdering,
        "action debit cannot skip a lower PENDING id");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "action-order rejection leaves output unchanged");

    auto stale_action = MakeActionKey(active, 0U, 0U);
    stale_action.object_plan_sha256 = Pattern<32U>(0x01U);
    output = sentinel;
    test->ExpectError(
        ingress::BuildReserveDebitActionV1(
            chain.header, active, stale_action, &output),
        ingress::ReserveStateV1Error::kImmutableFactChanged,
        "stale action commitment is rejected");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "stale action leaves output unchanged");

    ingress::ReserveStateSlotV1 debited{};
    const auto action0 = MakeActionKey(active, 0U, 0U);
    test->ExpectError(
        ingress::BuildReserveDebitActionV1(
            chain.header, active, action0, &debited),
        ingress::ReserveStateV1Error::kNone,
        "debit smallest pending action");
    ExpectPair(
        test,
        chain.header,
        active,
        debited,
        "PENDING action to DEBITED");
    test->Expect(
        debited.entries[0U].actions[0U].debit_generation ==
                debited.generation &&
            debited.entries[0U].activation_remaining_cap ==
                3U *
                    chain.header.allocation_quantum_bytes,
        "debit generation and remaining byte cap are mechanical");

    output = sentinel;
    test->ExpectError(
        ingress::BuildReserveDebitActionV1(
            chain.header,
            debited,
            MakeActionKey(debited, 0U, 1U),
            &output),
        ingress::ReserveStateV1Error::kInvalidTransition,
        "a second DEBITED action is rejected");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "second debit leaves output unchanged");

    ingress::ReserveStateSlotV1 completed{};
    test->ExpectError(
        ingress::BuildReserveCompleteActionV1(
            chain.header,
            debited,
            MakeActionKey(debited, 0U, 0U),
            &completed),
        ingress::ReserveStateV1Error::kNone,
        "complete sole debited action");
    ExpectPair(
        test,
        chain.header,
        debited,
        completed,
        "DEBITED action to COMPLETE");
    test->Expect(
        completed.entries[0U].actions[0U]
                .debit_generation ==
            debited.entries[0U].actions[0U]
                .debit_generation,
        "COMPLETE preserves the original debit generation");

    ingress::ReserveGrantTerminalReportV1 early_done{};
    early_done.grant = MakeGrantKey(completed, 0U);
    early_done.maintenance_report_sha256 =
        Pattern<32U>(0x11U);
    output = sentinel;
    test->ExpectError(
        ingress::BuildReserveMarkGrantDoneV1(
            chain.header, completed, early_done, &output),
        ingress::ReserveStateV1Error::kInvalidTransition,
        "DONE rejects a still-PENDING final report action");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "early DONE leaves output unchanged");

    ingress::ReserveStateSlotV1 report_debited{};
    test->ExpectError(
        ingress::BuildReserveDebitActionV1(
            chain.header,
            completed,
            MakeActionKey(completed, 0U, 1U),
            &report_debited),
        ingress::ReserveStateV1Error::kNone,
        "debit terminal report action");
    ingress::ReserveAtomicReportCompletionV1 atomic_report{};
    atomic_report.report_action =
        MakeActionKey(report_debited, 0U, 1U);
    atomic_report.maintenance_report_sha256 =
        Pattern<32U>(0x11U);
    ingress::ReserveStateSlotV1 first_done{};
    test->ExpectError(
        ingress::BuildReserveCompleteReportAndMarkGrantDoneV1(
            chain.header,
            report_debited,
            atomic_report,
            &first_done),
        ingress::ReserveStateV1Error::kNone,
        "atomically complete report receipt and grant");
    ExpectPair(
        test,
        chain.header,
        report_debited,
        first_done,
        "report-before-receipt window to DONE");
    test->Expect(
        first_done.entries[0U].grant_status ==
                ingress::ReserveGrantStatusV1::kDone &&
            first_done.entries[0U].actions[1U].action_state ==
                ingress::FinalizationActionStateV1::kComplete &&
            first_done.completed_bitmap == 1U &&
            first_done.active_entry_index ==
                ingress::kReserveStateV1NoActiveEntry,
        "atomic report completion sets DONE bitmap and clears active");

    ingress::ReserveStateSlotV1 second_active{};
    test->ExpectError(
        ingress::BuildReserveActivateNextGrantV1(
            chain.header,
            first_done,
            MakeActivation(first_done, 1U),
            &second_active),
        ingress::ReserveStateV1Error::kNone,
        "next grant activates only after first DONE");
    ingress::ReserveStateSlotV1 second_debited{};
    test->ExpectError(
        ingress::BuildReserveDebitActionV1(
            chain.header,
            second_active,
            MakeActionKey(second_active, 1U, 0U),
            &second_debited),
        ingress::ReserveStateV1Error::kNone,
        "debit action for second grant");
    ingress::ReserveStateSlotV1 second_failed{};
    test->ExpectError(
        ingress::BuildReserveFailDebitedActionV1(
            chain.header,
            second_debited,
            MakeActionKey(second_debited, 1U, 0U),
            &second_failed),
        ingress::ReserveStateV1Error::kNone,
        "atomically fail receipt and active grant");
    ExpectPair(
        test,
        chain.header,
        second_debited,
        second_failed,
        "DEBITED action and grant to FAILED");
    test->Expect(
        second_failed.entries[1U].actions[0U].action_state ==
                ingress::FinalizationActionStateV1::kFailed &&
            second_failed.entries[1U].grant_status ==
                ingress::ReserveGrantStatusV1::kFailed &&
            second_failed.active_entry_index ==
                ingress::kReserveStateV1NoActiveEntry &&
            second_failed.entries[1U]
                    .maintenance_report_sha256 ==
                ingress::ReserveStateV1Digest{},
        "action failure preserves zero report evidence and clears active");

    output = sentinel;
    test->ExpectError(
        ingress::BuildReserveActivateNextGrantV1(
            chain.header,
            second_failed,
            MakeActivation(second_failed, 2U),
            &output),
        ingress::ReserveStateV1Error::kInvalidTransition,
        "FAILED grant prevents activation of remaining PENDING grant");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "post-failure activation leaves output unchanged");

    auto max_generation = chain.consumed;
    max_generation.generation =
        std::numeric_limits<std::uint64_t>::max();
    output = sentinel;
    test->ExpectError(
        ingress::BuildReserveActivateNextGrantV1(
            chain.header,
            max_generation,
            MakeActivation(max_generation, 0U),
            &output),
        ingress::ReserveStateV1Error::kInvalidGeneration,
        "grant transition rejects generation wrap");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "grant generation overflow leaves output unchanged");
}

ingress::ReserveStateSlotV1 CompleteFirstAction(
    TestContext* test,
    const ReleaseChain& chain,
    const ingress::ReserveStateSlotV1& active) {
    ingress::ReserveStateSlotV1 debited{};
    ingress::ReserveStateSlotV1 completed{};
    auto error = ingress::BuildReserveDebitActionV1(
        chain.header,
        active,
        MakeActionKey(active, 0U, 0U),
        &debited);
    test->ExpectError(
        error,
        ingress::ReserveStateV1Error::kNone,
        "cross-failure fixture debits first action");
    if (error == ingress::ReserveStateV1Error::kNone) {
        error = ingress::BuildReserveCompleteActionV1(
            chain.header,
            debited,
            MakeActionKey(debited, 0U, 0U),
            &completed);
        test->ExpectError(
            error,
            ingress::ReserveStateV1Error::kNone,
            "cross-failure fixture completes first action");
    }
    return completed;
}

void TestSeparateDoneAndCrossObjectFailure(
    TestContext* test) {
    ReleaseChain chain{};
    if (!BuildReleaseChain(test, &chain)) {
        return;
    }
    ingress::ReserveStateSlotV1 active{};
    test->ExpectError(
        ingress::BuildReserveActivateNextGrantV1(
            chain.header,
            chain.consumed,
            MakeActivation(chain.consumed, 0U),
            &active),
        ingress::ReserveStateV1Error::kNone,
        "activate separate-DONE fixture");
    auto first_complete =
        CompleteFirstAction(test, chain, active);

    ingress::ReserveStateSlotV1 report_debited{};
    ingress::ReserveStateSlotV1 all_complete{};
    test->ExpectError(
        ingress::BuildReserveDebitActionV1(
            chain.header,
            first_complete,
            MakeActionKey(first_complete, 0U, 1U),
            &report_debited),
        ingress::ReserveStateV1Error::kNone,
        "debit report for separate completion");
    test->ExpectError(
        ingress::BuildReserveCompleteActionV1(
            chain.header,
            report_debited,
            MakeActionKey(report_debited, 0U, 1U),
            &all_complete),
        ingress::ReserveStateV1Error::kNone,
        "complete report while grant remains ACTIVE");
    test->Expect(
        all_complete.entries[0U].grant_status ==
                ingress::ReserveGrantStatusV1::kActive &&
            all_complete.entries[0U]
                    .maintenance_report_sha256 ==
                ingress::ReserveStateV1Digest{},
        "receipt COMPLETE does not invent report validation or DONE");

    ingress::ReserveGrantTerminalReportV1 terminal_report{};
    terminal_report.grant = MakeGrantKey(all_complete, 0U);
    terminal_report.maintenance_report_sha256 =
        Pattern<32U>(0x22U);
    ingress::ReserveStateSlotV1 done{};
    test->ExpectError(
        ingress::BuildReserveMarkGrantDoneV1(
            chain.header,
            all_complete,
            terminal_report,
            &done),
        ingress::ReserveStateV1Error::kNone,
        "mark all-COMPLETE grant DONE");
    ExpectPair(
        test,
        chain.header,
        all_complete,
        done,
        "all COMPLETE actions to DONE");

    ReleaseChain failure_chain{};
    if (!BuildReleaseChain(test, &failure_chain)) {
        return;
    }
    ingress::ReserveStateSlotV1 failure_active{};
    test->ExpectError(
        ingress::BuildReserveActivateNextGrantV1(
            failure_chain.header,
            failure_chain.consumed,
            MakeActivation(failure_chain.consumed, 0U),
            &failure_active),
        ingress::ReserveStateV1Error::kNone,
        "activate cross-object failure fixture");
    ingress::ReserveStateSlotV1 sentinel{};
    sentinel.generation = 123U;
    auto output = sentinel;
    ingress::ReserveGrantFailureV1 too_early{};
    too_early.grant = MakeGrantKey(failure_active, 0U);
    test->ExpectError(
        ingress::BuildReserveFailActiveGrantV1(
            failure_chain.header,
            failure_active,
            too_early,
            &output),
        ingress::ReserveStateV1Error::kInvalidTransition,
        "cross-object failure requires prior COMPLETE evidence");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "too-early cross-object failure leaves output unchanged");

    const auto partial_complete = CompleteFirstAction(
        test, failure_chain, failure_active);
    ingress::ReserveGrantFailureV1 premature_report{};
    premature_report.grant =
        MakeGrantKey(partial_complete, 0U);
    premature_report.maintenance_report_sha256 =
        Pattern<32U>(0x33U);
    output = sentinel;
    test->ExpectError(
        ingress::BuildReserveFailActiveGrantV1(
            failure_chain.header,
            partial_complete,
            premature_report,
            &output),
        ingress::ReserveStateV1Error::kInvalidTransition,
        "report hash is not accepted before report action COMPLETE");
    ExpectUnchanged(
        test,
        output,
        sentinel,
        "premature report evidence leaves output unchanged");

    ingress::ReserveGrantFailureV1 cross_failure{};
    cross_failure.grant =
        MakeGrantKey(partial_complete, 0U);
    ingress::ReserveStateSlotV1 failed{};
    test->ExpectError(
        ingress::BuildReserveFailActiveGrantV1(
            failure_chain.header,
            partial_complete,
            cross_failure,
            &failed),
        ingress::ReserveStateV1Error::kNone,
        "cross-object failure preserves COMPLETE receipt");
    ExpectPair(
        test,
        failure_chain.header,
        partial_complete,
        failed,
        "ACTIVE to FAILED after COMPLETE evidence");
    test->Expect(
        failed.entries[0U].actions[0U].action_state ==
                ingress::FinalizationActionStateV1::kComplete &&
            failed.entries[0U].grant_status ==
                ingress::ReserveGrantStatusV1::kFailed,
        "cross-object failure never rewrites COMPLETE to FAILED");

    ingress::ReserveGrantFailureV1 with_report{};
    with_report.grant = MakeGrantKey(all_complete, 0U);
    with_report.maintenance_report_sha256 =
        Pattern<32U>(0x44U);
    ingress::ReserveStateSlotV1 failed_with_report{};
    test->ExpectError(
        ingress::BuildReserveFailActiveGrantV1(
            chain.header,
            all_complete,
            with_report,
            &failed_with_report),
        ingress::ReserveStateV1Error::kNone,
        "all-COMPLETE cross-object failure retains report digest");
    test->Expect(
        failed_with_report.entries[0U]
                .maintenance_report_sha256 ==
                with_report.maintenance_report_sha256 &&
            failed_with_report.entries[0U].actions ==
                all_complete.entries[0U].actions,
        "failure stores existing report evidence without action rewrite");
}

void TestFencedNoAckAndZeroCapAction(TestContext* test) {
    const auto header = MakeHeader();
    const auto provisioned = MakeProvisioned(header, 40U);
    ingress::ReserveStateSlotV1 intent{};
    test->ExpectError(
        ingress::BuildReserveReleasingIntentV1(
            header,
            provisioned,
            MakeIntentRequest(),
            &intent),
        ingress::ReserveStateV1Error::kNone,
        "build FENCED_NO_ACK INTENT fixture");

    std::array<ingress::ReserveStateEntryV1, 3U> grants{};
    for (std::size_t index = 0U;
         index < grants.size();
         ++index) {
        grants[index] = MakeGrant(
            provisioned.entries[index],
            header,
            static_cast<std::uint8_t>(0xd0U + index * 8U));
    }
    auto& fenced = grants[0U];
    fenced.ack_status =
        ingress::ReserveAckStatusV1::kFencedNoAck;
    fenced.writer_instance = {};
    fenced.counter_validity =
        ingress::kReserveCounterAppendPair |
        ingress::kReserveCounterDurablePair;
    fenced.raw_counters.callback_published_records = 0U;
    fenced.raw_counters.callback_published_vendor_bytes = 0U;
    fenced.raw_counters.queued_record_count = 0U;
    fenced.raw_counters.queued_framed_wal_bytes = 0U;
    fenced.actions[0U].byte_cap_quanta = 0U;
    fenced.actions[0U].inode_cap = 0U;
    fenced.grant_bytes =
        3U * header.allocation_quantum_bytes;

    ingress::ReserveStateSlotV1 prepared{};
    test->ExpectError(
        ingress::BuildReserveReleasingPreparedV1(
            header,
            intent,
            MakePreparedRequest(header, grants),
            &prepared),
        ingress::ReserveStateV1Error::kNone,
        "PREPARED accepts externally fenced zero-mutation facts");
    ingress::ReserveStateSlotV1 consumed{};
    test->ExpectError(
        ingress::BuildReserveConsumedV1(
            header, prepared, &consumed),
        ingress::ReserveStateV1Error::kNone,
        "consume FENCED_NO_ACK fixture");

    auto activation = MakeActivation(consumed, 0U);
    activation.executor_instance = Pattern<16U>(0x05U);
    ingress::ReserveStateSlotV1 active{};
    test->ExpectError(
        ingress::BuildReserveActivateNextGrantV1(
            header, consumed, activation, &active),
        ingress::ReserveStateV1Error::kNone,
        "FENCED_NO_ACK grant accepts a nonzero replacement executor");
    test->Expect(
        active.entries[0U].writer_instance ==
                ingress::ReserveStateV1Identity{} &&
            active.entries[0U].executor_or_recovery_attempt ==
                activation.executor_instance,
        "fenced activation does not fabricate an ACK writer");

    ingress::ReserveStateSlotV1 debited{};
    test->ExpectError(
        ingress::BuildReserveDebitActionV1(
            header,
            active,
            MakeActionKey(active, 0U, 0U),
            &debited),
        ingress::ReserveStateV1Error::kNone,
        "zero-cap evidence action still enters DEBITED");
    test->Expect(
        debited.entries[0U].activation_remaining_cap ==
                active.entries[0U].activation_remaining_cap &&
            debited.entries[0U].actions[0U].debit_generation ==
                debited.generation,
        "zero-cap debit preserves remaining capacity but records generation");
    ingress::ReserveStateSlotV1 completed{};
    test->ExpectError(
        ingress::BuildReserveCompleteActionV1(
            header,
            debited,
            MakeActionKey(debited, 0U, 0U),
            &completed),
        ingress::ReserveStateV1Error::kNone,
        "zero-cap evidence action completes through full FSM");
}

}  // namespace

int main() {
    TestContext test;
    TestReleaseChainAndBoundaries(&test);
    TestGrantActionSuccessAndFailure(&test);
    TestSeparateDoneAndCrossObjectFailure(&test);
    TestFencedNoAckAndZeroCapAction(&test);
    if (test.failures != 0) {
        std::cerr << test.failures << " test(s) failed\n";
        return 1;
    }
    std::cout
        << "phase2 reserve emergency transition tests passed\n";
    return 0;
}
