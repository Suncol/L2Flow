#include "l2flow/common/crc32c.h"
#include "l2flow/common/sha256.h"
#include "l2flow/ingress/reserve_state_v1.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <optional>
#include <span>
#include <string>

namespace common = l2flow::common;
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

void PutU64(
    std::span<std::byte> output,
    std::size_t offset,
    std::uint64_t value) {
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        output[offset + index] =
            static_cast<std::byte>((value >> shift) & 0xffU);
    }
}

void PutU32(
    std::span<std::byte> output,
    std::size_t offset,
    std::uint32_t value) {
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        output[offset + index] =
            static_cast<std::byte>((value >> shift) & 0xffU);
    }
}

std::uint32_t GetU32(
    std::span<const std::byte> input,
    std::size_t offset) {
    std::uint32_t value = 0U;
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        value |=
            std::to_integer<std::uint32_t>(input[offset + index])
            << shift;
    }
    return value;
}

void RecomputeCrc(
    std::span<std::byte> wire,
    std::size_t crc_offset) {
    PutU32(wire, crc_offset, 0U);
    PutU32(
        wire,
        crc_offset,
        common::ComputeCrc32c(
            std::span<const std::byte>(wire)));
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
    std::uint32_t source_stream_id = 7U,
    std::uint32_t capture_date = 20260718U) {
    ingress::ReserveStateEntryV1 entry{};
    entry.source_stream_id = source_stream_id;
    entry.capture_date = capture_date;
    entry.stream_day_id = Pattern<16U>(
        static_cast<std::uint8_t>(0x90U + source_stream_id));
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
        0x0102030405060708ULL;
    return entry;
}

ingress::ReserveStateSlotV1 MakeRegistrySlot(
    const ingress::ReserveCoordinatorHeaderV1& header,
    std::uint64_t generation,
    ingress::ReserveCoordinatorPhaseV1 phase) {
    ingress::ReserveStateSlotV1 slot{};
    slot.coordinator_state = phase;
    slot.generation = generation;
    slot.reserve_state_uuid = header.reserve_state_uuid;
    slot.entry_count = 1U;
    slot.entries[0U] = MakeRegistryEntry();
    if (phase ==
        ingress::ReserveCoordinatorPhaseV1::
            kReleasingIntent) {
        slot.reason =
            ingress::ReserveReleaseReasonV1::kLowWatermark;
        slot.trigger =
            ingress::ReserveReleaseTriggerV1::
                kFilesystemBytes;
        slot.writer_set_sha256 = Pattern<32U>(0xc0U);
    }
    return slot;
}

void SetAction(
    ingress::ReserveStateEntryV1* entry,
    std::size_t action_id,
    ingress::FinalizationActionKindV1 kind,
    std::uint32_t byte_cap_quanta,
    std::uint32_t inode_cap,
    std::uint8_t digest_first) {
    auto& receipt = entry->actions[action_id];
    receipt.action_id =
        static_cast<std::uint16_t>(action_id);
    receipt.action_kind = kind;
    receipt.action_state =
        ingress::FinalizationActionStateV1::kPending;
    receipt.byte_cap_quanta = byte_cap_quanta;
    receipt.inode_cap = inode_cap;
    receipt.object_plan_sha256 =
        Pattern<32U>(digest_first);

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
        static_cast<std::uint8_t>(digest_first + 1U));
}

ingress::ReserveStateEntryV1 MakePendingGrantEntry() {
    ingress::ReserveStateEntryV1 entry{};
    entry.source_stream_id = 7U;
    entry.capture_date = 20260718U;
    entry.stream_day_id = Pattern<16U>(0x97U);
    entry.ack_status = ingress::ReserveAckStatusV1::kAcked;
    entry.counter_validity =
        ingress::kReserveCounterValidityMask;
    entry.grant_status =
        ingress::ReserveGrantStatusV1::kPending;
    entry.grant_flags =
        ingress::kReserveGrantRawFinalization;
    entry.writer_instance = Pattern<16U>(0xa0U);
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
        0x0102030405060708ULL;
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
    slot.reserve_state_uuid = header.reserve_state_uuid;
    slot.reason =
        ingress::ReserveReleaseReasonV1::kLowWatermark;
    slot.trigger =
        ingress::ReserveReleaseTriggerV1::
            kFilesystemBytes;
    slot.entry_count = 1U;
    slot.finalization_cycle_id = Pattern<16U>(0x30U);
    slot.writer_set_sha256 = Pattern<32U>(0x50U);
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

ingress::ReserveStateSlotV1 MakeConsumedPendingSlot(
    const ingress::ReserveCoordinatorHeaderV1& header,
    std::uint64_t generation) {
    ingress::ReserveStateSlotV1 slot =
        MakePreparedSlot(header, generation);
    slot.coordinator_state =
        ingress::ReserveCoordinatorPhaseV1::kConsumed;
    return slot;
}

void TestSchemaHash(TestContext* test) {
    test->Expect(
        ingress::kReserveStateV1FileBytes == 69632U,
        "state file size is exactly 69,632");
    test->Expect(
        std::any_of(
            ingress::kReserveStateV1SchemaSha256.begin(),
            ingress::kReserveStateV1SchemaSha256.end(),
            [](std::byte value) {
                return value != std::byte{0};
            }),
        "frozen schema SHA-256 constant is nonzero");
    test->Expect(
        ingress::reserve_state_v1_offset::slot::kPlans ==
            22016U &&
            ingress::reserve_state_v1_offset::slot::
                    kZeroTail ==
                32256U,
        "slot section offsets are frozen");

    common::Sha256Digest digest{};
    std::string error;
    const std::filesystem::path path =
        std::filesystem::path(__FILE__)
            .parent_path()
            .parent_path() /
        "schemas/reserve_state_v1.json";
    const bool hashed = common::ComputeFileSha256(
        path,
        &digest,
        &error,
        std::optional<std::uint64_t>{
            ingress::kReserveStateV1SchemaBytes});
    test->Expect(hashed, "frozen schema hashes: " + error);
    if (hashed) {
        test->Expect(
            digest == ingress::kReserveStateV1SchemaSha256,
            "schema digest constant matches exact file");
        test->Expect(
            common::Sha256Hex(digest) ==
                ingress::kReserveStateV1SchemaSha256Hex,
            "schema digest hex constant matches");
    }
    std::error_code size_error;
    const auto size = std::filesystem::file_size(
        path, size_error);
    test->Expect(
        !size_error &&
            size == ingress::kReserveStateV1SchemaBytes,
        "schema byte count is frozen");
}

void TestHeaderCodec(TestContext* test) {
    const auto header = MakeHeader();
    ingress::ReserveStateV1HeaderWire wire{};
    test->ExpectError(
        ingress::EncodeReserveCoordinatorHeaderV1(
            header, &wire),
        ingress::ReserveStateV1Error::kNone,
        "encode immutable header");
    test->Expect(
        std::equal(
            ingress::kReserveStateV1HeaderMagic.begin(),
            ingress::kReserveStateV1HeaderMagic.end(),
            wire.begin()),
        "header magic bytes");
    test->Expect(
        GetU32(
            wire,
            ingress::reserve_state_v1_offset::header::
                kHeaderSize) == 4096U &&
            GetU32(
                wire,
                ingress::reserve_state_v1_offset::header::
                    kFileSize) == 69632U,
        "header freezes header and file sizes");
    test->Expect(
        std::equal(
            ingress::kReserveStateV1SchemaSha256.begin(),
            ingress::kReserveStateV1SchemaSha256.end(),
            wire.begin() +
                ingress::reserve_state_v1_offset::header::
                    kSchemaSha256),
        "header embeds exact schema digest");

    auto zeroed = wire;
    PutU32(
        zeroed,
        ingress::reserve_state_v1_offset::header::
            kHeaderCrc32c,
        0U);
    test->Expect(
        GetU32(
            wire,
            ingress::reserve_state_v1_offset::header::
                kHeaderCrc32c) ==
            common::ComputeCrc32c(
                std::span<const std::byte>(zeroed)),
        "header CRC covers 4096 bytes with CRC field zero");

    ingress::ReserveCoordinatorHeaderV1 decoded{};
    test->ExpectError(
        ingress::DecodeReserveCoordinatorHeaderV1(
            wire, &decoded),
        ingress::ReserveStateV1Error::kNone,
        "decode immutable header");
    test->Expect(decoded == header, "header round trip");

    auto bad_reserved = wire;
    bad_reserved[300U] = std::byte{1};
    RecomputeCrc(
        bad_reserved,
        ingress::reserve_state_v1_offset::header::
            kHeaderCrc32c);
    test->ExpectError(
        ingress::DecodeReserveCoordinatorHeaderV1(
            bad_reserved, &decoded),
        ingress::ReserveStateV1Error::kNonzeroReserved,
        "nonzero header reserved bytes rejected despite valid CRC");

    auto wrong_schema = header;
    wrong_schema.schema_sha256 = Pattern<32U>(0x01U);
    test->ExpectError(
        ingress::EncodeReserveCoordinatorHeaderV1(
            wrong_schema, &wire),
        ingress::ReserveStateV1Error::kSchemaMismatch,
        "unknown schema identity rejected");
}

void TestBootstrapAndGolden(TestContext* test) {
    ingress::ReserveCoordinatorStateV1 state{};
    state.header = MakeHeader();
    state.slots[0U] = MakeRegistrySlot(
        state.header,
        1U,
        ingress::ReserveCoordinatorPhaseV1::kProvisioned);
    state.slots[1U] = state.slots[0U];
    ingress::ReserveStateV1FileWire wire{};
    test->ExpectError(
        ingress::EncodeReserveCoordinatorStateV1(
            state, &wire),
        ingress::ReserveStateV1Error::kNone,
        "encode byte-identical generation-1 bootstrap");
    test->Expect(
        std::equal(
            wire.begin() + ingress::kReserveStateV1HeaderBytes,
            wire.begin() + ingress::kReserveStateV1HeaderBytes +
                ingress::kReserveStateV1SlotBytes,
            wire.begin() + ingress::kReserveStateV1HeaderBytes +
                ingress::kReserveStateV1SlotBytes),
        "bootstrap slots are byte-identical");
    test->Expect(
        std::any_of(
            wire.begin() + ingress::kReserveStateV1HeaderBytes,
            wire.begin() + ingress::kReserveStateV1HeaderBytes +
                ingress::kReserveStateV1SlotBytes,
            [](std::byte value) {
                return value != std::byte{0};
            }),
        "byte-identical bootstrap slots are valid nonzero slots");

    ingress::ReserveCoordinatorStateV1 decoded{};
    test->ExpectError(
        ingress::DecodeAndSelectReserveCoordinatorStateV1(
            wire, &decoded),
        ingress::ReserveStateV1Error::kNone,
        "decode bootstrap state");
    test->Expect(
        decoded.selected_slot == 0U,
        "bootstrap logical selection is slot zero");

    const std::string golden_sha =
        common::Sha256Hex(common::ComputeSha256(wire));
    test->Expect(
        golden_sha ==
            "3676cefef391ba73b190f3afdb832254"
            "a149e84c96dafcf199be9ebd5533e04e",
        "complete 69,632-byte bootstrap golden SHA-256: " +
            golden_sha);

    auto discontinuous = state;
    discontinuous.slots[1U].generation = 3U;
    test->ExpectError(
        ingress::EncodeReserveCoordinatorStateV1(
            discontinuous, &wire),
        ingress::ReserveStateV1Error::kInvalidGeneration,
        "generation discontinuity rejected");

    auto equal_nonbootstrap = state;
    equal_nonbootstrap.slots[0U].generation = 2U;
    equal_nonbootstrap.slots[1U].generation = 2U;
    test->ExpectError(
        ingress::EncodeReserveCoordinatorStateV1(
            equal_nonbootstrap, &wire),
        ingress::ReserveStateV1Error::kInvalidGeneration,
        "equal generation only allowed for bootstrap");

    auto first_transition = state;
    first_transition.slots[1U].generation = 2U;
    test->ExpectError(
        ingress::EncodeReserveCoordinatorStateV1(
            first_transition, &wire),
        ingress::ReserveStateV1Error::kNone,
        "first transition writes physical slot one generation two");

    auto arbitrary_writer = first_transition;
    arbitrary_writer.slots[1U].entries[0U].writer_instance =
        Pattern<16U>(0xeeU);
    test->Expect(
        ingress::EncodeReserveCoordinatorStateV1(
            arbitrary_writer, &wire) !=
            ingress::ReserveStateV1Error::kNone,
        "ACTIVE writer identity cannot change in an arbitrary PROVISIONED-to-PROVISIONED generation");

    auto replaced_namespace = first_transition;
    replaced_namespace.slots[1U]
        .entries[0U]
        .stream_day_id = Pattern<16U>(0xefU);
    test->Expect(
        ingress::EncodeReserveCoordinatorStateV1(
            replaced_namespace, &wire) !=
            ingress::ReserveStateV1Error::kNone,
        "registered stream-day identity is immutable across generations");

    auto active_takeover = first_transition;
    auto& takeover =
        active_takeover.slots[1U].entries[0U];
    takeover.registry_status =
        ingress::ReserveRegistryStatusV1::kRecovering;
    takeover.recovery_origin =
        ingress::ReserveRecoveryOriginV1::kActiveTakeover;
    takeover.writer_instance = Pattern<16U>(0xd0U);
    takeover.executor_or_recovery_attempt =
        Pattern<16U>(0xe0U);
    test->ExpectError(
        ingress::EncodeReserveCoordinatorStateV1(
            active_takeover, &wire),
        ingress::ReserveStateV1Error::kNone,
        "ACTIVE takeover is an explicit identity-preserving transition with a fresh attempt");

    auto status_skip = first_transition;
    auto& skipped = status_skip.slots[1U].entries[0U];
    skipped.registry_status =
        ingress::ReserveRegistryStatusV1::kScaffolding;
    skipped.recovery_origin =
        ingress::ReserveRecoveryOriginV1::kFreshInit;
    skipped.grant_bytes = 4096U;
    test->Expect(
        ingress::EncodeReserveCoordinatorStateV1(
            status_skip, &wire) !=
            ingress::ReserveStateV1Error::kNone,
        "ACTIVE cannot regress to SCAFFOLDING");

    auto reversed_transition = state;
    reversed_transition.slots[0U].generation = 2U;
    test->ExpectError(
        ingress::EncodeReserveCoordinatorStateV1(
            reversed_transition, &wire),
        ingress::ReserveStateV1Error::kInvalidGeneration,
        "physical slot alternation rejects reversed first transition");

    auto nonidentical = state;
    nonidentical.slots[1U].entries[0U].writer_instance =
        Pattern<16U>(0xeeU);
    test->ExpectError(
        ingress::EncodeReserveCoordinatorStateV1(
            nonidentical, &wire),
        ingress::ReserveStateV1Error::kInvalidGeneration,
        "bootstrap requires byte-identical slots");
}

void TestPreparedConsumedCodec(TestContext* test) {
    ingress::ReserveCoordinatorStateV1 state{};
    state.header = MakeHeader();
    state.slots[0U] =
        MakeConsumedPendingSlot(state.header, 9U);
    state.slots[1U] = MakePreparedSlot(state.header, 8U);
    state.selected_slot = 0U;

    ingress::ReserveStateV1FileWire wire{};
    test->ExpectError(
        ingress::EncodeReserveCoordinatorStateV1(
            state, &wire),
        ingress::ReserveStateV1Error::kNone,
        "encode PREPARED to CONSUMED pair");
    ingress::ReserveCoordinatorStateV1 decoded{};
    test->ExpectError(
        ingress::DecodeAndSelectReserveCoordinatorStateV1(
            wire, &decoded),
        ingress::ReserveStateV1Error::kNone,
        "decode PREPARED to CONSUMED pair");
    test->Expect(
        decoded == state,
        "prepared/consumed full-file round trip");

    const std::size_t slot_one =
        ingress::kReserveStateV1HeaderBytes;
    const std::size_t entry_zero =
        slot_one +
        ingress::reserve_state_v1_offset::slot::kEntries;
    test->Expect(
        GetU32(
            wire,
            entry_zero +
                ingress::reserve_state_v1_offset::entry::
                    kSourceStreamId) == 7U,
        "base entry exact offset");
    auto entry_crc_domain =
        std::array<std::byte, ingress::kReserveStateV1EntryBytes>{};
    std::copy_n(
        wire.begin() + entry_zero,
        entry_crc_domain.size(),
        entry_crc_domain.begin());
    const std::uint32_t stored_entry_crc = GetU32(
        entry_crc_domain,
        ingress::reserve_state_v1_offset::entry::
            kEntryCrc32c);
    PutU32(
        entry_crc_domain,
        ingress::reserve_state_v1_offset::entry::
            kEntryCrc32c,
        0U);
    test->Expect(
        stored_entry_crc ==
            common::ComputeCrc32c(entry_crc_domain),
        "entry CRC covers exact 224 bytes with field zero");
    const std::size_t receipt_zero =
        slot_one +
        ingress::reserve_state_v1_offset::slot::kReceipts;
    test->Expect(
        std::to_integer<std::uint8_t>(
            wire[receipt_zero +
                 ingress::reserve_state_v1_offset::receipt::
                     kActionKind]) ==
            static_cast<std::uint8_t>(
                ingress::FinalizationActionKindV1::
                    kCurrentSegmentDrain),
        "receipt exact offset and ordering");
    auto receipt_crc_domain =
        std::array<
            std::byte,
            ingress::kReserveStateV1ReceiptBytes>{};
    std::copy_n(
        wire.begin() + receipt_zero,
        receipt_crc_domain.size(),
        receipt_crc_domain.begin());
    const std::uint32_t stored_receipt_crc = GetU32(
        receipt_crc_domain,
        ingress::reserve_state_v1_offset::receipt::
            kReceiptCrc32c);
    PutU32(
        receipt_crc_domain,
        ingress::reserve_state_v1_offset::receipt::
            kReceiptCrc32c,
        0U);
    test->Expect(
        stored_receipt_crc ==
            common::ComputeCrc32c(receipt_crc_domain),
        "receipt CRC covers exact 64 bytes with field zero");
    const std::size_t plan_one =
        slot_one +
        ingress::reserve_state_v1_offset::slot::kPlans +
        ingress::kReserveStateV1PlanBytes;
    test->Expect(
        std::to_integer<std::uint8_t>(
            wire[plan_one +
                 ingress::reserve_state_v1_offset::plan::
                     kObjectType]) ==
            static_cast<std::uint8_t>(
                ingress::FinalizationActionKindV1::
                    kFinalizationReport),
        "plan exact offset and action-id ordering");
    test->Expect(
        std::all_of(
            wire.begin() + slot_one +
                ingress::reserve_state_v1_offset::slot::
                    kZeroTail,
            wire.begin() + slot_one +
                ingress::kReserveStateV1SlotBytes,
            [](std::byte value) {
                return value == std::byte{0};
            }),
        "last 512 slot bytes are zero");
    auto slot_crc_domain =
        std::array<std::byte, ingress::kReserveStateV1SlotBytes>{};
    std::copy_n(
        wire.begin() + slot_one,
        slot_crc_domain.size(),
        slot_crc_domain.begin());
    const std::uint32_t stored_slot_crc = GetU32(
        slot_crc_domain,
        ingress::reserve_state_v1_offset::slot::kSlotCrc32c);
    PutU32(
        slot_crc_domain,
        ingress::reserve_state_v1_offset::slot::kSlotCrc32c,
        0U);
    test->Expect(
        stored_slot_crc ==
            common::ComputeCrc32c(slot_crc_domain),
        "slot CRC covers exact 32,768 bytes with field zero");

    auto changed = state;
    changed.slots[0U].entries[0U]
        .actions[0U]
        .byte_cap_quanta = 1U;
    changed.slots[0U].entries[0U].grant_bytes =
        4U * 4096U;
    changed.slots[0U].aggregate_grant_bytes =
        4U * 4096U;
    test->ExpectError(
        ingress::EncodeReserveCoordinatorStateV1(
            changed, &wire),
        ingress::ReserveStateV1Error::
            kImmutableFactChanged,
        "PREPARED to CONSUMED immutable action facts protected");

    auto activated_too_early = state;
    auto& early =
        activated_too_early.slots[0U].entries[0U];
    early.grant_status =
        ingress::ReserveGrantStatusV1::kActive;
    early.executor_or_recovery_attempt =
        early.writer_instance;
    early.activation_fs_free_baseline = 900000U;
    early.activation_quota_free_baseline = 800000U;
    early.activation_remaining_cap = early.grant_bytes;
    early.precharged_bytes = early.grant_bytes;
    activated_too_early.slots[0U].active_entry_index = 0U;
    activated_too_early.slots[0U]
        .active_fs_free_inode_baseline = 800U;
    activated_too_early.slots[0U]
        .active_quota_free_inode_baseline = 700U;
    test->ExpectError(
        ingress::EncodeReserveCoordinatorStateV1(
            activated_too_early, &wire),
        ingress::ReserveStateV1Error::kInvalidTransition,
        "PREPARED to CONSUMED cannot skip the all-PENDING barrier");
}

void TestGenerationAndCorruptionPolicy(TestContext* test) {
    ingress::ReserveCoordinatorStateV1 state{};
    state.header = MakeHeader();
    state.slots[0U] = MakeRegistrySlot(
        state.header,
        1U,
        ingress::ReserveCoordinatorPhaseV1::kProvisioned);
    state.slots[1U] = state.slots[0U];
    ingress::ReserveStateV1FileWire wire{};
    test->ExpectError(
        ingress::EncodeReserveCoordinatorStateV1(
            state, &wire),
        ingress::ReserveStateV1Error::kNone,
        "encode corruption-policy fixture");

    const std::size_t newer =
        ingress::kReserveStateV1HeaderBytes +
        ingress::kReserveStateV1SlotBytes;
    PutU64(
        wire,
        newer +
            ingress::reserve_state_v1_offset::slot::kGeneration,
        2U);
    wire[newer +
         ingress::reserve_state_v1_offset::slot::
             kReservedTail] = std::byte{1};
    RecomputeCrc(
        std::span<std::byte>(wire).subspan(
            newer, ingress::kReserveStateV1SlotBytes),
        ingress::reserve_state_v1_offset::slot::kSlotCrc32c);

    ingress::ReserveCoordinatorStateV1 decoded{};
    test->ExpectError(
        ingress::DecodeAndSelectReserveCoordinatorStateV1(
            wire, &decoded),
        ingress::ReserveStateV1Error::
            kSlotCorruptionFatal,
        "plausible newer slot with valid CRC but invalid schema is fatal");

    ingress::ReserveStateV1SlotWire zero_slot{};
    ingress::ReserveStateSlotV1 slot{};
    test->ExpectError(
        ingress::DecodeReserveStateSlotV1(
            state.header, zero_slot, &slot),
        ingress::ReserveStateV1Error::
            kSlotCorruptionFatal,
        "published all-zero slot rejected");
}

void TestOrderingFlagsAndArithmetic(TestContext* test) {
    const auto header = MakeHeader();
    auto slot = MakePreparedSlot(header, 4U);
    ingress::ReserveStateV1SlotWire wire{};

    slot.entry_count = 2U;
    slot.entries[1U] = slot.entries[0U];
    slot.entries[1U].stream_day_id = Pattern<16U>(0xf0U);
    slot.aggregate_grant_bytes *= 2U;
    slot.aggregate_inode_grant *= 2U;
    test->ExpectError(
        ingress::EncodeReserveStateSlotV1(
            header, slot, &wire),
        ingress::ReserveStateV1Error::kDuplicateRoute,
        "route uniqueness is source/date, not full key");

    slot = MakePreparedSlot(header, 4U);
    slot.entries[0U].actions[0U].action_flags =
        0x00010000U;
    test->ExpectError(
        ingress::EncodeReserveStateSlotV1(
            header, slot, &wire),
        ingress::ReserveStateV1Error::kUnknownFlags,
        "high action flag bits rejected");

    slot = MakePreparedSlot(header, 4U);
    slot.entries[0U].plans[0U].plan_flags =
        0x0100U;
    slot.entries[0U].actions[0U].action_flags =
        0x0100U;
    test->ExpectError(
        ingress::EncodeReserveStateSlotV1(
            header, slot, &wire),
        ingress::ReserveStateV1Error::kUnknownFlags,
        "unknown plan flag rejected");

    slot = MakePreparedSlot(header, 4U);
    slot.entries[0U].actions[0U] = {};
    slot.entries[0U].plans[0U] = {};
    test->ExpectError(
        ingress::EncodeReserveStateSlotV1(
            header, slot, &wire),
        ingress::ReserveStateV1Error::kInvalidReceipt,
        "used actions must be contiguous from zero");

    slot = MakePreparedSlot(header, 4U);
    std::swap(
        slot.entries[0U].actions[0U],
        slot.entries[0U].actions[1U]);
    std::swap(
        slot.entries[0U].plans[0U],
        slot.entries[0U].plans[1U]);
    slot.entries[0U].actions[0U].action_id = 0U;
    slot.entries[0U].actions[1U].action_id = 1U;
    test->ExpectError(
        ingress::EncodeReserveStateSlotV1(
            header, slot, &wire),
        ingress::ReserveStateV1Error::kInvalidReceipt,
        "FINALIZATION_REPORT must be report-last");

    auto huge_header = header;
    huge_header.allocation_quantum_bytes =
        std::numeric_limits<std::uint64_t>::max();
    huge_header.declared_releasable_bytes =
        std::numeric_limits<std::uint64_t>::max();
    slot = MakePreparedSlot(huge_header, 4U);
    test->ExpectError(
        ingress::EncodeReserveStateSlotV1(
            huge_header, slot, &wire),
        ingress::ReserveStateV1Error::kArithmeticOverflow,
        "byte cap multiplication uses checked arithmetic");
}

void TestConsumedFsm(TestContext* test) {
    const auto header = MakeHeader();
    auto pending = MakeConsumedPendingSlot(header, 20U);
    auto active = pending;
    active.generation = 21U;
    active.entries[0U].grant_status =
        ingress::ReserveGrantStatusV1::kActive;
    active.entries[0U].executor_or_recovery_attempt =
        active.entries[0U].writer_instance;
    active.entries[0U].activation_fs_free_baseline =
        900000U;
    active.entries[0U].activation_quota_free_baseline =
        800000U;
    active.entries[0U].activation_remaining_cap =
        active.entries[0U].grant_bytes;
    active.entries[0U].precharged_bytes =
        active.entries[0U].grant_bytes;
    active.active_entry_index = 0U;
    active.active_fs_free_inode_baseline = 800U;
    active.active_quota_free_inode_baseline = 700U;

    ingress::ReserveStateV1SlotWire pending_wire{};
    ingress::ReserveStateV1SlotWire active_wire{};
    test->ExpectError(
        ingress::EncodeReserveStateSlotV1(
            header, pending, &pending_wire),
        ingress::ReserveStateV1Error::kNone,
        "encode pending grant");
    auto bad_pending = pending;
    bad_pending.entries[0U].actions[0U].byte_cap_quanta =
        0U;
    bad_pending.entries[0U].actions[0U].inode_cap = 0U;
    bad_pending.entries[0U].actions[0U].action_state =
        ingress::FinalizationActionStateV1::kComplete;
    bad_pending.entries[0U].actions[0U].debit_generation =
        19U;
    bad_pending.entries[0U].grant_bytes =
        3U * header.allocation_quantum_bytes;
    bad_pending.aggregate_grant_bytes =
        bad_pending.entries[0U].grant_bytes;
    bad_pending.aggregate_inode_grant = 2U;
    test->ExpectError(
        ingress::EncodeReserveStateSlotV1(
            header, bad_pending, &active_wire),
        ingress::ReserveStateV1Error::kInvalidActivation,
        "PENDING rejects even a zero-cap COMPLETE receipt");
    test->ExpectError(
        ingress::EncodeReserveStateSlotV1(
            header, active, &active_wire),
        ingress::ReserveStateV1Error::kNone,
        "PENDING to ACTIVE performs full precharge");
    test->ExpectError(
        ingress::ValidateReserveStateSlotPairV1(
            header,
            pending,
            pending_wire,
            active,
            active_wire),
        ingress::ReserveStateV1Error::kNone,
        "PENDING to ACTIVE transition accepted");

    auto bad_active = active;
    bad_active.entries[0U].activation_remaining_cap -= 1U;
    test->ExpectError(
        ingress::EncodeReserveStateSlotV1(
            header, bad_active, &active_wire),
        ingress::ReserveStateV1Error::kInvalidActivation,
        "remaining cap is mechanically derived");

    auto debited = active;
    debited.generation = 22U;
    debited.entries[0U].actions[0U].action_state =
        ingress::FinalizationActionStateV1::kDebited;
    debited.entries[0U].actions[0U].debit_generation = 22U;
    debited.entries[0U].activation_remaining_cap =
        3U * header.allocation_quantum_bytes;
    ingress::ReserveStateV1SlotWire debited_wire{};
    test->ExpectError(
        ingress::EncodeReserveStateSlotV1(
            header, debited, &debited_wire),
        ingress::ReserveStateV1Error::kNone,
        "encode sole-DEBITED action");
    test->ExpectError(
        ingress::ValidateReserveStateSlotPairV1(
            header,
            active,
            active_wire,
            debited,
            debited_wire),
        ingress::ReserveStateV1Error::kNone,
        "debit generation equals durable transition generation");

    auto two_debited = debited;
    two_debited.entries[0U].actions[1U].action_state =
        ingress::FinalizationActionStateV1::kDebited;
    two_debited.entries[0U].actions[1U].debit_generation =
        22U;
    two_debited.entries[0U].activation_remaining_cap = 0U;
    test->ExpectError(
        ingress::EncodeReserveStateSlotV1(
            header, two_debited, &active_wire),
        ingress::ReserveStateV1Error::kInvalidReceipt,
        "slot rejects a second DEBITED action");

    auto completed = debited;
    completed.generation = 23U;
    completed.entries[0U].actions[0U].action_state =
        ingress::FinalizationActionStateV1::kComplete;
    ingress::ReserveStateV1SlotWire completed_wire{};
    test->ExpectError(
        ingress::EncodeReserveStateSlotV1(
            header, completed, &completed_wire),
        ingress::ReserveStateV1Error::kNone,
        "DEBITED to COMPLETE keeps original debit generation");
    test->ExpectError(
        ingress::ValidateReserveStateSlotPairV1(
            header,
            debited,
            debited_wire,
            completed,
            completed_wire),
        ingress::ReserveStateV1Error::kNone,
        "DEBITED to COMPLETE transition accepted");

    auto report_debited = completed;
    report_debited.generation = 24U;
    report_debited.entries[0U].actions[1U].action_state =
        ingress::FinalizationActionStateV1::kDebited;
    report_debited.entries[0U].actions[1U]
        .debit_generation = 24U;
    report_debited.entries[0U].activation_remaining_cap = 0U;
    ingress::ReserveStateV1SlotWire report_debited_wire{};
    test->ExpectError(
        ingress::EncodeReserveStateSlotV1(
            header, report_debited, &report_debited_wire),
        ingress::ReserveStateV1Error::kNone,
        "report action debit accepted after predecessor complete");

    auto done = report_debited;
    done.generation = 25U;
    done.entries[0U].actions[1U].action_state =
        ingress::FinalizationActionStateV1::kComplete;
    done.entries[0U].grant_status =
        ingress::ReserveGrantStatusV1::kDone;
    done.entries[0U].maintenance_report_sha256 =
        Pattern<32U>(0x11U);
    done.active_entry_index =
        ingress::kReserveStateV1NoActiveEntry;
    done.active_fs_free_inode_baseline = 0U;
    done.active_quota_free_inode_baseline = 0U;
    done.completed_bitmap = 1U;
    ingress::ReserveStateV1SlotWire done_wire{};
    test->ExpectError(
        ingress::EncodeReserveStateSlotV1(
            header, done, &done_wire),
        ingress::ReserveStateV1Error::kNone,
        "DONE requires complete report receipt and report hash");
    test->ExpectError(
        ingress::ValidateReserveStateSlotPairV1(
            header,
            report_debited,
            report_debited_wire,
            done,
            done_wire),
        ingress::ReserveStateV1Error::kNone,
        "report receipt COMPLETE and grant DONE are atomic");

    auto failed = debited;
    failed.generation = 23U;
    failed.entries[0U].actions[0U].action_state =
        ingress::FinalizationActionStateV1::kFailed;
    failed.entries[0U].grant_status =
        ingress::ReserveGrantStatusV1::kFailed;
    failed.active_entry_index =
        ingress::kReserveStateV1NoActiveEntry;
    failed.active_fs_free_inode_baseline = 0U;
    failed.active_quota_free_inode_baseline = 0U;
    ingress::ReserveStateV1SlotWire failed_wire{};
    test->ExpectError(
        ingress::EncodeReserveStateSlotV1(
            header, failed, &failed_wire),
        ingress::ReserveStateV1Error::kNone,
        "FAILED preserves precharge and pending remainder");
    test->ExpectError(
        ingress::ValidateReserveStateSlotPairV1(
            header,
            debited,
            debited_wire,
            failed,
            failed_wire),
        ingress::ReserveStateV1Error::kNone,
        "DEBITED receipt and grant fail atomically");
}

void TestImmutableGrantHashDomain(TestContext* test) {
    const auto header = MakeHeader();
    const auto prepared = MakePreparedSlot(header, 20U);
    ingress::ReserveStateV1Digest digest{};
    test->ExpectError(
        ingress::ComputeImmutableFinalizationGrantSha256V1(
            header, prepared, 0U, &digest),
        ingress::ReserveStateV1Error::kNone,
        "immutable grant digest accepts a valid PREPARED entry");
    test->Expect(
        common::Sha256Hex(digest) ==
            "c466140a8f85a017234a8c0bf22b9c7f"
            "579e6be1389f25143cb7286fc5d822c4",
        "immutable grant digest matches frozen binary-domain golden");

    auto active = MakeConsumedPendingSlot(header, 21U);
    active.entries[0U].grant_status =
        ingress::ReserveGrantStatusV1::kActive;
    active.entries[0U].executor_or_recovery_attempt =
        Pattern<16U>(0x41U);
    active.entries[0U].activation_fs_free_baseline =
        700000U;
    active.entries[0U].activation_quota_free_baseline =
        600000U;
    active.entries[0U].activation_remaining_cap =
        active.entries[0U].grant_bytes;
    active.entries[0U].precharged_bytes =
        active.entries[0U].grant_bytes;
    active.active_entry_index = 0U;
    active.active_fs_free_inode_baseline = 300U;
    active.active_quota_free_inode_baseline = 250U;

    ingress::ReserveStateV1Digest active_digest{};
    test->ExpectError(
        ingress::ComputeImmutableFinalizationGrantSha256V1(
            header, active, 0U, &active_digest),
        ingress::ReserveStateV1Error::kNone,
        "immutable grant digest accepts a valid ACTIVE grant");
    test->Expect(
        active_digest == digest,
        "executor, activation baselines, precharge and mutable status are excluded");

    auto debited = active;
    debited.generation = 22U;
    debited.entries[0U].actions[0U].action_state =
        ingress::FinalizationActionStateV1::kDebited;
    debited.entries[0U].actions[0U].debit_generation =
        22U;
    debited.entries[0U].activation_remaining_cap =
        3U * header.allocation_quantum_bytes;
    ingress::ReserveStateV1Digest debited_digest{};
    test->ExpectError(
        ingress::ComputeImmutableFinalizationGrantSha256V1(
            header, debited, 0U, &debited_digest),
        ingress::ReserveStateV1Error::kNone,
        "immutable grant digest accepts a valid DEBITED receipt");
    test->Expect(
        debited_digest == digest,
        "receipt state, debit generation and remaining cap are excluded");

    auto changed_plan = prepared;
    ++changed_plan.entries[0U]
          .plans[0U]
          .range_end_or_size;
    ingress::ReserveStateV1Digest changed_digest{};
    test->ExpectError(
        ingress::ComputeImmutableFinalizationGrantSha256V1(
            header, changed_plan, 0U, &changed_digest),
        ingress::ReserveStateV1Error::kNone,
        "a different valid immutable action plan still hashes");
    test->Expect(
        changed_digest != digest,
        "exact 40-byte action plan is in the immutable grant domain");

    ingress::ReserveStateV1Digest sentinel =
        Pattern<32U>(0x55U);
    const auto unchanged = sentinel;
    test->ExpectError(
        ingress::ComputeImmutableFinalizationGrantSha256V1(
            header, prepared, 1U, &sentinel),
        ingress::ReserveStateV1Error::kInvalidEntry,
        "out-of-range grant selection is rejected");
    test->Expect(
        sentinel == unchanged,
        "immutable grant digest output is atomic on failure");
}

}  // namespace

int main() {
    TestContext test;
    TestSchemaHash(&test);
    TestHeaderCodec(&test);
    TestBootstrapAndGolden(&test);
    TestPreparedConsumedCodec(&test);
    TestGenerationAndCorruptionPolicy(&test);
    TestOrderingFlagsAndArithmetic(&test);
    TestConsumedFsm(&test);
    TestImmutableGrantHashDomain(&test);
    if (test.failures != 0) {
        std::cerr << test.failures << " test(s) failed\n";
        return 1;
    }
    std::cout << "reserve state V1 tests passed\n";
    return 0;
}
