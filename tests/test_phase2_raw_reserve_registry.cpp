#include "l2flow/ingress/raw_reserve_registry.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string_view>

namespace {

namespace ingress = l2flow::ingress;

int failures = 0;

void Expect(bool condition, std::string_view message) {
    if (!condition) {
        ++failures;
        std::cerr << "FAIL: " << message << '\n';
    }
}

void ExpectError(
    ingress::ReserveStateV1Error actual,
    ingress::ReserveStateV1Error expected,
    std::string_view message) {
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
    header.quota_identity_sha256 = Pattern<32U>(0x20U);
    header.mount_identity_sha256 = Pattern<32U>(0x40U);
    header.device_id = 9U;
    header.declared_releasable_bytes = 1ULL << 30U;
    header.allocation_quantum_bytes = 4096U;
    header.declared_inode_reserve_count = 1024U;
    header.byte_probe_version = 1U;
    header.inode_probe_version = 1U;
    header.inode_inventory_sha256 = Pattern<32U>(0x60U);
    header.safe_stop_catalog_sha256 = Pattern<32U>(0x80U);
    return header;
}

ingress::RawReserveRegistryEntryKeyV1 MakeKey(
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    std::uint8_t identity_start) {
    ingress::RawReserveRegistryEntryKeyV1 key{};
    key.route.source_stream_id = source_stream_id;
    key.route.capture_date = capture_date;
    key.stream_day_id =
        Pattern<16U>(identity_start);
    key.recovery_attempt_id =
        Pattern<16U>(
            static_cast<std::uint8_t>(
                identity_start + 0x20U));
    return key;
}

ingress::ReserveStateEntryV1 MakeActiveEntry(
    const ingress::RawReserveRegistryEntryKeyV1& key,
    std::uint8_t writer_start = 0xa0U) {
    ingress::ReserveStateEntryV1 entry{};
    entry.source_stream_id = key.route.source_stream_id;
    entry.capture_date = key.route.capture_date;
    entry.stream_day_id = key.stream_day_id;
    entry.registry_status =
        ingress::ReserveRegistryStatusV1::kActive;
    entry.recovery_origin =
        ingress::ReserveRecoveryOriginV1::kFreshInit;
    entry.recovery_intent =
        ingress::ReserveRecoveryIntentV1::kResumeConnect;
    entry.writer_instance =
        Pattern<16U>(writer_start);
    entry.executor_or_recovery_attempt =
        key.recovery_attempt_id;
    entry.safe_stop_template_id = 17U;
    return entry;
}

ingress::ReserveStateSlotV1 MakeEmptySlot(
    const ingress::ReserveCoordinatorHeaderV1& header,
    std::uint64_t generation) {
    ingress::ReserveStateSlotV1 slot{};
    slot.generation = generation;
    slot.reserve_state_uuid = header.reserve_state_uuid;
    return slot;
}

void ExpectValidPair(
    const ingress::ReserveCoordinatorHeaderV1& header,
    const ingress::ReserveStateSlotV1& before,
    const ingress::ReserveStateSlotV1& after,
    std::string_view message) {
    ingress::ReserveStateV1SlotWire before_wire{};
    ingress::ReserveStateV1SlotWire after_wire{};
    const auto before_error =
        ingress::EncodeReserveStateSlotV1(
            header, before, &before_wire);
    const auto after_error =
        ingress::EncodeReserveStateSlotV1(
            header, after, &after_wire);
    const auto pair_error =
        before_error == ingress::ReserveStateV1Error::kNone &&
                after_error ==
                    ingress::ReserveStateV1Error::kNone
            ? ingress::ValidateReserveStateSlotPairV1(
                  header,
                  before,
                  before_wire,
                  after,
                  after_wire)
            : ingress::ReserveStateV1Error::kInvalidState;
    if (pair_error != ingress::ReserveStateV1Error::kNone) {
        ++failures;
        std::cerr
            << "FAIL: " << message << " ("
            << ingress::ReserveStateV1ErrorName(pair_error)
            << ")\n";
    }
}

ingress::ReserveStateSlotV1 Sentinel(
    const ingress::ReserveCoordinatorHeaderV1& header) {
    auto sentinel = MakeEmptySlot(header, 777U);
    sentinel.entry_count = 1U;
    const auto key = MakeKey(999U, 20260101U, 0x11U);
    sentinel.entries[0U] = MakeActiveEntry(key);
    return sentinel;
}

void TestFreshLifecycle() {
    const auto header = MakeHeader();
    const auto key = MakeKey(20U, 20260719U, 0x11U);
    const auto writer_one = Pattern<16U>(0x51U);
    const auto writer_two = Pattern<16U>(0x61U);
    const auto writer_three = Pattern<16U>(0x71U);

    auto before = MakeEmptySlot(header, 5U);
    ingress::RawReserveFreshScaffoldingV1 fresh{};
    fresh.key = key;
    fresh.writer_instance = writer_one;
    fresh.recovery_intent =
        ingress::ReserveRecoveryIntentV1::kResumeConnect;
    fresh.scaffolding_allocation_cap = 8192U;
    fresh.safe_stop_template_id = 33U;
    ingress::ReserveStateSlotV1 scaffolding{};
    ExpectError(
        ingress::BuildRawReserveRegisterFreshScaffoldingV1(
            header, before, fresh, &scaffolding),
        ingress::ReserveStateV1Error::kNone,
        "register fresh scaffolding");
    ExpectValidPair(
        header, before, scaffolding, "fresh pair valid");
    Expect(
        scaffolding.generation == 6U &&
            scaffolding.entry_count == 1U &&
            scaffolding.entries[0U].registry_status ==
                ingress::ReserveRegistryStatusV1::
                    kScaffolding &&
            scaffolding.entries[0U].recovery_origin ==
                ingress::ReserveRecoveryOriginV1::kFreshInit &&
            scaffolding.entries[0U].grant_bytes == 8192U,
        "fresh entry carries exact permit facts");

    ingress::RawReserveScaffoldingTakeoverV1 scaffold_takeover{};
    scaffold_takeover.key = key;
    scaffold_takeover.new_writer_instance = writer_two;
    scaffold_takeover.new_scaffolding_allocation_cap =
        12288U;
    ingress::ReserveStateSlotV1 taken{};
    ExpectError(
        ingress::BuildRawReserveTakeoverScaffoldingV1(
            header,
            scaffolding,
            scaffold_takeover,
            &taken),
        ingress::ReserveStateV1Error::kNone,
        "take over scaffolding");
    ExpectValidPair(
        header, scaffolding, taken, "scaffolding takeover pair");
    Expect(
        taken.entries[0U].writer_instance == writer_two &&
            taken.entries[0U].grant_bytes == 12288U &&
            taken.entries[0U]
                    .executor_or_recovery_attempt ==
                key.recovery_attempt_id &&
            taken.entries[0U].stream_day_id ==
                key.stream_day_id,
        "scaffolding takeover changes only writer and cap");

    ingress::ReserveStateSlotV1 init{};
    ExpectError(
        ingress::BuildRawReservePublishInitV1(
            header, taken, key, &init),
        ingress::ReserveStateV1Error::kNone,
        "publish INIT");
    ExpectValidPair(header, taken, init, "INIT pair");
    Expect(
        init.entries[0U].registry_status ==
                ingress::ReserveRegistryStatusV1::kInit &&
            init.entries[0U].grant_bytes == 0U &&
            init.entries[0U].writer_instance == writer_two,
        "INIT clears only scaffolding cap");

    ingress::RawReserveWriterTakeoverV1 init_takeover{};
    init_takeover.key = key;
    init_takeover.new_writer_instance = writer_three;
    ingress::ReserveStateSlotV1 recovering{};
    ExpectError(
        ingress::BuildRawReserveTakeoverInitV1(
            header, init, init_takeover, &recovering),
        ingress::ReserveStateV1Error::kNone,
        "INIT takeover");
    ExpectValidPair(
        header, init, recovering, "INIT takeover pair");
    Expect(
        recovering.entries[0U].registry_status ==
                ingress::ReserveRegistryStatusV1::
                    kRecovering &&
            recovering.entries[0U].recovery_origin ==
                ingress::ReserveRecoveryOriginV1::
                    kFreshInitTakeover &&
            recovering.entries[0U].writer_instance ==
                writer_three &&
            recovering.entries[0U]
                    .executor_or_recovery_attempt ==
                key.recovery_attempt_id,
        "INIT takeover fixes origin and preserves attempt");

    ingress::ReserveStateSlotV1 active{};
    ExpectError(
        ingress::BuildRawReservePublishActiveV1(
            header, recovering, key, &active),
        ingress::ReserveStateV1Error::kNone,
        "RECOVERING to ACTIVE");
    ExpectValidPair(
        header, recovering, active, "ACTIVE pair");
    Expect(
        active.entries[0U].registry_status ==
            ingress::ReserveRegistryStatusV1::kActive,
        "ACTIVE published only after RESUME_CONNECT");

    ingress::RawReserveActiveTakeoverV1 active_takeover{};
    active_takeover.old_key = key;
    active_takeover.new_writer_instance =
        Pattern<16U>(0x81U);
    active_takeover.new_recovery_attempt_id =
        Pattern<16U>(0x91U);
    active_takeover.recovery_intent =
        ingress::ReserveRecoveryIntentV1::kResumeConnect;
    ingress::ReserveStateSlotV1 active_recovery{};
    ExpectError(
        ingress::BuildRawReserveTakeoverActiveV1(
            header, active, active_takeover, &active_recovery),
        ingress::ReserveStateV1Error::kNone,
        "ACTIVE takeover");
    ExpectValidPair(
        header, active, active_recovery, "ACTIVE takeover pair");
    Expect(
        active_recovery.entries[0U].recovery_origin ==
                ingress::ReserveRecoveryOriginV1::
                    kActiveTakeover &&
            active_recovery.entries[0U]
                    .executor_or_recovery_attempt ==
                active_takeover.new_recovery_attempt_id,
        "ACTIVE takeover installs a fresh attempt");

    auto active_recovery_key = key;
    active_recovery_key.recovery_attempt_id =
        active_takeover.new_recovery_attempt_id;
    ingress::RawReserveWriterTakeoverV1 recovery_takeover{};
    recovery_takeover.key = active_recovery_key;
    recovery_takeover.new_writer_instance =
        Pattern<16U>(0xa1U);
    ingress::ReserveStateSlotV1 retaken{};
    ExpectError(
        ingress::BuildRawReserveTakeoverRecoveringV1(
            header,
            active_recovery,
            recovery_takeover,
            &retaken),
        ingress::ReserveStateV1Error::kNone,
        "RECOVERING takeover");
    ExpectValidPair(
        header,
        active_recovery,
        retaken,
        "RECOVERING takeover pair");
    Expect(
        retaken.entries[0U].recovery_origin ==
                ingress::ReserveRecoveryOriginV1::
                    kActiveTakeover &&
            retaken.entries[0U].recovery_intent ==
                ingress::ReserveRecoveryIntentV1::
                    kResumeConnect &&
            retaken.entries[0U]
                    .executor_or_recovery_attempt ==
                active_takeover.new_recovery_attempt_id,
        "RECOVERING takeover preserves origin, intent and attempt");

    ingress::ReserveStateSlotV1 reactivated{};
    ExpectError(
        ingress::BuildRawReservePublishActiveV1(
            header,
            retaken,
            active_recovery_key,
            &reactivated),
        ingress::ReserveStateV1Error::kNone,
        "reactivate after takeover");
    ingress::ReserveStateSlotV1 removed{};
    ExpectError(
        ingress::BuildRawReserveUnregisterActiveV1(
            header,
            reactivated,
            active_recovery_key,
            &removed),
        ingress::ReserveStateV1Error::kNone,
        "unregister ACTIVE");
    ExpectValidPair(
        header, reactivated, removed, "unregister pair");
    Expect(
        removed.entry_count == 0U &&
            removed.entries[0U] ==
                ingress::ReserveStateEntryV1{},
        "unregister clears compacted tail");
}

void TestExistingAnchorAndDirectInitActive() {
    const auto header = MakeHeader();
    const auto existing_key =
        MakeKey(8U, 20260718U, 0x21U);
    const auto later_key =
        MakeKey(30U, 20260718U, 0x31U);
    auto before = MakeEmptySlot(header, 10U);
    before.entry_count = 1U;
    before.entries[0U] = MakeActiveEntry(later_key);

    ingress::RawReserveExistingAnchorRecoveryV1 registration{};
    registration.key = existing_key;
    registration.writer_instance = Pattern<16U>(0x41U);
    registration.recovery_intent =
        ingress::ReserveRecoveryIntentV1::kRecoverSealOnly;
    registration.safe_stop_template_id = 77U;
    ingress::ReserveStateSlotV1 recovering{};
    ExpectError(
        ingress::
            BuildRawReserveRegisterExistingAnchorRecoveringV1(
                header, before, registration, &recovering),
        ingress::ReserveStateV1Error::kNone,
        "register existing anchor as RECOVERING");
    ExpectValidPair(
        header, before, recovering, "existing anchor pair");
    Expect(
        recovering.entry_count == 2U &&
            recovering.entries[0U].source_stream_id == 8U &&
            recovering.entries[1U].source_stream_id == 30U &&
            recovering.entries[0U].recovery_origin ==
                ingress::ReserveRecoveryOriginV1::
                    kAbsentRegistryExistingAnchor,
        "existing anchor insertion preserves route ordering");

    auto unchanged = Sentinel(header);
    const auto sentinel = unchanged;
    ExpectError(
        ingress::BuildRawReservePublishActiveV1(
            header, recovering, existing_key, &unchanged),
        ingress::ReserveStateV1Error::kInvalidTransition,
        "RECOVER_SEAL_ONLY cannot become ACTIVE");
    Expect(
        unchanged == sentinel,
        "rejected seal-only ACTIVE leaves output unchanged");

    auto wrong_intent = recovering;
    wrong_intent.entries[0U].recovery_intent =
        ingress::ReserveRecoveryIntentV1::kResumeConnect;
    unchanged = sentinel;
    ExpectError(
        ingress::
            BuildRawReserveUnregisterTerminalRecoveringV1(
                header,
                wrong_intent,
                existing_key,
                &unchanged),
        ingress::ReserveStateV1Error::kInvalidTransition,
        "terminal unregister rejects RESUME_CONNECT recovery");
    Expect(
        unchanged == sentinel,
        "wrong-intent terminal unregister leaves output unchanged");

    ingress::ReserveStateSlotV1 terminal_removed{};
    ExpectError(
        ingress::
            BuildRawReserveUnregisterTerminalRecoveringV1(
                header,
                recovering,
                existing_key,
                &terminal_removed),
        ingress::ReserveStateV1Error::kNone,
        "terminal report path may unregister RECOVER_SEAL_ONLY");
    ExpectValidPair(
        header,
        recovering,
        terminal_removed,
        "terminal RECOVERING unregister pair");
    Expect(
        terminal_removed.entry_count == 1U &&
            terminal_removed.entries[0U]
                    .source_stream_id ==
                later_key.route.source_stream_id &&
            terminal_removed.entries[1U] ==
                ingress::ReserveStateEntryV1{},
        "terminal unregister compacts the route table");

    ingress::RawReserveFreshScaffoldingV1 fresh{};
    fresh.key = MakeKey(40U, 20260718U, 0x51U);
    fresh.writer_instance = Pattern<16U>(0x61U);
    fresh.recovery_intent =
        ingress::ReserveRecoveryIntentV1::kResumeConnect;
    fresh.scaffolding_allocation_cap = 4096U;
    fresh.safe_stop_template_id = 88U;
    auto empty = MakeEmptySlot(header, 20U);
    ingress::ReserveStateSlotV1 scaffolding{};
    ingress::ReserveStateSlotV1 init{};
    ingress::ReserveStateSlotV1 active{};
    ExpectError(
        ingress::BuildRawReserveRegisterFreshScaffoldingV1(
            header, empty, fresh, &scaffolding),
        ingress::ReserveStateV1Error::kNone,
        "direct INIT fixture fresh");
    ExpectError(
        ingress::BuildRawReservePublishInitV1(
            header, scaffolding, fresh.key, &init),
        ingress::ReserveStateV1Error::kNone,
        "direct INIT fixture publish");
    ExpectError(
        ingress::BuildRawReservePublishActiveV1(
            header, init, fresh.key, &active),
        ingress::ReserveStateV1Error::kNone,
        "INIT may publish ACTIVE for RESUME_CONNECT");
    ExpectValidPair(
        header, init, active, "direct INIT ACTIVE pair");
}

void TestSortedInsertionAndCompaction() {
    const auto header = MakeHeader();
    const auto first_key =
        MakeKey(10U, 20260717U, 0x11U);
    const auto middle_key =
        MakeKey(10U, 20260718U, 0x31U);
    const auto last_key =
        MakeKey(10U, 20260719U, 0x51U);
    auto before = MakeEmptySlot(header, 30U);
    before.entry_count = 2U;
    before.entries[0U] =
        MakeActiveEntry(first_key, 0x71U);
    before.entries[1U] =
        MakeActiveEntry(last_key, 0x81U);

    ingress::RawReserveFreshScaffoldingV1 fresh{};
    fresh.key = middle_key;
    fresh.writer_instance = Pattern<16U>(0x91U);
    fresh.recovery_intent =
        ingress::ReserveRecoveryIntentV1::kResumeConnect;
    fresh.scaffolding_allocation_cap = 4096U;
    fresh.safe_stop_template_id = 99U;
    ingress::ReserveStateSlotV1 inserted{};
    ExpectError(
        ingress::BuildRawReserveRegisterFreshScaffoldingV1(
            header, before, fresh, &inserted),
        ingress::ReserveStateV1Error::kNone,
        "insert route between existing routes");
    Expect(
        inserted.entries[0U].capture_date == 20260717U &&
            inserted.entries[1U].capture_date == 20260718U &&
            inserted.entries[2U].capture_date == 20260719U,
        "route table remains sorted by capture date");

    auto all_active = inserted;
    all_active.entries[1U].registry_status =
        ingress::ReserveRegistryStatusV1::kActive;
    all_active.entries[1U].grant_bytes = 0U;
    ingress::ReserveStateV1SlotWire validation_wire{};
    ExpectError(
        ingress::EncodeReserveStateSlotV1(
            header, all_active, &validation_wire),
        ingress::ReserveStateV1Error::kNone,
        "all-active compaction fixture valid");
    ingress::ReserveStateSlotV1 compacted{};
    ExpectError(
        ingress::BuildRawReserveUnregisterActiveV1(
            header, all_active, middle_key, &compacted),
        ingress::ReserveStateV1Error::kNone,
        "remove middle ACTIVE route");
    Expect(
        compacted.entry_count == 2U &&
            compacted.entries[0U].capture_date == 20260717U &&
            compacted.entries[1U].capture_date == 20260719U &&
            compacted.entries[2U] ==
                ingress::ReserveStateEntryV1{},
        "unregister compacts and zeroes tail");
}

void TestNegativeAndMaliciousInputs() {
    const auto header = MakeHeader();
    const auto key = MakeKey(12U, 20260719U, 0x21U);
    ingress::RawReserveFreshScaffoldingV1 fresh{};
    fresh.key = key;
    fresh.writer_instance = Pattern<16U>(0x51U);
    fresh.recovery_intent =
        ingress::ReserveRecoveryIntentV1::kResumeConnect;
    fresh.scaffolding_allocation_cap = 4096U;
    fresh.safe_stop_template_id = 55U;
    const auto empty = MakeEmptySlot(header, 50U);

    ExpectError(
        ingress::BuildRawReserveRegisterFreshScaffoldingV1(
            header, empty, fresh, nullptr),
        ingress::ReserveStateV1Error::kNullOutput,
        "null output rejected");

    auto output = Sentinel(header);
    const auto sentinel = output;
    auto overflow = empty;
    overflow.generation =
        std::numeric_limits<std::uint64_t>::max();
    ExpectError(
        ingress::BuildRawReserveRegisterFreshScaffoldingV1(
            header, overflow, fresh, &output),
        ingress::ReserveStateV1Error::kInvalidGeneration,
        "generation overflow rejected");
    Expect(
        output == sentinel,
        "generation overflow leaves output unchanged");

    auto intent = empty;
    intent.coordinator_state =
        ingress::ReserveCoordinatorPhaseV1::
            kReleasingIntent;
    intent.reason =
        ingress::ReserveReleaseReasonV1::kLowWatermark;
    intent.trigger =
        ingress::ReserveReleaseTriggerV1::kFilesystemBytes;
    intent.writer_set_sha256 = Pattern<32U>(0x91U);
    ExpectError(
        ingress::BuildRawReserveRegisterFreshScaffoldingV1(
            header, intent, fresh, &output),
        ingress::ReserveStateV1Error::kInvalidState,
        "non-PROVISIONED state rejected");
    Expect(
        output == sentinel,
        "phase rejection leaves output unchanged");

    auto bad_fresh = fresh;
    bad_fresh.scaffolding_allocation_cap = 0U;
    ExpectError(
        ingress::BuildRawReserveRegisterFreshScaffoldingV1(
            header, empty, bad_fresh, &output),
        ingress::ReserveStateV1Error::kInvalidEntry,
        "zero scaffolding cap rejected");
    Expect(output == sentinel, "bad cap leaves output unchanged");

    bad_fresh = fresh;
    bad_fresh.key.route.source_stream_id = 0U;
    ExpectError(
        ingress::BuildRawReserveRegisterFreshScaffoldingV1(
            header, empty, bad_fresh, &output),
        ingress::ReserveStateV1Error::kInvalidEntry,
        "zero route rejected");
    bad_fresh = fresh;
    bad_fresh.key.stream_day_id = {};
    ExpectError(
        ingress::BuildRawReserveRegisterFreshScaffoldingV1(
            header, empty, bad_fresh, &output),
        ingress::ReserveStateV1Error::kInvalidEntry,
        "zero planned stream-day rejected");
    bad_fresh = fresh;
    bad_fresh.key.recovery_attempt_id = {};
    ExpectError(
        ingress::BuildRawReserveRegisterFreshScaffoldingV1(
            header, empty, bad_fresh, &output),
        ingress::ReserveStateV1Error::kInvalidEntry,
        "zero recovery attempt rejected");
    bad_fresh = fresh;
    bad_fresh.writer_instance = {};
    ExpectError(
        ingress::BuildRawReserveRegisterFreshScaffoldingV1(
            header, empty, bad_fresh, &output),
        ingress::ReserveStateV1Error::kInvalidEntry,
        "zero writer identity rejected");
    bad_fresh = fresh;
    bad_fresh.safe_stop_template_id = 0U;
    ExpectError(
        ingress::BuildRawReserveRegisterFreshScaffoldingV1(
            header, empty, bad_fresh, &output),
        ingress::ReserveStateV1Error::kInvalidEntry,
        "zero safe-stop template rejected");
    Expect(
        output == sentinel,
        "invalid registration identities leave output unchanged");

    bad_fresh = fresh;
    bad_fresh.recovery_intent =
        static_cast<ingress::ReserveRecoveryIntentV1>(
            0xffU);
    ExpectError(
        ingress::BuildRawReserveRegisterFreshScaffoldingV1(
            header, empty, bad_fresh, &output),
        ingress::ReserveStateV1Error::kUnknownEnum,
        "unknown intent rejected");
    Expect(
        output == sentinel,
        "unknown intent leaves output unchanged");

    ingress::ReserveStateSlotV1 scaffolding{};
    ExpectError(
        ingress::BuildRawReserveRegisterFreshScaffoldingV1(
            header, empty, fresh, &scaffolding),
        ingress::ReserveStateV1Error::kNone,
        "negative fixture fresh");
    ExpectError(
        ingress::BuildRawReserveRegisterFreshScaffoldingV1(
            header, scaffolding, fresh, &output),
        ingress::ReserveStateV1Error::kDuplicateRoute,
        "duplicate route rejected");
    Expect(
        output == sentinel,
        "duplicate route leaves output unchanged");

    auto stale_key = key;
    stale_key.stream_day_id = Pattern<16U>(0xd1U);
    ExpectError(
        ingress::BuildRawReservePublishInitV1(
            header, scaffolding, stale_key, &output),
        ingress::ReserveStateV1Error::kImmutableFactChanged,
        "stale stream-day rejected");
    stale_key = key;
    stale_key.recovery_attempt_id =
        Pattern<16U>(0xe1U);
    ExpectError(
        ingress::BuildRawReservePublishInitV1(
            header, scaffolding, stale_key, &output),
        ingress::ReserveStateV1Error::kImmutableFactChanged,
        "stale attempt rejected");
    Expect(
        output == sentinel,
        "stale identity leaves output unchanged");

    ingress::RawReserveScaffoldingTakeoverV1 same_writer{};
    same_writer.key = key;
    same_writer.new_writer_instance =
        fresh.writer_instance;
    same_writer.new_scaffolding_allocation_cap = 8192U;
    ExpectError(
        ingress::BuildRawReserveTakeoverScaffoldingV1(
            header, scaffolding, same_writer, &output),
        ingress::ReserveStateV1Error::kInvalidTransition,
        "takeover requires a new writer identity");

    ingress::ReserveStateSlotV1 init{};
    ExpectError(
        ingress::BuildRawReservePublishInitV1(
            header, scaffolding, key, &init),
        ingress::ReserveStateV1Error::kNone,
        "negative fixture INIT");
    ExpectError(
        ingress::BuildRawReservePublishInitV1(
            header, init, key, &output),
        ingress::ReserveStateV1Error::kInvalidTransition,
        "INIT cannot be published twice");

    ingress::RawReserveWriterTakeoverV1 wrong_status{};
    wrong_status.key = key;
    wrong_status.new_writer_instance =
        Pattern<16U>(0x71U);
    ExpectError(
        ingress::BuildRawReserveTakeoverRecoveringV1(
            header, init, wrong_status, &output),
        ingress::ReserveStateV1Error::kInvalidTransition,
        "RECOVERING takeover rejects INIT");
    Expect(
        output == sentinel,
        "status mismatch leaves output unchanged");

    ingress::ReserveStateSlotV1 active{};
    ExpectError(
        ingress::BuildRawReservePublishActiveV1(
            header, init, key, &active),
        ingress::ReserveStateV1Error::kNone,
        "negative fixture ACTIVE");
    ingress::RawReserveActiveTakeoverV1 active_takeover{};
    active_takeover.old_key = key;
    active_takeover.new_writer_instance =
        Pattern<16U>(0x81U);
    active_takeover.new_recovery_attempt_id =
        key.recovery_attempt_id;
    active_takeover.recovery_intent =
        ingress::ReserveRecoveryIntentV1::kResumeConnect;
    ExpectError(
        ingress::BuildRawReserveTakeoverActiveV1(
            header, active, active_takeover, &output),
        ingress::ReserveStateV1Error::kInvalidTransition,
        "ACTIVE takeover requires new attempt");
    active_takeover.new_recovery_attempt_id =
        Pattern<16U>(0x91U);
    active_takeover.recovery_intent =
        static_cast<ingress::ReserveRecoveryIntentV1>(
            0xfeU);
    ExpectError(
        ingress::BuildRawReserveTakeoverActiveV1(
            header, active, active_takeover, &output),
        ingress::ReserveStateV1Error::kUnknownEnum,
        "ACTIVE takeover rejects malicious intent");
    Expect(
        output == sentinel,
        "malicious takeover leaves output unchanged");

    auto invalid_before = active;
    invalid_before.entries[0U].writer_instance = {};
    ExpectError(
        ingress::BuildRawReserveUnregisterActiveV1(
            header, invalid_before, key, &output),
        ingress::ReserveStateV1Error::kInvalidEntry,
        "invalid before slot never reaches builder mutation");
    Expect(
        output == sentinel,
        "invalid before slot leaves output unchanged");

    auto full = MakeEmptySlot(header, 70U);
    full.entry_count =
        static_cast<std::uint16_t>(
            ingress::kReserveStateV1EntryCapacity);
    for (std::size_t index = 0U;
         index < ingress::kReserveStateV1EntryCapacity;
         ++index) {
        const auto full_key = MakeKey(
            static_cast<std::uint32_t>(index + 1U),
            20260719U,
            static_cast<std::uint8_t>(index + 1U));
        full.entries[index] = MakeActiveEntry(
            full_key,
            static_cast<std::uint8_t>(0x20U + index));
    }
    auto extra = fresh;
    extra.key = MakeKey(100U, 20260719U, 0xc1U);
    ExpectError(
        ingress::BuildRawReserveRegisterFreshScaffoldingV1(
            header, full, extra, &output),
        ingress::ReserveStateV1Error::kInvalidCount,
        "capacity sixteen is fail closed");
    Expect(
        output == sentinel,
        "capacity rejection leaves output unchanged");

    auto absent = key;
    absent.route.source_stream_id = 999U;
    ExpectError(
        ingress::BuildRawReserveUnregisterActiveV1(
            header, active, absent, &output),
        ingress::ReserveStateV1Error::kInvalidEntry,
        "unregister requires an existing exact route");
    Expect(
        output == sentinel,
        "absent unregister leaves output unchanged");
}

}  // namespace

int main() {
    TestFreshLifecycle();
    TestExistingAnchorAndDirectInitActive();
    TestSortedInsertionAndCompaction();
    TestNegativeAndMaliciousInputs();
    if (failures != 0) {
        std::cerr << failures << " test(s) failed\n";
        return 1;
    }
    std::cout << "raw reserve registry tests passed\n";
    return 0;
}
