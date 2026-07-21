#include "l2flow/ingress/raw_reserve_registry.h"

#include <cstddef>
#include <cstdint>
#include <limits>

namespace l2flow::ingress {
namespace {

struct CandidateContextV1 final {
    ReserveStateV1SlotWire before_wire{};
    ReserveStateSlotV1 candidate{};
};

[[nodiscard]] int CompareRoute(
    const ReserveStateEntryV1& entry,
    const RawReserveRegistryRouteV1& route) noexcept {
    if (entry.source_stream_id < route.source_stream_id) {
        return -1;
    }
    if (entry.source_stream_id > route.source_stream_id) {
        return 1;
    }
    if (entry.capture_date < route.capture_date) {
        return -1;
    }
    if (entry.capture_date > route.capture_date) {
        return 1;
    }
    return 0;
}

[[nodiscard]] ReserveStateV1Error PrepareCandidate(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    CandidateContextV1* context) noexcept {
    const ReserveStateV1Error before_error =
        EncodeReserveStateSlotV1(
            header, before, &context->before_wire);
    if (before_error != ReserveStateV1Error::kNone) {
        return before_error;
    }
    if (before.coordinator_state !=
        ReserveCoordinatorPhaseV1::kProvisioned) {
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
    const ReserveStateV1Error encode_error =
        EncodeReserveStateSlotV1(
            header, context.candidate, &after_wire);
    if (encode_error != ReserveStateV1Error::kNone) {
        return encode_error;
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

[[nodiscard]] bool FindRoute(
    const ReserveStateSlotV1& slot,
    const RawReserveRegistryRouteV1& route,
    std::size_t* index,
    std::size_t* insertion_index) noexcept {
    std::size_t position = 0U;
    while (position < slot.entry_count) {
        const int comparison =
            CompareRoute(slot.entries[position], route);
        if (comparison == 0) {
            if (index != nullptr) {
                *index = position;
            }
            if (insertion_index != nullptr) {
                *insertion_index = position;
            }
            return true;
        }
        if (comparison > 0) {
            break;
        }
        ++position;
    }
    if (insertion_index != nullptr) {
        *insertion_index = position;
    }
    return false;
}

[[nodiscard]] ReserveStateV1Error FindExactEntry(
    const ReserveStateSlotV1& slot,
    const RawReserveRegistryEntryKeyV1& key,
    ReserveRegistryStatusV1 required_status,
    std::size_t* index) noexcept {
    std::size_t found_index = 0U;
    if (!FindRoute(
            slot, key.route, &found_index, nullptr)) {
        return ReserveStateV1Error::kInvalidEntry;
    }
    const auto& entry = slot.entries[found_index];
    if (entry.stream_day_id != key.stream_day_id ||
        entry.executor_or_recovery_attempt !=
            key.recovery_attempt_id) {
        return ReserveStateV1Error::kImmutableFactChanged;
    }
    if (entry.registry_status != required_status) {
        return ReserveStateV1Error::kInvalidTransition;
    }
    *index = found_index;
    return ReserveStateV1Error::kNone;
}

[[nodiscard]] ReserveStateV1Error FindExactInitOrRecovering(
    const ReserveStateSlotV1& slot,
    const RawReserveRegistryEntryKeyV1& key,
    std::size_t* index) noexcept {
    std::size_t found_index = 0U;
    if (!FindRoute(
            slot, key.route, &found_index, nullptr)) {
        return ReserveStateV1Error::kInvalidEntry;
    }
    const auto& entry = slot.entries[found_index];
    if (entry.stream_day_id != key.stream_day_id ||
        entry.executor_or_recovery_attempt !=
            key.recovery_attempt_id) {
        return ReserveStateV1Error::kImmutableFactChanged;
    }
    if (entry.registry_status !=
            ReserveRegistryStatusV1::kInit &&
        entry.registry_status !=
            ReserveRegistryStatusV1::kRecovering) {
        return ReserveStateV1Error::kInvalidTransition;
    }
    *index = found_index;
    return ReserveStateV1Error::kNone;
}

[[nodiscard]] ReserveStateV1Error InsertRegistration(
    CandidateContextV1* context,
    const RawReserveRegistryRouteV1& route,
    const ReserveStateEntryV1& entry) noexcept {
    std::size_t insertion_index = 0U;
    if (FindRoute(
            context->candidate,
            route,
            nullptr,
            &insertion_index)) {
        return ReserveStateV1Error::kDuplicateRoute;
    }
    if (context->candidate.entry_count >=
        kReserveStateV1EntryCapacity) {
        return ReserveStateV1Error::kInvalidCount;
    }
    const std::size_t old_count =
        context->candidate.entry_count;
    for (std::size_t index = old_count;
         index > insertion_index;
         --index) {
        context->candidate.entries[index] =
            context->candidate.entries[index - 1U];
    }
    context->candidate.entries[insertion_index] = entry;
    context->candidate.entry_count =
        static_cast<std::uint16_t>(old_count + 1U);
    return ReserveStateV1Error::kNone;
}

[[nodiscard]] ReserveStateEntryV1 MakeRegistrationEntry(
    const RawReserveRegistryEntryKeyV1& key,
    ReserveStateV1Identity writer_instance,
    ReserveRegistryStatusV1 status,
    ReserveRecoveryOriginV1 origin,
    ReserveRecoveryIntentV1 intent,
    std::uint64_t scaffolding_allocation_cap,
    std::uint64_t safe_stop_template_id) noexcept {
    ReserveStateEntryV1 entry{};
    entry.source_stream_id = key.route.source_stream_id;
    entry.capture_date = key.route.capture_date;
    entry.stream_day_id = key.stream_day_id;
    entry.registry_status = status;
    entry.recovery_origin = origin;
    entry.recovery_intent = intent;
    entry.writer_instance = writer_instance;
    entry.executor_or_recovery_attempt =
        key.recovery_attempt_id;
    entry.grant_bytes = scaffolding_allocation_cap;
    entry.safe_stop_template_id = safe_stop_template_id;
    return entry;
}

}  // namespace

ReserveStateV1Error
BuildRawReserveRegisterFreshScaffoldingV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveFreshScaffoldingV1& registration,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    ReserveStateV1Error error =
        PrepareCandidate(header, before, &context);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    const ReserveStateEntryV1 entry =
        MakeRegistrationEntry(
            registration.key,
            registration.writer_instance,
            ReserveRegistryStatusV1::kScaffolding,
            ReserveRecoveryOriginV1::kFreshInit,
            registration.recovery_intent,
            registration.scaffolding_allocation_cap,
            registration.safe_stop_template_id);
    error = InsertRegistration(
        &context, registration.key.route, entry);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    return CommitCandidate(
        header, before, context, output);
}

ReserveStateV1Error
BuildRawReservePublishInitV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveRegistryEntryKeyV1& key,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    ReserveStateV1Error error =
        PrepareCandidate(header, before, &context);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    std::size_t index = 0U;
    error = FindExactEntry(
        context.candidate,
        key,
        ReserveRegistryStatusV1::kScaffolding,
        &index);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    auto& entry = context.candidate.entries[index];
    entry.registry_status = ReserveRegistryStatusV1::kInit;
    entry.grant_bytes = 0U;
    return CommitCandidate(
        header, before, context, output);
}

ReserveStateV1Error
BuildRawReserveRegisterExistingAnchorRecoveringV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveExistingAnchorRecoveryV1& registration,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    ReserveStateV1Error error =
        PrepareCandidate(header, before, &context);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    const ReserveStateEntryV1 entry =
        MakeRegistrationEntry(
            registration.key,
            registration.writer_instance,
            ReserveRegistryStatusV1::kRecovering,
            ReserveRecoveryOriginV1::
                kAbsentRegistryExistingAnchor,
            registration.recovery_intent,
            0U,
            registration.safe_stop_template_id);
    error = InsertRegistration(
        &context, registration.key.route, entry);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    return CommitCandidate(
        header, before, context, output);
}

ReserveStateV1Error
BuildRawReserveTakeoverScaffoldingV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveScaffoldingTakeoverV1& takeover,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    ReserveStateV1Error error =
        PrepareCandidate(header, before, &context);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    std::size_t index = 0U;
    error = FindExactEntry(
        context.candidate,
        takeover.key,
        ReserveRegistryStatusV1::kScaffolding,
        &index);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    auto& entry = context.candidate.entries[index];
    if (takeover.new_writer_instance ==
        entry.writer_instance) {
        return ReserveStateV1Error::kInvalidTransition;
    }
    entry.writer_instance = takeover.new_writer_instance;
    entry.grant_bytes =
        takeover.new_scaffolding_allocation_cap;
    return CommitCandidate(
        header, before, context, output);
}

ReserveStateV1Error
BuildRawReserveTakeoverInitV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveWriterTakeoverV1& takeover,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    ReserveStateV1Error error =
        PrepareCandidate(header, before, &context);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    std::size_t index = 0U;
    error = FindExactEntry(
        context.candidate,
        takeover.key,
        ReserveRegistryStatusV1::kInit,
        &index);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    auto& entry = context.candidate.entries[index];
    if (takeover.new_writer_instance ==
        entry.writer_instance) {
        return ReserveStateV1Error::kInvalidTransition;
    }
    entry.registry_status =
        ReserveRegistryStatusV1::kRecovering;
    entry.recovery_origin =
        ReserveRecoveryOriginV1::kFreshInitTakeover;
    entry.writer_instance = takeover.new_writer_instance;
    return CommitCandidate(
        header, before, context, output);
}

ReserveStateV1Error
BuildRawReserveTakeoverRecoveringV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveWriterTakeoverV1& takeover,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    ReserveStateV1Error error =
        PrepareCandidate(header, before, &context);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    std::size_t index = 0U;
    error = FindExactEntry(
        context.candidate,
        takeover.key,
        ReserveRegistryStatusV1::kRecovering,
        &index);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    auto& entry = context.candidate.entries[index];
    if (takeover.new_writer_instance ==
        entry.writer_instance) {
        return ReserveStateV1Error::kInvalidTransition;
    }
    entry.writer_instance = takeover.new_writer_instance;
    return CommitCandidate(
        header, before, context, output);
}

ReserveStateV1Error
BuildRawReserveTakeoverActiveV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveActiveTakeoverV1& takeover,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    ReserveStateV1Error error =
        PrepareCandidate(header, before, &context);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    std::size_t index = 0U;
    error = FindExactEntry(
        context.candidate,
        takeover.old_key,
        ReserveRegistryStatusV1::kActive,
        &index);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    auto& entry = context.candidate.entries[index];
    if (takeover.new_writer_instance ==
            entry.writer_instance ||
        takeover.new_recovery_attempt_id ==
            entry.executor_or_recovery_attempt) {
        return ReserveStateV1Error::kInvalidTransition;
    }
    entry.registry_status =
        ReserveRegistryStatusV1::kRecovering;
    entry.recovery_origin =
        ReserveRecoveryOriginV1::kActiveTakeover;
    entry.recovery_intent = takeover.recovery_intent;
    entry.writer_instance = takeover.new_writer_instance;
    entry.executor_or_recovery_attempt =
        takeover.new_recovery_attempt_id;
    return CommitCandidate(
        header, before, context, output);
}

ReserveStateV1Error
BuildRawReservePublishActiveV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveRegistryEntryKeyV1& key,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    ReserveStateV1Error error =
        PrepareCandidate(header, before, &context);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    std::size_t index = 0U;
    error = FindExactInitOrRecovering(
        context.candidate, key, &index);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    auto& entry = context.candidate.entries[index];
    if (entry.recovery_intent !=
        ReserveRecoveryIntentV1::kResumeConnect) {
        return ReserveStateV1Error::kInvalidTransition;
    }
    entry.registry_status = ReserveRegistryStatusV1::kActive;
    return CommitCandidate(
        header, before, context, output);
}

ReserveStateV1Error
BuildRawReserveUnregisterActiveV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveRegistryEntryKeyV1& key,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    ReserveStateV1Error error =
        PrepareCandidate(header, before, &context);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    std::size_t index = 0U;
    error = FindExactEntry(
        context.candidate,
        key,
        ReserveRegistryStatusV1::kActive,
        &index);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    const std::size_t old_count =
        context.candidate.entry_count;
    for (std::size_t position = index;
         position + 1U < old_count;
         ++position) {
        context.candidate.entries[position] =
            context.candidate.entries[position + 1U];
    }
    context.candidate.entries[old_count - 1U] = {};
    context.candidate.entry_count =
        static_cast<std::uint16_t>(old_count - 1U);
    return CommitCandidate(
        header, before, context, output);
}

ReserveStateV1Error
BuildRawReserveUnregisterTerminalRecoveringV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const RawReserveRegistryEntryKeyV1& key,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    CandidateContextV1 context{};
    ReserveStateV1Error error =
        PrepareCandidate(header, before, &context);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    std::size_t index = 0U;
    error = FindExactEntry(
        context.candidate,
        key,
        ReserveRegistryStatusV1::kRecovering,
        &index);
    if (error != ReserveStateV1Error::kNone) {
        return error;
    }
    if (context.candidate.entries[index]
            .recovery_intent !=
        ReserveRecoveryIntentV1::kRecoverSealOnly) {
        return ReserveStateV1Error::kInvalidTransition;
    }
    const std::size_t old_count =
        context.candidate.entry_count;
    for (std::size_t position = index;
         position + 1U < old_count;
         ++position) {
        context.candidate.entries[position] =
            context.candidate.entries[position + 1U];
    }
    context.candidate.entries[old_count - 1U] = {};
    context.candidate.entry_count =
        static_cast<std::uint16_t>(old_count - 1U);
    return CommitCandidate(
        header, before, context, output);
}

}  // namespace l2flow::ingress
