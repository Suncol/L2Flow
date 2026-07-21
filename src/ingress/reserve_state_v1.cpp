#include "l2flow/ingress/reserve_state_v1.h"

#include "l2flow/common/crc32c.h"

#include <algorithm>
#include <array>
#include <limits>
#include <span>

namespace l2flow::ingress {
namespace {

constexpr std::size_t kCrcBytes = sizeof(std::uint32_t);
constexpr std::size_t kTaggedPayloadBytes = 64U;
constexpr std::size_t kSlotZeroTailBytes = 512U;
constexpr std::size_t kImmutableGrantMaximumBytes = 2048U;

static_assert(kReserveStateV1FileBytes == 69632U);
static_assert(
    reserve_state_v1_offset::slot::kReceipts == 5632U);
static_assert(
    reserve_state_v1_offset::slot::kPlans == 22016U);
static_assert(
    reserve_state_v1_offset::slot::kZeroTail == 32256U);
static_assert(
    reserve_state_v1_offset::slot::kZeroTail +
        kSlotZeroTailBytes ==
    kReserveStateV1SlotBytes);
static_assert(
    reserve_state_v1_offset::entry::kReserved1 + 4U ==
    kReserveStateV1EntryBytes);
static_assert(
    reserve_state_v1_offset::receipt::kReserved + 4U ==
    kReserveStateV1ReceiptBytes);
static_assert(
    reserve_state_v1_offset::plan::kCausalId + 16U ==
    kReserveStateV1PlanBytes);

class ImmutableGrantEncoder final {
public:
    [[nodiscard]] bool Append(
        std::span<const std::byte> bytes) noexcept {
        if (bytes.size() > storage_.size() - size_) {
            return false;
        }
        std::copy(
            bytes.begin(),
            bytes.end(),
            storage_.begin() +
                static_cast<std::ptrdiff_t>(size_));
        size_ += bytes.size();
        return true;
    }

    [[nodiscard]] bool AppendU8(
        std::uint8_t value) noexcept {
        const std::array<std::byte, 1U> wire{
            static_cast<std::byte>(value)};
        return Append(wire);
    }

    [[nodiscard]] bool AppendU16(
        std::uint16_t value) noexcept {
        std::array<std::byte, 2U> wire{};
        wire[0U] =
            static_cast<std::byte>(value & 0xffU);
        wire[1U] =
            static_cast<std::byte>(
                (value >> 8U) & 0xffU);
        return Append(wire);
    }

    [[nodiscard]] bool AppendU32(
        std::uint32_t value) noexcept {
        std::array<std::byte, 4U> wire{};
        for (std::size_t index = 0U;
             index < wire.size();
             ++index) {
            wire[index] = static_cast<std::byte>(
                (value >>
                 static_cast<unsigned int>(
                     index * 8U)) &
                0xffU);
        }
        return Append(wire);
    }

    [[nodiscard]] bool AppendU64(
        std::uint64_t value) noexcept {
        std::array<std::byte, 8U> wire{};
        for (std::size_t index = 0U;
             index < wire.size();
             ++index) {
            wire[index] = static_cast<std::byte>(
                (value >>
                 static_cast<unsigned int>(
                     index * 8U)) &
                0xffU);
        }
        return Append(wire);
    }

    [[nodiscard]] std::span<const std::byte>
    bytes() const noexcept {
        return std::span<const std::byte>(
            storage_.data(), size_);
    }

private:
    std::array<
        std::byte,
        kImmutableGrantMaximumBytes>
        storage_{};
    std::size_t size_ = 0U;
};

void StoreU16Le(
    std::uint16_t value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    output[offset] = static_cast<std::byte>(value & 0xffU);
    output[offset + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32Le(
    std::uint32_t value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        output[offset + index] =
            static_cast<std::byte>((value >> shift) & 0xffU);
    }
}

void StoreU64Le(
    std::uint64_t value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        output[offset + index] =
            static_cast<std::byte>((value >> shift) & 0xffU);
    }
}

std::uint16_t LoadU16Le(
    std::span<const std::byte> input,
    std::size_t offset) noexcept {
    return static_cast<std::uint16_t>(
        std::to_integer<std::uint16_t>(input[offset]) |
        static_cast<std::uint16_t>(
            std::to_integer<std::uint16_t>(input[offset + 1U])
            << 8U));
}

std::uint32_t LoadU32Le(
    std::span<const std::byte> input,
    std::size_t offset) noexcept {
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

std::uint64_t LoadU64Le(
    std::span<const std::byte> input,
    std::size_t offset) noexcept {
    std::uint64_t value = 0U;
    for (std::size_t index = 0U; index < sizeof(value); ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        value |=
            std::to_integer<std::uint64_t>(input[offset + index])
            << shift;
    }
    return value;
}

template <std::size_t Size>
void StoreBytes(
    const std::array<std::byte, Size>& value,
    std::span<std::byte> output,
    std::size_t offset) noexcept {
    std::copy(value.begin(), value.end(), output.data() + offset);
}

template <std::size_t Size>
void LoadBytes(
    std::span<const std::byte> input,
    std::size_t offset,
    std::array<std::byte, Size>* output) noexcept {
    std::copy_n(input.data() + offset, Size, output->begin());
}

bool IsZero(std::span<const std::byte> bytes) noexcept {
    return std::all_of(
        bytes.begin(), bytes.end(), [](std::byte value) {
            return value == std::byte{0};
        });
}

template <std::size_t Size>
bool IsZero(
    const std::array<std::byte, Size>& bytes) noexcept {
    return IsZero(std::span<const std::byte>(bytes));
}

bool IsZeroRange(
    std::span<const std::byte> bytes,
    std::size_t offset,
    std::size_t size) noexcept {
    return IsZero(bytes.subspan(offset, size));
}

std::uint32_t ComputeCrcWithZeroedField(
    std::span<const std::byte> wire,
    std::size_t crc_offset) noexcept {
    constexpr std::array<std::byte, kCrcBytes> zero_crc{};
    l2flow::common::Crc32cState state;
    state.Update(wire.first(crc_offset));
    state.Update(zero_crc);
    state.Update(wire.subspan(crc_offset + kCrcBytes));
    return state.Finalize();
}

bool CheckedAdd(
    std::uint64_t lhs,
    std::uint64_t rhs,
    std::uint64_t* output) noexcept {
    if (rhs > std::numeric_limits<std::uint64_t>::max() - lhs) {
        return false;
    }
    *output = lhs + rhs;
    return true;
}

bool CheckedMul(
    std::uint64_t lhs,
    std::uint64_t rhs,
    std::uint64_t* output) noexcept {
    if (lhs != 0U &&
        rhs > std::numeric_limits<std::uint64_t>::max() / lhs) {
        return false;
    }
    *output = lhs * rhs;
    return true;
}

bool IsKnownCoordinatorPhase(
    ReserveCoordinatorPhaseV1 value) noexcept {
    switch (value) {
        case ReserveCoordinatorPhaseV1::kProvisioned:
        case ReserveCoordinatorPhaseV1::kReleasingIntent:
        case ReserveCoordinatorPhaseV1::kReleasingPrepared:
        case ReserveCoordinatorPhaseV1::kConsumed:
            return true;
    }
    return false;
}

bool IsKnownReason(ReserveReleaseReasonV1 value) noexcept {
    switch (value) {
        case ReserveReleaseReasonV1::kNone:
        case ReserveReleaseReasonV1::kLowWatermark:
        case ReserveReleaseReasonV1::kWriterAllocationFailure:
        case ReserveReleaseReasonV1::kOperatorRequested:
            return true;
    }
    return false;
}

bool IsKnownTrigger(ReserveReleaseTriggerV1 value) noexcept {
    switch (value) {
        case ReserveReleaseTriggerV1::kNone:
        case ReserveReleaseTriggerV1::kFilesystemBytes:
        case ReserveReleaseTriggerV1::kFilesystemInodes:
        case ReserveReleaseTriggerV1::kQuotaBytes:
        case ReserveReleaseTriggerV1::kQuotaInodes:
        case ReserveReleaseTriggerV1::kWriterEnospc:
        case ReserveReleaseTriggerV1::kWriterEdquot:
        case ReserveReleaseTriggerV1::kOperator:
            return true;
    }
    return false;
}

bool IsKnownRegistryStatus(
    ReserveRegistryStatusV1 value) noexcept {
    switch (value) {
        case ReserveRegistryStatusV1::kScaffolding:
        case ReserveRegistryStatusV1::kInit:
        case ReserveRegistryStatusV1::kRecovering:
        case ReserveRegistryStatusV1::kActive:
            return true;
        case ReserveRegistryStatusV1::kUnused:
            return false;
    }
    return false;
}

bool IsKnownRecoveryOrigin(
    ReserveRecoveryOriginV1 value) noexcept {
    switch (value) {
        case ReserveRecoveryOriginV1::kFreshInit:
        case ReserveRecoveryOriginV1::kFreshInitTakeover:
        case ReserveRecoveryOriginV1::kAbsentRegistryExistingAnchor:
        case ReserveRecoveryOriginV1::kActiveTakeover:
            return true;
        case ReserveRecoveryOriginV1::kUnused:
            return false;
    }
    return false;
}

bool IsKnownRecoveryIntent(
    ReserveRecoveryIntentV1 value) noexcept {
    switch (value) {
        case ReserveRecoveryIntentV1::kResumeConnect:
        case ReserveRecoveryIntentV1::kRecoverSealOnly:
            return true;
        case ReserveRecoveryIntentV1::kNone:
            return false;
    }
    return false;
}

bool IsKnownAckStatus(ReserveAckStatusV1 value) noexcept {
    return value == ReserveAckStatusV1::kAcked ||
           value == ReserveAckStatusV1::kFencedNoAck;
}

bool IsKnownGrantStatus(ReserveGrantStatusV1 value) noexcept {
    switch (value) {
        case ReserveGrantStatusV1::kPending:
        case ReserveGrantStatusV1::kActive:
        case ReserveGrantStatusV1::kDone:
        case ReserveGrantStatusV1::kFailed:
            return true;
        case ReserveGrantStatusV1::kUnused:
            return false;
    }
    return false;
}

bool IsKnownActionKind(
    FinalizationActionKindV1 value) noexcept {
    const auto wire = static_cast<std::uint8_t>(value);
    return wire >=
               static_cast<std::uint8_t>(
                   FinalizationActionKindV1::kCurrentSegmentDrain) &&
           wire <=
               static_cast<std::uint8_t>(
                   FinalizationActionKindV1::kFinalizationReport);
}

bool IsKnownActionState(
    FinalizationActionStateV1 value) noexcept {
    switch (value) {
        case FinalizationActionStateV1::kPending:
        case FinalizationActionStateV1::kDebited:
        case FinalizationActionStateV1::kComplete:
        case FinalizationActionStateV1::kFailed:
            return true;
        case FinalizationActionStateV1::kUnused:
            return false;
    }
    return false;
}

bool IsRawGrant(std::uint8_t flags) noexcept {
    return flags == kReserveGrantRawFinalization;
}

bool IsRegistryPhase(
    ReserveCoordinatorPhaseV1 phase) noexcept {
    return phase == ReserveCoordinatorPhaseV1::kProvisioned ||
           phase == ReserveCoordinatorPhaseV1::kReleasingIntent;
}

bool IsGrantPhase(
    ReserveCoordinatorPhaseV1 phase) noexcept {
    return phase == ReserveCoordinatorPhaseV1::kReleasingPrepared ||
           phase == ReserveCoordinatorPhaseV1::kConsumed;
}

bool IsZero(const RawFinalizationCountersV1& value) noexcept {
    return value == RawFinalizationCountersV1{};
}

bool IsZero(const ScaffoldingGrantPayloadV1& value) noexcept {
    return value == ScaffoldingGrantPayloadV1{};
}

bool IsZero(const AnchorOnlyGrantPayloadV1& value) noexcept {
    return value == AnchorOnlyGrantPayloadV1{};
}

bool IsZero(const FinalizationActionReceiptV1& value) noexcept {
    return value == FinalizationActionReceiptV1{};
}

bool IsZero(const FinalizationActionPlanV1& value) noexcept {
    return value == FinalizationActionPlanV1{};
}

bool IsZero(const ReserveStateEntryV1& value) noexcept {
    return value == ReserveStateEntryV1{};
}

bool ValidRegistryCombination(
    const ReserveStateEntryV1& entry) noexcept {
    switch (entry.registry_status) {
        case ReserveRegistryStatusV1::kScaffolding:
            return entry.recovery_origin ==
                       ReserveRecoveryOriginV1::kFreshInit &&
                   IsKnownRecoveryIntent(entry.recovery_intent);
        case ReserveRegistryStatusV1::kInit:
            return entry.recovery_origin ==
                       ReserveRecoveryOriginV1::kFreshInit &&
                   IsKnownRecoveryIntent(entry.recovery_intent);
        case ReserveRegistryStatusV1::kRecovering:
            return (entry.recovery_origin ==
                        ReserveRecoveryOriginV1::kFreshInitTakeover ||
                    entry.recovery_origin ==
                        ReserveRecoveryOriginV1::
                            kAbsentRegistryExistingAnchor ||
                    entry.recovery_origin ==
                        ReserveRecoveryOriginV1::kActiveTakeover) &&
                   IsKnownRecoveryIntent(entry.recovery_intent);
        case ReserveRegistryStatusV1::kActive:
            return IsKnownRecoveryOrigin(entry.recovery_origin) &&
                   entry.recovery_intent ==
                       ReserveRecoveryIntentV1::kResumeConnect;
        case ReserveRegistryStatusV1::kUnused:
            return false;
    }
    return false;
}

ReserveStateV1Error ValidateHeaderLogical(
    const ReserveCoordinatorHeaderV1& header) noexcept {
    if (header.schema_sha256 != kReserveStateV1SchemaSha256) {
        return ReserveStateV1Error::kSchemaMismatch;
    }
    if (IsZero(header.reserve_state_uuid) ||
        IsZero(header.quota_identity_sha256) ||
        IsZero(header.mount_identity_sha256) ||
        IsZero(header.inode_inventory_sha256) ||
        IsZero(header.safe_stop_catalog_sha256) ||
        header.device_id == 0U ||
        header.declared_releasable_bytes == 0U ||
        header.allocation_quantum_bytes == 0U ||
        header.declared_inode_reserve_count == 0U ||
        header.declared_inode_reserve_count >
            kReserveStateV1MaxInodeReserveCount ||
        header.byte_probe_version == 0U ||
        header.inode_probe_version == 0U) {
        return ReserveStateV1Error::kInvalidIdentity;
    }
    if (header.byte_probe_method !=
            ReserveReleaseProbeMethodV1::kFilesystemAndQuota ||
        header.inode_probe_method !=
            ReserveReleaseProbeMethodV1::kFilesystemAndQuota) {
        return ReserveStateV1Error::kUnknownEnum;
    }
    return ReserveStateV1Error::kNone;
}

ReserveStateV1Error ValidatePlanFlags(
    const FinalizationActionReceiptV1& receipt,
    const FinalizationActionPlanV1& plan) noexcept {
    if ((receipt.action_flags & 0xffff0000U) != 0U ||
        (receipt.action_flags &
         ~static_cast<std::uint32_t>(
             kFinalizationPlanFlagsMask)) != 0U ||
        (plan.plan_flags & ~kFinalizationPlanFlagsMask) != 0U) {
        return ReserveStateV1Error::kUnknownFlags;
    }
    if (static_cast<std::uint16_t>(receipt.action_flags) !=
        plan.plan_flags) {
        return ReserveStateV1Error::kInvalidPlan;
    }

    const std::uint16_t flags = plan.plan_flags;
    const bool existing =
        (flags & kFinalizationPlanExistingFinal) != 0U;
    const bool identical =
        (flags & kFinalizationPlanIdenticalCompleteTmp) != 0U;
    const bool durable =
        (flags & kFinalizationPlanDurabilityAlreadyProven) != 0U;
    const bool complete_tmp =
        (flags & kFinalizationPlanCompleteTmpOnly) != 0U;
    const bool partial =
        (flags & kFinalizationPlanRecognizedPartialTmp) != 0U;
    const bool derive =
        (flags & kFinalizationPlanDeriveTerminalFrontier) != 0U;
    const bool resumed =
        (flags & kFinalizationPlanReportResultResumedOpen) != 0U;
    const bool active_origin =
        (flags & kFinalizationPlanReportOriginWasActive) != 0U;

    if (identical && !existing) {
        return ReserveStateV1Error::kInvalidPlan;
    }
    if (complete_tmp && (existing || identical || durable)) {
        return ReserveStateV1Error::kInvalidPlan;
    }
    if (partial &&
        flags != kFinalizationPlanRecognizedPartialTmp) {
        return ReserveStateV1Error::kInvalidPlan;
    }
    if (derive &&
        (receipt.action_kind !=
             FinalizationActionKindV1::kSealedRawCertificate ||
         flags != kFinalizationPlanDeriveTerminalFrontier)) {
        return ReserveStateV1Error::kInvalidPlan;
    }
    if (receipt.action_kind ==
            FinalizationActionKindV1::kSealedRawCertificate &&
        !derive) {
        return ReserveStateV1Error::kInvalidPlan;
    }
    if ((resumed || active_origin) &&
        receipt.action_kind !=
            FinalizationActionKindV1::
                kPreexistingRecoveryReport) {
        return ReserveStateV1Error::kInvalidPlan;
    }
    if (active_origin && !resumed) {
        return ReserveStateV1Error::kInvalidPlan;
    }
    if (active_origin) {
        constexpr std::uint16_t kExactActiveHistorical =
            kFinalizationPlanExistingFinal |
            kFinalizationPlanDurabilityAlreadyProven |
            kFinalizationPlanReportResultResumedOpen |
            kFinalizationPlanReportOriginWasActive;
        if (flags != kExactActiveHistorical) {
            return ReserveStateV1Error::kInvalidPlan;
        }
    }
    return ReserveStateV1Error::kNone;
}

bool IsRawMutationAction(
    FinalizationActionKindV1 kind) noexcept {
    return kind == FinalizationActionKindV1::kCurrentSegmentDrain ||
           kind == FinalizationActionKindV1::kJournalGrowth ||
           kind == FinalizationActionKindV1::kContinuation ||
           kind == FinalizationActionKindV1::kIndex ||
           kind == FinalizationActionKindV1::kManifest;
}

ReserveStateV1Error ValidateCertificatePlan(
    const ReserveStateEntryV1& entry,
    std::size_t action_index,
    const FinalizationActionPlanV1& plan) noexcept {
    if ((plan.plan_flags &
         kFinalizationPlanDeriveTerminalFrontier) == 0U) {
        return ReserveStateV1Error::kNone;
    }
    if (plan.object_sequence != 0U ||
        plan.causal_id != entry.stream_day_id ||
        plan.range_end_or_size == 0U ||
        (plan.range_start >> 16U) != 0U) {
        return ReserveStateV1Error::kInvalidPlan;
    }
    const auto first =
        static_cast<std::uint8_t>(plan.range_start & 0xffU);
    const auto last = static_cast<std::uint8_t>(
        (plan.range_start >> 8U) & 0xffU);
    if (first == 0xffU || last == 0xffU) {
        if (first != 0xffU || last != 0xffU ||
            action_index != 0U) {
            return ReserveStateV1Error::kInvalidPlan;
        }
        return ReserveStateV1Error::kNone;
    }
    if (first > last ||
        static_cast<std::size_t>(last) >= action_index) {
        return ReserveStateV1Error::kInvalidPlan;
    }
    for (std::size_t index = 0U; index < action_index; ++index) {
        if (IsRawMutationAction(entry.actions[index].action_kind) &&
            (index < static_cast<std::size_t>(first) ||
             index > static_cast<std::size_t>(last))) {
            return ReserveStateV1Error::kInvalidPlan;
        }
    }
    return ReserveStateV1Error::kNone;
}

ReserveStateV1Error ValidateActionPlan(
    const ReserveStateEntryV1& entry,
    std::size_t action_index) noexcept {
    const auto& receipt = entry.actions[action_index];
    const auto& plan = entry.plans[action_index];
    if (plan.plan_version != 1U ||
        plan.object_type != receipt.action_kind ||
        IsZero(plan.causal_id)) {
        return ReserveStateV1Error::kInvalidPlan;
    }
    const ReserveStateV1Error flag_error =
        ValidatePlanFlags(receipt, plan);
    if (flag_error != ReserveStateV1Error::kNone) {
        return flag_error;
    }
    return ValidateCertificatePlan(entry, action_index, plan);
}

struct ActionTotals final {
    std::uint64_t byte_grant = 0U;
    std::uint64_t inode_grant = 0U;
    std::uint64_t pending_bytes = 0U;
    std::uint64_t pending_inodes = 0U;
    std::size_t used_count = 0U;
    std::size_t debited_count = 0U;
    std::size_t failed_count = 0U;
};

ReserveStateV1Error ValidateActions(
    const ReserveCoordinatorHeaderV1& header,
    ReserveCoordinatorPhaseV1 phase,
    const ReserveStateEntryV1& entry,
    ActionTotals* totals) noexcept {
    bool saw_unused = false;
    bool saw_non_complete = false;
    bool saw_tombstone = false;
    bool saw_certificate = false;
    bool saw_journal_anchor = false;
    std::size_t final_report_count = 0U;

    for (std::size_t index = 0U;
         index < kReserveStateV1ActionCapacity;
         ++index) {
        const auto& receipt = entry.actions[index];
        const auto& plan = entry.plans[index];
        const bool unused =
            IsZero(receipt) && IsZero(plan);
        if (unused) {
            saw_unused = true;
            continue;
        }
        if (saw_unused || IsZero(receipt) || IsZero(plan)) {
            return ReserveStateV1Error::kInvalidReceipt;
        }
        if (receipt.action_id != index ||
            !IsKnownActionKind(receipt.action_kind) ||
            !IsKnownActionState(receipt.action_state) ||
            IsZero(receipt.object_plan_sha256)) {
            return ReserveStateV1Error::kInvalidReceipt;
        }
        const ReserveStateV1Error plan_error =
            ValidateActionPlan(entry, index);
        if (plan_error != ReserveStateV1Error::kNone) {
            return plan_error;
        }

        std::uint64_t byte_cap = 0U;
        if (!CheckedMul(
                static_cast<std::uint64_t>(
                    receipt.byte_cap_quanta),
                header.allocation_quantum_bytes,
                &byte_cap) ||
            !CheckedAdd(
                totals->byte_grant,
                byte_cap,
                &totals->byte_grant) ||
            !CheckedAdd(
                totals->inode_grant,
                static_cast<std::uint64_t>(receipt.inode_cap),
                &totals->inode_grant)) {
            return ReserveStateV1Error::kArithmeticOverflow;
        }
        if (receipt.action_state ==
            FinalizationActionStateV1::kPending) {
            if (receipt.debit_generation != 0U ||
                !CheckedAdd(
                    totals->pending_bytes,
                    byte_cap,
                    &totals->pending_bytes) ||
                !CheckedAdd(
                    totals->pending_inodes,
                    static_cast<std::uint64_t>(
                        receipt.inode_cap),
                    &totals->pending_inodes)) {
                return receipt.debit_generation != 0U
                           ? ReserveStateV1Error::kInvalidReceipt
                           : ReserveStateV1Error::kArithmeticOverflow;
            }
            saw_non_complete = true;
        } else {
            if (receipt.debit_generation == 0U) {
                return ReserveStateV1Error::kInvalidReceipt;
            }
            if (receipt.action_state ==
                FinalizationActionStateV1::kDebited) {
                ++totals->debited_count;
                saw_non_complete = true;
            } else if (
                receipt.action_state ==
                FinalizationActionStateV1::kFailed) {
                ++totals->failed_count;
                saw_non_complete = true;
            } else if (saw_non_complete) {
                // COMPLETE receipts form a prefix.  A later COMPLETE would
                // violate canonical action-id execution order.
                return ReserveStateV1Error::kInvalidReceipt;
            }
        }

        if (phase ==
                ReserveCoordinatorPhaseV1::
                    kReleasingPrepared &&
            (receipt.action_state !=
                 FinalizationActionStateV1::kPending ||
             receipt.debit_generation != 0U)) {
            return ReserveStateV1Error::kInvalidReceipt;
        }

        if (receipt.action_kind ==
            FinalizationActionKindV1::kFinalizationReport) {
            ++final_report_count;
        }
        saw_tombstone =
            saw_tombstone ||
            receipt.action_kind ==
                FinalizationActionKindV1::
                    kEmptyAnchorTombstone;
        saw_certificate =
            saw_certificate ||
            receipt.action_kind ==
                FinalizationActionKindV1::
                    kSealedRawCertificate;
        saw_journal_anchor =
            saw_journal_anchor ||
            receipt.action_kind ==
                FinalizationActionKindV1::kJournalAnchor;
        ++totals->used_count;
    }

    if (totals->used_count == 0U ||
        final_report_count != 1U ||
        entry.actions[totals->used_count - 1U].action_kind !=
            FinalizationActionKindV1::kFinalizationReport ||
        totals->debited_count > 1U ||
        totals->failed_count > 1U) {
        return ReserveStateV1Error::kInvalidReceipt;
    }

    if (entry.grant_flags == kReserveGrantScaffoldingOnly ||
        entry.grant_flags == kReserveGrantRawAnchorOnly) {
        if (!saw_journal_anchor ||
            !saw_tombstone ||
            saw_certificate) {
            return ReserveStateV1Error::kInvalidReceipt;
        }
        for (std::size_t index = 0U;
             index < totals->used_count;
             ++index) {
            const auto kind = entry.actions[index].action_kind;
            const bool allowed =
                kind ==
                    FinalizationActionKindV1::
                        kDirectoryLeaseScaffold ||
                kind ==
                    FinalizationActionKindV1::kJournalAnchor ||
                kind ==
                    FinalizationActionKindV1::kTypedTmpCleanup ||
                kind ==
                    FinalizationActionKindV1::
                        kEmptyAnchorTombstone ||
                kind ==
                    FinalizationActionKindV1::
                        kFinalizationReport;
            if (!allowed ||
                (entry.grant_flags ==
                     kReserveGrantRawAnchorOnly &&
                 kind ==
                     FinalizationActionKindV1::
                         kDirectoryLeaseScaffold)) {
                return ReserveStateV1Error::kInvalidReceipt;
            }
        }
    }
    return ReserveStateV1Error::kNone;
}

ReserveStateV1Error ValidateRegistryEntry(
    const ReserveStateEntryV1& entry) noexcept {
    if (!IsKnownRegistryStatus(entry.registry_status) ||
        !IsKnownRecoveryOrigin(entry.recovery_origin) ||
        !IsKnownRecoveryIntent(entry.recovery_intent)) {
        return ReserveStateV1Error::kUnknownEnum;
    }
    if (!ValidRegistryCombination(entry)) {
        return ReserveStateV1Error::kInvalidEntry;
    }
    if (entry.ack_status != ReserveAckStatusV1::kUnused ||
        entry.counter_validity != 0U ||
        entry.grant_status != ReserveGrantStatusV1::kUnused ||
        entry.grant_flags != 0U ||
        IsZero(entry.writer_instance) ||
        IsZero(entry.executor_or_recovery_attempt) ||
        !IsZero(entry.raw_counters) ||
        !IsZero(entry.scaffolding_payload) ||
        !IsZero(entry.anchor_only_payload) ||
        entry.continuation_allocation_cap != 0U ||
        entry.activation_fs_free_baseline != 0U ||
        entry.activation_quota_free_baseline != 0U ||
        entry.activation_remaining_cap != 0U ||
        entry.precharged_bytes != 0U ||
        !IsZero(entry.maintenance_report_sha256) ||
        entry.safe_stop_template_id == 0U) {
        return ReserveStateV1Error::kInvalidEntry;
    }
    const bool scaffolding =
        entry.registry_status ==
        ReserveRegistryStatusV1::kScaffolding;
    if ((scaffolding && entry.grant_bytes == 0U) ||
        (!scaffolding && entry.grant_bytes != 0U)) {
        return ReserveStateV1Error::kInvalidEntry;
    }
    for (std::size_t index = 0U;
         index < kReserveStateV1ActionCapacity;
         ++index) {
        if (!IsZero(entry.actions[index]) ||
            !IsZero(entry.plans[index])) {
            return ReserveStateV1Error::kInvalidEntry;
        }
    }
    return ReserveStateV1Error::kNone;
}

ReserveStateV1Error ValidateRawCounterValidity(
    const ReserveStateEntryV1& entry) noexcept {
    if ((entry.counter_validity &
         ~kReserveCounterValidityMask) != 0U) {
        return ReserveStateV1Error::kUnknownFlags;
    }
    const auto& counters = entry.raw_counters;
    if ((entry.counter_validity &
         kReserveCounterCallbackRecords) == 0U &&
        counters.callback_published_records != 0U) {
        return ReserveStateV1Error::kInvalidEntry;
    }
    if ((entry.counter_validity &
         kReserveCounterCallbackVendorBytes) == 0U &&
        counters.callback_published_vendor_bytes != 0U) {
        return ReserveStateV1Error::kInvalidEntry;
    }
    if ((entry.counter_validity & kReserveCounterAppendPair) == 0U &&
        (counters.append_global_wal_pos != 0U ||
         counters.append_ingress_sequence != 0U)) {
        return ReserveStateV1Error::kInvalidEntry;
    }
    if ((entry.counter_validity & kReserveCounterDurablePair) == 0U &&
        (counters.durable_global_wal_pos != 0U ||
         counters.durable_ingress_sequence != 0U)) {
        return ReserveStateV1Error::kInvalidEntry;
    }
    if ((entry.counter_validity & kReserveCounterQueuedPair) == 0U &&
        (counters.queued_record_count != 0U ||
         counters.queued_framed_wal_bytes != 0U)) {
        return ReserveStateV1Error::kInvalidEntry;
    }
    if ((entry.counter_validity & kReserveCounterAppendPair) != 0U &&
        (entry.counter_validity &
         kReserveCounterDurablePair) != 0U &&
        (counters.durable_global_wal_pos >
             counters.append_global_wal_pos ||
         counters.durable_ingress_sequence >
             counters.append_ingress_sequence)) {
        return ReserveStateV1Error::kInvalidEntry;
    }
    if (entry.ack_status == ReserveAckStatusV1::kAcked) {
        if (entry.counter_validity !=
            kReserveCounterValidityMask) {
            return ReserveStateV1Error::kInvalidEntry;
        }
    } else {
        constexpr std::uint8_t kNoAckAllowed =
            kReserveCounterAppendPair |
            kReserveCounterDurablePair;
        if ((entry.counter_validity & ~kNoAckAllowed) != 0U) {
            return ReserveStateV1Error::kInvalidEntry;
        }
    }
    return ReserveStateV1Error::kNone;
}

ReserveStateV1Error ValidateGrantPayload(
    const ReserveStateEntryV1& entry) noexcept {
    if ((entry.grant_flags & ~kReserveGrantFlagsMask) != 0U ||
        entry.grant_flags == kReserveGrantFlagsMask) {
        return ReserveStateV1Error::kUnknownFlags;
    }
    if (IsRawGrant(entry.grant_flags)) {
        if (!IsZero(entry.scaffolding_payload) ||
            !IsZero(entry.anchor_only_payload)) {
            return ReserveStateV1Error::kInvalidEntry;
        }
        return ValidateRawCounterValidity(entry);
    }
    if (entry.counter_validity != 0U ||
        !IsZero(entry.raw_counters) ||
        entry.continuation_allocation_cap != 0U) {
        return ReserveStateV1Error::kInvalidEntry;
    }
    if (entry.grant_flags == kReserveGrantScaffoldingOnly) {
        if (!IsZero(entry.anchor_only_payload) ||
            IsZero(
                entry.scaffolding_payload.recovery_attempt_id) ||
            IsZero(
                entry.scaffolding_payload
                    .object_snapshot_sha256) ||
            entry.scaffolding_payload.required_action_bitmap ==
                0U) {
            return ReserveStateV1Error::kInvalidEntry;
        }
        return ReserveStateV1Error::kNone;
    }
    if (!IsZero(entry.scaffolding_payload) ||
        IsZero(entry.anchor_only_payload.recovery_attempt_id) ||
        IsZero(entry.anchor_only_payload.journal_header_sha256) ||
        entry.anchor_only_payload.required_action_bitmap == 0U ||
        entry.anchor_only_payload.finalization_intent !=
            ReserveRecoveryIntentV1::kRecoverSealOnly ||
        (entry.anchor_only_payload.origin_registry_status !=
             ReserveRegistryStatusV1::kInit &&
         entry.anchor_only_payload.origin_registry_status !=
             ReserveRegistryStatusV1::kRecovering) ||
        (entry.anchor_only_payload.recovery_origin !=
             ReserveRecoveryOriginV1::kFreshInit &&
         entry.anchor_only_payload.recovery_origin !=
             ReserveRecoveryOriginV1::kFreshInitTakeover &&
         entry.anchor_only_payload.recovery_origin !=
             ReserveRecoveryOriginV1::
                 kAbsentRegistryExistingAnchor) ||
        !IsKnownRecoveryIntent(
            entry.anchor_only_payload.original_recovery_intent)) {
        return ReserveStateV1Error::kInvalidEntry;
    }
    return ReserveStateV1Error::kNone;
}

ReserveStateV1Error ValidateGrantEntry(
    const ReserveCoordinatorHeaderV1& header,
    ReserveCoordinatorPhaseV1 phase,
    const ReserveStateEntryV1& entry,
    std::uint64_t* inode_grant,
    std::size_t* debited_count) noexcept {
    if (entry.registry_status != ReserveRegistryStatusV1::kUnused ||
        entry.recovery_origin != ReserveRecoveryOriginV1::kUnused ||
        entry.recovery_intent != ReserveRecoveryIntentV1::kNone ||
        !IsKnownAckStatus(entry.ack_status) ||
        !IsKnownGrantStatus(entry.grant_status) ||
        entry.safe_stop_template_id == 0U ||
        entry.grant_bytes == 0U) {
        return ReserveStateV1Error::kInvalidEntry;
    }
    if ((entry.ack_status == ReserveAckStatusV1::kAcked &&
         IsZero(entry.writer_instance)) ||
        (entry.ack_status == ReserveAckStatusV1::kFencedNoAck &&
         !IsZero(entry.writer_instance)) ||
        (entry.ack_status == ReserveAckStatusV1::kFencedNoAck &&
         entry.continuation_allocation_cap != 0U) ||
        entry.continuation_allocation_cap > entry.grant_bytes) {
        return ReserveStateV1Error::kInvalidEntry;
    }
    const ReserveStateV1Error payload_error =
        ValidateGrantPayload(entry);
    if (payload_error != ReserveStateV1Error::kNone) {
        return payload_error;
    }

    ActionTotals totals{};
    const ReserveStateV1Error actions_error =
        ValidateActions(header, phase, entry, &totals);
    if (actions_error != ReserveStateV1Error::kNone) {
        return actions_error;
    }
    if (totals.byte_grant != entry.grant_bytes) {
        return ReserveStateV1Error::kInvalidAggregate;
    }
    *inode_grant = totals.inode_grant;
    *debited_count = totals.debited_count;

    const bool report_zero =
        IsZero(entry.maintenance_report_sha256);
    switch (entry.grant_status) {
        case ReserveGrantStatusV1::kPending:
            if (!IsZero(entry.executor_or_recovery_attempt) ||
                entry.activation_fs_free_baseline != 0U ||
                entry.activation_quota_free_baseline != 0U ||
                entry.activation_remaining_cap != 0U ||
                entry.precharged_bytes != 0U ||
                !report_zero ||
                totals.pending_bytes != entry.grant_bytes ||
                totals.debited_count != 0U ||
                totals.failed_count != 0U) {
                return ReserveStateV1Error::kInvalidActivation;
            }
            for (std::size_t index = 0U;
                 index < totals.used_count;
                 ++index) {
                if (entry.actions[index].action_state !=
                    FinalizationActionStateV1::kPending) {
                    return ReserveStateV1Error::
                        kInvalidActivation;
                }
            }
            break;
        case ReserveGrantStatusV1::kActive:
            if (IsZero(entry.executor_or_recovery_attempt) ||
                entry.activation_fs_free_baseline == 0U ||
                entry.activation_quota_free_baseline == 0U ||
                entry.activation_remaining_cap !=
                    totals.pending_bytes ||
                entry.precharged_bytes != entry.grant_bytes ||
                !report_zero ||
                totals.failed_count != 0U) {
                return ReserveStateV1Error::kInvalidActivation;
            }
            break;
        case ReserveGrantStatusV1::kDone:
            if (IsZero(entry.executor_or_recovery_attempt) ||
                entry.activation_fs_free_baseline == 0U ||
                entry.activation_quota_free_baseline == 0U ||
                entry.activation_remaining_cap != 0U ||
                entry.precharged_bytes != entry.grant_bytes ||
                report_zero ||
                totals.pending_bytes != 0U ||
                totals.debited_count != 0U ||
                totals.failed_count != 0U) {
                return ReserveStateV1Error::kInvalidActivation;
            }
            for (std::size_t index = 0U;
                 index < totals.used_count;
                 ++index) {
                if (entry.actions[index].action_state !=
                    FinalizationActionStateV1::kComplete) {
                    return ReserveStateV1Error::kInvalidActivation;
                }
            }
            break;
        case ReserveGrantStatusV1::kFailed:
            if (IsZero(entry.executor_or_recovery_attempt) ||
                entry.activation_fs_free_baseline == 0U ||
                entry.activation_quota_free_baseline == 0U ||
                entry.activation_remaining_cap !=
                    totals.pending_bytes ||
                entry.precharged_bytes != entry.grant_bytes ||
                totals.debited_count != 0U) {
                return ReserveStateV1Error::kInvalidActivation;
            }
            break;
        case ReserveGrantStatusV1::kUnused:
            return ReserveStateV1Error::kUnknownEnum;
    }

    if (phase ==
            ReserveCoordinatorPhaseV1::kReleasingPrepared &&
        entry.grant_status != ReserveGrantStatusV1::kPending) {
        return ReserveStateV1Error::kInvalidState;
    }
    return ReserveStateV1Error::kNone;
}

bool EntryKeyLess(
    const ReserveStateEntryV1& lhs,
    const ReserveStateEntryV1& rhs) noexcept {
    if (lhs.source_stream_id != rhs.source_stream_id) {
        return lhs.source_stream_id < rhs.source_stream_id;
    }
    if (lhs.capture_date != rhs.capture_date) {
        return lhs.capture_date < rhs.capture_date;
    }
    return lhs.stream_day_id < rhs.stream_day_id;
}

ReserveStateV1Error ValidateSlotLogical(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& slot) noexcept {
    if (!IsKnownCoordinatorPhase(slot.coordinator_state) ||
        !IsKnownReason(slot.reason) ||
        !IsKnownTrigger(slot.trigger)) {
        return ReserveStateV1Error::kUnknownEnum;
    }
    if (slot.generation == 0U ||
        slot.reserve_state_uuid != header.reserve_state_uuid) {
        return ReserveStateV1Error::kInvalidGeneration;
    }
    if (slot.entry_count > kReserveStateV1EntryCapacity) {
        return ReserveStateV1Error::kInvalidCount;
    }
    const bool provisioned =
        slot.coordinator_state ==
        ReserveCoordinatorPhaseV1::kProvisioned;
    if (provisioned !=
        (slot.reason == ReserveReleaseReasonV1::kNone &&
         slot.trigger == ReserveReleaseTriggerV1::kNone)) {
        return ReserveStateV1Error::kInvalidState;
    }
    if (!provisioned &&
        (slot.reason == ReserveReleaseReasonV1::kNone ||
         slot.trigger == ReserveReleaseTriggerV1::kNone)) {
        return ReserveStateV1Error::kInvalidState;
    }

    for (std::size_t index = 0U;
         index < kReserveStateV1EntryCapacity;
         ++index) {
        const bool used = index < slot.entry_count;
        if (used) {
            const auto& entry = slot.entries[index];
            if (entry.source_stream_id == 0U ||
                entry.capture_date == 0U ||
                IsZero(entry.stream_day_id)) {
                return ReserveStateV1Error::kInvalidEntry;
            }
            if (index > 0U &&
                !EntryKeyLess(slot.entries[index - 1U], entry)) {
                return ReserveStateV1Error::kInvalidOrdering;
            }
            for (std::size_t previous = 0U;
                 previous < index;
                 ++previous) {
                if (slot.entries[previous].source_stream_id ==
                        entry.source_stream_id &&
                    slot.entries[previous].capture_date ==
                        entry.capture_date) {
                    return ReserveStateV1Error::kDuplicateRoute;
                }
            }
        } else if (!IsZero(slot.entries[index])) {
            return ReserveStateV1Error::kInvalidEntry;
        }
    }

    if (IsRegistryPhase(slot.coordinator_state)) {
        for (std::size_t index = 0U;
             index < slot.entry_count;
             ++index) {
            const ReserveStateV1Error error =
                ValidateRegistryEntry(slot.entries[index]);
            if (error != ReserveStateV1Error::kNone) {
                return error;
            }
        }
        const bool intent =
            slot.coordinator_state ==
            ReserveCoordinatorPhaseV1::kReleasingIntent;
        if (!IsZero(slot.finalization_cycle_id) ||
            slot.aggregate_grant_bytes != 0U ||
            slot.aggregate_inode_grant != 0U ||
            slot.completed_bitmap != 0U ||
            slot.active_entry_index !=
                kReserveStateV1NoActiveEntry ||
            slot.pre_release_fs_free_bytes != 0U ||
            slot.pre_release_quota_free_bytes != 0U ||
            slot.pre_release_fs_free_inodes != 0U ||
            slot.pre_release_quota_free_inodes != 0U ||
            slot.expected_release_fs_bytes != 0U ||
            slot.expected_release_quota_bytes != 0U ||
            slot.expected_release_fs_inodes != 0U ||
            slot.expected_release_quota_inodes != 0U ||
            slot.reserved_margin_bytes != 0U ||
            slot.reserved_margin_inodes != 0U ||
            slot.effective_min_fs_free_bytes != 0U ||
            slot.effective_min_quota_free_bytes != 0U ||
            slot.effective_min_fs_free_inodes != 0U ||
            slot.effective_min_quota_free_inodes != 0U ||
            slot.active_fs_free_inode_baseline != 0U ||
            slot.active_quota_free_inode_baseline != 0U ||
            (intent && IsZero(slot.writer_set_sha256)) ||
            (!intent && !IsZero(slot.writer_set_sha256))) {
            return ReserveStateV1Error::kInvalidState;
        }
        return ReserveStateV1Error::kNone;
    }

    if (IsZero(slot.finalization_cycle_id) ||
        IsZero(slot.writer_set_sha256) ||
        (slot.active_entry_index !=
             kReserveStateV1NoActiveEntry &&
         slot.active_entry_index >= slot.entry_count)) {
        return ReserveStateV1Error::kInvalidState;
    }

    std::uint64_t aggregate_bytes = 0U;
    std::uint64_t aggregate_inodes = 0U;
    std::size_t active_count = 0U;
    std::size_t global_debited_count = 0U;
    std::uint16_t completed_bitmap = 0U;
    bool any_failed = false;
    for (std::size_t index = 0U;
         index < slot.entry_count;
         ++index) {
        std::uint64_t inode_grant = 0U;
        std::size_t debited_count = 0U;
        const ReserveStateV1Error error = ValidateGrantEntry(
            header,
            slot.coordinator_state,
            slot.entries[index],
            &inode_grant,
            &debited_count);
        if (error != ReserveStateV1Error::kNone) {
            return error;
        }
        if (!CheckedAdd(
                aggregate_bytes,
                slot.entries[index].grant_bytes,
                &aggregate_bytes) ||
            !CheckedAdd(
                aggregate_inodes,
                inode_grant,
                &aggregate_inodes)) {
            return ReserveStateV1Error::kArithmeticOverflow;
        }
        global_debited_count += debited_count;
        if (slot.entries[index].grant_status ==
            ReserveGrantStatusV1::kActive) {
            ++active_count;
            if (slot.active_entry_index != index) {
                return ReserveStateV1Error::kInvalidActivation;
            }
        }
        if (slot.entries[index].grant_status ==
            ReserveGrantStatusV1::kDone) {
            completed_bitmap |=
                static_cast<std::uint16_t>(1U << index);
        }
        any_failed =
            any_failed ||
            slot.entries[index].grant_status ==
                ReserveGrantStatusV1::kFailed;
    }

    if (aggregate_bytes != slot.aggregate_grant_bytes ||
        aggregate_inodes != slot.aggregate_inode_grant ||
        completed_bitmap != slot.completed_bitmap ||
        active_count > 1U ||
        global_debited_count > 1U ||
        (active_count == 0U &&
         slot.active_entry_index !=
             kReserveStateV1NoActiveEntry) ||
        (active_count != 0U && any_failed)) {
        return ReserveStateV1Error::kInvalidAggregate;
    }
    if ((active_count == 0U &&
         (slot.active_fs_free_inode_baseline != 0U ||
          slot.active_quota_free_inode_baseline != 0U)) ||
        (active_count == 1U &&
         (slot.active_fs_free_inode_baseline == 0U ||
          slot.active_quota_free_inode_baseline == 0U))) {
        return ReserveStateV1Error::kInvalidActivation;
    }

    if (slot.expected_release_fs_bytes == 0U ||
        slot.expected_release_quota_bytes == 0U ||
        slot.expected_release_fs_inodes == 0U ||
        slot.expected_release_quota_inodes == 0U ||
        slot.effective_min_fs_free_bytes == 0U ||
        slot.effective_min_quota_free_bytes == 0U ||
        slot.effective_min_fs_free_inodes == 0U ||
            slot.effective_min_quota_free_inodes == 0U) {
        return ReserveStateV1Error::kInvalidAggregate;
    }
    const std::uint64_t minimum_physical_inode_charge =
        static_cast<std::uint64_t>(
            header.declared_inode_reserve_count) +
        1U;
    if (slot.expected_release_fs_bytes <
            header.declared_releasable_bytes ||
        slot.expected_release_quota_bytes <
            header.declared_releasable_bytes ||
        slot.expected_release_fs_inodes <
            minimum_physical_inode_charge ||
        slot.expected_release_quota_inodes <
            minimum_physical_inode_charge) {
        // The data-reserve inode is released in addition to the explicitly
        // enumerated inode-reserve inventory. These fields are measured
        // filesystem/quota observations and may conservatively be larger,
        // but they must never understate the immutable physical reserve.
        return ReserveStateV1Error::kInvalidAggregate;
    }

    std::uint64_t required_bytes = 0U;
    std::uint64_t required_inodes = 0U;
    if (!CheckedAdd(
            slot.aggregate_grant_bytes,
            slot.reserved_margin_bytes,
            &required_bytes) ||
        !CheckedAdd(
            slot.aggregate_inode_grant,
            slot.reserved_margin_inodes,
            &required_inodes)) {
        return ReserveStateV1Error::kArithmeticOverflow;
    }
    if (required_bytes > header.declared_releasable_bytes ||
        required_bytes > slot.expected_release_fs_bytes ||
        required_bytes > slot.expected_release_quota_bytes ||
        required_bytes > slot.effective_min_fs_free_bytes ||
        required_bytes > slot.effective_min_quota_free_bytes ||
        required_inodes >
            header.declared_inode_reserve_count ||
        required_inodes > slot.expected_release_fs_inodes ||
        required_inodes > slot.expected_release_quota_inodes ||
        required_inodes > slot.effective_min_fs_free_inodes ||
        required_inodes >
            slot.effective_min_quota_free_inodes) {
        return ReserveStateV1Error::kInvalidAggregate;
    }
    return ReserveStateV1Error::kNone;
}

void StoreRawCounters(
    const RawFinalizationCountersV1& value,
    std::span<std::byte> output) noexcept {
    StoreU64Le(
        value.callback_published_records, output, 0U);
    StoreU64Le(
        value.callback_published_vendor_bytes, output, 8U);
    StoreU64Le(value.append_global_wal_pos, output, 16U);
    StoreU64Le(value.append_ingress_sequence, output, 24U);
    StoreU64Le(value.durable_global_wal_pos, output, 32U);
    StoreU64Le(value.durable_ingress_sequence, output, 40U);
    StoreU64Le(value.queued_record_count, output, 48U);
    StoreU64Le(value.queued_framed_wal_bytes, output, 56U);
}

void LoadRawCounters(
    std::span<const std::byte> input,
    RawFinalizationCountersV1* value) noexcept {
    value->callback_published_records = LoadU64Le(input, 0U);
    value->callback_published_vendor_bytes =
        LoadU64Le(input, 8U);
    value->append_global_wal_pos = LoadU64Le(input, 16U);
    value->append_ingress_sequence = LoadU64Le(input, 24U);
    value->durable_global_wal_pos = LoadU64Le(input, 32U);
    value->durable_ingress_sequence = LoadU64Le(input, 40U);
    value->queued_record_count = LoadU64Le(input, 48U);
    value->queued_framed_wal_bytes = LoadU64Le(input, 56U);
}

void StoreScaffoldingPayload(
    const ScaffoldingGrantPayloadV1& value,
    std::span<std::byte> output) noexcept {
    StoreBytes(value.recovery_attempt_id, output, 0U);
    StoreBytes(value.object_snapshot_sha256, output, 16U);
    StoreU64Le(value.observed_object_bitmap, output, 48U);
    StoreU64Le(value.required_action_bitmap, output, 56U);
}

void LoadScaffoldingPayload(
    std::span<const std::byte> input,
    ScaffoldingGrantPayloadV1* value) noexcept {
    LoadBytes(input, 0U, &value->recovery_attempt_id);
    LoadBytes(input, 16U, &value->object_snapshot_sha256);
    value->observed_object_bitmap = LoadU64Le(input, 48U);
    value->required_action_bitmap = LoadU64Le(input, 56U);
}

void StoreAnchorOnlyPayload(
    const AnchorOnlyGrantPayloadV1& value,
    std::span<std::byte> output) noexcept {
    StoreBytes(value.recovery_attempt_id, output, 0U);
    StoreBytes(value.journal_header_sha256, output, 16U);
    output[48U] = static_cast<std::byte>(
        static_cast<std::uint8_t>(
            value.origin_registry_status));
    output[49U] = static_cast<std::byte>(
        static_cast<std::uint8_t>(value.recovery_origin));
    output[50U] = static_cast<std::byte>(
        static_cast<std::uint8_t>(
            value.original_recovery_intent));
    output[51U] = static_cast<std::byte>(
        static_cast<std::uint8_t>(value.finalization_intent));
    StoreU32Le(value.recognized_tmp_bitmap, output, 52U);
    StoreU64Le(value.required_action_bitmap, output, 56U);
}

void LoadAnchorOnlyPayload(
    std::span<const std::byte> input,
    AnchorOnlyGrantPayloadV1* value) noexcept {
    LoadBytes(input, 0U, &value->recovery_attempt_id);
    LoadBytes(input, 16U, &value->journal_header_sha256);
    value->origin_registry_status =
        static_cast<ReserveRegistryStatusV1>(
            std::to_integer<std::uint8_t>(input[48U]));
    value->recovery_origin =
        static_cast<ReserveRecoveryOriginV1>(
            std::to_integer<std::uint8_t>(input[49U]));
    value->original_recovery_intent =
        static_cast<ReserveRecoveryIntentV1>(
            std::to_integer<std::uint8_t>(input[50U]));
    value->finalization_intent =
        static_cast<ReserveRecoveryIntentV1>(
            std::to_integer<std::uint8_t>(input[51U]));
    value->recognized_tmp_bitmap = LoadU32Le(input, 52U);
    value->required_action_bitmap = LoadU64Le(input, 56U);
}

void EncodeEntryUnchecked(
    ReserveCoordinatorPhaseV1 phase,
    const ReserveStateEntryV1& entry,
    std::span<std::byte> output) noexcept {
    std::fill(output.begin(), output.end(), std::byte{0});
    StoreU32Le(
        entry.source_stream_id,
        output,
        reserve_state_v1_offset::entry::kSourceStreamId);
    StoreU32Le(
        entry.capture_date,
        output,
        reserve_state_v1_offset::entry::kCaptureDate);
    StoreBytes(
        entry.stream_day_id,
        output,
        reserve_state_v1_offset::entry::kStreamDayId);

    if (IsRegistryPhase(phase)) {
        output[reserve_state_v1_offset::entry::kStateTag0] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    entry.registry_status));
        output[reserve_state_v1_offset::entry::kStateTag1] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    entry.recovery_origin));
        output[reserve_state_v1_offset::entry::kStateTag2] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    entry.recovery_intent));
    } else {
        output[reserve_state_v1_offset::entry::kStateTag0] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    entry.ack_status));
        output[reserve_state_v1_offset::entry::kStateTag1] =
            static_cast<std::byte>(entry.counter_validity);
        output[reserve_state_v1_offset::entry::kStateTag2] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(
                    entry.grant_status));
        output[reserve_state_v1_offset::entry::kStateTag3] =
            static_cast<std::byte>(entry.grant_flags);
    }

    StoreBytes(
        entry.writer_instance,
        output,
        reserve_state_v1_offset::entry::kWriterInstance);
    StoreBytes(
        entry.executor_or_recovery_attempt,
        output,
        reserve_state_v1_offset::entry::
            kExecutorOrRecoveryAttempt);

    std::span<std::byte> payload = output.subspan(
        reserve_state_v1_offset::entry::kTaggedPayload,
        kTaggedPayloadBytes);
    if (IsGrantPhase(phase)) {
        if (entry.grant_flags ==
            kReserveGrantScaffoldingOnly) {
            StoreScaffoldingPayload(
                entry.scaffolding_payload, payload);
        } else if (
            entry.grant_flags ==
            kReserveGrantRawAnchorOnly) {
            StoreAnchorOnlyPayload(
                entry.anchor_only_payload, payload);
        } else {
            StoreRawCounters(entry.raw_counters, payload);
        }
    }

    StoreU64Le(
        entry.grant_bytes,
        output,
        reserve_state_v1_offset::entry::kGrantBytes);
    StoreU64Le(
        entry.continuation_allocation_cap,
        output,
        reserve_state_v1_offset::entry::
            kContinuationAllocationCap);
    StoreU64Le(
        entry.activation_fs_free_baseline,
        output,
        reserve_state_v1_offset::entry::
            kActivationFsFreeBaseline);
    StoreU64Le(
        entry.activation_quota_free_baseline,
        output,
        reserve_state_v1_offset::entry::
            kActivationQuotaFreeBaseline);
    StoreU64Le(
        entry.activation_remaining_cap,
        output,
        reserve_state_v1_offset::entry::
            kActivationRemainingCap);
    StoreU64Le(
        entry.precharged_bytes,
        output,
        reserve_state_v1_offset::entry::kPrechargedBytes);
    StoreBytes(
        entry.maintenance_report_sha256,
        output,
        reserve_state_v1_offset::entry::
            kMaintenanceReportSha256);
    StoreU64Le(
        entry.safe_stop_template_id,
        output,
        reserve_state_v1_offset::entry::
            kSafeStopTemplateId);
    const std::uint32_t crc = ComputeCrcWithZeroedField(
        output,
        reserve_state_v1_offset::entry::kEntryCrc32c);
    StoreU32Le(
        crc,
        output,
        reserve_state_v1_offset::entry::kEntryCrc32c);
}

ReserveStateV1Error DecodeEntry(
    ReserveCoordinatorPhaseV1 phase,
    std::span<const std::byte> input,
    ReserveStateEntryV1* output) noexcept {
    if (IsZero(input)) {
        *output = ReserveStateEntryV1{};
        return ReserveStateV1Error::kNone;
    }
    if (!IsZeroRange(
            input,
            reserve_state_v1_offset::entry::kReserved0,
            4U) ||
        !IsZeroRange(
            input,
            reserve_state_v1_offset::entry::kReserved1,
            4U)) {
        return ReserveStateV1Error::kNonzeroReserved;
    }
    const std::uint32_t stored_crc = LoadU32Le(
        input,
        reserve_state_v1_offset::entry::kEntryCrc32c);
    if (stored_crc != ComputeCrcWithZeroedField(
                          input,
                          reserve_state_v1_offset::entry::
                              kEntryCrc32c)) {
        return ReserveStateV1Error::kCrcMismatch;
    }

    ReserveStateEntryV1 entry{};
    entry.source_stream_id = LoadU32Le(
        input,
        reserve_state_v1_offset::entry::kSourceStreamId);
    entry.capture_date = LoadU32Le(
        input,
        reserve_state_v1_offset::entry::kCaptureDate);
    LoadBytes(
        input,
        reserve_state_v1_offset::entry::kStreamDayId,
        &entry.stream_day_id);

    if (IsRegistryPhase(phase)) {
        entry.registry_status =
            static_cast<ReserveRegistryStatusV1>(
                std::to_integer<std::uint8_t>(
                    input[reserve_state_v1_offset::entry::
                              kStateTag0]));
        entry.recovery_origin =
            static_cast<ReserveRecoveryOriginV1>(
                std::to_integer<std::uint8_t>(
                    input[reserve_state_v1_offset::entry::
                              kStateTag1]));
        entry.recovery_intent =
            static_cast<ReserveRecoveryIntentV1>(
                std::to_integer<std::uint8_t>(
                    input[reserve_state_v1_offset::entry::
                              kStateTag2]));
        if (input[reserve_state_v1_offset::entry::kStateTag3] !=
            std::byte{0}) {
            return ReserveStateV1Error::kNonzeroReserved;
        }
    } else {
        entry.ack_status = static_cast<ReserveAckStatusV1>(
            std::to_integer<std::uint8_t>(
                input[reserve_state_v1_offset::entry::
                          kStateTag0]));
        entry.counter_validity =
            std::to_integer<std::uint8_t>(
                input[reserve_state_v1_offset::entry::
                          kStateTag1]);
        entry.grant_status =
            static_cast<ReserveGrantStatusV1>(
                std::to_integer<std::uint8_t>(
                    input[reserve_state_v1_offset::entry::
                              kStateTag2]));
        entry.grant_flags = std::to_integer<std::uint8_t>(
            input[reserve_state_v1_offset::entry::kStateTag3]);
    }

    LoadBytes(
        input,
        reserve_state_v1_offset::entry::kWriterInstance,
        &entry.writer_instance);
    LoadBytes(
        input,
        reserve_state_v1_offset::entry::
            kExecutorOrRecoveryAttempt,
        &entry.executor_or_recovery_attempt);
    const std::span<const std::byte> payload = input.subspan(
        reserve_state_v1_offset::entry::kTaggedPayload,
        kTaggedPayloadBytes);
    if (IsGrantPhase(phase)) {
        if (entry.grant_flags ==
            kReserveGrantScaffoldingOnly) {
            LoadScaffoldingPayload(
                payload, &entry.scaffolding_payload);
        } else if (
            entry.grant_flags ==
            kReserveGrantRawAnchorOnly) {
            LoadAnchorOnlyPayload(
                payload, &entry.anchor_only_payload);
        } else {
            LoadRawCounters(payload, &entry.raw_counters);
        }
    } else if (!IsZero(payload)) {
        return ReserveStateV1Error::kInvalidEntry;
    }

    entry.grant_bytes = LoadU64Le(
        input,
        reserve_state_v1_offset::entry::kGrantBytes);
    entry.continuation_allocation_cap = LoadU64Le(
        input,
        reserve_state_v1_offset::entry::
            kContinuationAllocationCap);
    entry.activation_fs_free_baseline = LoadU64Le(
        input,
        reserve_state_v1_offset::entry::
            kActivationFsFreeBaseline);
    entry.activation_quota_free_baseline = LoadU64Le(
        input,
        reserve_state_v1_offset::entry::
            kActivationQuotaFreeBaseline);
    entry.activation_remaining_cap = LoadU64Le(
        input,
        reserve_state_v1_offset::entry::
            kActivationRemainingCap);
    entry.precharged_bytes = LoadU64Le(
        input,
        reserve_state_v1_offset::entry::kPrechargedBytes);
    LoadBytes(
        input,
        reserve_state_v1_offset::entry::
            kMaintenanceReportSha256,
        &entry.maintenance_report_sha256);
    entry.safe_stop_template_id = LoadU64Le(
        input,
        reserve_state_v1_offset::entry::
            kSafeStopTemplateId);
    *output = entry;
    return ReserveStateV1Error::kNone;
}

void EncodeReceiptUnchecked(
    const FinalizationActionReceiptV1& receipt,
    std::span<std::byte> output) noexcept {
    std::fill(output.begin(), output.end(), std::byte{0});
    StoreU16Le(
        receipt.action_id,
        output,
        reserve_state_v1_offset::receipt::kActionId);
    output[reserve_state_v1_offset::receipt::kActionKind] =
        static_cast<std::byte>(
            static_cast<std::uint8_t>(receipt.action_kind));
    output[reserve_state_v1_offset::receipt::kActionState] =
        static_cast<std::byte>(
            static_cast<std::uint8_t>(receipt.action_state));
    StoreU32Le(
        receipt.byte_cap_quanta,
        output,
        reserve_state_v1_offset::receipt::kByteCapQuanta);
    StoreU32Le(
        receipt.inode_cap,
        output,
        reserve_state_v1_offset::receipt::kInodeCap);
    StoreU32Le(
        receipt.action_flags,
        output,
        reserve_state_v1_offset::receipt::kActionFlags);
    StoreU64Le(
        receipt.debit_generation,
        output,
        reserve_state_v1_offset::receipt::kDebitGeneration);
    StoreBytes(
        receipt.object_plan_sha256,
        output,
        reserve_state_v1_offset::receipt::
            kObjectPlanSha256);
    const std::uint32_t crc = ComputeCrcWithZeroedField(
        output,
        reserve_state_v1_offset::receipt::kReceiptCrc32c);
    StoreU32Le(
        crc,
        output,
        reserve_state_v1_offset::receipt::kReceiptCrc32c);
}

ReserveStateV1Error DecodeReceipt(
    std::span<const std::byte> input,
    FinalizationActionReceiptV1* output) noexcept {
    if (IsZero(input)) {
        *output = FinalizationActionReceiptV1{};
        return ReserveStateV1Error::kNone;
    }
    if (!IsZeroRange(
            input,
            reserve_state_v1_offset::receipt::kReserved,
            4U)) {
        return ReserveStateV1Error::kNonzeroReserved;
    }
    const std::uint32_t stored_crc = LoadU32Le(
        input,
        reserve_state_v1_offset::receipt::kReceiptCrc32c);
    if (stored_crc != ComputeCrcWithZeroedField(
                          input,
                          reserve_state_v1_offset::receipt::
                              kReceiptCrc32c)) {
        return ReserveStateV1Error::kCrcMismatch;
    }
    FinalizationActionReceiptV1 receipt{};
    receipt.action_id = LoadU16Le(
        input,
        reserve_state_v1_offset::receipt::kActionId);
    receipt.action_kind =
        static_cast<FinalizationActionKindV1>(
            std::to_integer<std::uint8_t>(
                input[reserve_state_v1_offset::receipt::
                          kActionKind]));
    receipt.action_state =
        static_cast<FinalizationActionStateV1>(
            std::to_integer<std::uint8_t>(
                input[reserve_state_v1_offset::receipt::
                          kActionState]));
    receipt.byte_cap_quanta = LoadU32Le(
        input,
        reserve_state_v1_offset::receipt::kByteCapQuanta);
    receipt.inode_cap = LoadU32Le(
        input,
        reserve_state_v1_offset::receipt::kInodeCap);
    receipt.action_flags = LoadU32Le(
        input,
        reserve_state_v1_offset::receipt::kActionFlags);
    receipt.debit_generation = LoadU64Le(
        input,
        reserve_state_v1_offset::receipt::kDebitGeneration);
    LoadBytes(
        input,
        reserve_state_v1_offset::receipt::
            kObjectPlanSha256,
        &receipt.object_plan_sha256);
    *output = receipt;
    return ReserveStateV1Error::kNone;
}

void EncodePlanUnchecked(
    const FinalizationActionPlanV1& plan,
    std::span<std::byte> output) noexcept {
    std::fill(output.begin(), output.end(), std::byte{0});
    output[reserve_state_v1_offset::plan::kPlanVersion] =
        static_cast<std::byte>(plan.plan_version);
    output[reserve_state_v1_offset::plan::kObjectType] =
        static_cast<std::byte>(
            static_cast<std::uint8_t>(plan.object_type));
    StoreU16Le(
        plan.plan_flags,
        output,
        reserve_state_v1_offset::plan::kPlanFlags);
    StoreU32Le(
        plan.object_sequence,
        output,
        reserve_state_v1_offset::plan::kObjectSequence);
    StoreU64Le(
        plan.range_start,
        output,
        reserve_state_v1_offset::plan::kRangeStart);
    StoreU64Le(
        plan.range_end_or_size,
        output,
        reserve_state_v1_offset::plan::kRangeEndOrSize);
    StoreBytes(
        plan.causal_id,
        output,
        reserve_state_v1_offset::plan::kCausalId);
}

void DecodePlan(
    std::span<const std::byte> input,
    FinalizationActionPlanV1* output) noexcept {
    if (IsZero(input)) {
        *output = FinalizationActionPlanV1{};
        return;
    }
    FinalizationActionPlanV1 plan{};
    plan.plan_version = std::to_integer<std::uint8_t>(
        input[reserve_state_v1_offset::plan::kPlanVersion]);
    plan.object_type = static_cast<FinalizationActionKindV1>(
        std::to_integer<std::uint8_t>(
            input[reserve_state_v1_offset::plan::kObjectType]));
    plan.plan_flags = LoadU16Le(
        input,
        reserve_state_v1_offset::plan::kPlanFlags);
    plan.object_sequence = LoadU32Le(
        input,
        reserve_state_v1_offset::plan::kObjectSequence);
    plan.range_start = LoadU64Le(
        input,
        reserve_state_v1_offset::plan::kRangeStart);
    plan.range_end_or_size = LoadU64Le(
        input,
        reserve_state_v1_offset::plan::kRangeEndOrSize);
    LoadBytes(
        input,
        reserve_state_v1_offset::plan::kCausalId,
        &plan.causal_id);
    *output = plan;
}

std::size_t EntryOffset(std::size_t entry_index) noexcept {
    return reserve_state_v1_offset::slot::kEntries +
           (entry_index * kReserveStateV1EntryBytes);
}

std::size_t ReceiptOffset(
    std::size_t entry_index,
    std::size_t action_index) noexcept {
    const std::size_t ordinal =
        (entry_index * kReserveStateV1ActionCapacity) +
        action_index;
    return reserve_state_v1_offset::slot::kReceipts +
           (ordinal * kReserveStateV1ReceiptBytes);
}

std::size_t PlanOffset(
    std::size_t entry_index,
    std::size_t action_index) noexcept {
    const std::size_t ordinal =
        (entry_index * kReserveStateV1ActionCapacity) +
        action_index;
    return reserve_state_v1_offset::slot::kPlans +
           (ordinal * kReserveStateV1PlanBytes);
}

void StoreSlotMetadata(
    const ReserveStateSlotV1& slot,
    std::span<std::byte> output) noexcept {
    StoreBytes(
        kReserveStateV1SlotMagic,
        output,
        reserve_state_v1_offset::slot::kMagic);
    StoreU16Le(
        kReserveStateV1Version,
        output,
        reserve_state_v1_offset::slot::kVersion);
    StoreU16Le(
        static_cast<std::uint16_t>(kReserveStateV1SlotBytes),
        output,
        reserve_state_v1_offset::slot::kSlotSize);
    StoreU16Le(
        static_cast<std::uint16_t>(
            kReserveStateV1SlotMetadataBytes),
        output,
        reserve_state_v1_offset::slot::kMetadataSize);
    output[reserve_state_v1_offset::slot::kCoordinatorState] =
        static_cast<std::byte>(
            static_cast<std::uint8_t>(
                slot.coordinator_state));
    StoreU64Le(
        slot.generation,
        output,
        reserve_state_v1_offset::slot::kGeneration);
    StoreBytes(
        slot.reserve_state_uuid,
        output,
        reserve_state_v1_offset::slot::kReserveStateUuid);
    StoreU16Le(
        static_cast<std::uint16_t>(slot.reason),
        output,
        reserve_state_v1_offset::slot::kReason);
    StoreU16Le(
        static_cast<std::uint16_t>(slot.trigger),
        output,
        reserve_state_v1_offset::slot::kTrigger);
    StoreU16Le(
        slot.entry_count,
        output,
        reserve_state_v1_offset::slot::kEntryCount);
    StoreU16Le(
        slot.active_entry_index,
        output,
        reserve_state_v1_offset::slot::kActiveEntryIndex);
    StoreU16Le(
        slot.completed_bitmap,
        output,
        reserve_state_v1_offset::slot::kCompletedBitmap);
    StoreBytes(
        slot.finalization_cycle_id,
        output,
        reserve_state_v1_offset::slot::
            kFinalizationCycleId);
    StoreBytes(
        slot.writer_set_sha256,
        output,
        reserve_state_v1_offset::slot::kWriterSetSha256);
    StoreU64Le(
        slot.aggregate_grant_bytes,
        output,
        reserve_state_v1_offset::slot::
            kAggregateGrantBytes);
    StoreU64Le(
        slot.aggregate_inode_grant,
        output,
        reserve_state_v1_offset::slot::
            kAggregateInodeGrant);
    StoreU64Le(
        slot.pre_release_fs_free_bytes,
        output,
        reserve_state_v1_offset::slot::
            kPreReleaseFsFreeBytes);
    StoreU64Le(
        slot.pre_release_quota_free_bytes,
        output,
        reserve_state_v1_offset::slot::
            kPreReleaseQuotaFreeBytes);
    StoreU64Le(
        slot.pre_release_fs_free_inodes,
        output,
        reserve_state_v1_offset::slot::
            kPreReleaseFsFreeInodes);
    StoreU64Le(
        slot.pre_release_quota_free_inodes,
        output,
        reserve_state_v1_offset::slot::
            kPreReleaseQuotaFreeInodes);
    StoreU64Le(
        slot.expected_release_fs_bytes,
        output,
        reserve_state_v1_offset::slot::
            kExpectedReleaseFsBytes);
    StoreU64Le(
        slot.expected_release_quota_bytes,
        output,
        reserve_state_v1_offset::slot::
            kExpectedReleaseQuotaBytes);
    StoreU64Le(
        slot.expected_release_fs_inodes,
        output,
        reserve_state_v1_offset::slot::
            kExpectedReleaseFsInodes);
    StoreU64Le(
        slot.expected_release_quota_inodes,
        output,
        reserve_state_v1_offset::slot::
            kExpectedReleaseQuotaInodes);
    StoreU64Le(
        slot.reserved_margin_bytes,
        output,
        reserve_state_v1_offset::slot::
            kReservedMarginBytes);
    StoreU64Le(
        slot.reserved_margin_inodes,
        output,
        reserve_state_v1_offset::slot::
            kReservedMarginInodes);
    StoreU64Le(
        slot.effective_min_fs_free_bytes,
        output,
        reserve_state_v1_offset::slot::
            kEffectiveMinFsFreeBytes);
    StoreU64Le(
        slot.effective_min_quota_free_bytes,
        output,
        reserve_state_v1_offset::slot::
            kEffectiveMinQuotaFreeBytes);
    StoreU64Le(
        slot.effective_min_fs_free_inodes,
        output,
        reserve_state_v1_offset::slot::
            kEffectiveMinFsFreeInodes);
    StoreU64Le(
        slot.effective_min_quota_free_inodes,
        output,
        reserve_state_v1_offset::slot::
            kEffectiveMinQuotaFreeInodes);
    StoreU64Le(
        slot.active_fs_free_inode_baseline,
        output,
        reserve_state_v1_offset::slot::
            kActiveFsFreeInodeBaseline);
    StoreU64Le(
        slot.active_quota_free_inode_baseline,
        output,
        reserve_state_v1_offset::slot::
            kActiveQuotaFreeInodeBaseline);
}

void LoadSlotMetadata(
    std::span<const std::byte> input,
    ReserveStateSlotV1* slot) noexcept {
    slot->coordinator_state =
        static_cast<ReserveCoordinatorPhaseV1>(
            std::to_integer<std::uint8_t>(
                input[reserve_state_v1_offset::slot::
                          kCoordinatorState]));
    slot->generation = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::kGeneration);
    LoadBytes(
        input,
        reserve_state_v1_offset::slot::kReserveStateUuid,
        &slot->reserve_state_uuid);
    slot->reason = static_cast<ReserveReleaseReasonV1>(
        LoadU16Le(
            input, reserve_state_v1_offset::slot::kReason));
    slot->trigger = static_cast<ReserveReleaseTriggerV1>(
        LoadU16Le(
            input, reserve_state_v1_offset::slot::kTrigger));
    slot->entry_count = LoadU16Le(
        input,
        reserve_state_v1_offset::slot::kEntryCount);
    slot->active_entry_index = LoadU16Le(
        input,
        reserve_state_v1_offset::slot::kActiveEntryIndex);
    slot->completed_bitmap = LoadU16Le(
        input,
        reserve_state_v1_offset::slot::kCompletedBitmap);
    LoadBytes(
        input,
        reserve_state_v1_offset::slot::
            kFinalizationCycleId,
        &slot->finalization_cycle_id);
    LoadBytes(
        input,
        reserve_state_v1_offset::slot::kWriterSetSha256,
        &slot->writer_set_sha256);
    slot->aggregate_grant_bytes = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::
            kAggregateGrantBytes);
    slot->aggregate_inode_grant = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::
            kAggregateInodeGrant);
    slot->pre_release_fs_free_bytes = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::
            kPreReleaseFsFreeBytes);
    slot->pre_release_quota_free_bytes = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::
            kPreReleaseQuotaFreeBytes);
    slot->pre_release_fs_free_inodes = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::
            kPreReleaseFsFreeInodes);
    slot->pre_release_quota_free_inodes = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::
            kPreReleaseQuotaFreeInodes);
    slot->expected_release_fs_bytes = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::
            kExpectedReleaseFsBytes);
    slot->expected_release_quota_bytes = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::
            kExpectedReleaseQuotaBytes);
    slot->expected_release_fs_inodes = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::
            kExpectedReleaseFsInodes);
    slot->expected_release_quota_inodes = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::
            kExpectedReleaseQuotaInodes);
    slot->reserved_margin_bytes = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::
            kReservedMarginBytes);
    slot->reserved_margin_inodes = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::
            kReservedMarginInodes);
    slot->effective_min_fs_free_bytes = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::
            kEffectiveMinFsFreeBytes);
    slot->effective_min_quota_free_bytes = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::
            kEffectiveMinQuotaFreeBytes);
    slot->effective_min_fs_free_inodes = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::
            kEffectiveMinFsFreeInodes);
    slot->effective_min_quota_free_inodes = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::
            kEffectiveMinQuotaFreeInodes);
    slot->active_fs_free_inode_baseline = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::
            kActiveFsFreeInodeBaseline);
    slot->active_quota_free_inode_baseline = LoadU64Le(
        input,
        reserve_state_v1_offset::slot::
            kActiveQuotaFreeInodeBaseline);
}

ReserveStateSlotV1 ImmutableProjection(
    ReserveStateSlotV1 slot) noexcept {
    slot.coordinator_state =
        ReserveCoordinatorPhaseV1::kReleasingPrepared;
    slot.generation = 0U;
    slot.completed_bitmap = 0U;
    slot.active_entry_index = kReserveStateV1NoActiveEntry;
    slot.active_fs_free_inode_baseline = 0U;
    slot.active_quota_free_inode_baseline = 0U;
    for (std::size_t entry_index = 0U;
         entry_index < slot.entry_count;
         ++entry_index) {
        auto& entry = slot.entries[entry_index];
        entry.grant_status = ReserveGrantStatusV1::kPending;
        entry.executor_or_recovery_attempt = {};
        entry.activation_fs_free_baseline = 0U;
        entry.activation_quota_free_baseline = 0U;
        entry.activation_remaining_cap = 0U;
        entry.precharged_bytes = 0U;
        entry.maintenance_report_sha256 = {};
        for (auto& action : entry.actions) {
            if (action.action_kind !=
                FinalizationActionKindV1::kUnused) {
                action.action_state =
                    FinalizationActionStateV1::kPending;
                action.debit_generation = 0U;
            }
        }
    }
    return slot;
}

bool SameRegistryEntries(
    const ReserveStateSlotV1& before,
    const ReserveStateSlotV1& after) noexcept {
    if (before.entry_count != after.entry_count) {
        return false;
    }
    for (std::size_t index = 0U; index < before.entry_count; ++index) {
        if (before.entries[index] != after.entries[index]) {
            return false;
        }
    }
    return true;
}

bool SameRegistrySlotMetadata(
    ReserveStateSlotV1 before,
    ReserveStateSlotV1 after) noexcept {
    before.generation = 0U;
    after.generation = 0U;
    before.entry_count = 0U;
    after.entry_count = 0U;
    before.entries = {};
    after.entries = {};
    return before == after;
}

bool SameLogicalRoute(
    const ReserveStateEntryV1& left,
    const ReserveStateEntryV1& right) noexcept {
    return left.source_stream_id ==
               right.source_stream_id &&
           left.capture_date == right.capture_date;
}

bool LogicalRouteLess(
    const ReserveStateEntryV1& left,
    const ReserveStateEntryV1& right) noexcept {
    if (left.source_stream_id != right.source_stream_id) {
        return left.source_stream_id <
               right.source_stream_id;
    }
    return left.capture_date < right.capture_date;
}

bool IsTerminalRegistryRemoval(
    const ReserveStateEntryV1& entry) noexcept {
    return entry.registry_status ==
               ReserveRegistryStatusV1::kActive ||
           (entry.registry_status ==
                ReserveRegistryStatusV1::kRecovering &&
            entry.recovery_intent ==
                ReserveRecoveryIntentV1::
                    kRecoverSealOnly);
}

ReserveStateV1Error ValidateRegistryEntryTransition(
    const ReserveStateEntryV1& before,
    const ReserveStateEntryV1& after) noexcept {
    if (!SameLogicalRoute(before, after) ||
        before.stream_day_id != after.stream_day_id ||
        before.safe_stop_template_id !=
            after.safe_stop_template_id) {
        return ReserveStateV1Error::kImmutableFactChanged;
    }
    if (before == after) {
        return ReserveStateV1Error::kNone;
    }

    ReserveStateEntryV1 expected = before;
    switch (before.registry_status) {
        case ReserveRegistryStatusV1::kScaffolding:
            if (after.registry_status ==
                ReserveRegistryStatusV1::kScaffolding) {
                // A fenced takeover preserves the planned namespace,
                // recovery attempt, origin and intent. Only the current
                // writer instance and bounded scaffolding cap may change.
                expected.writer_instance =
                    after.writer_instance;
                expected.grant_bytes = after.grant_bytes;
            } else if (
                after.registry_status ==
                ReserveRegistryStatusV1::kInit) {
                expected.registry_status =
                    ReserveRegistryStatusV1::kInit;
                expected.grant_bytes = 0U;
            } else {
                return ReserveStateV1Error::
                    kInvalidTransition;
            }
            break;
        case ReserveRegistryStatusV1::kInit:
            if (after.registry_status ==
                ReserveRegistryStatusV1::kRecovering) {
                expected.registry_status =
                    ReserveRegistryStatusV1::kRecovering;
                expected.recovery_origin =
                    ReserveRecoveryOriginV1::
                        kFreshInitTakeover;
                expected.writer_instance =
                    after.writer_instance;
            } else if (
                after.registry_status ==
                ReserveRegistryStatusV1::kActive) {
                expected.registry_status =
                    ReserveRegistryStatusV1::kActive;
            } else {
                return ReserveStateV1Error::
                    kInvalidTransition;
            }
            break;
        case ReserveRegistryStatusV1::kRecovering:
            if (after.registry_status ==
                ReserveRegistryStatusV1::kRecovering) {
                expected.writer_instance =
                    after.writer_instance;
            } else if (
                after.registry_status ==
                ReserveRegistryStatusV1::kActive) {
                expected.registry_status =
                    ReserveRegistryStatusV1::kActive;
            } else {
                return ReserveStateV1Error::
                    kInvalidTransition;
            }
            break;
        case ReserveRegistryStatusV1::kActive:
            if (after.registry_status !=
                    ReserveRegistryStatusV1::
                        kRecovering ||
                after.recovery_origin !=
                    ReserveRecoveryOriginV1::
                        kActiveTakeover ||
                after.executor_or_recovery_attempt ==
                    before.executor_or_recovery_attempt) {
                return ReserveStateV1Error::
                    kInvalidTransition;
            }
            expected.registry_status =
                ReserveRegistryStatusV1::kRecovering;
            expected.recovery_origin =
                ReserveRecoveryOriginV1::kActiveTakeover;
            expected.recovery_intent =
                after.recovery_intent;
            expected.writer_instance =
                after.writer_instance;
            expected.executor_or_recovery_attempt =
                after.executor_or_recovery_attempt;
            break;
        case ReserveRegistryStatusV1::kUnused:
            return ReserveStateV1Error::kInvalidTransition;
    }
    return expected == after
               ? ReserveStateV1Error::kNone
               : ReserveStateV1Error::
                     kImmutableFactChanged;
}

ReserveStateV1Error ValidateProvisionedRegistryTransition(
    const ReserveStateSlotV1& before,
    const ReserveStateSlotV1& after) noexcept {
    if (!SameRegistrySlotMetadata(before, after)) {
        return ReserveStateV1Error::kImmutableFactChanged;
    }

    std::size_t before_index = 0U;
    std::size_t after_index = 0U;
    std::size_t changes = 0U;
    while (before_index < before.entry_count ||
           after_index < after.entry_count) {
        if (before_index == before.entry_count) {
            const auto& added = after.entries[after_index++];
            if (++changes > 1U ||
                !((added.registry_status ==
                       ReserveRegistryStatusV1::
                           kScaffolding &&
                   added.recovery_origin ==
                       ReserveRecoveryOriginV1::
                           kFreshInit) ||
                  (added.registry_status ==
                       ReserveRegistryStatusV1::
                           kRecovering &&
                   added.recovery_origin ==
                       ReserveRecoveryOriginV1::
                           kAbsentRegistryExistingAnchor))) {
                return ReserveStateV1Error::
                    kInvalidTransition;
            }
            continue;
        }
        if (after_index == after.entry_count) {
            const auto& removed =
                before.entries[before_index++];
            if (++changes > 1U ||
                !IsTerminalRegistryRemoval(removed)) {
                return ReserveStateV1Error::
                    kInvalidTransition;
            }
            continue;
        }

        const auto& old_entry =
            before.entries[before_index];
        const auto& new_entry =
            after.entries[after_index];
        if (SameLogicalRoute(old_entry, new_entry)) {
            const ReserveStateV1Error result =
                ValidateRegistryEntryTransition(
                    old_entry, new_entry);
            if (result != ReserveStateV1Error::kNone) {
                return result;
            }
            if (old_entry != new_entry && ++changes > 1U) {
                return ReserveStateV1Error::
                    kInvalidTransition;
            }
            ++before_index;
            ++after_index;
            continue;
        }
        if (LogicalRouteLess(old_entry, new_entry)) {
            if (++changes > 1U ||
                !IsTerminalRegistryRemoval(
                    old_entry)) {
                return ReserveStateV1Error::
                    kInvalidTransition;
            }
            ++before_index;
            continue;
        }
        if (++changes > 1U ||
            !((new_entry.registry_status ==
                   ReserveRegistryStatusV1::kScaffolding &&
               new_entry.recovery_origin ==
                   ReserveRecoveryOriginV1::kFreshInit) ||
              (new_entry.registry_status ==
                   ReserveRegistryStatusV1::kRecovering &&
               new_entry.recovery_origin ==
                   ReserveRecoveryOriginV1::
                       kAbsentRegistryExistingAnchor))) {
            return ReserveStateV1Error::kInvalidTransition;
        }
        ++after_index;
    }
    return ReserveStateV1Error::kNone;
}

ReserveStateV1Error ValidateActionTransition(
    const FinalizationActionReceiptV1& before,
    const FinalizationActionReceiptV1& after,
    std::uint64_t after_generation) noexcept {
    if (before.action_kind != after.action_kind ||
        before.action_id != after.action_id ||
        before.byte_cap_quanta != after.byte_cap_quanta ||
        before.inode_cap != after.inode_cap ||
        before.action_flags != after.action_flags ||
        before.object_plan_sha256 != after.object_plan_sha256) {
        return ReserveStateV1Error::kImmutableFactChanged;
    }
    if (before.action_state == after.action_state) {
        if (before.debit_generation != after.debit_generation) {
            return ReserveStateV1Error::kInvalidTransition;
        }
        return ReserveStateV1Error::kNone;
    }
    if (before.action_state ==
            FinalizationActionStateV1::kPending &&
        after.action_state ==
            FinalizationActionStateV1::kDebited) {
        return after.debit_generation == after_generation
                   ? ReserveStateV1Error::kNone
                   : ReserveStateV1Error::kInvalidTransition;
    }
    if (before.action_state ==
            FinalizationActionStateV1::kDebited &&
        (after.action_state ==
             FinalizationActionStateV1::kComplete ||
         after.action_state ==
             FinalizationActionStateV1::kFailed) &&
        before.debit_generation == after.debit_generation) {
        return ReserveStateV1Error::kNone;
    }
    return ReserveStateV1Error::kInvalidTransition;
}

ReserveStateV1Error ValidateConsumedTransition(
    const ReserveStateSlotV1& before,
    const ReserveStateSlotV1& after) noexcept {
    const ReserveStateV1Error immutable_error =
        ValidateImmutableFinalizationFactsV1(before, after);
    if (immutable_error != ReserveStateV1Error::kNone) {
        return immutable_error;
    }

    std::size_t before_active = kReserveStateV1EntryCapacity;
    std::size_t after_active = kReserveStateV1EntryCapacity;
    bool before_failed = false;
    for (std::size_t index = 0U; index < before.entry_count; ++index) {
        if (before.entries[index].grant_status ==
            ReserveGrantStatusV1::kActive) {
            before_active = index;
        }
        if (after.entries[index].grant_status ==
            ReserveGrantStatusV1::kActive) {
            after_active = index;
        }
        before_failed =
            before_failed ||
            before.entries[index].grant_status ==
                ReserveGrantStatusV1::kFailed;
    }
    if (before_active != kReserveStateV1EntryCapacity &&
        after_active != kReserveStateV1EntryCapacity &&
        before_active != after_active) {
        return ReserveStateV1Error::kInvalidTransition;
    }
    if (before_failed &&
        after_active != kReserveStateV1EntryCapacity) {
        return ReserveStateV1Error::kInvalidTransition;
    }
    if (before_active == kReserveStateV1EntryCapacity &&
        after_active != kReserveStateV1EntryCapacity) {
        std::size_t first_pending = kReserveStateV1EntryCapacity;
        for (std::size_t index = 0U;
             index < before.entry_count;
             ++index) {
            if (before.entries[index].grant_status ==
                ReserveGrantStatusV1::kPending) {
                first_pending = index;
                break;
            }
        }
        if (after_active != first_pending) {
            return ReserveStateV1Error::kInvalidTransition;
        }
    }

    for (std::size_t entry_index = 0U;
         entry_index < before.entry_count;
         ++entry_index) {
        const auto& lhs = before.entries[entry_index];
        const auto& rhs = after.entries[entry_index];
        const bool was_active =
            lhs.grant_status == ReserveGrantStatusV1::kActive;
        const bool newly_active =
            lhs.grant_status == ReserveGrantStatusV1::kPending &&
            rhs.grant_status == ReserveGrantStatusV1::kActive;
        const bool same_status =
            lhs.grant_status == rhs.grant_status;
        const bool active_terminal =
            was_active &&
            (rhs.grant_status == ReserveGrantStatusV1::kDone ||
             rhs.grant_status ==
                 ReserveGrantStatusV1::kFailed);
        if (!(same_status || newly_active || active_terminal)) {
            return ReserveStateV1Error::kInvalidTransition;
        }
        if ((lhs.grant_status == ReserveGrantStatusV1::kDone ||
             lhs.grant_status ==
                 ReserveGrantStatusV1::kFailed) &&
            lhs != rhs) {
            return ReserveStateV1Error::kInvalidTransition;
        }
        if (!was_active && !newly_active && lhs != rhs) {
            return ReserveStateV1Error::kInvalidTransition;
        }
        if (newly_active &&
            rhs.ack_status == ReserveAckStatusV1::kAcked &&
            rhs.executor_or_recovery_attempt !=
                rhs.writer_instance) {
            return ReserveStateV1Error::kInvalidTransition;
        }
        if (was_active &&
            (lhs.activation_fs_free_baseline !=
                 rhs.activation_fs_free_baseline ||
             lhs.activation_quota_free_baseline !=
                 rhs.activation_quota_free_baseline ||
             lhs.precharged_bytes != rhs.precharged_bytes)) {
            return ReserveStateV1Error::kInvalidTransition;
        }
        if (was_active &&
            rhs.grant_status ==
                ReserveGrantStatusV1::kActive &&
            (before.active_fs_free_inode_baseline !=
                 after.active_fs_free_inode_baseline ||
             before.active_quota_free_inode_baseline !=
                 after.active_quota_free_inode_baseline)) {
            return ReserveStateV1Error::kInvalidTransition;
        }
        if (active_terminal &&
            lhs.executor_or_recovery_attempt !=
                rhs.executor_or_recovery_attempt) {
            return ReserveStateV1Error::kInvalidTransition;
        }

        for (std::size_t action_index = 0U;
             action_index < kReserveStateV1ActionCapacity;
             ++action_index) {
            const ReserveStateV1Error action_error =
                ValidateActionTransition(
                    lhs.actions[action_index],
                    rhs.actions[action_index],
                    after.generation);
            if (action_error != ReserveStateV1Error::kNone) {
                return action_error;
            }
            if ((!was_active && !newly_active) &&
                lhs.actions[action_index] !=
                    rhs.actions[action_index]) {
                return ReserveStateV1Error::kInvalidTransition;
            }
            if (newly_active &&
                lhs.actions[action_index] !=
                    rhs.actions[action_index]) {
                return ReserveStateV1Error::kInvalidTransition;
            }
        }
    }
    return ReserveStateV1Error::kNone;
}

}  // namespace

std::string_view ReserveStateV1ErrorName(
    ReserveStateV1Error error) noexcept {
    switch (error) {
        case ReserveStateV1Error::kNone:
            return "none";
        case ReserveStateV1Error::kNullOutput:
            return "null output";
        case ReserveStateV1Error::kInvalidWireSize:
            return "invalid wire size";
        case ReserveStateV1Error::kInvalidMagic:
            return "invalid magic";
        case ReserveStateV1Error::kUnsupportedVersion:
            return "unsupported version";
        case ReserveStateV1Error::kInvalidEndian:
            return "invalid endian";
        case ReserveStateV1Error::kInvalidSize:
            return "invalid size";
        case ReserveStateV1Error::kSchemaMismatch:
            return "schema mismatch";
        case ReserveStateV1Error::kInvalidIdentity:
            return "invalid identity";
        case ReserveStateV1Error::kUnknownEnum:
            return "unknown enum";
        case ReserveStateV1Error::kUnknownFlags:
            return "unknown flags";
        case ReserveStateV1Error::kNonzeroReserved:
            return "nonzero reserved";
        case ReserveStateV1Error::kCrcMismatch:
            return "CRC mismatch";
        case ReserveStateV1Error::kInvalidGeneration:
            return "invalid generation";
        case ReserveStateV1Error::kInvalidState:
            return "invalid state";
        case ReserveStateV1Error::kInvalidCount:
            return "invalid count";
        case ReserveStateV1Error::kInvalidOrdering:
            return "invalid ordering";
        case ReserveStateV1Error::kDuplicateRoute:
            return "duplicate route";
        case ReserveStateV1Error::kArithmeticOverflow:
            return "arithmetic overflow";
        case ReserveStateV1Error::kInvalidEntry:
            return "invalid entry";
        case ReserveStateV1Error::kInvalidReceipt:
            return "invalid receipt";
        case ReserveStateV1Error::kInvalidPlan:
            return "invalid plan";
        case ReserveStateV1Error::kInvalidAggregate:
            return "invalid aggregate";
        case ReserveStateV1Error::kInvalidActivation:
            return "invalid activation";
        case ReserveStateV1Error::kInvalidTransition:
            return "invalid transition";
        case ReserveStateV1Error::kImmutableFactChanged:
            return "immutable fact changed";
        case ReserveStateV1Error::kSlotCorruptionFatal:
            return "slot corruption fatal";
    }
    return "unknown reserve state error";
}

ReserveStateV1Error EncodeReserveCoordinatorHeaderV1(
    const ReserveCoordinatorHeaderV1& header,
    ReserveStateV1HeaderWire* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    const ReserveStateV1Error validation =
        ValidateHeaderLogical(header);
    if (validation != ReserveStateV1Error::kNone) {
        return validation;
    }
    output->fill(std::byte{0});
    std::span<std::byte> wire(*output);
    StoreBytes(
        kReserveStateV1HeaderMagic,
        wire,
        reserve_state_v1_offset::header::kMagic);
    StoreU16Le(
        kReserveStateV1Version,
        wire,
        reserve_state_v1_offset::header::kVersion);
    wire[reserve_state_v1_offset::header::kEndian] =
        static_cast<std::byte>(kReserveStateV1LittleEndian);
    StoreU32Le(
        static_cast<std::uint32_t>(
            kReserveStateV1HeaderBytes),
        wire,
        reserve_state_v1_offset::header::kHeaderSize);
    StoreU32Le(
        static_cast<std::uint32_t>(kReserveStateV1FileBytes),
        wire,
        reserve_state_v1_offset::header::kFileSize);
    StoreBytes(
        header.reserve_state_uuid,
        wire,
        reserve_state_v1_offset::header::kReserveStateUuid);
    StoreBytes(
        header.schema_sha256,
        wire,
        reserve_state_v1_offset::header::kSchemaSha256);
    StoreBytes(
        header.quota_identity_sha256,
        wire,
        reserve_state_v1_offset::header::
            kQuotaIdentitySha256);
    StoreBytes(
        header.mount_identity_sha256,
        wire,
        reserve_state_v1_offset::header::
            kMountIdentitySha256);
    StoreU64Le(
        header.device_id,
        wire,
        reserve_state_v1_offset::header::kDeviceId);
    StoreU64Le(
        header.declared_releasable_bytes,
        wire,
        reserve_state_v1_offset::header::
            kDeclaredReleasableBytes);
    StoreU64Le(
        header.allocation_quantum_bytes,
        wire,
        reserve_state_v1_offset::header::
            kAllocationQuantumBytes);
    StoreU32Le(
        header.declared_inode_reserve_count,
        wire,
        reserve_state_v1_offset::header::
            kDeclaredInodeReserveCount);
    StoreU16Le(
        static_cast<std::uint16_t>(
            header.byte_probe_method),
        wire,
        reserve_state_v1_offset::header::
            kByteProbeMethod);
    StoreU16Le(
        header.byte_probe_version,
        wire,
        reserve_state_v1_offset::header::
            kByteProbeVersion);
    StoreU16Le(
        static_cast<std::uint16_t>(
            header.inode_probe_method),
        wire,
        reserve_state_v1_offset::header::
            kInodeProbeMethod);
    StoreU16Le(
        header.inode_probe_version,
        wire,
        reserve_state_v1_offset::header::
            kInodeProbeVersion);
    StoreBytes(
        header.inode_inventory_sha256,
        wire,
        reserve_state_v1_offset::header::
            kInodeInventorySha256);
    StoreBytes(
        header.safe_stop_catalog_sha256,
        wire,
        reserve_state_v1_offset::header::
            kSafeStopCatalogSha256);
    const std::uint32_t crc = ComputeCrcWithZeroedField(
        wire,
        reserve_state_v1_offset::header::kHeaderCrc32c);
    StoreU32Le(
        crc,
        wire,
        reserve_state_v1_offset::header::kHeaderCrc32c);
    return ReserveStateV1Error::kNone;
}

ReserveStateV1Error DecodeReserveCoordinatorHeaderV1(
    std::span<const std::byte> wire,
    ReserveCoordinatorHeaderV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    if (wire.size() != kReserveStateV1HeaderBytes) {
        return ReserveStateV1Error::kInvalidWireSize;
    }
    if (!std::equal(
            kReserveStateV1HeaderMagic.begin(),
            kReserveStateV1HeaderMagic.end(),
            wire.begin() +
                reserve_state_v1_offset::header::kMagic)) {
        return ReserveStateV1Error::kInvalidMagic;
    }
    if (LoadU16Le(
            wire,
            reserve_state_v1_offset::header::kVersion) !=
        kReserveStateV1Version) {
        return ReserveStateV1Error::kUnsupportedVersion;
    }
    if (std::to_integer<std::uint8_t>(
            wire[reserve_state_v1_offset::header::kEndian]) !=
        kReserveStateV1LittleEndian) {
        return ReserveStateV1Error::kInvalidEndian;
    }
    if (LoadU32Le(
            wire,
            reserve_state_v1_offset::header::kHeaderSize) !=
            kReserveStateV1HeaderBytes ||
        LoadU32Le(
            wire,
            reserve_state_v1_offset::header::kFileSize) !=
            kReserveStateV1FileBytes) {
        return ReserveStateV1Error::kInvalidSize;
    }
    if (wire[reserve_state_v1_offset::header::kReserved0] !=
            std::byte{0} ||
        !IsZeroRange(
            wire,
            reserve_state_v1_offset::header::kReserved1,
            4U) ||
        !IsZeroRange(
            wire,
            reserve_state_v1_offset::header::kReserved2,
            4U) ||
        !IsZeroRange(
            wire,
            reserve_state_v1_offset::header::kReservedTail,
            kReserveStateV1HeaderBytes -
                reserve_state_v1_offset::header::
                    kReservedTail)) {
        return ReserveStateV1Error::kNonzeroReserved;
    }
    const std::uint32_t stored_crc = LoadU32Le(
        wire,
        reserve_state_v1_offset::header::kHeaderCrc32c);
    if (stored_crc != ComputeCrcWithZeroedField(
                          wire,
                          reserve_state_v1_offset::header::
                              kHeaderCrc32c)) {
        return ReserveStateV1Error::kCrcMismatch;
    }

    ReserveCoordinatorHeaderV1 header{};
    LoadBytes(
        wire,
        reserve_state_v1_offset::header::kReserveStateUuid,
        &header.reserve_state_uuid);
    LoadBytes(
        wire,
        reserve_state_v1_offset::header::kSchemaSha256,
        &header.schema_sha256);
    LoadBytes(
        wire,
        reserve_state_v1_offset::header::
            kQuotaIdentitySha256,
        &header.quota_identity_sha256);
    LoadBytes(
        wire,
        reserve_state_v1_offset::header::
            kMountIdentitySha256,
        &header.mount_identity_sha256);
    header.device_id = LoadU64Le(
        wire,
        reserve_state_v1_offset::header::kDeviceId);
    header.declared_releasable_bytes = LoadU64Le(
        wire,
        reserve_state_v1_offset::header::
            kDeclaredReleasableBytes);
    header.allocation_quantum_bytes = LoadU64Le(
        wire,
        reserve_state_v1_offset::header::
            kAllocationQuantumBytes);
    header.declared_inode_reserve_count = LoadU32Le(
        wire,
        reserve_state_v1_offset::header::
            kDeclaredInodeReserveCount);
    header.byte_probe_method =
        static_cast<ReserveReleaseProbeMethodV1>(
            LoadU16Le(
                wire,
                reserve_state_v1_offset::header::
                    kByteProbeMethod));
    header.byte_probe_version = LoadU16Le(
        wire,
        reserve_state_v1_offset::header::
            kByteProbeVersion);
    header.inode_probe_method =
        static_cast<ReserveReleaseProbeMethodV1>(
            LoadU16Le(
                wire,
                reserve_state_v1_offset::header::
                    kInodeProbeMethod));
    header.inode_probe_version = LoadU16Le(
        wire,
        reserve_state_v1_offset::header::
            kInodeProbeVersion);
    LoadBytes(
        wire,
        reserve_state_v1_offset::header::
            kInodeInventorySha256,
        &header.inode_inventory_sha256);
    LoadBytes(
        wire,
        reserve_state_v1_offset::header::
            kSafeStopCatalogSha256,
        &header.safe_stop_catalog_sha256);
    const ReserveStateV1Error validation =
        ValidateHeaderLogical(header);
    if (validation != ReserveStateV1Error::kNone) {
        return validation;
    }
    *output = header;
    return ReserveStateV1Error::kNone;
}

ReserveStateV1Error EncodeReserveStateSlotV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& slot,
    ReserveStateV1SlotWire* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    const ReserveStateV1Error header_error =
        ValidateHeaderLogical(header);
    if (header_error != ReserveStateV1Error::kNone) {
        return header_error;
    }
    const ReserveStateV1Error validation =
        ValidateSlotLogical(header, slot);
    if (validation != ReserveStateV1Error::kNone) {
        return validation;
    }

    output->fill(std::byte{0});
    std::span<std::byte> wire(*output);
    StoreSlotMetadata(slot, wire);
    for (std::size_t entry_index = 0U;
         entry_index < slot.entry_count;
         ++entry_index) {
        EncodeEntryUnchecked(
            slot.coordinator_state,
            slot.entries[entry_index],
            wire.subspan(
                EntryOffset(entry_index),
                kReserveStateV1EntryBytes));
        if (IsGrantPhase(slot.coordinator_state)) {
            for (std::size_t action_index = 0U;
                 action_index < kReserveStateV1ActionCapacity;
                 ++action_index) {
                const auto& receipt =
                    slot.entries[entry_index]
                        .actions[action_index];
                const auto& plan =
                    slot.entries[entry_index]
                        .plans[action_index];
                if (!IsZero(receipt)) {
                    EncodeReceiptUnchecked(
                        receipt,
                        wire.subspan(
                            ReceiptOffset(
                                entry_index, action_index),
                            kReserveStateV1ReceiptBytes));
                }
                if (!IsZero(plan)) {
                    EncodePlanUnchecked(
                        plan,
                        wire.subspan(
                            PlanOffset(
                                entry_index, action_index),
                            kReserveStateV1PlanBytes));
                }
            }
        }
    }
    const std::uint32_t crc = ComputeCrcWithZeroedField(
        wire,
        reserve_state_v1_offset::slot::kSlotCrc32c);
    StoreU32Le(
        crc,
        wire,
        reserve_state_v1_offset::slot::kSlotCrc32c);
    return ReserveStateV1Error::kNone;
}

ReserveStateV1Error DecodeReserveStateSlotV1(
    const ReserveCoordinatorHeaderV1& header,
    std::span<const std::byte> wire,
    ReserveStateSlotV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    if (wire.size() != kReserveStateV1SlotBytes) {
        return ReserveStateV1Error::kInvalidWireSize;
    }
    const ReserveStateV1Error header_error =
        ValidateHeaderLogical(header);
    if (header_error != ReserveStateV1Error::kNone) {
        return header_error;
    }
    if (IsZero(wire)) {
        return ReserveStateV1Error::kSlotCorruptionFatal;
    }
    if (!std::equal(
            kReserveStateV1SlotMagic.begin(),
            kReserveStateV1SlotMagic.end(),
            wire.begin() +
                reserve_state_v1_offset::slot::kMagic)) {
        return ReserveStateV1Error::kInvalidMagic;
    }
    if (LoadU16Le(
            wire,
            reserve_state_v1_offset::slot::kVersion) !=
        kReserveStateV1Version) {
        return ReserveStateV1Error::kUnsupportedVersion;
    }
    if (LoadU16Le(
            wire,
            reserve_state_v1_offset::slot::kSlotSize) !=
            kReserveStateV1SlotBytes ||
        LoadU16Le(
            wire,
            reserve_state_v1_offset::slot::kMetadataSize) !=
            kReserveStateV1SlotMetadataBytes) {
        return ReserveStateV1Error::kInvalidSize;
    }
    if (wire[reserve_state_v1_offset::slot::kReserved0] !=
            std::byte{0} ||
        !IsZeroRange(
            wire,
            reserve_state_v1_offset::slot::kReserved1,
            6U) ||
        !IsZeroRange(
            wire,
            reserve_state_v1_offset::slot::kReservedTail,
            kReserveStateV1SlotMetadataBytes -
                reserve_state_v1_offset::slot::
                    kReservedTail) ||
        !IsZeroRange(
            wire,
            reserve_state_v1_offset::slot::kZeroTail,
            kSlotZeroTailBytes)) {
        return ReserveStateV1Error::kNonzeroReserved;
    }
    const std::uint32_t stored_crc = LoadU32Le(
        wire,
        reserve_state_v1_offset::slot::kSlotCrc32c);
    if (stored_crc != ComputeCrcWithZeroedField(
                          wire,
                          reserve_state_v1_offset::slot::
                              kSlotCrc32c)) {
        return ReserveStateV1Error::kCrcMismatch;
    }

    ReserveStateSlotV1 slot{};
    LoadSlotMetadata(wire, &slot);
    for (std::size_t entry_index = 0U;
         entry_index < kReserveStateV1EntryCapacity;
         ++entry_index) {
        const ReserveStateV1Error entry_error = DecodeEntry(
            slot.coordinator_state,
            wire.subspan(
                EntryOffset(entry_index),
                kReserveStateV1EntryBytes),
            &slot.entries[entry_index]);
        if (entry_error != ReserveStateV1Error::kNone) {
            return entry_error;
        }
        for (std::size_t action_index = 0U;
             action_index < kReserveStateV1ActionCapacity;
             ++action_index) {
            const ReserveStateV1Error receipt_error =
                DecodeReceipt(
                    wire.subspan(
                        ReceiptOffset(
                            entry_index, action_index),
                        kReserveStateV1ReceiptBytes),
                    &slot.entries[entry_index]
                         .actions[action_index]);
            if (receipt_error != ReserveStateV1Error::kNone) {
                return receipt_error;
            }
            DecodePlan(
                wire.subspan(
                    PlanOffset(entry_index, action_index),
                    kReserveStateV1PlanBytes),
                &slot.entries[entry_index]
                     .plans[action_index]);
        }
    }
    const ReserveStateV1Error validation =
        ValidateSlotLogical(header, slot);
    if (validation != ReserveStateV1Error::kNone) {
        return validation;
    }
    *output = slot;
    return ReserveStateV1Error::kNone;
}

ReserveStateV1Error
ValidateImmutableFinalizationFactsV1(
    const ReserveStateSlotV1& before,
    const ReserveStateSlotV1& after) noexcept {
    if (!IsGrantPhase(before.coordinator_state) ||
        !IsGrantPhase(after.coordinator_state) ||
        before.entry_count != after.entry_count) {
        return ReserveStateV1Error::kInvalidTransition;
    }
    if (ImmutableProjection(before) !=
        ImmutableProjection(after)) {
        return ReserveStateV1Error::kImmutableFactChanged;
    }
    return ReserveStateV1Error::kNone;
}

ReserveStateV1Error
ComputeImmutableFinalizationGrantSha256V1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& slot,
    std::size_t entry_index,
    ReserveStateV1Digest* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    if (!IsGrantPhase(slot.coordinator_state) ||
        entry_index >=
            static_cast<std::size_t>(
                slot.entry_count) ||
        entry_index >= slot.entries.size()) {
        return ReserveStateV1Error::kInvalidEntry;
    }

    // Reuse the canonical slot validator so an invalid/unknown enum, flag,
    // plan, cap sum or tagged payload cannot acquire an immutable digest.
    ReserveStateV1SlotWire validated_wire{};
    const ReserveStateV1Error validation =
        EncodeReserveStateSlotV1(
            header, slot, &validated_wire);
    if (validation != ReserveStateV1Error::kNone) {
        return validation;
    }

    const ReserveStateEntryV1& entry =
        slot.entries[entry_index];
    ImmutableGrantEncoder encoder;
    const auto domain =
        std::as_bytes(
            std::span<const char>(
                kImmutableFinalizationGrantSha256DomainV1
                    .data(),
                kImmutableFinalizationGrantSha256DomainV1
                    .size()));
    const std::array<std::byte, 1U> separator{
        std::byte{0}};
    bool encoded =
        encoder.Append(domain) &&
        encoder.Append(separator) &&
        encoder.Append(slot.finalization_cycle_id) &&
        encoder.AppendU32(entry.source_stream_id) &&
        encoder.AppendU32(entry.capture_date) &&
        encoder.Append(entry.stream_day_id) &&
        encoder.AppendU8(
            static_cast<std::uint8_t>(
                entry.ack_status)) &&
        encoder.AppendU8(entry.counter_validity) &&
        encoder.Append(entry.writer_instance) &&
        encoder.AppendU64(
            entry.raw_counters
                .callback_published_records) &&
        encoder.AppendU64(
            entry.raw_counters
                .callback_published_vendor_bytes) &&
        encoder.AppendU64(
            entry.raw_counters
                .append_global_wal_pos) &&
        encoder.AppendU64(
            entry.raw_counters
                .append_ingress_sequence) &&
        encoder.AppendU64(
            entry.raw_counters
                .durable_global_wal_pos) &&
        encoder.AppendU64(
            entry.raw_counters
                .durable_ingress_sequence) &&
        encoder.AppendU64(
            entry.raw_counters
                .queued_record_count) &&
        encoder.AppendU64(
            entry.raw_counters
                .queued_framed_wal_bytes) &&
        encoder.AppendU64(entry.grant_bytes) &&
        encoder.AppendU64(
            entry.continuation_allocation_cap) &&
        encoder.AppendU64(
            entry.safe_stop_template_id) &&
        encoder.AppendU8(entry.grant_flags);

    if (entry.grant_flags ==
        kReserveGrantRawFinalization) {
        encoded =
            encoded && encoder.AppendU16(0U);
    } else if (
        entry.grant_flags ==
        kReserveGrantScaffoldingOnly) {
        const ScaffoldingGrantPayloadV1& payload =
            entry.scaffolding_payload;
        encoded =
            encoded &&
            encoder.AppendU16(
                static_cast<std::uint16_t>(
                    kTaggedPayloadBytes)) &&
            encoder.Append(
                payload.recovery_attempt_id) &&
            encoder.Append(
                payload.object_snapshot_sha256) &&
            encoder.AppendU64(
                payload.observed_object_bitmap) &&
            encoder.AppendU64(
                payload.required_action_bitmap);
    } else if (
        entry.grant_flags ==
        kReserveGrantRawAnchorOnly) {
        const AnchorOnlyGrantPayloadV1& payload =
            entry.anchor_only_payload;
        encoded =
            encoded &&
            encoder.AppendU16(
                static_cast<std::uint16_t>(
                    kTaggedPayloadBytes)) &&
            encoder.Append(
                payload.recovery_attempt_id) &&
            encoder.Append(
                payload.journal_header_sha256) &&
            encoder.AppendU8(
                static_cast<std::uint8_t>(
                    payload
                        .origin_registry_status)) &&
            encoder.AppendU8(
                static_cast<std::uint8_t>(
                    payload.recovery_origin)) &&
            encoder.AppendU8(
                static_cast<std::uint8_t>(
                    payload
                        .original_recovery_intent)) &&
            encoder.AppendU8(
                static_cast<std::uint8_t>(
                    payload
                        .finalization_intent)) &&
            encoder.AppendU32(
                payload.recognized_tmp_bitmap) &&
            encoder.AppendU64(
                payload.required_action_bitmap);
    } else {
        return ReserveStateV1Error::kUnknownFlags;
    }

    for (std::size_t action_index = 0U;
         encoded &&
         action_index <
             kReserveStateV1ActionCapacity;
         ++action_index) {
        const FinalizationActionReceiptV1& receipt =
            entry.actions[action_index];
        const FinalizationActionPlanV1& plan =
            entry.plans[action_index];
        encoded =
            encoder.AppendU8(
                static_cast<std::uint8_t>(
                    receipt.action_kind)) &&
            encoder.AppendU32(
                receipt.action_flags) &&
            encoder.AppendU32(
                receipt.byte_cap_quanta) &&
            encoder.AppendU32(receipt.inode_cap) &&
            encoder.Append(
                receipt.object_plan_sha256) &&
            encoder.AppendU8(plan.plan_version) &&
            encoder.AppendU8(
                static_cast<std::uint8_t>(
                    plan.object_type)) &&
            encoder.AppendU16(plan.plan_flags) &&
            encoder.AppendU32(
                plan.object_sequence) &&
            encoder.AppendU64(plan.range_start) &&
            encoder.AppendU64(
                plan.range_end_or_size) &&
            encoder.Append(plan.causal_id);
    }
    if (!encoded) {
        return ReserveStateV1Error::
            kArithmeticOverflow;
    }

    const ReserveStateV1Digest digest =
        l2flow::common::ComputeSha256(
            encoder.bytes());
    *output = digest;
    return ReserveStateV1Error::kNone;
}

ReserveStateV1Error ValidateReserveStateSlotPairV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveStateV1SlotWire& before_wire,
    const ReserveStateSlotV1& after,
    const ReserveStateV1SlotWire& after_wire) noexcept {
    const ReserveStateV1Error header_error =
        ValidateHeaderLogical(header);
    if (header_error != ReserveStateV1Error::kNone) {
        return header_error;
    }
    const ReserveStateV1Error before_error =
        ValidateSlotLogical(header, before);
    if (before_error != ReserveStateV1Error::kNone) {
        return before_error;
    }
    const ReserveStateV1Error after_error =
        ValidateSlotLogical(header, after);
    if (after_error != ReserveStateV1Error::kNone) {
        return after_error;
    }

    if (before.generation == after.generation) {
        if (before.generation != 1U ||
            before.coordinator_state !=
                ReserveCoordinatorPhaseV1::kProvisioned ||
            after.coordinator_state !=
                ReserveCoordinatorPhaseV1::kProvisioned ||
            before_wire != after_wire) {
            return ReserveStateV1Error::kInvalidGeneration;
        }
        return ReserveStateV1Error::kNone;
    }
    if (before.generation ==
            std::numeric_limits<std::uint64_t>::max() ||
        after.generation != before.generation + 1U) {
        return ReserveStateV1Error::kInvalidGeneration;
    }

    const auto before_phase = before.coordinator_state;
    const auto after_phase = after.coordinator_state;
    if (before_phase ==
            ReserveCoordinatorPhaseV1::kProvisioned &&
        after_phase ==
            ReserveCoordinatorPhaseV1::kProvisioned) {
        return ValidateProvisionedRegistryTransition(
            before, after);
    }
    if (before_phase ==
            ReserveCoordinatorPhaseV1::kProvisioned &&
        after_phase ==
            ReserveCoordinatorPhaseV1::kReleasingIntent) {
        if (!SameRegistryEntries(before, after)) {
            return ReserveStateV1Error::kImmutableFactChanged;
        }
        return ReserveStateV1Error::kNone;
    }
    if (before_phase ==
            ReserveCoordinatorPhaseV1::kReleasingIntent &&
        after_phase ==
            ReserveCoordinatorPhaseV1::kReleasingPrepared) {
        if (before.entry_count != after.entry_count ||
            before.reason != after.reason ||
            before.trigger != after.trigger ||
            before.writer_set_sha256 !=
                after.writer_set_sha256) {
            return ReserveStateV1Error::kImmutableFactChanged;
        }
        for (std::size_t index = 0U;
             index < before.entry_count;
             ++index) {
            const auto& registry = before.entries[index];
            const auto& grant = after.entries[index];
            if (registry.source_stream_id !=
                    grant.source_stream_id ||
                registry.capture_date != grant.capture_date ||
                registry.stream_day_id != grant.stream_day_id ||
                registry.safe_stop_template_id !=
                    grant.safe_stop_template_id ||
                (grant.ack_status ==
                     ReserveAckStatusV1::kAcked &&
                 registry.writer_instance !=
                     grant.writer_instance) ||
                (grant.grant_flags ==
                     kReserveGrantScaffoldingOnly &&
                 (registry.registry_status !=
                      ReserveRegistryStatusV1::
                          kScaffolding ||
                  registry.executor_or_recovery_attempt !=
                      grant.scaffolding_payload
                          .recovery_attempt_id)) ||
                (grant.grant_flags ==
                     kReserveGrantRawAnchorOnly &&
                 ((registry.registry_status !=
                       ReserveRegistryStatusV1::kInit &&
                   registry.registry_status !=
                       ReserveRegistryStatusV1::
                           kRecovering) ||
                  registry.executor_or_recovery_attempt !=
                      grant.anchor_only_payload
                          .recovery_attempt_id ||
                  registry.registry_status !=
                      grant.anchor_only_payload
                          .origin_registry_status ||
                  registry.recovery_origin !=
                      grant.anchor_only_payload
                          .recovery_origin ||
                  registry.recovery_intent !=
                      grant.anchor_only_payload
                          .original_recovery_intent)) ||
                (grant.grant_flags ==
                     kReserveGrantRawFinalization &&
                 registry.registry_status ==
                     ReserveRegistryStatusV1::
                         kScaffolding)) {
                return ReserveStateV1Error::
                    kImmutableFactChanged;
            }
        }
        return ReserveStateV1Error::kNone;
    }
    if (before_phase ==
            ReserveCoordinatorPhaseV1::kReleasingPrepared &&
        after_phase ==
            ReserveCoordinatorPhaseV1::kConsumed) {
        const ReserveStateV1Error immutable_error =
            ValidateImmutableFinalizationFactsV1(
                before, after);
        if (immutable_error != ReserveStateV1Error::kNone) {
            return immutable_error;
        }
        for (std::size_t index = 0U;
             index < after.entry_count;
             ++index) {
            if (after.entries[index].grant_status !=
                ReserveGrantStatusV1::kPending) {
                return ReserveStateV1Error::kInvalidTransition;
            }
        }
        return ReserveStateV1Error::kNone;
    }
    if (before_phase ==
            ReserveCoordinatorPhaseV1::kConsumed &&
        after_phase ==
            ReserveCoordinatorPhaseV1::kConsumed) {
        return ValidateConsumedTransition(before, after);
    }
    return ReserveStateV1Error::kInvalidTransition;
}

ReserveStateV1Error EncodeReserveCoordinatorStateV1(
    const ReserveCoordinatorStateV1& state,
    ReserveStateV1FileWire* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    ReserveStateV1HeaderWire header_wire{};
    const ReserveStateV1Error header_error =
        EncodeReserveCoordinatorHeaderV1(
            state.header, &header_wire);
    if (header_error != ReserveStateV1Error::kNone) {
        return header_error;
    }
    std::array<ReserveStateV1SlotWire, 2U> slot_wires{};
    for (std::size_t index = 0U; index < 2U; ++index) {
        const ReserveStateV1Error slot_error =
            EncodeReserveStateSlotV1(
                state.header,
                state.slots[index],
                &slot_wires[index]);
        if (slot_error != ReserveStateV1Error::kNone) {
            return slot_error;
        }
    }

    std::size_t older = 0U;
    std::size_t newer = 1U;
    if (state.slots[0U].generation >
        state.slots[1U].generation) {
        older = 1U;
        newer = 0U;
    }
    if (state.slots[0U].generation !=
            state.slots[1U].generation) {
        const std::size_t expected_newer =
            (state.slots[newer].generation & 1U) == 0U
                ? 1U
                : 0U;
        if (newer != expected_newer) {
            return ReserveStateV1Error::kInvalidGeneration;
        }
    }
    const ReserveStateV1Error pair_error =
        ValidateReserveStateSlotPairV1(
            state.header,
            state.slots[older],
            slot_wires[older],
            state.slots[newer],
            slot_wires[newer]);
    if (pair_error != ReserveStateV1Error::kNone) {
        return pair_error;
    }

    output->fill(std::byte{0});
    std::copy(
        header_wire.begin(), header_wire.end(), output->begin());
    std::copy(
        slot_wires[0U].begin(),
        slot_wires[0U].end(),
        output->begin() + kReserveStateV1HeaderBytes);
    std::copy(
        slot_wires[1U].begin(),
        slot_wires[1U].end(),
        output->begin() + kReserveStateV1HeaderBytes +
            kReserveStateV1SlotBytes);
    return ReserveStateV1Error::kNone;
}

ReserveStateV1Error DecodeAndSelectReserveCoordinatorStateV1(
    std::span<const std::byte> wire,
    ReserveCoordinatorStateV1* output) noexcept {
    if (output == nullptr) {
        return ReserveStateV1Error::kNullOutput;
    }
    if (wire.size() != kReserveStateV1FileBytes) {
        return ReserveStateV1Error::kInvalidWireSize;
    }
    ReserveCoordinatorStateV1 state{};
    const ReserveStateV1Error header_error =
        DecodeReserveCoordinatorHeaderV1(
            wire.first(kReserveStateV1HeaderBytes),
            &state.header);
    if (header_error != ReserveStateV1Error::kNone) {
        return header_error;
    }

    std::array<ReserveStateV1SlotWire, 2U> slot_wires{};
    for (std::size_t index = 0U; index < 2U; ++index) {
        const std::size_t offset =
            kReserveStateV1HeaderBytes +
            (index * kReserveStateV1SlotBytes);
        std::copy_n(
            wire.data() + offset,
            kReserveStateV1SlotBytes,
            slot_wires[index].begin());
        const ReserveStateV1Error slot_error =
            DecodeReserveStateSlotV1(
                state.header,
                slot_wires[index],
                &state.slots[index]);
        if (slot_error != ReserveStateV1Error::kNone) {
            // A published invalid inactive slot is fatal too.  Returning one
            // error class prevents callers from treating the other slot as a
            // fallback based on the specific failure mode.
            return ReserveStateV1Error::kSlotCorruptionFatal;
        }
    }

    std::size_t older = 0U;
    std::size_t newer = 1U;
    if (state.slots[0U].generation >
        state.slots[1U].generation) {
        older = 1U;
        newer = 0U;
    }
    if (state.slots[0U].generation !=
            state.slots[1U].generation) {
        const std::size_t expected_newer =
            (state.slots[newer].generation & 1U) == 0U
                ? 1U
                : 0U;
        if (newer != expected_newer) {
            return ReserveStateV1Error::kInvalidGeneration;
        }
    }
    const ReserveStateV1Error pair_error =
        ValidateReserveStateSlotPairV1(
            state.header,
            state.slots[older],
            slot_wires[older],
            state.slots[newer],
            slot_wires[newer]);
    if (pair_error != ReserveStateV1Error::kNone) {
        return pair_error;
    }
    if (state.slots[0U].generation ==
        state.slots[1U].generation) {
        state.selected_slot = 0U;
    } else {
        state.selected_slot = newer;
    }
    *output = state;
    return ReserveStateV1Error::kNone;
}

}  // namespace l2flow::ingress
