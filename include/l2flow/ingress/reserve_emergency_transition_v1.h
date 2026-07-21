#pragma once

#include "l2flow/ingress/reserve_state_v1.h"

#include <cstdint>
#include <span>

namespace l2flow::ingress {

// These builders only construct and validate the next logical state slot.
// They do not write or fsync the state file, stop/fence writers, release
// reserve objects, probe recovered capacity, or validate report artifacts.
// The caller must complete the corresponding external barrier before
// publishing a returned candidate.

struct ReserveReleaseIntentV1 final {
    ReserveReleaseReasonV1 reason = ReserveReleaseReasonV1::kNone;
    ReserveReleaseTriggerV1 trigger = ReserveReleaseTriggerV1::kNone;
    ReserveStateV1Digest writer_set_sha256{};
};

struct ReserveReleasePreparedV1 final {
    ReserveStateV1Identity finalization_cycle_id{};
    std::span<const ReserveStateEntryV1> grant_entries{};
    std::uint64_t pre_release_fs_free_bytes = 0U;
    std::uint64_t pre_release_quota_free_bytes = 0U;
    std::uint64_t pre_release_fs_free_inodes = 0U;
    std::uint64_t pre_release_quota_free_inodes = 0U;
    std::uint64_t expected_release_fs_bytes = 0U;
    std::uint64_t expected_release_quota_bytes = 0U;
    std::uint64_t expected_release_fs_inodes = 0U;
    std::uint64_t expected_release_quota_inodes = 0U;
    std::uint64_t reserved_margin_bytes = 0U;
    std::uint64_t reserved_margin_inodes = 0U;
    std::uint64_t effective_min_fs_free_bytes = 0U;
    std::uint64_t effective_min_quota_free_bytes = 0U;
    std::uint64_t effective_min_fs_free_inodes = 0U;
    std::uint64_t effective_min_quota_free_inodes = 0U;
};

// A grant selector deliberately binds more than the stable array index. This
// makes a stale cycle, namespace, ACK result, or safe-stop template fail
// closed before a mutable field can be changed.
struct ReserveFinalizationGrantKeyV1 final {
    ReserveStateV1Identity finalization_cycle_id{};
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    ReserveStateV1Identity stream_day_id{};
    ReserveAckStatusV1 ack_status = ReserveAckStatusV1::kUnused;
    ReserveStateV1Identity ack_writer_instance{};
    std::uint64_t safe_stop_template_id = 0U;

    friend bool operator==(
        const ReserveFinalizationGrantKeyV1&,
        const ReserveFinalizationGrantKeyV1&) = default;
};

struct ReserveGrantActivationV1 final {
    ReserveFinalizationGrantKeyV1 grant{};
    ReserveStateV1Identity executor_instance{};
    std::uint64_t filesystem_free_byte_baseline = 0U;
    std::uint64_t quota_free_byte_baseline = 0U;
    std::uint64_t filesystem_free_inode_baseline = 0U;
    std::uint64_t quota_free_inode_baseline = 0U;
};

struct ReserveFinalizationActionKeyV1 final {
    ReserveFinalizationGrantKeyV1 grant{};
    std::uint16_t action_id = 0U;
    FinalizationActionKindV1 action_kind =
        FinalizationActionKindV1::kUnused;
    ReserveStateV1Digest object_plan_sha256{};

    friend bool operator==(
        const ReserveFinalizationActionKeyV1&,
        const ReserveFinalizationActionKeyV1&) = default;
};

struct ReserveGrantTerminalReportV1 final {
    ReserveFinalizationGrantKeyV1 grant{};
    ReserveStateV1Digest maintenance_report_sha256{};
};

struct ReserveAtomicReportCompletionV1 final {
    ReserveFinalizationActionKeyV1 report_action{};
    ReserveStateV1Digest maintenance_report_sha256{};
};

// Optional report evidence for an ACTIVE -> FAILED cross-object failure. A
// nonzero report digest is accepted only when every action, including the
// terminal report action, is already COMPLETE.
struct ReserveGrantFailureV1 final {
    ReserveFinalizationGrantKeyV1 grant{};
    ReserveStateV1Digest maintenance_report_sha256{};
};

// PROVISIONED -> RELEASING_INTENT. Registry entries are copied exactly.
[[nodiscard]] ReserveStateV1Error BuildReserveReleasingIntentV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveReleaseIntentV1& intent,
    ReserveStateSlotV1* output) noexcept;

// RELEASING_INTENT -> RELEASING_PREPARED. The supplied entries are the
// externally collected ACK/fence/action-plan facts. Aggregate byte and inode
// grants are derived by this builder, never accepted as caller-maintained
// totals.
[[nodiscard]] ReserveStateV1Error BuildReserveReleasingPreparedV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveReleasePreparedV1& prepared,
    ReserveStateSlotV1* output) noexcept;

// RELEASING_PREPARED -> CONSUMED. Calling this is appropriate only after the
// external reserve-unlink/close/dirsync and release-probe barriers succeed.
[[nodiscard]] ReserveStateV1Error BuildReserveConsumedV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    ReserveStateSlotV1* output) noexcept;

// CONSUMED/PENDING -> CONSUMED/ACTIVE for exactly the first pending grant.
[[nodiscard]] ReserveStateV1Error BuildReserveActivateNextGrantV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveGrantActivationV1& activation,
    ReserveStateSlotV1* output) noexcept;

// Changes exactly the smallest PENDING action of the ACTIVE grant to DEBITED.
[[nodiscard]] ReserveStateV1Error BuildReserveDebitActionV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveFinalizationActionKeyV1& action,
    ReserveStateSlotV1* output) noexcept;

// Changes the sole DEBITED action to COMPLETE while leaving the grant ACTIVE.
[[nodiscard]] ReserveStateV1Error BuildReserveCompleteActionV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveFinalizationActionKeyV1& action,
    ReserveStateSlotV1* output) noexcept;

// Atomically changes the sole DEBITED action to FAILED, changes its ACTIVE
// grant to FAILED, and clears the slot's active selector.
[[nodiscard]] ReserveStateV1Error BuildReserveFailDebitedActionV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveFinalizationActionKeyV1& action,
    ReserveStateSlotV1* output) noexcept;

// Records a cross-object failure discovered after at least one receipt is
// COMPLETE. COMPLETE receipts are preserved; no illegal COMPLETE -> FAILED
// transition is synthesized.
[[nodiscard]] ReserveStateV1Error BuildReserveFailActiveGrantV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveGrantFailureV1& failure,
    ReserveStateSlotV1* output) noexcept;

// ACTIVE -> DONE after every used action, including FINALIZATION_REPORT, is
// already COMPLETE. The caller supplies a digest of an externally validated,
// durable report.
[[nodiscard]] ReserveStateV1Error BuildReserveMarkGrantDoneV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveGrantTerminalReportV1& report,
    ReserveStateSlotV1* output) noexcept;

// Closes the report-before-receipt crash window atomically: the sole DEBITED
// terminal report action becomes COMPLETE in the same slot where its grant
// becomes DONE and the externally validated report digest is recorded.
[[nodiscard]] ReserveStateV1Error
BuildReserveCompleteReportAndMarkGrantDoneV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveAtomicReportCompletionV1& report,
    ReserveStateSlotV1* output) noexcept;

// Every builder above verifies the canonical before/after encodings with
// ValidateReserveStateSlotPairV1 and leaves output byte-for-byte unchanged on
// every failure, including invalid input and generation/cap arithmetic
// overflow.

}  // namespace l2flow::ingress
