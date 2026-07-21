#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace l2flow::ingress {

// ReserveCoordinatorStateV1 is an L2Flow-private, explicitly little-endian
// disk format.  It is not an ABI and is never persisted by writing a C++
// object representation.
inline constexpr std::uint16_t kReserveStateV1Version = 1U;
inline constexpr std::uint8_t kReserveStateV1LittleEndian = 1U;
inline constexpr std::size_t kReserveStateV1HeaderBytes = 4096U;
inline constexpr std::size_t kReserveStateV1SlotBytes = 32768U;
inline constexpr std::size_t kReserveStateV1FileBytes =
    kReserveStateV1HeaderBytes + (2U * kReserveStateV1SlotBytes);
inline constexpr std::size_t kReserveStateV1SlotMetadataBytes = 2048U;
inline constexpr std::size_t kReserveStateV1EntryBytes = 224U;
inline constexpr std::size_t kReserveStateV1ReceiptBytes = 64U;
inline constexpr std::size_t kReserveStateV1PlanBytes = 40U;
inline constexpr std::size_t kReserveStateV1EntryCapacity = 16U;
inline constexpr std::size_t kReserveStateV1ActionCapacity = 16U;
inline constexpr std::uint16_t kReserveStateV1NoActiveEntry = 0xffffU;
inline constexpr std::uint32_t kReserveStateV1MaxInodeReserveCount =
    100000000U;

inline constexpr std::array<std::byte, 8U>
    kReserveStateV1HeaderMagic{
        std::byte{'L'}, std::byte{'2'}, std::byte{'R'},
        std::byte{'C'}, std::byte{'S'}, std::byte{'1'},
        std::byte{0}, std::byte{0}};
inline constexpr std::array<std::byte, 8U>
    kReserveStateV1SlotMagic{
        std::byte{'L'}, std::byte{'2'}, std::byte{'R'},
        std::byte{'S'}, std::byte{'S'}, std::byte{'1'},
        std::byte{0}, std::byte{0}};

// SHA-256 over the exact UTF-8 bytes of schemas/reserve_state_v1.json,
// including its single trailing LF.  This repository-private schema freezes
// the V1 choices that docs/design.md intentionally leaves to the schema.
inline constexpr std::size_t kReserveStateV1SchemaBytes = 28560U;
inline constexpr std::string_view kReserveStateV1SchemaSha256Hex =
    "9a6d5c254524241d780b171512a2405a"
    "20742290d20b1249d092fadc7cadee92";
inline constexpr l2flow::common::Sha256Digest
    kReserveStateV1SchemaSha256{
        std::byte{0x9a}, std::byte{0x6d}, std::byte{0x5c},
        std::byte{0x25}, std::byte{0x45}, std::byte{0x24},
        std::byte{0x24}, std::byte{0x1d}, std::byte{0x78},
        std::byte{0x0b}, std::byte{0x17}, std::byte{0x15},
        std::byte{0x12}, std::byte{0xa2}, std::byte{0x40},
        std::byte{0x5a}, std::byte{0x20}, std::byte{0x74},
        std::byte{0x22}, std::byte{0x90}, std::byte{0xd2},
        std::byte{0x0b}, std::byte{0x12}, std::byte{0x49},
        std::byte{0xd0}, std::byte{0x92}, std::byte{0xfa},
        std::byte{0xdc}, std::byte{0x7c}, std::byte{0xad},
        std::byte{0xee}, std::byte{0x92}};

using ReserveStateV1Identity = l2flow::common::Identity128;
using ReserveStateV1Digest = l2flow::common::Sha256Digest;
using ReserveStateV1HeaderWire =
    std::array<std::byte, kReserveStateV1HeaderBytes>;
using ReserveStateV1SlotWire =
    std::array<std::byte, kReserveStateV1SlotBytes>;
using ReserveStateV1FileWire =
    std::array<std::byte, kReserveStateV1FileBytes>;

namespace reserve_state_v1_offset {

namespace header {
inline constexpr std::size_t kMagic = 0U;
inline constexpr std::size_t kVersion = 8U;
inline constexpr std::size_t kEndian = 10U;
inline constexpr std::size_t kReserved0 = 11U;
inline constexpr std::size_t kHeaderSize = 12U;
inline constexpr std::size_t kFileSize = 16U;
inline constexpr std::size_t kReserved1 = 20U;
inline constexpr std::size_t kReserveStateUuid = 24U;
inline constexpr std::size_t kSchemaSha256 = 40U;
inline constexpr std::size_t kQuotaIdentitySha256 = 72U;
inline constexpr std::size_t kMountIdentitySha256 = 104U;
inline constexpr std::size_t kDeviceId = 136U;
inline constexpr std::size_t kDeclaredReleasableBytes = 144U;
inline constexpr std::size_t kAllocationQuantumBytes = 152U;
inline constexpr std::size_t kDeclaredInodeReserveCount = 160U;
inline constexpr std::size_t kByteProbeMethod = 164U;
inline constexpr std::size_t kByteProbeVersion = 166U;
inline constexpr std::size_t kInodeProbeMethod = 168U;
inline constexpr std::size_t kInodeProbeVersion = 170U;
inline constexpr std::size_t kReserved2 = 172U;
inline constexpr std::size_t kInodeInventorySha256 = 176U;
inline constexpr std::size_t kSafeStopCatalogSha256 = 208U;
inline constexpr std::size_t kHeaderCrc32c = 240U;
inline constexpr std::size_t kReservedTail = 244U;
}  // namespace header

namespace slot {
inline constexpr std::size_t kMagic = 0U;
inline constexpr std::size_t kVersion = 8U;
inline constexpr std::size_t kSlotSize = 10U;
inline constexpr std::size_t kMetadataSize = 12U;
inline constexpr std::size_t kCoordinatorState = 14U;
inline constexpr std::size_t kReserved0 = 15U;
inline constexpr std::size_t kGeneration = 16U;
inline constexpr std::size_t kReserveStateUuid = 24U;
inline constexpr std::size_t kReason = 40U;
inline constexpr std::size_t kTrigger = 42U;
inline constexpr std::size_t kEntryCount = 44U;
inline constexpr std::size_t kActiveEntryIndex = 46U;
inline constexpr std::size_t kCompletedBitmap = 48U;
inline constexpr std::size_t kReserved1 = 50U;
inline constexpr std::size_t kFinalizationCycleId = 56U;
inline constexpr std::size_t kWriterSetSha256 = 72U;
inline constexpr std::size_t kAggregateGrantBytes = 104U;
inline constexpr std::size_t kAggregateInodeGrant = 112U;
inline constexpr std::size_t kPreReleaseFsFreeBytes = 120U;
inline constexpr std::size_t kPreReleaseQuotaFreeBytes = 128U;
inline constexpr std::size_t kPreReleaseFsFreeInodes = 136U;
inline constexpr std::size_t kPreReleaseQuotaFreeInodes = 144U;
inline constexpr std::size_t kExpectedReleaseFsBytes = 152U;
inline constexpr std::size_t kExpectedReleaseQuotaBytes = 160U;
inline constexpr std::size_t kExpectedReleaseFsInodes = 168U;
inline constexpr std::size_t kExpectedReleaseQuotaInodes = 176U;
inline constexpr std::size_t kReservedMarginBytes = 184U;
inline constexpr std::size_t kReservedMarginInodes = 192U;
inline constexpr std::size_t kEffectiveMinFsFreeBytes = 200U;
inline constexpr std::size_t kEffectiveMinQuotaFreeBytes = 208U;
inline constexpr std::size_t kEffectiveMinFsFreeInodes = 216U;
inline constexpr std::size_t kEffectiveMinQuotaFreeInodes = 224U;
inline constexpr std::size_t kActiveFsFreeInodeBaseline = 232U;
inline constexpr std::size_t kActiveQuotaFreeInodeBaseline = 240U;
inline constexpr std::size_t kSlotCrc32c = 248U;
inline constexpr std::size_t kReservedTail = 252U;
inline constexpr std::size_t kEntries = 2048U;
inline constexpr std::size_t kReceipts =
    kEntries +
    (kReserveStateV1EntryCapacity * kReserveStateV1EntryBytes);
inline constexpr std::size_t kPlans =
    kReceipts +
    (kReserveStateV1EntryCapacity *
     kReserveStateV1ActionCapacity *
     kReserveStateV1ReceiptBytes);
inline constexpr std::size_t kZeroTail =
    kPlans +
    (kReserveStateV1EntryCapacity *
     kReserveStateV1ActionCapacity *
     kReserveStateV1PlanBytes);
}  // namespace slot

namespace entry {
inline constexpr std::size_t kSourceStreamId = 0U;
inline constexpr std::size_t kCaptureDate = 4U;
inline constexpr std::size_t kStreamDayId = 8U;
inline constexpr std::size_t kStateTag0 = 24U;
inline constexpr std::size_t kStateTag1 = 25U;
inline constexpr std::size_t kStateTag2 = 26U;
inline constexpr std::size_t kStateTag3 = 27U;
inline constexpr std::size_t kReserved0 = 28U;
inline constexpr std::size_t kWriterInstance = 32U;
inline constexpr std::size_t kExecutorOrRecoveryAttempt = 48U;
inline constexpr std::size_t kTaggedPayload = 64U;
inline constexpr std::size_t kGrantBytes = 128U;
inline constexpr std::size_t kContinuationAllocationCap = 136U;
inline constexpr std::size_t kActivationFsFreeBaseline = 144U;
inline constexpr std::size_t kActivationQuotaFreeBaseline = 152U;
inline constexpr std::size_t kActivationRemainingCap = 160U;
inline constexpr std::size_t kPrechargedBytes = 168U;
inline constexpr std::size_t kMaintenanceReportSha256 = 176U;
inline constexpr std::size_t kEntryCrc32c = 208U;
inline constexpr std::size_t kSafeStopTemplateId = 212U;
inline constexpr std::size_t kReserved1 = 220U;
}  // namespace entry

namespace receipt {
inline constexpr std::size_t kActionId = 0U;
inline constexpr std::size_t kActionKind = 2U;
inline constexpr std::size_t kActionState = 3U;
inline constexpr std::size_t kByteCapQuanta = 4U;
inline constexpr std::size_t kInodeCap = 8U;
inline constexpr std::size_t kActionFlags = 12U;
inline constexpr std::size_t kDebitGeneration = 16U;
inline constexpr std::size_t kObjectPlanSha256 = 24U;
inline constexpr std::size_t kReceiptCrc32c = 56U;
inline constexpr std::size_t kReserved = 60U;
}  // namespace receipt

namespace plan {
inline constexpr std::size_t kPlanVersion = 0U;
inline constexpr std::size_t kObjectType = 1U;
inline constexpr std::size_t kPlanFlags = 2U;
inline constexpr std::size_t kObjectSequence = 4U;
inline constexpr std::size_t kRangeStart = 8U;
inline constexpr std::size_t kRangeEndOrSize = 16U;
inline constexpr std::size_t kCausalId = 24U;
}  // namespace plan

}  // namespace reserve_state_v1_offset

enum class ReserveCoordinatorPhaseV1 : std::uint8_t {
    kProvisioned = 1U,
    kReleasingIntent = 2U,
    kReleasingPrepared = 3U,
    kConsumed = 4U,
};

enum class ReserveReleaseReasonV1 : std::uint16_t {
    kNone = 0U,
    kLowWatermark = 1U,
    kWriterAllocationFailure = 2U,
    kOperatorRequested = 3U,
};

enum class ReserveReleaseTriggerV1 : std::uint16_t {
    kNone = 0U,
    kFilesystemBytes = 1U,
    kFilesystemInodes = 2U,
    kQuotaBytes = 3U,
    kQuotaInodes = 4U,
    kWriterEnospc = 5U,
    kWriterEdquot = 6U,
    kOperator = 7U,
};

enum class ReserveReleaseProbeMethodV1 : std::uint16_t {
    kFilesystemAndQuota = 1U,
};

enum class ReserveRegistryStatusV1 : std::uint8_t {
    kUnused = 0U,
    kScaffolding = 1U,
    kInit = 2U,
    kRecovering = 3U,
    kActive = 4U,
};

enum class ReserveRecoveryOriginV1 : std::uint8_t {
    kUnused = 0U,
    kFreshInit = 1U,
    kFreshInitTakeover = 2U,
    kAbsentRegistryExistingAnchor = 3U,
    kActiveTakeover = 4U,
};

enum class ReserveRecoveryIntentV1 : std::uint8_t {
    kNone = 0U,
    kResumeConnect = 1U,
    kRecoverSealOnly = 2U,
};

enum class ReserveAckStatusV1 : std::uint8_t {
    kUnused = 0U,
    kAcked = 1U,
    kFencedNoAck = 2U,
};

enum class ReserveGrantStatusV1 : std::uint8_t {
    kUnused = 0U,
    kPending = 1U,
    kActive = 2U,
    kDone = 3U,
    kFailed = 4U,
};

enum class FinalizationActionKindV1 : std::uint8_t {
    kUnused = 0U,
    kCurrentSegmentDrain = 1U,
    kJournalGrowth = 2U,
    kContinuation = 3U,
    kIndex = 4U,
    kManifest = 5U,
    kDirectoryLeaseScaffold = 6U,
    kJournalAnchor = 7U,
    kTypedTmpCleanup = 8U,
    kEmptyAnchorTombstone = 9U,
    kSealedRawCertificate = 10U,
    kPreexistingRecoveryReport = 11U,
    kFinalizationReport = 12U,
};

enum class FinalizationActionStateV1 : std::uint8_t {
    kUnused = 0U,
    kPending = 1U,
    kDebited = 2U,
    kComplete = 3U,
    kFailed = 4U,
};

inline constexpr std::uint8_t kReserveCounterCallbackRecords = 0x01U;
inline constexpr std::uint8_t kReserveCounterCallbackVendorBytes = 0x02U;
inline constexpr std::uint8_t kReserveCounterAppendPair = 0x04U;
inline constexpr std::uint8_t kReserveCounterDurablePair = 0x08U;
inline constexpr std::uint8_t kReserveCounterQueuedPair = 0x10U;
inline constexpr std::uint8_t kReserveCounterValidityMask = 0x1fU;

inline constexpr std::uint8_t kReserveGrantRawFinalization = 0x00U;
inline constexpr std::uint8_t kReserveGrantScaffoldingOnly = 0x01U;
inline constexpr std::uint8_t kReserveGrantRawAnchorOnly = 0x02U;
inline constexpr std::uint8_t kReserveGrantFlagsMask = 0x03U;

inline constexpr std::uint16_t kFinalizationPlanExistingFinal = 0x0001U;
inline constexpr std::uint16_t
    kFinalizationPlanIdenticalCompleteTmp = 0x0002U;
inline constexpr std::uint16_t
    kFinalizationPlanDurabilityAlreadyProven = 0x0004U;
inline constexpr std::uint16_t kFinalizationPlanCompleteTmpOnly = 0x0008U;
inline constexpr std::uint16_t
    kFinalizationPlanRecognizedPartialTmp = 0x0010U;
inline constexpr std::uint16_t
    kFinalizationPlanDeriveTerminalFrontier = 0x0020U;
inline constexpr std::uint16_t
    kFinalizationPlanReportResultResumedOpen = 0x0040U;
inline constexpr std::uint16_t
    kFinalizationPlanReportOriginWasActive = 0x0080U;
inline constexpr std::uint16_t kFinalizationPlanFlagsMask = 0x00ffU;
inline constexpr std::string_view
    kImmutableFinalizationGrantSha256DomainV1 =
        "L2FLOW_IMMUTABLE_FINALIZATION_GRANT_V1";

enum class ReserveStateV1Error : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidWireSize,
    kInvalidMagic,
    kUnsupportedVersion,
    kInvalidEndian,
    kInvalidSize,
    kSchemaMismatch,
    kInvalidIdentity,
    kUnknownEnum,
    kUnknownFlags,
    kNonzeroReserved,
    kCrcMismatch,
    kInvalidGeneration,
    kInvalidState,
    kInvalidCount,
    kInvalidOrdering,
    kDuplicateRoute,
    kArithmeticOverflow,
    kInvalidEntry,
    kInvalidReceipt,
    kInvalidPlan,
    kInvalidAggregate,
    kInvalidActivation,
    kInvalidTransition,
    kImmutableFactChanged,
    kSlotCorruptionFatal,
};

[[nodiscard]] std::string_view ReserveStateV1ErrorName(
    ReserveStateV1Error error) noexcept;

struct ReserveCoordinatorHeaderV1 final {
    ReserveStateV1Identity reserve_state_uuid{};
    ReserveStateV1Digest schema_sha256 = kReserveStateV1SchemaSha256;
    ReserveStateV1Digest quota_identity_sha256{};
    ReserveStateV1Digest mount_identity_sha256{};
    std::uint64_t device_id = 0U;
    std::uint64_t declared_releasable_bytes = 0U;
    std::uint64_t allocation_quantum_bytes = 0U;
    std::uint32_t declared_inode_reserve_count = 0U;
    ReserveReleaseProbeMethodV1 byte_probe_method =
        ReserveReleaseProbeMethodV1::kFilesystemAndQuota;
    std::uint16_t byte_probe_version = 0U;
    ReserveReleaseProbeMethodV1 inode_probe_method =
        ReserveReleaseProbeMethodV1::kFilesystemAndQuota;
    std::uint16_t inode_probe_version = 0U;
    ReserveStateV1Digest inode_inventory_sha256{};
    ReserveStateV1Digest safe_stop_catalog_sha256{};

    friend bool operator==(
        const ReserveCoordinatorHeaderV1&,
        const ReserveCoordinatorHeaderV1&) = default;
};

struct RawFinalizationCountersV1 final {
    std::uint64_t callback_published_records = 0U;
    std::uint64_t callback_published_vendor_bytes = 0U;
    std::uint64_t append_global_wal_pos = 0U;
    std::uint64_t append_ingress_sequence = 0U;
    std::uint64_t durable_global_wal_pos = 0U;
    std::uint64_t durable_ingress_sequence = 0U;
    std::uint64_t queued_record_count = 0U;
    std::uint64_t queued_framed_wal_bytes = 0U;

    friend bool operator==(
        const RawFinalizationCountersV1&,
        const RawFinalizationCountersV1&) = default;
};

struct ScaffoldingGrantPayloadV1 final {
    ReserveStateV1Identity recovery_attempt_id{};
    ReserveStateV1Digest object_snapshot_sha256{};
    std::uint64_t observed_object_bitmap = 0U;
    std::uint64_t required_action_bitmap = 0U;

    friend bool operator==(
        const ScaffoldingGrantPayloadV1&,
        const ScaffoldingGrantPayloadV1&) = default;
};

struct AnchorOnlyGrantPayloadV1 final {
    ReserveStateV1Identity recovery_attempt_id{};
    ReserveStateV1Digest journal_header_sha256{};
    ReserveRegistryStatusV1 origin_registry_status =
        ReserveRegistryStatusV1::kUnused;
    ReserveRecoveryOriginV1 recovery_origin =
        ReserveRecoveryOriginV1::kUnused;
    ReserveRecoveryIntentV1 original_recovery_intent =
        ReserveRecoveryIntentV1::kNone;
    ReserveRecoveryIntentV1 finalization_intent =
        ReserveRecoveryIntentV1::kNone;
    std::uint32_t recognized_tmp_bitmap = 0U;
    std::uint64_t required_action_bitmap = 0U;

    friend bool operator==(
        const AnchorOnlyGrantPayloadV1&,
        const AnchorOnlyGrantPayloadV1&) = default;
};

struct FinalizationActionReceiptV1 final {
    std::uint16_t action_id = 0U;
    FinalizationActionKindV1 action_kind =
        FinalizationActionKindV1::kUnused;
    FinalizationActionStateV1 action_state =
        FinalizationActionStateV1::kUnused;
    std::uint32_t byte_cap_quanta = 0U;
    std::uint32_t inode_cap = 0U;
    std::uint32_t action_flags = 0U;
    std::uint64_t debit_generation = 0U;
    ReserveStateV1Digest object_plan_sha256{};

    friend bool operator==(
        const FinalizationActionReceiptV1&,
        const FinalizationActionReceiptV1&) = default;
};

struct FinalizationActionPlanV1 final {
    std::uint8_t plan_version = 0U;
    FinalizationActionKindV1 object_type =
        FinalizationActionKindV1::kUnused;
    std::uint16_t plan_flags = 0U;
    std::uint32_t object_sequence = 0U;
    std::uint64_t range_start = 0U;
    std::uint64_t range_end_or_size = 0U;
    ReserveStateV1Identity causal_id{};

    friend bool operator==(
        const FinalizationActionPlanV1&,
        const FinalizationActionPlanV1&) = default;
};

struct ReserveStateEntryV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    ReserveStateV1Identity stream_day_id{};

    // Registry interpretation (PROVISIONED/RELEASING_INTENT).
    ReserveRegistryStatusV1 registry_status =
        ReserveRegistryStatusV1::kUnused;
    ReserveRecoveryOriginV1 recovery_origin =
        ReserveRecoveryOriginV1::kUnused;
    ReserveRecoveryIntentV1 recovery_intent =
        ReserveRecoveryIntentV1::kNone;

    // Grant interpretation (RELEASING_PREPARED/CONSUMED).
    ReserveAckStatusV1 ack_status = ReserveAckStatusV1::kUnused;
    std::uint8_t counter_validity = 0U;
    ReserveGrantStatusV1 grant_status =
        ReserveGrantStatusV1::kUnused;
    std::uint8_t grant_flags = 0U;

    // Current writer in registry states, ACK writer in grant states.
    ReserveStateV1Identity writer_instance{};
    // Recovery attempt in registry states, executor in grant states.
    ReserveStateV1Identity executor_or_recovery_attempt{};

    RawFinalizationCountersV1 raw_counters{};
    ScaffoldingGrantPayloadV1 scaffolding_payload{};
    AnchorOnlyGrantPayloadV1 anchor_only_payload{};

    // In SCAFFOLDING registry state, grant_bytes is the bounded scaffolding
    // allocation cap.  In grant states it has its normal grant meaning.
    std::uint64_t grant_bytes = 0U;
    std::uint64_t continuation_allocation_cap = 0U;
    std::uint64_t activation_fs_free_baseline = 0U;
    std::uint64_t activation_quota_free_baseline = 0U;
    std::uint64_t activation_remaining_cap = 0U;
    std::uint64_t precharged_bytes = 0U;
    ReserveStateV1Digest maintenance_report_sha256{};
    std::uint64_t safe_stop_template_id = 0U;

    std::array<
        FinalizationActionReceiptV1,
        kReserveStateV1ActionCapacity>
        actions{};
    std::array<
        FinalizationActionPlanV1,
        kReserveStateV1ActionCapacity>
        plans{};

    friend bool operator==(
        const ReserveStateEntryV1&,
        const ReserveStateEntryV1&) = default;
};

struct ReserveStateSlotV1 final {
    ReserveCoordinatorPhaseV1 coordinator_state =
        ReserveCoordinatorPhaseV1::kProvisioned;
    std::uint64_t generation = 0U;
    ReserveStateV1Identity reserve_state_uuid{};
    ReserveReleaseReasonV1 reason = ReserveReleaseReasonV1::kNone;
    ReserveReleaseTriggerV1 trigger = ReserveReleaseTriggerV1::kNone;
    std::uint16_t entry_count = 0U;
    std::uint16_t active_entry_index = kReserveStateV1NoActiveEntry;
    std::uint16_t completed_bitmap = 0U;
    ReserveStateV1Identity finalization_cycle_id{};
    ReserveStateV1Digest writer_set_sha256{};
    std::uint64_t aggregate_grant_bytes = 0U;
    std::uint64_t aggregate_inode_grant = 0U;
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
    std::uint64_t active_fs_free_inode_baseline = 0U;
    std::uint64_t active_quota_free_inode_baseline = 0U;
    std::array<ReserveStateEntryV1, kReserveStateV1EntryCapacity>
        entries{};

    friend bool operator==(
        const ReserveStateSlotV1&,
        const ReserveStateSlotV1&) = default;
};

struct ReserveCoordinatorStateV1 final {
    ReserveCoordinatorHeaderV1 header{};
    std::array<ReserveStateSlotV1, 2U> slots{};
    std::size_t selected_slot = 0U;

    friend bool operator==(
        const ReserveCoordinatorStateV1&,
        const ReserveCoordinatorStateV1&) = default;
};

[[nodiscard]] ReserveStateV1Error EncodeReserveCoordinatorHeaderV1(
    const ReserveCoordinatorHeaderV1& header,
    ReserveStateV1HeaderWire* output) noexcept;

[[nodiscard]] ReserveStateV1Error DecodeReserveCoordinatorHeaderV1(
    std::span<const std::byte> wire,
    ReserveCoordinatorHeaderV1* output) noexcept;

[[nodiscard]] ReserveStateV1Error EncodeReserveStateSlotV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& slot,
    ReserveStateV1SlotWire* output) noexcept;

[[nodiscard]] ReserveStateV1Error DecodeReserveStateSlotV1(
    const ReserveCoordinatorHeaderV1& header,
    std::span<const std::byte> wire,
    ReserveStateSlotV1* output) noexcept;

[[nodiscard]] ReserveStateV1Error EncodeReserveCoordinatorStateV1(
    const ReserveCoordinatorStateV1& state,
    ReserveStateV1FileWire* output) noexcept;

// Both published slots must independently pass CRC/schema/reserved/state
// validation.  There is deliberately no fallback to an older valid slot.
[[nodiscard]] ReserveStateV1Error DecodeAndSelectReserveCoordinatorStateV1(
    std::span<const std::byte> wire,
    ReserveCoordinatorStateV1* output) noexcept;

// Validates the two consecutive logical generations, including the unique
// generation=1 byte-identical bootstrap and immutable PREPARED/CONSUMED grant
// facts.  before_wire/after_wire are used only for bootstrap byte identity.
[[nodiscard]] ReserveStateV1Error ValidateReserveStateSlotPairV1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& before,
    const ReserveStateV1SlotWire& before_wire,
    const ReserveStateSlotV1& after,
    const ReserveStateV1SlotWire& after_wire) noexcept;

[[nodiscard]] ReserveStateV1Error
ValidateImmutableFinalizationFactsV1(
    const ReserveStateSlotV1& before,
    const ReserveStateSlotV1& after) noexcept;

// Computes the frozen immutable-grant hash domain from one fully valid
// RELEASING_PREPARED/CONSUMED entry.  Mutable receipt state, debit
// generations, executor identity, activation baselines/remaining,
// precharge, report digest and slot generation are intentionally excluded.
// The output is unchanged on every failure.
[[nodiscard]] ReserveStateV1Error
ComputeImmutableFinalizationGrantSha256V1(
    const ReserveCoordinatorHeaderV1& header,
    const ReserveStateSlotV1& slot,
    std::size_t entry_index,
    ReserveStateV1Digest* output) noexcept;

}  // namespace l2flow::ingress
