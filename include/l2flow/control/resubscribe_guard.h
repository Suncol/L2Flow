#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/control/control_decoder.h"

#include <cstdint>

namespace l2flow::control {

inline constexpr std::uint32_t
    kResubscribeMaintenanceWindowSchemaVersionV1 = 1U;
inline constexpr std::uint32_t
    kDurableResubscribeAuditSchemaVersionV1 = 1U;

enum class ResubscribeOperationV1 : std::uint8_t {
    kReplaceRequestedManifest = 1U,
};

struct ResubscribeMaintenanceWindowV1 final {
    std::uint32_t schema_version = 0U;
    bool explicitly_outside_trading_interval = false;
};

struct DurableResubscribeAuditReceiptV1 final {
    std::uint32_t schema_version = 0U;
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    l2flow::common::Identity128 stream_day_id{};
    l2flow::common::Sha256Digest old_manifest_sha256{};
    l2flow::common::Sha256Digest proposed_manifest_sha256{};
    l2flow::common::Sha256Digest actor_sha256{};
    l2flow::common::Sha256Digest reason_sha256{};
    l2flow::common::Sha256Digest audit_record_sha256{};
    ResubscribeOperationV1 operation =
        ResubscribeOperationV1::kReplaceRequestedManifest;
    bool file_barrier_complete = false;
    bool actual_parent_directory_barrier_complete = false;
};

struct ResubscribeAuthorizationV1 final {
    bool reviewed_operational_authorization = false;
};

enum class ResubscribeGuardResultV1 : std::uint8_t {
    kAllowed = 0U,
    kControlPoisoned,
    kNotLoggedIn,
    kMaintenanceWindowMissing,
    kTradingIntervalNotExcluded,
    kAuditReceiptMissing,
    kAuditNamespaceMismatch,
    kAuditManifestMismatch,
    kAuditIdentityMissing,
    kAuditBarrierIncomplete,
    kAuthorizationMissing,
    kManifestUnchanged,
    kUnsupportedSchemaVersion,
    // V1 has no ordered Raw input carrying replacement keys and policies.
    // Authorizing an SDK call would make full Raw replay diverge from live
    // state, so replacement remains structurally unavailable.
    kNoReplayableManifestTransition,
};

// Pure fail-closed policy. Phase-3 V1 deliberately never returns kAllowed:
// a replacement manifest is not represented by an authoritative ordered Raw
// input yet, so allowing the SDK call would make replay nondeterministic.
[[nodiscard]] ResubscribeGuardResultV1 AuthorizeResubscribeV1(
    const ControlDecoderSnapshotV1& state,
    const l2flow::common::Sha256Digest& proposed_manifest_sha256,
    const ResubscribeMaintenanceWindowV1& maintenance_window,
    const DurableResubscribeAuditReceiptV1& audit,
    const ResubscribeAuthorizationV1& authorization) noexcept;

}  // namespace l2flow::control
