#include "l2flow/control/resubscribe_guard.h"

#include "l2flow/common/identity128.h"

#include <algorithm>
#include <cstddef>

namespace l2flow::control {
namespace {

bool DigestIsZero(
    const l2flow::common::Sha256Digest& digest) noexcept {
    return std::all_of(
        digest.begin(), digest.end(), [](std::byte value) {
            return value == std::byte{0};
        });
}

}  // namespace

ResubscribeGuardResultV1 AuthorizeResubscribeV1(
    const ControlDecoderSnapshotV1& state,
    const l2flow::common::Sha256Digest& proposed_manifest_sha256,
    const ResubscribeMaintenanceWindowV1& maintenance_window,
    const DurableResubscribeAuditReceiptV1& audit,
    const ResubscribeAuthorizationV1& authorization) noexcept {
    if (state.poisoned) {
        return ResubscribeGuardResultV1::kControlPoisoned;
    }
    if (!state.logged_in ||
        state.session_phase != ControlSessionPhaseV1::kLoggedIn) {
        return ResubscribeGuardResultV1::kNotLoggedIn;
    }
    if (maintenance_window.schema_version == 0U) {
        return ResubscribeGuardResultV1::kMaintenanceWindowMissing;
    }
    if (maintenance_window.schema_version !=
        kResubscribeMaintenanceWindowSchemaVersionV1) {
        return ResubscribeGuardResultV1::kUnsupportedSchemaVersion;
    }
    if (!maintenance_window.explicitly_outside_trading_interval) {
        return ResubscribeGuardResultV1::
            kTradingIntervalNotExcluded;
    }
    if (audit.schema_version == 0U) {
        return ResubscribeGuardResultV1::kAuditReceiptMissing;
    }
    if (audit.schema_version !=
        kDurableResubscribeAuditSchemaVersionV1) {
        return ResubscribeGuardResultV1::kUnsupportedSchemaVersion;
    }
    if (audit.source_stream_id != state.source_stream_id ||
        audit.capture_date != state.capture_date ||
        audit.stream_day_id != state.stream_day_id) {
        return ResubscribeGuardResultV1::kAuditNamespaceMismatch;
    }
    if (audit.old_manifest_sha256 !=
            state.requested_manifest_sha256 ||
        audit.proposed_manifest_sha256 !=
            proposed_manifest_sha256 ||
        audit.operation !=
            ResubscribeOperationV1::kReplaceRequestedManifest) {
        return ResubscribeGuardResultV1::kAuditManifestMismatch;
    }
    if (DigestIsZero(audit.actor_sha256) ||
        DigestIsZero(audit.reason_sha256) ||
        DigestIsZero(audit.audit_record_sha256) ||
        DigestIsZero(proposed_manifest_sha256)) {
        return ResubscribeGuardResultV1::kAuditIdentityMissing;
    }
    if (!audit.file_barrier_complete ||
        !audit.actual_parent_directory_barrier_complete) {
        return ResubscribeGuardResultV1::kAuditBarrierIncomplete;
    }
    if (!authorization.reviewed_operational_authorization) {
        return ResubscribeGuardResultV1::kAuthorizationMissing;
    }
    if (proposed_manifest_sha256 ==
        state.requested_manifest_sha256) {
        return ResubscribeGuardResultV1::kManifestUnchanged;
    }
    return ResubscribeGuardResultV1::
        kNoReplayableManifestTransition;
}

}  // namespace l2flow::control
