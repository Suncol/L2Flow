#pragma once

#include "l2flow/ingress/reserve_emergency_transition_v1.h"

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace l2flow::ingress {

// This planner is deliberately state-first and mutation-free. A normal route
// discovery result is only permission to enter the recovery/discovery
// pipeline; no result from this API is permission to call SDK Connect.
enum class RawPhase2StartupDispositionV1 : std::uint8_t {
    kP0 = 0U,
    kNormalRouteDiscovery,
    kResumeReleasingIntent,
    kResumeReleasingPrepared,
    kActivateGrant,
    kResumeDebited,
    kDebitNext,
    kValidateReportAndDone,
    kArchiveReprovision,
};

enum class RawPhase2StartupP0ReasonV1 : std::uint8_t {
    kNone = 0U,
    kCodecRejected,
    kCanonicalRoundTripMismatch,
    kInvalidConsumedTopology,
    kDurableGrantFailed,
    kDurableActionFailed,
    kImmutableGrantHashRejected,
};

enum class RawPhase2StartupPlanErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kStateEncodeRejected,
    kStateDecodeRejected,
    kCanonicalRoundTripMismatch,
    kInvalidConsumedTopology,
    kImmutableGrantHashRejected,
};

[[nodiscard]] std::string_view
RawPhase2StartupDispositionV1Name(
    RawPhase2StartupDispositionV1 value) noexcept;
[[nodiscard]] std::string_view
RawPhase2StartupPlanErrorV1Name(
    RawPhase2StartupPlanErrorV1 value) noexcept;

struct RawPhase2StartupPlanV1 final {
    RawPhase2StartupDispositionV1 disposition =
        RawPhase2StartupDispositionV1::kP0;
    RawPhase2StartupP0ReasonV1 p0_reason =
        RawPhase2StartupP0ReasonV1::kNone;

    // Always false. A later ACTIVE runtime/WAL/live-tail/observer gate owns
    // the only transition that can authorize SDK Connect.
    bool connect_allowed = false;
    bool normal_route_pipeline_allowed = false;

    // False on every codec/canonicalization failure. In that case the
    // default durable_phase value is only zero-initialized storage and must
    // not be interpreted as a verified PROVISIONED observation.
    bool durable_state_verified = false;
    ReserveCoordinatorPhaseV1 durable_phase =
        ReserveCoordinatorPhaseV1::kProvisioned;
    std::size_t selected_slot = 0U;
    std::uint64_t generation = 0U;
    ReserveStateV1Identity reserve_state_uuid{};
    ReserveStateV1Identity finalization_cycle_id{};

    bool has_grant = false;
    // Exact selector accepted by the existing reserve transition API.
    ReserveFinalizationGrantKeyV1 grant{};
    // Audit evidence only; the transition selector deliberately does not
    // trust the mutable array position as grant identity.
    std::uint16_t grant_entry_index = 0U;
    std::uint8_t grant_flags = 0U;
    ReserveStateV1Digest immutable_grant_sha256{};
    ReserveGrantStatusV1 grant_status =
        ReserveGrantStatusV1::kUnused;
    ReserveStateV1Identity executor_instance{};

    bool has_action = false;
    // Exact selector accepted by the existing reserve transition API.
    ReserveFinalizationActionKeyV1 action{};
    // Mutable receipt evidence is kept outside the immutable action key.
    FinalizationActionStateV1 action_state =
        FinalizationActionStateV1::kUnused;
    std::uint64_t action_debit_generation = 0U;

    // Preserves the frozen codec's exact reason when encode/decode rejects
    // the caller-provided object. It is kNone for a valid durable FAILED
    // state whose operational disposition is nevertheless kP0.
    ReserveStateV1Error codec_error =
        ReserveStateV1Error::kNone;

    friend bool operator==(
        const RawPhase2StartupPlanV1&,
        const RawPhase2StartupPlanV1&) = default;
};

// Treats input as untrusted structured data. It first serializes through the
// frozen little-endian codec, then decodes/selects both slots and verifies a
// byte-identical canonical re-encode. Decisions are made only from that
// decoded selected slot. This function performs no filesystem or coordinator
// mutation. In particular, kArchiveReprovision only classifies an all-DONE
// state for the separately locked, offline maintenance workflow; it does not
// authorize archive publication or reserve provisioning.
[[nodiscard]] RawPhase2StartupPlanErrorV1
BuildRawPhase2StartupPlanV1(
    const ReserveCoordinatorStateV1& untrusted_state,
    RawPhase2StartupPlanV1* output) noexcept;

}  // namespace l2flow::ingress
