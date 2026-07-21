#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/control/control_record_v1.h"
#include "l2flow/ingress/raw_control_page.h"
#include "l2flow/ingress/raw_live_tail.h"
#include "l2flow/ingress/raw_reader.h"
#include "l2flow/ingress/raw_replay.h"
#include "l2flow/sdk/subscription_manifest.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace l2flow::control {

enum class SubscriptionPolicyV1 : std::uint8_t {
    kRequired = phase3_schema_v1::
        subscription_policy_v1_value::kRequired,
    kOptional = phase3_schema_v1::
        subscription_policy_v1_value::kOptional,
};

struct ControlSubscriptionStateV1 final {
    l2flow::sdk::MessageKey key{};
    SubscriptionPolicyV1 policy =
        SubscriptionPolicyV1::kRequired;
    std::uint32_t status = 0U;
    bool status_known = false;

    [[nodiscard]] friend constexpr bool operator==(
        const ControlSubscriptionStateV1&,
        const ControlSubscriptionStateV1&) noexcept = default;
};

enum class ControlSessionPhaseV1 : std::uint8_t {
    kInitial = phase3_schema_v1::
        control_session_phase_v1_value::kInitial,
    kConnecting = phase3_schema_v1::
        control_session_phase_v1_value::kConnecting,
    kLoggedIn = phase3_schema_v1::
        control_session_phase_v1_value::kLoggedIn,
    kConnectError = phase3_schema_v1::
        control_session_phase_v1_value::kConnectError,
    kDisconnected = phase3_schema_v1::
        control_session_phase_v1_value::kDisconnected,
    kLogonFailed = phase3_schema_v1::
        control_session_phase_v1_value::kLogonFailed,
    kPoisoned = phase3_schema_v1::
        control_session_phase_v1_value::kPoisoned,
};

struct ControlDecoderConfigV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    l2flow::common::Identity128 stream_day_id{};
    l2flow::common::Sha256Digest stable_config_sha256{};
    std::vector<l2flow::sdk::MessageKey> required;
    std::vector<l2flow::sdk::MessageKey> optional;
};

struct ControlDecoderCountersV1 final {
    std::uint64_t processed_records = 0U;
    std::uint64_t emitted_control_records = 0U;
    std::uint64_t logon_success = 0U;
    std::uint64_t logon_failure = 0U;
    std::uint64_t disconnect = 0U;
    std::uint64_t subscription_responses = 0U;
    std::uint64_t control_decode_errors = 0U;

    [[nodiscard]] friend constexpr bool operator==(
        const ControlDecoderCountersV1&,
        const ControlDecoderCountersV1&) noexcept = default;
};

struct ControlDecoderSnapshotV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    l2flow::common::Identity128 stream_day_id{};
    l2flow::common::Sha256Digest stable_config_sha256{};
    l2flow::common::Sha256Digest requested_manifest_sha256{};
    l2flow::common::Sha256Digest response_manifest_sha256{};
    l2flow::common::Sha256Digest current_address_sha256{};
    l2flow::common::Sha256Digest last_success_address_sha256{};
    l2flow::common::Sha256Digest state_sha256{};
    std::vector<ControlSubscriptionStateV1> subscriptions;
    ControlDecoderCountersV1 counters{};
    ControlSessionPhaseV1 session_phase =
        ControlSessionPhaseV1::kInitial;
    std::uint32_t connection_epoch = 0U;
    std::uint32_t subscription_epoch = 0U;
    std::uint64_t next_ingress_sequence = 1U;
    std::uint64_t processed_ingress_sequence = 0U;
    std::uint64_t processed_record_start_wal_pos = 0U;
    std::uint64_t processed_record_end_wal_pos = 0U;
    std::uint64_t latest_response_ingress_sequence = 0U;
    std::uint64_t latest_response_record_end_wal_pos = 0U;
    std::uint64_t quality_flags = 0U;
    std::uint64_t required_first_seen_mask = 0U;
    std::uint64_t effective_success_mask = 0U;
    bool logged_in = false;
    bool disconnected_window = false;
    bool connection_switched = false;
    bool poisoned = false;
    bool control_ready = false;
    // Decoder-local evidence only. Production READY additionally requires a
    // newly sampled coherent Raw append frontier, writer/decoder heartbeats,
    // fatal checks and catch-up in ControlReadinessGateV1.
    bool decoder_evidence_ready = false;
};

struct ControlRecordAttributionV1 final {
    std::uint32_t connection_epoch = 0U;
    std::uint32_t subscription_epoch = 0U;
    std::uint64_t quality_flags = 0U;
};

enum class ControlProcessErrorV1 : std::uint16_t {
    kNone = 0U,
    kNullOutput,
    kNamespaceMismatch,
    kIngressSequenceMismatch,
    kWalCursorRegression,
    kLiveSegmentMismatch,
    kReplaySegmentMismatch,
    kMalformedApiControl,
    kMalformedSysControl,
    kCounterOverflow,
    kStateHashFailure,
    kResourceExhausted,
};

struct ControlProcessResultV1 final {
    ControlProcessErrorV1 error = ControlProcessErrorV1::kNone;
    ControlRecordAttributionV1 attribution{};
    std::optional<ControlRecordV1> control_record;
    bool cursor_committed = false;
    bool control_state_poisoned = false;

    [[nodiscard]] bool ok() const noexcept {
        return error == ControlProcessErrorV1::kNone;
    }
};

inline constexpr std::uint32_t kControlDecoderCheckpointVersionV1 = 1U;

// Logical checkpoint model. The binary checkpoint codec is defined separately
// and binds this state to its namespace, cursor, stable config and state hash.
struct ControlDecoderCheckpointV1 final {
    std::uint32_t schema_version =
        kControlDecoderCheckpointVersionV1;
    ControlDecoderSnapshotV1 state{};
};

enum class ControlDecoderCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfig,
    kInvalidCheckpoint,
    kCheckpointConfigMismatch,
    kCheckpointStateHashMismatch,
    kResourceExhausted,
};

class ControlDecoderV1 final {
public:
    ControlDecoderV1(const ControlDecoderV1&) = delete;
    ControlDecoderV1& operator=(const ControlDecoderV1&) = delete;
    ControlDecoderV1(ControlDecoderV1&&) = delete;
    ControlDecoderV1& operator=(ControlDecoderV1&&) = delete;
    ~ControlDecoderV1();

    [[nodiscard]] static ControlDecoderCreateErrorV1 Create(
        ControlDecoderConfigV1 config,
        std::unique_ptr<ControlDecoderV1>* output) noexcept;
    [[nodiscard]] static ControlDecoderCreateErrorV1 Restore(
        ControlDecoderConfigV1 config,
        const ControlDecoderCheckpointV1& checkpoint,
        const l2flow::ingress::RawReplayRecord& boundary_record,
        const l2flow::ingress::RawControlSnapshot& durable_frontier,
        std::unique_ptr<ControlDecoderV1>* output) noexcept;

    // Both live and replay call this exact transition function. A malformed
    // control body commits only the audit cursor and sticky poison; Raw capture
    // remains outside this failure domain.
    [[nodiscard]] ControlProcessResultV1 Process(
        const l2flow::ingress::RawLiveRecord& record) noexcept;
    [[nodiscard]] ControlProcessResultV1 Process(
        const l2flow::ingress::RawReplayRecord& record) noexcept;

    [[nodiscard]] ControlDecoderSnapshotV1 Snapshot() const;
    [[nodiscard]] ControlDecoderCheckpointV1 Checkpoint() const;

private:
    class Impl;
    explicit ControlDecoderV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] l2flow::common::Sha256Digest
ComputeRequestedSubscriptionManifestSha256V1(
    std::span<const ControlSubscriptionStateV1>
        sorted_subscriptions) noexcept;

// Recomputes the canonical, domain-separated identity of a complete decoder
// snapshot. The stored state_sha256 field is intentionally not part of its
// own hash domain. Checkpoint validation and decoder transitions share this
// function so a structurally valid checkpoint cannot carry an unrelated
// state digest.
[[nodiscard]] bool ComputeControlDecoderStateSha256V1(
    const ControlDecoderSnapshotV1& state,
    l2flow::common::Sha256Digest* digest) noexcept;

}  // namespace l2flow::control
