#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/control/control_decoder.h"
#include "l2flow/ingress/raw_control_page.h"

#include <cstdint>
#include <optional>

namespace l2flow::control {

inline constexpr std::uint32_t kMarketSilenceProofSchemaVersionV1 = 1U;

// A calendar subsystem may supply this explicit, namespace-bound proof when
// the current market phase is one in which required records are not expected.
// A bare wall-clock guess or boolean is intentionally insufficient.
struct MarketSilenceProofV1 final {
    std::uint32_t schema_version = 0U;
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    l2flow::common::Identity128 stream_day_id{};
    l2flow::common::Sha256Digest calendar_sha256{};
    std::uint64_t valid_from_realtime_ns = 0U;
    std::uint64_t valid_until_realtime_ns = 0U;
    std::uint64_t evaluated_realtime_ns = 0U;
    bool required_market_records_not_expected = false;
};

struct ControlReadinessGateConfigV1 final {
    std::uint64_t maximum_decoder_lag_bytes = 0U;
    std::uint64_t maximum_durability_lag_bytes = 0U;
    std::uint64_t heartbeat_timeout_ns = 0U;
    // Zero disables the calendar exception. A supplied proof must bind to
    // this exact reviewed calendar/configuration identity.
    l2flow::common::Sha256Digest approved_calendar_sha256{};
};

// Volatile worker evidence. A worker must set healthy=false after any non-ok
// Process() result that did not deliberately commit a malformed-control
// diagnostic transition.
struct ControlReadinessRuntimeV1 final {
    l2flow::common::Identity128 writer_instance{};
    // Includes validated segment-header transitions, which are not Raw
    // records and therefore do not change decoder state or its state hash.
    std::uint64_t processed_wal_pos = 0U;
    std::uint64_t processed_ingress_sequence = 0U;
    std::uint64_t decoder_heartbeat_monotonic_ns = 0U;
    // The owner increments connect_generation before each SDK Connect
    // generation and records a matching successful generation only after the
    // decoder commits that generation's LogonSuccess control record. The
    // first-ingress boundary prevents checkpoint/replay evidence from an old
    // process generation from satisfying a new live generation.
    std::uint64_t connect_generation = 0U;
    std::uint64_t generation_first_ingress_sequence = 0U;
    std::uint64_t successful_logon_connect_generation = 0U;
    std::uint64_t successful_logon_ingress_sequence = 0U;
    // Required only when a market-silence proof is used.
    std::uint64_t sampled_realtime_ns = 0U;
    bool decoder_healthy = false;
    bool capture_pipeline_healthy = false;
};

enum class ControlReadinessReasonV1 : std::uint8_t {
    kReady = 0U,
    kInvalidConfig,
    // The live worker could not obtain a new coherent Raw control sample.
    // No prior sample may be reused for this decision.
    kRawControlSampleUnavailable,
    kWriterInstanceMismatch,
    kNamespaceMismatch,
    kRawFrontierInvalid,
    kDecoderCursorInvalid,
    kWriterFatal,
    kCapturePipelineUnhealthy,
    kWriterHeartbeatMissing,
    kWriterHeartbeatClockRegression,
    kWriterHeartbeatTimedOut,
    kDecoderHeartbeatMissing,
    kDecoderHeartbeatClockRegression,
    kDecoderHeartbeatTimedOut,
    kDecoderUnhealthy,
    kLiveGenerationInvalid,
    kCurrentGenerationLogonMissing,
    kControlPoisoned,
    kControlEvidenceIncomplete,
    kMarketEvidenceIncomplete,
    kCalendarProofInvalid,
    kDurabilityLagExceeded,
    kDecoderLagExceeded,
    kDecoderNotCaughtUp,
};

struct ControlReadinessResultV1 final {
    ControlReadinessReasonV1 reason =
        ControlReadinessReasonV1::kInvalidConfig;
    std::uint32_t connection_epoch = 0U;
    std::uint32_t subscription_epoch = 0U;
    std::uint64_t sampled_append_wal_pos = 0U;
    std::uint64_t decoder_processed_wal_pos = 0U;
    std::uint64_t decoder_lag_bytes = 0U;
    std::uint64_t durability_lag_bytes = 0U;
    std::uint64_t writer_heartbeat_age_ns = 0U;
    std::uint64_t decoder_heartbeat_age_ns = 0U;
    bool calendar_exception_used = false;
    bool ready = false;
};

// Evaluates every call against the caller's newly acquired coherent Raw
// control-page snapshot. It never caches READY and permits processed>sampled
// because the decoder may advance after that snapshot was acquired.
[[nodiscard]] ControlReadinessResultV1 EvaluateControlReadinessV1(
    const ControlReadinessGateConfigV1& config,
    const ControlDecoderSnapshotV1& decoder,
    const ControlReadinessRuntimeV1& runtime,
    const l2flow::ingress::RawControlSnapshot& sampled_raw,
    const std::optional<MarketSilenceProofV1>& calendar_proof,
    std::uint64_t now_monotonic_ns) noexcept;

}  // namespace l2flow::control
