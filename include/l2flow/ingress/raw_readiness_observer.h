#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/ingress/raw_control_page.h"
#include "l2flow/ingress/raw_live_tail.h"
#include "l2flow/ingress/raw_reader.h"
#include "l2flow/ingress/raw_replay.h"
#include "l2flow/sdk/subscription_manifest.h"

#include <cstdint>
#include <memory>
#include <vector>

namespace l2flow::ingress {

// Phase-2's temporary readiness observer. It consumes only records and
// segment-header transition facts that have already passed Raw V1
// framing/CRC/namespace/continuity validation; it is never a ByteRing
// consumer and it does not create an authoritative connection epoch.
struct RawReadinessObserverConfig final {
    std::uint32_t source_stream_id = 0U;
    std::uint8_t market_service_id = 0U;
    std::vector<l2flow::sdk::MessageKey> required_market_messages;
    std::uint64_t maximum_lag_bytes = 0U;
    std::uint64_t heartbeat_timeout_ns = 0U;
};

// One observer generation is bound to both the Raw writer instance and the
// SDK Connect generation. The recovery WAL/segment cursor is the exact
// exclusive Raw frontier returned by recovery; evidence before it is
// intentionally ineligible.
struct RawReadinessObserverGeneration final {
    l2flow::common::Identity128 writer_instance{};
    l2flow::common::Identity128 stream_day_id{};
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    std::uint64_t connect_generation = 0U;
    std::uint64_t recovery_wal_pos = 0U;
    std::uint64_t recovery_next_ingress_sequence = 0U;
    std::uint32_t recovery_segment_sequence = 0U;
    std::uint64_t recovery_segment_offset = 0U;
};

enum class RawReadinessObserverCreateError : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfig,
    kResourceExhausted,
};

enum class RawReadinessObserveResult : std::uint8_t {
    kProcessed = 0U,
    kNoGeneration,
    kWriterInstanceMismatch,
    kNamespaceMismatch,
    kWalCursorMismatch,
    kIngressSequenceMismatch,
    kSegmentTransitionMismatch,
    kClockRegression,
    kMalformedControl,
    kIngressSequenceExhausted,
};

struct RawReadinessObserverSnapshot final {
    RawReadinessObserverGeneration generation{};
    std::uint64_t observer_processed_wal_pos = 0U;
    std::uint64_t observer_processed_ingress_sequence = 0U;
    std::uint32_t observer_processed_segment_sequence = 0U;
    std::uint64_t observer_processed_segment_offset = 0U;
    std::uint64_t observer_heartbeat_monotonic_ns = 0U;
    std::uint64_t logon_generation = 0U;
    std::uint64_t required_market_mask = 0U;
    std::uint64_t required_first_seen_mask = 0U;
    std::uint64_t required_subscription_ok_mask = 0U;
    std::uint64_t required_subscription_failed_mask = 0U;
    bool generation_active = false;
    bool observer_healthy = false;
    bool latest_logon_ok = false;
    // This is evidence completeness only. A reader must still call
    // Evaluate() with a newly acquired append snapshot before reporting READY.
    bool evidence_complete = false;
    bool authoritative_epoch = false;
};

enum class RawObservationalGateReason : std::uint8_t {
    kReady = 0U,
    kNoGeneration,
    kObserverUnhealthy,
    kEvidenceIncomplete,
    kWriterInstanceMismatch,
    kNamespaceMismatch,
    kWriterFatal,
    kWriterHeartbeatMissing,
    kWriterHeartbeatClockRegression,
    kWriterHeartbeatTimedOut,
    kAppendBeforeRecovery,
    kLagExceeded,
    kNotCaughtUp,
    kHeartbeatMissing,
    kHeartbeatClockRegression,
    kHeartbeatTimedOut,
};

struct RawObservationalGateResult final {
    RawObservationalGateReason reason =
        RawObservationalGateReason::kNoGeneration;
    std::uint64_t connect_generation = 0U;
    std::uint64_t logon_generation = 0U;
    std::uint64_t sampled_append_wal_pos = 0U;
    std::uint64_t observer_processed_wal_pos = 0U;
    std::uint64_t lag_bytes = 0U;
    std::uint64_t heartbeat_age_ns = 0U;
    bool ready = false;
    // Frozen false for this Phase-2 compatibility observer. Phase 3's control
    // decoder owns authoritative epoch semantics.
    bool authoritative_epoch = false;
};

class RawReadinessObserver final {
public:
    RawReadinessObserver(const RawReadinessObserver&) = delete;
    RawReadinessObserver& operator=(const RawReadinessObserver&) = delete;
    RawReadinessObserver(RawReadinessObserver&&) = delete;
    RawReadinessObserver& operator=(RawReadinessObserver&&) = delete;
    ~RawReadinessObserver();

    [[nodiscard]] static RawReadinessObserverCreateError Create(
        RawReadinessObserverConfig config,
        std::unique_ptr<RawReadinessObserver>* output);

    // Starts a fresh NOT_READY generation. Reusing the same
    // writer-instance/Connect pair, or moving a Connect generation backwards
    // within one writer instance, is rejected.
    [[nodiscard]] bool BeginGeneration(
        const RawReadinessObserverGeneration& generation,
        std::uint64_t now_monotonic_ns);

    // The caller supplies the writer instance attached by the live-tail gate.
    // Records must begin exactly at observer_processed_wal_pos and carry the
    // next ingress sequence; no recovered historical record can be skipped
    // into the current generation.
    [[nodiscard]] RawReadinessObserveResult Observe(
        const RawRecordView& record,
        const l2flow::common::Identity128& writer_instance,
        std::uint64_t now_monotonic_ns);

    [[nodiscard]] RawReadinessObserveResult Observe(
        const RawReplayRecord& record,
        const l2flow::common::Identity128& writer_instance,
        std::uint64_t now_monotonic_ns);

    // Advances only across an explicit transition emitted by RawLiveTail
    // after it validated both segment headers, the sealed old logical end,
    // sequence/base continuity, namespace, and unchanged ingress frontier.
    [[nodiscard]] RawReadinessObserveResult
    ObserveSegmentTransition(
        const RawLiveSegmentTransitionV1& transition,
        std::uint64_t now_monotonic_ns);

    // Call on live-tail idle polls so heartbeat does not depend on traffic.
    [[nodiscard]] bool PublishHeartbeat(
        const l2flow::common::Identity128& writer_instance,
        std::uint64_t now_monotonic_ns);

    [[nodiscard]] RawReadinessObserverSnapshot Snapshot() const;

    // sampled_append must be a newly acquired, coherent Raw control-page
    // snapshot. This method never reuses a cached READY result.
    [[nodiscard]] RawObservationalGateResult Evaluate(
        const RawControlSnapshot& sampled_append,
        std::uint64_t now_monotonic_ns) const;

private:
    class Impl;

    explicit RawReadinessObserver(
        RawReadinessObserverConfig config);

    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::ingress
