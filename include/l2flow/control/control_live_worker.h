#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/control/control_decoder.h"
#include "l2flow/control/control_readiness_gate.h"
#include "l2flow/ingress/raw_live_tail.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <span>

namespace l2flow::control {

using ControlLiveWorkerMonotonicNow =
    std::uint64_t (*)(void* context) noexcept;

enum class ControlLiveWorkerFailureV1 : std::uint8_t {
    kNone = 0U,
    kRunAlreadyStarted,
    kClockInvalid,
    kLiveTailInstanceChanged,
    kLiveTailIdentityMismatch,
    kLiveTailFailure,
    kDecoderRejected,
    kControlRecordSinkRejected,
    kControlRecordSinkTimedOut,
    kStopTargetInvalid,
    kStopCursorExceeded,
    kStopCatchUpTimedOut,
    kAborted,
};

using ControlLiveWorkerFailureCallbackV1 = void (*)(
    void* context,
    ControlLiveWorkerFailureV1 failure) noexcept;

enum class ControlRecordPublishResultV1 : std::uint8_t {
    kPublishedNew = 0U,
    kAcceptedIdentical,
    kConflict,
    kFailure,
};

// ControlRecord publication is deliberately an explicit dependency. A sink
// rejection fail-stops this worker after the decoder's committed Raw cursor;
// restart/replay is then required to regenerate the missing derived record.
// No checkpoint may be published from a failed worker generation.
class ControlRecordSinkV1 {
public:
    virtual ~ControlRecordSinkV1() = default;
    // kPublishedNew and kAcceptedIdentical acknowledge that the exact
    // canonical wire has reached replay-safe retention. The idempotency key
    // is (source_stream_id, capture_date, stream_day_id,
    // origin_ingress_sequence, origin_record_end_wal_pos). Replaying the same
    // key and identical wire must return kAcceptedIdentical; the same key with
    // different bytes must return kConflict. Volatile enqueueing is
    // kFailure. This synchronous call MUST return by the absolute monotonic
    // deadline; implementations that cannot enforce that bound are invalid.
    [[nodiscard]] virtual ControlRecordPublishResultV1 Publish(
        const ControlRecordV1& record,
        const ControlRecordWireV1& canonical_wire,
        std::uint64_t deadline_monotonic_ns) noexcept = 0;
};

struct ControlLiveWorkerConfigV1 final {
    l2flow::common::Identity128 writer_instance{};
    std::uint64_t connect_generation = 0U;
    // Maximum permitted duration of one synchronous sink acknowledgement.
    // The worker passes an absolute monotonic deadline to Publish().
    std::uint64_t record_publish_timeout_ns =
        UINT64_C(1'000'000'000);
    std::uint64_t final_catch_up_timeout_ns =
        UINT64_C(5'000'000'000);
    ControlLiveWorkerMonotonicNow monotonic_now = nullptr;
    void* monotonic_clock_context = nullptr;
    ControlLiveWorkerFailureCallbackV1 failure_callback = nullptr;
    void* failure_context = nullptr;
};

struct ControlLiveStopCursorV1 final {
    l2flow::common::Identity128 writer_instance{};
    l2flow::common::Identity128 stream_day_id{};
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    std::uint32_t segment_sequence = 0U;
    std::uint64_t global_wal_pos = 0U;
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t segment_offset = 0U;

    [[nodiscard]] friend constexpr bool operator==(
        const ControlLiveStopCursorV1&,
        const ControlLiveStopCursorV1&) noexcept = default;
};

enum class ControlLiveWorkerCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kNullDependency,
    kInvalidConfig,
    kControlSampleUnavailable,
    kInitialControlInvalid,
    // At creation the fresh Raw append cursor must exactly equal the tail
    // attach cursor. A tail starting behind an existing backlog cannot define
    // a new SDK Connect generation.
    kInitialAppendMismatch,
    // The decoder must have been fully replayed or restored through the
    // record immediately preceding the tail attach cursor. Phase-2's
    // recovery cursor alone is not authoritative control state.
    kInitialCursorMismatch,
    kNamespaceMismatch,
    kResourceExhausted,
};

enum class ControlLiveWorkerReplayProofErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidInput,
    kInvalidScan,
    kSegmentChainMismatch,
    kDecoderMismatch,
    kResourceExhausted,
};

// Non-default-constructible receipt produced by revalidating the complete
// immutable Raw replay snapshot against the final decoder state. This is what
// permits a live attach cursor to be ahead of the last decoded record by one
// or more real, validated segment headers. Numeric WAL distance alone is
// never accepted as segment-transition proof.
class ControlLiveWorkerReplayProofV1 final {
public:
    ControlLiveWorkerReplayProofV1(
        const ControlLiveWorkerReplayProofV1&) = delete;
    ControlLiveWorkerReplayProofV1& operator=(
        const ControlLiveWorkerReplayProofV1&) = delete;
    ControlLiveWorkerReplayProofV1(
        ControlLiveWorkerReplayProofV1&&) = delete;
    ControlLiveWorkerReplayProofV1& operator=(
        ControlLiveWorkerReplayProofV1&&) = delete;
    ~ControlLiveWorkerReplayProofV1() = default;

    [[nodiscard]] static ControlLiveWorkerReplayProofErrorV1 Create(
        l2flow::common::Identity128 writer_instance,
        std::span<const l2flow::ingress::RawSegmentScanResult> scans,
        const l2flow::ingress::RawLiveTailAttachV1& validated_frontier,
        const ControlDecoderSnapshotV1& decoder,
        std::unique_ptr<ControlLiveWorkerReplayProofV1>* output) noexcept;

    [[nodiscard]] std::uint32_t
    decoder_processed_segment_sequence() const noexcept {
        return decoder_processed_segment_sequence_;
    }
    [[nodiscard]] std::uint64_t
    validated_frontier_global_wal_pos() const noexcept {
        return validated_frontier_.global_wal_pos;
    }

private:
    friend class ControlLiveWorkerV1;

    ControlLiveWorkerReplayProofV1(
        l2flow::common::Identity128 writer_instance,
        l2flow::ingress::RawLiveTailAttachV1 validated_frontier,
        std::uint64_t replay_record_count,
        std::uint32_t decoder_processed_segment_sequence,
        std::uint64_t decoder_processed_segment_offset,
        std::uint64_t decoder_processed_record_start_wal_pos,
        std::uint64_t decoder_processed_record_end_wal_pos,
        std::uint64_t decoder_processed_ingress_sequence,
        l2flow::common::Sha256Digest decoder_state_sha256) noexcept;

    l2flow::common::Identity128 writer_instance_{};
    l2flow::ingress::RawLiveTailAttachV1 validated_frontier_{};
    std::uint64_t replay_record_count_ = 0U;
    std::uint32_t decoder_processed_segment_sequence_ = 0U;
    std::uint64_t decoder_processed_segment_offset_ = 0U;
    std::uint64_t decoder_processed_record_start_wal_pos_ = 0U;
    std::uint64_t decoder_processed_record_end_wal_pos_ = 0U;
    std::uint64_t decoder_processed_ingress_sequence_ = 0U;
    l2flow::common::Sha256Digest decoder_state_sha256_{};
};

struct ControlLiveWorkerSnapshotV1 final {
    ControlLiveWorkerFailureV1 failure =
        ControlLiveWorkerFailureV1::kNone;
    ControlProcessErrorV1 process_error =
        ControlProcessErrorV1::kNone;
    l2flow::ingress::RawLiveTailError live_tail_error =
        l2flow::ingress::RawLiveTailError::kNone;
    ControlLiveStopCursorV1 requested_stop{};
    std::uint64_t processed_wal_pos = 0U;
    std::uint64_t processed_ingress_sequence = 0U;
    std::uint32_t processed_segment_sequence = 0U;
    std::uint64_t processed_segment_offset = 0U;
    std::uint64_t emitted_control_records = 0U;
    std::uint64_t committed_malformed_controls = 0U;
    bool startup_complete = false;
    bool startup_succeeded = false;
    bool stop_requested = false;
    bool finished = false;
    bool decoder_healthy = false;
};

// Production-capable single-consumer bridge from validated RawLiveTail facts
// to the authoritative Phase-3 decoder. It owns both the tail and decoder so
// no second consumer can skip or independently mutate their cursor. Segment
// headers advance the volatile worker WAL frontier but never decoder state.
// Create() must run only after callbacks from the preceding SDK generation
// have quiesced, and the service must not issue the new SDK Connect until
// Create() succeeds; a control-page generation is an ordering receipt, not a
// substitute for that lifecycle barrier.
// The service must StopAt() or Abort(), join the Run() thread, and only then
// destroy this object and the RawLiveTailSource borrowed by its tail. A sink
// call cannot be preempted by the worker; meeting the Publish deadline is a
// mandatory sink implementation property, and a late return is fail-stopped.
class ControlLiveWorkerV1 final {
public:
    ControlLiveWorkerV1(const ControlLiveWorkerV1&) = delete;
    ControlLiveWorkerV1& operator=(const ControlLiveWorkerV1&) = delete;
    ControlLiveWorkerV1(ControlLiveWorkerV1&&) = delete;
    ControlLiveWorkerV1& operator=(ControlLiveWorkerV1&&) = delete;
    ~ControlLiveWorkerV1();

    [[nodiscard]] static ControlLiveWorkerCreateErrorV1 Create(
        ControlLiveWorkerConfigV1 config,
        std::unique_ptr<l2flow::ingress::RawLiveTail> tail,
        std::unique_ptr<ControlDecoderV1> decoder,
        std::unique_ptr<ControlRecordSinkV1> record_sink,
        std::unique_ptr<ControlLiveWorkerV1>* output) noexcept;

    // Production entry point. The proof is mandatory when the decoder's last
    // record precedes the attach frontier because validated segment headers
    // do not mutate decoder state or ingress sequence.
    [[nodiscard]] static ControlLiveWorkerCreateErrorV1
    CreateWithReplayProof(
        ControlLiveWorkerConfigV1 config,
        const ControlLiveWorkerReplayProofV1& replay_proof,
        std::unique_ptr<l2flow::ingress::RawLiveTail> tail,
        std::unique_ptr<ControlDecoderV1> decoder,
        std::unique_ptr<ControlRecordSinkV1> record_sink,
        std::unique_ptr<ControlLiveWorkerV1>* output) noexcept;

    // Blocking loop; the service owns the thread. Run returns true only after
    // StopAt() and exact catch-up to that immutable terminal cursor.
    [[nodiscard]] bool Run() noexcept;
    [[nodiscard]] bool StopAt(
        const ControlLiveStopCursorV1& final_cursor) noexcept;
    void Abort() noexcept;

    [[nodiscard]] ControlLiveWorkerSnapshotV1 Snapshot() const noexcept;
    [[nodiscard]] ControlDecoderSnapshotV1 DecoderSnapshot() const;
    // Returns no checkpoint while startup has not established a healthy
    // generation, while a ControlRecord sink acknowledgement is outstanding,
    // or after any fatal failure. The caller must still bind a returned model
    // to a freshly sampled durable Raw frontier in the checkpoint store.
    [[nodiscard]] std::optional<ControlDecoderCheckpointV1>
    Checkpoint() const;

    // Performs a direct, non-cached Raw control read and evaluates it with one
    // mutex-consistent decoder/worker snapshot. capture_pipeline_healthy and
    // sampled_realtime_ns must come from the same service monitor sample; the
    // worker never manufactures either external fact. A Raw read failure is
    // an explicit fail-closed NOT_READY result.
    [[nodiscard]] ControlReadinessResultV1 EvaluateReadiness(
        const ControlReadinessGateConfigV1& config,
        const std::optional<MarketSilenceProofV1>& calendar_proof,
        std::uint64_t now_monotonic_ns,
        std::uint64_t sampled_realtime_ns,
        bool capture_pipeline_healthy) const noexcept;

private:
    [[nodiscard]] static ControlLiveWorkerCreateErrorV1 CreateImpl(
        ControlLiveWorkerConfigV1 config,
        const ControlLiveWorkerReplayProofV1* replay_proof,
        std::unique_ptr<l2flow::ingress::RawLiveTail> tail,
        std::unique_ptr<ControlDecoderV1> decoder,
        std::unique_ptr<ControlRecordSinkV1> record_sink,
        std::unique_ptr<ControlLiveWorkerV1>* output) noexcept;

    ControlLiveWorkerV1(
        ControlLiveWorkerConfigV1 config,
        std::uint64_t start_control_generation,
        std::uint64_t generation_first_ingress_sequence,
        std::unique_ptr<l2flow::ingress::RawLiveTail> tail,
        std::unique_ptr<ControlDecoderV1> decoder,
        std::unique_ptr<ControlRecordSinkV1> record_sink);

    [[nodiscard]] std::uint64_t MonotonicNowNs() const noexcept;
    [[nodiscard]] bool PublishHeartbeat(std::uint64_t now_ns) noexcept;
    [[nodiscard]] bool StopReached() noexcept;
    [[nodiscard]] bool StopCursorValid(
        const ControlLiveStopCursorV1& cursor) const noexcept;
    [[nodiscard]] ControlReadinessRuntimeV1 ReadinessRuntimeLocked(
        std::uint64_t sampled_realtime_ns,
        bool capture_pipeline_healthy) const noexcept;
    void Trip(
        ControlLiveWorkerFailureV1 failure,
        l2flow::ingress::RawLiveTailError live_tail_error =
            l2flow::ingress::RawLiveTailError::kNone,
        ControlProcessErrorV1 process_error =
            ControlProcessErrorV1::kNone) noexcept;

    ControlLiveWorkerConfigV1 config_{};
    std::uint64_t start_control_generation_ = 0U;
    std::uint64_t generation_first_ingress_sequence_ = 0U;
    std::unique_ptr<l2flow::ingress::RawLiveTail> tail_;
    std::unique_ptr<ControlDecoderV1> decoder_;
    std::unique_ptr<ControlRecordSinkV1> record_sink_;

    mutable std::mutex state_mutex_;
    std::uint64_t processed_wal_pos_ = 0U;
    std::uint64_t processed_ingress_sequence_ = 0U;
    std::uint32_t processed_segment_sequence_ = 0U;
    std::uint64_t processed_segment_offset_ = 0U;
    std::uint64_t decoder_heartbeat_monotonic_ns_ = 0U;
    std::uint64_t successful_logon_connect_generation_ = 0U;
    std::uint64_t successful_logon_ingress_sequence_ = 0U;
    std::uint64_t emitted_control_records_ = 0U;
    std::uint64_t committed_malformed_controls_ = 0U;
    bool decoder_healthy_ = false;

    mutable std::mutex stop_mutex_;
    ControlLiveStopCursorV1 stop_cursor_{};
    std::atomic<bool> run_started_{false};
    std::atomic<bool> startup_complete_{false};
    std::atomic<bool> startup_succeeded_{false};
    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> finished_{false};
    std::atomic<bool> fatal_{false};
    std::atomic_flag trip_claimed_ = ATOMIC_FLAG_INIT;
    std::atomic<std::uint8_t> failure_{
        static_cast<std::uint8_t>(ControlLiveWorkerFailureV1::kNone)};
    std::atomic<std::uint8_t> live_tail_error_{
        static_cast<std::uint8_t>(
            l2flow::ingress::RawLiveTailError::kNone)};
    std::atomic<std::uint16_t> process_error_{
        static_cast<std::uint16_t>(ControlProcessErrorV1::kNone)};
};

}  // namespace l2flow::control
