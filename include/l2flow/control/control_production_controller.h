#pragma once

#include "l2flow/control/control_checkpoint_posix_store.h"
#include "l2flow/control/control_live_worker.h"
#include "l2flow/ingress/raw_production_runtime.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

namespace l2flow::control {

enum class ControlProductionControllerStateV1 : std::uint8_t {
    kConstructed = 0U,
    kPrepared,
    kStarting,
    kRunning,
    kStopping,
    kStopped,
    kFailed,
};

enum class ControlProductionControllerCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kNullDependency,
    kInvalidConfig,
    kCheckpointDirectoryUnsafe,
    kReplaySnapshot,
    kCheckpointDiscovery,
    kCheckpointBoundaryMissing,
    kDecoderCreate,
    kReplayCreate,
    kReplayFailure,
    kDerivedSinkFailure,
    kReplayProof,
    kLiveTailAttach,
    kWorkerCreate,
    kConsumerInstall,
    kAllocationFailure,
};

struct ControlProductionControllerConfigV1 final {
    ControlDecoderConfigV1 decoder{};
    ControlLiveWorkerConfigV1 worker{};
    l2flow::ingress::RawProductionReplaySnapshotLimitsV1
        replay_limits{};
    std::uint64_t startup_timeout_ns = UINT64_C(5'000'000'000);
    // -1 disables checkpoint load/publication. When enabled, Create retains a
    // verified owner-only alias; the caller's descriptor may then be closed.
    int checkpoint_directory_fd = -1;
};

struct ControlProductionControllerSnapshotV1 final {
    ControlProductionControllerStateV1 state =
        ControlProductionControllerStateV1::kConstructed;
    ControlProductionControllerCreateErrorV1 create_error =
        ControlProductionControllerCreateErrorV1::kNone;
    l2flow::ingress::RawProductionReplaySnapshotErrorV1
        replay_snapshot_error =
            l2flow::ingress::RawProductionReplaySnapshotErrorV1::kNone;
    ControlCheckpointPosixStoreErrorV1 checkpoint_load_error =
        ControlCheckpointPosixStoreErrorV1::kNone;
    ControlDecoderCreateErrorV1 checkpoint_restore_error =
        ControlDecoderCreateErrorV1::kNone;
    ControlCheckpointPosixStoreErrorV1 checkpoint_publish_error =
        ControlCheckpointPosixStoreErrorV1::kNone;
    ControlCheckpointPosixDispositionV1 checkpoint_disposition =
        ControlCheckpointPosixDispositionV1::kNone;
    ControlLiveWorkerFailureV1 worker_failure =
        ControlLiveWorkerFailureV1::kNone;
    ControlProcessErrorV1 worker_process_error =
        ControlProcessErrorV1::kNone;
    l2flow::ingress::RawLiveTailError worker_live_tail_error =
        l2flow::ingress::RawLiveTailError::kNone;
    std::uint64_t replayed_records = 0U;
    std::uint64_t replayed_control_records = 0U;
    bool checkpoint_enabled = false;
    bool checkpoint_loaded = false;
    bool checkpoint_restored = false;
    bool checkpoint_rejected_to_full_replay = false;
    bool checkpoint_publication_attempted = false;
    bool checkpoint_published = false;
    bool worker_thread_result_known = false;
    bool worker_thread_result = false;
};

struct ControlProductionReadinessSampleV1 final {
    ControlReadinessResultV1 gate{};
    ControlProductionControllerStateV1 controller_state =
        ControlProductionControllerStateV1::kConstructed;
    l2flow::ingress::RawIngressAppState ingress_state =
        l2flow::ingress::RawIngressAppState::kConstructed;
    bool capture_pipeline_healthy = false;
};

// Owns one Phase-2 runtime generation and is its sole authoritative Raw tail
// consumer. Create performs immutable recovery replay and installs the worker;
// Initialize starts that worker through RawIngressApp's pre-Connect barrier.
class ControlProductionControllerV1 final
    : public l2flow::ingress::RawIngressExternalTailConsumerV1 {
public:
    ~ControlProductionControllerV1() override;

    ControlProductionControllerV1(
        const ControlProductionControllerV1&) = delete;
    ControlProductionControllerV1& operator=(
        const ControlProductionControllerV1&) = delete;
    ControlProductionControllerV1(
        ControlProductionControllerV1&&) = delete;
    ControlProductionControllerV1& operator=(
        ControlProductionControllerV1&&) = delete;

    [[nodiscard]] static ControlProductionControllerCreateErrorV1 Create(
        ControlProductionControllerConfigV1 config,
        std::unique_ptr<
            l2flow::ingress::RawProductionAuthoritativeRuntimeV1> runtime,
        std::unique_ptr<ControlRecordSinkV1> record_sink,
        std::unique_ptr<ControlProductionControllerV1>* output,
        std::string* error = nullptr) noexcept;

    [[nodiscard]] bool Initialize(
        std::string* error = nullptr) noexcept;
    [[nodiscard]] bool Stop(
        std::string* error = nullptr) noexcept;
    [[nodiscard]] ControlProductionReadinessSampleV1 EvaluateReadiness(
        const ControlReadinessGateConfigV1& config,
        const std::optional<MarketSilenceProofV1>& calendar_proof,
        std::uint64_t now_monotonic_ns,
        std::uint64_t sampled_realtime_ns) const noexcept;
    [[nodiscard]] ControlProductionControllerSnapshotV1
    Snapshot() const noexcept;
    [[nodiscard]] ControlDecoderSnapshotV1 DecoderSnapshot() const;

    [[nodiscard]] bool StartBeforeConnect(
        std::string* error) noexcept override;
    [[nodiscard]] bool StopAtAndJoin(
        const l2flow::ingress::RawReadinessStopCursorV1& final_cursor,
        l2flow::ingress::RawIngressTailConsumerEvidenceV1* evidence,
        std::string* error) noexcept override;
    void AbortAndJoin() noexcept override;
    [[nodiscard]] bool Healthy() const noexcept override;

private:
    ControlProductionControllerV1(
        ControlProductionControllerConfigV1 config,
        std::unique_ptr<
            l2flow::ingress::RawProductionAuthoritativeRuntimeV1>
            runtime,
        int checkpoint_directory_fd) noexcept;

    [[nodiscard]] ControlProductionControllerCreateErrorV1 Prepare(
        std::unique_ptr<ControlRecordSinkV1> record_sink,
        std::string* error) noexcept;
    [[nodiscard]] bool ReplayIntoDecoder(
        const l2flow::ingress::RawProductionReplaySnapshotV1& replay,
        std::optional<std::uint64_t> begin_ingress_sequence,
        ControlDecoderV1& decoder,
        ControlRecordSinkV1& sink,
        std::string* error) noexcept;
    [[nodiscard]] std::uint64_t MonotonicNowNs() const noexcept;
    void JoinWorker() noexcept;
    [[nodiscard]] bool StartFailureSupervisor() noexcept;
    void FailureSupervisorRun() noexcept;
    void RequestFailureSupervisorExit() noexcept;
    void JoinFailureSupervisor() noexcept;
    void PublishFinalCheckpoint(
        const l2flow::ingress::RawReadinessStopCursorV1& final_cursor)
        noexcept;
    static void WorkerFailure(
        void* context,
        ControlLiveWorkerFailureV1 failure) noexcept;
    void OnWorkerFailure(ControlLiveWorkerFailureV1 failure) noexcept;
    void SetCreateFailure(
        ControlProductionControllerCreateErrorV1 failure) noexcept;

    // Declared before worker_: reverse destruction keeps the worker/tail dead
    // before its borrowed Phase-2 POSIX source is released.
    std::unique_ptr<
        l2flow::ingress::RawProductionAuthoritativeRuntimeV1> runtime_;
    ControlProductionControllerConfigV1 config_{};
    ControlLiveWorkerFailureCallbackV1 delegated_failure_callback_ = nullptr;
    void* delegated_failure_context_ = nullptr;
    int checkpoint_directory_fd_ = -1;
    std::unique_ptr<ControlLiveWorkerV1> worker_;
    std::thread worker_thread_;
    std::thread failure_supervisor_thread_;
    mutable std::mutex failure_supervisor_mutex_;
    std::condition_variable failure_supervisor_cv_;
    bool failure_stop_requested_ = false;
    bool failure_stop_suppressed_ = false;
    bool failure_supervisor_exit_requested_ = false;
    std::atomic<bool> worker_thread_result_known_{false};
    std::atomic<bool> worker_thread_result_{false};
    std::atomic<ControlProductionControllerStateV1> state_{
        ControlProductionControllerStateV1::kConstructed};
    mutable std::mutex snapshot_mutex_;
    ControlProductionControllerSnapshotV1 snapshot_{};
};

}  // namespace l2flow::control
