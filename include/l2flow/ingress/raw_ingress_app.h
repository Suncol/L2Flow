#pragma once

#include "l2flow/ingress/byte_ring.h"
#include "l2flow/ingress/callback_handler.h"
#include "l2flow/ingress/capture_clock.h"
#include "l2flow/ingress/capture_metrics.h"
#include "l2flow/ingress/raw_capture_worker.h"
#include "l2flow/ingress/raw_emergency_writer_ack.h"
#include "l2flow/ingress/raw_ingress_config.h"
#include "l2flow/ingress/raw_live_tail.h"
#include "l2flow/ingress/raw_readiness_observer.h"
#include "l2flow/ingress/raw_readiness_worker.h"
#include "l2flow/ops/fatal_latch.h"
#include "l2flow/sdk/endpoint_contract.h"
#include "l2flow/sdk/ingress_config.h"
#include "l2flow/sdk/sdk_runtime.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

namespace l2flow::ingress {

class RawUnifiedCallbackRouter;
class RawFinalizationContinuationPosixV1;

enum class RawIngressAppState : std::uint8_t {
    kConstructed = 0U,
    kInitializing,
    kRunning,
    kStopping,
    kStopped,
};

enum class RawIngressLifecycleEvent : std::uint8_t {
    kCaptureWorkerStarted = 0U,
    kReadinessWorkerStarted,
    kSdkConnectStarting,
    kHandlerBeginStopping,
    kHandlerQuiesced,
    kEmergencyWriterPaused,
    kEmergencyWriterAckFrozen,
    kCaptureWorkerJoined,
    kReadinessWorkerJoined,
    kCleanStopBarrierComplete,
};

class RawIngressLifecycleObserver {
public:
    virtual ~RawIngressLifecycleObserver() = default;
    virtual void Observe(
        RawIngressLifecycleEvent event) noexcept = 0;
};

struct RawIngressAppConfigV1 final {
    RawIngressConfig stable{};
    // Immutable proof derived from the exact endpoint-contract bytes named
    // by stable.endpoint_contract_sha256.
    std::shared_ptr<
        const l2flow::sdk::VerifiedEndpointContract>
        endpoint;
    // Runtime-only credential bytes. They are never rendered or included in
    // the stable Raw config hash.
    std::string credential_token;
    RawIngressRuntimeState recovered{};
    std::uint64_t connect_generation = 0U;
};

struct RawIngressAppOptionsV1 final {
    std::chrono::milliseconds callback_quiesce_timeout{
        std::chrono::seconds(5)};
    std::chrono::milliseconds emergency_writer_pause_timeout{
        std::chrono::seconds(5)};
    std::string sdk_log_runtime_prefix;
    std::uint64_t observer_maximum_lag_bytes =
        64U * 1024U * 1024U;
    std::uint64_t observer_heartbeat_timeout_ns =
        5U * 1'000'000'000U;
    std::uint64_t observer_final_catch_up_timeout_ns =
        5U * 1'000'000'000U;
    std::uint64_t writer_idle_heartbeat_interval_ns =
        kRawCaptureDefaultIdleHeartbeatIntervalNs;
    RawCaptureMonotonicNow worker_monotonic_now = nullptr;
    void* worker_monotonic_clock_context = nullptr;
    // Optional shared SourceFrontier producer binding.  The mapping is
    // borrowed and must outlive this app and both worker threads.
    l2flow::canonical::SourceFrontierPageV1* source_frontier = nullptr;
    l2flow::common::Identity128 frontier_writer_instance{};
    std::uint64_t frontier_generation = 0U;
    std::chrono::nanoseconds source_frontier_busy_timeout =
        l2flow::canonical::kSourceFrontierDefaultBusyTimeoutV1;
};

struct RawIngressCleanStopEvidenceV1 final {
    RawIngressRuntimeState started_runtime{};
    CaptureMetricsSnapshot callback{};
    RawCaptureWorkerSnapshot capture{};
    RawReadinessObserverSnapshot observer{};
    RawWalSinkIdentityV1 final_sink_identity{};
    RawWalWriterSnapshot final_wal{};
    RawCaptureReconciliation reconciliation{};
    std::uint64_t ring_used_bytes = 0U;

    // Cross-domain clean-stop proof: callback/ring/worker counts, vendor
    // bytes, framed Raw bytes, segment-header WAL growth, ingress cursor,
    // sink durability and observer frontier must all describe one terminal
    // state.
    [[nodiscard]] bool exact() const noexcept;
};

// The concrete service implementation uses this final gate to publish/reuse
// the terminal sealed certificate and durably unregister the coordinator
// route. Returning true means both barriers completed. RawIngressApp never
// reports a clean stop without this gate.
class RawIngressCleanStopGateV1 {
public:
    virtual ~RawIngressCleanStopGateV1() = default;
    [[nodiscard]] virtual bool Complete(
        const RawIngressCleanStopEvidenceV1&
            evidence) noexcept = 0;
};

// Runtime-only Phase-2 lifecycle after service-level recovery has produced:
// (1) an initialized Raw sink/control page, (2) an attach-gated live tail,
// and (3) typed recovered runtime state. The capture and readiness consumers
// are both started before SDK Connect. The SDK callback never competes with a
// shadow sink for the ByteRing.
class RawIngressApp final {
public:
    RawIngressApp(
        RawIngressAppConfigV1 config,
        std::shared_ptr<l2flow::sdk::SdkFactory>
            sdk_factory,
        std::unique_ptr<CaptureClock> clock,
        std::unique_ptr<RawWalSink> prepared_sink,
        std::unique_ptr<RawLiveTail> live_tail,
        std::unique_ptr<RawIngressCleanStopGateV1>
            clean_stop_gate,
        RawIngressAppOptionsV1 options = {},
        RawIngressLifecycleObserver* lifecycle_observer =
            nullptr);
    ~RawIngressApp();

    RawIngressApp(const RawIngressApp&) = delete;
    RawIngressApp& operator=(const RawIngressApp&) =
        delete;
    RawIngressApp(RawIngressApp&&) = delete;
    RawIngressApp& operator=(RawIngressApp&&) = delete;

    [[nodiscard]] bool Initialize(
        std::string* error) noexcept;
    // First half of a normal multi-source stop.  It closes the callback
    // admission gate without waiting in the vendor SDK, so a service can
    // quiesce all four producers before the first potentially blocking
    // IOManager::Shutdown call.  Stop() remains responsible for SDK shutdown,
    // callback quiescence, ring drain, Raw seal and the final clean-stop gate.
    [[nodiscard]] bool PrepareCleanStop(
        std::string* error) noexcept;
    [[nodiscard]] bool Stop(
        std::string* error) noexcept;
    // Permanently revokes this app's Running/READY lifecycle, shuts the SDK
    // down, quiesces callbacks, and parks the sole writer before returning an
    // unforgeable snapshot capability. It neither drains/seals the queued
    // suffix nor performs a coordinator state transition.
    [[nodiscard]] std::unique_ptr<
        RawEmergencyWriterAckV1>
    BeginEmergencyStop(
        std::string* error) noexcept;

    [[nodiscard]] RawIngressAppState state()
        const noexcept;
    [[nodiscard]] bool fatal() const noexcept;
    [[nodiscard]] std::string last_error() const;
    [[nodiscard]] CaptureMetricsSnapshot
    capture_metrics() const noexcept;
    [[nodiscard]] RawCaptureWorkerSnapshot
    capture_snapshot() const noexcept;
    [[nodiscard]] RawReadinessObserverSnapshot
    observer_snapshot() const;
    [[nodiscard]] RawCaptureReconciliation
    reconciliation() const noexcept;
    [[nodiscard]] RawWalWriterSnapshot
    wal_snapshot() const noexcept;
    [[nodiscard]] RawWalFailure
    wal_failure() const noexcept;
    // Bounded Prometheus exposition containing only Raw progress, health and
    // exact-reconciliation facts. It never renders credential bytes,
    // endpoint addresses or filesystem paths.
    [[nodiscard]] std::string prometheus_metrics() const;
    [[nodiscard]] RawObservationalGateResult
    EvaluateReadiness(
        const RawControlSnapshot& sampled_control,
        std::uint64_t now_monotonic_ns) const;

private:
    static const l2flow::sdk::IngressSpec&
    ValidateAndGetSpec(
        const RawIngressAppConfigV1& config);
    static std::size_t CheckedRingCapacity(
        const RawIngressConfig& config);
    static CallbackHandlerConfig MakeHandlerConfig(
        const RawIngressAppConfigV1& config,
        const l2flow::sdk::IngressSpec& spec,
        const RawIngressAppOptionsV1& options);
    static RawCaptureWorkerConfig MakeCaptureConfig(
        const RawIngressConfig& config,
        const RawIngressAppOptionsV1& options,
        RawIngressApp* app) noexcept;
    static RawReadinessWorkerConfig
    MakeReadinessWorkerConfig(
        const RawIngressAppOptionsV1& options,
        RawIngressApp* app) noexcept;

    void AddSubscriptions();
    void StartWorkers();
    [[nodiscard]] bool StopLocked() noexcept;
    [[nodiscard]] bool StopEmergencyLocked() noexcept;
    [[nodiscard]] bool ShutdownAndQuiesceLocked(
        bool preserve_callbacks_until_shutdown) noexcept;
    [[nodiscard]] bool ActiveSinkIdentityValid(
        const RawWalSinkIdentityV1& identity,
        const RawWalWriterSnapshot& wal) const noexcept;
    [[nodiscard]] bool ConsumeEmergencyWriterAckForFinalization(
        RawEmergencyWriterAckV1& ack,
        RawWalSink** writer,
        ByteRing** ring,
        std::string* error) noexcept;
    [[nodiscard]] bool PreparedSinkMatchesRecovery()
        const noexcept;
    void SetFailure(std::string message) noexcept;
    void SetFailureLiteral(const char* message) noexcept;
    void SetFailureException(
        std::string_view stage,
        const std::exception& exception) noexcept;
    void CopyError(std::string* error) const noexcept;
    void Observe(
        RawIngressLifecycleEvent event) noexcept;
    static void CaptureFailure(
        void* context,
        RawCaptureFatalSignal signal) noexcept;
    static void ReadinessFailure(
        void* context,
        RawReadinessWorkerFailureKind failure) noexcept;

    RawIngressAppConfigV1 config_;
    const l2flow::sdk::IngressSpec* spec_ = nullptr;
    std::shared_ptr<l2flow::sdk::SdkFactory>
        sdk_factory_;
    std::unique_ptr<CaptureClock> clock_;
    std::unique_ptr<RawWalSink> sink_;
    std::unique_ptr<RawLiveTail> live_tail_;
    std::unique_ptr<RawIngressCleanStopGateV1>
        clean_stop_gate_;
    RawIngressAppOptionsV1 options_{};
    RawIngressLifecycleObserver* lifecycle_observer_ =
        nullptr;

    l2flow::ops::FatalLatch capture_fatal_;
    CaptureMetrics capture_metrics_;
    ByteRing ring_;
    CallbackHandler handler_;
    std::unique_ptr<RawUnifiedCallbackRouter>
        sdk_callback_router_;
    std::unique_ptr<RawReadinessObserver>
        readiness_observer_;
    std::unique_ptr<RawCaptureWorker> capture_worker_;
    std::unique_ptr<RawReadinessWorker>
        readiness_worker_;

    std::thread capture_thread_;
    std::thread readiness_thread_;
    std::atomic<bool> capture_result_known_{false};
    std::atomic<bool> capture_result_{false};
    std::atomic<bool> readiness_result_known_{false};
    std::atomic<bool> readiness_result_{false};
    bool capture_start_attempted_ = false;
    bool readiness_start_attempted_ = false;

    std::unique_ptr<l2flow::sdk::SdkManager> manager_;
    std::unique_ptr<l2flow::sdk::SdkSubscriber>
        subscriber_;

    mutable std::mutex lifecycle_mutex_;
    std::atomic<RawIngressAppState> state_{
        RawIngressAppState::kConstructed};
    std::atomic<bool> lifecycle_fatal_{false};
    std::string last_error_;
    std::shared_ptr<RawEmergencyWriterAckControlV1>
        emergency_ack_control_ =
            std::make_shared<
                RawEmergencyWriterAckControlV1>();
    std::uint64_t emergency_ack_epoch_ = 0U;
    bool sdk_shutdown_returned_ = false;
    bool callback_quiesced_ = false;
    bool handler_stopping_begun_ = false;
    bool emergency_ack_issued_ = false;
    bool emergency_ack_consumed_ = false;

    friend class RawFinalizationContinuationPosixV1;
};

}  // namespace l2flow::ingress
