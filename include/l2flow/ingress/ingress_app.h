#pragma once

#include "l2flow/ingress/byte_ring.h"
#include "l2flow/ingress/callback_handler.h"
#include "l2flow/ingress/capture_clock.h"
#include "l2flow/ingress/capture_metrics.h"
#include "l2flow/ingress/shadow_capture.h"
#include "l2flow/ops/fatal_latch.h"
#include "l2flow/sdk/ingress_config.h"
#include "l2flow/sdk/sdk_runtime.h"

#include <atomic>
#include <chrono>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

namespace l2flow::ingress {

class UnifiedCallbackRouter;

enum class IngressAppState : std::uint8_t {
    Constructed = 0U,
    Initializing,
    Running,
    Stopping,
    Stopped,
};

enum class IngressLifecycleEvent : std::uint8_t {
    ShadowWriterStarted = 0U,
    HandlerBeginStopping,
    HandlerQuiesced,
    ShadowWriterStopRequested,
    ShadowWriterJoined,
};

// Optional test/telemetry observer. It cannot alter control flow.
class IngressLifecycleObserver {
public:
    virtual ~IngressLifecycleObserver() = default;
    virtual void Observe(IngressLifecycleEvent event) noexcept = 0;
};

struct IngressAppOptions final {
    std::chrono::milliseconds callback_quiesce_timeout{
        std::chrono::seconds(5)};
    // Production may anchor the vendor's path-only logging API under a
    // retained /proc/self/fd directory lease. This runtime-only override is
    // deliberately excluded from the canonical ingress configuration hash.
    std::string sdk_log_runtime_prefix;
};

// Owns exactly one manager and one subscriber.  CallbackHandler is declared
// before the SDK objects and therefore remains alive through their explicit
// release and through the final factory/loader destruction.
class IngressApp final {
public:
    IngressApp(
        l2flow::sdk::IngressConfig config,
        std::shared_ptr<l2flow::sdk::SdkFactory> sdk_factory,
        std::unique_ptr<CaptureClock> clock,
        std::unique_ptr<ShadowCaptureOutput> shadow_output = nullptr,
        IngressAppOptions options = {},
        IngressLifecycleObserver* observer = nullptr);
    ~IngressApp();

    IngressApp(const IngressApp&) = delete;
    IngressApp& operator=(const IngressApp&) = delete;
    IngressApp(IngressApp&&) = delete;
    IngressApp& operator=(IngressApp&&) = delete;

    // Both methods contain ordinary exceptions and are serialized.
    // Initialize() performs a complete ordered Stop() before returning any
    // ordinary failure. A throwing opaque SdkManager::Shutdown() is the one
    // deliberate process fail-stop: callback convergence cannot be proven,
    // so returning and later destroying the callback target would be unsafe.
    [[nodiscard]] bool Initialize(std::string* error) noexcept;
    [[nodiscard]] bool Stop(std::string* error) noexcept;

    [[nodiscard]] IngressAppState state() const noexcept;
    [[nodiscard]] bool fatal() const noexcept;
    [[nodiscard]] std::string last_error() const;
    [[nodiscard]] CaptureMetricsSnapshot capture_metrics() const noexcept;
    [[nodiscard]] ShadowCaptureStats shadow_stats() const noexcept;
    // Phase-1 readiness is observational only: successful logon, every
    // required subscription status OK, and first legal record for every
    // required market message. It does not create an authoritative epoch.
    [[nodiscard]] bool ready_for_shadow() const noexcept;
    // Returns zero when not ready. A nonzero value is the accepted logon
    // generation backing the current observational readiness.
    [[nodiscard]] std::uint64_t
    shadow_readiness_generation() const noexcept;
    [[nodiscard]] ShadowCaptureReconciliation
    reconciliation() const noexcept;
    [[nodiscard]] std::string prometheus_metrics() const;

private:
    static const l2flow::sdk::IngressSpec& ValidateAndGetSpec(
        const l2flow::sdk::IngressConfig& config);
    static std::size_t CheckedRingCapacity(
        const l2flow::sdk::IngressConfig& config);
    static CallbackHandlerConfig MakeHandlerConfig(
        const l2flow::sdk::IngressConfig& config,
        const l2flow::sdk::IngressSpec& spec);
    static ShadowCaptureConfig MakeShadowConfig(
        const l2flow::sdk::IngressSpec& spec);

    void AddSubscriptions();
    void StartShadowWriter();
    [[nodiscard]] bool StopLocked() noexcept;
    void SetFailure(std::string message) noexcept;
    void SetFailureLiteral(const char* message) noexcept;
    void SetFailureException(
        std::string_view stage,
        const std::exception& exception) noexcept;
    void SetFailureStage(
        std::string_view stage,
        const char* suffix) noexcept;
    void CopyError(std::string* error) const noexcept;
    void Observe(IngressLifecycleEvent event) noexcept;

    l2flow::sdk::IngressConfig config_;
    const l2flow::sdk::IngressSpec* spec_;
    std::shared_ptr<l2flow::sdk::SdkFactory> sdk_factory_;
    std::unique_ptr<CaptureClock> clock_;
    std::unique_ptr<ShadowCaptureOutput> pending_shadow_output_;
    IngressAppOptions options_;
    IngressLifecycleObserver* observer_;

    l2flow::ops::FatalLatch capture_fatal_;
    CaptureMetrics capture_metrics_;
    ByteRing ring_;
    CallbackHandler handler_;
    // Bypasses the vendor MessageHandler dispatcher, whose implementation
    // dereferences msg->GetHead() before reaching CallbackHandler's stopping
    // gate. The router forwards every callback family into the handler's
    // unified capture path without inspecting vendor memory first.
    std::unique_ptr<UnifiedCallbackRouter>
        sdk_callback_router_;

    std::unique_ptr<ShadowCaptureWriter> shadow_writer_;
    // Published once and never withdrawn during the IngressApp lifetime.
    // Monitoring accessors use this atomic pointer instead of racing the
    // owning unique_ptr while Initialize() constructs the writer.
    std::atomic<ShadowCaptureWriter*> published_shadow_writer_{nullptr};
    std::thread shadow_thread_;
    std::atomic<bool> shadow_result_known_{false};
    std::atomic<bool> shadow_result_{false};

    std::unique_ptr<l2flow::sdk::SdkManager> manager_;
    std::unique_ptr<l2flow::sdk::SdkSubscriber> subscriber_;

    mutable std::mutex lifecycle_mutex_;
    std::atomic<IngressAppState> state_{IngressAppState::Constructed};
    std::atomic<bool> lifecycle_fatal_{false};
    std::string last_error_;
};

}  // namespace l2flow::ingress
