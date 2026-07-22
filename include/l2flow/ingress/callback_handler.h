#pragma once

#include "l2flow/ingress/byte_ring.h"
#include "l2flow/ingress/capture_clock.h"
#include "l2flow/ingress/capture_metrics.h"
#include "l2flow/ops/fatal_latch.h"
#include "l2flow/canonical/source_frontier_v1.h"

#include "mdl_api.h"

#include <atomic>
#include <chrono>
#include <cstdint>

namespace l2flow::ingress {

struct CallbackHandlerConfig {
    std::uint32_t source_stream_id = 0U;
    std::uint8_t market_service_id = 0U;
    std::uint32_t max_message_bytes = 0U;
    std::uint32_t capture_date = 0U;
    std::uint64_t first_ingress_sequence = 1U;
    // Optional producer-side SourceFrontier binding.  When enabled, all
    // three fields are mandatory and the page must outlive this handler.
    l2flow::canonical::SourceFrontierPageV1* source_frontier = nullptr;
    l2flow::common::Identity128 frontier_writer_instance{};
    std::uint64_t frontier_generation = 0U;
    std::chrono::nanoseconds source_frontier_busy_timeout =
        l2flow::canonical::kSourceFrontierDefaultBusyTimeoutV1;
};

// The four vendor callbacks are intentionally identical entry points into one
// capture path. This class does not decode any control or market body.
class CallbackHandler final : public datayes::mdl::MessageHandler {
public:
    CallbackHandler(CallbackHandlerConfig config,
                    ByteRing& ring,
                    CaptureClock& clock,
                    l2flow::ops::FatalLatch& fatal,
                    CaptureMetrics& metrics);

    void OnMDLAPIMessage(
        const datayes::mdl::MDLMessage* message) noexcept override;
    void OnMDLSysMessage(
        const datayes::mdl::MDLMessage* message) noexcept override;
    void OnMDLSHL2Message(
        const datayes::mdl::MDLMessage* message) noexcept override;
    void OnMDLSZL2Message(
        const datayes::mdl::MDLMessage* message) noexcept override;

    void SetConnectionEpochHint(std::uint32_t value) noexcept;
    void SetCaptureDate(std::uint32_t value) noexcept;

    // Call BeginStopping before IOManager::Shutdown(), then call Quiesce only
    // after Shutdown() has returned and the SDK has stopped starting callbacks.
    void BeginStopping() noexcept;
    [[nodiscard]] bool Quiesce(std::chrono::milliseconds timeout) noexcept;

    [[nodiscard]] bool callback_inflight() const noexcept;
    [[nodiscard]] std::uint64_t captured_sequence() const noexcept;
    // Safe only after Quiesce() or in a single-threaded test.
    [[nodiscard]] std::uint64_t
    next_ingress_sequence_when_quiescent() const noexcept;

private:
    void CaptureMessage(
        const datayes::mdl::MDLMessage* message) noexcept;
    [[nodiscard]] bool CaptureMessageImpl(
        const datayes::mdl::MDLMessage* message,
        std::uint64_t* captured_sequence);

    CallbackHandlerConfig config_;
    ByteRing& ring_;
    CaptureClock& clock_;
    l2flow::ops::FatalLatch& fatal_;
    CaptureMetrics& metrics_;

    std::atomic_flag callback_gate_ = ATOMIC_FLAG_INIT;
    std::atomic<bool> callback_inflight_{false};
    std::atomic<bool> accepting_{true};
    std::atomic<std::uint32_t> connection_epoch_hint_{0U};
    std::atomic<std::uint32_t> capture_date_;
    std::uint64_t next_ingress_sequence_;
    std::atomic<std::uint64_t> captured_sequence_;
};

}  // namespace l2flow::ingress
