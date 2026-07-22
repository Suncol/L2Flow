#pragma once

#include "l2flow/ingress/byte_ring.h"
#include "l2flow/ingress/capture_metrics.h"
#include "l2flow/ingress/raw_wal_writer.h"
#include "l2flow/canonical/source_frontier_v1.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace l2flow::ingress {

inline constexpr std::uint64_t
    kRawCaptureDefaultDurableIntervalNs = 10'000'000U;
inline constexpr std::uint64_t
    kRawCaptureDefaultDurableBatchBytes = 4U * 1024U * 1024U;
inline constexpr std::uint64_t
    kRawCaptureDefaultIdleHeartbeatIntervalNs = 1'000'000'000U;

enum class RawCaptureFatalSignal : std::uint8_t {
    kRawWalIo = 0U,
    kRingCorruption,
    kSourceFrontier,
};

using RawCaptureFailureCallback = void (*)(
    void* context,
    RawCaptureFatalSignal signal) noexcept;

// The service adapter maps kRawWalIo to FatalReason::RAW_WAL_IO and
// kRingCorruption to FatalReason::RING_CORRUPTION. Keeping the callback
// independent of FatalLatch lets this worker land before the Phase-2 fatal
// reason is added to the process-wide enum.
struct RawCaptureFailureSink final {
    RawCaptureFailureCallback callback = nullptr;
    void* context = nullptr;

    void Notify(RawCaptureFatalSignal signal) const noexcept {
        callback(context, signal);
    }
};

using RawCaptureMonotonicNow = std::uint64_t (*)(
    void* context) noexcept;

struct RawCaptureWorkerConfig final {
    std::uint64_t durable_interval_ns =
        kRawCaptureDefaultDurableIntervalNs;
    std::uint64_t durable_batch_bytes =
        kRawCaptureDefaultDurableBatchBytes;
    // With no market records, FlushDurable is still called at this cadence
    // by the sole writer thread so the production Raw control heartbeat keeps
    // proving liveness.  It must be shorter than the aggregate writer timeout.
    std::uint64_t idle_heartbeat_interval_ns =
        kRawCaptureDefaultIdleHeartbeatIntervalNs;
    RawCaptureFailureSink failure_sink{};
    // A null function selects std::chrono::steady_clock. Tests can inject a
    // deterministic monotonic clock without placing a virtual call in the
    // hot loop.
    RawCaptureMonotonicNow monotonic_now = nullptr;
    void* monotonic_clock_context = nullptr;
    // Optional producer append frontier.  The callback bound to the same
    // page must publish captured before this sole writer publishes append.
    l2flow::canonical::SourceFrontierPageV1* source_frontier = nullptr;
    l2flow::common::Identity128 frontier_writer_instance{};
    std::uint64_t frontier_generation = 0U;
    std::chrono::nanoseconds source_frontier_busy_timeout =
        l2flow::canonical::kSourceFrontierDefaultBusyTimeoutV1;
};

struct RawCaptureProgress final {
    std::uint64_t records = 0U;
    std::uint64_t vendor_bytes = 0U;
    std::uint64_t framed_wal_bytes = 0U;
    std::uint64_t last_ingress_sequence = 0U;

    [[nodiscard]] friend constexpr bool operator==(
        const RawCaptureProgress&,
        const RawCaptureProgress&) noexcept = default;
};

enum class RawCaptureWorkerFailureKind : std::uint8_t {
    kNone = 0U,
    kRunAlreadyStarted,
    kWriterNotReady,
    kRingCorruption,
    kWriterAppend,
    kWriterFlush,
    kWriterSeal,
    kProgressOverflow,
    kClockRegression,
    kSourceFrontier,
};

struct RawCaptureWorkerSnapshot final {
    RawCaptureProgress append;
    RawCaptureProgress durable;
    RawCaptureWorkerFailureKind failure_kind =
        RawCaptureWorkerFailureKind::kNone;
    RawWalFailure writer_failure{};
    bool stop_requested = false;
    bool startup_complete = false;
    bool startup_succeeded = false;
    bool finished = false;
    bool emergency_pause_requested = false;
    bool emergency_paused = false;
    bool emergency_abandoned = false;
};

struct RawCaptureReconciliation final {
    std::uint64_t callback_records = 0U;
    std::uint64_t append_records = 0U;
    std::uint64_t durable_records = 0U;
    std::uint64_t callback_vendor_bytes = 0U;
    std::uint64_t append_vendor_bytes = 0U;
    std::uint64_t durable_vendor_bytes = 0U;

    [[nodiscard]] bool append_exact() const noexcept {
        return callback_records == append_records &&
               callback_vendor_bytes == append_vendor_bytes;
    }

    [[nodiscard]] bool durable_exact() const noexcept {
        return callback_records == durable_records &&
               callback_vendor_bytes == durable_vendor_bytes;
    }

    [[nodiscard]] bool exact() const noexcept {
        return append_exact() && durable_exact();
    }
};

// The sole ByteRing consumer for Phase 2.
//
// The owner initializes RawWalWriter before starting Run(), quiesces the SDK
// callback producer before StopAndDrain(), and joins the Run() thread before
// destroying the ring or writer. Run() publishes append progress only after
// RawWalWriter has completed a record trailer, and durable progress only after
// the segment-sync -> marker-write -> journal-sync protocol succeeds.
class RawCaptureWorker final {
public:
    RawCaptureWorker(
        RawCaptureWorkerConfig config,
        ByteRing& ring,
        RawWalSink& writer);
    ~RawCaptureWorker() = default;

    RawCaptureWorker(const RawCaptureWorker&) = delete;
    RawCaptureWorker& operator=(const RawCaptureWorker&) = delete;
    RawCaptureWorker(RawCaptureWorker&&) = delete;
    RawCaptureWorker& operator=(RawCaptureWorker&&) = delete;

    // Exactly one call is permitted. A clean return means exact ring drain,
    // final durability publication, and a sealed/closed Raw segment.
    [[nodiscard]] bool Run() noexcept;

    // Release-publish this only after the callback producer is quiescent.
    void StopAndDrain() noexcept;

    // Emergency stop is a different lifecycle from StopAndDrain(). The
    // request lets the current bounded writer operation finish, then parks
    // the sole consumer at a record boundary without draining or sealing the
    // queued suffix. WaitForEmergencyPause is the acquire barrier used before
    // an ACK snapshot is frozen.
    [[nodiscard]] bool RequestEmergencyPause() noexcept;
    [[nodiscard]] bool WaitForEmergencyPause(
        std::chrono::milliseconds timeout) noexcept;

    // Ends a paused Run() without touching the ring or writer. This is used
    // before a capability-gated finalization executor takes sole ownership,
    // and by teardown when no such executor will run.
    void AbandonEmergencyPause() noexcept;

    [[nodiscard]] bool stop_requested() const noexcept;
    [[nodiscard]] bool startup_complete() const noexcept;
    [[nodiscard]] bool startup_succeeded() const noexcept;
    [[nodiscard]] bool finished() const noexcept;
    [[nodiscard]] RawCaptureWorkerSnapshot Snapshot() const noexcept;
    [[nodiscard]] RawCaptureReconciliation Reconcile(
        const CaptureMetricsSnapshot& callback) const noexcept;

private:
    [[nodiscard]] std::uint64_t MonotonicNowNs() const noexcept;
    [[nodiscard]] bool ConsumeRecord() noexcept;
    [[nodiscard]] bool MaybeFlushDurable(
        std::uint64_t now_ns,
        bool force) noexcept;
    [[nodiscard]] bool FinishCleanly() noexcept;
    [[nodiscard]] bool AddAppendProgress(
        std::uint64_t vendor_bytes,
        std::uint64_t framed_wal_bytes,
        std::uint64_t ingress_sequence) noexcept;
    [[nodiscard]] bool WaitForCapturedFrontier(
        const CaptureMetaV1& metadata) noexcept;
    [[nodiscard]] bool PublishAppendFrontier(
        const CaptureMetaV1& metadata,
        const RawWalCursor& append) noexcept;
    void PublishProgress(
        const RawCaptureProgress& append,
        const RawCaptureProgress& durable) noexcept;
    void Trip(
        RawCaptureWorkerFailureKind kind,
        RawCaptureFatalSignal signal) noexcept;
    void TripWriter(
        RawCaptureWorkerFailureKind kind) noexcept;
    void MarkFinished() noexcept;

    RawCaptureWorkerConfig config_;
    ByteRing& ring_;
    RawWalSink& writer_;
    ByteRingRecord record_;
    RawCaptureProgress append_progress_{};
    RawCaptureProgress durable_progress_{};
    std::uint64_t last_flush_monotonic_ns_ = 0U;
    bool have_flush_clock_ = false;

    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> run_started_{false};
    std::atomic<bool> startup_complete_{false};
    std::atomic<bool> startup_succeeded_{false};
    std::atomic<bool> finished_{false};
    std::atomic<bool> emergency_pause_requested_{false};
    std::atomic<bool> emergency_paused_{false};
    std::atomic<bool> emergency_abandon_requested_{false};
    std::atomic<bool> emergency_abandoned_{false};
    std::atomic<std::uint8_t> failure_kind_{
        static_cast<std::uint8_t>(
            RawCaptureWorkerFailureKind::kNone)};
    std::atomic<std::uint8_t> writer_failure_kind_{
        static_cast<std::uint8_t>(
            RawWalFailureKind::kNone)};
    std::atomic<int> writer_failure_errno_{0};

    // Even generations are stable. All eight progress fields form one
    // coherent publication; observers never pair a new durable count with an
    // old durable byte total.
    std::atomic<std::uint64_t> progress_generation_{0U};
    std::atomic<std::uint64_t> append_records_{0U};
    std::atomic<std::uint64_t> append_vendor_bytes_{0U};
    std::atomic<std::uint64_t> append_framed_wal_bytes_{0U};
    std::atomic<std::uint64_t> append_last_ingress_sequence_{0U};
    std::atomic<std::uint64_t> durable_records_{0U};
    std::atomic<std::uint64_t> durable_vendor_bytes_{0U};
    std::atomic<std::uint64_t> durable_framed_wal_bytes_{0U};
    std::atomic<std::uint64_t> durable_last_ingress_sequence_{0U};
};

}  // namespace l2flow::ingress
