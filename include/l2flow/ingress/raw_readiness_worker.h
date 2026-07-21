#pragma once

#include "l2flow/ingress/raw_live_tail.h"
#include "l2flow/ingress/raw_readiness_observer.h"

#include <atomic>
#include <cstdint>
#include <mutex>

namespace l2flow::ingress {

using RawReadinessWorkerMonotonicNow =
    std::uint64_t (*)(void* context) noexcept;

enum class RawReadinessWorkerFailureKind : std::uint8_t {
    kNone = 0U,
    kRunAlreadyStarted,
    kGenerationRejected,
    kObserveRejected,
    kHeartbeatRejected,
    kLiveTailInstanceChanged,
    kLiveTailRecordIdentityMismatch,
    kLiveTailFailure,
    kStopTargetInvalid,
    kStopCursorExceeded,
    kStopCatchUpTimedOut,
    kAborted,
};

using RawReadinessWorkerFailureCallback = void (*)(
    void* context,
    RawReadinessWorkerFailureKind failure) noexcept;

struct RawReadinessWorkerConfig final {
    RawReadinessWorkerMonotonicNow monotonic_now = nullptr;
    void* monotonic_clock_context = nullptr;
    // Bounds the final append-visible catch-up after StopAt() is observed by
    // the worker. RawLiveTail::Next() is nonblocking, so this also bounds the
    // owner's readiness-thread join in a conforming source implementation.
    std::uint64_t final_catch_up_timeout_ns =
        5U * 1'000'000'000U;
    RawReadinessWorkerFailureCallback failure_callback = nullptr;
    void* failure_context = nullptr;
};

// Exact, exclusive Raw frontier sampled only after the capture worker has
// drained and the final sink has sealed. A numeric WAL position alone is not
// sufficient: the namespace, writer generation, segment-local cursor, and
// ingress frontier must all describe the same final sink state.
struct RawReadinessStopCursorV1 final {
    l2flow::common::Identity128 writer_instance{};
    l2flow::common::Identity128 stream_day_id{};
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    std::uint32_t segment_sequence = 0U;
    std::uint64_t global_wal_pos = 0U;
    // Last accepted ingress sequence at global_wal_pos. It is zero for a
    // stream-day whose current segment contains no records.
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t segment_offset = 0U;

    [[nodiscard]] friend constexpr bool operator==(
        const RawReadinessStopCursorV1&,
        const RawReadinessStopCursorV1&) noexcept = default;
};

struct RawReadinessWorkerSnapshot final {
    RawReadinessWorkerFailureKind failure_kind =
        RawReadinessWorkerFailureKind::kNone;
    RawLiveTailError live_tail_error =
        RawLiveTailError::kNone;
    RawReadinessObserveResult observe_result =
        RawReadinessObserveResult::kProcessed;
    RawReadinessStopCursorV1 requested_stop{};
    std::uint64_t processed_wal_pos = 0U;
    std::uint64_t processed_ingress_sequence = 0U;
    std::uint32_t processed_segment_sequence = 0U;
    std::uint64_t processed_segment_offset = 0U;
    std::uint32_t live_tail_segment_sequence = 0U;
    std::uint64_t live_tail_segment_offset = 0U;
    std::uint64_t live_tail_next_ingress_sequence = 0U;
    bool startup_complete = false;
    bool startup_succeeded = false;
    bool stop_requested = false;
    bool finished = false;
};

// Single-consumer bridge between append-visible RawLiveTail and the temporary
// Phase-2 readiness observer. The owner starts this worker before SDK
// Connect. On clean shutdown it first quiesces callbacks and drains/seals the
// Raw writer, then requests the exact final append cursor and joins this
// worker. A successful return proves that observer evidence caught up exactly
// to that cursor; it does not create an authoritative Phase-3 epoch.
class RawReadinessWorker final {
public:
    RawReadinessWorker(
        RawReadinessWorkerConfig config,
        RawLiveTail& tail,
        RawReadinessObserver& observer,
        RawReadinessObserverGeneration generation);

    RawReadinessWorker(
        const RawReadinessWorker&) = delete;
    RawReadinessWorker& operator=(
        const RawReadinessWorker&) = delete;
    RawReadinessWorker(
        RawReadinessWorker&&) = delete;
    RawReadinessWorker& operator=(
        RawReadinessWorker&&) = delete;

    [[nodiscard]] bool Run() noexcept;

    // final_cursor is sampled after the Raw sink has sealed cleanly. It is
    // immutable after the first successful call. An identical retry is
    // idempotent; a conflicting retry is fatal.
    [[nodiscard]] bool StopAt(
        const RawReadinessStopCursorV1&
            final_cursor) noexcept;
    void Abort() noexcept;

    [[nodiscard]] RawReadinessWorkerSnapshot
    Snapshot() const noexcept;

private:
    [[nodiscard]] std::uint64_t MonotonicNowNs()
        const noexcept;
    [[nodiscard]] bool PublishHeartbeat(
        std::uint64_t now_ns) noexcept;
    [[nodiscard]] bool StopReached() noexcept;
    void Trip(
        RawReadinessWorkerFailureKind kind,
        RawLiveTailError live_tail_error =
            RawLiveTailError::kNone,
        RawReadinessObserveResult observe_result =
            RawReadinessObserveResult::kProcessed) noexcept;

    RawReadinessWorkerConfig config_{};
    RawLiveTail& tail_;
    RawReadinessObserver& observer_;
    RawReadinessObserverGeneration generation_{};

    std::atomic<bool> run_started_{false};
    std::atomic<bool> startup_complete_{false};
    std::atomic<bool> startup_succeeded_{false};
    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> finished_{false};
    std::atomic<bool> fatal_{false};
    std::atomic<std::uint64_t> processed_wal_pos_{0U};
    std::atomic<std::uint64_t>
        processed_ingress_sequence_{0U};
    std::atomic<std::uint32_t>
        processed_segment_sequence_{0U};
    std::atomic<std::uint64_t>
        processed_segment_offset_{0U};
    std::atomic<std::uint32_t>
        live_tail_segment_sequence_{0U};
    std::atomic<std::uint64_t>
        live_tail_segment_offset_{0U};
    std::atomic<std::uint64_t>
        live_tail_next_ingress_sequence_{0U};
    mutable std::mutex stop_mutex_;
    RawReadinessStopCursorV1 stop_cursor_{};
    std::atomic_flag trip_claimed_ = ATOMIC_FLAG_INIT;
    std::atomic<std::uint8_t> failure_kind_{
        static_cast<std::uint8_t>(
            RawReadinessWorkerFailureKind::kNone)};
    std::atomic<std::uint8_t> live_tail_error_{
        static_cast<std::uint8_t>(
            RawLiveTailError::kNone)};
    std::atomic<std::uint8_t> observe_result_{
        static_cast<std::uint8_t>(
            RawReadinessObserveResult::kProcessed)};
};

}  // namespace l2flow::ingress
