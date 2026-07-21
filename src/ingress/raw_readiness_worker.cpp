#include "l2flow/ingress/raw_readiness_worker.h"

#include <chrono>
#include <limits>
#include <stdexcept>
#include <thread>
#include <utility>

namespace l2flow::ingress {

namespace {

bool IsValidStopCursor(
    const RawReadinessStopCursorV1& cursor,
    const RawReadinessObserverGeneration&
        generation) noexcept {
    if (generation.recovery_next_ingress_sequence ==
            0U ||
        generation.recovery_segment_sequence == 0U ||
        generation.recovery_segment_offset <
            kRawV1SegmentHeaderBytes ||
        generation.recovery_wal_pos <
            generation.recovery_segment_offset) {
        return false;
    }
    const std::uint64_t recovery_ingress_sequence =
        generation.recovery_next_ingress_sequence - 1U;
    return
        !l2flow::common::IsZeroIdentity(
            cursor.writer_instance) &&
        !l2flow::common::IsZeroIdentity(
            cursor.stream_day_id) &&
        cursor.writer_instance ==
            generation.writer_instance &&
        cursor.stream_day_id ==
            generation.stream_day_id &&
        cursor.source_stream_id != 0U &&
        cursor.source_stream_id ==
            generation.source_stream_id &&
        cursor.capture_date != 0U &&
        cursor.capture_date ==
            generation.capture_date &&
        cursor.segment_sequence != 0U &&
        cursor.global_wal_pos >=
            kRawV1SegmentHeaderBytes &&
        cursor.segment_offset >=
            kRawV1SegmentHeaderBytes &&
        cursor.global_wal_pos >=
            cursor.segment_offset &&
        cursor.global_wal_pos >=
            generation.recovery_wal_pos &&
        cursor.segment_sequence >=
            generation.recovery_segment_sequence &&
        (cursor.segment_sequence !=
             generation.recovery_segment_sequence ||
         cursor.segment_offset >=
             generation.recovery_segment_offset) &&
        cursor.ingress_sequence >=
            recovery_ingress_sequence &&
        cursor.ingress_sequence !=
            std::numeric_limits<std::uint64_t>::max();
}

}  // namespace

RawReadinessWorker::RawReadinessWorker(
    RawReadinessWorkerConfig config,
    RawLiveTail& tail,
    RawReadinessObserver& observer,
    RawReadinessObserverGeneration generation)
    : config_(config),
      tail_(tail),
      observer_(observer),
      generation_(std::move(generation)) {
    if (config_.failure_callback == nullptr ||
        config_.final_catch_up_timeout_ns == 0U) {
        throw std::invalid_argument(
            "Raw readiness worker config is invalid");
    }
    live_tail_segment_sequence_.store(
        tail_.segment_sequence(),
        std::memory_order_relaxed);
    live_tail_segment_offset_.store(
        tail_.segment_offset(),
        std::memory_order_relaxed);
    live_tail_next_ingress_sequence_.store(
        tail_.next_ingress_sequence(),
        std::memory_order_relaxed);
}

bool RawReadinessWorker::Run() noexcept {
    bool expected = false;
    if (!run_started_.compare_exchange_strong(
            expected,
            true,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        Trip(
            RawReadinessWorkerFailureKind::
                kRunAlreadyStarted);
        return false;
    }

    const std::uint64_t started_ns = MonotonicNowNs();
    bool success =
        observer_.BeginGeneration(
            generation_, started_ns);
    if (!success) {
        Trip(
            RawReadinessWorkerFailureKind::
                kGenerationRejected);
    } else {
        processed_wal_pos_.store(
            generation_.recovery_wal_pos,
            std::memory_order_release);
        processed_ingress_sequence_.store(
            generation_
                    .recovery_next_ingress_sequence -
                1U,
            std::memory_order_release);
        processed_segment_sequence_.store(
            generation_.recovery_segment_sequence,
            std::memory_order_release);
        processed_segment_offset_.store(
            generation_.recovery_segment_offset,
            std::memory_order_release);
    }
    startup_succeeded_.store(
        success, std::memory_order_relaxed);
    startup_complete_.store(
        true, std::memory_order_release);

    bool stop_timer_started = false;
    std::uint64_t stop_started_ns = 0U;
    while (success &&
           !fatal_.load(std::memory_order_acquire)) {
        const std::uint64_t now_ns = MonotonicNowNs();
        RawLiveTailStep step = tail_.Next();
        live_tail_segment_sequence_.store(
            tail_.segment_sequence(),
            std::memory_order_release);
        live_tail_segment_offset_.store(
            tail_.segment_offset(),
            std::memory_order_release);
        live_tail_next_ingress_sequence_.store(
            tail_.next_ingress_sequence(),
            std::memory_order_release);
        if (step.kind == RawLiveTailStepKind::kRecord) {
            if (!step.record.has_value()) {
                Trip(
                    RawReadinessWorkerFailureKind::
                        kLiveTailFailure,
                    RawLiveTailError::kRecordInvalid);
                success = false;
                break;
            }
            if (step.record->writer_instance !=
                    generation_.writer_instance) {
                Trip(
                    RawReadinessWorkerFailureKind::
                        kLiveTailInstanceChanged,
                    RawLiveTailError::
                        kInstanceChanged);
                success = false;
                break;
            }
            if (step.record->segment.stream_day_id !=
                    generation_.stream_day_id ||
                step.record->segment.source_stream_id !=
                    generation_.source_stream_id ||
                step.record->segment.capture_date !=
                    generation_.capture_date ||
                step.record->segment.segment_sequence !=
                    tail_.segment_sequence()) {
                Trip(
                    RawReadinessWorkerFailureKind::
                        kLiveTailRecordIdentityMismatch,
                    RawLiveTailError::
                        kSegmentIdentityMismatch);
                success = false;
                break;
            }
            const RawReadinessObserveResult observed =
                observer_.Observe(
                    step.record->view,
                    generation_.writer_instance,
                    now_ns);
            if (observed !=
                RawReadinessObserveResult::kProcessed) {
                Trip(
                    RawReadinessWorkerFailureKind::
                        kObserveRejected,
                    RawLiveTailError::kNone,
                    observed);
                success = false;
                break;
            }
            const RawReadinessObserverSnapshot snapshot =
                observer_.Snapshot();
            processed_wal_pos_.store(
                snapshot.observer_processed_wal_pos,
                std::memory_order_release);
            processed_ingress_sequence_.store(
                snapshot
                    .observer_processed_ingress_sequence,
                std::memory_order_release);
            processed_segment_sequence_.store(
                snapshot
                    .observer_processed_segment_sequence,
                std::memory_order_release);
            processed_segment_offset_.store(
                snapshot
                    .observer_processed_segment_offset,
                std::memory_order_release);
        } else if (
            step.kind ==
            RawLiveTailStepKind::kSegmentTransition) {
            if (!step.segment_transition.has_value()) {
                Trip(
                    RawReadinessWorkerFailureKind::
                        kLiveTailFailure,
                    RawLiveTailError::
                        kSegmentOrderViolation);
                success = false;
                break;
            }
            const RawLiveSegmentTransitionV1&
                transition =
                    *step.segment_transition;
            if (transition.writer_instance !=
                    generation_.writer_instance ||
                transition.previous_segment
                        .stream_day_id !=
                    generation_.stream_day_id ||
                transition.next_segment.stream_day_id !=
                    generation_.stream_day_id ||
                transition.previous_segment
                        .source_stream_id !=
                    generation_.source_stream_id ||
                transition.next_segment
                        .source_stream_id !=
                    generation_.source_stream_id ||
                transition.previous_segment.capture_date !=
                    generation_.capture_date ||
                transition.next_segment.capture_date !=
                    generation_.capture_date ||
                transition.next_segment
                        .segment_sequence !=
                    tail_.segment_sequence()) {
                Trip(
                    RawReadinessWorkerFailureKind::
                        kLiveTailRecordIdentityMismatch,
                    RawLiveTailError::
                        kSegmentIdentityMismatch);
                success = false;
                break;
            }
            const RawReadinessObserveResult observed =
                observer_.ObserveSegmentTransition(
                    transition, now_ns);
            if (observed !=
                RawReadinessObserveResult::kProcessed) {
                Trip(
                    RawReadinessWorkerFailureKind::
                        kObserveRejected,
                    RawLiveTailError::kNone,
                    observed);
                success = false;
                break;
            }
            const RawReadinessObserverSnapshot snapshot =
                observer_.Snapshot();
            processed_wal_pos_.store(
                snapshot.observer_processed_wal_pos,
                std::memory_order_release);
            processed_ingress_sequence_.store(
                snapshot
                    .observer_processed_ingress_sequence,
                std::memory_order_release);
            processed_segment_sequence_.store(
                snapshot
                    .observer_processed_segment_sequence,
                std::memory_order_release);
            processed_segment_offset_.store(
                snapshot
                    .observer_processed_segment_offset,
                std::memory_order_release);
        } else if (
            step.kind ==
            RawLiveTailStepKind::kInstanceChanged) {
            Trip(
                RawReadinessWorkerFailureKind::
                    kLiveTailInstanceChanged,
                step.error);
            success = false;
            break;
        } else if (
            step.kind == RawLiveTailStepKind::kError) {
            Trip(
                RawReadinessWorkerFailureKind::
                    kLiveTailFailure,
                step.error);
            success = false;
            break;
        } else if (
            step.kind != RawLiveTailStepKind::kWouldBlock &&
            step.kind != RawLiveTailStepKind::kEnd) {
            Trip(
                RawReadinessWorkerFailureKind::
                    kLiveTailFailure,
                RawLiveTailError::kRecordInvalid);
            success = false;
            break;
        }

        if (!PublishHeartbeat(now_ns)) {
            success = false;
            break;
        }
        if (StopReached()) {
            break;
        }
        if (stop_requested_.load(
                std::memory_order_acquire)) {
            if (!stop_timer_started) {
                stop_timer_started = true;
                stop_started_ns = now_ns;
            } else if (
                now_ns >= stop_started_ns &&
                now_ns - stop_started_ns >=
                    config_
                        .final_catch_up_timeout_ns) {
                Trip(
                    RawReadinessWorkerFailureKind::
                        kStopCatchUpTimedOut);
                success = false;
                break;
            }
        }
        if (step.kind != RawLiveTailStepKind::kRecord) {
            std::this_thread::yield();
        }
    }

    success =
        success &&
        !fatal_.load(std::memory_order_acquire) &&
        stop_requested_.load(std::memory_order_acquire) &&
        StopReached();
    finished_.store(true, std::memory_order_release);
    return success;
}

bool RawReadinessWorker::StopAt(
    const RawReadinessStopCursorV1&
        final_cursor) noexcept {
    if (!IsValidStopCursor(
            final_cursor, generation_)) {
        Trip(
            RawReadinessWorkerFailureKind::
                kStopTargetInvalid);
        return false;
    }
    bool conflicting_retry = false;
    {
        std::lock_guard<std::mutex> lock(
            stop_mutex_);
        if (stop_requested_.load(
                std::memory_order_acquire)) {
            if (stop_cursor_ != final_cursor) {
                conflicting_retry = true;
            } else {
                return !fatal_.load(
                    std::memory_order_acquire);
            }
        } else {
            stop_cursor_ = final_cursor;
            stop_requested_.store(
                true, std::memory_order_release);
        }
    }
    if (conflicting_retry) {
        Trip(
            RawReadinessWorkerFailureKind::
                kStopTargetInvalid);
        return false;
    }
    return !fatal_.load(std::memory_order_acquire);
}

void RawReadinessWorker::Abort() noexcept {
    Trip(RawReadinessWorkerFailureKind::kAborted);
}

RawReadinessWorkerSnapshot
RawReadinessWorker::Snapshot() const noexcept {
    RawReadinessWorkerSnapshot result;
    result.failure_kind =
        static_cast<RawReadinessWorkerFailureKind>(
            failure_kind_.load(
                std::memory_order_acquire));
    result.live_tail_error =
        static_cast<RawLiveTailError>(
            live_tail_error_.load(
                std::memory_order_relaxed));
    result.observe_result =
        static_cast<RawReadinessObserveResult>(
            observe_result_.load(
                std::memory_order_relaxed));
    result.stop_requested =
        stop_requested_.load(
            std::memory_order_acquire);
    if (result.stop_requested) {
        result.requested_stop = stop_cursor_;
    }
    result.processed_wal_pos =
        processed_wal_pos_.load(
            std::memory_order_acquire);
    result.processed_ingress_sequence =
        processed_ingress_sequence_.load(
            std::memory_order_acquire);
    result.processed_segment_sequence =
        processed_segment_sequence_.load(
            std::memory_order_acquire);
    result.processed_segment_offset =
        processed_segment_offset_.load(
            std::memory_order_acquire);
    result.live_tail_segment_sequence =
        live_tail_segment_sequence_.load(
            std::memory_order_acquire);
    result.live_tail_segment_offset =
        live_tail_segment_offset_.load(
            std::memory_order_acquire);
    result.live_tail_next_ingress_sequence =
        live_tail_next_ingress_sequence_.load(
            std::memory_order_acquire);
    result.startup_complete =
        startup_complete_.load(
            std::memory_order_acquire);
    result.startup_succeeded =
        result.startup_complete &&
        startup_succeeded_.load(
            std::memory_order_relaxed);
    result.finished =
        finished_.load(std::memory_order_acquire);
    return result;
}

std::uint64_t
RawReadinessWorker::MonotonicNowNs() const noexcept {
    if (config_.monotonic_now != nullptr) {
        return config_.monotonic_now(
            config_.monotonic_clock_context);
    }
    const auto value =
        std::chrono::duration_cast<
            std::chrono::nanoseconds>(
            std::chrono::steady_clock::now()
                .time_since_epoch())
            .count();
    return value <= 0
               ? 0U
               : static_cast<std::uint64_t>(value);
}

bool RawReadinessWorker::PublishHeartbeat(
    std::uint64_t now_ns) noexcept {
    if (observer_.PublishHeartbeat(
            generation_.writer_instance, now_ns)) {
        return true;
    }
    Trip(
        RawReadinessWorkerFailureKind::
            kHeartbeatRejected);
    return false;
}

bool RawReadinessWorker::StopReached() noexcept {
    if (!stop_requested_.load(std::memory_order_acquire)) {
        return false;
    }
    const RawReadinessStopCursorV1 target =
        stop_cursor_;
    const RawReadinessObserverSnapshot snapshot =
        observer_.Snapshot();
    const std::uint64_t processed_wal_pos =
        snapshot.observer_processed_wal_pos;
    processed_wal_pos_.store(
        processed_wal_pos, std::memory_order_release);
    const std::uint64_t processed_ingress_sequence =
        snapshot.observer_processed_ingress_sequence;
    processed_ingress_sequence_.store(
        processed_ingress_sequence,
        std::memory_order_release);
    processed_segment_sequence_.store(
        snapshot.observer_processed_segment_sequence,
        std::memory_order_release);
    processed_segment_offset_.store(
        snapshot.observer_processed_segment_offset,
        std::memory_order_release);

    const std::uint32_t tail_segment_sequence =
        tail_.segment_sequence();
    const std::uint64_t tail_segment_offset =
        tail_.segment_offset();
    const std::uint64_t
        tail_next_ingress_sequence =
            tail_.next_ingress_sequence();
    live_tail_segment_sequence_.store(
        tail_segment_sequence,
        std::memory_order_release);
    live_tail_segment_offset_.store(
        tail_segment_offset,
        std::memory_order_release);
    live_tail_next_ingress_sequence_.store(
        tail_next_ingress_sequence,
        std::memory_order_release);
    const bool tail_past_target =
        tail_segment_sequence >
            target.segment_sequence ||
        (tail_segment_sequence ==
             target.segment_sequence &&
         tail_segment_offset >
             target.segment_offset);
    if (processed_wal_pos >
            target.global_wal_pos ||
        processed_ingress_sequence >
            target.ingress_sequence ||
        snapshot.observer_processed_segment_sequence >
            target.segment_sequence ||
        (snapshot
                 .observer_processed_segment_sequence ==
             target.segment_sequence &&
         snapshot.observer_processed_segment_offset >
             target.segment_offset) ||
        tail_past_target ||
        tail_next_ingress_sequence >
            target.ingress_sequence + 1U) {
        Trip(
            RawReadinessWorkerFailureKind::
                kStopCursorExceeded);
        return false;
    }
    return
        processed_wal_pos ==
            target.global_wal_pos &&
        processed_ingress_sequence ==
            target.ingress_sequence &&
        snapshot.observer_processed_segment_sequence ==
            target.segment_sequence &&
        snapshot.observer_processed_segment_offset ==
            target.segment_offset &&
        tail_segment_sequence ==
            target.segment_sequence &&
        tail_segment_offset ==
            target.segment_offset &&
        tail_next_ingress_sequence ==
            target.ingress_sequence + 1U;
}

void RawReadinessWorker::Trip(
    RawReadinessWorkerFailureKind kind,
    RawLiveTailError live_tail_error,
    RawReadinessObserveResult observe_result) noexcept {
    if (!trip_claimed_.test_and_set(
            std::memory_order_acq_rel)) {
        live_tail_error_.store(
            static_cast<std::uint8_t>(
                live_tail_error),
            std::memory_order_relaxed);
        observe_result_.store(
            static_cast<std::uint8_t>(
                observe_result),
            std::memory_order_relaxed);
        // Publish the complete first-failure detail before the kind. A
        // Snapshot() acquire-load of failure_kind_ therefore cannot observe
        // a non-kNone kind paired with stale detail from the initializer.
        failure_kind_.store(
            static_cast<std::uint8_t>(kind),
            std::memory_order_release);
        fatal_.store(true, std::memory_order_release);
        config_.failure_callback(
            config_.failure_context, kind);
    }
}

}  // namespace l2flow::ingress
