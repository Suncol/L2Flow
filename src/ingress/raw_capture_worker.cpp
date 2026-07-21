#include "l2flow/ingress/raw_capture_worker.h"

#include "l2flow/ingress/raw_v1.h"

#include <chrono>
#include <limits>
#include <stdexcept>
#include <thread>
#include <utility>

namespace l2flow::ingress {
namespace {

[[nodiscard]] bool CheckedAdd(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (right >
        std::numeric_limits<std::uint64_t>::max() - left) {
        return false;
    }
    *result = left + right;
    return true;
}

[[nodiscard]] bool SameCursor(
    const RawWalCursor& left,
    const RawWalCursor& right) noexcept {
    return left == right;
}

}  // namespace

RawCaptureWorker::RawCaptureWorker(
    RawCaptureWorkerConfig config,
    ByteRing& ring,
    RawWalSink& writer)
    : config_(std::move(config)),
      ring_(ring),
      writer_(writer),
      record_(ring.max_body_bytes()) {
    if (config_.durable_interval_ns == 0U ||
        config_.durable_batch_bytes == 0U ||
        config_.failure_sink.callback == nullptr ||
        ring_.max_message_bytes() <
            kVendorMessageHeadBytes) {
        throw std::invalid_argument(
            "invalid Raw capture worker configuration");
    }
}

bool RawCaptureWorker::Run() noexcept {
    bool expected = false;
    if (!run_started_.compare_exchange_strong(
            expected,
            true,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        Trip(
            RawCaptureWorkerFailureKind::kRunAlreadyStarted,
            RawCaptureFatalSignal::kRawWalIo);
        return false;
    }

    const RawWalWriterSnapshot initial = writer_.Snapshot();
    bool success =
        initial.initialized && !initial.sealed &&
        !initial.closed && !initial.fatal &&
        SameCursor(initial.append, initial.durable);
    if (!success) {
        TripWriter(
            RawCaptureWorkerFailureKind::kWriterNotReady);
    } else {
        // A rotated segment begins at the preceding segment's sealed
        // ingress sequence even though this worker has appended zero local
        // records. Preserve that cursor baseline so an empty rotated segment
        // can still be flushed and sealed coherently.
        append_progress_.last_ingress_sequence =
            initial.append.ingress_sequence;
        durable_progress_.last_ingress_sequence =
            initial.durable.ingress_sequence;
        PublishProgress(
            append_progress_, durable_progress_);
        last_flush_monotonic_ns_ = MonotonicNowNs();
        have_flush_clock_ = true;
    }

    startup_succeeded_.store(
        success, std::memory_order_relaxed);
    startup_complete_.store(true, std::memory_order_release);

    bool drained = false;
    bool emergency_abandoned = false;
    try {
        while (success) {
            if (emergency_pause_requested_.load(
                    std::memory_order_acquire)) {
                emergency_paused_.store(
                    true, std::memory_order_release);
                for (;;) {
                    if (emergency_abandon_requested_.load(
                            std::memory_order_acquire)) {
                        emergency_abandoned = true;
                        break;
                    }
                    if (stop_requested_.load(
                            std::memory_order_acquire)) {
                        break;
                    }
                    std::this_thread::yield();
                }
                emergency_paused_.store(
                    false, std::memory_order_release);
                if (emergency_abandoned) {
                    break;
                }
                emergency_pause_requested_.store(
                    false, std::memory_order_release);
            }

            ByteRingPopResult result = ring_.try_pop(record_);
            if (result == ByteRingPopResult::EMPTY) {
                const std::uint64_t now = MonotonicNowNs();
                if (!MaybeFlushDurable(now, false)) {
                    success = false;
                    break;
                }
                if (!stop_requested_.load(
                        std::memory_order_acquire)) {
                    std::this_thread::yield();
                    continue;
                }

                // The stop acquire follows callback quiescence. Re-reading
                // the independent ring publication prevents an EMPTY
                // observation made before that acquire from ending the
                // drain.
                result = ring_.try_pop(record_);
                if (result == ByteRingPopResult::EMPTY) {
                    drained = true;
                    break;
                }
            }

            if (result == ByteRingPopResult::CORRUPT ||
                result ==
                    ByteRingPopResult::OUTPUT_TOO_SMALL) {
                Trip(
                    RawCaptureWorkerFailureKind::
                        kRingCorruption,
                    RawCaptureFatalSignal::
                        kRingCorruption);
                success = false;
                break;
            }
            if (result != ByteRingPopResult::RECORD) {
                Trip(
                    RawCaptureWorkerFailureKind::
                        kRingCorruption,
                    RawCaptureFatalSignal::
                        kRingCorruption);
                success = false;
                break;
            }

            if (!ConsumeRecord() ||
                !MaybeFlushDurable(
                    MonotonicNowNs(), false)) {
                success = false;
                break;
            }
        }
    } catch (...) {
        Trip(
            RawCaptureWorkerFailureKind::kRingCorruption,
            RawCaptureFatalSignal::kRingCorruption);
        success = false;
    }

    if (success && !emergency_abandoned) {
        if (!drained) {
            Trip(
                RawCaptureWorkerFailureKind::
                    kRingCorruption,
                RawCaptureFatalSignal::kRingCorruption);
            success = false;
        } else {
            success = FinishCleanly();
        }
    }
    if (emergency_abandoned) {
        emergency_abandoned_.store(
            true, std::memory_order_release);
        success = false;
    }

    MarkFinished();
    return success;
}

void RawCaptureWorker::StopAndDrain() noexcept {
    stop_requested_.store(true, std::memory_order_release);
}

bool RawCaptureWorker::RequestEmergencyPause() noexcept {
    if (!run_started_.load(std::memory_order_acquire) ||
        !startup_complete() || !startup_succeeded() ||
        finished() ||
        stop_requested_.load(std::memory_order_acquire) ||
        emergency_abandon_requested_.load(
            std::memory_order_acquire) ||
        failure_kind_.load(std::memory_order_acquire) !=
            static_cast<std::uint8_t>(
                RawCaptureWorkerFailureKind::kNone)) {
        return false;
    }
    bool expected = false;
    return emergency_pause_requested_.compare_exchange_strong(
        expected,
        true,
        std::memory_order_acq_rel,
        std::memory_order_acquire);
}

bool RawCaptureWorker::WaitForEmergencyPause(
    std::chrono::milliseconds timeout) noexcept {
    if (timeout < std::chrono::milliseconds::zero() ||
        !emergency_pause_requested_.load(
            std::memory_order_acquire)) {
        return false;
    }
    using Clock = std::chrono::steady_clock;
    const Clock::time_point now = Clock::now();
    const auto remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            Clock::time_point::max() - now);
    const Clock::time_point deadline =
        timeout >= remaining
            ? Clock::time_point::max()
            : now + std::chrono::duration_cast<Clock::duration>(
                        timeout);
    for (;;) {
        if (emergency_paused_.load(
                std::memory_order_acquire)) {
            return true;
        }
        if (finished() ||
            emergency_abandon_requested_.load(
                std::memory_order_acquire) ||
            Clock::now() >= deadline) {
            return false;
        }
        std::this_thread::yield();
    }
}

void RawCaptureWorker::AbandonEmergencyPause() noexcept {
    emergency_abandon_requested_.store(
        true, std::memory_order_release);
}

bool RawCaptureWorker::stop_requested() const noexcept {
    return stop_requested_.load(std::memory_order_acquire);
}

bool RawCaptureWorker::startup_complete() const noexcept {
    return startup_complete_.load(std::memory_order_acquire);
}

bool RawCaptureWorker::startup_succeeded() const noexcept {
    return startup_complete() &&
           startup_succeeded_.load(
               std::memory_order_relaxed);
}

bool RawCaptureWorker::finished() const noexcept {
    return finished_.load(std::memory_order_acquire);
}

RawCaptureWorkerSnapshot
RawCaptureWorker::Snapshot() const noexcept {
    RawCaptureWorkerSnapshot result;
    for (;;) {
        const std::uint64_t before =
            progress_generation_.load(
                std::memory_order_acquire);
        if ((before & 1U) != 0U) {
            continue;
        }
        result.append.records =
            append_records_.load(std::memory_order_relaxed);
        result.append.vendor_bytes =
            append_vendor_bytes_.load(
                std::memory_order_relaxed);
        result.append.framed_wal_bytes =
            append_framed_wal_bytes_.load(
                std::memory_order_relaxed);
        result.append.last_ingress_sequence =
            append_last_ingress_sequence_.load(
                std::memory_order_relaxed);
        result.durable.records =
            durable_records_.load(
                std::memory_order_relaxed);
        result.durable.vendor_bytes =
            durable_vendor_bytes_.load(
                std::memory_order_relaxed);
        result.durable.framed_wal_bytes =
            durable_framed_wal_bytes_.load(
                std::memory_order_relaxed);
        result.durable.last_ingress_sequence =
            durable_last_ingress_sequence_.load(
                std::memory_order_relaxed);
        std::atomic_thread_fence(
            std::memory_order_seq_cst);
        const std::uint64_t after =
            progress_generation_.load(
                std::memory_order_acquire);
        if (before == after && (after & 1U) == 0U) {
            break;
        }
    }

    result.failure_kind =
        static_cast<RawCaptureWorkerFailureKind>(
            failure_kind_.load(std::memory_order_acquire));
    result.writer_failure.kind =
        static_cast<RawWalFailureKind>(
            writer_failure_kind_.load(
                std::memory_order_relaxed));
    result.writer_failure.error_number =
        writer_failure_errno_.load(
            std::memory_order_relaxed);
    result.stop_requested =
        stop_requested_.load(std::memory_order_acquire);
    result.startup_complete =
        startup_complete_.load(std::memory_order_acquire);
    result.startup_succeeded =
        result.startup_complete &&
        startup_succeeded_.load(
            std::memory_order_relaxed);
    result.finished =
        finished_.load(std::memory_order_acquire);
    result.emergency_pause_requested =
        emergency_pause_requested_.load(
            std::memory_order_acquire);
    result.emergency_paused =
        emergency_paused_.load(std::memory_order_acquire);
    result.emergency_abandoned =
        emergency_abandoned_.load(
            std::memory_order_acquire);
    return result;
}

RawCaptureReconciliation RawCaptureWorker::Reconcile(
    const CaptureMetricsSnapshot& callback) const noexcept {
    const RawCaptureWorkerSnapshot sink = Snapshot();
    RawCaptureReconciliation result;
    result.callback_records = callback.captured_records;
    result.append_records = sink.append.records;
    result.durable_records = sink.durable.records;
    result.callback_vendor_bytes =
        callback.captured_vendor_bytes;
    result.append_vendor_bytes =
        sink.append.vendor_bytes;
    result.durable_vendor_bytes =
        sink.durable.vendor_bytes;
    return result;
}

std::uint64_t
RawCaptureWorker::MonotonicNowNs() const noexcept {
    if (config_.monotonic_now != nullptr) {
        return config_.monotonic_now(
            config_.monotonic_clock_context);
    }

    const auto count =
        std::chrono::duration_cast<
            std::chrono::nanoseconds>(
            std::chrono::steady_clock::now()
                .time_since_epoch())
            .count();
    if (count <= 0) {
        return 0U;
    }
    return static_cast<std::uint64_t>(count);
}

bool RawCaptureWorker::ConsumeRecord() noexcept {
    const RawWalWriterSnapshot before = writer_.Snapshot();
    RawRecordLayoutV1 layout{};
    if (ComputeRawRecordLayoutV1(
            record_.body.size(),
            &layout) != RawV1Error::kNone ||
        layout.record_size == 0U) {
        Trip(
            RawCaptureWorkerFailureKind::
                kProgressOverflow,
            RawCaptureFatalSignal::kRawWalIo);
        return false;
    }
    const RawWalRecordInputV1 input{
        record_.meta,
        std::span<const std::byte>(
            record_.head.data(), record_.head.size()),
        std::span<const std::byte>(
            record_.body.data(), record_.body.size())};
    if (!writer_.AppendRecord(input)) {
        TripWriter(
            RawCaptureWorkerFailureKind::kWriterAppend);
        return false;
    }

    const RawWalWriterSnapshot after = writer_.Snapshot();
    if (after.fatal ||
        after.append.ingress_sequence !=
            record_.meta.ingress_sequence ||
        after.append.global_wal_pos <=
            before.append.global_wal_pos) {
        TripWriter(
            RawCaptureWorkerFailureKind::kWriterAppend);
        return false;
    }

    const std::uint64_t global_delta =
        after.append.global_wal_pos -
        before.append.global_wal_pos;
    const std::uint64_t record_bytes =
        layout.record_size;
    std::uint64_t rotated_delta = 0U;
    const bool rotated_delta_valid =
        CheckedAdd(
            kRawV1SegmentHeaderBytes,
            record_bytes,
            &rotated_delta);
    const bool no_rotation =
        global_delta == record_bytes &&
        after.append.segment_offset >
            before.append.segment_offset &&
        after.append.segment_offset -
                before.append.segment_offset ==
            record_bytes;
    const bool one_rotation =
        rotated_delta_valid &&
        global_delta == rotated_delta &&
        after.append.segment_offset ==
            rotated_delta;
    if (!no_rotation && !one_rotation) {
        TripWriter(
            RawCaptureWorkerFailureKind::kWriterAppend);
        return false;
    }

    const std::size_t vendor_size =
        record_.head.size() + record_.body.size();
    if (vendor_size >
        static_cast<std::size_t>(
            std::numeric_limits<std::uint64_t>::max())) {
        Trip(
            RawCaptureWorkerFailureKind::
                kProgressOverflow,
            RawCaptureFatalSignal::kRawWalIo);
        return false;
    }
    return AddAppendProgress(
        static_cast<std::uint64_t>(vendor_size),
        record_bytes,
        record_.meta.ingress_sequence);
}

bool RawCaptureWorker::MaybeFlushDurable(
    std::uint64_t now_ns,
    bool force) noexcept {
    if (!have_flush_clock_) {
        last_flush_monotonic_ns_ = now_ns;
        have_flush_clock_ = true;
    } else if (now_ns < last_flush_monotonic_ns_) {
        Trip(
            RawCaptureWorkerFailureKind::kClockRegression,
            RawCaptureFatalSignal::kRawWalIo);
        return false;
    }

    if (append_progress_.framed_wal_bytes <
            durable_progress_.framed_wal_bytes ||
        append_progress_.records <
            durable_progress_.records ||
        append_progress_.vendor_bytes <
            durable_progress_.vendor_bytes) {
        Trip(
            RawCaptureWorkerFailureKind::
                kProgressOverflow,
            RawCaptureFatalSignal::kRawWalIo);
        return false;
    }
    const std::uint64_t pending_bytes =
        append_progress_.framed_wal_bytes -
        durable_progress_.framed_wal_bytes;
    const bool time_due =
        pending_bytes != 0U &&
        now_ns - last_flush_monotonic_ns_ >=
            config_.durable_interval_ns;
    const bool bytes_due =
        pending_bytes >= config_.durable_batch_bytes;
    if (!force && !time_due && !bytes_due) {
        return true;
    }

    if (!writer_.FlushDurable()) {
        TripWriter(
            RawCaptureWorkerFailureKind::kWriterFlush);
        return false;
    }
    const RawWalWriterSnapshot writer_snapshot =
        writer_.Snapshot();
    if (writer_snapshot.fatal ||
        !SameCursor(
            writer_snapshot.append,
            writer_snapshot.durable) ||
        writer_snapshot.durable.ingress_sequence !=
            append_progress_.last_ingress_sequence) {
        TripWriter(
            RawCaptureWorkerFailureKind::kWriterFlush);
        return false;
    }

    durable_progress_ = append_progress_;
    last_flush_monotonic_ns_ = now_ns;
    PublishProgress(
        append_progress_, durable_progress_);
    return true;
}

bool RawCaptureWorker::FinishCleanly() noexcept {
    if (!MaybeFlushDurable(MonotonicNowNs(), true)) {
        return false;
    }
    if (!writer_.SealAndClose()) {
        TripWriter(
            RawCaptureWorkerFailureKind::kWriterSeal);
        return false;
    }
    const RawWalWriterSnapshot sealed = writer_.Snapshot();
    if (sealed.fatal || !sealed.sealed || !sealed.closed ||
        !SameCursor(sealed.append, sealed.durable) ||
        sealed.durable.ingress_sequence !=
            durable_progress_.last_ingress_sequence) {
        TripWriter(
            RawCaptureWorkerFailureKind::kWriterSeal);
        return false;
    }
    return true;
}

bool RawCaptureWorker::AddAppendProgress(
    std::uint64_t vendor_bytes,
    std::uint64_t framed_wal_bytes,
    std::uint64_t ingress_sequence) noexcept {
    RawCaptureProgress next = append_progress_;
    if (!CheckedAdd(next.records, 1U, &next.records) ||
        !CheckedAdd(
            next.vendor_bytes,
            vendor_bytes,
            &next.vendor_bytes) ||
        !CheckedAdd(
            next.framed_wal_bytes,
            framed_wal_bytes,
            &next.framed_wal_bytes) ||
        (next.last_ingress_sequence != 0U &&
         ingress_sequence <=
             next.last_ingress_sequence)) {
        Trip(
            RawCaptureWorkerFailureKind::
                kProgressOverflow,
            RawCaptureFatalSignal::kRawWalIo);
        return false;
    }
    next.last_ingress_sequence = ingress_sequence;
    append_progress_ = next;
    PublishProgress(
        append_progress_, durable_progress_);
    return true;
}

void RawCaptureWorker::PublishProgress(
    const RawCaptureProgress& append,
    const RawCaptureProgress& durable) noexcept {
    progress_generation_.fetch_add(
        1U, std::memory_order_acq_rel);
    append_records_.store(
        append.records, std::memory_order_relaxed);
    append_vendor_bytes_.store(
        append.vendor_bytes, std::memory_order_relaxed);
    append_framed_wal_bytes_.store(
        append.framed_wal_bytes,
        std::memory_order_relaxed);
    append_last_ingress_sequence_.store(
        append.last_ingress_sequence,
        std::memory_order_relaxed);
    durable_records_.store(
        durable.records, std::memory_order_relaxed);
    durable_vendor_bytes_.store(
        durable.vendor_bytes,
        std::memory_order_relaxed);
    durable_framed_wal_bytes_.store(
        durable.framed_wal_bytes,
        std::memory_order_relaxed);
    durable_last_ingress_sequence_.store(
        durable.last_ingress_sequence,
        std::memory_order_relaxed);
    progress_generation_.fetch_add(
        1U, std::memory_order_release);
}

void RawCaptureWorker::Trip(
    RawCaptureWorkerFailureKind kind,
    RawCaptureFatalSignal signal) noexcept {
    std::uint8_t expected = static_cast<std::uint8_t>(
        RawCaptureWorkerFailureKind::kNone);
    if (failure_kind_.compare_exchange_strong(
            expected,
            static_cast<std::uint8_t>(kind),
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        config_.failure_sink.Notify(signal);
    }
}

void RawCaptureWorker::TripWriter(
    RawCaptureWorkerFailureKind kind) noexcept {
    const RawWalFailure failure = writer_.failure();
    writer_failure_kind_.store(
        static_cast<std::uint8_t>(failure.kind),
        std::memory_order_relaxed);
    writer_failure_errno_.store(
        failure.error_number, std::memory_order_relaxed);
    Trip(kind, RawCaptureFatalSignal::kRawWalIo);
}

void RawCaptureWorker::MarkFinished() noexcept {
    finished_.store(true, std::memory_order_release);
}

}  // namespace l2flow::ingress
