#include "l2flow/control/control_live_worker.h"

#include "l2flow/common/identity128.h"
#include "l2flow/control/control_checkpoint_v1.h"
#include "l2flow/control/raw_frontier_v1.h"

#include <chrono>
#include <limits>
#include <new>
#include <thread>
#include <utility>

namespace l2flow::control {
namespace {

bool IsCommittedMalformed(
    const ControlProcessResultV1& result) noexcept {
    const bool malformed =
        result.error == ControlProcessErrorV1::kMalformedApiControl ||
        result.error == ControlProcessErrorV1::kMalformedSysControl;
    return malformed && result.cursor_committed &&
           result.control_state_poisoned &&
           result.control_record.has_value() &&
           result.control_record->control_type ==
               ControlTypeV1::kDecodeError;
}

bool InitialStateMatches(
    const ControlLiveWorkerConfigV1& config,
    const l2flow::ingress::RawLiveTail& tail,
    const ControlDecoderSnapshotV1& decoder) noexcept {
    if (l2flow::common::IsZeroIdentity(config.writer_instance) ||
        config.connect_generation == 0U ||
        config.record_publish_timeout_ns == 0U ||
        config.final_catch_up_timeout_ns == 0U ||
        config.failure_callback == nullptr ||
        tail.writer_instance() != config.writer_instance ||
        tail.source_stream_id() == 0U ||
        tail.capture_date() == 0U ||
        tail.segment_sequence() == 0U ||
        tail.segment_offset() <
            l2flow::ingress::kRawV1SegmentHeaderBytes ||
        tail.initial_global_wal_pos() < tail.segment_offset() ||
        tail.next_ingress_sequence() == 0U) {
        return false;
    }
    if (decoder.source_stream_id != tail.source_stream_id() ||
        decoder.capture_date != tail.capture_date() ||
        decoder.stream_day_id != tail.stream_day_id()) {
        return false;
    }
    if (decoder.next_ingress_sequence !=
            tail.next_ingress_sequence() ||
        decoder.processed_ingress_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        decoder.next_ingress_sequence !=
            decoder.processed_ingress_sequence + 1U) {
        return false;
    }
    if (decoder.counters.processed_records == 0U) {
        return decoder.processed_ingress_sequence == 0U &&
               decoder.processed_record_start_wal_pos == 0U &&
               decoder.processed_record_end_wal_pos == 0U &&
               decoder.next_ingress_sequence == 1U &&
               tail.segment_sequence() == 1U &&
               tail.segment_offset() ==
                   l2flow::ingress::kRawV1SegmentHeaderBytes &&
               tail.initial_global_wal_pos() ==
                   l2flow::ingress::kRawV1SegmentHeaderBytes;
    }
    return decoder.processed_record_start_wal_pos != 0U &&
           decoder.processed_record_end_wal_pos >
               decoder.processed_record_start_wal_pos &&
           decoder.processed_record_end_wal_pos ==
               tail.initial_global_wal_pos();
}

}  // namespace

ControlLiveWorkerV1::ControlLiveWorkerV1(
    ControlLiveWorkerConfigV1 config,
    std::uint64_t start_control_generation,
    std::uint64_t generation_first_ingress_sequence,
    std::unique_ptr<l2flow::ingress::RawLiveTail> tail,
    std::unique_ptr<ControlDecoderV1> decoder,
    std::unique_ptr<ControlRecordSinkV1> record_sink)
    : config_(config),
      start_control_generation_(start_control_generation),
      generation_first_ingress_sequence_(
          generation_first_ingress_sequence),
      tail_(std::move(tail)),
      decoder_(std::move(decoder)),
      record_sink_(std::move(record_sink)) {
    const ControlDecoderSnapshotV1 state = decoder_->Snapshot();
    processed_wal_pos_ = tail_->initial_global_wal_pos();
    processed_ingress_sequence_ = state.processed_ingress_sequence;
    processed_segment_sequence_ = tail_->segment_sequence();
    processed_segment_offset_ = tail_->segment_offset();
}

ControlLiveWorkerV1::~ControlLiveWorkerV1() = default;

ControlLiveWorkerCreateErrorV1 ControlLiveWorkerV1::Create(
    ControlLiveWorkerConfigV1 config,
    std::unique_ptr<l2flow::ingress::RawLiveTail> tail,
    std::unique_ptr<ControlDecoderV1> decoder,
    std::unique_ptr<ControlRecordSinkV1> record_sink,
    std::unique_ptr<ControlLiveWorkerV1>* output) noexcept {
    if (output == nullptr) {
        return ControlLiveWorkerCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (tail == nullptr || decoder == nullptr || record_sink == nullptr) {
        return ControlLiveWorkerCreateErrorV1::kNullDependency;
    }
    if (l2flow::common::IsZeroIdentity(config.writer_instance) ||
        config.connect_generation == 0U ||
        config.record_publish_timeout_ns == 0U ||
        config.final_catch_up_timeout_ns == 0U ||
        config.failure_callback == nullptr) {
        return ControlLiveWorkerCreateErrorV1::kInvalidConfig;
    }
    try {
        const l2flow::ingress::RawLiveControlSampleV1 start =
            tail->SampleControlFresh();
        if (!start.ok()) {
            return start.error ==
                       l2flow::ingress::RawLiveTailError::
                           kControlUnavailable
                       ? ControlLiveWorkerCreateErrorV1::
                             kControlSampleUnavailable
                       : ControlLiveWorkerCreateErrorV1::
                             kInitialControlInvalid;
        }
        if (start.snapshot.fatal_state != 0U ||
            !RawFrontierCursorShapeValidV1(start.snapshot)) {
            return ControlLiveWorkerCreateErrorV1::
                kInitialControlInvalid;
        }
        const l2flow::ingress::RawLiveTailAttachV1 attach =
            tail->initial_attach();
        if (start.snapshot.segment_sequence !=
                attach.segment_sequence ||
            start.snapshot.append_global_wal_pos !=
                attach.global_wal_pos ||
            start.snapshot.append_segment_offset !=
                attach.segment_offset ||
            start.snapshot.append_ingress_sequence ==
                std::numeric_limits<std::uint64_t>::max() ||
            start.snapshot.append_ingress_sequence + 1U !=
                attach.next_ingress_sequence) {
            return ControlLiveWorkerCreateErrorV1::
                kInitialAppendMismatch;
        }
        const ControlDecoderSnapshotV1 state = decoder->Snapshot();
        if (state.source_stream_id != tail->source_stream_id() ||
            state.capture_date != tail->capture_date() ||
            state.stream_day_id != tail->stream_day_id()) {
            return ControlLiveWorkerCreateErrorV1::kNamespaceMismatch;
        }
        if (!InitialStateMatches(config, *tail, state)) {
            return ControlLiveWorkerCreateErrorV1::kInitialCursorMismatch;
        }
        const std::uint64_t generation_first_ingress_sequence =
            attach.next_ingress_sequence;
        output->reset(new ControlLiveWorkerV1(
            config,
            start.generation,
            generation_first_ingress_sequence,
            std::move(tail),
            std::move(decoder),
            std::move(record_sink)));
        return ControlLiveWorkerCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return ControlLiveWorkerCreateErrorV1::kResourceExhausted;
    } catch (...) {
        return ControlLiveWorkerCreateErrorV1::kInvalidConfig;
    }
}

bool ControlLiveWorkerV1::Run() noexcept {
    bool expected = false;
    if (!run_started_.compare_exchange_strong(
            expected,
            true,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        Trip(ControlLiveWorkerFailureV1::kRunAlreadyStarted);
        return false;
    }

    const std::uint64_t started_ns = MonotonicNowNs();
    bool success = started_ns != 0U;
    if (started_ns == 0U) {
        Trip(ControlLiveWorkerFailureV1::kClockInvalid);
    } else {
        std::lock_guard<std::mutex> lock(state_mutex_);
        decoder_heartbeat_monotonic_ns_ = started_ns;
        decoder_healthy_ =
            !fatal_.load(std::memory_order_acquire);
        success = decoder_healthy_;
    }
    startup_succeeded_.store(success, std::memory_order_release);
    startup_complete_.store(true, std::memory_order_release);

    bool stop_timer_started = false;
    std::uint64_t stop_started_ns = 0U;
    while (success && !fatal_.load(std::memory_order_acquire)) {
        const std::uint64_t now_ns = MonotonicNowNs();
        if (now_ns == 0U) {
            Trip(ControlLiveWorkerFailureV1::kClockInvalid);
            success = false;
            break;
        }

        l2flow::ingress::RawLiveTailStep step = tail_->Next();
        if (step.kind == l2flow::ingress::RawLiveTailStepKind::kRecord) {
            if (!step.record.has_value()) {
                Trip(
                    ControlLiveWorkerFailureV1::kLiveTailFailure,
                    l2flow::ingress::RawLiveTailError::kRecordInvalid);
                success = false;
                break;
            }
            const l2flow::ingress::RawLiveRecord& live = *step.record;
            if (live.writer_instance != config_.writer_instance ||
                live.segment.stream_day_id != tail_->stream_day_id() ||
                live.segment.source_stream_id != tail_->source_stream_id() ||
                live.segment.capture_date != tail_->capture_date() ||
                live.segment.segment_sequence != tail_->segment_sequence()) {
                Trip(
                    ControlLiveWorkerFailureV1::kLiveTailIdentityMismatch,
                    l2flow::ingress::RawLiveTailError::
                        kSegmentIdentityMismatch);
                success = false;
                break;
            }

            ControlProcessResultV1 processed;
            std::optional<ControlRecordV1> emitted;
            bool malformed_committed = false;
            {
                std::lock_guard<std::mutex> lock(state_mutex_);
                processed = decoder_->Process(live);
                if (processed.cursor_committed) {
                    processed_wal_pos_ = live.view.record_end_wal_pos();
                    processed_ingress_sequence_ =
                        live.view.header().ingress_sequence;
                    processed_segment_sequence_ =
                        tail_->segment_sequence();
                    processed_segment_offset_ = tail_->segment_offset();
                }
                malformed_committed = IsCommittedMalformed(processed);
                if (processed.control_record.has_value()) {
                    emitted = processed.control_record;
                    // Until the derived record sink acknowledges publication,
                    // a concurrent service sample must fail closed.
                    decoder_healthy_ = false;
                } else if (!processed.ok()) {
                    decoder_healthy_ = false;
                }
            }

            if (emitted.has_value()) {
                ControlRecordWireV1 canonical_wire{};
                if (EncodeControlRecordV1(
                        *emitted, &canonical_wire) !=
                    ControlRecordV1Error::kNone) {
                    Trip(
                        ControlLiveWorkerFailureV1::kDecoderRejected,
                        l2flow::ingress::RawLiveTailError::kNone,
                        processed.error);
                    success = false;
                    break;
                }
                const std::uint64_t publish_now_ns =
                    MonotonicNowNs();
                if (publish_now_ns == 0U ||
                    publish_now_ns < now_ns ||
                    publish_now_ns >
                        std::numeric_limits<std::uint64_t>::max() -
                            config_.record_publish_timeout_ns) {
                    Trip(ControlLiveWorkerFailureV1::kClockInvalid);
                    success = false;
                    break;
                }
                const std::uint64_t publish_deadline_ns =
                    publish_now_ns +
                    config_.record_publish_timeout_ns;
                const ControlRecordPublishResultV1 publish_result =
                    record_sink_->Publish(
                        *emitted,
                        canonical_wire,
                        publish_deadline_ns);
                const std::uint64_t publish_finished_ns =
                    MonotonicNowNs();
                if (publish_finished_ns == 0U ||
                    publish_finished_ns < publish_now_ns) {
                    Trip(ControlLiveWorkerFailureV1::kClockInvalid);
                    success = false;
                    break;
                }
                if (publish_finished_ns > publish_deadline_ns) {
                    Trip(
                        ControlLiveWorkerFailureV1::
                            kControlRecordSinkTimedOut,
                        l2flow::ingress::RawLiveTailError::kNone,
                        processed.error);
                    success = false;
                    break;
                }
                if (publish_result !=
                        ControlRecordPublishResultV1::kPublishedNew &&
                    publish_result !=
                        ControlRecordPublishResultV1::
                            kAcceptedIdentical) {
                    Trip(
                        ControlLiveWorkerFailureV1::
                            kControlRecordSinkRejected,
                        l2flow::ingress::RawLiveTailError::kNone,
                        processed.error);
                    success = false;
                    break;
                }
                bool counter_overflow = false;
                {
                    std::lock_guard<std::mutex> lock(state_mutex_);
                    if (emitted_control_records_ ==
                        std::numeric_limits<std::uint64_t>::max()) {
                        counter_overflow = true;
                    } else {
                        ++emitted_control_records_;
                    }
                    if (malformed_committed) {
                        if (committed_malformed_controls_ ==
                            std::numeric_limits<std::uint64_t>::max()) {
                            counter_overflow = true;
                        } else {
                            ++committed_malformed_controls_;
                        }
                    }
                    if (!counter_overflow &&
                        emitted->control_type ==
                            ControlTypeV1::kLogonSuccess &&
                        emitted->origin_ingress_sequence >=
                            generation_first_ingress_sequence_ &&
                        live.control_generation >
                            start_control_generation_) {
                        successful_logon_connect_generation_ =
                            config_.connect_generation;
                        successful_logon_ingress_sequence_ =
                            emitted->origin_ingress_sequence;
                    }
                    decoder_healthy_ =
                        !counter_overflow &&
                        !fatal_.load(std::memory_order_acquire) &&
                        (processed.ok() || malformed_committed);
                }
                if (counter_overflow) {
                    Trip(ControlLiveWorkerFailureV1::kDecoderRejected);
                    success = false;
                    break;
                }
            }
            if (!processed.ok() && !malformed_committed) {
                Trip(
                    ControlLiveWorkerFailureV1::kDecoderRejected,
                    l2flow::ingress::RawLiveTailError::kNone,
                    processed.error);
                success = false;
                break;
            }
        } else if (
            step.kind ==
            l2flow::ingress::RawLiveTailStepKind::kSegmentTransition) {
            if (!step.segment_transition.has_value()) {
                Trip(
                    ControlLiveWorkerFailureV1::kLiveTailFailure,
                    l2flow::ingress::RawLiveTailError::
                        kSegmentOrderViolation);
                success = false;
                break;
            }
            const l2flow::ingress::RawLiveSegmentTransitionV1& transition =
                *step.segment_transition;
            bool transition_valid =
                transition.writer_instance == config_.writer_instance &&
                transition.next_segment.stream_day_id ==
                    tail_->stream_day_id() &&
                transition.next_segment.source_stream_id ==
                    tail_->source_stream_id() &&
                transition.next_segment.capture_date ==
                    tail_->capture_date() &&
                transition.next_segment.segment_sequence ==
                    tail_->segment_sequence() &&
                transition.next_ingress_sequence ==
                    tail_->next_ingress_sequence();
            {
                std::lock_guard<std::mutex> lock(state_mutex_);
                transition_valid = transition_valid &&
                    processed_ingress_sequence_ !=
                        std::numeric_limits<std::uint64_t>::max() &&
                    transition.next_ingress_sequence ==
                        processed_ingress_sequence_ + 1U &&
                    transition.next_data_begin_wal_pos >
                        processed_wal_pos_;
                if (transition_valid) {
                    processed_wal_pos_ =
                        transition.next_data_begin_wal_pos;
                    processed_segment_sequence_ =
                        transition.next_segment.segment_sequence;
                    processed_segment_offset_ =
                        l2flow::ingress::kRawV1SegmentHeaderBytes;
                }
            }
            if (!transition_valid) {
                Trip(
                    ControlLiveWorkerFailureV1::kLiveTailIdentityMismatch,
                    l2flow::ingress::RawLiveTailError::
                        kSegmentOrderViolation);
                success = false;
                break;
            }
        } else if (
            step.kind ==
            l2flow::ingress::RawLiveTailStepKind::kInstanceChanged) {
            Trip(
                ControlLiveWorkerFailureV1::kLiveTailInstanceChanged,
                step.error);
            success = false;
            break;
        } else if (
            step.kind == l2flow::ingress::RawLiveTailStepKind::kError) {
            Trip(
                ControlLiveWorkerFailureV1::kLiveTailFailure,
                step.error);
            success = false;
            break;
        } else if (
            step.kind !=
                l2flow::ingress::RawLiveTailStepKind::kWouldBlock &&
            step.kind != l2flow::ingress::RawLiveTailStepKind::kEnd) {
            Trip(
                ControlLiveWorkerFailureV1::kLiveTailFailure,
                l2flow::ingress::RawLiveTailError::kRecordInvalid);
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
        if (stop_requested_.load(std::memory_order_acquire)) {
            if (!stop_timer_started) {
                stop_timer_started = true;
                stop_started_ns = now_ns;
            } else if (now_ns < stop_started_ns ||
                       now_ns - stop_started_ns >=
                           config_.final_catch_up_timeout_ns) {
                Trip(ControlLiveWorkerFailureV1::kStopCatchUpTimedOut);
                success = false;
                break;
            }
        }
        if (step.kind != l2flow::ingress::RawLiveTailStepKind::kRecord) {
            std::this_thread::yield();
        }
    }

    success = success && !fatal_.load(std::memory_order_acquire) &&
              stop_requested_.load(std::memory_order_acquire) &&
              StopReached();
    finished_.store(true, std::memory_order_release);
    return success;
}

bool ControlLiveWorkerV1::StopAt(
    const ControlLiveStopCursorV1& final_cursor) noexcept {
    if (!StopCursorValid(final_cursor)) {
        Trip(ControlLiveWorkerFailureV1::kStopTargetInvalid);
        return false;
    }
    bool conflicting_retry = false;
    {
        std::lock_guard<std::mutex> lock(stop_mutex_);
        if (stop_requested_.load(std::memory_order_acquire)) {
            if (stop_cursor_ != final_cursor) {
                conflicting_retry = true;
            } else {
                return !fatal_.load(std::memory_order_acquire);
            }
        } else {
            stop_cursor_ = final_cursor;
            stop_requested_.store(true, std::memory_order_release);
        }
    }
    if (conflicting_retry) {
        Trip(ControlLiveWorkerFailureV1::kStopTargetInvalid);
        return false;
    }
    return !fatal_.load(std::memory_order_acquire);
}

void ControlLiveWorkerV1::Abort() noexcept {
    Trip(ControlLiveWorkerFailureV1::kAborted);
}

ControlLiveWorkerSnapshotV1 ControlLiveWorkerV1::Snapshot() const noexcept {
    ControlLiveWorkerSnapshotV1 result;
    result.failure = static_cast<ControlLiveWorkerFailureV1>(
        failure_.load(std::memory_order_acquire));
    result.live_tail_error =
        static_cast<l2flow::ingress::RawLiveTailError>(
            live_tail_error_.load(std::memory_order_relaxed));
    result.process_error = static_cast<ControlProcessErrorV1>(
        process_error_.load(std::memory_order_relaxed));
    result.stop_requested =
        stop_requested_.load(std::memory_order_acquire);
    if (result.stop_requested) {
        std::lock_guard<std::mutex> lock(stop_mutex_);
        result.requested_stop = stop_cursor_;
    }
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        result.processed_wal_pos = processed_wal_pos_;
        result.processed_ingress_sequence =
            processed_ingress_sequence_;
        result.processed_segment_sequence =
            processed_segment_sequence_;
        result.processed_segment_offset = processed_segment_offset_;
        result.emitted_control_records = emitted_control_records_;
        result.committed_malformed_controls =
            committed_malformed_controls_;
        result.decoder_healthy = decoder_healthy_;
    }
    result.startup_complete =
        startup_complete_.load(std::memory_order_acquire);
    result.startup_succeeded =
        result.startup_complete &&
        startup_succeeded_.load(std::memory_order_relaxed);
    result.finished = finished_.load(std::memory_order_acquire);
    return result;
}

ControlDecoderSnapshotV1 ControlLiveWorkerV1::DecoderSnapshot() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return decoder_->Snapshot();
}

std::optional<ControlDecoderCheckpointV1>
ControlLiveWorkerV1::Checkpoint() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (!decoder_healthy_ ||
        fatal_.load(std::memory_order_acquire) ||
        !startup_succeeded_.load(std::memory_order_acquire)) {
        return std::nullopt;
    }
    ControlDecoderCheckpointV1 checkpoint = decoder_->Checkpoint();
    if (ValidateControlDecoderCheckpointV1(checkpoint) !=
        ControlCheckpointV1Error::kNone) {
        return std::nullopt;
    }
    return checkpoint;
}

ControlReadinessResultV1 ControlLiveWorkerV1::EvaluateReadiness(
    const ControlReadinessGateConfigV1& config,
    const std::optional<MarketSilenceProofV1>& calendar_proof,
    std::uint64_t now_monotonic_ns,
    std::uint64_t sampled_realtime_ns,
    bool capture_pipeline_healthy) const noexcept {
    const l2flow::ingress::RawLiveControlSampleV1 sampled_raw =
        tail_->SampleControlFresh();
    if (!sampled_raw.ok()) {
        ControlReadinessResultV1 result;
        result.reason = ControlReadinessReasonV1::
            kRawControlSampleUnavailable;
        return result;
    }
    try {
        std::lock_guard<std::mutex> lock(state_mutex_);
        const ControlDecoderSnapshotV1 decoder = decoder_->Snapshot();
        const ControlReadinessRuntimeV1 runtime = ReadinessRuntimeLocked(
            sampled_realtime_ns, capture_pipeline_healthy);
        return EvaluateControlReadinessV1(
            config,
            decoder,
            runtime,
            sampled_raw.snapshot,
            calendar_proof,
            now_monotonic_ns);
    } catch (...) {
        ControlReadinessResultV1 result;
        result.reason = ControlReadinessReasonV1::kDecoderUnhealthy;
        return result;
    }
}

std::uint64_t ControlLiveWorkerV1::MonotonicNowNs() const noexcept {
    if (config_.monotonic_now != nullptr) {
        return config_.monotonic_now(config_.monotonic_clock_context);
    }
    const auto value =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count();
    return value <= 0 ? 0U : static_cast<std::uint64_t>(value);
}

bool ControlLiveWorkerV1::PublishHeartbeat(std::uint64_t now_ns) noexcept {
    bool valid = false;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        valid = now_ns != 0U &&
                now_ns >= decoder_heartbeat_monotonic_ns_;
        if (valid) {
            decoder_heartbeat_monotonic_ns_ = now_ns;
        }
    }
    if (!valid) {
        Trip(ControlLiveWorkerFailureV1::kClockInvalid);
    }
    return valid;
}

bool ControlLiveWorkerV1::StopReached() noexcept {
    if (!stop_requested_.load(std::memory_order_acquire)) {
        return false;
    }
    const ControlLiveStopCursorV1 target = stop_cursor_;
    std::uint64_t processed_wal = 0U;
    std::uint64_t processed_ingress = 0U;
    std::uint32_t processed_segment = 0U;
    std::uint64_t processed_offset = 0U;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        processed_wal = processed_wal_pos_;
        processed_ingress = processed_ingress_sequence_;
        processed_segment = processed_segment_sequence_;
        processed_offset = processed_segment_offset_;
    }
    const std::uint32_t tail_segment = tail_->segment_sequence();
    const std::uint64_t tail_offset = tail_->segment_offset();
    const std::uint64_t tail_next = tail_->next_ingress_sequence();
    const bool exceeded =
        processed_wal > target.global_wal_pos ||
        processed_ingress > target.ingress_sequence ||
        processed_segment > target.segment_sequence ||
        (processed_segment == target.segment_sequence &&
         processed_offset > target.segment_offset) ||
        tail_segment > target.segment_sequence ||
        (tail_segment == target.segment_sequence &&
         tail_offset > target.segment_offset) ||
        tail_next > target.ingress_sequence + 1U;
    if (exceeded) {
        Trip(ControlLiveWorkerFailureV1::kStopCursorExceeded);
        return false;
    }
    return processed_wal == target.global_wal_pos &&
           processed_ingress == target.ingress_sequence &&
           processed_segment == target.segment_sequence &&
           processed_offset == target.segment_offset &&
           tail_segment == target.segment_sequence &&
           tail_offset == target.segment_offset &&
           tail_next == target.ingress_sequence + 1U;
}

bool ControlLiveWorkerV1::StopCursorValid(
    const ControlLiveStopCursorV1& cursor) const noexcept {
    std::uint32_t current_segment = 0U;
    std::uint64_t current_wal_pos = 0U;
    std::uint64_t current_ingress_sequence = 0U;
    std::uint64_t current_segment_offset = 0U;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        current_segment = processed_segment_sequence_;
        current_wal_pos = processed_wal_pos_;
        current_ingress_sequence = processed_ingress_sequence_;
        current_segment_offset = processed_segment_offset_;
    }
    if (l2flow::common::IsZeroIdentity(cursor.writer_instance) ||
        cursor.writer_instance != config_.writer_instance ||
        l2flow::common::IsZeroIdentity(cursor.stream_day_id) ||
        cursor.stream_day_id != tail_->stream_day_id() ||
        cursor.source_stream_id != tail_->source_stream_id() ||
        cursor.capture_date != tail_->capture_date() ||
        cursor.segment_sequence == 0U ||
        cursor.segment_sequence < current_segment ||
        cursor.global_wal_pos < current_wal_pos ||
        cursor.ingress_sequence < current_ingress_sequence ||
        cursor.segment_offset <
            l2flow::ingress::kRawV1SegmentHeaderBytes ||
        (cursor.global_wal_pos %
         l2flow::ingress::kRawV1RecordAlignment) != 0U ||
        (cursor.segment_offset %
         l2flow::ingress::kRawV1RecordAlignment) != 0U ||
        cursor.global_wal_pos < cursor.segment_offset ||
        cursor.ingress_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        cursor.ingress_sequence + 1U <
            generation_first_ingress_sequence_) {
        return false;
    }

    const std::uint64_t cursor_segment_base =
        cursor.global_wal_pos - cursor.segment_offset;
    const std::uint64_t minimum_segment_base =
        static_cast<std::uint64_t>(cursor.segment_sequence - 1U) *
        l2flow::ingress::kRawV1SegmentHeaderBytes;
    if (cursor_segment_base < minimum_segment_base ||
        (cursor.segment_sequence == 1U && cursor_segment_base != 0U) ||
        !RawCursorAdvancePlausibleV1(
            current_wal_pos,
            current_ingress_sequence,
            cursor.global_wal_pos,
            cursor.ingress_sequence)) {
        return false;
    }

    if (cursor.segment_sequence == current_segment) {
        return current_wal_pos >= current_segment_offset &&
               cursor.segment_offset >= current_segment_offset &&
               cursor_segment_base ==
                   current_wal_pos - current_segment_offset;
    }

    const std::uint64_t segment_delta =
        static_cast<std::uint64_t>(
            cursor.segment_sequence - current_segment);
    const std::uint64_t minimum_intervening_headers =
        (segment_delta - 1U) *
        l2flow::ingress::kRawV1SegmentHeaderBytes;
    return cursor_segment_base >= current_wal_pos &&
           cursor_segment_base - current_wal_pos >=
               minimum_intervening_headers;
}

ControlReadinessRuntimeV1 ControlLiveWorkerV1::ReadinessRuntimeLocked(
    std::uint64_t sampled_realtime_ns,
    bool capture_pipeline_healthy) const noexcept {
    ControlReadinessRuntimeV1 result;
    result.writer_instance = config_.writer_instance;
    result.processed_wal_pos = processed_wal_pos_;
    result.processed_ingress_sequence = processed_ingress_sequence_;
    result.decoder_heartbeat_monotonic_ns =
        decoder_heartbeat_monotonic_ns_;
    result.connect_generation = config_.connect_generation;
    result.generation_first_ingress_sequence =
        generation_first_ingress_sequence_;
    result.successful_logon_connect_generation =
        successful_logon_connect_generation_;
    result.successful_logon_ingress_sequence =
        successful_logon_ingress_sequence_;
    result.sampled_realtime_ns = sampled_realtime_ns;
    result.decoder_healthy = decoder_healthy_ &&
        !fatal_.load(std::memory_order_acquire) &&
        run_started_.load(std::memory_order_acquire) &&
        startup_succeeded_.load(std::memory_order_acquire) &&
        !stop_requested_.load(std::memory_order_acquire) &&
        !finished_.load(std::memory_order_acquire);
    result.capture_pipeline_healthy = capture_pipeline_healthy;
    return result;
}

void ControlLiveWorkerV1::Trip(
    ControlLiveWorkerFailureV1 failure,
    l2flow::ingress::RawLiveTailError live_tail_error,
    ControlProcessErrorV1 process_error) noexcept {
    if (!trip_claimed_.test_and_set(std::memory_order_acq_rel)) {
        // Latch fatal before taking the state mutex so no concurrent Run()
        // path can publish decoder_healthy=true after this failure wins.
        fatal_.store(true, std::memory_order_release);
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            decoder_healthy_ = false;
        }
        live_tail_error_.store(
            static_cast<std::uint8_t>(live_tail_error),
            std::memory_order_relaxed);
        process_error_.store(
            static_cast<std::uint16_t>(process_error),
            std::memory_order_relaxed);
        failure_.store(
            static_cast<std::uint8_t>(failure),
            std::memory_order_release);
        config_.failure_callback(config_.failure_context, failure);
    }
}

}  // namespace l2flow::control
