#include "l2flow/ingress/raw_replay.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <new>
#include <thread>
#include <utility>

namespace l2flow::ingress {
namespace {

[[nodiscard]] bool AddU64(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        left > std::numeric_limits<std::uint64_t>::max() - right) {
        return false;
    }
    *result = left + right;
    return true;
}

[[nodiscard]] bool IsValidRange(
    const RawReplayHalfOpenRange& range) noexcept {
    return !range.begin.has_value() ||
        !range.end.has_value() ||
        *range.begin <= *range.end;
}

[[nodiscard]] bool InRange(
    std::uint64_t value,
    const RawReplayHalfOpenRange& range) noexcept {
    if (range.begin.has_value() && value < *range.begin) {
        return false;
    }
    return !range.end.has_value() || value < *range.end;
}

template <typename Value>
[[nodiscard]] bool Matches(
    Value value,
    const std::vector<Value>& accepted) noexcept {
    return accepted.empty() ||
        std::find(accepted.begin(), accepted.end(), value) !=
            accepted.end();
}

[[nodiscard]] bool MatchesFilter(
    const RawRecordView& view,
    const SegmentHeaderV1& segment,
    const RawReplayFilter& filter) noexcept {
    const RawRecordHeaderV1& header = view.header();
    return Matches(
               segment.source_stream_id,
               filter.source_stream_ids) &&
        Matches(segment.capture_date, filter.capture_dates) &&
        Matches(
               header.vendor_service_id,
               filter.vendor_service_ids) &&
        Matches(
               header.vendor_service_version,
               filter.vendor_service_versions) &&
        Matches(
               header.vendor_message_id,
               filter.vendor_message_ids) &&
        InRange(view.record_start_wal_pos(), filter.wal) &&
        InRange(
               header.ingress_sequence,
               filter.ingress_sequence) &&
        InRange(
               header.recv_realtime_ns,
               filter.recv_realtime_ns);
}

[[nodiscard]] RawReplayClockIdentity ClockIdentity(
    const SegmentHeaderV1& segment) noexcept {
    return RawReplayClockIdentity{
        segment.clock_epoch_algorithm,
        segment.clock_epoch_digest,
        segment.clock_epoch_label};
}

[[nodiscard]] RawReplaySegmentContext SegmentContext(
    const SegmentHeaderV1& segment) noexcept {
    return RawReplaySegmentContext{
        segment.source_stream_id,
        segment.capture_date,
        segment.stream_day_id,
        segment.segment_sequence,
        ClockIdentity(segment)};
}

[[nodiscard]] RawReplayLocator Locator(
    const RawReplayRecord& record) noexcept {
    return RawReplayLocator{
        record.segment.source_stream_id,
        record.segment.capture_date,
        record.segment.stream_day_id,
        record.view.header().ingress_sequence,
        record.view.record_start_wal_pos(),
        record.view.record_end_wal_pos()};
}

[[nodiscard]] bool SameClockEpoch(
    const RawReplayClockIdentity& left,
    const RawReplayClockIdentity& right) noexcept {
    return left.algorithm == right.algorithm &&
        left.digest == right.digest;
}

// Computes floor(value * multiply / divide) without an overflowing
// intermediate. First split value by divide. The residual multiplication uses
// binary long division while keeping every remainder below divide.
[[nodiscard]] bool MulDivFloorU64(
    std::uint64_t value,
    std::uint64_t multiply,
    std::uint64_t divide,
    std::uint64_t* result) noexcept {
    if (result == nullptr || divide == 0U) {
        return false;
    }

    const std::uint64_t whole = value / divide;
    const std::uint64_t remainder = value % divide;
    if (whole != 0U &&
        multiply >
            std::numeric_limits<std::uint64_t>::max() / whole) {
        return false;
    }
    std::uint64_t quotient = whole * multiply;

    std::uint64_t residual_quotient = 0U;
    std::uint64_t residual_remainder = 0U;
    for (std::size_t bit = 64U; bit > 0U; --bit) {
        if (residual_quotient >
            std::numeric_limits<std::uint64_t>::max() / 2U) {
            return false;
        }
        residual_quotient *= 2U;

        if (residual_remainder >=
            divide - residual_remainder) {
            residual_remainder -=
                divide - residual_remainder;
            if (residual_quotient ==
                std::numeric_limits<std::uint64_t>::max()) {
                return false;
            }
            ++residual_quotient;
        } else {
            residual_remainder += residual_remainder;
        }

        const std::size_t shift = bit - 1U;
        if (((multiply >> shift) & 1U) != 0U) {
            if (residual_remainder >= divide - remainder) {
                residual_remainder -= divide - remainder;
                if (residual_quotient ==
                    std::numeric_limits<std::uint64_t>::max()) {
                    return false;
                }
                ++residual_quotient;
            } else {
                residual_remainder += remainder;
            }
        }
    }

    if (quotient >
        std::numeric_limits<std::uint64_t>::max() -
            residual_quotient) {
        return false;
    }
    quotient += residual_quotient;
    *result = quotient;
    return true;
}

[[nodiscard]] bool RecordAlignedDurableFrontier(
    const RawReplaySegmentInput& input) noexcept {
    if (input.scan == nullptr) {
        return false;
    }
    std::uint64_t data_begin = 0U;
    if (!AddU64(
            input.scan->segment.segment_base_wal_pos,
            static_cast<std::uint64_t>(
                kRawV1SegmentHeaderBytes),
            &data_begin)) {
        return false;
    }
    if (input.durable_end_wal_pos == data_begin) {
        return true;
    }
    return std::any_of(
        input.scan->records.begin(),
        input.scan->records.end(),
        [&input](const RawRecordView& record) {
            return record.record_end_wal_pos() ==
                input.durable_end_wal_pos;
        });
}

[[nodiscard]] RawReplayBoundary MakeBoundary(
    const RawReplayRecord& previous,
    const RawReplayRecord& current) noexcept {
    return RawReplayBoundary{
        Locator(previous),
        Locator(current),
        previous.segment.clock_epoch,
        current.segment.clock_epoch,
        previous.view.header().recv_monotonic_ns,
        current.view.header().recv_monotonic_ns};
}

}  // namespace

std::uint64_t SteadyRawReplayClock::NowMonotonicNs()
    noexcept {
    const auto now =
        std::chrono::steady_clock::now().time_since_epoch();
    const auto nanoseconds =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            now);
    if (nanoseconds.count() <= 0) {
        return 0U;
    }
    return static_cast<std::uint64_t>(
        nanoseconds.count());
}

bool ThreadRawReplaySleeper::SleepForNs(
    std::uint64_t duration_ns) noexcept {
    constexpr std::uint64_t kMaximumChunk =
        static_cast<std::uint64_t>(
            std::chrono::nanoseconds::max().count());
    try {
        while (duration_ns != 0U) {
            const std::uint64_t chunk =
                std::min(duration_ns, kMaximumChunk);
            std::this_thread::sleep_for(
                std::chrono::nanoseconds(
                    static_cast<std::int64_t>(chunk)));
            duration_ns -= chunk;
        }
    } catch (...) {
        return false;
    }
    return true;
}

std::string_view RawReplayErrorName(
    RawReplayError error) noexcept {
    switch (error) {
        case RawReplayError::kNone:
            return "none";
        case RawReplayError::kNullOutput:
            return "null_output";
        case RawReplayError::kNullInput:
            return "null_input";
        case RawReplayError::kInvalidScan:
            return "invalid_scan";
        case RawReplayError::kInvalidDurableFrontier:
            return "invalid_durable_frontier";
        case RawReplayError::kInvalidRange:
            return "invalid_range";
        case RawReplayError::kInvalidPace:
            return "invalid_pace";
        case RawReplayError::kInvalidExtent:
            return "invalid_extent";
        case RawReplayError::kInvalidMultiplier:
            return "invalid_multiplier";
        case RawReplayError::kClockRequired:
            return "clock_required";
        case RawReplayError::kSleeperRequired:
            return "sleeper_required";
        case RawReplayError::kDurationOverflow:
            return "duration_overflow";
        case RawReplayError::kSleepFailure:
            return "sleep_failure";
        case RawReplayError::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

RawReplayEngine::RawReplayEngine(
    std::vector<RawReplayRecord> records,
    RawReplayRunSettings settings,
    RawReplayClock* clock,
    RawReplaySleeper* sleeper) noexcept
    : records_(std::move(records)),
      settings_(settings),
      clock_(clock),
      sleeper_(sleeper) {}

RawReplayError RawReplayEngine::Create(
    std::span<const RawReplaySegmentInput> inputs,
    const RawReplayFilter& filter,
    const RawReplayRunSettings& settings,
    RawReplayClock* clock,
    RawReplaySleeper* sleeper,
    std::unique_ptr<RawReplayEngine>* output) noexcept {
    if (output == nullptr) {
        return RawReplayError::kNullOutput;
    }
    output->reset();
    if (inputs.data() == nullptr && !inputs.empty()) {
        return RawReplayError::kNullInput;
    }
    if (!IsValidRange(filter.wal) ||
        !IsValidRange(filter.ingress_sequence) ||
        !IsValidRange(filter.recv_realtime_ns)) {
        return RawReplayError::kInvalidRange;
    }
    if (settings.pace !=
            RawReplayPace::kAsFastAsPossible &&
        settings.pace != RawReplayPace::kFixedMultiplier &&
        settings.pace !=
            RawReplayPace::kOriginalMonotonic) {
        return RawReplayError::kInvalidPace;
    }
    if (settings.pace == RawReplayPace::kFixedMultiplier &&
        (settings.speed_numerator == 0U ||
         settings.speed_denominator == 0U)) {
        return RawReplayError::kInvalidMultiplier;
    }
    if (settings.pace !=
            RawReplayPace::kAsFastAsPossible &&
        clock == nullptr) {
        return RawReplayError::kClockRequired;
    }
    if (settings.pace !=
            RawReplayPace::kAsFastAsPossible &&
        sleeper == nullptr) {
        return RawReplayError::kSleeperRequired;
    }

    try {
        std::vector<RawReplayRecord> records;
        for (const RawReplaySegmentInput& input : inputs) {
            if (input.scan == nullptr) {
                return RawReplayError::kNullInput;
            }
            if (!input.scan->ok()) {
                return RawReplayError::kInvalidScan;
            }
            if (input.extent !=
                    RawReplayScanExtent::kDurableOnly &&
                input.extent !=
                    RawReplayScanExtent::
                        kIncludesRecoveredAppendOnly) {
                return RawReplayError::kInvalidExtent;
            }

            if (input.extent ==
                RawReplayScanExtent::
                    kIncludesRecoveredAppendOnly) {
                if (!RecordAlignedDurableFrontier(input) ||
                    input.durable_end_wal_pos >
                        input.scan->validated_end_wal_pos) {
                    return
                        RawReplayError::kInvalidDurableFrontier;
                }
            }

            const RawReplaySegmentContext context =
                SegmentContext(input.scan->segment);
            for (const RawRecordView& view :
                 input.scan->records) {
                RawReplayProvenance provenance =
                    RawReplayProvenance::kDurable;
                if (input.extent ==
                        RawReplayScanExtent::
                            kIncludesRecoveredAppendOnly &&
                    view.record_end_wal_pos() >
                        input.durable_end_wal_pos) {
                    if (view.record_start_wal_pos() <
                        input.durable_end_wal_pos) {
                        return RawReplayError::
                            kInvalidDurableFrontier;
                    }
                    provenance = RawReplayProvenance::
                        kRecoveredAppendOnly;
                }

                if (provenance ==
                        RawReplayProvenance::
                            kRecoveredAppendOnly &&
                    !settings.
                        include_recovered_append_only) {
                    continue;
                }
                if (!MatchesFilter(
                        view, input.scan->segment, filter)) {
                    continue;
                }
                records.push_back(
                    RawReplayRecord{
                        view, context, provenance});
            }
        }

        output->reset(new RawReplayEngine(
            std::move(records), settings, clock, sleeper));
    } catch (const std::bad_alloc&) {
        return RawReplayError::kResourceExhausted;
    } catch (...) {
        return RawReplayError::kResourceExhausted;
    }
    return RawReplayError::kNone;
}

bool RawReplayEngine::HasAdvancePermit(
    bool* single_step,
    std::uint64_t* pacing_reset_generation) const noexcept {
    if (single_step == nullptr ||
        pacing_reset_generation == nullptr) {
        return false;
    }
    const std::lock_guard<std::mutex> lock(control_mutex_);
    *single_step = paused_ && single_step_budget_ != 0U;
    *pacing_reset_generation = pacing_reset_generation_;
    return !paused_ || *single_step;
}

void RawReplayEngine::ConsumeSingleStepPermit() noexcept {
    const std::lock_guard<std::mutex> lock(control_mutex_);
    if (paused_ && single_step_budget_ != 0U) {
        --single_step_budget_;
    }
}

RawReplayStep RawReplayEngine::Fail(
    RawReplayError error) noexcept {
    terminal_error_ = error;
    RawReplayStep step;
    step.kind = RawReplayStepKind::kError;
    step.error = error;
    return step;
}

RawReplayStep RawReplayEngine::Next() noexcept {
    if (terminal_error_ != RawReplayError::kNone) {
        return Fail(terminal_error_);
    }
    if (next_index_ >= records_.size()) {
        RawReplayStep step;
        step.kind = RawReplayStepKind::kEnd;
        return step;
    }

    bool single_step = false;
    std::uint64_t pacing_reset_generation = 0U;
    if (!HasAdvancePermit(
            &single_step, &pacing_reset_generation)) {
        RawReplayStep step;
        step.kind = RawReplayStepKind::kPaused;
        return step;
    }
    if (pacing_reset_generation !=
        observed_pacing_reset_generation_) {
        pace_anchor_valid_ = false;
        observed_pacing_reset_generation_ =
            pacing_reset_generation;
    }

    const RawReplayRecord& current = records_[next_index_];
    if (has_previous_record_ &&
        !notice_emitted_for_next_) {
        const RawReplayRecord& previous =
            records_[previous_index_];
        if (!SameClockEpoch(
                previous.segment.clock_epoch,
                current.segment.clock_epoch)) {
            notice_emitted_for_next_ = true;
            reset_pacing_for_next_ = true;
            RawReplayStep step;
            step.kind =
                RawReplayStepKind::kClockEpochBoundary;
            step.boundary =
                MakeBoundary(previous, current);
            return step;
        }
        if (current.view.header().recv_monotonic_ns <
            previous.view.header().recv_monotonic_ns) {
            notice_emitted_for_next_ = true;
            reset_pacing_for_next_ = true;
            RawReplayStep step;
            step.kind =
                RawReplayStepKind::kMonotonicRegression;
            step.boundary =
                MakeBoundary(previous, current);
            return step;
        }
    }

    std::uint64_t sleep_ns = 0U;
    if (settings_.pace !=
            RawReplayPace::kAsFastAsPossible &&
        !single_step) {
        const std::uint64_t source_now =
            current.view.header().recv_monotonic_ns;
        if (!pace_anchor_valid_ ||
            reset_pacing_for_next_) {
            source_anchor_ns_ = source_now;
            replay_anchor_ns_ =
                clock_->NowMonotonicNs();
            pace_anchor_valid_ = true;
        } else {
            const std::uint64_t source_elapsed =
                source_now - source_anchor_ns_;
            std::uint64_t replay_elapsed = source_elapsed;
            if (settings_.pace ==
                RawReplayPace::kFixedMultiplier) {
                if (!MulDivFloorU64(
                        source_elapsed,
                        settings_.speed_denominator,
                        settings_.speed_numerator,
                        &replay_elapsed)) {
                    return Fail(
                        RawReplayError::
                            kDurationOverflow);
                }
            }

            std::uint64_t target = 0U;
            if (!AddU64(
                    replay_anchor_ns_,
                    replay_elapsed,
                    &target)) {
                return Fail(
                    RawReplayError::kDurationOverflow);
            }
            const std::uint64_t now =
                clock_->NowMonotonicNs();
            if (target > now) {
                sleep_ns = target - now;
                if (!sleeper_->SleepForNs(sleep_ns)) {
                    return Fail(
                        RawReplayError::kSleepFailure);
                }
            }
        }
    } else if (settings_.pace !=
                   RawReplayPace::kAsFastAsPossible &&
               (reset_pacing_for_next_ ||
                !pace_anchor_valid_)) {
        // Single-step bypasses a wait but establishes a fresh origin so
        // resume never tries to catch up across manual stepping.
        source_anchor_ns_ =
            current.view.header().recv_monotonic_ns;
        replay_anchor_ns_ = clock_->NowMonotonicNs();
        pace_anchor_valid_ = true;
    }

    RawReplayStep step;
    step.kind = RawReplayStepKind::kRecord;
    step.record = current;
    step.sleep_ns = sleep_ns;

    previous_index_ = next_index_;
    has_previous_record_ = true;
    ++next_index_;
    notice_emitted_for_next_ = false;
    reset_pacing_for_next_ = false;
    if (single_step) {
        ConsumeSingleStepPermit();
    }
    return step;
}

void RawReplayEngine::Pause() noexcept {
    const std::lock_guard<std::mutex> lock(control_mutex_);
    paused_ = true;
}

void RawReplayEngine::Resume() noexcept {
    const std::lock_guard<std::mutex> lock(control_mutex_);
    if (paused_) {
        ++pacing_reset_generation_;
    }
    paused_ = false;
    single_step_budget_ = 0U;
}

void RawReplayEngine::RequestSingleStep() noexcept {
    const std::lock_guard<std::mutex> lock(control_mutex_);
    paused_ = true;
    if (single_step_budget_ !=
        std::numeric_limits<std::uint64_t>::max()) {
        ++single_step_budget_;
    }
}

bool RawReplayEngine::paused() const noexcept {
    const std::lock_guard<std::mutex> lock(control_mutex_);
    return paused_;
}

}  // namespace l2flow::ingress
