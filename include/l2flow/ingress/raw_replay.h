#pragma once

#include "l2flow/ingress/raw_reader.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace l2flow::ingress {

enum class RawReplayPace : std::uint8_t {
    kAsFastAsPossible = 0U,
    kFixedMultiplier,
    kOriginalMonotonic,
};

enum class RawReplayProvenance : std::uint8_t {
    kDurable = 0U,
    kRecoveredAppendOnly,
};

// A recovered scan can contain a durable prefix followed by complete records
// recovered from the append-only suffix. The explicit extent and frontier
// prevent that suffix from silently becoming durable replay input.
enum class RawReplayScanExtent : std::uint8_t {
    kDurableOnly = 0U,
    kIncludesRecoveredAppendOnly,
};

struct RawReplaySegmentInput final {
    const RawSegmentScanResult* scan = nullptr;
    RawReplayScanExtent extent = RawReplayScanExtent::kDurableOnly;
    // Used only for kIncludesRecoveredAppendOnly. This is an exclusive,
    // record-aligned stream-day WAL cursor. Records ending at or before it
    // are durable; later records are recovered append-only.
    std::uint64_t durable_end_wal_pos = 0U;
};

struct RawReplayHalfOpenRange final {
    std::optional<std::uint64_t> begin;
    std::optional<std::uint64_t> end;
};

// Empty selector vectors mean "any". All numeric ranges are half-open and
// select on record_start_wal_pos, ingress_sequence, or recv_realtime_ns.
struct RawReplayFilter final {
    std::vector<std::uint32_t> source_stream_ids;
    std::vector<std::uint32_t> capture_dates;
    std::vector<std::uint8_t> vendor_service_ids;
    std::vector<std::uint16_t> vendor_service_versions;
    std::vector<std::uint16_t> vendor_message_ids;
    RawReplayHalfOpenRange wal;
    RawReplayHalfOpenRange ingress_sequence;
    RawReplayHalfOpenRange recv_realtime_ns;
};

struct RawReplayRunSettings final {
    RawReplayPace pace = RawReplayPace::kAsFastAsPossible;

    // Playback speed is numerator / denominator. These fields are used only
    // by kFixedMultiplier and must both be nonzero. A 2/1 multiplier replays
    // source monotonic time at 2x (half the original delay).
    std::uint64_t speed_numerator = 1U;
    std::uint64_t speed_denominator = 1U;

    // This is retained as run provenance only. Raw replay performs no
    // transformation, randomization, dropping, duplication, or reordering.
    std::uint64_t determinism_seed = 0U;

    // False by default so audit replay consumes only durable records.
    bool include_recovered_append_only = false;
};

class RawReplayClock {
public:
    virtual ~RawReplayClock() = default;

    [[nodiscard]] virtual std::uint64_t NowMonotonicNs()
        noexcept = 0;
};

class RawReplaySleeper {
public:
    virtual ~RawReplaySleeper() = default;

    // Returns false if the requested wait could not be completed.
    [[nodiscard]] virtual bool SleepForNs(
        std::uint64_t duration_ns) noexcept = 0;
};

// Default implementations for command-line/runtime callers. Tests can inject
// a virtual clock and sleeper without wall-clock delays.
class SteadyRawReplayClock final : public RawReplayClock {
public:
    [[nodiscard]] std::uint64_t NowMonotonicNs()
        noexcept override;
};

class ThreadRawReplaySleeper final : public RawReplaySleeper {
public:
    [[nodiscard]] bool SleepForNs(
        std::uint64_t duration_ns) noexcept override;
};

struct RawReplayClockIdentity final {
    std::uint32_t algorithm = 0U;
    RawV1Digest digest{};
    // Informational only; equality is algorithm + full digest.
    std::uint64_t label = 0U;
};

struct RawReplaySegmentContext final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    RawV1Identity stream_day_id{};
    std::uint32_t segment_sequence = 0U;
    RawReplayClockIdentity clock_epoch{};
};

struct RawReplayLocator final {
    std::uint32_t source_stream_id = 0U;
    std::uint32_t capture_date = 0U;
    RawV1Identity stream_day_id{};
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t record_start_wal_pos = 0U;
    std::uint64_t record_end_wal_pos = 0U;
};

struct RawReplayRecord final {
    RawRecordView view;
    RawReplaySegmentContext segment;
    RawReplayProvenance provenance = RawReplayProvenance::kDurable;
};

struct RawReplayBoundary final {
    RawReplayLocator previous;
    RawReplayLocator current;
    RawReplayClockIdentity previous_clock_epoch{};
    RawReplayClockIdentity current_clock_epoch{};
    std::uint64_t previous_recv_monotonic_ns = 0U;
    std::uint64_t current_recv_monotonic_ns = 0U;
};

enum class RawReplayError : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kNullInput,
    kInvalidScan,
    kInvalidDurableFrontier,
    kInvalidRange,
    kInvalidPace,
    kInvalidExtent,
    kInvalidMultiplier,
    kClockRequired,
    kSleeperRequired,
    kDurationOverflow,
    kSleepFailure,
    kResourceExhausted,
};

[[nodiscard]] std::string_view RawReplayErrorName(
    RawReplayError error) noexcept;

enum class RawReplayStepKind : std::uint8_t {
    kRecord = 0U,
    kClockEpochBoundary,
    kMonotonicRegression,
    kPaused,
    kEnd,
    kError,
};

struct RawReplayStep final {
    RawReplayStepKind kind = RawReplayStepKind::kEnd;
    RawReplayError error = RawReplayError::kNone;
    std::optional<RawReplayRecord> record;
    std::optional<RawReplayBoundary> boundary;
    // Actual relative wait requested from the injected sleeper for this step.
    std::uint64_t sleep_ns = 0U;
};

class RawReplayEngine final {
public:
    RawReplayEngine(const RawReplayEngine&) = delete;
    RawReplayEngine& operator=(const RawReplayEngine&) = delete;
    RawReplayEngine(RawReplayEngine&&) = delete;
    RawReplayEngine& operator=(RawReplayEngine&&) = delete;
    ~RawReplayEngine() = default;

    // Copies selected RawRecordViews and segment identities. Consequently,
    // views returned by Next() keep their immutable backing buffers alive even
    // after the input scan objects and engine have been destroyed.
    [[nodiscard]] static RawReplayError Create(
        std::span<const RawReplaySegmentInput> inputs,
        const RawReplayFilter& filter,
        const RawReplayRunSettings& settings,
        RawReplayClock* clock,
        RawReplaySleeper* sleeper,
        std::unique_ptr<RawReplayEngine>* output) noexcept;

    // Next() has one consumer. Pause(), Resume(), RequestSingleStep(), and
    // paused() are thread-safe controls that may be called from other threads.
    // This is a nonblocking control state machine: while paused, Next() returns
    // kPaused instead of waiting on a condition variable.
    [[nodiscard]] RawReplayStep Next() noexcept;
    void Pause() noexcept;
    void Resume() noexcept;
    void RequestSingleStep() noexcept;
    [[nodiscard]] bool paused() const noexcept;

    [[nodiscard]] const RawReplayRunSettings& settings()
        const noexcept {
        return settings_;
    }
    [[nodiscard]] std::size_t selected_record_count()
        const noexcept {
        return records_.size();
    }

private:
    explicit RawReplayEngine(
        std::vector<RawReplayRecord> records,
        RawReplayRunSettings settings,
        RawReplayClock* clock,
        RawReplaySleeper* sleeper) noexcept;

    [[nodiscard]] bool HasAdvancePermit(
        bool* single_step,
        std::uint64_t* pacing_reset_generation) const noexcept;
    void ConsumeSingleStepPermit() noexcept;
    [[nodiscard]] RawReplayStep Fail(
        RawReplayError error) noexcept;

    std::vector<RawReplayRecord> records_;
    RawReplayRunSettings settings_{};
    RawReplayClock* clock_ = nullptr;
    RawReplaySleeper* sleeper_ = nullptr;
    std::size_t next_index_ = 0U;

    bool has_previous_record_ = false;
    std::size_t previous_index_ = 0U;
    bool notice_emitted_for_next_ = false;
    bool reset_pacing_for_next_ = false;

    bool pace_anchor_valid_ = false;
    std::uint64_t source_anchor_ns_ = 0U;
    std::uint64_t replay_anchor_ns_ = 0U;

    RawReplayError terminal_error_ = RawReplayError::kNone;

    mutable std::mutex control_mutex_;
    bool paused_ = false;
    std::uint64_t single_step_budget_ = 0U;
    std::uint64_t pacing_reset_generation_ = 0U;
    std::uint64_t observed_pacing_reset_generation_ = 0U;
};

}  // namespace l2flow::ingress
