#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/market/intraday_instrument_store_v1.h"
#include "l2flow/market/instrument_runtime_state_v2.h"
#include "l2flow/market/market_types_v1.h"
#include "l2flow/market/realtime_kline_v1.h"
#include "l2flow/market/realtime_latest_read_model_v1.h"
#include "l2flow/realtime/processing_progress_v2.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string_view>

namespace l2flow::market {

inline constexpr std::size_t kRealtimeHistorySourceCountV1 = 4U;

// A source cut is an exclusive prefix of the sequence assigned by this
// process. It is intentionally unrelated to vendor sequence numbers:
// source_sequence < sequence_exclusive belongs to this generation.
struct RealtimeSourceWatermarkV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint64_t sequence_exclusive = 0U;
};

// Immutable identity of one frozen daily-catalog generation. The source
// sequence vector proves the process-owned input prefix, while the exact
// runtime-state snapshot fixes catalog identity and data availability. Its
// completeness claim is limited to the declared subscribed A-share scope.
struct RealtimeHistoryWatermarkV1 final {
    l2flow::common::Identity128 run_id{};
    std::uint64_t generation = 0U;
    std::uint32_t trade_date = 0U;
    std::uint64_t ingress_sequence_exclusive = 0U;
    std::uint64_t recv_monotonic_cut_ns = 0U;
    std::shared_ptr<const DailyInstrumentCatalogSnapshotV2>
        catalog_snapshot;
    l2flow::realtime::ProcessingProgressV2 processing_progress{};
    std::array<RealtimeSourceWatermarkV1,
               kRealtimeHistorySourceCountV1>
        sources{};
    l2flow::common::Sha256Digest input_identity_sha256{};
};

enum class RealtimeHistoryWatermarkErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidRun,
    kInvalidGeneration,
    kInvalidTradeDate,
    kInvalidIngressCut,
    kInvalidCatalog,
    kInvalidProgress,
    kInvalidSource,
    kDuplicateSource,
    kHashFailure,
};

[[nodiscard]] std::string_view RealtimeHistoryWatermarkErrorNameV1(
    RealtimeHistoryWatermarkErrorV1 error) noexcept;

[[nodiscard]] RealtimeHistoryWatermarkErrorV1
BuildRealtimeHistoryWatermarkV1(
    l2flow::common::Identity128 run_id,
    std::uint64_t generation,
    std::uint32_t trade_date,
    std::uint64_t ingress_sequence_exclusive,
    std::uint64_t recv_monotonic_cut_ns,
    std::shared_ptr<const DailyInstrumentCatalogSnapshotV2>
        catalog_snapshot,
    l2flow::realtime::ProcessingProgressV2 processing_progress,
    std::span<const RealtimeSourceWatermarkV1,
              kRealtimeHistorySourceCountV1> sources,
    RealtimeHistoryWatermarkV1* output) noexcept;

class RealtimeHistoryRecordV1 final {
public:
    RealtimeHistoryRecordV1(const RealtimeHistoryRecordV1&) = delete;
    RealtimeHistoryRecordV1& operator=(
        const RealtimeHistoryRecordV1&) = delete;
    RealtimeHistoryRecordV1(RealtimeHistoryRecordV1&&) = delete;
    RealtimeHistoryRecordV1& operator=(RealtimeHistoryRecordV1&&) = delete;
    // The matching store segment invokes this exactly once.  It dispatches
    // destruction of the placement-constructed exact event payload.
    ~RealtimeHistoryRecordV1();

    [[nodiscard]] std::uint8_t source_slot() const noexcept {
        return source_slot_;
    }
    [[nodiscard]] std::uint32_t source_stream_id() const noexcept {
        return source_stream_id_;
    }
    [[nodiscard]] std::uint64_t source_sequence() const noexcept {
        return source_sequence_;
    }
    [[nodiscard]] std::uint64_t ingress_sequence() const noexcept {
        return ingress_sequence_;
    }
    [[nodiscard]] std::uint64_t tick_stream_sequence() const noexcept {
        return tick_stream_sequence_;
    }
    [[nodiscard]] std::uint32_t instrument_id() const noexcept {
        return instrument_id_;
    }
    [[nodiscard]] MarketEventKindV1 kind() const noexcept { return kind_; }
    [[nodiscard]] std::int64_t event_time_ns() const noexcept {
        return event_time_ns_;
    }
    [[nodiscard]] std::int64_t recv_realtime_ns() const noexcept {
        return recv_realtime_ns_;
    }
    [[nodiscard]] std::int64_t recv_monotonic_ns() const noexcept {
        return recv_monotonic_ns_;
    }
    [[nodiscard]] StoredMarketEventViewV1 event() const noexcept;

private:
    RealtimeHistoryRecordV1(
        std::uint8_t source_slot,
        std::uint32_t source_stream_id,
        std::uint64_t source_sequence,
        std::uint64_t ingress_sequence,
        std::uint64_t tick_stream_sequence,
        std::uint32_t instrument_id,
        MarketEventKindV1 kind,
        std::int64_t event_time_ns,
        std::int64_t recv_realtime_ns,
        std::int64_t recv_monotonic_ns,
        std::uint32_t payload_delta) noexcept;

    std::uint8_t source_slot_ = 0U;
    std::uint32_t source_stream_id_ = 0U;
    std::uint64_t source_sequence_ = 0U;
    std::uint64_t ingress_sequence_ = 0U;
    std::uint64_t tick_stream_sequence_ = 0U;
    std::uint32_t instrument_id_ = 0U;
    MarketEventKindV1 kind_ = MarketEventKindV1::kShanghaiSnapshot;
    std::int64_t event_time_ns_ = 0;
    std::int64_t recv_realtime_ns_ = 0;
    std::int64_t recv_monotonic_ns_ = 0;
    // Exact payload is in the same segment allocation at
    // reinterpret_cast<byte*>(this) + payload_delta_.  A zero delta is never
    // published.
    std::uint32_t payload_delta_ = 0U;

    friend class IntradayInstrumentStoreV1;
};

// Optional application-composition boundary invoked only after Store append,
// every enabled KLine update, handoff release, and the in-process latest
// projection have all succeeded. Implementations run on permanent history
// workers and therefore must be allocation-free, nonblocking, and noexcept.
// Returning false is a required-projection failure: History marks all live
// projections coverage-lost and transitions the pipeline to fatal.
class RealtimeAppliedRecordSinkV1 {
public:
    virtual ~RealtimeAppliedRecordSinkV1() = default;

    [[nodiscard]] virtual bool PublishApplied(
        std::size_t ordinal,
        const RealtimeHistoryRecordV1& record) noexcept = 0;
    virtual void MarkCoverageLost() noexcept = 0;

    // History calls this terminal lifecycle barrier after every worker has
    // stopped publishing and before its append-only Store can be destroyed.
    // A sink that retained borrowed RealtimeHistoryRecordV1 pointers must
    // synchronously stop using them before returning. Most sinks copy/project
    // during PublishApplied and therefore need no work here.
    virtual void QuiesceRecordReferences() noexcept {}
};

// Move-only decoder output envelope. It contains no retained heap control
// block: the history handoff pool bounds its transient lifetime, and the
// instrument owner worker consumes it into the store arena.
class RealtimeHistoryEventInputV1 final {
public:
    RealtimeHistoryEventInputV1(
        const RealtimeHistoryEventInputV1&) = delete;
    RealtimeHistoryEventInputV1& operator=(
        const RealtimeHistoryEventInputV1&) = delete;
    RealtimeHistoryEventInputV1(
        RealtimeHistoryEventInputV1&& other) noexcept;
    RealtimeHistoryEventInputV1& operator=(
        RealtimeHistoryEventInputV1&& other) noexcept;
    ~RealtimeHistoryEventInputV1() = default;

    [[nodiscard]] static std::optional<RealtimeHistoryEventInputV1> Create(
        std::uint8_t source_slot,
        std::uint64_t ingress_sequence,
        DecodedMarketEventV1&& event,
        std::uint64_t tick_stream_sequence = 0U) noexcept;

    [[nodiscard]] std::uint8_t source_slot() const noexcept {
        return source_slot_;
    }
    [[nodiscard]] std::uint32_t source_stream_id() const noexcept {
        return source_stream_id_;
    }
    [[nodiscard]] std::uint64_t source_sequence() const noexcept {
        return source_sequence_;
    }
    [[nodiscard]] std::uint64_t ingress_sequence() const noexcept {
        return ingress_sequence_;
    }
    [[nodiscard]] std::uint64_t tick_stream_sequence() const noexcept {
        return tick_stream_sequence_;
    }
    [[nodiscard]] std::uint32_t instrument_id() const noexcept {
        return instrument_id_;
    }
    [[nodiscard]] std::size_t ordinal() const noexcept {
        return ordinal_;
    }
    [[nodiscard]] MarketEventKindV1 kind() const noexcept { return kind_; }
    [[nodiscard]] std::int64_t event_time_ns() const noexcept {
        return event_time_ns_;
    }
    [[nodiscard]] std::int64_t recv_realtime_ns() const noexcept {
        return recv_realtime_ns_;
    }
    [[nodiscard]] std::int64_t recv_monotonic_ns() const noexcept {
        return recv_monotonic_ns_;
    }
    [[nodiscard]] std::uint32_t vendor_local_time_raw() const noexcept {
        return vendor_local_time_raw_;
    }
    [[nodiscard]] std::uint64_t accounted_record_bytes() const noexcept {
        return accounted_record_bytes_;
    }
    [[nodiscard]] bool valid() const noexcept { return valid_; }
    [[nodiscard]] const DecodedMarketEventV1& event() const noexcept {
        return event_;
    }
    [[nodiscard]] DecodedMarketEventV1&& TakeEvent() && noexcept {
        valid_ = false;
        return std::move(event_);
    }

private:
    RealtimeHistoryEventInputV1(
        std::uint8_t source_slot,
        std::uint32_t source_stream_id,
        std::uint64_t source_sequence,
        std::uint64_t ingress_sequence,
        std::uint64_t tick_stream_sequence,
        std::uint32_t instrument_id,
        std::size_t ordinal,
        MarketEventKindV1 kind,
        std::int64_t event_time_ns,
        std::int64_t recv_realtime_ns,
        std::int64_t recv_monotonic_ns,
        std::uint32_t vendor_local_time_raw,
        std::uint64_t accounted_record_bytes,
        DecodedMarketEventV1&& event) noexcept;

    std::uint8_t source_slot_ = 0U;
    std::uint32_t source_stream_id_ = 0U;
    std::uint64_t source_sequence_ = 0U;
    std::uint64_t ingress_sequence_ = 0U;
    std::uint64_t tick_stream_sequence_ = 0U;
    std::uint32_t instrument_id_ = 0U;
    std::size_t ordinal_ =
        std::numeric_limits<std::size_t>::max();
    MarketEventKindV1 kind_ = MarketEventKindV1::kShanghaiSnapshot;
    std::int64_t event_time_ns_ = 0;
    std::int64_t recv_realtime_ns_ = 0;
    std::int64_t recv_monotonic_ns_ = 0;
    // Exact hhmmssmmm value from MDLMessageHead::LocalTime.  It remains a
    // time-of-day without a calendar date; the optional production latency
    // observer combines it only with its explicitly configured capture date.
    std::uint32_t vendor_local_time_raw_ = 0U;
    std::uint64_t accounted_record_bytes_ = 0U;
    DecodedMarketEventV1 event_;
    bool valid_ = true;
};

// A small, allocation-free publication action invoked only while the runtime
// still owns the exact current store generation under its commit lock. The
// action must be noexcept and must not call back into this runtime.
using RealtimeHistoryCommitActionV1 = void (*)(void* context) noexcept;

// Optional measurement hook invoked by the permanent owner worker only after
// IntradayInstrumentStoreV1::Append has returned kNone and an immediate
// acquire-read of the matching Store-owned record through the live latest
// model has succeeded.  The first monotonic clock read after Append defines
// append_complete_monotonic_ns; the realtime observation follows it.
// inprocess_latest_read_complete_monotonic_ns is observed only after the
// immediate read has returned and its exact record pointer, instrument,
// category, and ingress sequence have been verified.  No immutable generation
// is cut or acquired for this read.  The hook runs after these observations,
// so its own aggregation cost is excluded from both measured boundaries.  It
// must be allocation-free, nonblocking, and noexcept.
struct RealtimeHistoryAppendObservationV1 final {
    std::uint32_t worker = 0U;
    std::uint8_t source_slot = 0U;
    std::uint64_t ingress_sequence = 0U;
    std::uint32_t vendor_local_time_raw = 0U;
    std::int64_t recv_realtime_ns = 0;
    std::int64_t recv_monotonic_ns = 0;
    std::uint64_t append_start_monotonic_ns = 0U;
    std::uint64_t append_complete_monotonic_ns = 0U;
    std::uint64_t append_complete_realtime_ns = 0U;
    std::uint64_t inprocess_latest_read_complete_monotonic_ns = 0U;
    bool clock_observation_valid = false;
    bool inprocess_latest_read_observation_valid = false;
};

using RealtimeHistoryAppendObserverV1 = void (*)(
    void* context,
    const RealtimeHistoryAppendObservationV1& observation) noexcept;

// Called only after Store, KLine, latest projection, required external
// projection, and runtime availability have all succeeded. When timing is
// enabled, external_publication_complete_monotonic_ns is sampled immediately
// after the required applied_record_sink returns success. Store-append timing
// is carried by RealtimeHistoryAppendObservationV1 at its exact earlier
// boundary. applied_complete_monotonic_ns is sampled after the dense runtime
// state update. The callback advances the global contiguous applied prefix;
// it must be nonblocking, allocation-free, and noexcept.
struct RealtimeHistoryAppliedObservationV2 final {
    std::uint64_t ingress_sequence = 0U;
    std::int64_t recv_monotonic_ns = 0;
    std::uint64_t external_publication_complete_monotonic_ns = 0U;
    std::uint64_t applied_complete_monotonic_ns = 0U;
    std::uint8_t source_slot = 0U;
    bool external_publication_present = false;
    bool external_publication_clock_valid = false;
    bool applied_complete_clock_valid = false;
};

using RealtimeHistoryAppliedObserverV2 = bool (*)(
    void* context,
    const RealtimeHistoryAppliedObservationV2& observation) noexcept;

struct RealtimeHistoryRuntimeConfigV1 final {
    std::array<std::uint32_t, kRealtimeHistorySourceCountV1>
        source_stream_ids{};
    std::uint32_t worker_count = 0U;
    // Maximum in-flight record handoffs per source×worker. The command ring
    // reserves one additional internal slot for the generation fence.
    std::size_t queue_capacity_per_source_worker = 0U;
    // Borrowed dense runtime state. Its immutable identity covers every daily
    // catalog row; Store and latest reads use the same fixed ordinals.
    InstrumentRuntimeStateV2* runtime_state = nullptr;
    IntradayInstrumentStoreConfigV1 intraday_store{};
    // Empty windows disable KLine aggregation. When enabled, trade_date must
    // be the server/operator date used by the decoder. maximum_bars may be
    // zero; Create then derives a hard per-worker bound from the retained
    // history record bound and window count.
    KLineAggregatorConfigV1 kline{};
    std::shared_ptr<RealtimeAppliedRecordSinkV1> applied_record_sink;
    RealtimeHistoryAppendObserverV1 append_observer = nullptr;
    void* append_observer_context = nullptr;
    bool measure_applied_latency = false;
    RealtimeHistoryAppliedObserverV2 applied_observer = nullptr;
    void* applied_observer_context = nullptr;
};

enum class RealtimeHistoryCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kResourceExhausted,
    kThreadStartFailed,
    kStoreCreateFailed,
    kKLineCreateFailed,
    kLatestReadModelCreateFailed,
};

enum class RealtimeHistorySubmitErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidRecord,
    kSourceMismatch,
    kSequenceNotIncreasing,
    kQueueFull,
    kStopped,
    kFatal,
};

enum class RealtimeHistoryGenerationErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidWatermark,
    kGenerationNotBegun,
    kGenerationConflict,
    kSourceAlreadySealed,
    kQueueFull,
    kTimeout,
    kStopped,
    kFatal,
    kResourceExhausted,
    kStoreFailed,
    kKLineFailed,
};

[[nodiscard]] std::string_view RealtimeHistoryCreateErrorNameV1(
    RealtimeHistoryCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view RealtimeHistorySubmitErrorNameV1(
    RealtimeHistorySubmitErrorV1 error) noexcept;
[[nodiscard]] std::string_view RealtimeHistoryGenerationErrorNameV1(
    RealtimeHistoryGenerationErrorV1 error) noexcept;

// Four source owners submit concurrently. For a given source, TrySubmit and
// SealSource must be called by the same serial decoder owner. The upstream
// ingress authority must assign every accepted record one globally unique,
// dense ingress_sequence and one dense per-source source_sequence. This
// runtime validates per-source order and generation cuts; it does not invent
// a second cross-source sequence authority. Internally there is one SPSC queue
// per source×worker. An instrument is permanently owned by
// ordinal % worker_count, and IntradayInstrumentStoreV1 is the only retained
// record container. The runtime state referenced by config must outlive
// the runtime and every snapshot/generation derived from it.
class RealtimeHistoryRuntimeV1 final {
public:
    RealtimeHistoryRuntimeV1(const RealtimeHistoryRuntimeV1&) = delete;
    RealtimeHistoryRuntimeV1& operator=(
        const RealtimeHistoryRuntimeV1&) = delete;
    RealtimeHistoryRuntimeV1(RealtimeHistoryRuntimeV1&&) = delete;
    RealtimeHistoryRuntimeV1& operator=(RealtimeHistoryRuntimeV1&&) = delete;
    ~RealtimeHistoryRuntimeV1();

    [[nodiscard]] static RealtimeHistoryCreateErrorV1 Create(
        RealtimeHistoryRuntimeConfigV1 config,
        std::unique_ptr<RealtimeHistoryRuntimeV1>* output) noexcept;

    [[nodiscard]] RealtimeHistorySubmitErrorV1 TrySubmit(
        RealtimeHistoryEventInputV1&& input) noexcept;

    // BeginGeneration is called before the four decoder markers are admitted.
    // Each decoder calls SealSource after it has routed every event preceding
    // its marker. A worker parks fence-after data until all four source fences
    // for that generation have arrived and its immutable slice is complete.
    [[nodiscard]] RealtimeHistoryGenerationErrorV1 BeginGeneration(
        const RealtimeHistoryWatermarkV1& watermark) noexcept;
    [[nodiscard]] RealtimeHistoryGenerationErrorV1 SealSource(
        std::uint8_t source_slot,
        std::uint64_t generation) noexcept;
    [[nodiscard]] RealtimeHistoryGenerationErrorV1 WaitForGeneration(
        std::uint64_t generation,
        std::chrono::nanoseconds timeout,
        std::shared_ptr<const IntradayInstrumentStoreGenerationV1>* output,
        std::shared_ptr<const RealtimeKLineGenerationV1>* kline_output =
            nullptr)
        noexcept;

    [[nodiscard]] std::shared_ptr<
        const IntradayInstrumentStoreGenerationV1>
    AcquireLatestGeneration() const noexcept;
    [[nodiscard]] std::shared_ptr<const RealtimeKLineGenerationV1>
    AcquireLatestKLineGeneration() const noexcept;
    // Allocation-free live point reads. A returned record is the latest
    // successfully applied record for that instrument and category, ordered
    // by process ingress_sequence. Batch reads preserve input order but are
    // per-instrument observations, not one cross-instrument generation.
    // Borrowed record pointers remain valid only while this runtime lives.
    [[nodiscard]] RealtimeLatestQueryErrorV1 GetLatestSnapshot(
        std::uint32_t instrument_id,
        RealtimeLatestRecordViewV1* output) const noexcept;
    [[nodiscard]] RealtimeLatestQueryErrorV1 GetLatestSnapshots(
        std::span<const std::uint32_t> instrument_ids,
        std::span<RealtimeLatestRecordViewV1> output) const noexcept;
    [[nodiscard]] RealtimeLatestQueryErrorV1 GetLatestTick(
        std::uint32_t instrument_id,
        RealtimeLatestRecordViewV1* output) const noexcept;
    [[nodiscard]] RealtimeLatestQueryErrorV1 GetLatestTicks(
        std::span<const std::uint32_t> instrument_ids,
        std::span<RealtimeLatestRecordViewV1> output) const noexcept;
    [[nodiscard]] IntradayInstrumentStoreSnapshotV1
    StoreSnapshot() const noexcept;
    [[nodiscard]] bool IsGenerationCurrentAndHealthy(
        const std::shared_ptr<
            const IntradayInstrumentStoreGenerationV1>& generation) const
        noexcept;

    // Linearizes a downstream whole-generation publication with store
    // generation replacement, sticky coverage failure, fatal transition, and
    // StopAndDrain. "Exact" requires both the same pointer and the same
    // shared_ptr owner/control block. Returns false without invoking action
    // unless generation is still that exact current healthy handle. This is
    // the factor publication commit guard; it is not an API for long-running
    // calculation.
    [[nodiscard]] bool CommitIfCurrentAndHealthy(
        const std::shared_ptr<
            const IntradayInstrumentStoreGenerationV1>& generation,
        RealtimeHistoryCommitActionV1 action,
        void* context) const noexcept;

    [[nodiscard]] std::uint32_t WorkerForInstrument(
        std::uint32_t instrument_id) const noexcept;
    [[nodiscard]] bool fatal() const noexcept;
    [[nodiscard]] RealtimeHistoryGenerationErrorV1 FailureError()
        const noexcept;
    void MarkFatal() noexcept;
    void StopAndDrain() noexcept;

    [[nodiscard]] const RealtimeHistoryRuntimeConfigV1& config()
        const noexcept;

private:
    class Impl;
    explicit RealtimeHistoryRuntimeV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::market
