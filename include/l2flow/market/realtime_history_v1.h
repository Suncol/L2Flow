#pragma once

#include "l2flow/common/identity128.h"
#include "l2flow/common/sha256.h"
#include "l2flow/market/intraday_instrument_store_v1.h"
#include "l2flow/market/instrument_registry.h"
#include "l2flow/market/market_types_v1.h"

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
// process.  It is intentionally unrelated to vendor sequence numbers and WAL
// offsets: source_sequence < sequence_exclusive belongs to this generation.
struct RealtimeSourceWatermarkV1 final {
    std::uint32_t source_stream_id = 0U;
    std::uint64_t sequence_exclusive = 0U;
};

// Immutable identity of one complete market-history generation.  The ingress
// sequence vector is the completeness authority.  recv_monotonic_cut_ns is an
// observation timestamp for latency/staleness only; it is not presented as an
// exchange-event-time completeness proof.
struct RealtimeHistoryWatermarkV1 final {
    l2flow::common::Identity128 run_id{};
    std::uint64_t generation = 0U;
    std::uint32_t trade_date = 0U;
    std::uint64_t ingress_sequence_exclusive = 0U;
    std::uint64_t recv_monotonic_cut_ns = 0U;
    std::uint64_t registry_version = 0U;
    l2flow::common::Sha256Digest registry_sha256{};
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
    kInvalidRegistry,
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
    const InstrumentRegistryV1& registry,
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

// Move-only decoder output envelope.  It contains no durable owner or heap
// control block: the history handoff pool bounds its transient lifetime, and
// the instrument owner worker consumes it into the store arena.
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
        DecodedMarketEventV1&& event) noexcept;

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
    [[nodiscard]] std::uint32_t instrument_id() const noexcept {
        return instrument_id_;
    }
    [[nodiscard]] std::size_t registry_ordinal() const noexcept {
        return registry_ordinal_;
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
        std::uint32_t instrument_id,
        std::size_t registry_ordinal,
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
    std::uint32_t instrument_id_ = 0U;
    std::size_t registry_ordinal_ =
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
// IntradayInstrumentStoreV1::Append has returned kNone.  The first monotonic
// clock read after that return defines append_complete_monotonic_ns; the
// realtime observation follows it.  The hook runs after both observations, so
// its own aggregation cost is excluded from the measured append boundary.
// It must be allocation-free, nonblocking, and noexcept.
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
    bool clock_observation_valid = false;
};

using RealtimeHistoryAppendObserverV1 = void (*)(
    void* context,
    const RealtimeHistoryAppendObservationV1& observation) noexcept;

struct RealtimeHistoryRuntimeConfigV1 final {
    std::array<std::uint32_t, kRealtimeHistorySourceCountV1>
        source_stream_ids{};
    std::uint32_t worker_count = 0U;
    // Maximum in-flight record handoffs per source×worker. The command ring
    // reserves one additional internal slot for the generation fence.
    std::size_t queue_capacity_per_source_worker = 0U;
    const InstrumentRegistryV1* registry = nullptr;
    IntradayInstrumentStoreConfigV1 intraday_store{};
    RealtimeHistoryAppendObserverV1 append_observer = nullptr;
    void* append_observer_context = nullptr;
};

enum class RealtimeHistoryCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kResourceExhausted,
    kThreadStartFailed,
    kStoreCreateFailed,
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
// instrument_id % worker_count, and IntradayInstrumentStoreV1 is the only
// retained record container. The immutable registry referenced by config must
// outlive the runtime and all generations/cursors derived from it.
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
        std::shared_ptr<const IntradayInstrumentStoreGenerationV1>* output)
        noexcept;

    [[nodiscard]] std::shared_ptr<
        const IntradayInstrumentStoreGenerationV1>
    AcquireLatestGeneration() const noexcept;
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
