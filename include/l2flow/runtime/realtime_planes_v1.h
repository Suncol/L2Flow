#pragma once

#include "l2flow/common/linux_thread_affinity_v1.h"
#include "l2flow/market/fast_tick_store_v1.h"
#include "l2flow/market/mutable_kline_history_v1.h"
#include "l2flow/market/ordered_event_history_v1.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

namespace l2flow::runtime {

struct RealtimePlaneAffinityV1 final {
    // Production sets enforce=true and supplies exactly one nonempty mask per
    // live worker. Tick/Event/KLine masks must be pairwise disjoint. Tests may
    // disable affinity explicitly to remain portable across CI cpusets.
    bool enforce = false;
    std::vector<l2flow::common::LinuxCpuSetV1> tick_workers;
    std::vector<l2flow::common::LinuxCpuSetV1> event_workers;
    std::vector<l2flow::common::LinuxCpuSetV1> kline_workers;
};

struct RealtimePlanesConfigV1 final {
    l2flow::market::FastTickStoreConfigV1 fast;
    l2flow::market::OrderedEventHistoryConfigV1 event;
    l2flow::market::MutableKLineHistoryConfigV1 kline;
    std::size_t tick_queue_capacity_per_source_worker = 4096U;
    std::size_t event_queue_capacity_per_source_worker = 4096U;
    std::size_t kline_queue_capacity_per_source_worker = 4096U;
    std::size_t live_batch_budget = 256U;
    RealtimePlaneAffinityV1 affinity{};
};

enum class RealtimePlanesCreateErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidConfiguration,
    kFastStoreCreateFailed,
    kEventHistoryCreateFailed,
    kKLineHistoryCreateFailed,
    kThreadStartFailed,
    kAffinityFailed,
    kResourceExhausted,
};

enum class RealtimeRouteErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidInput,
    kConcurrentSourceProducer,
    kFastQueueFull,
    kFastCoverageLost,
    kStopped,
};

[[nodiscard]] std::string_view RealtimePlanesCreateErrorNameV1(
    RealtimePlanesCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view RealtimeRouteErrorNameV1(
    RealtimeRouteErrorV1 error) noexcept;

struct RealtimeRouteResultV1 final {
    RealtimeRouteErrorV1 error = RealtimeRouteErrorV1::kNone;
    bool fast_enqueued = false;
    bool event_enqueued = false;
    bool event_repair_registered = false;
    bool kline_enqueued = false;
    bool kline_repair_registered = false;
};

struct RealtimePlanesSnapshotV1 final {
    std::uint64_t routed_ticks = 0U;
    std::uint64_t fast_queue_failures = 0U;
    std::uint64_t fast_unrecoverable_drops = 0U;
    std::uint64_t event_queue_failures = 0U;
    std::uint64_t kline_queue_failures = 0U;
    std::uint64_t fast_applied = 0U;
    std::uint64_t event_applied = 0U;
    std::uint64_t kline_applied = 0U;
    std::uint64_t event_rebuild_attempts = 0U;
    std::uint64_t kline_rebuild_attempts = 0U;
    bool accepting = false;
    bool stopped = false;
};

// Two serialized decoder/source owners call RouteDecoded. The router always
// performs the FAST TryPush first. Event and KLine use independent queue
// matrices, worker threads, repair threads, route tables, stores, wakeups and
// failure states; neither can wait in or backpressure the source callback.
class RealtimePlanesV1 final {
public:
    RealtimePlanesV1(const RealtimePlanesV1&) = delete;
    RealtimePlanesV1& operator=(const RealtimePlanesV1&) = delete;
    RealtimePlanesV1(RealtimePlanesV1&&) = delete;
    RealtimePlanesV1& operator=(RealtimePlanesV1&&) = delete;
    ~RealtimePlanesV1();

    [[nodiscard]] static RealtimePlanesCreateErrorV1 Create(
        RealtimePlanesConfigV1 config,
        std::unique_ptr<RealtimePlanesV1>* output,
        std::string* detail = nullptr) noexcept;

    [[nodiscard]] RealtimeRouteResultV1 RouteDecoded(
        l2flow::market::CompactFastTickV1 compact,
        l2flow::market::DecodedFastTickV1&& owned_tick) noexcept;

    // Test/operator drain primitive; this samples only one instrument's FAST
    // publication and never establishes a cross-plane or global cut.
    [[nodiscard]] bool WaitFastPublished(
        std::uint32_t instrument_id,
        std::uint64_t arrival_id,
        std::chrono::nanoseconds timeout) const noexcept;

    [[nodiscard]] const l2flow::market::FastTickStoreV1& fast_store()
        const noexcept;
    [[nodiscard]] const l2flow::market::OrderedEventHistoryV1&
    event_history() const noexcept;
    [[nodiscard]] const l2flow::market::MutableKLineHistoryV1&
    kline_history() const noexcept;
    [[nodiscard]] RealtimePlanesSnapshotV1 Snapshot() const noexcept;
    [[nodiscard]] const RealtimePlanesConfigV1& config()
    const noexcept;

    // Decoder/admission failure means the raw fact prefix is incomplete for
    // this instrument.  FAST and both derived planes then fail closed for
    // completeness while unrelated instruments continue normally.
    void MarkFastCoverageLost(std::uint32_t instrument_id) noexcept;

    void StopAndDrain() noexcept;

private:
    class Impl;
    explicit RealtimePlanesV1(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::runtime
