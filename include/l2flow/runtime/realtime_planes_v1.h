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
    // live worker. Tick/Event/KLine masks must be pairwise disjoint. Tick
    // masks are applied by FastTickPipelineV1, which owns decoding and FAST
    // publication; the derived-plane masks are applied here. Tests may disable
    // affinity explicitly to remain portable across CI cpusets.
    bool enforce = false;
    std::vector<l2flow::common::LinuxCpuSetV1> tick_workers;
    std::vector<l2flow::common::LinuxCpuSetV1> event_workers;
    std::vector<l2flow::common::LinuxCpuSetV1> kline_workers;
};

struct RealtimePlanesConfigV1 final {
    l2flow::market::FastTickStoreConfigV1 fast;
    l2flow::market::OrderedEventHistoryConfigV1 event;
    l2flow::market::MutableKLineHistoryConfigV1 kline;
    // Each queue has exactly one Tick-worker producer and one derived-worker
    // consumer. There is no decoded Tick queue in front of FAST history.
    std::size_t event_queue_capacity_per_tick_worker = 4096U;
    std::size_t kline_queue_capacity_per_tick_worker = 4096U;
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

enum class RealtimePublishErrorV1 : std::uint8_t {
    kNone = 0U,
    kInvalidInput,
    kWrongTickWorker,
    kConcurrentTickWorker,
    kFastAppendFailed,
    kFastCoverageLost,
    kStopped,
};

[[nodiscard]] std::string_view RealtimePlanesCreateErrorNameV1(
    RealtimePlanesCreateErrorV1 error) noexcept;
[[nodiscard]] std::string_view RealtimePublishErrorNameV1(
    RealtimePublishErrorV1 error) noexcept;

struct RealtimePublishResultV1 final {
    RealtimePublishErrorV1 error = RealtimePublishErrorV1::kNone;
    bool fast_published = false;
    bool event_enqueued = false;
    bool event_repair_registered = false;
    bool kline_enqueued = false;
    bool kline_repair_registered = false;
};

struct RealtimePlanesSnapshotV1 final {
    std::uint64_t fast_append_failures = 0U;
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

// A permanent Tick worker calls PublishDecoded after it has decoded and
// projected one raw message. PublishDecoded appends and publishes FAST
// synchronously on that same worker before attempting either compact derived
// queue. Event and KLine use independent Tick-worker-by-derived-worker SPSC
// matrices, route tables, stores, wakeups and failure states. Event suffix
// repair is cooperatively sliced on its owning Event worker; KLine retains a
// separate cold-rebuild worker.
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

    [[nodiscard]] RealtimePublishResultV1 PublishDecoded(
        std::uint32_t tick_worker,
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
