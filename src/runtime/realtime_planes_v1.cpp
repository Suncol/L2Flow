#include "l2flow/runtime/realtime_planes_v1.h"

#include <array>
#include <atomic>
#include <chrono>
#include <limits>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace l2flow::runtime {
namespace {

template <typename T>
class SpscQueueV1 final {
public:
    explicit SpscQueueV1(std::size_t capacity)
        : slot_count_(capacity + 1U),
          slots_(std::make_unique<T[]>(slot_count_)) {}

    SpscQueueV1(const SpscQueueV1&) = delete;
    SpscQueueV1& operator=(const SpscQueueV1&) = delete;

    [[nodiscard]] bool TryPush(T&& value) noexcept {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t next = Advance(tail);
        if (next == head_.load(std::memory_order_acquire)) {
            return false;
        }
        slots_[tail] = std::move(value);
        tail_.store(next, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool TryPop(T* output) noexcept {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t tail = tail_.load(std::memory_order_acquire);
        if (head == tail) {
            return false;
        }
        *output = std::move(slots_[head]);
        head_.store(Advance(head), std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) ==
               tail_.load(std::memory_order_acquire);
    }

private:
    [[nodiscard]] std::size_t Advance(std::size_t index) const noexcept {
        ++index;
        return index == slot_count_ ? 0U : index;
    }

    std::size_t slot_count_ = 0U;
    std::unique_ptr<T[]> slots_;
    alignas(64) std::atomic<std::size_t> head_{0U};
    alignas(64) std::atomic<std::size_t> tail_{0U};
};

struct WorkerSignalV1 final {
    std::atomic<std::uint64_t> epoch{0U};

    void Notify() noexcept {
        epoch.fetch_add(1U, std::memory_order_release);
        epoch.notify_one();
    }

    void NotifyAll() noexcept {
        epoch.fetch_add(1U, std::memory_order_release);
        epoch.notify_all();
    }
};

[[nodiscard]] bool CpuSetsOverlap(
    const l2flow::common::LinuxCpuSetV1& lhs,
    const l2flow::common::LinuxCpuSetV1& rhs) noexcept {
    for (std::size_t cpu = 0U;
         cpu < l2flow::common::kLinuxCpuSetMaximumCpuCountV1;
         ++cpu) {
        if (lhs.contains(cpu) && rhs.contains(cpu)) {
            return true;
        }
    }
    return false;
}

[[nodiscard]] bool ValidAffinity(
    const RealtimePlanesConfigV1& config) noexcept {
    if (!config.affinity.enforce) {
        return true;
    }
    if (config.affinity.tick_workers.size() !=
            config.fast.worker_count ||
        config.affinity.event_workers.size() !=
            config.event.worker_count ||
        config.affinity.kline_workers.size() !=
            config.kline.worker_count) {
        return false;
    }
    std::vector<const l2flow::common::LinuxCpuSetV1*> masks;
    for (const auto& mask : config.affinity.tick_workers) {
        if (mask.empty()) {
            return false;
        }
        masks.push_back(&mask);
    }
    for (const auto& mask : config.affinity.event_workers) {
        if (mask.empty()) {
            return false;
        }
        masks.push_back(&mask);
    }
    for (const auto& mask : config.affinity.kline_workers) {
        if (mask.empty()) {
            return false;
        }
        masks.push_back(&mask);
    }
    for (std::size_t lhs = 0U; lhs < masks.size(); ++lhs) {
        for (std::size_t rhs = lhs + 1U; rhs < masks.size(); ++rhs) {
            if (CpuSetsOverlap(*masks[lhs], *masks[rhs])) {
                return false;
            }
        }
    }
    return true;
}

[[nodiscard]] bool SameIdentity(
    const l2flow::common::Identity128& lhs,
    const l2flow::common::Identity128& rhs) noexcept {
    return lhs == rhs;
}

}  // namespace

std::string_view RealtimePlanesCreateErrorNameV1(
    RealtimePlanesCreateErrorV1 error) noexcept {
    switch (error) {
        case RealtimePlanesCreateErrorV1::kNone:
            return "none";
        case RealtimePlanesCreateErrorV1::kNullOutput:
            return "null_output";
        case RealtimePlanesCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case RealtimePlanesCreateErrorV1::kFastStoreCreateFailed:
            return "fast_store_create_failed";
        case RealtimePlanesCreateErrorV1::kEventHistoryCreateFailed:
            return "event_history_create_failed";
        case RealtimePlanesCreateErrorV1::kKLineHistoryCreateFailed:
            return "kline_history_create_failed";
        case RealtimePlanesCreateErrorV1::kThreadStartFailed:
            return "thread_start_failed";
        case RealtimePlanesCreateErrorV1::kAffinityFailed:
            return "affinity_failed";
        case RealtimePlanesCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

std::string_view RealtimePublishErrorNameV1(
    RealtimePublishErrorV1 error) noexcept {
    switch (error) {
        case RealtimePublishErrorV1::kNone:
            return "none";
        case RealtimePublishErrorV1::kInvalidInput:
            return "invalid_input";
        case RealtimePublishErrorV1::kWrongTickWorker:
            return "wrong_tick_worker";
        case RealtimePublishErrorV1::kConcurrentTickWorker:
            return "concurrent_tick_worker";
        case RealtimePublishErrorV1::kFastAppendFailed:
            return "fast_append_failed";
        case RealtimePublishErrorV1::kFastCoverageLost:
            return "fast_coverage_lost";
        case RealtimePublishErrorV1::kStopped:
            return "stopped";
    }
    return "unknown";
}

class RealtimePlanesV1::Impl final {
public:
    explicit Impl(RealtimePlanesConfigV1 config)
        : config_(std::move(config)),
          event_signals_(std::make_unique<WorkerSignalV1[]>(
              config_.event.worker_count)),
          kline_signals_(std::make_unique<WorkerSignalV1[]>(
              config_.kline.worker_count)),
          kline_repair_signals_(std::make_unique<WorkerSignalV1[]>(
              config_.kline.worker_count)),
          event_repair_scan_requested_(
              std::make_unique<std::atomic<bool>[]>(
                  config_.event.worker_count)),
          fast_route_live_(std::make_unique<std::atomic<bool>[]>(
              config_.fast.instrument_count)),
          tick_worker_guards_(std::make_unique<std::atomic_flag[]>(
              config_.fast.worker_count)) {
        for (std::size_t ordinal = 0U;
             ordinal < config_.fast.instrument_count; ++ordinal) {
            fast_route_live_[ordinal].store(
                true, std::memory_order_relaxed);
        }
        for (std::uint32_t worker = 0U;
             worker < config_.event.worker_count; ++worker) {
            event_repair_scan_requested_[worker].store(
                false, std::memory_order_relaxed);
        }
    }

    ~Impl() { StopAndDrain(); }

    [[nodiscard]] std::size_t QueueIndex(
        std::size_t tick_worker,
        std::uint32_t worker,
        std::uint32_t worker_count) const noexcept {
        return tick_worker * worker_count + worker;
    }

    [[nodiscard]] bool ApplyAffinity(
        const std::vector<l2flow::common::LinuxCpuSetV1>& masks,
        std::uint32_t worker) noexcept {
        if (!config_.affinity.enforce) {
            return true;
        }
        return worker < masks.size() &&
               l2flow::common::ApplyCurrentLinuxThreadAffinityExactV1(
                   masks[worker]) ==
                   l2flow::common::LinuxThreadAffinityErrorV1::kNone;
    }

    void MarkStarted(bool affinity_ok) noexcept {
        if (!affinity_ok) {
            affinity_failed_.store(true, std::memory_order_release);
        }
        started_threads_.fetch_add(1U, std::memory_order_release);
        started_threads_.notify_all();
    }

    [[nodiscard]] bool CompactQueuesEmpty(
        const std::vector<std::unique_ptr<
            SpscQueueV1<l2flow::market::CompactFastTickV1>>>& queues,
        std::uint32_t worker,
        std::uint32_t worker_count) const noexcept {
        for (std::size_t tick_worker = 0U;
             tick_worker < config_.fast.worker_count;
             ++tick_worker) {
            if (!queues[QueueIndex(
                    tick_worker, worker, worker_count)]->empty()) {
                return false;
            }
        }
        return true;
    }

    void EventLoop(std::uint32_t worker) noexcept {
        const bool affinity_ok = ApplyAffinity(
            config_.affinity.event_workers, worker);
        MarkStarted(affinity_ok);
        std::size_t producer_cursor = 0U;
        std::vector<l2flow::market::CompactFastTickV1> batch;
        batch.reserve(config_.live_batch_budget);
        std::vector<std::size_t> assigned_ordinals;
        for (std::size_t ordinal = 0U;
             ordinal < config_.event.instrument_count; ++ordinal) {
            if (config_.event.event_routes[ordinal] == worker) {
                assigned_ordinals.push_back(ordinal);
            }
        }
        // Fixed-capacity, worker-local dirty queue. Normal micro-batches only
        // inspect the instruments they touched; the O(route-count) scan is
        // reserved for the exceptional queue-overflow notification.
        std::vector<std::size_t> repair_ring(assigned_ordinals.size());
        std::vector<bool> repair_queued(
            config_.event.instrument_count, false);
        std::size_t repair_head = 0U;
        std::size_t repair_tail = 0U;
        std::size_t repair_count = 0U;
        const auto repair_pending =
            [this](std::uint32_t instrument_id) noexcept {
            const auto state = event_history_->RepairState(instrument_id);
            return state == l2flow::market::EventRepairStateV1::
                                kRepairRequired ||
                   state == l2flow::market::EventRepairStateV1::
                                kRebuilding ||
                   state == l2flow::market::EventRepairStateV1::
                                kCatchingUp;
        };
        const auto enqueue_repair = [&](std::size_t ordinal) noexcept {
            if (ordinal >= config_.event.instrument_count ||
                config_.event.event_routes[ordinal] != worker ||
                repair_queued[ordinal] ||
                !repair_pending(
                    static_cast<std::uint32_t>(ordinal + 1U)) ||
                repair_count >= repair_ring.size()) {
                return;
            }
            repair_ring[repair_tail] = ordinal;
            repair_tail = (repair_tail + 1U) % repair_ring.size();
            ++repair_count;
            repair_queued[ordinal] = true;
        };
        const auto scan_repairs = [&]() noexcept {
            for (std::size_t ordinal : assigned_ordinals) {
                enqueue_repair(ordinal);
            }
        };
        while (affinity_ok) {
            const std::uint64_t observed_epoch =
                event_signals_[worker].epoch.load(
                    std::memory_order_acquire);
            bool worked = false;
            batch.clear();
            for (std::size_t count = 0U;
                 count < config_.live_batch_budget; ++count) {
                bool consumed = false;
                for (std::size_t offset = 0U;
                     offset < config_.fast.worker_count;
                     ++offset) {
                    const std::size_t tick_worker =
                        (producer_cursor + offset) %
                        config_.fast.worker_count;
                    l2flow::market::CompactFastTickV1 input{};
                    if (!event_queues_[QueueIndex(
                            tick_worker,
                            worker,
                            config_.event.worker_count)]->TryPop(
                            &input)) {
                        continue;
                    }
                    batch.push_back(std::move(input));
                    producer_cursor = (tick_worker + 1U) %
                        config_.fast.worker_count;
                    worked = true;
                    consumed = true;
                    break;
                }
                if (!consumed) {
                    break;
                }
            }
            if (!batch.empty()) {
                const auto applied = event_history_->ApplyBatch(
                    worker, batch);
                event_applied_.fetch_add(
                    applied.published_inputs,
                    std::memory_order_relaxed);
                for (const auto& input : batch) {
                    enqueue_repair(input.ordinal);
                }
            }
            if (event_repair_scan_requested_[worker].exchange(
                    false, std::memory_order_acq_rel)) {
                scan_repairs();
            }

            if (repair_count != 0U) {
                const std::size_t ordinal = repair_ring[repair_head];
                repair_head = (repair_head + 1U) % repair_ring.size();
                --repair_count;
                repair_queued[ordinal] = false;
                const std::uint32_t instrument_id =
                    static_cast<std::uint32_t>(ordinal + 1U);
                if (repair_pending(instrument_id)) {
                    const auto advanced =
                        event_history_->AdvanceDirtyReplay(
                        worker,
                        event_routes_[ordinal],
                        config_.event.repair_replay_record_budget,
                        config_.event.repair_cpu_budget_per_round);
                    if (advanced.cold_fallback_required) {
                        const std::uint64_t through =
                            event_history_->RepairThrough(instrument_id);
                        // A journal gap is exceptional. Coalesce it until
                        // this drain round observes no queued Event input;
                        // rebuilding a growing FAST prefix once per callback
                        // would amplify overload and spend CDC capacity on
                        // obsolete roots.
                        if (batch.empty() &&
                            (through == 0U ||
                             fast_store_->PublishedThrough(
                                 instrument_id, through))) {
                            event_rebuild_attempts_.fetch_add(
                                1U, std::memory_order_relaxed);
                            const auto rebuilt =
                                event_history_->RebuildFromFast(
                                    worker,
                                    event_routes_[ordinal],
                                    *fast_store_);
                            worked = worked || rebuilt.complete;
                            if (worker_stop_requested_.load(
                                    std::memory_order_acquire) &&
                                rebuilt.error != l2flow::market::
                                    OrderedEventHistoryErrorV1::kNone) {
                                event_history_->MarkUnrecoverable(
                                    instrument_id);
                            }
                        }
                    } else if (advanced.worked || advanced.committed) {
                        event_rebuild_attempts_.fetch_add(
                            1U, std::memory_order_relaxed);
                        worked = true;
                    }
                    if (worker_stop_requested_.load(
                            std::memory_order_acquire) &&
                        advanced.error != l2flow::market::
                            OrderedEventHistoryErrorV1::kNone) {
                        event_history_->MarkUnrecoverable(instrument_id);
                    }
                    enqueue_repair(ordinal);
                }
            }
            if (worker_stop_requested_.load(std::memory_order_acquire) &&
                CompactQueuesEmpty(
                    event_queues_, worker, config_.event.worker_count)) {
                if (repair_count == 0U) {
                    scan_repairs();
                }
                if (repair_count == 0U) {
                    break;
                }
            }
            if (!worked) {
                if (repair_count != 0U ||
                    event_repair_scan_requested_[worker].load(
                        std::memory_order_acquire)) {
                    std::this_thread::yield();
                } else {
                    event_signals_[worker].epoch.wait(
                        observed_epoch, std::memory_order_acquire);
                }
            }
        }
    }

    void KLineLoop(std::uint32_t worker) noexcept {
        const bool affinity_ok = ApplyAffinity(
            config_.affinity.kline_workers, worker);
        MarkStarted(affinity_ok);
        std::size_t producer_cursor = 0U;
        while (affinity_ok) {
            const std::uint64_t observed_epoch =
                kline_signals_[worker].epoch.load(
                    std::memory_order_acquire);
            bool worked = false;
            for (std::size_t count = 0U;
                 count < config_.live_batch_budget; ++count) {
                bool consumed = false;
                for (std::size_t offset = 0U;
                     offset < config_.fast.worker_count;
                     ++offset) {
                    const std::size_t tick_worker =
                        (producer_cursor + offset) %
                        config_.fast.worker_count;
                    l2flow::market::CompactFastTickV1 input{};
                    if (!kline_queues_[QueueIndex(
                            tick_worker,
                            worker,
                            config_.kline.worker_count)]->TryPop(
                            &input)) {
                        continue;
                    }
                    const auto& route = kline_routes_[input.ordinal];
                    if (kline_history_->RepairState(input.instrument_id) ==
                        l2flow::market::EventRepairStateV1::kLive) {
                        const auto applied = kline_history_->ApplyLive(
                            worker, route, input);
                        if (applied.error ==
                                l2flow::market::
                                    MutableKLineHistoryErrorV1::kNone &&
                            applied.disposition ==
                                l2flow::market::
                                    KLineInputDispositionV1::kUpserted) {
                            kline_applied_.fetch_add(
                                1U, std::memory_order_relaxed);
                        }
                        if (kline_history_->RepairState(
                                input.instrument_id) ==
                            l2flow::market::EventRepairStateV1::
                                kRepairRequired) {
                            kline_repair_signals_[worker].Notify();
                        }
                    } else {
                        kline_history_->MarkRepairRequired(
                            input.instrument_id, input.arrival_id);
                        kline_repair_signals_[worker].Notify();
                    }
                    producer_cursor = (tick_worker + 1U) %
                        config_.fast.worker_count;
                    worked = true;
                    consumed = true;
                    break;
                }
                if (!consumed) {
                    break;
                }
            }
            if (worker_stop_requested_.load(std::memory_order_acquire) &&
                CompactQueuesEmpty(
                    kline_queues_, worker, config_.kline.worker_count)) {
                break;
            }
            if (!worked) {
                kline_signals_[worker].epoch.wait(
                    observed_epoch, std::memory_order_acquire);
            }
        }
    }

    [[nodiscard]] bool KLineRepairPending(std::uint32_t worker) const
        noexcept {
        for (std::size_t ordinal = 0U;
             ordinal < config_.kline.instrument_count;
             ++ordinal) {
            if (config_.kline.kline_routes[ordinal] != worker) {
                continue;
            }
            const auto state = kline_history_->RepairState(
                static_cast<std::uint32_t>(ordinal + 1U));
            if (state == l2flow::market::EventRepairStateV1::
                             kRepairRequired ||
                state == l2flow::market::EventRepairStateV1::
                             kRebuilding ||
                state == l2flow::market::EventRepairStateV1::
                             kCatchingUp) {
                return true;
            }
        }
        return false;
    }

    void KLineRepairLoop(std::uint32_t worker) noexcept {
        const bool affinity_ok = ApplyAffinity(
            config_.affinity.kline_workers, worker);
        MarkStarted(affinity_ok);
        std::size_t next_ordinal = 0U;
        while (affinity_ok) {
            const std::uint64_t observed_epoch =
                kline_repair_signals_[worker].epoch.load(
                    std::memory_order_acquire);
            bool worked = false;
            bool retry_backoff = false;
            for (std::size_t offset = 0U;
                 offset < config_.kline.instrument_count; ++offset) {
                const std::size_t ordinal =
                    (next_ordinal + offset) %
                    config_.kline.instrument_count;
                if (config_.kline.kline_routes[ordinal] != worker) {
                    continue;
                }
                const std::uint32_t instrument_id =
                    static_cast<std::uint32_t>(ordinal + 1U);
                const auto state = kline_history_->RepairState(
                    instrument_id);
                if (state != l2flow::market::EventRepairStateV1::
                                 kRepairRequired &&
                    state != l2flow::market::EventRepairStateV1::
                                 kRebuilding &&
                    state != l2flow::market::EventRepairStateV1::
                                 kCatchingUp) {
                    continue;
                }
                const std::uint64_t repair_through =
                    kline_history_->RepairThrough(instrument_id);
                if (repair_through != 0U &&
                    !fast_store_->PublishedThrough(
                        instrument_id, repair_through)) {
                    continue;
                }
                kline_rebuild_attempts_.fetch_add(
                    1U, std::memory_order_relaxed);
                const auto rebuild = kline_history_->RebuildFromFast(
                    worker, kline_routes_[ordinal], *fast_store_);
                next_ordinal = (ordinal + 1U) %
                    config_.kline.instrument_count;
                if (repair_stop_requested_.load(
                        std::memory_order_acquire) &&
                    rebuild.error !=
                        l2flow::market::MutableKLineHistoryErrorV1::kNone) {
                    kline_history_->MarkUnrecoverable(instrument_id);
                }
                retry_backoff = rebuild.error ==
                                    l2flow::market::
                                        MutableKLineHistoryErrorV1::
                                            kResourceExhausted ||
                                rebuild.error ==
                                    l2flow::market::
                                        MutableKLineHistoryErrorV1::
                                            kNotLive;
                worked = true;
                break;
            }
            if (repair_stop_requested_.load(std::memory_order_acquire) &&
                !KLineRepairPending(worker)) {
                break;
            }
            if (retry_backoff) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(1));
            } else if (!worked) {
                kline_repair_signals_[worker].epoch.wait(
                    observed_epoch, std::memory_order_acquire);
            }
        }
    }

    void NotifyAllWorkers() noexcept {
        for (std::uint32_t worker = 0U;
             worker < config_.event.worker_count; ++worker) {
            event_signals_[worker].NotifyAll();
        }
        for (std::uint32_t worker = 0U;
             worker < config_.kline.worker_count; ++worker) {
            kline_signals_[worker].NotifyAll();
            kline_repair_signals_[worker].NotifyAll();
        }
    }

    void StopAndDrain() noexcept {
        bool expected = false;
        if (!stopping_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            while (!stopped_.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            return;
        }
        accepting_.store(false, std::memory_order_release);
        worker_stop_requested_.store(true, std::memory_order_release);
        NotifyAllWorkers();
        for (std::thread& thread : event_threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
        for (std::thread& thread : kline_threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
        repair_stop_requested_.store(true, std::memory_order_release);
        NotifyAllWorkers();
        for (std::thread& thread : kline_repair_threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
        stopped_.store(true, std::memory_order_release);
    }

    RealtimePlanesConfigV1 config_{};
    std::unique_ptr<l2flow::market::FastTickStoreV1> fast_store_;
    std::unique_ptr<l2flow::market::OrderedEventHistoryV1>
        event_history_;
    std::unique_ptr<l2flow::market::MutableKLineHistoryV1>
        kline_history_;
    std::vector<l2flow::market::FastTickRouteTokenV1> tick_routes_;
    std::vector<l2flow::market::EventRouteTokenV1> event_routes_;
    std::vector<l2flow::market::KLineRouteTokenV1> kline_routes_;
    std::vector<std::unique_ptr<
        SpscQueueV1<l2flow::market::CompactFastTickV1>>>
        event_queues_;
    std::vector<std::unique_ptr<
        SpscQueueV1<l2flow::market::CompactFastTickV1>>>
        kline_queues_;
    std::unique_ptr<WorkerSignalV1[]> event_signals_;
    std::unique_ptr<WorkerSignalV1[]> kline_signals_;
    std::unique_ptr<WorkerSignalV1[]> kline_repair_signals_;
    std::unique_ptr<std::atomic<bool>[]>
        event_repair_scan_requested_;
    std::vector<std::thread> event_threads_;
    std::vector<std::thread> kline_threads_;
    std::vector<std::thread> kline_repair_threads_;
    std::unique_ptr<std::atomic<bool>[]> fast_route_live_;
    std::unique_ptr<std::atomic_flag[]> tick_worker_guards_;
    std::atomic<std::uint64_t> started_threads_{0U};
    std::atomic<bool> affinity_failed_{false};
    std::atomic<bool> accepting_{false};
    std::atomic<bool> worker_stop_requested_{false};
    std::atomic<bool> repair_stop_requested_{false};
    std::atomic<bool> stopping_{false};
    std::atomic<bool> stopped_{false};
    std::atomic<std::uint64_t> fast_append_failures_{0U};
    std::atomic<std::uint64_t> fast_unrecoverable_drops_{0U};
    std::atomic<std::uint64_t> event_queue_failures_{0U};
    std::atomic<std::uint64_t> kline_queue_failures_{0U};
    std::atomic<std::uint64_t> fast_applied_{0U};
    std::atomic<std::uint64_t> event_applied_{0U};
    std::atomic<std::uint64_t> kline_applied_{0U};
    std::atomic<std::uint64_t> event_rebuild_attempts_{0U};
    std::atomic<std::uint64_t> kline_rebuild_attempts_{0U};
};

RealtimePlanesV1::RealtimePlanesV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

RealtimePlanesV1::~RealtimePlanesV1() {
    if (impl_ != nullptr) {
        impl_->StopAndDrain();
    }
}

RealtimePlanesCreateErrorV1 RealtimePlanesV1::Create(
    RealtimePlanesConfigV1 config,
    std::unique_ptr<RealtimePlanesV1>* output,
    std::string* detail) noexcept {
    if (output == nullptr) {
        return RealtimePlanesCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (detail != nullptr) {
        detail->clear();
    }
    if (!SameIdentity(config.fast.session_id, config.event.session_id) ||
        !SameIdentity(config.fast.session_id, config.kline.session_id) ||
        config.fast.trade_date != config.event.trade_date ||
        config.fast.trade_date != config.kline.trade_date ||
        config.fast.instrument_count != config.event.instrument_count ||
        config.fast.instrument_count != config.kline.instrument_count ||
        config.event_queue_capacity_per_tick_worker == 0U ||
        config.kline_queue_capacity_per_tick_worker == 0U ||
        config.event_queue_capacity_per_tick_worker ==
            std::numeric_limits<std::size_t>::max() ||
        config.kline_queue_capacity_per_tick_worker ==
            std::numeric_limits<std::size_t>::max() ||
        config.live_batch_budget == 0U || !ValidAffinity(config)) {
        return RealtimePlanesCreateErrorV1::kInvalidConfiguration;
    }
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        if (l2flow::market::FastTickStoreV1::Create(
                impl->config_.fast, &impl->fast_store_) !=
            l2flow::market::FastTickStoreCreateErrorV1::kNone) {
            return RealtimePlanesCreateErrorV1::kFastStoreCreateFailed;
        }
        if (l2flow::market::OrderedEventHistoryV1::Create(
                impl->config_.event, &impl->event_history_) !=
            l2flow::market::OrderedEventHistoryCreateErrorV1::kNone) {
            return RealtimePlanesCreateErrorV1::
                kEventHistoryCreateFailed;
        }
        if (l2flow::market::MutableKLineHistoryV1::Create(
                impl->config_.kline, &impl->kline_history_) !=
            l2flow::market::MutableKLineHistoryCreateErrorV1::kNone) {
            return RealtimePlanesCreateErrorV1::
                kKLineHistoryCreateFailed;
        }

        const std::size_t instruments = impl->config_.fast.instrument_count;
        impl->tick_routes_.resize(instruments);
        impl->event_routes_.resize(instruments);
        impl->kline_routes_.resize(instruments);
        for (std::size_t ordinal = 0U; ordinal < instruments; ++ordinal) {
            const std::uint32_t instrument_id =
                static_cast<std::uint32_t>(ordinal + 1U);
            if (impl->fast_store_->ResolveRoute(
                    ordinal,
                    instrument_id,
                    &impl->tick_routes_[ordinal]) !=
                    l2flow::market::FastTickStoreQueryErrorV1::kNone ||
                impl->event_history_->ResolveRoute(
                    ordinal,
                    instrument_id,
                    &impl->event_routes_[ordinal]) !=
                    l2flow::market::OrderedEventHistoryErrorV1::kNone ||
                impl->kline_history_->ResolveRoute(
                    ordinal,
                    instrument_id,
                    &impl->kline_routes_[ordinal]) !=
                    l2flow::market::MutableKLineHistoryErrorV1::kNone) {
                return RealtimePlanesCreateErrorV1::
                    kInvalidConfiguration;
            }
        }

        for (std::uint32_t tick_worker = 0U;
             tick_worker < impl->config_.fast.worker_count;
             ++tick_worker) {
            for (std::uint32_t worker = 0U;
                 worker < impl->config_.event.worker_count; ++worker) {
                impl->event_queues_.push_back(std::make_unique<
                    SpscQueueV1<l2flow::market::CompactFastTickV1>>(
                    impl->config_
                        .event_queue_capacity_per_tick_worker));
            }
            for (std::uint32_t worker = 0U;
                 worker < impl->config_.kline.worker_count; ++worker) {
                impl->kline_queues_.push_back(std::make_unique<
                    SpscQueueV1<l2flow::market::CompactFastTickV1>>(
                    impl->config_
                        .kline_queue_capacity_per_tick_worker));
            }
        }

        impl->event_threads_.reserve(impl->config_.event.worker_count);
        impl->kline_threads_.reserve(impl->config_.kline.worker_count);
        impl->kline_repair_threads_.reserve(
            impl->config_.kline.worker_count);
        for (std::uint32_t worker = 0U;
             worker < impl->config_.event.worker_count; ++worker) {
            impl->event_threads_.emplace_back(
                [owner = impl.get(), worker] { owner->EventLoop(worker); });
        }
        for (std::uint32_t worker = 0U;
             worker < impl->config_.kline.worker_count; ++worker) {
            impl->kline_threads_.emplace_back(
                [owner = impl.get(), worker] { owner->KLineLoop(worker); });
            impl->kline_repair_threads_.emplace_back(
                [owner = impl.get(), worker] {
                    owner->KLineRepairLoop(worker);
                });
        }
        const std::uint64_t expected_threads =
            static_cast<std::uint64_t>(
                impl->config_.event.worker_count) +
            2ULL * static_cast<std::uint64_t>(
                impl->config_.kline.worker_count);
        std::uint64_t started = impl->started_threads_.load(
            std::memory_order_acquire);
        while (started != expected_threads) {
            impl->started_threads_.wait(started, std::memory_order_acquire);
            started = impl->started_threads_.load(
                std::memory_order_acquire);
        }
        if (impl->affinity_failed_.load(std::memory_order_acquire)) {
            if (detail != nullptr) {
                *detail = "worker affinity application/readback failed";
            }
            impl->StopAndDrain();
            return RealtimePlanesCreateErrorV1::kAffinityFailed;
        }
        impl->accepting_.store(true, std::memory_order_release);
        output->reset(new RealtimePlanesV1(std::move(impl)));
        return RealtimePlanesCreateErrorV1::kNone;
    } catch (const std::system_error& error) {
        if (detail != nullptr) {
            try {
                *detail = error.what();
            } catch (...) {
                detail->clear();
            }
        }
        return RealtimePlanesCreateErrorV1::kThreadStartFailed;
    } catch (...) {
        return RealtimePlanesCreateErrorV1::kResourceExhausted;
    }
}

RealtimePublishResultV1 RealtimePlanesV1::PublishDecoded(
    std::uint32_t tick_worker,
    l2flow::market::CompactFastTickV1 compact,
    l2flow::market::DecodedFastTickV1&& owned_tick) noexcept {
    RealtimePublishResultV1 result{};
    if (impl_ == nullptr || compact.instrument_id == 0U ||
        tick_worker >= impl_->config_.fast.worker_count ||
        compact.ordinal >= static_cast<std::size_t>(
            std::numeric_limits<std::uint32_t>::max()) ||
        compact.ordinal >= impl_->config_.fast.instrument_count ||
        compact.instrument_id !=
            static_cast<std::uint32_t>(compact.ordinal + 1U)) {
        result.error = RealtimePublishErrorV1::kInvalidInput;
        return result;
    }
    if (!impl_->accepting_.load(std::memory_order_acquire)) {
        result.error = RealtimePublishErrorV1::kStopped;
        return result;
    }
    const std::size_t source = static_cast<std::size_t>(compact.source);
    if (source >= l2flow::market::kFastTickSourceCountV1) {
        result.error = RealtimePublishErrorV1::kInvalidInput;
        return result;
    }
    const auto& tick_route = impl_->tick_routes_[compact.ordinal];
    if (tick_route.worker != tick_worker) {
        impl_->fast_store_->MarkCoverageLost(compact.instrument_id);
        impl_->event_history_->MarkUnrecoverable(compact.instrument_id);
        impl_->kline_history_->MarkUnrecoverable(compact.instrument_id);
        impl_->fast_route_live_[compact.ordinal].store(
            false, std::memory_order_release);
        impl_->fast_unrecoverable_drops_.fetch_add(
            1U, std::memory_order_relaxed);
        result.error = RealtimePublishErrorV1::kWrongTickWorker;
        return result;
    }
    if (impl_->tick_worker_guards_[tick_worker].test_and_set(
            std::memory_order_acquire)) {
        impl_->fast_store_->MarkCoverageLost(compact.instrument_id);
        impl_->event_history_->MarkUnrecoverable(compact.instrument_id);
        impl_->kline_history_->MarkUnrecoverable(compact.instrument_id);
        impl_->fast_route_live_[compact.ordinal].store(
            false, std::memory_order_release);
        impl_->fast_unrecoverable_drops_.fetch_add(
            1U, std::memory_order_relaxed);
        result.error = RealtimePublishErrorV1::kConcurrentTickWorker;
        return result;
    }
    struct Guard final {
        std::atomic_flag* flag = nullptr;
        ~Guard() { flag->clear(std::memory_order_release); }
    } guard{&impl_->tick_worker_guards_[tick_worker]};

    if (!impl_->fast_route_live_[compact.ordinal].load(
            std::memory_order_acquire)) {
        impl_->fast_unrecoverable_drops_.fetch_add(
            1U, std::memory_order_relaxed);
        result.error = RealtimePublishErrorV1::kFastCoverageLost;
        return result;
    }
    const auto& event_route = impl_->event_routes_[compact.ordinal];
    const auto& kline_route = impl_->kline_routes_[compact.ordinal];
    const auto append_error = impl_->fast_store_->Append(
        tick_worker, tick_route, compact, std::move(owned_tick));
    if (append_error !=
        l2flow::market::FastTickStoreAppendErrorV1::kNone) {
        impl_->fast_append_failures_.fetch_add(
            1U, std::memory_order_relaxed);
        impl_->fast_store_->MarkCoverageLost(compact.instrument_id);
        impl_->event_history_->MarkUnrecoverable(compact.instrument_id);
        impl_->kline_history_->MarkUnrecoverable(compact.instrument_id);
        impl_->fast_route_live_[compact.ordinal].store(
            false, std::memory_order_release);
        result.error = RealtimePublishErrorV1::kFastAppendFailed;
        return result;
    }
    result.fast_published = true;
    impl_->fast_applied_.fetch_add(1U, std::memory_order_relaxed);
    impl_->kline_repair_signals_[kline_route.worker].Notify();

    auto event_envelope = compact;
    if (impl_->event_queues_[impl_->QueueIndex(
            tick_worker,
            event_route.worker,
            impl_->config_.event.worker_count)]->TryPush(
            std::move(event_envelope))) {
        result.event_enqueued = true;
        impl_->event_signals_[event_route.worker].Notify();
    } else {
        impl_->event_queue_failures_.fetch_add(
            1U, std::memory_order_relaxed);
        impl_->event_history_->MarkRepairRequired(
            compact.instrument_id, compact.arrival_id);
        result.event_repair_registered = true;
        impl_->event_repair_scan_requested_[event_route.worker].store(
            true, std::memory_order_release);
        impl_->event_signals_[event_route.worker].Notify();
    }

    if (l2flow::market::IsKLineTradeV1(compact)) {
        if (impl_->kline_history_->RepairState(compact.instrument_id) ==
            l2flow::market::EventRepairStateV1::kLive) {
            auto kline_envelope = compact;
            if (impl_->kline_queues_[impl_->QueueIndex(
                    tick_worker,
                    kline_route.worker,
                    impl_->config_.kline.worker_count)]->TryPush(
                    std::move(kline_envelope))) {
                result.kline_enqueued = true;
                impl_->kline_signals_[kline_route.worker].Notify();
            } else {
                impl_->kline_queue_failures_.fetch_add(
                    1U, std::memory_order_relaxed);
                impl_->kline_history_->MarkRepairRequired(
                    compact.instrument_id, compact.arrival_id);
                result.kline_repair_registered = true;
                impl_->kline_repair_signals_[kline_route.worker].Notify();
            }
        } else {
            impl_->kline_history_->MarkRepairRequired(
                compact.instrument_id, compact.arrival_id);
            result.kline_repair_registered = true;
            impl_->kline_repair_signals_[kline_route.worker].Notify();
        }
    }
    return result;
}

bool RealtimePlanesV1::WaitFastPublished(
    std::uint32_t instrument_id,
    std::uint64_t arrival_id,
    std::chrono::nanoseconds timeout) const noexcept {
    if (impl_ == nullptr || timeout <= std::chrono::nanoseconds::zero()) {
        return false;
    }
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (impl_->fast_store_->PublishedThrough(
                instrument_id, arrival_id)) {
            return true;
        }
        std::this_thread::yield();
    }
    return impl_->fast_store_->PublishedThrough(
        instrument_id, arrival_id);
}

const l2flow::market::FastTickStoreV1& RealtimePlanesV1::fast_store()
    const noexcept {
    return *impl_->fast_store_;
}

const l2flow::market::OrderedEventHistoryV1&
RealtimePlanesV1::event_history() const noexcept {
    return *impl_->event_history_;
}

const l2flow::market::MutableKLineHistoryV1&
RealtimePlanesV1::kline_history() const noexcept {
    return *impl_->kline_history_;
}

RealtimePlanesSnapshotV1 RealtimePlanesV1::Snapshot() const noexcept {
    RealtimePlanesSnapshotV1 result{};
    if (impl_ == nullptr) {
        return result;
    }
    result.fast_append_failures = impl_->fast_append_failures_.load(
        std::memory_order_acquire);
    result.fast_unrecoverable_drops =
        impl_->fast_unrecoverable_drops_.load(
            std::memory_order_acquire);
    result.event_queue_failures = impl_->event_queue_failures_.load(
        std::memory_order_acquire);
    result.kline_queue_failures = impl_->kline_queue_failures_.load(
        std::memory_order_acquire);
    result.fast_applied = impl_->fast_applied_.load(
        std::memory_order_acquire);
    result.event_applied = impl_->event_applied_.load(
        std::memory_order_acquire);
    result.kline_applied = impl_->kline_applied_.load(
        std::memory_order_acquire);
    result.event_rebuild_attempts = impl_->event_rebuild_attempts_.load(
        std::memory_order_acquire);
    result.kline_rebuild_attempts = impl_->kline_rebuild_attempts_.load(
        std::memory_order_acquire);
    result.accepting = impl_->accepting_.load(std::memory_order_acquire);
    result.stopped = impl_->stopped_.load(std::memory_order_acquire);
    return result;
}

const RealtimePlanesConfigV1& RealtimePlanesV1::config()
    const noexcept {
    return impl_->config_;
}

void RealtimePlanesV1::MarkFastCoverageLost(
    std::uint32_t instrument_id) noexcept {
    if (impl_ == nullptr) {
        return;
    }
    impl_->fast_store_->MarkCoverageLost(instrument_id);
    impl_->event_history_->MarkUnrecoverable(instrument_id);
    impl_->kline_history_->MarkUnrecoverable(instrument_id);
    if (instrument_id != 0U &&
        instrument_id <= impl_->config_.fast.instrument_count) {
        impl_->fast_route_live_[instrument_id - 1U].store(
            false, std::memory_order_release);
    }
}

void RealtimePlanesV1::StopAndDrain() noexcept {
    if (impl_ != nullptr) {
        impl_->StopAndDrain();
    }
}

}  // namespace l2flow::runtime
