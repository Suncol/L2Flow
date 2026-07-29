#include "l2flow/realtime/contiguous_sequence_tracker_v2.h"

#include <algorithm>
#include <limits>
#include <new>
#include <thread>

namespace l2flow::realtime {

std::string_view ContiguousSequenceTrackerCreateErrorNameV2(
    ContiguousSequenceTrackerCreateErrorV2 error) noexcept {
    switch (error) {
        case ContiguousSequenceTrackerCreateErrorV2::kNone:
            return "none";
        case ContiguousSequenceTrackerCreateErrorV2::kNullOutput:
            return "null_output";
        case ContiguousSequenceTrackerCreateErrorV2::kInvalidCapacity:
            return "invalid_capacity";
        case ContiguousSequenceTrackerCreateErrorV2::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

std::string_view ContiguousSequenceMarkErrorNameV2(
    ContiguousSequenceMarkErrorV2 error) noexcept {
    switch (error) {
        case ContiguousSequenceMarkErrorV2::kNone:
            return "none";
        case ContiguousSequenceMarkErrorV2::kInvalidSequence:
            return "invalid_sequence";
        case ContiguousSequenceMarkErrorV2::kDuplicateSequence:
            return "duplicate_sequence";
        case ContiguousSequenceMarkErrorV2::kReorderWindowExceeded:
            return "reorder_window_exceeded";
        case ContiguousSequenceMarkErrorV2::kSlotConflict:
            return "slot_conflict";
    }
    return "unknown";
}

class ContiguousSequenceTrackerV2::Impl final {
public:
    explicit Impl(std::size_t capacity)
        : capacity_(capacity),
          slots_(std::make_unique<std::atomic<std::uint64_t>[]>(capacity)) {
        for (std::size_t index = 0U; index < capacity_; ++index) {
            slots_[index].store(0U, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] ContiguousSequenceMarkErrorV2 Mark(
        std::uint64_t sequence) noexcept {
        if (sequence == 0U ||
            sequence == std::numeric_limits<std::uint64_t>::max()) {
            failed_marks_.fetch_add(1U, std::memory_order_relaxed);
            return ContiguousSequenceMarkErrorV2::kInvalidSequence;
        }

        // Mark validation, cell publication, and prefix advancement share one
        // short critical section. Without this ordering, a duplicate caller
        // could read the old prefix, be descheduled, then republish into a
        // cell after the first caller had cleared it while advancing.
        std::size_t spins = 0U;
        while (advance_lock_.test_and_set(std::memory_order_acquire)) {
            ++spins;
            if ((spins & 0xffU) == 0U) {
                std::this_thread::yield();
            }
        }
        struct AdvanceLockGuard final {
            std::atomic_flag* lock = nullptr;
            ~AdvanceLockGuard() {
                lock->clear(std::memory_order_release);
            }
        } advance_guard{&advance_lock_};

        const std::uint64_t next =
            next_sequence_.load(std::memory_order_acquire);
        if (sequence < next) {
            failed_marks_.fetch_add(1U, std::memory_order_relaxed);
            return ContiguousSequenceMarkErrorV2::kDuplicateSequence;
        }
        const std::uint64_t distance = sequence - next;
        if (distance >= static_cast<std::uint64_t>(capacity_)) {
            failed_marks_.fetch_add(1U, std::memory_order_relaxed);
            return ContiguousSequenceMarkErrorV2::
                kReorderWindowExceeded;
        }

        std::atomic<std::uint64_t>& slot =
            slots_[Index(sequence)];
        std::uint64_t empty = 0U;
        if (!slot.compare_exchange_strong(
                empty,
                sequence,
                std::memory_order_release,
                std::memory_order_acquire)) {
            failed_marks_.fetch_add(1U, std::memory_order_relaxed);
            return empty == sequence
                       ? ContiguousSequenceMarkErrorV2::
                             kDuplicateSequence
                       : ContiguousSequenceMarkErrorV2::kSlotConflict;
        }
        pending_sequences_.fetch_add(1U, std::memory_order_relaxed);

        std::uint64_t highest =
            highest_observed_sequence_.load(std::memory_order_relaxed);
        while (highest < sequence &&
               !highest_observed_sequence_.compare_exchange_weak(
                   highest,
                   sequence,
                   std::memory_order_relaxed,
                   std::memory_order_relaxed)) {
        }

        Advance();
        return ContiguousSequenceMarkErrorV2::kNone;
    }

    [[nodiscard]] std::uint64_t contiguous_sequence() const noexcept {
        return next_sequence_.load(std::memory_order_acquire) - 1U;
    }

    [[nodiscard]] ContiguousSequenceTrackerSnapshotV2 Snapshot()
        const noexcept {
        ContiguousSequenceTrackerSnapshotV2 result{};
        result.capacity = capacity_;
        result.contiguous_sequence = contiguous_sequence();
        result.highest_observed_sequence =
            highest_observed_sequence_.load(std::memory_order_acquire);
        result.pending_sequences =
            pending_sequences_.load(std::memory_order_acquire);
        result.failed_marks =
            failed_marks_.load(std::memory_order_acquire);
        return result;
    }

private:
    [[nodiscard]] std::size_t Index(
        std::uint64_t sequence) const noexcept {
        return static_cast<std::size_t>(
            (sequence - 1U) %
            static_cast<std::uint64_t>(capacity_));
    }

    void Advance() noexcept {
        std::uint64_t next =
            next_sequence_.load(std::memory_order_relaxed);
        while (true) {
            std::atomic<std::uint64_t>& slot = slots_[Index(next)];
            if (slot.load(std::memory_order_acquire) != next) {
                break;
            }
            const std::uint64_t removed =
                slot.exchange(0U, std::memory_order_acq_rel);
            if (removed != next) {
                // The advance lock gives this function the only consumer.
                // A mismatch can therefore arise only from memory
                // corruption; leave the prefix conservative.
                if (removed != 0U) {
                    slot.store(removed, std::memory_order_release);
                }
                break;
            }
            pending_sequences_.fetch_sub(
                1U, std::memory_order_relaxed);
            ++next;
            next_sequence_.store(next, std::memory_order_release);
        }
    }

    std::size_t capacity_ = 0U;
    std::unique_ptr<std::atomic<std::uint64_t>[]> slots_;
    std::atomic<std::uint64_t> next_sequence_{1U};
    std::atomic<std::uint64_t> highest_observed_sequence_{0U};
    std::atomic<std::uint64_t> pending_sequences_{0U};
    std::atomic<std::uint64_t> failed_marks_{0U};
    std::atomic_flag advance_lock_ = ATOMIC_FLAG_INIT;
};

ContiguousSequenceTrackerV2::ContiguousSequenceTrackerV2(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ContiguousSequenceTrackerV2::~ContiguousSequenceTrackerV2() = default;

ContiguousSequenceTrackerCreateErrorV2
ContiguousSequenceTrackerV2::Create(
    std::size_t capacity,
    std::unique_ptr<ContiguousSequenceTrackerV2>* output) noexcept {
    if (output == nullptr) {
        return ContiguousSequenceTrackerCreateErrorV2::kNullOutput;
    }
    output->reset();
    if (capacity == 0U ||
        capacity >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max())) {
        return ContiguousSequenceTrackerCreateErrorV2::
            kInvalidCapacity;
    }
    try {
        auto impl = std::make_unique<Impl>(capacity);
        output->reset(
            new ContiguousSequenceTrackerV2(std::move(impl)));
        return ContiguousSequenceTrackerCreateErrorV2::kNone;
    } catch (...) {
        output->reset();
        return ContiguousSequenceTrackerCreateErrorV2::
            kResourceExhausted;
    }
}

ContiguousSequenceMarkErrorV2
ContiguousSequenceTrackerV2::MarkCompleted(
    std::uint64_t sequence) noexcept {
    return impl_->Mark(sequence);
}

std::uint64_t
ContiguousSequenceTrackerV2::contiguous_sequence() const noexcept {
    return impl_->contiguous_sequence();
}

ContiguousSequenceTrackerSnapshotV2
ContiguousSequenceTrackerV2::Snapshot() const noexcept {
    return impl_->Snapshot();
}

}  // namespace l2flow::realtime
