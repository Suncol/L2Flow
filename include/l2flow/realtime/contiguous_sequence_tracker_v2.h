#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

namespace l2flow::realtime {

enum class ContiguousSequenceTrackerCreateErrorV2 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidCapacity,
    kResourceExhausted,
};

enum class ContiguousSequenceMarkErrorV2 : std::uint8_t {
    kNone = 0U,
    kInvalidSequence,
    kDuplicateSequence,
    kReorderWindowExceeded,
    kSlotConflict,
};

[[nodiscard]] std::string_view
ContiguousSequenceTrackerCreateErrorNameV2(
    ContiguousSequenceTrackerCreateErrorV2 error) noexcept;
[[nodiscard]] std::string_view ContiguousSequenceMarkErrorNameV2(
    ContiguousSequenceMarkErrorV2 error) noexcept;

struct ContiguousSequenceTrackerSnapshotV2 final {
    std::size_t capacity = 0U;
    std::uint64_t contiguous_sequence = 0U;
    std::uint64_t highest_observed_sequence = 0U;
    std::uint64_t pending_sequences = 0U;
    std::uint64_t failed_marks = 0U;
};

// Tracks the greatest contiguous completed prefix when permanent Store
// workers complete globally sequenced records out of order. The caller must
// enforce sequence - contiguous_sequence <= capacity before admitting work
// that can complete; queue capacities alone do not prove that bound. Every
// accepted sequence is marked exactly once; duplicates and a sequence that
// could alias a still-pending ring cell fail closed.
class ContiguousSequenceTrackerV2 final {
public:
    ContiguousSequenceTrackerV2(
        const ContiguousSequenceTrackerV2&) = delete;
    ContiguousSequenceTrackerV2& operator=(
        const ContiguousSequenceTrackerV2&) = delete;
    ContiguousSequenceTrackerV2(
        ContiguousSequenceTrackerV2&&) = delete;
    ContiguousSequenceTrackerV2& operator=(
        ContiguousSequenceTrackerV2&&) = delete;
    ~ContiguousSequenceTrackerV2();

    [[nodiscard]] static ContiguousSequenceTrackerCreateErrorV2 Create(
        std::size_t capacity,
        std::unique_ptr<ContiguousSequenceTrackerV2>* output) noexcept;

    [[nodiscard]] ContiguousSequenceMarkErrorV2 MarkCompleted(
        std::uint64_t sequence) noexcept;

    [[nodiscard]] std::uint64_t contiguous_sequence() const noexcept;
    [[nodiscard]] ContiguousSequenceTrackerSnapshotV2 Snapshot()
        const noexcept;

private:
    class Impl;
    explicit ContiguousSequenceTrackerV2(
        std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace l2flow::realtime
