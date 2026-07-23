#include "l2flow/market/instrument_history_v1.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <iterator>
#include <limits>
#include <mutex>
#include <new>
#include <shared_mutex>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>

namespace l2flow::market {
namespace {

constexpr std::size_t kLaneCount = 2U;

[[nodiscard]] std::size_t LaneIndex(
    InstrumentHistoryLaneV1 lane) noexcept {
    return static_cast<std::size_t>(lane);
}

[[nodiscard]] bool ValidLane(InstrumentHistoryLaneV1 lane) noexcept {
    return LaneIndex(lane) < kLaneCount;
}

struct RetainedDescription final {
    const DecodedMarketCommonV1* common = nullptr;
    MarketEventKindV1 expected_kind =
        MarketEventKindV1::kShanghaiSnapshot;
};

[[nodiscard]] RetainedDescription DescribeRetained(
    const RetainedMarketEventV1& event) noexcept {
    return std::visit(
        [](const auto& owner) noexcept {
            using Owner = std::decay_t<decltype(owner)>;
            using Element = typename Owner::element_type;
            using Value = std::remove_const_t<Element>;
            RetainedDescription result{};
            if (owner == nullptr) {
                return result;
            }
            result.common = &owner->common;
            if constexpr (std::is_same_v<Value, ShanghaiSnapshotV1>) {
                result.expected_kind =
                    MarketEventKindV1::kShanghaiSnapshot;
            } else if constexpr (std::is_same_v<Value, ShanghaiTickV1>) {
                result.expected_kind = MarketEventKindV1::kShanghaiTick;
            } else if constexpr (
                std::is_same_v<Value, ShenzhenSnapshotV1>) {
                result.expected_kind =
                    MarketEventKindV1::kShenzhenSnapshot;
            } else if constexpr (std::is_same_v<Value, ShenzhenOrderV1>) {
                result.expected_kind = MarketEventKindV1::kShenzhenOrder;
            } else {
                static_assert(
                    std::is_same_v<Value, ShenzhenTransactionV1>);
                result.expected_kind =
                    MarketEventKindV1::kShenzhenTransaction;
            }
            return result;
        },
        event);
}

enum class StoreAppendError : std::uint8_t {
    kNone = 0U,
    kWrongShard,
    kInvalidSource,
    kSequenceNotIncreasing,
    kRecordCapacity,
    kInstrumentCapacity,
    kPayloadCapacity,
    kResourceExhausted,
};

class InstrumentShardStore final {
private:
    struct Chunk;
    struct ImmutableChunkSlice;

public:
    struct StoredHandle final {
        std::shared_ptr<const void> owner;
        const OwnedInstrumentEventEnvelopeV1* record = nullptr;
    };

    InstrumentShardStore(
        std::uint8_t logical_shard,
        std::size_t chunk_capacity,
        std::uint64_t maximum_records,
        std::uint32_t maximum_instruments,
        std::uint64_t maximum_owned_payload_bytes,
        InstrumentHistoryRuntimeConfigV1::AfterQuerySnapshotHook
            after_query_snapshot_hook,
        void* after_query_snapshot_hook_context)
        : logical_shard_(logical_shard),
          chunk_capacity_(chunk_capacity),
          maximum_records_(maximum_records),
          maximum_instruments_(maximum_instruments),
          maximum_owned_payload_bytes_(maximum_owned_payload_bytes),
          after_query_snapshot_hook_(after_query_snapshot_hook),
          after_query_snapshot_hook_context_(
              after_query_snapshot_hook_context) {}

    [[nodiscard]] StoreAppendError Append(
        OwnedInstrumentEventEnvelopeV1&& envelope) noexcept {
        if (envelope.logical_shard() != logical_shard_) {
            return StoreAppendError::kWrongShard;
        }
        if (envelope.source_slot() >= kInstrumentHistorySourceCountV1 ||
            !ValidLane(envelope.lane())) {
            return StoreAppendError::kInvalidSource;
        }

        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (record_count_ >= maximum_records_) {
            return StoreAppendError::kRecordCapacity;
        }
        const std::uintmax_t payload_bytes =
            static_cast<std::uintmax_t>(envelope.owned_payload_bytes());
        if (payload_bytes > maximum_owned_payload_bytes_ ||
            owned_payload_bytes_ >
                maximum_owned_payload_bytes_ -
                    static_cast<std::uint64_t>(payload_bytes)) {
            return StoreAppendError::kPayloadCapacity;
        }
        const std::uint32_t instrument_id = envelope.instrument_id();
        const std::uint8_t source_slot = envelope.source_slot();
        const std::size_t lane_index = LaneIndex(envelope.lane());
        auto found = instruments_.find(instrument_id);
        if (found == instruments_.end()) {
            if (instruments_.size() >= maximum_instruments_) {
                return StoreAppendError::kInstrumentCapacity;
            }
            try {
                auto state = std::make_unique<InstrumentState>();
                auto lane_owner = std::make_unique<LaneState>();
                LaneState& lane = *lane_owner;
                auto chunk = std::make_shared<Chunk>(chunk_capacity_);
                chunk->Append(std::move(envelope));
                lane.record_count = 1U;
                lane.chunks.push_back(std::move(chunk));
                state->lanes[source_slot][lane_index] =
                    std::move(lane_owner);
                const auto inserted = instruments_.emplace(
                    instrument_id, std::move(state));
                if (!inserted.second) {
                    return StoreAppendError::kResourceExhausted;
                }
                ++record_count_;
                owned_payload_bytes_ +=
                    static_cast<std::uint64_t>(payload_bytes);
                return StoreAppendError::kNone;
            } catch (...) {
                return StoreAppendError::kResourceExhausted;
            }
        }

        std::unique_ptr<LaneState>& lane_owner =
            found->second->lanes[source_slot][lane_index];
        if (lane_owner == nullptr) {
            try {
                auto candidate_lane = std::make_unique<LaneState>();
                auto chunk = std::make_shared<Chunk>(chunk_capacity_);
                chunk->Append(std::move(envelope));
                candidate_lane->record_count = 1U;
                candidate_lane->chunks.push_back(std::move(chunk));
                lane_owner = std::move(candidate_lane);
                ++record_count_;
                owned_payload_bytes_ +=
                    static_cast<std::uint64_t>(payload_bytes);
                return StoreAppendError::kNone;
            } catch (...) {
                return StoreAppendError::kResourceExhausted;
            }
        }
        LaneState& lane = *lane_owner;
        if (!lane.chunks.empty() &&
            envelope.source_sequence() <=
                lane.chunks.back()->last_source_sequence) {
            return StoreAppendError::kSequenceNotIncreasing;
        }
        if (!lane.chunks.empty() &&
            lane.chunks.back()->records.size() < chunk_capacity_) {
            // Chunk capacity was reserved at creation, and the envelope move
            // is noexcept.  Publication cannot allocate in this branch.
            lane.chunks.back()->Append(std::move(envelope));
            ++lane.record_count;
            ++record_count_;
            owned_payload_bytes_ +=
                static_cast<std::uint64_t>(payload_bytes);
            return StoreAppendError::kNone;
        }

        try {
            auto chunk = std::make_shared<Chunk>(chunk_capacity_);
            chunk->Append(std::move(envelope));
            lane.chunks.push_back(std::move(chunk));
            ++lane.record_count;
            ++record_count_;
            owned_payload_bytes_ +=
                static_cast<std::uint64_t>(payload_bytes);
            return StoreAppendError::kNone;
        } catch (...) {
            return StoreAppendError::kResourceExhausted;
        }
    }

    [[nodiscard]] InstrumentHistoryQueryErrorV1 Latest(
        std::uint32_t instrument_id,
        std::uint8_t source_slot,
        InstrumentHistoryLaneV1 lane,
        std::uint64_t visible_source_sequence,
        StoredHandle* output) const noexcept {
        if (output == nullptr) {
            return InstrumentHistoryQueryErrorV1::kNullOutput;
        }
        *output = StoredHandle{};
        if (instrument_id == 0U ||
            instrument_id % kInstrumentHistoryLogicalShardCountV1 !=
                logical_shard_ ||
            source_slot >= kInstrumentHistorySourceCountV1 ||
            !ValidLane(lane) || visible_source_sequence == 0U) {
            return InstrumentHistoryQueryErrorV1::kInvalidArgument;
        }
        std::shared_lock<std::shared_mutex> lock(mutex_);
        const LaneState* state = FindLaneLocked(
            instrument_id, source_slot, lane);
        if (state == nullptr || state->chunks.empty()) {
            return InstrumentHistoryQueryErrorV1::kNotFound;
        }
        auto chunk_it = std::upper_bound(
            state->chunks.begin(), state->chunks.end(),
            visible_source_sequence,
            [](std::uint64_t sequence,
               const std::shared_ptr<Chunk>& chunk) noexcept {
                return sequence < chunk->first_source_sequence;
            });
        if (chunk_it == state->chunks.begin()) {
            return InstrumentHistoryQueryErrorV1::kNotFound;
        }
        --chunk_it;
        const std::shared_ptr<Chunk>& chunk = *chunk_it;
        const auto record_it = std::upper_bound(
            chunk->records.begin(), chunk->records.end(),
            visible_source_sequence,
            [](std::uint64_t sequence,
               const OwnedInstrumentEventEnvelopeV1& record) noexcept {
                return sequence < record.source_sequence();
            });
        if (record_it == chunk->records.begin()) {
            return InstrumentHistoryQueryErrorV1::kNotFound;
        }
        const auto selected = std::prev(record_it);
        output->owner = std::shared_ptr<const void>(
            chunk, static_cast<const void*>(chunk.get()));
        output->record = &*selected;
        return InstrumentHistoryQueryErrorV1::kNone;
    }

    [[nodiscard]] InstrumentHistoryQueryErrorV1 Tail(
        std::uint32_t instrument_id,
        std::uint8_t source_slot,
        InstrumentHistoryLaneV1 lane,
        std::uint64_t visible_source_sequence,
        std::size_t count,
        std::vector<StoredHandle>* output) const noexcept {
        if (output == nullptr) {
            return InstrumentHistoryQueryErrorV1::kNullOutput;
        }
        output->clear();
        if (instrument_id == 0U ||
            instrument_id % kInstrumentHistoryLogicalShardCountV1 !=
                logical_shard_ ||
            source_slot >= kInstrumentHistorySourceCountV1 ||
            !ValidLane(lane) || visible_source_sequence == 0U ||
            count == 0U) {
            return InstrumentHistoryQueryErrorV1::kInvalidArgument;
        }
        try {
            // Reserve before taking the shard lock.  Every subsequent push
            // under the lock is non-allocating and bounded by the query cap.
            output->reserve(count);
            std::vector<ImmutableChunkSlice> immutable_slices;
            immutable_slices.reserve(count);
            {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                const LaneState* state = FindLaneLocked(
                    instrument_id, source_slot, lane);
                if (state == nullptr || state->chunks.empty()) {
                    return InstrumentHistoryQueryErrorV1::kNotFound;
                }

                auto chunk_it = std::upper_bound(
                    state->chunks.begin(), state->chunks.end(),
                    visible_source_sequence,
                    [](std::uint64_t sequence,
                       const std::shared_ptr<Chunk>& chunk) noexcept {
                        return sequence < chunk->first_source_sequence;
                    });
                if (chunk_it == state->chunks.begin()) {
                    return InstrumentHistoryQueryErrorV1::kNotFound;
                }
                std::size_t chunk_index = static_cast<std::size_t>(
                    std::distance(state->chunks.begin(), chunk_it) - 1);
                bool have_chunk = true;

                // Only the final non-full chunk can still be appended.  Read
                // its bounded suffix while the lock is held.  reserve() at
                // chunk creation guarantees these existing record addresses
                // remain stable when later records are constructed.
                const std::shared_ptr<Chunk>& newest =
                    state->chunks[chunk_index];
                if (chunk_index + 1U == state->chunks.size() &&
                    newest->records.size() < chunk_capacity_) {
                    const auto eligible_end = std::upper_bound(
                        newest->records.begin(), newest->records.end(),
                        visible_source_sequence,
                        [](std::uint64_t sequence,
                           const OwnedInstrumentEventEnvelopeV1& record)
                            noexcept {
                            return sequence < record.source_sequence();
                        });
                    for (auto record_it =
                             std::make_reverse_iterator(eligible_end);
                         record_it != newest->records.rend() &&
                         output->size() < count;
                         ++record_it) {
                        output->push_back(StoredHandle{
                            std::shared_ptr<const void>(
                                newest,
                                static_cast<const void*>(newest.get())),
                            &*record_it});
                    }
                    if (chunk_index == 0U) {
                        have_chunk = false;
                    } else {
                        --chunk_index;
                    }
                }

                // Every remaining candidate is full and therefore immutable:
                // append never changes its vector, bounds, or record objects.
                // Snapshot only the exact suffix needed for this Tail; record
                // traversal and per-record shared_ptr construction happen
                // after the shard lock is released.
                std::size_t planned_records = output->size();
                while (have_chunk && planned_records < count) {
                    const std::shared_ptr<Chunk>& chunk =
                        state->chunks[chunk_index];
                    const auto eligible_end_it = std::upper_bound(
                        chunk->records.begin(), chunk->records.end(),
                        visible_source_sequence,
                        [](std::uint64_t sequence,
                           const OwnedInstrumentEventEnvelopeV1& record)
                            noexcept {
                            return sequence < record.source_sequence();
                        });
                    const std::size_t eligible_end =
                        static_cast<std::size_t>(std::distance(
                            chunk->records.begin(), eligible_end_it));
                    const std::size_t remaining = count - planned_records;
                    const std::size_t take =
                        std::min(remaining, eligible_end);
                    if (take != 0U) {
                        immutable_slices.push_back(ImmutableChunkSlice{
                            std::shared_ptr<const Chunk>(chunk),
                            eligible_end - take,
                            eligible_end});
                        planned_records += take;
                        if (planned_records == count) {
                            break;
                        }
                    }
                    if (chunk_index == 0U) {
                        break;
                    }
                    --chunk_index;
                }
            }

            if (after_query_snapshot_hook_ != nullptr) {
                after_query_snapshot_hook_(
                    after_query_snapshot_hook_context_);
            }
            for (const ImmutableChunkSlice& slice : immutable_slices) {
                for (std::size_t index = slice.end;
                     index > slice.begin && output->size() < count;
                     --index) {
                    output->push_back(StoredHandle{
                        std::shared_ptr<const void>(
                            slice.chunk,
                            static_cast<const void*>(slice.chunk.get())),
                        &slice.chunk->records[index - 1U]});
                }
            }
            if (output->empty()) {
                return InstrumentHistoryQueryErrorV1::kNotFound;
            }
            std::reverse(output->begin(), output->end());
            return InstrumentHistoryQueryErrorV1::kNone;
        } catch (...) {
            output->clear();
            return InstrumentHistoryQueryErrorV1::kResourceExhausted;
        }
    }

    [[nodiscard]] InstrumentHistoryRangeResultV1 Range(
        std::uint32_t instrument_id,
        std::uint8_t source_slot,
        InstrumentHistoryLaneV1 lane,
        std::uint64_t begin_inclusive,
        std::uint64_t end_exclusive,
        std::uint64_t visible_source_sequence,
        std::size_t maximum_records,
        std::vector<StoredHandle>* output) const noexcept {
        InstrumentHistoryRangeResultV1 result{};
        if (output == nullptr) {
            result.error = InstrumentHistoryQueryErrorV1::kNullOutput;
            return result;
        }
        output->clear();
        if (instrument_id == 0U ||
            instrument_id % kInstrumentHistoryLogicalShardCountV1 !=
                logical_shard_ ||
            source_slot >= kInstrumentHistorySourceCountV1 ||
            !ValidLane(lane) || begin_inclusive >= end_exclusive ||
            maximum_records == 0U || visible_source_sequence == 0U) {
            result.error = InstrumentHistoryQueryErrorV1::kInvalidArgument;
            return result;
        }
        const std::uint64_t visible_end =
            visible_source_sequence ==
                    std::numeric_limits<std::uint64_t>::max()
                ? end_exclusive
                : std::min(
                      end_exclusive, visible_source_sequence + 1U);
        if (begin_inclusive >= visible_end) {
            result.error = InstrumentHistoryQueryErrorV1::kNotFound;
            return result;
        }

        try {
            output->reserve(maximum_records);
            std::vector<ImmutableChunkSlice> immutable_slices;
            immutable_slices.reserve(maximum_records + 1U);
            std::vector<StoredHandle> mutable_handles;
            mutable_handles.reserve(
                std::min(maximum_records, chunk_capacity_));
            bool mutable_has_more = false;
            {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                const LaneState* state = FindLaneLocked(
                    instrument_id, source_slot, lane);
                if (state == nullptr || state->chunks.empty()) {
                    result.error = InstrumentHistoryQueryErrorV1::kNotFound;
                    return result;
                }

                auto chunk_it = std::lower_bound(
                    state->chunks.begin(), state->chunks.end(),
                    begin_inclusive,
                    [](const std::shared_ptr<Chunk>& chunk,
                       std::uint64_t sequence) noexcept {
                        return chunk->last_source_sequence < sequence;
                    });
                // Snapshot at most maximum_records + 1 exact matches.  The
                // extra match proves truncation without scanning the rest of
                // an all-day range while holding the shard lock.
                const std::size_t evidence_limit = maximum_records + 1U;
                std::size_t evidence_records = 0U;
                for (; chunk_it != state->chunks.end(); ++chunk_it) {
                    const std::shared_ptr<Chunk>& chunk = *chunk_it;
                    if (chunk->first_source_sequence >= visible_end) {
                        break;
                    }

                    const bool mutable_chunk =
                        std::next(chunk_it) == state->chunks.end() &&
                        chunk->records.size() < chunk_capacity_;
                    const auto record_begin = std::lower_bound(
                        chunk->records.begin(), chunk->records.end(),
                        begin_inclusive,
                        [](const OwnedInstrumentEventEnvelopeV1& record,
                           std::uint64_t sequence) noexcept {
                            return record.source_sequence() < sequence;
                        });
                    const auto record_end = std::lower_bound(
                        record_begin, chunk->records.end(), visible_end,
                        [](const OwnedInstrumentEventEnvelopeV1& record,
                           std::uint64_t sequence) noexcept {
                            return record.source_sequence() < sequence;
                        });
                    if (mutable_chunk) {
                        for (auto record_it = record_begin;
                             record_it != record_end; ++record_it) {
                            if (evidence_records == evidence_limit) {
                                mutable_has_more = true;
                                break;
                            }
                            if (evidence_records < maximum_records) {
                                mutable_handles.push_back(StoredHandle{
                                    std::shared_ptr<const void>(
                                        chunk,
                                        static_cast<const void*>(chunk.get())),
                                    &*record_it});
                            } else {
                                mutable_has_more = true;
                            }
                            ++evidence_records;
                        }
                        break;
                    }

                    const std::size_t begin_index =
                        static_cast<std::size_t>(std::distance(
                            chunk->records.begin(), record_begin));
                    const std::size_t match_count =
                        static_cast<std::size_t>(std::distance(
                            record_begin, record_end));
                    if (match_count == 0U) {
                        continue;
                    }
                    const std::size_t remaining_evidence =
                        evidence_limit - evidence_records;
                    const std::size_t take =
                        std::min(match_count, remaining_evidence);
                    immutable_slices.push_back(ImmutableChunkSlice{
                        std::shared_ptr<const Chunk>(chunk),
                        begin_index,
                        begin_index + take});
                    evidence_records += take;
                    if (evidence_records == evidence_limit) {
                        break;
                    }
                }
            }

            if (after_query_snapshot_hook_ != nullptr) {
                after_query_snapshot_hook_(
                    after_query_snapshot_hook_context_);
            }
            for (const ImmutableChunkSlice& slice : immutable_slices) {
                for (std::size_t index = slice.begin;
                     index < slice.end; ++index) {
                    if (output->size() == maximum_records) {
                        result.matched_records = static_cast<std::uint64_t>(
                            output->size());
                        result.truncated = true;
                        return result;
                    }
                    output->push_back(StoredHandle{
                        std::shared_ptr<const void>(
                            slice.chunk,
                            static_cast<const void*>(slice.chunk.get())),
                        &slice.chunk->records[index]});
                }
            }
            for (StoredHandle& handle : mutable_handles) {
                if (output->size() == maximum_records) {
                    result.matched_records = static_cast<std::uint64_t>(
                        output->size());
                    result.truncated = true;
                    return result;
                }
                output->push_back(std::move(handle));
            }
            if (mutable_has_more) {
                result.matched_records =
                    static_cast<std::uint64_t>(output->size());
                result.truncated = true;
                return result;
            }
            if (output->empty()) {
                result.error = InstrumentHistoryQueryErrorV1::kNotFound;
                return result;
            }
            result.matched_records =
                static_cast<std::uint64_t>(output->size());
            return result;
        } catch (...) {
            output->clear();
            result.error =
                InstrumentHistoryQueryErrorV1::kResourceExhausted;
            result.matched_records = 0U;
            result.truncated = false;
            return result;
        }
    }

private:
    struct Chunk final {
        explicit Chunk(std::size_t capacity) {
            records.reserve(capacity);
        }

        void Append(OwnedInstrumentEventEnvelopeV1&& envelope) noexcept {
            const std::uint64_t sequence = envelope.source_sequence();
            records.emplace_back(std::move(envelope));
            if (records.size() == 1U) {
                first_source_sequence = sequence;
            }
            last_source_sequence = sequence;
        }

        std::vector<OwnedInstrumentEventEnvelopeV1> records;
        std::uint64_t first_source_sequence = 0U;
        std::uint64_t last_source_sequence = 0U;
    };

    struct ImmutableChunkSlice final {
        std::shared_ptr<const Chunk> chunk;
        std::size_t begin = 0U;
        std::size_t end = 0U;
    };

    struct LaneState final {
        std::vector<std::shared_ptr<Chunk>> chunks;
        std::uint64_t record_count = 0U;
    };

    struct InstrumentState final {
        std::array<std::array<std::unique_ptr<LaneState>, kLaneCount>,
                   kInstrumentHistorySourceCountV1>
            lanes;
    };

    [[nodiscard]] const LaneState* FindLaneLocked(
        std::uint32_t instrument_id,
        std::uint8_t source_slot,
        InstrumentHistoryLaneV1 lane) const noexcept {
        const auto found = instruments_.find(instrument_id);
        if (found == instruments_.end()) {
            return nullptr;
        }
        return found->second->lanes[source_slot][LaneIndex(lane)].get();
    }

    const std::uint8_t logical_shard_;
    const std::size_t chunk_capacity_;
    const std::uint64_t maximum_records_;
    const std::uint32_t maximum_instruments_;
    const std::uint64_t maximum_owned_payload_bytes_;
    const InstrumentHistoryRuntimeConfigV1::AfterQuerySnapshotHook
        after_query_snapshot_hook_;
    void* const after_query_snapshot_hook_context_;
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::uint32_t, std::unique_ptr<InstrumentState>>
        instruments_;
    std::uint64_t record_count_ = 0U;
    std::uint64_t owned_payload_bytes_ = 0U;
};

class BoundedSpscQueue final {
public:
    explicit BoundedSpscQueue(std::size_t usable_capacity)
        : slots_(usable_capacity + 1U) {}

    [[nodiscard]] bool CanPush() const noexcept {
        const std::size_t write = write_.load(std::memory_order_relaxed);
        const std::size_t next = Next(write);
        return next != read_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool TryPush(
        OwnedInstrumentEventEnvelopeV1&& envelope) noexcept {
        const std::size_t write = write_.load(std::memory_order_relaxed);
        const std::size_t next = Next(write);
        if (next == read_.load(std::memory_order_acquire)) {
            return false;
        }
        slots_[write].emplace(std::move(envelope));
        write_.store(next, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool TryPop(
        std::optional<OwnedInstrumentEventEnvelopeV1>* output) noexcept {
        const std::size_t read = read_.load(std::memory_order_relaxed);
        if (read == write_.load(std::memory_order_acquire)) {
            return false;
        }
        output->emplace(std::move(*slots_[read]));
        slots_[read].reset();
        read_.store(Next(read), std::memory_order_release);
        return true;
    }

private:
    [[nodiscard]] std::size_t Next(std::size_t index) const noexcept {
        ++index;
        return index == slots_.size() ? 0U : index;
    }

    std::vector<std::optional<OwnedInstrumentEventEnvelopeV1>> slots_;
    alignas(64) std::atomic<std::size_t> write_{0U};
    alignas(64) std::atomic<std::size_t> read_{0U};
};

enum class PrepareTicketError : std::uint8_t {
    kNone = 0U,
    kFatal,
    kSequence,
    kInflight,
};

class SourceCompletionTracker final {
public:
    explicit SourceCompletionTracker(std::size_t maximum_inflight)
        : slots_(maximum_inflight) {}

    [[nodiscard]] PrepareTicketError Prepare(
        std::uint64_t source_sequence,
        std::uint64_t* ticket) noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        if (fatal_) {
            return PrepareTicketError::kFatal;
        }
        if (source_sequence == 0U ||
            (submitted_ticket_ != 0U &&
             source_sequence <= submitted_source_sequence_)) {
            return PrepareTicketError::kSequence;
        }
        if (submitted_ticket_ ==
            std::numeric_limits<std::uint64_t>::max()) {
            fatal_ = true;
            condition_.notify_all();
            return PrepareTicketError::kFatal;
        }
        if (submitted_ticket_ - acknowledged_ticket_ >= slots_.size()) {
            return PrepareTicketError::kInflight;
        }
        const std::uint64_t candidate = submitted_ticket_ + 1U;
        CompletionSlot& slot = slots_[SlotIndex(candidate)];
        if (slot.ticket != 0U) {
            fatal_ = true;
            condition_.notify_all();
            return PrepareTicketError::kFatal;
        }
        slot.ticket = candidate;
        slot.source_sequence = source_sequence;
        slot.previous_submitted_source_sequence =
            submitted_source_sequence_;
        slot.completed = false;
        submitted_ticket_ = candidate;
        submitted_source_sequence_ = source_sequence;
        *ticket = candidate;
        return PrepareTicketError::kNone;
    }

    void CancelLast(std::uint64_t ticket) noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        if (ticket == 0U || ticket != submitted_ticket_) {
            fatal_ = true;
            condition_.notify_all();
            return;
        }
        CompletionSlot& slot = slots_[SlotIndex(ticket)];
        if (slot.ticket != ticket || slot.completed) {
            fatal_ = true;
            condition_.notify_all();
            return;
        }
        submitted_source_sequence_ =
            slot.previous_submitted_source_sequence;
        submitted_ticket_ = ticket - 1U;
        slot = CompletionSlot{};
    }

    [[nodiscard]] bool Acknowledge(
        std::uint64_t ticket,
        std::uint64_t source_sequence) noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        if (fatal_ || ticket == 0U || ticket > submitted_ticket_) {
            fatal_ = true;
            condition_.notify_all();
            return false;
        }
        CompletionSlot& slot = slots_[SlotIndex(ticket)];
        if (slot.ticket != ticket ||
            slot.source_sequence != source_sequence || slot.completed) {
            fatal_ = true;
            condition_.notify_all();
            return false;
        }
        slot.completed = true;
        ++completed_unacknowledged_;
        while (acknowledged_ticket_ < submitted_ticket_) {
            const std::uint64_t next = acknowledged_ticket_ + 1U;
            CompletionSlot& next_slot = slots_[SlotIndex(next)];
            if (next_slot.ticket != next || !next_slot.completed) {
                break;
            }
            acknowledged_ticket_ = next;
            acknowledged_source_sequence_ = next_slot.source_sequence;
            --completed_unacknowledged_;
            next_slot = CompletionSlot{};
        }
        condition_.notify_all();
        return true;
    }

    void MarkFatal() noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        fatal_ = true;
        condition_.notify_all();
    }

    [[nodiscard]] InstrumentHistorySourceFrontierV1 Snapshot()
        const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return InstrumentHistorySourceFrontierV1{
            submitted_ticket_, acknowledged_ticket_,
            submitted_source_sequence_, acknowledged_source_sequence_,
            completed_unacknowledged_, fatal_};
    }

    [[nodiscard]] InstrumentHistoryBarrierV1 Capture(
        std::uint8_t source_slot) const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return InstrumentHistoryBarrierV1{
            source_slot, submitted_ticket_, submitted_source_sequence_, true};
    }

    [[nodiscard]] InstrumentHistoryBarrierWaitErrorV1 Wait(
        const InstrumentHistoryBarrierV1& barrier,
        std::chrono::nanoseconds timeout) const noexcept {
        std::unique_lock<std::mutex> lock(mutex_);
        const auto reached = [this, &barrier]() noexcept {
            return fatal_ || acknowledged_ticket_ >= barrier.ticket;
        };
        if (!reached()) {
            if (timeout.count() < 0 ||
                !condition_.wait_for(lock, timeout, reached)) {
                return InstrumentHistoryBarrierWaitErrorV1::kTimeout;
            }
        }
        if (fatal_) {
            return InstrumentHistoryBarrierWaitErrorV1::kSourceFatal;
        }
        return InstrumentHistoryBarrierWaitErrorV1::kNone;
    }

private:
    struct CompletionSlot final {
        std::uint64_t ticket = 0U;
        std::uint64_t source_sequence = 0U;
        std::uint64_t previous_submitted_source_sequence = 0U;
        bool completed = false;
    };

    [[nodiscard]] std::size_t SlotIndex(
        std::uint64_t ticket) const noexcept {
        return static_cast<std::size_t>(
            (ticket - 1U) % static_cast<std::uint64_t>(slots_.size()));
    }

    mutable std::mutex mutex_;
    mutable std::condition_variable condition_;
    std::vector<CompletionSlot> slots_;
    std::uint64_t submitted_ticket_ = 0U;
    std::uint64_t acknowledged_ticket_ = 0U;
    std::uint64_t submitted_source_sequence_ = 0U;
    std::uint64_t acknowledged_source_sequence_ = 0U;
    std::uint64_t completed_unacknowledged_ = 0U;
    bool fatal_ = false;
};

[[nodiscard]] bool ValidConfig(
    const InstrumentHistoryRuntimeConfigV1& config,
    InstrumentHistoryCreateErrorV1* error) noexcept {
    for (std::size_t index = 0U;
         index < config.source_stream_ids.size(); ++index) {
        if (config.source_stream_ids[index] == 0U) {
            *error = InstrumentHistoryCreateErrorV1::kInvalidSourceIds;
            return false;
        }
        for (std::size_t prior = 0U; prior < index; ++prior) {
            if (config.source_stream_ids[prior] ==
                config.source_stream_ids[index]) {
                *error = InstrumentHistoryCreateErrorV1::kInvalidSourceIds;
                return false;
            }
        }
    }
    if (config.physical_worker_count == 0U ||
        config.physical_worker_count >
            kInstrumentHistoryLogicalShardCountV1) {
        *error = InstrumentHistoryCreateErrorV1::kInvalidWorkerCount;
        return false;
    }
    if (config.use_explicit_worker_mapping) {
        std::array<bool, kInstrumentHistoryLogicalShardCountV1> seen{};
        for (std::uint8_t worker :
             config.physical_worker_by_logical_shard) {
            if (worker >= config.physical_worker_count) {
                *error =
                    InstrumentHistoryCreateErrorV1::kInvalidWorkerMapping;
                return false;
            }
            seen[worker] = true;
        }
        for (std::uint32_t worker = 0U;
             worker < config.physical_worker_count; ++worker) {
            if (!seen[worker]) {
                *error =
                    InstrumentHistoryCreateErrorV1::kInvalidWorkerMapping;
                return false;
            }
        }
    }
    if (config.queue_capacity == 0U ||
        config.queue_capacity ==
            std::numeric_limits<std::size_t>::max()) {
        *error = InstrumentHistoryCreateErrorV1::kInvalidQueueCapacity;
        return false;
    }
    if (config.maximum_inflight_per_source == 0U) {
        *error = InstrumentHistoryCreateErrorV1::kInvalidInflightLimit;
        return false;
    }
    if (config.chunk_record_capacity == 0U) {
        *error = InstrumentHistoryCreateErrorV1::kInvalidChunkCapacity;
        return false;
    }
    if (config.maximum_records_per_query == 0U ||
        config.maximum_records_per_query ==
            std::numeric_limits<std::size_t>::max() ||
        static_cast<std::uintmax_t>(config.maximum_records_per_query) >
            std::numeric_limits<std::uint64_t>::max()) {
        *error = InstrumentHistoryCreateErrorV1::kInvalidQueryLimit;
        return false;
    }
    if (config.maximum_records_per_logical_shard == 0U ||
        config.maximum_instruments_per_logical_shard == 0U ||
        config.maximum_owned_payload_bytes_per_logical_shard == 0U) {
        *error = InstrumentHistoryCreateErrorV1::kInvalidStoreLimits;
        return false;
    }
    return true;
}

}  // namespace

InstrumentHistoryLaneV1 InstrumentHistoryLaneForKindV1(
    MarketEventKindV1 kind) noexcept {
    return kind == MarketEventKindV1::kShanghaiSnapshot ||
                   kind == MarketEventKindV1::kShenzhenSnapshot
        ? InstrumentHistoryLaneV1::kSnapshot
        : InstrumentHistoryLaneV1::kTick;
}

OwnedInstrumentEventEnvelopeV1::OwnedInstrumentEventEnvelopeV1(
    std::uint8_t source_slot,
    std::uint32_t source_stream_id,
    std::uint64_t source_sequence,
    std::uint32_t instrument_id,
    InstrumentHistoryLaneV1 lane,
    MarketEventKindV1 kind,
    std::int64_t recv_monotonic_ns,
    std::size_t owned_payload_bytes,
    RetainedMarketEventV1 event) noexcept
    : source_slot_(source_slot),
      logical_shard_(static_cast<std::uint8_t>(
          instrument_id % kInstrumentHistoryLogicalShardCountV1)),
      lane_(lane),
      kind_(kind),
      source_stream_id_(source_stream_id),
      source_sequence_(source_sequence),
      instrument_id_(instrument_id),
      recv_monotonic_ns_(recv_monotonic_ns),
      owned_payload_bytes_(owned_payload_bytes),
      event_(std::move(event)) {}

OwnedInstrumentEventCreateErrorV1 OwnedInstrumentEventEnvelopeV1::Create(
    std::uint8_t source_slot,
    RetainedMarketEventV1 event,
    std::optional<OwnedInstrumentEventEnvelopeV1>* output) noexcept {
    if (output == nullptr) {
        return OwnedInstrumentEventCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (source_slot >= kInstrumentHistorySourceCountV1) {
        return OwnedInstrumentEventCreateErrorV1::kInvalidSourceSlot;
    }
    const RetainedDescription description = DescribeRetained(event);
    if (description.common == nullptr) {
        return OwnedInstrumentEventCreateErrorV1::kNullEvent;
    }
    const DecodedMarketCommonV1& common = *description.common;
    if (common.kind != description.expected_kind) {
        return OwnedInstrumentEventCreateErrorV1::kInvalidKind;
    }
    if (!common.origin.body.empty()) {
        return OwnedInstrumentEventCreateErrorV1::kBorrowedBody;
    }
    if (common.origin.source_stream_id == 0U) {
        return OwnedInstrumentEventCreateErrorV1::kInvalidSource;
    }
    if (common.origin.source_sequence == 0U) {
        return OwnedInstrumentEventCreateErrorV1::kInvalidSourceSequence;
    }
    if (common.origin.recv_monotonic_ns < 0) {
        return OwnedInstrumentEventCreateErrorV1::kInvalidReceiveTime;
    }
    if (common.instrument_id == 0U) {
        return OwnedInstrumentEventCreateErrorV1::kUnknownInstrument;
    }
    const std::size_t payload_bytes =
        EstimateRetainedMarketEventBytesV1(event);
    if (payload_bytes == std::numeric_limits<std::size_t>::max()) {
        return OwnedInstrumentEventCreateErrorV1::kNullEvent;
    }
    *output = OwnedInstrumentEventEnvelopeV1(
        source_slot,
        common.origin.source_stream_id,
        common.origin.source_sequence,
        common.instrument_id,
        InstrumentHistoryLaneForKindV1(common.kind),
        common.kind,
        common.origin.recv_monotonic_ns,
        payload_bytes,
        std::move(event));
    return OwnedInstrumentEventCreateErrorV1::kNone;
}

std::string_view OwnedInstrumentEventCreateErrorNameV1(
    OwnedInstrumentEventCreateErrorV1 error) noexcept {
    switch (error) {
        case OwnedInstrumentEventCreateErrorV1::kNone:
            return "none";
        case OwnedInstrumentEventCreateErrorV1::kNullOutput:
            return "null_output";
        case OwnedInstrumentEventCreateErrorV1::kInvalidSourceSlot:
            return "invalid_source_slot";
        case OwnedInstrumentEventCreateErrorV1::kNullEvent:
            return "null_event";
        case OwnedInstrumentEventCreateErrorV1::kBorrowedBody:
            return "borrowed_body";
        case OwnedInstrumentEventCreateErrorV1::kInvalidSource:
            return "invalid_source";
        case OwnedInstrumentEventCreateErrorV1::kInvalidSourceSequence:
            return "invalid_source_sequence";
        case OwnedInstrumentEventCreateErrorV1::kInvalidReceiveTime:
            return "invalid_receive_time";
        case OwnedInstrumentEventCreateErrorV1::kUnknownInstrument:
            return "unknown_instrument";
        case OwnedInstrumentEventCreateErrorV1::kInvalidKind:
            return "invalid_kind";
    }
    return "invalid_owned_instrument_event_create_error";
}

class InstrumentHistoryRuntimeV1::Impl final {
public:
    struct WorkerControl final {
        mutable std::mutex mutex;
        std::condition_variable condition;
        std::atomic<std::size_t> pending{0U};
        std::atomic<bool> stop_requested{false};
        std::thread thread;
    };

    explicit Impl(InstrumentHistoryRuntimeConfigV1 value)
        : config(std::move(value)) {
        if (!config.use_explicit_worker_mapping) {
            for (std::size_t shard = 0U;
                 shard < kInstrumentHistoryLogicalShardCountV1; ++shard) {
                config.physical_worker_by_logical_shard[shard] =
                    static_cast<std::uint8_t>(
                        shard % config.physical_worker_count);
            }
        }
        for (std::size_t index = 0U; index < queues.size(); ++index) {
            queues[index] =
                std::make_unique<BoundedSpscQueue>(config.queue_capacity);
        }
        for (std::size_t shard = 0U;
             shard < kInstrumentHistoryLogicalShardCountV1; ++shard) {
            stores[shard] = std::make_unique<InstrumentShardStore>(
                static_cast<std::uint8_t>(shard),
                config.chunk_record_capacity,
                config.maximum_records_per_logical_shard,
                config.maximum_instruments_per_logical_shard,
                config.maximum_owned_payload_bytes_per_logical_shard,
                config.after_query_snapshot_hook,
                config.after_query_snapshot_hook_context);
        }
        for (std::size_t source = 0U;
             source < kInstrumentHistorySourceCountV1; ++source) {
            trackers[source] = std::make_unique<SourceCompletionTracker>(
                config.maximum_inflight_per_source);
        }
        workers.reserve(config.physical_worker_count);
        for (std::uint32_t worker = 0U;
             worker < config.physical_worker_count; ++worker) {
            workers.push_back(std::make_unique<WorkerControl>());
        }
    }

    ~Impl() {
        StopAndDrain();
    }

    [[nodiscard]] bool Start() noexcept {
        try {
            for (std::uint32_t worker = 0U;
                 worker < config.physical_worker_count; ++worker) {
                workers[worker]->thread = std::thread(
                    [this, worker]() noexcept { WorkerMain(worker); });
            }
            return true;
        } catch (...) {
            accepting.store(false, std::memory_order_release);
            for (const auto& worker : workers) {
                {
                    std::lock_guard<std::mutex> lock(worker->mutex);
                    worker->stop_requested.store(
                        true, std::memory_order_release);
                }
                worker->condition.notify_all();
            }
            for (const auto& worker : workers) {
                if (worker->thread.joinable()) {
                    worker->thread.join();
                }
            }
            return false;
        }
    }

    [[nodiscard]] std::size_t QueueIndex(
        std::size_t source,
        std::size_t shard) const noexcept {
        return source * kInstrumentHistoryLogicalShardCountV1 + shard;
    }

    void WorkerMain(std::uint32_t worker_id) noexcept {
        WorkerControl& worker = *workers[worker_id];
        for (;;) {
            bool progressed = false;
            for (std::size_t shard = 0U;
                 shard < kInstrumentHistoryLogicalShardCountV1; ++shard) {
                if (config.physical_worker_by_logical_shard[shard] !=
                    worker_id) {
                    continue;
                }
                for (std::size_t source = 0U;
                     source < kInstrumentHistorySourceCountV1; ++source) {
                    std::optional<OwnedInstrumentEventEnvelopeV1> envelope;
                    if (!queues[QueueIndex(source, shard)]->TryPop(
                            &envelope)) {
                        continue;
                    }
                    worker.pending.fetch_sub(1U, std::memory_order_acq_rel);
                    progressed = true;
                    if (trackers[source]->Snapshot().fatal) {
                        continue;
                    }
                    const std::uint64_t ticket =
                        envelope->dispatch_ticket();
                    const std::uint64_t sequence =
                        envelope->source_sequence();
                    if (config.before_append_hook != nullptr) {
                        config.before_append_hook(
                            config.before_append_hook_context,
                            worker_id,
                            *envelope);
                    }
                    const StoreAppendError append_error =
                        stores[shard]->Append(std::move(*envelope));
                    if (append_error != StoreAppendError::kNone ||
                        !trackers[source]->Acknowledge(ticket, sequence)) {
                        any_source_fatal.store(
                            true, std::memory_order_release);
                        trackers[source]->MarkFatal();
                    }
                }
            }
            if (worker.stop_requested.load(std::memory_order_acquire) &&
                worker.pending.load(std::memory_order_acquire) == 0U) {
                return;
            }
            if (!progressed) {
                std::unique_lock<std::mutex> lock(worker.mutex);
                worker.condition.wait(lock, [&worker]() noexcept {
                    return worker.stop_requested.load(
                               std::memory_order_acquire) ||
                           worker.pending.load(
                               std::memory_order_acquire) != 0U;
                });
            }
        }
    }

    void StopAndDrain() noexcept {
        std::lock_guard<std::mutex> stop_lock(stop_mutex);
        std::scoped_lock admission_lock(
            admission_mutexes[0], admission_mutexes[1],
            admission_mutexes[2], admission_mutexes[3]);
        const bool was_accepting =
            accepting.exchange(false, std::memory_order_acq_rel);
        if (!was_accepting && stopped) {
            return;
        }
        for (const auto& worker : workers) {
            {
                std::lock_guard<std::mutex> lock(worker->mutex);
                worker->stop_requested.store(
                    true, std::memory_order_release);
            }
            worker->condition.notify_all();
        }
        // Admission locks remain held until accepting is false for every
        // source.  Join does not require them, but releasing a scoped_lock
        // early is impossible; worker paths never acquire these locks.
        for (const auto& worker : workers) {
            if (worker->thread.joinable()) {
                worker->thread.join();
            }
        }
        stopped = true;
    }

    InstrumentHistoryRuntimeConfigV1 config;
    std::array<std::unique_ptr<BoundedSpscQueue>,
               kInstrumentHistorySourceCountV1 *
                   kInstrumentHistoryLogicalShardCountV1>
        queues;
    std::array<std::unique_ptr<InstrumentShardStore>,
               kInstrumentHistoryLogicalShardCountV1>
        stores;
    std::array<std::unique_ptr<SourceCompletionTracker>,
               kInstrumentHistorySourceCountV1>
        trackers;
    std::vector<std::unique_ptr<WorkerControl>> workers;
    std::array<std::mutex, kInstrumentHistorySourceCountV1>
        admission_mutexes;
    std::mutex stop_mutex;
    std::array<std::atomic<bool>, kInstrumentHistorySourceCountV1>
        source_admission_open{true, true, true, true};
    std::atomic<bool> accepting{true};
    std::atomic<bool> any_source_fatal{false};
    bool stopped = false;
};

InstrumentHistoryRuntimeV1::InstrumentHistoryRuntimeV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

InstrumentHistoryRuntimeV1::~InstrumentHistoryRuntimeV1() = default;

InstrumentHistoryCreateErrorV1 InstrumentHistoryRuntimeV1::Create(
    InstrumentHistoryRuntimeConfigV1 config,
    std::unique_ptr<InstrumentHistoryRuntimeV1>* output) noexcept {
    if (output == nullptr) {
        return InstrumentHistoryCreateErrorV1::kNullOutput;
    }
    output->reset();
    InstrumentHistoryCreateErrorV1 validation =
        InstrumentHistoryCreateErrorV1::kNone;
    if (!ValidConfig(config, &validation)) {
        return validation;
    }
    try {
        auto impl = std::make_unique<Impl>(std::move(config));
        if (!impl->Start()) {
            return InstrumentHistoryCreateErrorV1::kThreadStartFailed;
        }
        output->reset(new InstrumentHistoryRuntimeV1(std::move(impl)));
        return InstrumentHistoryCreateErrorV1::kNone;
    } catch (...) {
        return InstrumentHistoryCreateErrorV1::kResourceExhausted;
    }
}

InstrumentHistorySubmitErrorV1 InstrumentHistoryRuntimeV1::TrySubmit(
    OwnedInstrumentEventEnvelopeV1&& envelope) noexcept {
    const std::uint8_t source = envelope.source_slot();
    if (source >= kInstrumentHistorySourceCountV1) {
        return InstrumentHistorySubmitErrorV1::kSourceMismatch;
    }
    std::lock_guard<std::mutex> admission_lock(
        impl_->admission_mutexes[source]);
    if (!impl_->accepting.load(std::memory_order_acquire)) {
        return InstrumentHistorySubmitErrorV1::kStopped;
    }
    if (!impl_->source_admission_open[source].load(
            std::memory_order_acquire)) {
        return InstrumentHistorySubmitErrorV1::kSourceFatal;
    }
    if (envelope.source_stream_id() !=
        impl_->config.source_stream_ids[source]) {
        return InstrumentHistorySubmitErrorV1::kSourceMismatch;
    }
    if (envelope.dispatch_ticket() != 0U) {
        return InstrumentHistorySubmitErrorV1::kEnvelopeAlreadySubmitted;
    }
    const std::size_t shard = envelope.logical_shard();
    BoundedSpscQueue& queue =
        *impl_->queues[impl_->QueueIndex(source, shard)];
    // With exactly one producer for this source/queue, a true CanPush cannot
    // become false before this producer's TryPush; the consumer only frees
    // capacity.  Queue-full therefore returns before any envelope mutation.
    if (!queue.CanPush()) {
        return InstrumentHistorySubmitErrorV1::kQueueFull;
    }
    const InstrumentHistorySourceFrontierV1 before =
        impl_->trackers[source]->Snapshot();
    if (before.fatal) {
        return InstrumentHistorySubmitErrorV1::kSourceFatal;
    }
    std::uint64_t ticket = 0U;
    const PrepareTicketError prepare =
        impl_->trackers[source]->Prepare(
            envelope.source_sequence(), &ticket);
    switch (prepare) {
        case PrepareTicketError::kFatal:
            return InstrumentHistorySubmitErrorV1::kSourceFatal;
        case PrepareTicketError::kSequence:
            return InstrumentHistorySubmitErrorV1::
                kSourceSequenceNotIncreasing;
        case PrepareTicketError::kInflight:
            return InstrumentHistorySubmitErrorV1::kInflightLimit;
        case PrepareTicketError::kNone:
            break;
    }

    const std::uint8_t worker_id =
        impl_->config.physical_worker_by_logical_shard[shard];
    auto& worker = *impl_->workers[worker_id];
    worker.pending.fetch_add(1U, std::memory_order_acq_rel);
    envelope.SetDispatchTicket(ticket);
    if (!queue.TryPush(std::move(envelope))) {
        // Defensive only: the SPSC ownership rule makes this impossible after
        // CanPush.  Restore every observable caller/runtime mutation anyway.
        envelope.SetDispatchTicket(0U);
        worker.pending.fetch_sub(1U, std::memory_order_acq_rel);
        impl_->trackers[source]->CancelLast(ticket);
        return InstrumentHistorySubmitErrorV1::kQueueFull;
    }
    // pending is the condition predicate.  This empty critical section is
    // the wake-up handshake: if the worker just observed zero, it must enter
    // wait (and release this mutex) before this producer can notify.
    {
        std::lock_guard<std::mutex> wake_lock(worker.mutex);
    }
    worker.condition.notify_one();
    return InstrumentHistorySubmitErrorV1::kNone;
}

InstrumentHistorySourceFrontierV1 InstrumentHistoryRuntimeV1::Frontier(
    std::uint8_t source_slot) const noexcept {
    if (source_slot >= kInstrumentHistorySourceCountV1) {
        InstrumentHistorySourceFrontierV1 invalid{};
        invalid.fatal = true;
        return invalid;
    }
    return impl_->trackers[source_slot]->Snapshot();
}

InstrumentHistoryBarrierV1 InstrumentHistoryRuntimeV1::CaptureBarrier(
    std::uint8_t source_slot) const noexcept {
    if (source_slot >= kInstrumentHistorySourceCountV1) {
        return {};
    }
    std::lock_guard<std::mutex> admission_lock(
        impl_->admission_mutexes[source_slot]);
    return impl_->trackers[source_slot]->Capture(source_slot);
}

InstrumentHistoryBarrierWaitErrorV1
InstrumentHistoryRuntimeV1::WaitForBarrier(
    const InstrumentHistoryBarrierV1& barrier,
    std::chrono::nanoseconds timeout) const noexcept {
    if (!barrier.valid ||
        barrier.source_slot >= kInstrumentHistorySourceCountV1) {
        return InstrumentHistoryBarrierWaitErrorV1::kInvalidBarrier;
    }
    return impl_->trackers[barrier.source_slot]->Wait(barrier, timeout);
}

void InstrumentHistoryRuntimeV1::MarkSourceFatal(
    std::uint8_t source_slot) noexcept {
    if (source_slot >= kInstrumentHistorySourceCountV1) {
        return;
    }
    std::lock_guard<std::mutex> admission_lock(
        impl_->admission_mutexes[source_slot]);
    impl_->any_source_fatal.store(true, std::memory_order_release);
    impl_->source_admission_open[source_slot].store(
        false, std::memory_order_release);
    impl_->trackers[source_slot]->MarkFatal();
}

bool InstrumentHistoryRuntimeV1::AnySourceFatal() const noexcept {
    return impl_->any_source_fatal.load(std::memory_order_acquire);
}

InstrumentHistoryQueryErrorV1 InstrumentHistoryRuntimeV1::Latest(
    std::uint32_t instrument_id,
    std::uint8_t source_slot,
    InstrumentHistoryLaneV1 lane,
    InstrumentHistoryRecordHandleV1* output) const noexcept {
    if (output == nullptr) {
        return InstrumentHistoryQueryErrorV1::kNullOutput;
    }
    *output = InstrumentHistoryRecordHandleV1{};
    if (instrument_id == 0U ||
        source_slot >= kInstrumentHistorySourceCountV1 ||
        !ValidLane(lane)) {
        return InstrumentHistoryQueryErrorV1::kInvalidArgument;
    }
    const std::uint8_t shard = static_cast<std::uint8_t>(
        instrument_id % kInstrumentHistoryLogicalShardCountV1);
    const auto frontier = impl_->trackers[source_slot]->Snapshot();
    if (frontier.fatal) {
        return InstrumentHistoryQueryErrorV1::kSourceFatal;
    }
    if (frontier.acknowledged_source_sequence == 0U) {
        return InstrumentHistoryQueryErrorV1::kNotFound;
    }
    InstrumentShardStore::StoredHandle handle;
    const auto error = impl_->stores[shard]->Latest(
        instrument_id, source_slot, lane,
        frontier.acknowledged_source_sequence, &handle);
    if (impl_->trackers[source_slot]->Snapshot().fatal) {
        return InstrumentHistoryQueryErrorV1::kSourceFatal;
    }
    if (error == InstrumentHistoryQueryErrorV1::kNone) {
        *output = InstrumentHistoryRecordHandleV1(
            std::move(handle.owner), handle.record);
    }
    return error;
}

InstrumentHistoryQueryErrorV1
InstrumentHistoryRuntimeV1::LatestProvisional(
    std::uint32_t instrument_id,
    std::uint8_t source_slot,
    InstrumentHistoryLaneV1 lane,
    InstrumentHistoryRecordHandleV1* output) const noexcept {
    if (output == nullptr) {
        return InstrumentHistoryQueryErrorV1::kNullOutput;
    }
    *output = InstrumentHistoryRecordHandleV1{};
    if (instrument_id == 0U ||
        source_slot >= kInstrumentHistorySourceCountV1 ||
        !ValidLane(lane)) {
        return InstrumentHistoryQueryErrorV1::kInvalidArgument;
    }
    const std::uint8_t shard = static_cast<std::uint8_t>(
        instrument_id % kInstrumentHistoryLogicalShardCountV1);
    if (impl_->trackers[source_slot]->Snapshot().fatal) {
        return InstrumentHistoryQueryErrorV1::kSourceFatal;
    }
    InstrumentShardStore::StoredHandle handle;
    const auto error = impl_->stores[shard]->Latest(
        instrument_id, source_slot, lane,
        std::numeric_limits<std::uint64_t>::max(), &handle);
    if (impl_->trackers[source_slot]->Snapshot().fatal) {
        return InstrumentHistoryQueryErrorV1::kSourceFatal;
    }
    if (error == InstrumentHistoryQueryErrorV1::kNone) {
        *output = InstrumentHistoryRecordHandleV1(
            std::move(handle.owner), handle.record);
    }
    return error;
}

InstrumentHistoryQueryErrorV1 InstrumentHistoryRuntimeV1::Tail(
    std::uint32_t instrument_id,
    std::uint8_t source_slot,
    InstrumentHistoryLaneV1 lane,
    std::size_t count,
    std::vector<InstrumentHistoryRecordHandleV1>* output) const noexcept {
    if (output == nullptr) {
        return InstrumentHistoryQueryErrorV1::kNullOutput;
    }
    output->clear();
    if (instrument_id == 0U ||
        source_slot >= kInstrumentHistorySourceCountV1 ||
        !ValidLane(lane) || count == 0U) {
        return InstrumentHistoryQueryErrorV1::kInvalidArgument;
    }
    if (count > impl_->config.maximum_records_per_query) {
        return InstrumentHistoryQueryErrorV1::kQueryLimitExceeded;
    }
    const std::uint8_t shard = static_cast<std::uint8_t>(
        instrument_id % kInstrumentHistoryLogicalShardCountV1);
    const auto frontier = impl_->trackers[source_slot]->Snapshot();
    if (frontier.fatal) {
        return InstrumentHistoryQueryErrorV1::kSourceFatal;
    }
    if (frontier.acknowledged_source_sequence == 0U) {
        return InstrumentHistoryQueryErrorV1::kNotFound;
    }
    std::vector<InstrumentShardStore::StoredHandle> handles;
    const auto error = impl_->stores[shard]->Tail(
        instrument_id, source_slot, lane,
        frontier.acknowledged_source_sequence, count, &handles);
    if (impl_->trackers[source_slot]->Snapshot().fatal) {
        return InstrumentHistoryQueryErrorV1::kSourceFatal;
    }
    if (error != InstrumentHistoryQueryErrorV1::kNone) {
        return error;
    }
    try {
        output->reserve(handles.size());
        for (auto& handle : handles) {
            output->push_back(InstrumentHistoryRecordHandleV1(
                std::move(handle.owner), handle.record));
        }
        return InstrumentHistoryQueryErrorV1::kNone;
    } catch (...) {
        output->clear();
        return InstrumentHistoryQueryErrorV1::kResourceExhausted;
    }
}

InstrumentHistoryQueryErrorV1
InstrumentHistoryRuntimeV1::TailProvisional(
    std::uint32_t instrument_id,
    std::uint8_t source_slot,
    InstrumentHistoryLaneV1 lane,
    std::size_t count,
    std::vector<InstrumentHistoryRecordHandleV1>* output) const noexcept {
    if (output == nullptr) {
        return InstrumentHistoryQueryErrorV1::kNullOutput;
    }
    output->clear();
    if (instrument_id == 0U ||
        source_slot >= kInstrumentHistorySourceCountV1 ||
        !ValidLane(lane) || count == 0U) {
        return InstrumentHistoryQueryErrorV1::kInvalidArgument;
    }
    if (count > impl_->config.maximum_records_per_query) {
        return InstrumentHistoryQueryErrorV1::kQueryLimitExceeded;
    }
    const std::uint8_t shard = static_cast<std::uint8_t>(
        instrument_id % kInstrumentHistoryLogicalShardCountV1);
    if (impl_->trackers[source_slot]->Snapshot().fatal) {
        return InstrumentHistoryQueryErrorV1::kSourceFatal;
    }
    std::vector<InstrumentShardStore::StoredHandle> handles;
    const auto error = impl_->stores[shard]->Tail(
        instrument_id, source_slot, lane,
        std::numeric_limits<std::uint64_t>::max(), count, &handles);
    if (impl_->trackers[source_slot]->Snapshot().fatal) {
        return InstrumentHistoryQueryErrorV1::kSourceFatal;
    }
    if (error != InstrumentHistoryQueryErrorV1::kNone) {
        return error;
    }
    try {
        output->reserve(handles.size());
        for (auto& handle : handles) {
            output->push_back(InstrumentHistoryRecordHandleV1(
                std::move(handle.owner), handle.record));
        }
        return InstrumentHistoryQueryErrorV1::kNone;
    } catch (...) {
        output->clear();
        return InstrumentHistoryQueryErrorV1::kResourceExhausted;
    }
}

InstrumentHistoryRangeResultV1
InstrumentHistoryRuntimeV1::RangeBySourceSequence(
    std::uint32_t instrument_id,
    std::uint8_t source_slot,
    InstrumentHistoryLaneV1 lane,
    std::uint64_t begin_inclusive,
    std::uint64_t end_exclusive,
    std::size_t maximum_records,
    std::vector<InstrumentHistoryRecordHandleV1>* output) const noexcept {
    InstrumentHistoryRangeResultV1 result{};
    if (output == nullptr) {
        result.error = InstrumentHistoryQueryErrorV1::kNullOutput;
        return result;
    }
    output->clear();
    if (instrument_id == 0U ||
        source_slot >= kInstrumentHistorySourceCountV1 ||
        !ValidLane(lane) || begin_inclusive >= end_exclusive ||
        maximum_records == 0U) {
        result.error = InstrumentHistoryQueryErrorV1::kInvalidArgument;
        return result;
    }
    if (maximum_records > impl_->config.maximum_records_per_query) {
        result.error =
            InstrumentHistoryQueryErrorV1::kQueryLimitExceeded;
        return result;
    }
    const std::uint8_t shard = static_cast<std::uint8_t>(
        instrument_id % kInstrumentHistoryLogicalShardCountV1);
    const auto frontier = impl_->trackers[source_slot]->Snapshot();
    if (frontier.fatal) {
        result.error = InstrumentHistoryQueryErrorV1::kSourceFatal;
        return result;
    }
    if (frontier.acknowledged_source_sequence == 0U) {
        result.error = InstrumentHistoryQueryErrorV1::kNotFound;
        return result;
    }
    std::vector<InstrumentShardStore::StoredHandle> handles;
    result = impl_->stores[shard]->Range(
        instrument_id, source_slot, lane, begin_inclusive, end_exclusive,
        frontier.acknowledged_source_sequence, maximum_records, &handles);
    if (impl_->trackers[source_slot]->Snapshot().fatal) {
        result.error = InstrumentHistoryQueryErrorV1::kSourceFatal;
        result.matched_records = 0U;
        result.truncated = false;
        return result;
    }
    if (result.error != InstrumentHistoryQueryErrorV1::kNone) {
        return result;
    }
    try {
        output->reserve(handles.size());
        for (auto& handle : handles) {
            output->push_back(InstrumentHistoryRecordHandleV1(
                std::move(handle.owner), handle.record));
        }
        return result;
    } catch (...) {
        output->clear();
        result.error = InstrumentHistoryQueryErrorV1::kResourceExhausted;
        result.matched_records = 0U;
        result.truncated = false;
        return result;
    }
}

std::uint8_t InstrumentHistoryRuntimeV1::PhysicalWorkerForLogicalShard(
    std::uint8_t logical_shard) const noexcept {
    return logical_shard < kInstrumentHistoryLogicalShardCountV1
        ? impl_->config.physical_worker_by_logical_shard[logical_shard]
        : std::numeric_limits<std::uint8_t>::max();
}

void InstrumentHistoryRuntimeV1::StopAndDrain() noexcept {
    impl_->StopAndDrain();
}

const InstrumentHistoryRuntimeConfigV1&
InstrumentHistoryRuntimeV1::config() const noexcept {
    return impl_->config;
}

std::string_view InstrumentHistoryCreateErrorNameV1(
    InstrumentHistoryCreateErrorV1 error) noexcept {
    switch (error) {
        case InstrumentHistoryCreateErrorV1::kNone:
            return "none";
        case InstrumentHistoryCreateErrorV1::kNullOutput:
            return "null_output";
        case InstrumentHistoryCreateErrorV1::kInvalidSourceIds:
            return "invalid_source_ids";
        case InstrumentHistoryCreateErrorV1::kInvalidWorkerCount:
            return "invalid_worker_count";
        case InstrumentHistoryCreateErrorV1::kInvalidWorkerMapping:
            return "invalid_worker_mapping";
        case InstrumentHistoryCreateErrorV1::kInvalidQueueCapacity:
            return "invalid_queue_capacity";
        case InstrumentHistoryCreateErrorV1::kInvalidInflightLimit:
            return "invalid_inflight_limit";
        case InstrumentHistoryCreateErrorV1::kInvalidChunkCapacity:
            return "invalid_chunk_capacity";
        case InstrumentHistoryCreateErrorV1::kInvalidStoreLimits:
            return "invalid_store_limits";
        case InstrumentHistoryCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case InstrumentHistoryCreateErrorV1::kThreadStartFailed:
            return "thread_start_failed";
        case InstrumentHistoryCreateErrorV1::kInvalidQueryLimit:
            return "invalid_query_limit";
    }
    return "invalid_instrument_history_create_error";
}

std::string_view InstrumentHistorySubmitErrorNameV1(
    InstrumentHistorySubmitErrorV1 error) noexcept {
    switch (error) {
        case InstrumentHistorySubmitErrorV1::kNone:
            return "none";
        case InstrumentHistorySubmitErrorV1::kStopped:
            return "stopped";
        case InstrumentHistorySubmitErrorV1::kSourceFatal:
            return "source_fatal";
        case InstrumentHistorySubmitErrorV1::kSourceMismatch:
            return "source_mismatch";
        case InstrumentHistorySubmitErrorV1::kEnvelopeAlreadySubmitted:
            return "envelope_already_submitted";
        case InstrumentHistorySubmitErrorV1::kSourceSequenceNotIncreasing:
            return "source_sequence_not_increasing";
        case InstrumentHistorySubmitErrorV1::kInflightLimit:
            return "inflight_limit";
        case InstrumentHistorySubmitErrorV1::kQueueFull:
            return "queue_full";
    }
    return "invalid_instrument_history_submit_error";
}

std::string_view InstrumentHistoryBarrierWaitErrorNameV1(
    InstrumentHistoryBarrierWaitErrorV1 error) noexcept {
    switch (error) {
        case InstrumentHistoryBarrierWaitErrorV1::kNone:
            return "none";
        case InstrumentHistoryBarrierWaitErrorV1::kInvalidBarrier:
            return "invalid_barrier";
        case InstrumentHistoryBarrierWaitErrorV1::kTimeout:
            return "timeout";
        case InstrumentHistoryBarrierWaitErrorV1::kSourceFatal:
            return "source_fatal";
    }
    return "invalid_instrument_history_barrier_wait_error";
}

std::string_view InstrumentHistoryQueryErrorNameV1(
    InstrumentHistoryQueryErrorV1 error) noexcept {
    switch (error) {
        case InstrumentHistoryQueryErrorV1::kNone:
            return "none";
        case InstrumentHistoryQueryErrorV1::kNullOutput:
            return "null_output";
        case InstrumentHistoryQueryErrorV1::kInvalidArgument:
            return "invalid_argument";
        case InstrumentHistoryQueryErrorV1::kSourceFatal:
            return "source_fatal";
        case InstrumentHistoryQueryErrorV1::kNotFound:
            return "not_found";
        case InstrumentHistoryQueryErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case InstrumentHistoryQueryErrorV1::kQueryLimitExceeded:
            return "query_limit_exceeded";
    }
    return "invalid_instrument_history_query_error";
}

}  // namespace l2flow::market
