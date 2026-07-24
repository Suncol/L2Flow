#include "l2flow/market/intraday_instrument_store_v1.h"

#include "l2flow/market/instrument_registry.h"
#include "l2flow/market/market_types_v1.h"
#include "l2flow/market/realtime_history_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <limits>
#include <mutex>
#include <new>
#include <optional>
#include <type_traits>
#include <utility>

namespace l2flow::market {
namespace {

using RecordHandle = std::shared_ptr<const RealtimeHistoryRecordV1>;

static_assert(
    kIntradayInstrumentStoreSourceCountV1 ==
    kRealtimeHistorySourceCountV1);

[[nodiscard]] bool ValidDirection(
    IntradayInstrumentScanDirectionV1 direction) noexcept {
    switch (direction) {
        case IntradayInstrumentScanDirectionV1::kOldestFirst:
        case IntradayInstrumentScanDirectionV1::kNewestFirst:
            return true;
    }
    return false;
}

[[nodiscard]] bool SnapshotKind(MarketEventKindV1 kind) noexcept {
    return kind == MarketEventKindV1::kShanghaiSnapshot ||
           kind == MarketEventKindV1::kShenzhenSnapshot;
}

[[nodiscard]] bool KindBelongsToSource(
    MarketEventKindV1 kind,
    std::uint8_t source) noexcept {
    switch (source) {
        case 0U:
            return kind == MarketEventKindV1::kShanghaiSnapshot;
        case 1U:
            return kind == MarketEventKindV1::kShanghaiTick;
        case 2U:
            return kind == MarketEventKindV1::kShenzhenSnapshot;
        case 3U:
            return kind == MarketEventKindV1::kShenzhenOrder ||
                   kind == MarketEventKindV1::kShenzhenTransaction;
        default:
            return false;
    }
}

[[nodiscard]] bool CheckedAdd(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        left > std::numeric_limits<std::uint64_t>::max() - right) {
        return false;
    }
    *output = left + right;
    return true;
}

[[nodiscard]] bool CheckedMultiply(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* output) noexcept {
    if (output == nullptr ||
        (right != 0U &&
         left > std::numeric_limits<std::uint64_t>::max() / right)) {
        return false;
    }
    *output = left * right;
    return true;
}

void SaturatingAtomicIncrement(
    std::atomic<std::uint64_t>* value) noexcept {
    std::uint64_t current = value->load(std::memory_order_relaxed);
    while (current != std::numeric_limits<std::uint64_t>::max() &&
           !value->compare_exchange_weak(
               current,
               current + 1U,
               std::memory_order_relaxed,
               std::memory_order_relaxed)) {
    }
}

void SaturatingAtomicAdd(
    std::atomic<std::uint64_t>* value,
    std::uint64_t amount) noexcept {
    std::uint64_t current = value->load(std::memory_order_relaxed);
    for (;;) {
        const std::uint64_t next =
            current > std::numeric_limits<std::uint64_t>::max() - amount
                ? std::numeric_limits<std::uint64_t>::max()
                : current + amount;
        if (value->compare_exchange_weak(
                current,
                next,
                std::memory_order_relaxed,
                std::memory_order_relaxed)) {
            return;
        }
    }
}

[[nodiscard]] bool TryReserve(
    std::atomic<std::uint64_t>* reserved,
    std::uint64_t amount,
    std::uint64_t limit) noexcept {
    if (reserved == nullptr || amount > limit) {
        return false;
    }
    std::uint64_t current = reserved->load(std::memory_order_relaxed);
    for (;;) {
        if (current > limit - amount) {
            return false;
        }
        if (reserved->compare_exchange_weak(
                current,
                current + amount,
                std::memory_order_acq_rel,
                std::memory_order_relaxed)) {
            return true;
        }
    }
}

struct IntradayChunk final {
    explicit IntradayChunk(std::size_t capacity)
        : records(std::make_unique<RecordHandle[]>(capacity)) {}

    std::unique_ptr<RecordHandle[]> records;
    std::unique_ptr<IntradayChunk> owned_next;
    IntradayChunk* previous = nullptr;
    std::uint64_t first_ingress_sequence = 0U;
    std::uint64_t last_ingress_sequence = 0U;
    std::uint64_t first_source_sequence = 0U;
    std::uint64_t last_source_sequence = 0U;
};

struct MutableLane final {
    MutableLane() = default;
    MutableLane(const MutableLane&) = delete;
    MutableLane& operator=(const MutableLane&) = delete;
    MutableLane(MutableLane&&) noexcept = default;
    MutableLane& operator=(MutableLane&&) noexcept = default;

    ~MutableLane() {
        // Avoid recursive destruction of a potentially very long hot-symbol
        // chunk chain.
        while (owned_head != nullptr) {
            std::unique_ptr<IntradayChunk> next =
                std::move(owned_head->owned_next);
            owned_head->owned_next.reset();
            owned_head = std::move(next);
        }
    }

    std::unique_ptr<IntradayChunk> owned_head;
    IntradayChunk* head = nullptr;
    IntradayChunk* tail = nullptr;
    std::size_t tail_used = 0U;
    std::uint64_t record_count = 0U;
    std::uint64_t accounted_bytes = 0U;
    std::uint64_t last_ingress_sequence = 0U;
    std::uint64_t last_source_sequence = 0U;
};

struct MutableInstrumentRow final {
    MutableInstrumentRow(
        std::uint32_t value_instrument_id,
        std::size_t value_ordinal) noexcept
        : instrument_id(value_instrument_id), ordinal(value_ordinal) {}

    MutableInstrumentRow(const MutableInstrumentRow&) = delete;
    MutableInstrumentRow& operator=(const MutableInstrumentRow&) = delete;
    MutableInstrumentRow(MutableInstrumentRow&&) noexcept = default;
    MutableInstrumentRow& operator=(MutableInstrumentRow&&) noexcept = default;

    std::uint32_t instrument_id = 0U;
    std::size_t ordinal = 0U;
    std::array<MutableLane, kIntradayInstrumentStoreSourceCountV1> lanes;
    const RealtimeHistoryRecordV1* latest_snapshot = nullptr;
    const RealtimeHistoryRecordV1* latest_tick = nullptr;
};

struct WorkerState final {
    std::vector<MutableInstrumentRow> rows;
    std::uint64_t last_captured_generation = 0U;
};

struct OrdinalEntry final {
    std::uint32_t instrument_id = 0U;
    std::uint32_t worker = 0U;
    std::size_t local_index = 0U;
};

struct CapturedLane final {
    const IntradayChunk* head = nullptr;
    const IntradayChunk* tail = nullptr;
    std::size_t tail_used = 0U;
    std::uint64_t record_count = 0U;
    std::uint64_t accounted_bytes = 0U;
};

struct CapturedInstrumentRow final {
    std::uint32_t instrument_id = 0U;
    std::size_t ordinal = 0U;
    std::array<CapturedLane, kIntradayInstrumentStoreSourceCountV1> lanes;
    const RealtimeHistoryRecordV1* latest_snapshot = nullptr;
    const RealtimeHistoryRecordV1* latest_tick = nullptr;
};

struct SessionState final {
    IntradayInstrumentStoreConfigV1 config{};
    std::uint32_t worker_count = 0U;
    const InstrumentRegistryV1* registry = nullptr;
    std::uint64_t registry_version = 0U;
    l2flow::common::Sha256Digest registry_sha256{};
    std::vector<std::unique_ptr<WorkerState>> workers;
    std::vector<OrdinalEntry> ordinals;
    std::array<std::atomic<std::uint32_t>,
               kIntradayInstrumentStoreSourceCountV1>
        source_stream_ids{};
    std::atomic<std::uint64_t> reserved_records{0U};
    // Includes base index state, every allocated chunk, and conservative
    // retained-record accounting. This is the one session byte authority.
    std::atomic<std::uint64_t> reserved_session_bytes{0U};
    std::atomic<std::uint64_t> appended_records{0U};
    std::atomic<std::uint64_t> accounted_record_bytes{0U};
    std::atomic<std::uint64_t> allocated_index_bytes{0U};
    std::atomic<std::uint64_t> allocated_chunks{0U};
    std::atomic<std::uint64_t> failed_appends{0U};
    std::atomic<std::uint64_t> latest_generation{0U};
    std::atomic<bool> coverage_lost{false};
    std::uint64_t base_index_bytes = 0U;
    std::uint64_t chunk_allocation_bytes = 0U;
    mutable std::mutex generation_mutex;
};

struct WorkerSliceData final {
    std::shared_ptr<SessionState> session;
    std::uint32_t worker = 0U;
    std::uint64_t generation = 0U;
    std::vector<CapturedInstrumentRow> rows;
};

struct GenerationData final {
    RealtimeHistoryWatermarkV1 watermark{};
    std::shared_ptr<const SessionState> session;
    std::vector<CapturedInstrumentRow> rows;
    std::uint64_t record_count = 0U;
    std::uint64_t accounted_record_bytes = 0U;
    std::uint64_t allocated_index_bytes = 0U;
    std::size_t maximum_records_per_batch = 0U;
    bool coverage_from_open = false;
};

[[nodiscard]] const OrdinalEntry* FindOrdinal(
    const SessionState& session,
    std::uint32_t instrument_id) noexcept {
    const auto found = std::lower_bound(
        session.ordinals.begin(),
        session.ordinals.end(),
        instrument_id,
        [](const OrdinalEntry& entry, std::uint32_t id) noexcept {
            return entry.instrument_id < id;
        });
    return found != session.ordinals.end() &&
                   found->instrument_id == instrument_id
               ? &*found
               : nullptr;
}

[[nodiscard]] const CapturedInstrumentRow* FindCapturedRow(
    const GenerationData& generation,
    std::uint32_t instrument_id) noexcept {
    const auto found = std::lower_bound(
        generation.rows.begin(),
        generation.rows.end(),
        instrument_id,
        [](const CapturedInstrumentRow& row, std::uint32_t id) noexcept {
            return row.instrument_id < id;
        });
    return found != generation.rows.end() &&
                   found->instrument_id == instrument_id
               ? &*found
               : nullptr;
}

void FillSummary(
    const CapturedInstrumentRow& row,
    IntradayInstrumentSummaryV1* output) noexcept {
    *output = IntradayInstrumentSummaryV1{};
    output->instrument_id = row.instrument_id;
    output->latest_snapshot = row.latest_snapshot;
    output->latest_tick = row.latest_tick;
    for (std::size_t source = 0U; source < row.lanes.size(); ++source) {
        output->source_record_counts[source] =
            row.lanes[source].record_count;
        output->record_count += row.lanes[source].record_count;
    }
}

[[nodiscard]] std::uint64_t CapturedLaneBytes(
    const CapturedLane& lane) noexcept {
    return lane.accounted_bytes;
}

[[nodiscard]] std::uint64_t CapturedLaneChunks(
    const CapturedLane& lane,
    std::size_t chunk_capacity) noexcept {
    if (lane.record_count == 0U || chunk_capacity == 0U) {
        return 0U;
    }
    const std::uint64_t capacity =
        static_cast<std::uint64_t>(chunk_capacity);
    return 1U + (lane.record_count - 1U) / capacity;
}

[[nodiscard]] bool ValidScanOptions(
    const IntradayInstrumentScanOptionsV1& options) noexcept {
    return options.ingress_sequence_begin_inclusive != 0U &&
           options.ingress_sequence_begin_inclusive <
               options.ingress_sequence_end_exclusive &&
           options.maximum_records != 0U &&
           ValidDirection(options.direction);
}

[[nodiscard]] bool SameWatermark(
    const RealtimeHistoryWatermarkV1& left,
    const RealtimeHistoryWatermarkV1& right) noexcept {
    bool same_sources = true;
    for (std::size_t source = 0U;
         source < left.sources.size();
         ++source) {
        same_sources =
            same_sources &&
            left.sources[source].source_stream_id ==
                right.sources[source].source_stream_id &&
            left.sources[source].sequence_exclusive ==
                right.sources[source].sequence_exclusive;
    }
    return left.run_id == right.run_id &&
           left.generation == right.generation &&
           left.trade_date == right.trade_date &&
           left.ingress_sequence_exclusive ==
               right.ingress_sequence_exclusive &&
           left.recv_monotonic_cut_ns == right.recv_monotonic_cut_ns &&
           left.registry_version == right.registry_version &&
           left.registry_sha256 == right.registry_sha256 &&
           same_sources &&
           left.input_identity_sha256 == right.input_identity_sha256;
}

[[nodiscard]] bool ValidateCapturedLane(
    const CapturedLane& lane,
    std::size_t chunk_capacity,
    std::uint8_t source,
    const RealtimeHistoryWatermarkV1& watermark) noexcept {
    if (lane.record_count == 0U) {
        return lane.head == nullptr && lane.tail == nullptr &&
               lane.tail_used == 0U && lane.accounted_bytes == 0U;
    }
    if (lane.head == nullptr || lane.tail == nullptr ||
        lane.tail_used == 0U || lane.tail_used > chunk_capacity ||
        lane.accounted_bytes == 0U) {
        return false;
    }
    const std::uint64_t capacity =
        static_cast<std::uint64_t>(chunk_capacity);
    const std::uint64_t complete_chunks =
        (lane.record_count - 1U) / capacity;
    const std::size_t expected_tail_used = static_cast<std::size_t>(
        lane.record_count - complete_chunks * capacity);
    if (lane.tail_used != expected_tail_used) {
        return false;
    }
    const RecordHandle& last =
        lane.tail->records[lane.tail_used - 1U];
    return last != nullptr && last->source_slot() == source &&
           last->source_stream_id() ==
               watermark.sources[source].source_stream_id &&
           last->source_sequence() <
               watermark.sources[source].sequence_exclusive &&
           last->ingress_sequence() <
               watermark.ingress_sequence_exclusive;
}

struct LanePosition final {
    CapturedLane endpoint{};
    const IntradayChunk* chunk = nullptr;
    std::size_t index = 0U;
    std::uint64_t remaining = 0U;
};

class MergedInstrumentReader final {
public:
    MergedInstrumentReader(
        const CapturedInstrumentRow& row,
        IntradayInstrumentScanOptionsV1 options,
        std::size_t chunk_capacity) noexcept
        : options_(options), chunk_capacity_(chunk_capacity) {
        for (std::size_t source = 0U; source < positions_.size(); ++source) {
            LanePosition& position = positions_[source];
            position.endpoint = row.lanes[source];
            position.remaining = position.endpoint.record_count;
            if (options_.direction ==
                IntradayInstrumentScanDirectionV1::kOldestFirst) {
                position.chunk = position.endpoint.head;
                position.index = 0U;
            } else {
                position.chunk = position.endpoint.tail;
                position.index = position.endpoint.tail_used;
            }
            current_[source] = Normalize(position);
        }
        RefreshDone();
    }

    [[nodiscard]] bool Pop(
        const RealtimeHistoryRecordV1** output) noexcept {
        if (output == nullptr || done_) {
            return false;
        }
        std::size_t selected = current_.size();
        for (std::size_t source = 0U; source < current_.size(); ++source) {
            const RealtimeHistoryRecordV1* candidate = current_[source];
            if (candidate == nullptr) {
                continue;
            }
            if (selected == current_.size() ||
                Better(candidate, current_[selected])) {
                selected = source;
            }
        }
        if (selected == current_.size()) {
            done_ = true;
            return false;
        }

        *output = current_[selected];
        Advance(positions_[selected]);
        ++emitted_;
        current_[selected] = Normalize(positions_[selected]);
        RefreshDone();
        return true;
    }

    [[nodiscard]] bool done() const noexcept { return done_; }

private:
    [[nodiscard]] std::size_t UsedInCurrentChunk(
        const LanePosition& position) const noexcept {
        return position.chunk == position.endpoint.tail
                   ? position.endpoint.tail_used
                   : chunk_capacity_;
    }

    [[nodiscard]] const RealtimeHistoryRecordV1* Current(
        const LanePosition& position) const noexcept {
        if (position.remaining == 0U || position.chunk == nullptr) {
            return nullptr;
        }
        if (options_.direction ==
            IntradayInstrumentScanDirectionV1::kOldestFirst) {
            const std::size_t used = UsedInCurrentChunk(position);
            return position.index < used
                       ? position.chunk->records[position.index].get()
                       : nullptr;
        }
        return position.index != 0U
                   ? position.chunk->records[position.index - 1U].get()
                   : nullptr;
    }

    void Advance(LanePosition& position) noexcept {
        if (position.remaining == 0U || position.chunk == nullptr) {
            return;
        }
        --position.remaining;
        if (options_.direction ==
            IntradayInstrumentScanDirectionV1::kOldestFirst) {
            ++position.index;
            if (position.remaining != 0U &&
                position.index >= UsedInCurrentChunk(position)) {
                position.chunk = position.chunk->owned_next.get();
                position.index = 0U;
            }
            return;
        }

        --position.index;
        if (position.remaining != 0U && position.index == 0U) {
            position.chunk = position.chunk->previous;
            position.index = chunk_capacity_;
        }
    }

    [[nodiscard]] const RealtimeHistoryRecordV1* Normalize(
        LanePosition& position) noexcept {
        while (position.remaining != 0U) {
            const RealtimeHistoryRecordV1* record = Current(position);
            if (record == nullptr) {
                position.remaining = 0U;
                return nullptr;
            }
            const std::uint64_t ingress = record->ingress_sequence();
            if (options_.direction ==
                IntradayInstrumentScanDirectionV1::kOldestFirst) {
                if (ingress < options_.ingress_sequence_begin_inclusive) {
                    Advance(position);
                    continue;
                }
                if (ingress >= options_.ingress_sequence_end_exclusive) {
                    position.remaining = 0U;
                    return nullptr;
                }
                return record;
            }
            if (ingress >= options_.ingress_sequence_end_exclusive) {
                Advance(position);
                continue;
            }
            if (ingress < options_.ingress_sequence_begin_inclusive) {
                position.remaining = 0U;
                return nullptr;
            }
            return record;
        }
        return nullptr;
    }

    [[nodiscard]] bool Better(
        const RealtimeHistoryRecordV1* candidate,
        const RealtimeHistoryRecordV1* selected) const noexcept {
        if (options_.direction ==
            IntradayInstrumentScanDirectionV1::kOldestFirst) {
            return candidate->ingress_sequence() <
                   selected->ingress_sequence();
        }
        return candidate->ingress_sequence() >
               selected->ingress_sequence();
    }

    void RefreshDone() noexcept {
        done_ = emitted_ >= options_.maximum_records ||
                std::none_of(
                    current_.begin(),
                    current_.end(),
                    [](const RealtimeHistoryRecordV1* value) noexcept {
                        return value != nullptr;
                    });
    }

    IntradayInstrumentScanOptionsV1 options_{};
    std::size_t chunk_capacity_ = 0U;
    std::array<LanePosition, kIntradayInstrumentStoreSourceCountV1>
        positions_{};
    std::array<const RealtimeHistoryRecordV1*,
               kIntradayInstrumentStoreSourceCountV1>
        current_{};
    std::uint64_t emitted_ = 0U;
    bool done_ = false;
};

}  // namespace

class IntradayInstrumentCursorV1::Impl final {
public:
    Impl(
        std::shared_ptr<const GenerationData> value_generation,
        const CapturedInstrumentRow& row,
        IntradayInstrumentScanOptionsV1 options) noexcept
        : generation(std::move(value_generation)),
          reader(
              row,
              options,
              generation->session->config.chunk_record_capacity) {}

    std::shared_ptr<const GenerationData> generation;
    MergedInstrumentReader reader;
};

class IntradayUniverseCursorV1::Impl final {
public:
    Impl(
        std::shared_ptr<const GenerationData> value_generation,
        std::size_t ordinal_begin,
        std::size_t ordinal_end_exclusive,
        IntradayInstrumentScanOptionsV1 value_options) noexcept
        : generation(std::move(value_generation)),
          options(value_options),
          ordinal(ordinal_begin),
          ordinal_end(ordinal_end_exclusive) {
        PrepareReader();
    }

    [[nodiscard]] bool Pop(
        const RealtimeHistoryRecordV1** output) noexcept {
        if (output == nullptr || done) {
            return false;
        }
        for (;;) {
            PrepareReader();
            if (done || !reader.has_value()) {
                return false;
            }
            if (reader->Pop(output)) {
                break;
            }
            reader.reset();
            ++ordinal;
        }
        ++emitted;
        if (reader->done()) {
            reader.reset();
            ++ordinal;
        }
        if (emitted >= options.maximum_records) {
            done = true;
        } else {
            PrepareReader();
        }
        return true;
    }

    void PrepareReader() noexcept {
        while (!done && reader == std::nullopt) {
            if (emitted >= options.maximum_records ||
                ordinal >= ordinal_end) {
                done = true;
                return;
            }
            IntradayInstrumentScanOptionsV1 instrument_options = options;
            instrument_options.maximum_records =
                options.maximum_records - emitted;
            reader.emplace(
                generation->rows[ordinal],
                instrument_options,
                generation->session->config.chunk_record_capacity);
            if (reader->done()) {
                reader.reset();
                ++ordinal;
            }
        }
    }

    std::shared_ptr<const GenerationData> generation;
    IntradayInstrumentScanOptionsV1 options{};
    std::size_t ordinal = 0U;
    std::size_t ordinal_end = 0U;
    std::uint64_t emitted = 0U;
    std::optional<MergedInstrumentReader> reader;
    bool done = false;
};

class IntradayInstrumentStoreGenerationV1::Impl final {
public:
    explicit Impl(
        std::shared_ptr<const GenerationData> value) noexcept
        : data(std::move(value)) {}

    std::shared_ptr<const GenerationData> data;
};

class IntradayInstrumentStoreWorkerSliceV1::Impl final {
public:
    explicit Impl(WorkerSliceData value) noexcept
        : data(std::move(value)) {}

    WorkerSliceData data;
};

class IntradayInstrumentStoreV1::Impl final {
public:
    explicit Impl(std::shared_ptr<SessionState> value) noexcept
        : session(std::move(value)) {}

    [[nodiscard]] IntradayInstrumentStoreAppendErrorV1 FailAppend(
        IntradayInstrumentStoreAppendErrorV1 error) noexcept {
        if (!session->coverage_lost.exchange(
                true, std::memory_order_acq_rel)) {
            SaturatingAtomicIncrement(&session->failed_appends);
        }
        return error;
    }

    std::shared_ptr<SessionState> session;
};

std::string_view IntradayInstrumentStoreCreateErrorNameV1(
    IntradayInstrumentStoreCreateErrorV1 error) noexcept {
    switch (error) {
        case IntradayInstrumentStoreCreateErrorV1::kNone:
            return "none";
        case IntradayInstrumentStoreCreateErrorV1::kNullOutput:
            return "null_output";
        case IntradayInstrumentStoreCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case IntradayInstrumentStoreCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "invalid_intraday_instrument_store_create_error";
}

std::string_view IntradayInstrumentStoreAppendErrorNameV1(
    IntradayInstrumentStoreAppendErrorV1 error) noexcept {
    switch (error) {
        case IntradayInstrumentStoreAppendErrorV1::kNone:
            return "none";
        case IntradayInstrumentStoreAppendErrorV1::kCoverageLost:
            return "coverage_lost";
        case IntradayInstrumentStoreAppendErrorV1::kInvalidRecord:
            return "invalid_record";
        case IntradayInstrumentStoreAppendErrorV1::kWrongWorker:
            return "wrong_worker";
        case IntradayInstrumentStoreAppendErrorV1::kSequenceNotIncreasing:
            return "sequence_not_increasing";
        case IntradayInstrumentStoreAppendErrorV1::kRecordCapacity:
            return "record_capacity";
        case IntradayInstrumentStoreAppendErrorV1::kByteCapacity:
            return "byte_capacity";
        case IntradayInstrumentStoreAppendErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "invalid_intraday_instrument_store_append_error";
}

std::string_view IntradayInstrumentStoreGenerationErrorNameV1(
    IntradayInstrumentStoreGenerationErrorV1 error) noexcept {
    switch (error) {
        case IntradayInstrumentStoreGenerationErrorV1::kNone:
            return "none";
        case IntradayInstrumentStoreGenerationErrorV1::kNullOutput:
            return "null_output";
        case IntradayInstrumentStoreGenerationErrorV1::kCoverageLost:
            return "coverage_lost";
        case IntradayInstrumentStoreGenerationErrorV1::kInvalidWorker:
            return "invalid_worker";
        case IntradayInstrumentStoreGenerationErrorV1::kInvalidGeneration:
            return "invalid_generation";
        case IntradayInstrumentStoreGenerationErrorV1::kInvalidWatermark:
            return "invalid_watermark";
        case IntradayInstrumentStoreGenerationErrorV1::kIncompleteWorkerSet:
            return "incomplete_worker_set";
        case IntradayInstrumentStoreGenerationErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "invalid_intraday_instrument_store_generation_error";
}

std::string_view IntradayInstrumentStoreQueryErrorNameV1(
    IntradayInstrumentStoreQueryErrorV1 error) noexcept {
    switch (error) {
        case IntradayInstrumentStoreQueryErrorV1::kNone:
            return "none";
        case IntradayInstrumentStoreQueryErrorV1::kNullOutput:
            return "null_output";
        case IntradayInstrumentStoreQueryErrorV1::kInvalidArgument:
            return "invalid_argument";
        case IntradayInstrumentStoreQueryErrorV1::kNotFound:
            return "not_found";
        case IntradayInstrumentStoreQueryErrorV1::kBatchLimitExceeded:
            return "batch_limit_exceeded";
        case IntradayInstrumentStoreQueryErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "invalid_intraday_instrument_store_query_error";
}

std::uint64_t EstimateIntradayInstrumentRecordBytesV1(
    const RecordHandle& record) noexcept {
    if (record == nullptr) {
        return std::numeric_limits<std::uint64_t>::max();
    }
    const std::size_t retained =
        EstimateRetainedMarketEventBytesV1(record->event());
    if (retained == std::numeric_limits<std::size_t>::max() ||
        retained < sizeof(RetainedMarketEventV1)) {
        return std::numeric_limits<std::uint64_t>::max();
    }
    std::uint64_t result =
        static_cast<std::uint64_t>(sizeof(RealtimeHistoryRecordV1));
    const std::uint64_t retained_tail = static_cast<std::uint64_t>(
        retained - sizeof(RetainedMarketEventV1));
    if (!CheckedAdd(result, retained_tail, &result) ||
        !CheckedAdd(
            result,
            static_cast<std::uint64_t>(sizeof(RecordHandle)),
            &result)) {
        return std::numeric_limits<std::uint64_t>::max();
    }
    return result;
}

IntradayInstrumentCursorV1::IntradayInstrumentCursorV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

IntradayInstrumentCursorV1::IntradayInstrumentCursorV1(
    IntradayInstrumentCursorV1&&) noexcept = default;

IntradayInstrumentCursorV1& IntradayInstrumentCursorV1::operator=(
    IntradayInstrumentCursorV1&&) noexcept = default;

IntradayInstrumentCursorV1::~IntradayInstrumentCursorV1() = default;

IntradayInstrumentStoreQueryErrorV1
IntradayInstrumentCursorV1::ReadBatch(
    std::span<const RealtimeHistoryRecordV1*> output,
    std::size_t* written) noexcept {
    if (written == nullptr) {
        return IntradayInstrumentStoreQueryErrorV1::kNullOutput;
    }
    *written = 0U;
    if (impl_ == nullptr) {
        return IntradayInstrumentStoreQueryErrorV1::kInvalidArgument;
    }
    if (output.size() >
        impl_->generation->maximum_records_per_batch) {
        return IntradayInstrumentStoreQueryErrorV1::kBatchLimitExceeded;
    }
    if (impl_->reader.done()) {
        return IntradayInstrumentStoreQueryErrorV1::kNone;
    }
    if (output.empty()) {
        return IntradayInstrumentStoreQueryErrorV1::kInvalidArgument;
    }
    while (*written < output.size()) {
        const RealtimeHistoryRecordV1* record = nullptr;
        if (!impl_->reader.Pop(&record)) {
            break;
        }
        output[*written] = record;
        ++*written;
    }
    return IntradayInstrumentStoreQueryErrorV1::kNone;
}

bool IntradayInstrumentCursorV1::done() const noexcept {
    return impl_ == nullptr || impl_->reader.done();
}

IntradayUniverseCursorV1::IntradayUniverseCursorV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

IntradayUniverseCursorV1::IntradayUniverseCursorV1(
    IntradayUniverseCursorV1&&) noexcept = default;

IntradayUniverseCursorV1& IntradayUniverseCursorV1::operator=(
    IntradayUniverseCursorV1&&) noexcept = default;

IntradayUniverseCursorV1::~IntradayUniverseCursorV1() = default;

IntradayInstrumentStoreQueryErrorV1
IntradayUniverseCursorV1::ReadBatch(
    std::span<const RealtimeHistoryRecordV1*> output,
    std::size_t* written) noexcept {
    if (written == nullptr) {
        return IntradayInstrumentStoreQueryErrorV1::kNullOutput;
    }
    *written = 0U;
    if (impl_ == nullptr) {
        return IntradayInstrumentStoreQueryErrorV1::kInvalidArgument;
    }
    if (output.size() >
        impl_->generation->maximum_records_per_batch) {
        return IntradayInstrumentStoreQueryErrorV1::kBatchLimitExceeded;
    }
    if (impl_->done) {
        return IntradayInstrumentStoreQueryErrorV1::kNone;
    }
    if (output.empty()) {
        return IntradayInstrumentStoreQueryErrorV1::kInvalidArgument;
    }
    while (*written < output.size()) {
        const RealtimeHistoryRecordV1* record = nullptr;
        if (!impl_->Pop(&record)) {
            break;
        }
        output[*written] = record;
        ++*written;
    }
    return IntradayInstrumentStoreQueryErrorV1::kNone;
}

bool IntradayUniverseCursorV1::done() const noexcept {
    return impl_ == nullptr || impl_->done;
}

IntradayInstrumentStoreGenerationV1::
    IntradayInstrumentStoreGenerationV1(
        std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

IntradayInstrumentStoreGenerationV1::
    ~IntradayInstrumentStoreGenerationV1() = default;

const RealtimeHistoryWatermarkV1&
IntradayInstrumentStoreGenerationV1::watermark() const noexcept {
    return impl_->data->watermark;
}

std::size_t
IntradayInstrumentStoreGenerationV1::instrument_count() const noexcept {
    return impl_->data->rows.size();
}

std::uint64_t
IntradayInstrumentStoreGenerationV1::record_count() const noexcept {
    return impl_->data->record_count;
}

std::uint64_t
IntradayInstrumentStoreGenerationV1::accounted_record_bytes()
    const noexcept {
    return impl_->data->accounted_record_bytes;
}

std::uint64_t
IntradayInstrumentStoreGenerationV1::allocated_index_bytes()
    const noexcept {
    return impl_->data->allocated_index_bytes;
}

bool IntradayInstrumentStoreGenerationV1::coverage_from_open()
    const noexcept {
    return impl_->data->coverage_from_open;
}

IntradayInstrumentStoreQueryErrorV1
IntradayInstrumentStoreGenerationV1::Find(
    std::uint32_t instrument_id,
    IntradayInstrumentSummaryV1* output) const noexcept {
    if (output == nullptr) {
        return IntradayInstrumentStoreQueryErrorV1::kNullOutput;
    }
    *output = IntradayInstrumentSummaryV1{};
    if (instrument_id == 0U) {
        return IntradayInstrumentStoreQueryErrorV1::kInvalidArgument;
    }
    const CapturedInstrumentRow* row =
        FindCapturedRow(*impl_->data, instrument_id);
    if (row == nullptr) {
        return IntradayInstrumentStoreQueryErrorV1::kNotFound;
    }
    FillSummary(*row, output);
    return IntradayInstrumentStoreQueryErrorV1::kNone;
}

IntradayInstrumentStoreQueryErrorV1
IntradayInstrumentStoreGenerationV1::SummaryAt(
    std::size_t ordinal,
    IntradayInstrumentSummaryV1* output) const noexcept {
    if (output == nullptr) {
        return IntradayInstrumentStoreQueryErrorV1::kNullOutput;
    }
    *output = IntradayInstrumentSummaryV1{};
    if (ordinal >= impl_->data->rows.size()) {
        return IntradayInstrumentStoreQueryErrorV1::kNotFound;
    }
    FillSummary(impl_->data->rows[ordinal], output);
    return IntradayInstrumentStoreQueryErrorV1::kNone;
}

IntradayInstrumentStoreQueryErrorV1
IntradayInstrumentStoreGenerationV1::OpenInstrumentCursor(
    std::uint32_t instrument_id,
    IntradayInstrumentScanOptionsV1 options,
    std::unique_ptr<IntradayInstrumentCursorV1>* output) const noexcept {
    if (output == nullptr) {
        return IntradayInstrumentStoreQueryErrorV1::kNullOutput;
    }
    output->reset();
    if (instrument_id == 0U || !ValidScanOptions(options)) {
        return IntradayInstrumentStoreQueryErrorV1::kInvalidArgument;
    }
    const CapturedInstrumentRow* row =
        FindCapturedRow(*impl_->data, instrument_id);
    if (row == nullptr) {
        return IntradayInstrumentStoreQueryErrorV1::kNotFound;
    }
    try {
        auto cursor_impl = std::make_unique<
            IntradayInstrumentCursorV1::Impl>(
            impl_->data, *row, options);
        output->reset(
            new IntradayInstrumentCursorV1(std::move(cursor_impl)));
        return IntradayInstrumentStoreQueryErrorV1::kNone;
    } catch (...) {
        return IntradayInstrumentStoreQueryErrorV1::kResourceExhausted;
    }
}

IntradayInstrumentStoreQueryErrorV1
IntradayInstrumentStoreGenerationV1::OpenTailCursor(
    std::uint32_t instrument_id,
    std::uint64_t count,
    std::unique_ptr<IntradayInstrumentCursorV1>* output) const noexcept {
    if (output == nullptr) {
        return IntradayInstrumentStoreQueryErrorV1::kNullOutput;
    }
    output->reset();
    if (count == 0U) {
        return IntradayInstrumentStoreQueryErrorV1::kInvalidArgument;
    }
    IntradayInstrumentScanOptionsV1 options{};
    options.maximum_records = count;
    options.direction =
        IntradayInstrumentScanDirectionV1::kNewestFirst;
    return OpenInstrumentCursor(instrument_id, options, output);
}

IntradayInstrumentStoreQueryErrorV1
IntradayInstrumentStoreGenerationV1::OpenUniverseCursor(
    IntradayInstrumentScanOptionsV1 options,
    std::unique_ptr<IntradayUniverseCursorV1>* output) const noexcept {
    return OpenUniverseRangeCursor(
        0U, impl_->data->rows.size(), options, output);
}

IntradayInstrumentStoreQueryErrorV1
IntradayInstrumentStoreGenerationV1::OpenUniverseRangeCursor(
    std::size_t ordinal_begin,
    std::size_t ordinal_end_exclusive,
    IntradayInstrumentScanOptionsV1 options,
    std::unique_ptr<IntradayUniverseCursorV1>* output) const noexcept {
    if (output == nullptr) {
        return IntradayInstrumentStoreQueryErrorV1::kNullOutput;
    }
    output->reset();
    if (!ValidScanOptions(options) ||
        options.direction !=
            IntradayInstrumentScanDirectionV1::kOldestFirst ||
        ordinal_begin > ordinal_end_exclusive ||
        ordinal_end_exclusive > impl_->data->rows.size()) {
        return IntradayInstrumentStoreQueryErrorV1::kInvalidArgument;
    }
    try {
        auto cursor_impl =
            std::make_unique<IntradayUniverseCursorV1::Impl>(
                impl_->data,
                ordinal_begin,
                ordinal_end_exclusive,
                options);
        output->reset(
            new IntradayUniverseCursorV1(std::move(cursor_impl)));
        return IntradayInstrumentStoreQueryErrorV1::kNone;
    } catch (...) {
        return IntradayInstrumentStoreQueryErrorV1::kResourceExhausted;
    }
}

IntradayInstrumentStoreWorkerSliceV1::
    IntradayInstrumentStoreWorkerSliceV1(
        std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

IntradayInstrumentStoreWorkerSliceV1::
    ~IntradayInstrumentStoreWorkerSliceV1() = default;

IntradayInstrumentStoreV1::IntradayInstrumentStoreV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

IntradayInstrumentStoreV1::~IntradayInstrumentStoreV1() = default;

IntradayInstrumentStoreCreateErrorV1
IntradayInstrumentStoreV1::Create(
    IntradayInstrumentStoreConfigV1 config,
    std::uint32_t worker_count,
    const InstrumentRegistryV1* registry,
    std::unique_ptr<IntradayInstrumentStoreV1>* output) noexcept {
    if (output == nullptr) {
        return IntradayInstrumentStoreCreateErrorV1::kNullOutput;
    }
    output->reset();
    if (worker_count == 0U || worker_count > 256U ||
        registry == nullptr || registry->empty() ||
        config.chunk_record_capacity == 0U ||
        config.chunk_record_capacity >
            kIntradayInstrumentStoreMaximumChunkRecordsV1 ||
        config.maximum_records_per_batch == 0U ||
        config.maximum_records_per_batch >
            kIntradayInstrumentStoreMaximumBatchRecordsV1 ||
        config.chunk_record_capacity >
            std::numeric_limits<std::size_t>::max() /
                sizeof(RecordHandle) ||
        config.maximum_session_records == 0U ||
        config.maximum_session_accounted_bytes == 0U) {
        return IntradayInstrumentStoreCreateErrorV1::
            kInvalidConfiguration;
    }

    try {
        auto session = std::make_shared<SessionState>();
        session->config = config;
        session->worker_count = worker_count;
        session->registry = registry;
        session->registry_version = registry->registry_version();
        session->registry_sha256 = registry->registry_sha256();
        session->workers.reserve(worker_count);
        for (std::uint32_t worker = 0U; worker < worker_count; ++worker) {
            session->workers.push_back(std::make_unique<WorkerState>());
        }

        session->ordinals.reserve(registry->size());
        for (const InstrumentRegistryEntryV1& entry :
             registry->entries()) {
            session->ordinals.push_back(
                OrdinalEntry{entry.instrument_id, 0U, 0U});
        }
        std::sort(
            session->ordinals.begin(),
            session->ordinals.end(),
            [](const OrdinalEntry& left,
               const OrdinalEntry& right) noexcept {
                return left.instrument_id < right.instrument_id;
            });

        std::vector<std::size_t> worker_sizes(worker_count, 0U);
        for (const OrdinalEntry& ordinal : session->ordinals) {
            ++worker_sizes[ordinal.instrument_id % worker_count];
        }
        for (std::uint32_t worker = 0U; worker < worker_count; ++worker) {
            session->workers[worker]->rows.reserve(worker_sizes[worker]);
        }
        for (std::size_t ordinal_index = 0U;
             ordinal_index < session->ordinals.size();
             ++ordinal_index) {
            OrdinalEntry& ordinal = session->ordinals[ordinal_index];
            ordinal.worker = ordinal.instrument_id % worker_count;
            WorkerState& worker = *session->workers[ordinal.worker];
            ordinal.local_index = worker.rows.size();
            worker.rows.emplace_back(
                ordinal.instrument_id, ordinal_index);
        }

        std::uint64_t chunk_slots = 0U;
        if (!CheckedMultiply(
                static_cast<std::uint64_t>(
                    config.chunk_record_capacity),
                static_cast<std::uint64_t>(
                    sizeof(RecordHandle)),
                &chunk_slots) ||
            !CheckedAdd(
                static_cast<std::uint64_t>(sizeof(IntradayChunk)),
                chunk_slots,
                &session->chunk_allocation_bytes)) {
            return IntradayInstrumentStoreCreateErrorV1::
                kInvalidConfiguration;
        }

        std::uint64_t base_bytes =
            static_cast<std::uint64_t>(sizeof(SessionState));
        std::uint64_t term = 0U;
        std::uint64_t worker_bytes = 0U;
        if (!CheckedMultiply(
                static_cast<std::uint64_t>(worker_count),
                static_cast<std::uint64_t>(
                    sizeof(std::unique_ptr<WorkerState>) +
                    sizeof(WorkerState)),
                &worker_bytes) ||
            !CheckedAdd(base_bytes, worker_bytes, &base_bytes) ||
            !CheckedMultiply(
                static_cast<std::uint64_t>(
                    session->ordinals.size()),
                static_cast<std::uint64_t>(
                    sizeof(OrdinalEntry) +
                    sizeof(MutableInstrumentRow)),
                &term) ||
            !CheckedAdd(base_bytes, term, &base_bytes)) {
            return IntradayInstrumentStoreCreateErrorV1::
                kInvalidConfiguration;
        }
        if (base_bytes >
            config.maximum_session_accounted_bytes) {
            return IntradayInstrumentStoreCreateErrorV1::
                kInvalidConfiguration;
        }
        session->base_index_bytes = base_bytes;
        session->allocated_index_bytes.store(
            base_bytes, std::memory_order_relaxed);
        session->reserved_session_bytes.store(
            base_bytes, std::memory_order_relaxed);

        auto impl = std::make_unique<Impl>(std::move(session));
        output->reset(new IntradayInstrumentStoreV1(std::move(impl)));
        return IntradayInstrumentStoreCreateErrorV1::kNone;
    } catch (...) {
        output->reset();
        return IntradayInstrumentStoreCreateErrorV1::
            kResourceExhausted;
    }
}

IntradayInstrumentStoreAppendErrorV1
IntradayInstrumentStoreV1::Append(
    std::uint32_t worker,
    RecordHandle record) noexcept {
    SessionState& session = *impl_->session;
    if (session.coverage_lost.load(std::memory_order_acquire)) {
        return IntradayInstrumentStoreAppendErrorV1::kCoverageLost;
    }
    if (record == nullptr ||
        record->source_slot() >=
            kIntradayInstrumentStoreSourceCountV1 ||
        record->source_stream_id() == 0U ||
        record->source_sequence() == 0U ||
        record->source_sequence() ==
            std::numeric_limits<std::uint64_t>::max() ||
        record->ingress_sequence() == 0U ||
        record->ingress_sequence() ==
            std::numeric_limits<std::uint64_t>::max() ||
        record->instrument_id() == 0U ||
        !KindBelongsToSource(
            record->kind(), record->source_slot())) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::kInvalidRecord);
    }
    const OrdinalEntry* ordinal =
        FindOrdinal(session, record->instrument_id());
    if (ordinal == nullptr) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::kInvalidRecord);
    }
    if (worker >= session.worker_count ||
        ordinal->worker != worker) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::kWrongWorker);
    }

    WorkerState& worker_state = *session.workers[worker];
    if (ordinal->local_index >= worker_state.rows.size()) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::kWrongWorker);
    }
    MutableInstrumentRow& row =
        worker_state.rows[ordinal->local_index];
    if (row.instrument_id != record->instrument_id()) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::kInvalidRecord);
    }
    MutableLane& lane = row.lanes[record->source_slot()];
    if (lane.record_count != 0U &&
        (record->source_sequence() <= lane.last_source_sequence ||
         record->ingress_sequence() <=
             lane.last_ingress_sequence)) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::
                kSequenceNotIncreasing);
    }

    std::uint32_t expected_stream = session.source_stream_ids[
        record->source_slot()].load(std::memory_order_acquire);
    if (expected_stream == 0U) {
        static_cast<void>(
            session.source_stream_ids[record->source_slot()]
                .compare_exchange_strong(
                    expected_stream,
                    record->source_stream_id(),
                    std::memory_order_acq_rel,
                    std::memory_order_acquire));
        expected_stream = session.source_stream_ids[
            record->source_slot()].load(std::memory_order_acquire);
    }
    if (expected_stream != record->source_stream_id()) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::kInvalidRecord);
    }

    const std::uint64_t accounted_bytes =
        EstimateIntradayInstrumentRecordBytesV1(record);
    if (accounted_bytes ==
        std::numeric_limits<std::uint64_t>::max()) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::kInvalidRecord);
    }
    const bool needs_chunk =
        lane.tail == nullptr ||
        lane.tail_used == session.config.chunk_record_capacity;
    std::uint64_t byte_reservation = accounted_bytes;
    if (needs_chunk &&
        !CheckedAdd(
            byte_reservation,
            session.chunk_allocation_bytes,
            &byte_reservation)) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::kByteCapacity);
    }
    if (!TryReserve(
            &session.reserved_records,
            1U,
            session.config.maximum_session_records)) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::kRecordCapacity);
    }
    if (!TryReserve(
            &session.reserved_session_bytes,
            byte_reservation,
            session.config.maximum_session_accounted_bytes)) {
        session.reserved_records.fetch_sub(
            1U, std::memory_order_acq_rel);
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::kByteCapacity);
    }

    try {
        if (needs_chunk) {
            auto candidate = std::make_unique<IntradayChunk>(
                session.config.chunk_record_capacity);
            candidate->previous = lane.tail;
            IntradayChunk* const raw = candidate.get();
            if (lane.tail == nullptr) {
                lane.owned_head = std::move(candidate);
                lane.head = raw;
            } else {
                lane.tail->owned_next = std::move(candidate);
            }
            lane.tail = raw;
            lane.tail_used = 0U;
            SaturatingAtomicIncrement(&session.allocated_chunks);
            SaturatingAtomicAdd(
                &session.allocated_index_bytes,
                session.chunk_allocation_bytes);
        }

        const std::uint64_t next_lane_bytes =
            lane.accounted_bytes + accounted_bytes;
        IntradayChunk& chunk = *lane.tail;
        chunk.records[lane.tail_used] = std::move(record);
        const RealtimeHistoryRecordV1* const appended =
            chunk.records[lane.tail_used].get();
        if (lane.tail_used == 0U) {
            chunk.first_ingress_sequence =
                appended->ingress_sequence();
            chunk.first_source_sequence =
                appended->source_sequence();
        }
        chunk.last_ingress_sequence =
            appended->ingress_sequence();
        chunk.last_source_sequence =
            appended->source_sequence();
        ++lane.tail_used;
        ++lane.record_count;
        lane.accounted_bytes = next_lane_bytes;
        lane.last_ingress_sequence =
            appended->ingress_sequence();
        lane.last_source_sequence =
            appended->source_sequence();

        const RealtimeHistoryRecordV1*& latest =
            SnapshotKind(appended->kind())
                ? row.latest_snapshot
                : row.latest_tick;
        if (latest == nullptr ||
            latest->ingress_sequence() <
                appended->ingress_sequence()) {
            latest = appended;
        }
        session.appended_records.fetch_add(
            1U, std::memory_order_release);
        session.accounted_record_bytes.fetch_add(
            accounted_bytes, std::memory_order_release);
        return IntradayInstrumentStoreAppendErrorV1::kNone;
    } catch (...) {
        session.reserved_session_bytes.fetch_sub(
            byte_reservation, std::memory_order_acq_rel);
        session.reserved_records.fetch_sub(
            1U, std::memory_order_acq_rel);
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::
                kResourceExhausted);
    }
}

IntradayInstrumentStoreGenerationErrorV1
IntradayInstrumentStoreV1::CaptureWorker(
    std::uint32_t worker,
    std::uint64_t generation,
    std::unique_ptr<IntradayInstrumentStoreWorkerSliceV1>* output)
    noexcept {
    if (output == nullptr) {
        return IntradayInstrumentStoreGenerationErrorV1::kNullOutput;
    }
    output->reset();
    SessionState& session = *impl_->session;
    if (session.coverage_lost.load(std::memory_order_acquire)) {
        return IntradayInstrumentStoreGenerationErrorV1::kCoverageLost;
    }
    if (worker >= session.worker_count) {
        return IntradayInstrumentStoreGenerationErrorV1::kInvalidWorker;
    }
    WorkerState& worker_state = *session.workers[worker];
    if (generation == 0U ||
        generation <= worker_state.last_captured_generation) {
        return IntradayInstrumentStoreGenerationErrorV1::
            kInvalidGeneration;
    }

    try {
        WorkerSliceData slice{};
        slice.session = impl_->session;
        slice.worker = worker;
        slice.generation = generation;
        slice.rows.reserve(worker_state.rows.size());
        for (const MutableInstrumentRow& row : worker_state.rows) {
            CapturedInstrumentRow captured{};
            captured.instrument_id = row.instrument_id;
            captured.ordinal = row.ordinal;
            captured.latest_snapshot = row.latest_snapshot;
            captured.latest_tick = row.latest_tick;
            for (std::size_t source = 0U;
                 source < row.lanes.size();
                 ++source) {
                const MutableLane& lane = row.lanes[source];
                captured.lanes[source] = CapturedLane{
                    lane.head,
                    lane.tail,
                    lane.tail_used,
                    lane.record_count,
                    lane.accounted_bytes};
            }
            slice.rows.push_back(captured);
        }
        auto slice_impl =
            std::make_unique<IntradayInstrumentStoreWorkerSliceV1::Impl>(
                std::move(slice));
        output->reset(new IntradayInstrumentStoreWorkerSliceV1(
            std::move(slice_impl)));
        worker_state.last_captured_generation = generation;
        return IntradayInstrumentStoreGenerationErrorV1::kNone;
    } catch (...) {
        output->reset();
        return IntradayInstrumentStoreGenerationErrorV1::
            kResourceExhausted;
    }
}

IntradayInstrumentStoreGenerationErrorV1
IntradayInstrumentStoreV1::BuildGeneration(
    const RealtimeHistoryWatermarkV1& watermark,
    std::vector<
        std::unique_ptr<IntradayInstrumentStoreWorkerSliceV1>>
        worker_slices,
    std::shared_ptr<const IntradayInstrumentStoreGenerationV1>* output)
    noexcept {
    if (output == nullptr) {
        return IntradayInstrumentStoreGenerationErrorV1::kNullOutput;
    }
    output->reset();
    SessionState& session = *impl_->session;
    if (session.coverage_lost.load(std::memory_order_acquire)) {
        return IntradayInstrumentStoreGenerationErrorV1::kCoverageLost;
    }
    if (watermark.generation == 0U) {
        return IntradayInstrumentStoreGenerationErrorV1::
            kInvalidGeneration;
    }
    if (worker_slices.size() != session.worker_count) {
        return IntradayInstrumentStoreGenerationErrorV1::
            kIncompleteWorkerSet;
    }

    RealtimeHistoryWatermarkV1 rebuilt{};
    if (BuildRealtimeHistoryWatermarkV1(
            watermark.run_id,
            watermark.generation,
            watermark.trade_date,
            watermark.ingress_sequence_exclusive,
            watermark.recv_monotonic_cut_ns,
            *session.registry,
            watermark.sources,
            &rebuilt) !=
            RealtimeHistoryWatermarkErrorV1::kNone ||
        !SameWatermark(watermark, rebuilt) ||
        watermark.registry_version != session.registry_version ||
        watermark.registry_sha256 != session.registry_sha256) {
        return IntradayInstrumentStoreGenerationErrorV1::
            kInvalidWatermark;
    }

    try {
        std::lock_guard<std::mutex> lock(session.generation_mutex);
        if (session.coverage_lost.load(std::memory_order_acquire)) {
            return IntradayInstrumentStoreGenerationErrorV1::
                kCoverageLost;
        }
        if (watermark.generation <=
            session.latest_generation.load(std::memory_order_acquire)) {
            return IntradayInstrumentStoreGenerationErrorV1::
                kInvalidGeneration;
        }

        auto generation = std::make_shared<GenerationData>();
        generation->watermark = watermark;
        generation->session = impl_->session;
        generation->rows.resize(session.ordinals.size());
        generation->maximum_records_per_batch =
            session.config.maximum_records_per_batch;
        generation->coverage_from_open =
            session.config.coverage_from_open;

        std::array<std::uint64_t,
                   kIntradayInstrumentStoreSourceCountV1>
            source_counts{};
        std::uint64_t total_records = 0U;
        std::uint64_t total_bytes = 0U;
        std::uint64_t total_chunks = 0U;

        for (std::size_t worker = 0U;
             worker < worker_slices.size();
             ++worker) {
            const auto& owner = worker_slices[worker];
            if (owner == nullptr || owner->impl_ == nullptr) {
                return IntradayInstrumentStoreGenerationErrorV1::
                    kIncompleteWorkerSet;
            }
            const WorkerSliceData& slice = owner->impl_->data;
            if (slice.session.get() != &session ||
                slice.worker != worker ||
                slice.generation != watermark.generation ||
                slice.rows.size() !=
                    session.workers[worker]->rows.size()) {
                return IntradayInstrumentStoreGenerationErrorV1::
                    kIncompleteWorkerSet;
            }
            for (const CapturedInstrumentRow& row : slice.rows) {
                if (row.instrument_id == 0U ||
                    row.ordinal >= generation->rows.size() ||
                    generation->rows[row.ordinal].instrument_id != 0U ||
                    session.ordinals[row.ordinal].instrument_id !=
                        row.instrument_id ||
                    session.ordinals[row.ordinal].worker != worker) {
                    return IntradayInstrumentStoreGenerationErrorV1::
                        kIncompleteWorkerSet;
                }
                for (std::size_t source = 0U;
                     source < row.lanes.size();
                     ++source) {
                    const CapturedLane& lane = row.lanes[source];
                    if (!ValidateCapturedLane(
                            lane,
                            session.config.chunk_record_capacity,
                            static_cast<std::uint8_t>(source),
                            watermark) ||
                        !CheckedAdd(
                            source_counts[source],
                            lane.record_count,
                            &source_counts[source]) ||
                        !CheckedAdd(
                            total_records,
                            lane.record_count,
                            &total_records) ||
                        !CheckedAdd(
                            total_bytes,
                            CapturedLaneBytes(lane),
                            &total_bytes) ||
                        !CheckedAdd(
                            total_chunks,
                            CapturedLaneChunks(
                                lane,
                                session.config.chunk_record_capacity),
                            &total_chunks)) {
                        return IntradayInstrumentStoreGenerationErrorV1::
                            kInvalidWatermark;
                    }
                }
                const auto valid_latest =
                    [&watermark](
                        const RealtimeHistoryRecordV1* latest) noexcept {
                        return latest == nullptr ||
                               latest->ingress_sequence() <
                                   watermark
                                       .ingress_sequence_exclusive;
                    };
                if (!valid_latest(row.latest_snapshot) ||
                    !valid_latest(row.latest_tick)) {
                    return IntradayInstrumentStoreGenerationErrorV1::
                        kInvalidWatermark;
                }
                generation->rows[row.ordinal] = row;
            }
        }

        if (total_records !=
            watermark.ingress_sequence_exclusive - 1U) {
            return IntradayInstrumentStoreGenerationErrorV1::
                kInvalidWatermark;
        }
        for (std::size_t source = 0U;
             source < source_counts.size();
             ++source) {
            if (source_counts[source] !=
                watermark.sources[source].sequence_exclusive - 1U) {
                return IntradayInstrumentStoreGenerationErrorV1::
                    kInvalidWatermark;
            }
            const std::uint32_t observed_stream =
                session.source_stream_ids[source].load(
                    std::memory_order_acquire);
            if (observed_stream != 0U &&
                observed_stream !=
                    watermark.sources[source].source_stream_id) {
                return IntradayInstrumentStoreGenerationErrorV1::
                    kInvalidWatermark;
            }
        }

        std::uint64_t chunk_bytes = 0U;
        std::uint64_t generation_index_bytes = 0U;
        if (!CheckedMultiply(
                total_chunks,
                session.chunk_allocation_bytes,
                &chunk_bytes) ||
            !CheckedAdd(
                session.base_index_bytes,
                chunk_bytes,
                &generation_index_bytes)) {
            return IntradayInstrumentStoreGenerationErrorV1::
                kResourceExhausted;
        }
        generation->record_count = total_records;
        generation->accounted_record_bytes = total_bytes;
        generation->allocated_index_bytes =
            generation_index_bytes;

        if (session.coverage_lost.load(std::memory_order_acquire)) {
            return IntradayInstrumentStoreGenerationErrorV1::
                kCoverageLost;
        }
        auto generation_impl =
            std::make_unique<IntradayInstrumentStoreGenerationV1::Impl>(
                std::shared_ptr<const GenerationData>(
                    std::move(generation)));
        std::shared_ptr<const IntradayInstrumentStoreGenerationV1>
            published(
                new IntradayInstrumentStoreGenerationV1(
                    std::move(generation_impl)));
        session.latest_generation.store(
            watermark.generation, std::memory_order_release);
        *output = std::move(published);
        return IntradayInstrumentStoreGenerationErrorV1::kNone;
    } catch (...) {
        output->reset();
        return IntradayInstrumentStoreGenerationErrorV1::
            kResourceExhausted;
    }
}

void IntradayInstrumentStoreV1::MarkCoverageLost() noexcept {
    impl_->session->coverage_lost.store(
        true, std::memory_order_release);
}

IntradayInstrumentStoreSnapshotV1
IntradayInstrumentStoreV1::Snapshot() const noexcept {
    const SessionState& session = *impl_->session;
    IntradayInstrumentStoreSnapshotV1 result{};
    result.maximum_session_records =
        session.config.maximum_session_records;
    result.maximum_session_accounted_bytes =
        session.config.maximum_session_accounted_bytes;
    result.appended_records =
        session.appended_records.load(std::memory_order_acquire);
    result.accounted_record_bytes =
        session.accounted_record_bytes.load(std::memory_order_acquire);
    result.allocated_index_bytes =
        session.allocated_index_bytes.load(std::memory_order_acquire);
    result.allocated_chunks =
        session.allocated_chunks.load(std::memory_order_acquire);
    result.failed_appends =
        session.failed_appends.load(std::memory_order_acquire);
    result.latest_generation =
        session.latest_generation.load(std::memory_order_acquire);
    result.coverage_lost =
        session.coverage_lost.load(std::memory_order_acquire);
    result.coverage_from_open =
        session.config.coverage_from_open && !result.coverage_lost;
    return result;
}

std::uint32_t IntradayInstrumentStoreV1::WorkerForInstrument(
    std::uint32_t instrument_id) const noexcept {
    if (impl_ == nullptr || impl_->session->worker_count == 0U) {
        return std::numeric_limits<std::uint32_t>::max();
    }
    return instrument_id % impl_->session->worker_count;
}

const IntradayInstrumentStoreConfigV1&
IntradayInstrumentStoreV1::config() const noexcept {
    return impl_->session->config;
}

}  // namespace l2flow::market
