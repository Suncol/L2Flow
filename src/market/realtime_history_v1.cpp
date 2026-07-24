#include "l2flow/market/realtime_history_v1.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <new>
#include <optional>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>

namespace l2flow::market {
namespace {

constexpr std::string_view kWatermarkHashDomainV1 =
    "L2FLOW_REALTIME_HISTORY_WATERMARK_V1";

bool DigestNonzero(const l2flow::common::Sha256Digest& digest) noexcept {
    return std::any_of(
        digest.begin(), digest.end(), [](std::byte value) {
            return value != std::byte{0U};
        });
}

bool ValidTradeDate(std::uint32_t value) noexcept {
    const std::uint32_t year = value / 10000U;
    const std::uint32_t month = (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (year == 0U || month == 0U || month > 12U || day == 0U) {
        return false;
    }
    constexpr std::array<std::uint32_t, 12U> days_by_month{
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U};
    std::uint32_t maximum_day = days_by_month[month - 1U];
    const bool leap =
        (year % 4U == 0U && year % 100U != 0U) ||
        year % 400U == 0U;
    if (month == 2U && leap) {
        maximum_day = 29U;
    }
    return day <= maximum_day;
}

bool HashBytes(
    l2flow::common::Sha256Hasher* hasher,
    std::span<const std::byte> bytes) noexcept {
    return hasher != nullptr && hasher->Update(bytes);
}

bool HashU32(
    l2flow::common::Sha256Hasher* hasher,
    std::uint32_t value) noexcept {
    std::array<std::byte, 4U> bytes{};
    for (std::size_t index = 0U; index < bytes.size(); ++index) {
        bytes[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
    return HashBytes(hasher, bytes);
}

bool HashU64(
    l2flow::common::Sha256Hasher* hasher,
    std::uint64_t value) noexcept {
    std::array<std::byte, 8U> bytes{};
    for (std::size_t index = 0U; index < bytes.size(); ++index) {
        bytes[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
    return HashBytes(hasher, bytes);
}

bool ComputeWatermarkIdentity(
    l2flow::common::Identity128 run_id,
    std::uint64_t generation,
    std::uint32_t trade_date,
    std::uint64_t ingress_sequence_exclusive,
    std::uint64_t registry_version,
    const l2flow::common::Sha256Digest& registry_sha256,
    std::span<const RealtimeSourceWatermarkV1,
              kRealtimeHistorySourceCountV1> sources,
    l2flow::common::Sha256Digest* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    l2flow::common::Sha256Hasher hasher;
    const auto domain = std::span<const char>(
        kWatermarkHashDomainV1.data(), kWatermarkHashDomainV1.size());
    constexpr std::array<std::byte, 1U> separator{std::byte{0U}};
    if (!hasher.Update(std::as_bytes(domain)) ||
        !hasher.Update(separator) ||
        !hasher.Update(run_id) ||
        !HashU64(&hasher, generation) ||
        !HashU32(&hasher, trade_date) ||
        !HashU64(&hasher, ingress_sequence_exclusive) ||
        !HashU64(&hasher, registry_version) ||
        !hasher.Update(registry_sha256)) {
        return false;
    }
    for (const RealtimeSourceWatermarkV1& source : sources) {
        if (!HashU32(&hasher, source.source_stream_id) ||
            !HashU64(&hasher, source.sequence_exclusive)) {
            return false;
        }
    }
    return hasher.Finalize(output);
}

template <typename Event>
constexpr MarketEventKindV1 EventKind() noexcept;

template <>
constexpr MarketEventKindV1 EventKind<ShanghaiSnapshotV1>() noexcept {
    return MarketEventKindV1::kShanghaiSnapshot;
}

template <>
constexpr MarketEventKindV1 EventKind<ShanghaiTickV1>() noexcept {
    return MarketEventKindV1::kShanghaiTick;
}

template <>
constexpr MarketEventKindV1 EventKind<ShenzhenSnapshotV1>() noexcept {
    return MarketEventKindV1::kShenzhenSnapshot;
}

template <>
constexpr MarketEventKindV1 EventKind<ShenzhenOrderV1>() noexcept {
    return MarketEventKindV1::kShenzhenOrder;
}

template <>
constexpr MarketEventKindV1 EventKind<ShenzhenTransactionV1>() noexcept {
    return MarketEventKindV1::kShenzhenTransaction;
}

struct RetainedEventDescription final {
    const DecodedMarketCommonV1* common = nullptr;
    MarketEventKindV1 kind = MarketEventKindV1::kShanghaiSnapshot;
};

RetainedEventDescription Describe(
    const RetainedMarketEventV1& event) noexcept {
    return std::visit(
        [](const auto& owner) noexcept {
            using Owner = std::decay_t<decltype(owner)>;
            using Element = typename Owner::element_type;
            using Event = std::remove_const_t<Element>;
            RetainedEventDescription result{};
            if (owner != nullptr) {
                result.common = &owner->common;
                result.kind = EventKind<Event>();
            }
            return result;
        },
        event);
}

bool KindBelongsToSource(
    MarketEventKindV1 kind,
    std::uint8_t source_slot) noexcept {
    switch (source_slot) {
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

bool IsSnapshotKind(MarketEventKindV1 kind) noexcept {
    return kind == MarketEventKindV1::kShanghaiSnapshot ||
           kind == MarketEventKindV1::kShenzhenSnapshot;
}

template <typename Value>
class SpscQueue final {
public:
    explicit SpscQueue(std::size_t capacity)
        : slots_(capacity + 1U) {}

    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;

    bool TryPush(Value value) noexcept {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t next = Increment(tail);
        if (next == head_.load(std::memory_order_acquire)) {
            return false;
        }
        slots_[tail].emplace(std::move(value));
        tail_.store(next, std::memory_order_release);
        return true;
    }

    bool TryPop(Value* output) noexcept {
        if (output == nullptr) {
            return false;
        }
        const std::size_t head = head_.load(std::memory_order_relaxed);
        if (head == tail_.load(std::memory_order_acquire)) {
            return false;
        }
        std::optional<Value>& slot = slots_[head];
        *output = std::move(*slot);
        slot.reset();
        head_.store(Increment(head), std::memory_order_release);
        return true;
    }

    bool Empty() const noexcept {
        return head_.load(std::memory_order_acquire) ==
               tail_.load(std::memory_order_acquire);
    }

private:
    std::size_t Increment(std::size_t value) const noexcept {
        ++value;
        return value == slots_.size() ? 0U : value;
    }

    std::vector<std::optional<Value>> slots_;
    alignas(64) std::atomic<std::size_t> head_{0U};
    alignas(64) std::atomic<std::size_t> tail_{0U};
};

}  // namespace

std::string_view RealtimeHistoryWatermarkErrorNameV1(
    RealtimeHistoryWatermarkErrorV1 error) noexcept {
    switch (error) {
        case RealtimeHistoryWatermarkErrorV1::kNone:
            return "none";
        case RealtimeHistoryWatermarkErrorV1::kNullOutput:
            return "null_output";
        case RealtimeHistoryWatermarkErrorV1::kInvalidRun:
            return "invalid_run";
        case RealtimeHistoryWatermarkErrorV1::kInvalidGeneration:
            return "invalid_generation";
        case RealtimeHistoryWatermarkErrorV1::kInvalidTradeDate:
            return "invalid_trade_date";
        case RealtimeHistoryWatermarkErrorV1::kInvalidIngressCut:
            return "invalid_ingress_cut";
        case RealtimeHistoryWatermarkErrorV1::kInvalidRegistry:
            return "invalid_registry";
        case RealtimeHistoryWatermarkErrorV1::kInvalidSource:
            return "invalid_source";
        case RealtimeHistoryWatermarkErrorV1::kDuplicateSource:
            return "duplicate_source";
        case RealtimeHistoryWatermarkErrorV1::kHashFailure:
            return "hash_failure";
    }
    return "unknown";
}

RealtimeHistoryWatermarkErrorV1 BuildRealtimeHistoryWatermarkV1(
    l2flow::common::Identity128 run_id,
    std::uint64_t generation,
    std::uint32_t trade_date,
    std::uint64_t ingress_sequence_exclusive,
    std::uint64_t recv_monotonic_cut_ns,
    const InstrumentRegistryV1& registry,
    std::span<const RealtimeSourceWatermarkV1,
              kRealtimeHistorySourceCountV1> sources,
    RealtimeHistoryWatermarkV1* output) noexcept {
    if (output == nullptr) {
        return RealtimeHistoryWatermarkErrorV1::kNullOutput;
    }
    if (l2flow::common::IsZeroIdentity(run_id)) {
        return RealtimeHistoryWatermarkErrorV1::kInvalidRun;
    }
    if (generation == 0U) {
        return RealtimeHistoryWatermarkErrorV1::kInvalidGeneration;
    }
    if (!ValidTradeDate(trade_date)) {
        return RealtimeHistoryWatermarkErrorV1::kInvalidTradeDate;
    }
    if (ingress_sequence_exclusive == 0U) {
        return RealtimeHistoryWatermarkErrorV1::kInvalidIngressCut;
    }
    if (registry.registry_version() == 0U || registry.empty() ||
        !DigestNonzero(registry.registry_sha256())) {
        return RealtimeHistoryWatermarkErrorV1::kInvalidRegistry;
    }
    std::uint64_t source_message_count = 0U;
    for (std::size_t index = 0U; index < sources.size(); ++index) {
        if (sources[index].source_stream_id == 0U ||
            sources[index].sequence_exclusive == 0U) {
            return RealtimeHistoryWatermarkErrorV1::kInvalidSource;
        }
        const std::uint64_t source_count =
            sources[index].sequence_exclusive - 1U;
        if (source_count >
            std::numeric_limits<std::uint64_t>::max() -
                source_message_count) {
            return RealtimeHistoryWatermarkErrorV1::kInvalidIngressCut;
        }
        source_message_count += source_count;
        for (std::size_t prior = 0U; prior < index; ++prior) {
            if (sources[prior].source_stream_id ==
                sources[index].source_stream_id) {
                return RealtimeHistoryWatermarkErrorV1::kDuplicateSource;
            }
        }
    }
    // The callback admission path assigns both counters without gaps: every
    // admitted message advances exactly one source counter and the global
    // counter.  Encoding that invariant in the watermark prevents a hash
    // from blessing mutually contradictory global and per-source prefixes.
    if (ingress_sequence_exclusive - 1U != source_message_count) {
        return RealtimeHistoryWatermarkErrorV1::kInvalidIngressCut;
    }

    RealtimeHistoryWatermarkV1 candidate{};
    candidate.run_id = run_id;
    candidate.generation = generation;
    candidate.trade_date = trade_date;
    candidate.ingress_sequence_exclusive =
        ingress_sequence_exclusive;
    candidate.recv_monotonic_cut_ns = recv_monotonic_cut_ns;
    candidate.registry_version = registry.registry_version();
    candidate.registry_sha256 = registry.registry_sha256();
    std::copy(sources.begin(), sources.end(), candidate.sources.begin());
    if (!ComputeWatermarkIdentity(
            run_id,
            generation,
            trade_date,
            ingress_sequence_exclusive,
            candidate.registry_version,
            candidate.registry_sha256,
            candidate.sources,
            &candidate.input_identity_sha256) ||
        !DigestNonzero(candidate.input_identity_sha256)) {
        return RealtimeHistoryWatermarkErrorV1::kHashFailure;
    }
    *output = candidate;
    return RealtimeHistoryWatermarkErrorV1::kNone;
}

RealtimeHistoryRecordV1::RealtimeHistoryRecordV1(
    std::uint8_t source_slot,
    std::uint32_t source_stream_id,
    std::uint64_t source_sequence,
    std::uint64_t ingress_sequence,
    std::uint32_t instrument_id,
    MarketEventKindV1 kind,
    std::int64_t event_time_ns,
    std::int64_t recv_realtime_ns,
    std::int64_t recv_monotonic_ns,
    RetainedMarketEventV1 event) noexcept
    : source_slot_(source_slot),
      source_stream_id_(source_stream_id),
      source_sequence_(source_sequence),
      ingress_sequence_(ingress_sequence),
      instrument_id_(instrument_id),
      kind_(kind),
      event_time_ns_(event_time_ns),
      recv_realtime_ns_(recv_realtime_ns),
      recv_monotonic_ns_(recv_monotonic_ns),
      event_(std::move(event)) {}

bool RealtimeHistoryRecordV1::Create(
    std::uint8_t source_slot,
    std::uint64_t ingress_sequence,
    RetainedMarketEventV1 event,
    std::shared_ptr<const RealtimeHistoryRecordV1>* output) noexcept {
    if (output == nullptr ||
        source_slot >= kRealtimeHistorySourceCountV1 ||
        ingress_sequence == 0U ||
        ingress_sequence == std::numeric_limits<std::uint64_t>::max()) {
        return false;
    }
    const RetainedEventDescription description = Describe(event);
    if (description.common == nullptr ||
        description.common->kind != description.kind ||
        !KindBelongsToSource(description.kind, source_slot) ||
        description.common->origin.source_stream_id == 0U ||
        description.common->origin.source_sequence == 0U ||
        description.common->origin.source_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        description.common->instrument_id == 0U ||
        !description.common->origin.body.empty()) {
        return false;
    }
    const std::int64_t event_time_ns =
        description.common->exchange_time.valid &&
                description.common->exchange_time.unix_nanoseconds_valid
            ? description.common->exchange_time.unix_nanoseconds
            : 0;
    try {
        std::shared_ptr<const RealtimeHistoryRecordV1> candidate(
            new RealtimeHistoryRecordV1(
                source_slot,
                description.common->origin.source_stream_id,
                description.common->origin.source_sequence,
                ingress_sequence,
                description.common->instrument_id,
                description.kind,
                event_time_ns,
                description.common->origin.recv_realtime_ns,
                description.common->origin.recv_monotonic_ns,
                std::move(event)));
        *output = std::move(candidate);
        return true;
    } catch (...) {
        return false;
    }
}

RealtimeHistoryGenerationV1::RealtimeHistoryGenerationV1(
    RealtimeHistoryWatermarkV1 watermark,
    std::vector<RealtimeInstrumentGenerationV1> instruments,
    std::shared_ptr<const IntradayInstrumentStoreGenerationV1>
        intraday_store_generation) noexcept
    : watermark_(std::move(watermark)),
      instruments_(std::move(instruments)),
      intraday_store_generation_(
          std::move(intraday_store_generation)) {}

const RealtimeInstrumentGenerationV1* RealtimeHistoryGenerationV1::Find(
    std::uint32_t instrument_id) const noexcept {
    const auto found = std::lower_bound(
        instruments_.begin(),
        instruments_.end(),
        instrument_id,
        [](const RealtimeInstrumentGenerationV1& row,
           std::uint32_t id) { return row.instrument_id < id; });
    return found != instruments_.end() &&
                   found->instrument_id == instrument_id
               ? &*found
               : nullptr;
}

std::string_view RealtimeHistoryCreateErrorNameV1(
    RealtimeHistoryCreateErrorV1 error) noexcept {
    switch (error) {
        case RealtimeHistoryCreateErrorV1::kNone:
            return "none";
        case RealtimeHistoryCreateErrorV1::kNullOutput:
            return "null_output";
        case RealtimeHistoryCreateErrorV1::kInvalidConfiguration:
            return "invalid_configuration";
        case RealtimeHistoryCreateErrorV1::kIntradayStoreCreateFailed:
            return "intraday_store_create_failed";
        case RealtimeHistoryCreateErrorV1::kResourceExhausted:
            return "resource_exhausted";
        case RealtimeHistoryCreateErrorV1::kThreadStartFailed:
            return "thread_start_failed";
    }
    return "unknown";
}

std::string_view RealtimeHistorySubmitErrorNameV1(
    RealtimeHistorySubmitErrorV1 error) noexcept {
    switch (error) {
        case RealtimeHistorySubmitErrorV1::kNone:
            return "none";
        case RealtimeHistorySubmitErrorV1::kInvalidRecord:
            return "invalid_record";
        case RealtimeHistorySubmitErrorV1::kSourceMismatch:
            return "source_mismatch";
        case RealtimeHistorySubmitErrorV1::kSequenceNotIncreasing:
            return "sequence_not_increasing";
        case RealtimeHistorySubmitErrorV1::kQueueFull:
            return "queue_full";
        case RealtimeHistorySubmitErrorV1::kStopped:
            return "stopped";
        case RealtimeHistorySubmitErrorV1::kFatal:
            return "fatal";
    }
    return "unknown";
}

std::string_view RealtimeHistoryGenerationErrorNameV1(
    RealtimeHistoryGenerationErrorV1 error) noexcept {
    switch (error) {
        case RealtimeHistoryGenerationErrorV1::kNone:
            return "none";
        case RealtimeHistoryGenerationErrorV1::kNullOutput:
            return "null_output";
        case RealtimeHistoryGenerationErrorV1::kInvalidWatermark:
            return "invalid_watermark";
        case RealtimeHistoryGenerationErrorV1::kGenerationNotBegun:
            return "generation_not_begun";
        case RealtimeHistoryGenerationErrorV1::kGenerationConflict:
            return "generation_conflict";
        case RealtimeHistoryGenerationErrorV1::kSourceAlreadySealed:
            return "source_already_sealed";
        case RealtimeHistoryGenerationErrorV1::kQueueFull:
            return "queue_full";
        case RealtimeHistoryGenerationErrorV1::kTimeout:
            return "timeout";
        case RealtimeHistoryGenerationErrorV1::kStopped:
            return "stopped";
        case RealtimeHistoryGenerationErrorV1::kFatal:
            return "fatal";
        case RealtimeHistoryGenerationErrorV1::kIntradayStoreFailed:
            return "intraday_store_failed";
        case RealtimeHistoryGenerationErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

class RealtimeHistoryRuntimeV1::Impl final {
public:
    enum class CommandKind : std::uint8_t {
        kRecord = 0U,
        kFence,
    };

    struct Command final {
        CommandKind kind = CommandKind::kRecord;
        RealtimeHistoryRecordHandleV1 record;
        std::uint64_t generation = 0U;
    };

    struct MutableInstrument final {
        std::uint32_t instrument_id = 0U;
        RealtimeHistoryRecordHandleV1 latest_snapshot;
        RealtimeHistoryRecordHandleV1 latest_tick;
        std::vector<RealtimeHistoryRecordHandleV1> history;
    };

    struct PendingGeneration final {
        RealtimeHistoryWatermarkV1 watermark{};
        std::array<bool, kRealtimeHistorySourceCountV1> source_sealed{};
        std::vector<std::optional<
            std::vector<RealtimeInstrumentGenerationV1>>> worker_slices;
        std::vector<std::unique_ptr<
            IntradayInstrumentStoreWorkerSliceV1>>
            intraday_worker_slices;
        std::size_t completed_workers = 0U;
    };

    Impl(
        RealtimeHistoryRuntimeConfigV1 config,
        std::unique_ptr<IntradayInstrumentStoreV1> intraday_store)
        : config_(std::move(config)),
          intraday_store_(std::move(intraday_store)),
          worker_rows_(config_.worker_count),
          queues_(kRealtimeHistorySourceCountV1 * config_.worker_count) {
        universe_ids_.reserve(config_.registry->size());
        for (const InstrumentRegistryEntryV1& entry :
             config_.registry->entries()) {
            universe_ids_.push_back(entry.instrument_id);
            worker_rows_[WorkerFor(entry.instrument_id)].push_back(
                MutableInstrument{entry.instrument_id, {}, {}, {}});
        }
        std::sort(universe_ids_.begin(), universe_ids_.end());
        for (std::vector<MutableInstrument>& rows : worker_rows_) {
            std::sort(
                rows.begin(), rows.end(),
                [](const MutableInstrument& left,
                   const MutableInstrument& right) {
                    return left.instrument_id < right.instrument_id;
                });
        }
        for (std::unique_ptr<SpscQueue<Command>>& queue : queues_) {
            queue = std::make_unique<SpscQueue<Command>>(
                config_.queue_capacity_per_source_worker);
        }
    }

    ~Impl() { StopAndDrain(); }

    bool Start() noexcept {
        try {
            workers_.reserve(config_.worker_count);
            for (std::uint32_t worker = 0U;
                 worker < config_.worker_count;
                 ++worker) {
                workers_.emplace_back([this, worker] {
                    WorkerLoop(worker);
                });
            }
            return true;
        } catch (...) {
            admission_open_.store(false, std::memory_order_release);
            stopping_.store(true, std::memory_order_release);
            work_cv_.notify_all();
            for (std::thread& worker : workers_) {
                if (worker.joinable()) {
                    worker.join();
                }
            }
            return false;
        }
    }

    std::uint32_t WorkerFor(std::uint32_t instrument_id) const noexcept {
        return instrument_id % config_.worker_count;
    }

    bool IntradayRequired() const noexcept {
        return config_.intraday_store.mode ==
                   IntradayInstrumentStoreModeV1::kRequired ||
               config_.intraday_store.mode ==
                   IntradayInstrumentStoreModeV1::kPrimary;
    }

    RealtimeHistoryGenerationErrorV1 FatalGenerationError()
        const noexcept {
        return intraday_store_failed_.load(
                   std::memory_order_acquire)
                   ? RealtimeHistoryGenerationErrorV1::
                         kIntradayStoreFailed
                   : RealtimeHistoryGenerationErrorV1::kFatal;
    }

    SpscQueue<Command>& Queue(
        std::uint8_t source_slot,
        std::uint32_t worker) noexcept {
        const std::size_t index =
            static_cast<std::size_t>(source_slot) *
                static_cast<std::size_t>(config_.worker_count) +
            static_cast<std::size_t>(worker);
        return *queues_[index];
    }

    RealtimeHistorySubmitErrorV1 Submit(
        RealtimeHistoryRecordHandleV1 record) noexcept {
        if (!admission_open_.load(std::memory_order_acquire)) {
            return RealtimeHistorySubmitErrorV1::kStopped;
        }
        if (fatal_.load(std::memory_order_acquire)) {
            return RealtimeHistorySubmitErrorV1::kFatal;
        }
        if (record == nullptr || record->instrument_id() == 0U ||
            record->source_slot() >= kRealtimeHistorySourceCountV1 ||
            record->source_sequence() == 0U ||
            record->ingress_sequence() == 0U ||
            !KindBelongsToSource(record->kind(), record->source_slot()) ||
            config_.registry->LookupById(record->instrument_id()).known() ==
                false) {
            MarkFatal();
            return RealtimeHistorySubmitErrorV1::kInvalidRecord;
        }

        const std::uint8_t source = record->source_slot();
        {
            std::lock_guard<std::mutex> lock(generation_mutex_);
            if (!admission_open_.load(std::memory_order_acquire) ||
                stopping_.load(std::memory_order_acquire)) {
                return RealtimeHistorySubmitErrorV1::kStopped;
            }
            if (fatal_.load(std::memory_order_acquire)) {
                return RealtimeHistorySubmitErrorV1::kFatal;
            }
            if (record->source_stream_id() !=
                config_.source_stream_ids[source]) {
                MarkFatalLocked();
                return RealtimeHistorySubmitErrorV1::kSourceMismatch;
            }
            if (source_last_sequence_[source] ==
                    std::numeric_limits<std::uint64_t>::max() ||
                record->source_sequence() !=
                    source_last_sequence_[source] + 1U ||
                record->ingress_sequence() <=
                    source_last_ingress_sequence_[source]) {
                MarkFatalLocked();
                return RealtimeHistorySubmitErrorV1::kSequenceNotIncreasing;
            }
            if (pending_ != nullptr) {
                const bool sealed = pending_->source_sealed[source];
                const RealtimeSourceWatermarkV1& cut =
                    pending_->watermark.sources[source];
                const bool source_before_cut =
                    record->source_sequence() < cut.sequence_exclusive;
                const bool ingress_before_cut =
                    record->ingress_sequence() <
                    pending_->watermark.ingress_sequence_exclusive;
                if (sealed == source_before_cut ||
                    sealed == ingress_before_cut) {
                    MarkFatalLocked();
                    return RealtimeHistorySubmitErrorV1::kSequenceNotIncreasing;
                }
            }
            Command command{};
            command.kind = CommandKind::kRecord;
            command.record = record;
            const std::uint32_t worker =
                WorkerFor(command.record->instrument_id());
            if (!Queue(source, worker).TryPush(std::move(command))) {
                MarkFatalLocked();
                return RealtimeHistorySubmitErrorV1::kQueueFull;
            }
            source_last_sequence_[source] = record->source_sequence();
            source_last_ingress_sequence_[source] =
                record->ingress_sequence();
        }
        work_cv_.notify_all();
        return RealtimeHistorySubmitErrorV1::kNone;
    }

    RealtimeHistoryGenerationErrorV1 Begin(
        const RealtimeHistoryWatermarkV1& watermark) noexcept {
        if (!admission_open_.load(std::memory_order_acquire)) {
            return RealtimeHistoryGenerationErrorV1::kStopped;
        }
        if (fatal_.load(std::memory_order_acquire)) {
            return FatalGenerationError();
        }
        RealtimeHistoryWatermarkV1 rebuilt{};
        if (BuildRealtimeHistoryWatermarkV1(
                watermark.run_id,
                watermark.generation,
                watermark.trade_date,
                watermark.ingress_sequence_exclusive,
                watermark.recv_monotonic_cut_ns,
                *config_.registry,
                watermark.sources,
                &rebuilt) != RealtimeHistoryWatermarkErrorV1::kNone ||
            rebuilt.registry_version != watermark.registry_version ||
            rebuilt.registry_sha256 != watermark.registry_sha256 ||
            rebuilt.input_identity_sha256 !=
                watermark.input_identity_sha256) {
            return RealtimeHistoryGenerationErrorV1::kInvalidWatermark;
        }
        for (std::size_t source = 0U;
             source < kRealtimeHistorySourceCountV1;
             ++source) {
            if (watermark.sources[source].source_stream_id !=
                config_.source_stream_ids[source]) {
                return RealtimeHistoryGenerationErrorV1::kInvalidWatermark;
            }
        }
        try {
            std::lock_guard<std::mutex> lock(generation_mutex_);
            if (!admission_open_.load(std::memory_order_acquire) ||
                stopping_.load(std::memory_order_acquire)) {
                return RealtimeHistoryGenerationErrorV1::kStopped;
            }
            if (fatal_.load(std::memory_order_acquire)) {
                return FatalGenerationError();
            }
            if (pending_ != nullptr) {
                return RealtimeHistoryGenerationErrorV1::
                    kGenerationConflict;
            }
            if (watermark.generation <= last_started_generation_) {
                return RealtimeHistoryGenerationErrorV1::
                    kGenerationConflict;
            }
            auto pending = std::make_unique<PendingGeneration>();
            pending->watermark = watermark;
            pending->worker_slices.resize(config_.worker_count);
            if (intraday_store_ != nullptr) {
                const IntradayInstrumentStoreSnapshotV1 snapshot =
                    intraday_store_->Snapshot();
                if (snapshot.coverage_lost && IntradayRequired()) {
                    intraday_store_failed_.store(
                        true, std::memory_order_release);
                    MarkFatalLocked();
                    return RealtimeHistoryGenerationErrorV1::
                        kIntradayStoreFailed;
                }
                pending->intraday_worker_slices.resize(
                    config_.worker_count);
            }
            pending_ = std::move(pending);
            last_started_generation_ = watermark.generation;
            return RealtimeHistoryGenerationErrorV1::kNone;
        } catch (...) {
            MarkFatal();
            return RealtimeHistoryGenerationErrorV1::kResourceExhausted;
        }
    }

    RealtimeHistoryGenerationErrorV1 Seal(
        std::uint8_t source,
        std::uint64_t generation) noexcept {
        if (source >= kRealtimeHistorySourceCountV1) {
            return RealtimeHistoryGenerationErrorV1::kInvalidWatermark;
        }
        if (!admission_open_.load(std::memory_order_acquire)) {
            return RealtimeHistoryGenerationErrorV1::kStopped;
        }
        if (fatal_.load(std::memory_order_acquire)) {
            return FatalGenerationError();
        }
        {
            std::lock_guard<std::mutex> lock(generation_mutex_);
            if (!admission_open_.load(std::memory_order_acquire) ||
                stopping_.load(std::memory_order_acquire)) {
                return RealtimeHistoryGenerationErrorV1::kStopped;
            }
            if (fatal_.load(std::memory_order_acquire)) {
                return FatalGenerationError();
            }
            if (pending_ == nullptr) {
                return RealtimeHistoryGenerationErrorV1::
                    kGenerationNotBegun;
            }
            if (pending_->watermark.generation != generation) {
                return RealtimeHistoryGenerationErrorV1::
                    kGenerationConflict;
            }
            if (pending_->source_sealed[source]) {
                return RealtimeHistoryGenerationErrorV1::
                    kSourceAlreadySealed;
            }
            if (source_last_sequence_[source] ==
                    std::numeric_limits<std::uint64_t>::max() ||
                source_last_sequence_[source] + 1U !=
                    pending_->watermark.sources[source]
                        .sequence_exclusive ||
                source_last_ingress_sequence_[source] >=
                    pending_->watermark.ingress_sequence_exclusive) {
                MarkFatalLocked();
                return RealtimeHistoryGenerationErrorV1::
                    kInvalidWatermark;
            }
            pending_->source_sealed[source] = true;
            // Keep the generation lock through all nonblocking fence pushes.
            // Stop/fatal and worker ReportSlice therefore observe either the
            // complete source fence admission or the fatal partial failure.
            for (std::uint32_t worker = 0U;
                 worker < config_.worker_count;
                 ++worker) {
                Command command{};
                command.kind = CommandKind::kFence;
                command.generation = generation;
                if (!Queue(source, worker).TryPush(std::move(command))) {
                    MarkFatalLocked();
                    return RealtimeHistoryGenerationErrorV1::kQueueFull;
                }
            }
        }
        work_cv_.notify_all();
        return RealtimeHistoryGenerationErrorV1::kNone;
    }

    RealtimeHistoryGenerationErrorV1 Wait(
        std::uint64_t generation,
        std::chrono::nanoseconds timeout,
        std::shared_ptr<const RealtimeHistoryGenerationV1>* output) noexcept {
        if (output == nullptr) {
            return RealtimeHistoryGenerationErrorV1::kNullOutput;
        }
        std::unique_lock<std::mutex> lock(generation_mutex_);
        const auto ready = [this, generation] {
            const auto latest = std::atomic_load_explicit(
                &latest_generation_, std::memory_order_acquire);
            return (latest != nullptr &&
                    latest->watermark().generation >= generation) ||
                   fatal_.load(std::memory_order_acquire) ||
                   stopping_.load(std::memory_order_acquire) ||
                   pending_ == nullptr ||
                   pending_->watermark.generation != generation;
        };
        if (!ready() &&
            !generation_cv_.wait_for(lock, timeout, ready)) {
            return RealtimeHistoryGenerationErrorV1::kTimeout;
        }
        const auto latest = std::atomic_load_explicit(
            &latest_generation_, std::memory_order_acquire);
        if (latest != nullptr &&
            latest->watermark().generation == generation) {
            *output = latest;
            return RealtimeHistoryGenerationErrorV1::kNone;
        }
        if (latest != nullptr &&
            latest->watermark().generation > generation) {
            return RealtimeHistoryGenerationErrorV1::kGenerationConflict;
        }
        if (fatal_.load(std::memory_order_acquire)) {
            return FatalGenerationError();
        }
        if (stopping_.load(std::memory_order_acquire)) {
            return RealtimeHistoryGenerationErrorV1::kStopped;
        }
        if (pending_ == nullptr) {
            return RealtimeHistoryGenerationErrorV1::kGenerationNotBegun;
        }
        return RealtimeHistoryGenerationErrorV1::kGenerationConflict;
    }

    void WorkerLoop(std::uint32_t worker) noexcept {
        std::array<bool, kRealtimeHistorySourceCountV1> parked{};
        std::uint64_t parked_generation = 0U;
        while (true) {
            bool progressed = false;
            if (stopping_.load(std::memory_order_acquire)) {
                parked.fill(false);
                parked_generation = 0U;
            }
            for (std::uint8_t source = 0U;
                 source < kRealtimeHistorySourceCountV1;
                 ++source) {
                if (parked[source]) {
                    continue;
                }
                Command command{};
                if (!Queue(source, worker).TryPop(&command)) {
                    continue;
                }
                progressed = true;
                if (command.kind == CommandKind::kRecord) {
                    if (!Append(worker, source, command.record)) {
                        MarkFatal();
                    }
                } else if (!stopping_.load(std::memory_order_acquire)) {
                    if (command.generation == 0U ||
                        (parked_generation != 0U &&
                         parked_generation != command.generation)) {
                        MarkFatal();
                    } else {
                        parked_generation = command.generation;
                        parked[source] = true;
                    }
                }
            }

            if (!stopping_.load(std::memory_order_acquire) &&
                std::all_of(
                    parked.begin(), parked.end(),
                    [](bool value) { return value; })) {
                if (!FreezeAndReport(worker, parked_generation) &&
                    !stopping_.load(std::memory_order_acquire)) {
                    MarkFatal();
                }
                parked.fill(false);
                parked_generation = 0U;
                progressed = true;
            }

            if (stopping_.load(std::memory_order_acquire) &&
                AllQueuesEmpty(worker)) {
                return;
            }
            if (!progressed) {
                std::unique_lock<std::mutex> lock(work_mutex_);
                work_cv_.wait_for(lock, std::chrono::milliseconds(1));
            }
        }
    }

    bool Append(
        std::uint32_t worker,
        std::uint8_t source,
        const RealtimeHistoryRecordHandleV1& record) noexcept {
        if (record == nullptr || record->source_slot() != source ||
            WorkerFor(record->instrument_id()) != worker) {
            return false;
        }
        std::vector<MutableInstrument>& rows = worker_rows_[worker];
        const auto found = std::lower_bound(
            rows.begin(), rows.end(), record->instrument_id(),
            [](const MutableInstrument& row, std::uint32_t id) {
                return row.instrument_id < id;
            });
        if (found == rows.end() ||
            found->instrument_id != record->instrument_id()) {
            return false;
        }
        try {
            // Source decoders are serial only within one source. A worker can
            // therefore observe, for example, a later snapshot before an
            // earlier tick for the same instrument. Keep the row ordered by
            // the process-wide ingress sequence rather than worker arrival.
            const auto position = std::lower_bound(
                found->history.begin(),
                found->history.end(),
                record->ingress_sequence(),
                [](const RealtimeHistoryRecordHandleV1& existing,
                   std::uint64_t ingress_sequence) {
                    return existing->ingress_sequence() < ingress_sequence;
                });
            if (position != found->history.end() &&
                (*position)->ingress_sequence() ==
                    record->ingress_sequence()) {
                return false;
            }
            found->history.insert(position, record);
            if (found->history.size() >
                config_.maximum_records_per_instrument) {
                found->history.erase(found->history.begin());
            }
            RealtimeHistoryRecordHandleV1& latest =
                IsSnapshotKind(record->kind())
                    ? found->latest_snapshot
                    : found->latest_tick;
            if (latest == nullptr ||
                latest->ingress_sequence() < record->ingress_sequence()) {
                latest = record;
            }
            if (intraday_store_ != nullptr) {
                const IntradayInstrumentStoreAppendErrorV1
                    intraday_error =
                        intraday_store_->Append(worker, record);
                if (intraday_error !=
                    IntradayInstrumentStoreAppendErrorV1::kNone) {
                    intraday_store_->MarkCoverageLost();
                    if (IntradayRequired()) {
                        intraday_store_failed_.store(
                            true, std::memory_order_release);
                        return false;
                    }
                }
            }
            return true;
        } catch (...) {
            return false;
        }
    }

    bool FreezeAndReport(
        std::uint32_t worker,
        std::uint64_t generation) noexcept {
        try {
            std::vector<RealtimeInstrumentGenerationV1> slice;
            slice.reserve(worker_rows_[worker].size());
            for (const MutableInstrument& row : worker_rows_[worker]) {
                slice.push_back(RealtimeInstrumentGenerationV1{
                    row.instrument_id,
                    row.latest_snapshot,
                    row.latest_tick,
                    row.history});
            }
            std::unique_ptr<IntradayInstrumentStoreWorkerSliceV1>
                intraday_slice;
            if (intraday_store_ != nullptr &&
                !intraday_store_->Snapshot().coverage_lost) {
                const IntradayInstrumentStoreGenerationErrorV1 error =
                    intraday_store_->CaptureWorker(
                        worker, generation, &intraday_slice);
                if (error !=
                    IntradayInstrumentStoreGenerationErrorV1::kNone) {
                    intraday_store_->MarkCoverageLost();
                    if (IntradayRequired()) {
                        intraday_store_failed_.store(
                            true, std::memory_order_release);
                        return false;
                    }
                    intraday_slice.reset();
                }
            }
            return ReportSlice(
                worker,
                generation,
                std::move(slice),
                std::move(intraday_slice));
        } catch (...) {
            return false;
        }
    }

    bool ReportSlice(
        std::uint32_t worker,
        std::uint64_t generation,
        std::vector<RealtimeInstrumentGenerationV1> slice,
        std::unique_ptr<IntradayInstrumentStoreWorkerSliceV1>
            intraday_slice) noexcept {
        try {
            std::lock_guard<std::mutex> lock(generation_mutex_);
            if (!admission_open_.load(std::memory_order_acquire) ||
                stopping_.load(std::memory_order_acquire) ||
                fatal_.load(std::memory_order_acquire) ||
                pending_ == nullptr ||
                pending_->watermark.generation != generation ||
                worker >= pending_->worker_slices.size() ||
                pending_->worker_slices[worker].has_value()) {
                return false;
            }
            pending_->worker_slices[worker].emplace(std::move(slice));
            if (!pending_->intraday_worker_slices.empty()) {
                pending_->intraday_worker_slices[worker] =
                    std::move(intraday_slice);
            }
            ++pending_->completed_workers;
            if (pending_->completed_workers != config_.worker_count) {
                return true;
            }
            if (!std::all_of(
                    pending_->source_sealed.begin(),
                    pending_->source_sealed.end(),
                    [](bool value) { return value; })) {
                return false;
            }

            std::vector<RealtimeInstrumentGenerationV1> instruments;
            instruments.reserve(universe_ids_.size());
            for (auto& worker_slice : pending_->worker_slices) {
                if (!worker_slice.has_value()) {
                    return false;
                }
                for (RealtimeInstrumentGenerationV1& row : *worker_slice) {
                    instruments.push_back(std::move(row));
                }
            }
            std::sort(
                instruments.begin(), instruments.end(),
                [](const RealtimeInstrumentGenerationV1& left,
                   const RealtimeInstrumentGenerationV1& right) {
                    return left.instrument_id < right.instrument_id;
                });
            if (instruments.size() != universe_ids_.size()) {
                return false;
            }
            for (std::size_t index = 0U;
                 index < universe_ids_.size();
                 ++index) {
                if (instruments[index].instrument_id !=
                    universe_ids_[index]) {
                    return false;
                }
            }
            std::shared_ptr<
                const IntradayInstrumentStoreGenerationV1>
                intraday_generation;
            if (intraday_store_ != nullptr) {
                const IntradayInstrumentStoreSnapshotV1 snapshot =
                    intraday_store_->Snapshot();
                if (!snapshot.coverage_lost) {
                    const IntradayInstrumentStoreGenerationErrorV1
                        intraday_error =
                            intraday_store_->BuildGeneration(
                                pending_->watermark,
                                std::move(
                                    pending_->
                                        intraday_worker_slices),
                                &intraday_generation);
                    if (intraday_error !=
                        IntradayInstrumentStoreGenerationErrorV1::
                            kNone) {
                        intraday_store_->MarkCoverageLost();
                        intraday_generation.reset();
                        if (IntradayRequired()) {
                            intraday_store_failed_.store(
                                true, std::memory_order_release);
                            return false;
                        }
                    }
                } else if (IntradayRequired()) {
                    intraday_store_failed_.store(
                        true, std::memory_order_release);
                    return false;
                }
            }
            if (IntradayRequired() &&
                intraday_generation == nullptr) {
                intraday_store_failed_.store(
                    true, std::memory_order_release);
                return false;
            }
            auto published =
                std::make_shared<const RealtimeHistoryGenerationV1>(
                    pending_->watermark,
                    std::move(instruments),
                    std::move(intraday_generation));
            std::atomic_store_explicit(
                &latest_generation_, published,
                std::memory_order_release);
            pending_.reset();
            generation_cv_.notify_all();
            return true;
        } catch (...) {
            return false;
        }
    }

    bool AllQueuesEmpty(std::uint32_t worker) const noexcept {
        for (std::uint8_t source = 0U;
             source < kRealtimeHistorySourceCountV1;
             ++source) {
            const std::size_t index =
                static_cast<std::size_t>(source) *
                    static_cast<std::size_t>(config_.worker_count) +
                static_cast<std::size_t>(worker);
            if (!queues_[index]->Empty()) {
                return false;
            }
        }
        return true;
    }

    void MarkFatalLocked() noexcept {
        fatal_.store(true, std::memory_order_release);
        generation_cv_.notify_all();
        work_cv_.notify_all();
    }

    void MarkFatal() noexcept {
        std::lock_guard<std::mutex> lock(generation_mutex_);
        MarkFatalLocked();
    }

    void StopAndDrain() noexcept {
        std::lock_guard<std::mutex> stop_lock(stop_mutex_);
        if (stop_complete_) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(generation_mutex_);
            stopping_.store(true, std::memory_order_release);
            admission_open_.store(false, std::memory_order_release);
        }
        work_cv_.notify_all();
        generation_cv_.notify_all();
        for (std::thread& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        stop_complete_ = true;
    }

    RealtimeHistoryRuntimeConfigV1 config_{};
    std::unique_ptr<IntradayInstrumentStoreV1> intraday_store_;
    std::vector<std::uint32_t> universe_ids_;
    std::vector<std::vector<MutableInstrument>> worker_rows_;
    std::vector<std::unique_ptr<SpscQueue<Command>>> queues_;
    std::vector<std::thread> workers_;

    std::atomic<bool> admission_open_{true};
    std::atomic<bool> stopping_{false};
    std::atomic<bool> fatal_{false};
    std::atomic<bool> intraday_store_failed_{false};

    std::mutex stop_mutex_;
    bool stop_complete_ = false;

    mutable std::mutex generation_mutex_;
    std::condition_variable generation_cv_;
    std::unique_ptr<PendingGeneration> pending_;
    std::uint64_t last_started_generation_ = 0U;
    std::array<std::uint64_t, kRealtimeHistorySourceCountV1>
        source_last_sequence_{};
    std::array<std::uint64_t, kRealtimeHistorySourceCountV1>
        source_last_ingress_sequence_{};
    std::shared_ptr<const RealtimeHistoryGenerationV1>
        latest_generation_;

    std::mutex work_mutex_;
    std::condition_variable work_cv_;
};

RealtimeHistoryRuntimeV1::RealtimeHistoryRuntimeV1(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

RealtimeHistoryRuntimeV1::~RealtimeHistoryRuntimeV1() = default;

RealtimeHistoryCreateErrorV1 RealtimeHistoryRuntimeV1::Create(
    RealtimeHistoryRuntimeConfigV1 config,
    std::unique_ptr<RealtimeHistoryRuntimeV1>* output) noexcept {
    if (output == nullptr) {
        return RealtimeHistoryCreateErrorV1::kNullOutput;
    }
    if (config.registry == nullptr || config.registry->empty() ||
        config.worker_count == 0U || config.worker_count > 256U ||
        config.queue_capacity_per_source_worker == 0U ||
        config.queue_capacity_per_source_worker ==
            std::numeric_limits<std::size_t>::max() ||
        config.maximum_records_per_instrument == 0U) {
        return RealtimeHistoryCreateErrorV1::kInvalidConfiguration;
    }
    for (std::size_t source = 0U;
         source < kRealtimeHistorySourceCountV1;
         ++source) {
        if (config.source_stream_ids[source] == 0U) {
            return RealtimeHistoryCreateErrorV1::kInvalidConfiguration;
        }
        for (std::size_t prior = 0U; prior < source; ++prior) {
            if (config.source_stream_ids[prior] ==
                config.source_stream_ids[source]) {
                return RealtimeHistoryCreateErrorV1::
                    kInvalidConfiguration;
            }
        }
    }
    try {
        std::unique_ptr<IntradayInstrumentStoreV1> intraday_store;
        if (config.intraday_store.mode !=
            IntradayInstrumentStoreModeV1::kDisabled) {
            const IntradayInstrumentStoreCreateErrorV1 error =
                IntradayInstrumentStoreV1::Create(
                    config.intraday_store,
                    config.worker_count,
                    config.registry,
                    &intraday_store);
            if (error !=
                    IntradayInstrumentStoreCreateErrorV1::kNone ||
                intraday_store == nullptr) {
                return RealtimeHistoryCreateErrorV1::
                    kIntradayStoreCreateFailed;
            }
        }
        auto impl = std::make_unique<Impl>(
            config, std::move(intraday_store));
        if (!impl->Start()) {
            return RealtimeHistoryCreateErrorV1::kThreadStartFailed;
        }
        auto runtime = std::unique_ptr<RealtimeHistoryRuntimeV1>(
            new RealtimeHistoryRuntimeV1(std::move(impl)));
        *output = std::move(runtime);
        return RealtimeHistoryCreateErrorV1::kNone;
    } catch (const std::bad_alloc&) {
        return RealtimeHistoryCreateErrorV1::kResourceExhausted;
    } catch (...) {
        return RealtimeHistoryCreateErrorV1::kResourceExhausted;
    }
}

RealtimeHistorySubmitErrorV1 RealtimeHistoryRuntimeV1::TrySubmit(
    RealtimeHistoryRecordHandleV1 record) noexcept {
    return impl_->Submit(std::move(record));
}

RealtimeHistoryGenerationErrorV1
RealtimeHistoryRuntimeV1::BeginGeneration(
    const RealtimeHistoryWatermarkV1& watermark) noexcept {
    return impl_->Begin(watermark);
}

RealtimeHistoryGenerationErrorV1 RealtimeHistoryRuntimeV1::SealSource(
    std::uint8_t source_slot,
    std::uint64_t generation) noexcept {
    return impl_->Seal(source_slot, generation);
}

RealtimeHistoryGenerationErrorV1
RealtimeHistoryRuntimeV1::WaitForGeneration(
    std::uint64_t generation,
    std::chrono::nanoseconds timeout,
    std::shared_ptr<const RealtimeHistoryGenerationV1>* output) noexcept {
    return impl_->Wait(generation, timeout, output);
}

std::shared_ptr<const RealtimeHistoryGenerationV1>
RealtimeHistoryRuntimeV1::AcquireLatestGeneration() const noexcept {
    return std::atomic_load_explicit(
        &impl_->latest_generation_, std::memory_order_acquire);
}

std::shared_ptr<const IntradayInstrumentStoreGenerationV1>
RealtimeHistoryRuntimeV1::AcquireLatestIntradayStoreGeneration()
    const noexcept {
    const auto history = AcquireLatestGeneration();
    return history == nullptr
               ? nullptr
               : history->intraday_store_generation();
}

IntradayInstrumentStoreSnapshotV1
RealtimeHistoryRuntimeV1::IntradayStoreSnapshot() const noexcept {
    if (impl_->intraday_store_ == nullptr) {
        IntradayInstrumentStoreSnapshotV1 snapshot{};
        snapshot.mode = impl_->config_.intraday_store.mode;
        snapshot.maximum_session_records =
            impl_->config_.intraday_store.maximum_session_records;
        snapshot.maximum_session_accounted_bytes =
            impl_->config_.intraday_store
                .maximum_session_accounted_bytes;
        return snapshot;
    }
    return impl_->intraday_store_->Snapshot();
}

bool RealtimeHistoryRuntimeV1::IsGenerationCurrentAndHealthy(
    const std::shared_ptr<const RealtimeHistoryGenerationV1>& generation)
    const noexcept {
    if (generation == nullptr || impl_->fatal_.load(std::memory_order_acquire) ||
        impl_->stopping_.load(std::memory_order_acquire)) {
        return false;
    }
    const auto latest = std::atomic_load_explicit(
        &impl_->latest_generation_, std::memory_order_acquire);
    return latest != nullptr && latest.get() == generation.get();
}

bool RealtimeHistoryRuntimeV1::CommitIfCurrentAndHealthy(
    const std::shared_ptr<const RealtimeHistoryGenerationV1>& generation,
    RealtimeHistoryCommitActionV1 action,
    void* context) const noexcept {
    if (generation == nullptr || action == nullptr) {
        return false;
    }
    std::lock_guard<std::mutex> lock(impl_->generation_mutex_);
    if (impl_->fatal_.load(std::memory_order_acquire) ||
        impl_->stopping_.load(std::memory_order_acquire)) {
        return false;
    }
    const auto latest = std::atomic_load_explicit(
        &impl_->latest_generation_, std::memory_order_acquire);
    if (latest == nullptr || latest.get() != generation.get()) {
        return false;
    }
    action(context);
    return true;
}

std::uint32_t RealtimeHistoryRuntimeV1::WorkerForInstrument(
    std::uint32_t instrument_id) const noexcept {
    return impl_->WorkerFor(instrument_id);
}

bool RealtimeHistoryRuntimeV1::fatal() const noexcept {
    return impl_->fatal_.load(std::memory_order_acquire);
}

void RealtimeHistoryRuntimeV1::MarkFatal() noexcept {
    impl_->MarkFatal();
}

void RealtimeHistoryRuntimeV1::StopAndDrain() noexcept {
    impl_->StopAndDrain();
}

const RealtimeHistoryRuntimeConfigV1& RealtimeHistoryRuntimeV1::config()
    const noexcept {
    return impl_->config_;
}

}  // namespace l2flow::market
