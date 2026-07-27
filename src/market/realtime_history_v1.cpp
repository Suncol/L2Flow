#include "l2flow/market/realtime_history_v1.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <ctime>
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

[[nodiscard]] bool ReadClockNs(
    clockid_t clock,
    std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    struct timespec value {};
    if (::clock_gettime(clock, &value) != 0 || value.tv_sec < 0 ||
        value.tv_nsec < 0 || value.tv_nsec >= 1'000'000'000L) {
        return false;
    }
    const std::uint64_t seconds =
        static_cast<std::uint64_t>(value.tv_sec);
    constexpr std::uint64_t kNanosecondsPerSecond = 1'000'000'000ULL;
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() -
         static_cast<std::uint64_t>(value.tv_nsec)) /
            kNanosecondsPerSecond) {
        return false;
    }
    *output = seconds * kNanosecondsPerSecond +
              static_cast<std::uint64_t>(value.tv_nsec);
    return true;
}

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

struct DecodedEventDescription final {
    const DecodedMarketCommonV1* common = nullptr;
    MarketEventKindV1 kind = MarketEventKindV1::kShanghaiSnapshot;
};

DecodedEventDescription Describe(
    const DecodedMarketEventV1& event) noexcept {
    return std::visit(
        [](const auto& value) noexcept {
            using Event = std::decay_t<decltype(value)>;
            DecodedEventDescription result{};
            result.common = &value.common;
            result.kind = EventKind<Event>();
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
        const std::size_t head =
            head_.load(std::memory_order_acquire);
        if (next == head) {
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

// The command ring deliberately carries only one pointer-sized lease, not the
// multi-kilobyte decoded-event variant.  Slots are allocated lazily up to the
// queue's hard bound and then recycled over a reverse SPSC ring.  The source
// decoder is the sole allocator/acquirer and the instrument worker is the
// sole releaser.
class HistoryHandoffPool final {
public:
    struct Slot final {
        alignas(RealtimeHistoryEventInputV1)
            std::array<std::byte, sizeof(RealtimeHistoryEventInputV1)>
                storage{};
        bool engaged = false;

        [[nodiscard]] RealtimeHistoryEventInputV1* Input() noexcept {
            return std::launder(
                reinterpret_cast<RealtimeHistoryEventInputV1*>(
                    storage.data()));
        }
    };

    explicit HistoryHandoffPool(std::size_t capacity)
        : capacity_(capacity), returned_(capacity) {
        owned_.reserve(capacity);
        producer_free_.reserve(capacity);
    }

    HistoryHandoffPool(const HistoryHandoffPool&) = delete;
    HistoryHandoffPool& operator=(const HistoryHandoffPool&) = delete;

    ~HistoryHandoffPool() {
        Slot* returned = nullptr;
        while (returned_.TryPop(&returned)) {
            producer_free_.push_back(returned);
        }
        for (const std::unique_ptr<Slot>& slot : owned_) {
            if (slot->engaged) {
                std::destroy_at(slot->Input());
                slot->engaged = false;
            }
        }
    }

    [[nodiscard]] bool Acquire(
        RealtimeHistoryEventInputV1&& input,
        Slot** output) noexcept {
        if (output == nullptr) {
            return false;
        }
        DrainReturned();
        Slot* slot = nullptr;
        if (!producer_free_.empty()) {
            slot = producer_free_.back();
            producer_free_.pop_back();
        } else {
            if (owned_.size() >= capacity_) {
                return false;
            }
            std::unique_ptr<Slot> candidate(new (std::nothrow) Slot());
            if (candidate == nullptr) {
                return false;
            }
            slot = candidate.get();
            // reserve(capacity_) in the constructor makes this nonallocating.
            owned_.push_back(std::move(candidate));
        }
        std::construct_at(slot->Input(), std::move(input));
        slot->engaged = true;
        *output = slot;
        return true;
    }

    void ReleaseFromProducer(Slot* slot) noexcept {
        Destroy(slot);
        producer_free_.push_back(slot);
    }

    [[nodiscard]] bool ReleaseFromConsumer(Slot* slot) noexcept {
        Destroy(slot);
        return returned_.TryPush(slot);
    }

private:
    void DrainReturned() noexcept {
        Slot* slot = nullptr;
        while (returned_.TryPop(&slot)) {
            producer_free_.push_back(slot);
        }
    }

    static void Destroy(Slot* slot) noexcept {
        if (slot == nullptr || !slot->engaged) {
            return;
        }
        std::destroy_at(slot->Input());
        slot->engaged = false;
    }

    std::size_t capacity_ = 0U;
    SpscQueue<Slot*> returned_;
    std::vector<std::unique_ptr<Slot>> owned_;
    std::vector<Slot*> producer_free_;
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
    std::uint32_t payload_delta) noexcept
    : source_slot_(source_slot),
      source_stream_id_(source_stream_id),
      source_sequence_(source_sequence),
      ingress_sequence_(ingress_sequence),
      instrument_id_(instrument_id),
      kind_(kind),
      event_time_ns_(event_time_ns),
      recv_realtime_ns_(recv_realtime_ns),
      recv_monotonic_ns_(recv_monotonic_ns),
      payload_delta_(payload_delta) {}

RealtimeHistoryRecordV1::~RealtimeHistoryRecordV1() {
    if (payload_delta_ == 0U) {
        return;
    }
    std::byte* const payload =
        reinterpret_cast<std::byte*>(this) + payload_delta_;
    switch (kind_) {
        case MarketEventKindV1::kShanghaiSnapshot:
            std::destroy_at(
                reinterpret_cast<ShanghaiSnapshotV1*>(payload));
            return;
        case MarketEventKindV1::kShanghaiTick:
            std::destroy_at(reinterpret_cast<ShanghaiTickV1*>(payload));
            return;
        case MarketEventKindV1::kShenzhenSnapshot:
            std::destroy_at(
                reinterpret_cast<ShenzhenSnapshotV1*>(payload));
            return;
        case MarketEventKindV1::kShenzhenOrder:
            std::destroy_at(reinterpret_cast<ShenzhenOrderV1*>(payload));
            return;
        case MarketEventKindV1::kShenzhenTransaction:
            std::destroy_at(
                reinterpret_cast<ShenzhenTransactionV1*>(payload));
            return;
    }
}

StoredMarketEventViewV1 RealtimeHistoryRecordV1::event() const noexcept {
    const std::byte* const payload =
        reinterpret_cast<const std::byte*>(this) + payload_delta_;
    switch (kind_) {
        case MarketEventKindV1::kShanghaiSnapshot:
            return StoredMarketEventViewV1(
                std::in_place_type<const ShanghaiSnapshotV1*>,
                reinterpret_cast<const ShanghaiSnapshotV1*>(payload));
        case MarketEventKindV1::kShanghaiTick:
            return StoredMarketEventViewV1(
                std::in_place_type<const ShanghaiTickV1*>,
                reinterpret_cast<const ShanghaiTickV1*>(payload));
        case MarketEventKindV1::kShenzhenSnapshot:
            return StoredMarketEventViewV1(
                std::in_place_type<const ShenzhenSnapshotV1*>,
                reinterpret_cast<const ShenzhenSnapshotV1*>(payload));
        case MarketEventKindV1::kShenzhenOrder:
            return StoredMarketEventViewV1(
                std::in_place_type<const ShenzhenOrderV1*>,
                reinterpret_cast<const ShenzhenOrderV1*>(payload));
        case MarketEventKindV1::kShenzhenTransaction:
            return StoredMarketEventViewV1(
                std::in_place_type<const ShenzhenTransactionV1*>,
                reinterpret_cast<const ShenzhenTransactionV1*>(payload));
    }
    return StoredMarketEventViewV1(
        std::in_place_type<const ShanghaiSnapshotV1*>, nullptr);
}

RealtimeHistoryEventInputV1::RealtimeHistoryEventInputV1(
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
    DecodedMarketEventV1&& event) noexcept
    : source_slot_(source_slot),
      source_stream_id_(source_stream_id),
      source_sequence_(source_sequence),
      ingress_sequence_(ingress_sequence),
      instrument_id_(instrument_id),
      registry_ordinal_(registry_ordinal),
      kind_(kind),
      event_time_ns_(event_time_ns),
      recv_realtime_ns_(recv_realtime_ns),
      recv_monotonic_ns_(recv_monotonic_ns),
      vendor_local_time_raw_(vendor_local_time_raw),
      accounted_record_bytes_(accounted_record_bytes),
      event_(std::move(event)) {}

RealtimeHistoryEventInputV1::RealtimeHistoryEventInputV1(
    RealtimeHistoryEventInputV1&& other) noexcept
    : source_slot_(other.source_slot_),
      source_stream_id_(other.source_stream_id_),
      source_sequence_(other.source_sequence_),
      ingress_sequence_(other.ingress_sequence_),
      instrument_id_(other.instrument_id_),
      registry_ordinal_(other.registry_ordinal_),
      kind_(other.kind_),
      event_time_ns_(other.event_time_ns_),
      recv_realtime_ns_(other.recv_realtime_ns_),
      recv_monotonic_ns_(other.recv_monotonic_ns_),
      vendor_local_time_raw_(other.vendor_local_time_raw_),
      accounted_record_bytes_(other.accounted_record_bytes_),
      event_(std::move(other.event_)),
      valid_(other.valid_) {
    other.valid_ = false;
}

RealtimeHistoryEventInputV1& RealtimeHistoryEventInputV1::operator=(
    RealtimeHistoryEventInputV1&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    source_slot_ = other.source_slot_;
    source_stream_id_ = other.source_stream_id_;
    source_sequence_ = other.source_sequence_;
    ingress_sequence_ = other.ingress_sequence_;
    instrument_id_ = other.instrument_id_;
    registry_ordinal_ = other.registry_ordinal_;
    kind_ = other.kind_;
    event_time_ns_ = other.event_time_ns_;
    recv_realtime_ns_ = other.recv_realtime_ns_;
    recv_monotonic_ns_ = other.recv_monotonic_ns_;
    vendor_local_time_raw_ = other.vendor_local_time_raw_;
    accounted_record_bytes_ = other.accounted_record_bytes_;
    event_ = std::move(other.event_);
    valid_ = other.valid_;
    other.valid_ = false;
    return *this;
}

std::optional<RealtimeHistoryEventInputV1>
RealtimeHistoryEventInputV1::Create(
    std::uint8_t source_slot,
    std::uint64_t ingress_sequence,
    DecodedMarketEventV1&& event) noexcept {
    if (source_slot >= kRealtimeHistorySourceCountV1 ||
        ingress_sequence == 0U ||
        ingress_sequence == std::numeric_limits<std::uint64_t>::max()) {
        return std::nullopt;
    }
    const DecodedEventDescription description = Describe(event);
    if (description.common == nullptr ||
        description.common->kind != description.kind ||
        !KindBelongsToSource(description.kind, source_slot) ||
        description.common->origin.source_stream_id == 0U ||
        description.common->origin.source_sequence == 0U ||
        description.common->origin.source_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        description.common->instrument_id == 0U ||
        description.common->registry_ordinal ==
            std::numeric_limits<std::size_t>::max() ||
        !description.common->origin.body.empty()) {
        return std::nullopt;
    }
    const std::int64_t event_time_ns =
        description.common->exchange_time.valid &&
                description.common->exchange_time.unix_nanoseconds_valid
            ? description.common->exchange_time.unix_nanoseconds
            : 0;
    const std::size_t event_bytes =
        EstimateStoredMarketEventBytesV1(event);
    if (event_bytes == std::numeric_limits<std::size_t>::max() ||
        event_bytes >
            std::numeric_limits<std::uint64_t>::max() -
                sizeof(RealtimeHistoryRecordV1)) {
        return std::nullopt;
    }
    const std::uint64_t accounted_record_bytes =
        static_cast<std::uint64_t>(event_bytes) +
        static_cast<std::uint64_t>(sizeof(RealtimeHistoryRecordV1));
    return RealtimeHistoryEventInputV1(
        source_slot,
        description.common->origin.source_stream_id,
        description.common->origin.source_sequence,
        ingress_sequence,
        description.common->instrument_id,
        description.common->registry_ordinal,
        description.kind,
        event_time_ns,
        description.common->origin.recv_realtime_ns,
        description.common->origin.recv_monotonic_ns,
        description.common->origin.vendor_local_time_raw,
        accounted_record_bytes,
        std::move(event));
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
        case RealtimeHistoryCreateErrorV1::kStoreCreateFailed:
            return "store_create_failed";
        case RealtimeHistoryCreateErrorV1::kKLineCreateFailed:
            return "kline_create_failed";
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
        case RealtimeHistoryGenerationErrorV1::kStoreFailed:
            return "store_failed";
        case RealtimeHistoryGenerationErrorV1::kKLineFailed:
            return "kline_failed";
        case RealtimeHistoryGenerationErrorV1::kResourceExhausted:
            return "resource_exhausted";
    }
    return "unknown";
}

class RealtimeHistoryRuntimeV1::Impl final {
public:
    static constexpr std::uint64_t kSubmissionsClosed =
        std::uint64_t{1U} << 63U;
    static constexpr std::uint64_t kSubmissionCountMask =
        kSubmissionsClosed - 1U;

    enum class CommandKind : std::uint8_t {
        kRecord = 0U,
        kFence,
    };

    struct Command final {
        CommandKind kind = CommandKind::kRecord;
        HistoryHandoffPool::Slot* slot = nullptr;
        InstrumentRouteTokenV1 route{};
        std::uint64_t generation = 0U;
    };

    struct PendingGeneration final {
        RealtimeHistoryWatermarkV1 watermark{};
        std::array<bool, kRealtimeHistorySourceCountV1> source_sealed{};
        std::vector<std::unique_ptr<
            IntradayInstrumentStoreWorkerSliceV1>>
            store_worker_slices;
        std::vector<std::shared_ptr<
            const KLineAggregatorSnapshotV1>>
            kline_worker_snapshots;
        std::size_t completed_workers = 0U;
    };

    struct WorkerSignal final {
        std::mutex mutex;
        std::condition_variable cv;
        std::atomic<bool> sleeping{false};
    };

    struct alignas(64) SourceOwnerState final {
        std::atomic<std::uint64_t> submission_state{0U};
        std::uint64_t last_sequence = 0U;
        std::uint64_t last_ingress_sequence = 0U;
    };

    Impl(
        RealtimeHistoryRuntimeConfigV1 config,
        std::unique_ptr<IntradayInstrumentStoreV1> store,
        std::vector<std::unique_ptr<KLineAggregatorV1>>
            kline_aggregators)
        : config_(std::move(config)),
          store_(std::move(store)),
          kline_aggregators_(std::move(kline_aggregators)),
          queues_(kRealtimeHistorySourceCountV1 * config_.worker_count),
          handoff_pools_(
              kRealtimeHistorySourceCountV1 * config_.worker_count),
          worker_signals_(config_.worker_count) {
        for (std::size_t index = 0U; index < queues_.size(); ++index) {
            queues_[index] = std::make_unique<SpscQueue<Command>>(
                config_.queue_capacity_per_source_worker + 1U);
            handoff_pools_[index] =
                std::make_unique<HistoryHandoffPool>(
                    config_.queue_capacity_per_source_worker);
        }
        for (std::unique_ptr<WorkerSignal>& signal : worker_signals_) {
            signal = std::make_unique<WorkerSignal>();
        }
    }

    ~Impl() { StopAndDrain(); }

    bool Start() noexcept {
        try {
            builder_thread_ = std::thread([this] { BuilderLoop(); });
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
            CloseSubmissionGates();
            stopping_.store(true, std::memory_order_release);
            {
                std::lock_guard<std::mutex> lock(builder_mutex_);
                builder_stop_requested_ = true;
            }
            WakeAllWorkers();
            builder_cv_.notify_all();
            for (std::thread& worker : workers_) {
                if (worker.joinable()) {
                    worker.join();
                }
            }
            if (builder_thread_.joinable()) {
                builder_thread_.join();
            }
            return false;
        }
    }

    std::uint32_t WorkerFor(std::uint32_t instrument_id) const noexcept {
        return instrument_id % config_.worker_count;
    }

    RealtimeHistoryGenerationErrorV1 FatalGenerationError()
        const noexcept {
        if (store_failed_.load(std::memory_order_acquire)) {
            return RealtimeHistoryGenerationErrorV1::kStoreFailed;
        }
        if (kline_failed_.load(std::memory_order_acquire)) {
            return RealtimeHistoryGenerationErrorV1::kKLineFailed;
        }
        return RealtimeHistoryGenerationErrorV1::kFatal;
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

    HistoryHandoffPool& HandoffPool(
        std::uint8_t source_slot,
        std::uint32_t worker) noexcept {
        const std::size_t index =
            static_cast<std::size_t>(source_slot) *
                static_cast<std::size_t>(config_.worker_count) +
            static_cast<std::size_t>(worker);
        return *handoff_pools_[index];
    }

    [[nodiscard]] bool BeginSubmission(std::uint8_t source) noexcept {
        std::atomic<std::uint64_t>& submission_state =
            source_owner_states_[source].submission_state;
        std::uint64_t state =
            submission_state.load(std::memory_order_acquire);
        for (;;) {
            if ((state & kSubmissionsClosed) != 0U ||
                (state & kSubmissionCountMask) ==
                    kSubmissionCountMask) {
                return false;
            }
            if (submission_state.compare_exchange_weak(
                    state,
                    state + 1U,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                return true;
            }
        }
    }

    void EndSubmission(std::uint8_t source) noexcept {
        const std::uint64_t previous =
            source_owner_states_[source].submission_state.fetch_sub(
                1U, std::memory_order_acq_rel);
        if ((previous & kSubmissionsClosed) != 0U &&
            (previous & kSubmissionCountMask) == 1U) {
            std::lock_guard<std::mutex> lock(submission_mutex_);
            submission_cv_.notify_all();
        }
    }

    void CloseSubmissionGates() noexcept {
        for (SourceOwnerState& state : source_owner_states_) {
            state.submission_state.fetch_or(
                kSubmissionsClosed, std::memory_order_acq_rel);
        }
    }

    [[nodiscard]] bool SubmissionsDrained() const noexcept {
        return std::all_of(
            source_owner_states_.begin(),
            source_owner_states_.end(),
            [](const SourceOwnerState& state) noexcept {
                return (state.submission_state.load(
                            std::memory_order_acquire) &
                        kSubmissionCountMask) == 0U;
            });
    }

    RealtimeHistorySubmitErrorV1 Submit(
        RealtimeHistoryEventInputV1&& input) noexcept {
        const std::uint8_t source =
            input.source_slot() < kRealtimeHistorySourceCountV1
                ? input.source_slot()
                : 0U;
        if (!BeginSubmission(source)) {
            return RealtimeHistorySubmitErrorV1::kStopped;
        }
        struct SubmissionGuard final {
            Impl* owner = nullptr;
            std::uint8_t source = 0U;
            ~SubmissionGuard() {
                if (owner != nullptr) {
                    owner->EndSubmission(source);
                }
            }
        } submission_guard{this, source};
        if (!admission_open_.load(std::memory_order_acquire)) {
            return RealtimeHistorySubmitErrorV1::kStopped;
        }
        if (fatal_.load(std::memory_order_acquire)) {
            return RealtimeHistorySubmitErrorV1::kFatal;
        }
        if (!input.valid() || input.instrument_id() == 0U ||
            input.source_slot() >= kRealtimeHistorySourceCountV1 ||
            input.source_sequence() == 0U ||
            input.ingress_sequence() == 0U ||
            !KindBelongsToSource(input.kind(), input.source_slot())) {
            MarkFatal();
            return RealtimeHistorySubmitErrorV1::kInvalidRecord;
        }

        if (input.source_stream_id() != config_.source_stream_ids[source]) {
            MarkFatal();
            return RealtimeHistorySubmitErrorV1::kSourceMismatch;
        }
        // This state is source-owner local.  Submit and SealSource for one
        // source must run on its single serial decoder owner, so the ordinary
        // append path does not acquire the generation commit mutex.
        SourceOwnerState& source_owner = source_owner_states_[source];
        if (source_owner.last_sequence ==
                std::numeric_limits<std::uint64_t>::max() ||
            input.source_sequence() != source_owner.last_sequence + 1U ||
            input.ingress_sequence() <=
                source_owner.last_ingress_sequence) {
            MarkFatal();
            return RealtimeHistorySubmitErrorV1::kSequenceNotIncreasing;
        }

        InstrumentRouteTokenV1 route{};
        if (store_->ResolveRouteToken(
                input.registry_ordinal(),
                input.instrument_id(),
                &route) != IntradayInstrumentStoreQueryErrorV1::kNone ||
            route.worker != WorkerFor(input.instrument_id())) {
            MarkFatal();
            return RealtimeHistorySubmitErrorV1::kInvalidRecord;
        }
        const std::uint32_t worker = route.worker;
        const std::uint64_t accepted_source_sequence =
            input.source_sequence();
        const std::uint64_t accepted_ingress_sequence =
            input.ingress_sequence();
        HistoryHandoffPool::Slot* slot = nullptr;
        HistoryHandoffPool& pool = HandoffPool(source, worker);
        if (!pool.Acquire(std::move(input), &slot) || slot == nullptr) {
            MarkFatal();
            return RealtimeHistorySubmitErrorV1::kQueueFull;
        }
        Command command{};
        command.kind = CommandKind::kRecord;
        command.slot = slot;
        command.route = route;
        if (!Queue(source, worker).TryPush(std::move(command))) {
            pool.ReleaseFromProducer(slot);
            MarkFatal();
            return RealtimeHistorySubmitErrorV1::kQueueFull;
        }
        source_owner.last_sequence = accepted_source_sequence;
        source_owner.last_ingress_sequence =
            accepted_ingress_sequence;
        SignalWorkIfNeeded(worker);
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
                watermark.input_identity_sha256 ||
            (config_.kline.enabled() &&
             watermark.trade_date != config_.kline.trade_date)) {
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
            if (pending_ != nullptr || building_generation_ != 0U) {
                return RealtimeHistoryGenerationErrorV1::
                    kGenerationConflict;
            }
            if (watermark.generation <= last_started_generation_) {
                return RealtimeHistoryGenerationErrorV1::
                    kGenerationConflict;
            }
            auto pending = std::make_unique<PendingGeneration>();
            pending->watermark = watermark;
            if (store_->Snapshot().coverage_lost) {
                store_failed_.store(true, std::memory_order_release);
                MarkFatalLocked(true);
                return RealtimeHistoryGenerationErrorV1::kStoreFailed;
            }
            pending->store_worker_slices.resize(config_.worker_count);
            if (config_.kline.enabled()) {
                pending->kline_worker_snapshots.resize(
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
            const SourceOwnerState& source_owner =
                source_owner_states_[source];
            if (source_owner.last_sequence ==
                    std::numeric_limits<std::uint64_t>::max() ||
                source_owner.last_sequence + 1U !=
                    pending_->watermark.sources[source]
                        .sequence_exclusive ||
                source_owner.last_ingress_sequence >=
                    pending_->watermark.ingress_sequence_exclusive) {
                MarkFatalLocked(false);
                return RealtimeHistoryGenerationErrorV1::
                    kInvalidWatermark;
            }
            pending_->source_sealed[source] = true;
            // The source marker and all of this source's records share one
            // decoder FIFO.  Holding the commit lock only for the rare marker
            // transition makes the source's complete fence fan-out visible
            // atomically to Stop/fatal/generation state.
            for (std::uint32_t worker = 0U;
                 worker < config_.worker_count;
                 ++worker) {
                Command command{};
                command.kind = CommandKind::kFence;
                command.generation = generation;
                if (!Queue(source, worker).TryPush(std::move(command))) {
                    MarkFatalLocked(false);
                    return RealtimeHistoryGenerationErrorV1::kQueueFull;
                }
                SignalWorkIfNeeded(worker);
            }
        }
        return RealtimeHistoryGenerationErrorV1::kNone;
    }

    RealtimeHistoryGenerationErrorV1 Wait(
        std::uint64_t generation,
        std::chrono::nanoseconds timeout,
        std::shared_ptr<const IntradayInstrumentStoreGenerationV1>* output,
        std::shared_ptr<const RealtimeKLineGenerationV1>* kline_output)
        noexcept {
        if (output == nullptr) {
            return RealtimeHistoryGenerationErrorV1::kNullOutput;
        }
        output->reset();
        if (kline_output != nullptr) {
            kline_output->reset();
        }
        std::unique_lock<std::mutex> lock(generation_mutex_);
        const auto ready = [this, generation] {
            const auto latest = std::atomic_load_explicit(
                &latest_generation_, std::memory_order_acquire);
            return (latest != nullptr &&
                    latest->watermark().generation >= generation) ||
                   fatal_.load(std::memory_order_acquire) ||
                   stopping_.load(std::memory_order_acquire) ||
                   ((pending_ == nullptr ||
                     pending_->watermark.generation != generation) &&
                    building_generation_ != generation);
        };
        if (!ready() &&
            !generation_cv_.wait_for(lock, timeout, ready)) {
            return RealtimeHistoryGenerationErrorV1::kTimeout;
        }
        const auto latest = std::atomic_load_explicit(
            &latest_generation_, std::memory_order_acquire);
        if (latest != nullptr &&
            latest->watermark().generation == generation) {
            if (config_.kline.enabled()) {
                const auto latest_kline = std::atomic_load_explicit(
                    &latest_kline_generation_,
                    std::memory_order_acquire);
                if (latest_kline == nullptr ||
                    latest_kline->watermark().generation != generation ||
                    latest_kline->input_store().get() != latest.get() ||
                    latest_kline->input_store().owner_before(latest) ||
                    latest.owner_before(
                        latest_kline->input_store())) {
                    return RealtimeHistoryGenerationErrorV1::
                        kKLineFailed;
                }
                if (kline_output != nullptr) {
                    *kline_output = latest_kline;
                }
            }
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
        constexpr std::size_t kMaximumMicroDrain = 64U;
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
                for (std::size_t drained = 0U;
                     drained < kMaximumMicroDrain;
                     ++drained) {
                    Command command{};
                    if (!Queue(source, worker).TryPop(&command)) {
                        break;
                    }
                    progressed = true;
                    if (command.kind == CommandKind::kRecord) {
                        if (!Append(
                                worker, source, command.slot, command.route)) {
                            MarkFatal();
                        }
                    } else if (!stopping_.load(
                                   std::memory_order_acquire)) {
                        if (command.generation == 0U ||
                            (parked_generation != 0U &&
                             parked_generation != command.generation)) {
                            MarkFatal();
                        } else {
                            parked_generation = command.generation;
                            parked[source] = true;
                        }
                        break;
                    } else {
                        // Stop drains post-cut records and ignores fence
                        // commands; no unpublished generation can commit.
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
                WaitForWork(worker);
            }
        }
    }

    bool Append(
        std::uint32_t worker,
        std::uint8_t source,
        HistoryHandoffPool::Slot* slot,
        const InstrumentRouteTokenV1& route) noexcept {
        if (slot == nullptr || !slot->engaged) {
            return false;
        }
        RealtimeHistoryEventInputV1* const input = slot->Input();
        KLineTradeV1 kline_trade{};
        KLineTradeProjectionV1 kline_projection =
            KLineTradeProjectionV1::kNotTrade;
        if (config_.kline.enabled() && input->valid()) {
            kline_projection = ProjectKLineTradeV1(
                input->event(),
                input->ingress_sequence(),
                &kline_trade);
        }
        RealtimeHistoryAppendObservationV1 observation{};
        const bool observe = config_.append_observer != nullptr;
        bool append_start_clock_valid = false;
        if (observe) {
            observation.worker = worker;
            observation.source_slot = source;
            observation.ingress_sequence = input->ingress_sequence();
            observation.vendor_local_time_raw =
                input->vendor_local_time_raw();
            observation.recv_realtime_ns = input->recv_realtime_ns();
            observation.recv_monotonic_ns = input->recv_monotonic_ns();
            append_start_clock_valid = ReadClockNs(
                CLOCK_MONOTONIC,
                &observation.append_start_monotonic_ns);
        }
        IntradayInstrumentStoreAppendErrorV1 error =
            IntradayInstrumentStoreAppendErrorV1::kInvalidRecord;
        if (input->valid() && input->source_slot() == source &&
            route.worker == worker &&
            route.instrument_id == input->instrument_id()) {
            error = store_->Append(worker, route, std::move(*input));
        }
        if (observe &&
            error == IntradayInstrumentStoreAppendErrorV1::kNone) {
            const bool append_complete_monotonic_valid = ReadClockNs(
                CLOCK_MONOTONIC,
                &observation.append_complete_monotonic_ns);
            const bool append_complete_realtime_valid = ReadClockNs(
                CLOCK_REALTIME,
                &observation.append_complete_realtime_ns);
            observation.clock_observation_valid =
                append_start_clock_valid &&
                append_complete_monotonic_valid &&
                append_complete_realtime_valid;
        }
        KLineAppendErrorV1 kline_error = KLineAppendErrorV1::kNone;
        if (error == IntradayInstrumentStoreAppendErrorV1::kNone) {
            if (kline_projection ==
                KLineTradeProjectionV1::kTrade) {
                if (worker >= kline_aggregators_.size() ||
                    kline_aggregators_[worker] == nullptr) {
                    kline_error = KLineAppendErrorV1::kFailed;
                } else {
                    kline_error =
                        kline_aggregators_[worker]->Append(
                            kline_trade,
                            route.worker_local_row);
                }
            } else if (
                kline_projection ==
                KLineTradeProjectionV1::kInvalidTrade) {
                // Publishing a "complete" KLine while silently omitting a
                // decoded trade with invalid event time/price/quantity would
                // be a factual error. Fail the optional KLine chain closed.
                kline_error = KLineAppendErrorV1::kInvalidTrade;
            }
            if (kline_error != KLineAppendErrorV1::kNone) {
                kline_failed_.store(true, std::memory_order_release);
            }
        }
        const bool released =
            HandoffPool(source, worker).ReleaseFromConsumer(slot);
        if (observe &&
            error == IntradayInstrumentStoreAppendErrorV1::kNone) {
            config_.append_observer(
                config_.append_observer_context, observation);
        }
        if (!released) {
            store_->MarkCoverageLost();
            store_failed_.store(true, std::memory_order_release);
            return false;
        }
        if (error != IntradayInstrumentStoreAppendErrorV1::kNone) {
            store_->MarkCoverageLost();
            store_failed_.store(true, std::memory_order_release);
            return false;
        }
        if (kline_error != KLineAppendErrorV1::kNone) {
            return false;
        }
        return true;
    }

    bool FreezeAndReport(
        std::uint32_t worker,
        std::uint64_t generation) noexcept {
        try {
            if (store_->Snapshot().coverage_lost) {
                store_failed_.store(true, std::memory_order_release);
                return false;
            }
            std::unique_ptr<IntradayInstrumentStoreWorkerSliceV1> slice;
            const IntradayInstrumentStoreGenerationErrorV1 error =
                store_->CaptureWorker(worker, generation, &slice);
            if (error !=
                    IntradayInstrumentStoreGenerationErrorV1::kNone ||
                slice == nullptr) {
                store_->MarkCoverageLost();
                store_failed_.store(true, std::memory_order_release);
                return false;
            }
            std::shared_ptr<const KLineAggregatorSnapshotV1>
                kline_snapshot;
            if (config_.kline.enabled()) {
                if (worker >= kline_aggregators_.size() ||
                    kline_aggregators_[worker] == nullptr ||
                    kline_aggregators_[worker]->Capture(
                        &kline_snapshot) !=
                        KLineCaptureErrorV1::kNone ||
                    kline_snapshot == nullptr) {
                    kline_failed_.store(
                        true, std::memory_order_release);
                    return false;
                }
            }
            return ReportSlice(
                worker,
                generation,
                std::move(slice),
                std::move(kline_snapshot));
        } catch (...) {
            return false;
        }
    }

    bool ReportSlice(
        std::uint32_t worker,
        std::uint64_t generation,
        std::unique_ptr<IntradayInstrumentStoreWorkerSliceV1>
            slice,
        std::shared_ptr<const KLineAggregatorSnapshotV1>
            kline_snapshot) noexcept {
        try {
            {
                std::lock_guard<std::mutex> lock(generation_mutex_);
                if (!admission_open_.load(std::memory_order_acquire) ||
                    stopping_.load(std::memory_order_acquire) ||
                    fatal_.load(std::memory_order_acquire) ||
                    pending_ == nullptr ||
                    pending_->watermark.generation != generation ||
                    slice == nullptr ||
                    worker >= pending_->store_worker_slices.size() ||
                    pending_->store_worker_slices[worker] != nullptr ||
                    (config_.kline.enabled() &&
                     (worker >=
                          pending_->kline_worker_snapshots.size() ||
                      kline_snapshot == nullptr ||
                      pending_->kline_worker_snapshots[worker] !=
                          nullptr)) ||
                    (!config_.kline.enabled() &&
                     kline_snapshot != nullptr) ||
                    building_generation_ != 0U) {
                    return false;
                }
                pending_->store_worker_slices[worker] = std::move(slice);
                if (config_.kline.enabled()) {
                    pending_->kline_worker_snapshots[worker] =
                        std::move(kline_snapshot);
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
                std::lock_guard<std::mutex> builder_lock(builder_mutex_);
                if (builder_job_ != nullptr) {
                    return false;
                }
                building_generation_ = generation;
                builder_job_ = std::move(pending_);
            }
            // The dedicated builder owns the immutable endpoint slices from
            // here. This instrument worker immediately resumes post-cut
            // append work; no owner is held behind the O(I) generation build.
            builder_cv_.notify_one();
            return true;
        } catch (...) {
            store_->MarkCoverageLost();
            store_failed_.store(true, std::memory_order_release);
            std::lock_guard<std::mutex> lock(generation_mutex_);
            if (building_generation_ == generation) {
                building_generation_ = 0U;
            }
            MarkFatalLocked(true);
            return false;
        }
    }

    void BuilderLoop() noexcept {
        while (true) {
            std::unique_ptr<PendingGeneration> building;
            bool stop_requested = false;
            {
                std::unique_lock<std::mutex> lock(builder_mutex_);
                builder_cv_.wait(lock, [this] {
                    return builder_stop_requested_ ||
                           builder_job_ != nullptr;
                });
                stop_requested = builder_stop_requested_;
                building = std::move(builder_job_);
            }
            if (building == nullptr) {
                if (stop_requested) {
                    return;
                }
                continue;
            }
            const std::uint64_t generation =
                building->watermark.generation;
            {
                std::lock_guard<std::mutex> lock(generation_mutex_);
                if (building_generation_ != generation ||
                    !admission_open_.load(std::memory_order_acquire) ||
                    stopping_.load(std::memory_order_acquire) ||
                    fatal_.load(std::memory_order_acquire)) {
                    if (building_generation_ == generation) {
                        building_generation_ = 0U;
                    }
                    generation_cv_.notify_all();
                    if (stopping_.load(std::memory_order_acquire)) {
                        return;
                    }
                    continue;
                }
            }
            BuildAndCommit(std::move(building));
            if (stopping_.load(std::memory_order_acquire)) {
                return;
            }
        }
    }

    void BuildAndCommit(
        std::unique_ptr<PendingGeneration> building) noexcept {
        const std::uint64_t generation =
            building->watermark.generation;
        try {
            // Captured lane endpoints are immutable. The O(I) validation and
            // index build run without the history commit mutex while decoder
            // and instrument-owner queues continue routing post-cut records.
            std::shared_ptr<const IntradayInstrumentStoreGenerationV1>
                candidate;
            const IntradayInstrumentStoreGenerationErrorV1 error =
                store_->BuildGeneration(
                    building->watermark,
                    std::move(building->store_worker_slices),
                    &candidate);
            const bool build_failed =
                error != IntradayInstrumentStoreGenerationErrorV1::kNone ||
                candidate == nullptr;
            if (build_failed) {
                store_->MarkCoverageLost();
                store_failed_.store(true, std::memory_order_release);
            }
            std::shared_ptr<const RealtimeKLineGenerationV1>
                kline_candidate;
            bool kline_build_failed = false;
            if (!build_failed && config_.kline.enabled()) {
                const RealtimeKLineGenerationErrorV1 kline_error =
                    RealtimeKLineGenerationV1::Build(
                        candidate,
                        std::move(
                            building->kline_worker_snapshots),
                        config_.worker_count,
                        &kline_candidate);
                kline_build_failed =
                    kline_error !=
                        RealtimeKLineGenerationErrorV1::kNone ||
                    kline_candidate == nullptr;
                if (kline_build_failed) {
                    kline_failed_.store(
                        true, std::memory_order_release);
                }
            }

            std::lock_guard<std::mutex> lock(generation_mutex_);
            if (building_generation_ != generation) {
                return;
            }
            building_generation_ = 0U;
            if (build_failed || kline_build_failed) {
                MarkFatalLocked(build_failed);
                return;
            }
            if (!admission_open_.load(std::memory_order_acquire) ||
                stopping_.load(std::memory_order_acquire) ||
                fatal_.load(std::memory_order_acquire)) {
                generation_cv_.notify_all();
                return;
            }
            if (store_->PublishGeneration(candidate) !=
                IntradayInstrumentStoreGenerationErrorV1::kNone) {
                store_->MarkCoverageLost();
                MarkFatalLocked(true);
                return;
            }
            std::atomic_store_explicit(
                &latest_generation_, candidate,
                std::memory_order_release);
            if (config_.kline.enabled()) {
                std::atomic_store_explicit(
                    &latest_kline_generation_,
                    kline_candidate,
                    std::memory_order_release);
            }
            generation_cv_.notify_all();
        } catch (...) {
            store_->MarkCoverageLost();
            store_failed_.store(true, std::memory_order_release);
            std::lock_guard<std::mutex> lock(generation_mutex_);
            if (building_generation_ == generation) {
                building_generation_ = 0U;
            }
            MarkFatalLocked(true);
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

    void SignalWorkIfNeeded(std::uint32_t worker) noexcept {
        WorkerSignal& signal = *worker_signals_[worker];
        // Both sides deliberately use an acq_rel RMW. A load-only sleeping
        // hint plus a possibly stale ring head permits the producer and
        // waiter to observe each other's old state and lose the only wake.
        // The exchange notifies only when the worker has armed its idle gate.
        if (!signal.sleeping.exchange(false, std::memory_order_acq_rel)) {
            return;
        }
        std::lock_guard<std::mutex> lock(signal.mutex);
        signal.cv.notify_one();
    }

    void WaitForWork(std::uint32_t worker) noexcept {
        WorkerSignal& signal = *worker_signals_[worker];
        std::unique_lock<std::mutex> lock(signal.mutex);
        signal.sleeping.exchange(true, std::memory_order_acq_rel);
        if (stopping_.load(std::memory_order_acquire) ||
            !AllQueuesEmpty(worker)) {
            signal.sleeping.store(false, std::memory_order_release);
            return;
        }
        signal.cv.wait(lock, [this, &signal] {
            return stopping_.load(std::memory_order_acquire) ||
                   !signal.sleeping.load(std::memory_order_acquire);
        });
        signal.sleeping.store(false, std::memory_order_release);
    }

    void WakeAllWorkers() noexcept {
        for (const std::unique_ptr<WorkerSignal>& owned_signal :
             worker_signals_) {
            if (owned_signal == nullptr) {
                continue;
            }
            WorkerSignal& signal = *owned_signal;
            signal.sleeping.store(false, std::memory_order_release);
            std::lock_guard<std::mutex> lock(signal.mutex);
            signal.cv.notify_one();
        }
    }

    void MarkFatalLocked(bool store_failure) noexcept {
        store_->MarkCoverageLost();
        if (store_failure) {
            store_failed_.store(true, std::memory_order_release);
        }
        fatal_.store(true, std::memory_order_release);
        generation_cv_.notify_all();
    }

    void MarkFatal() noexcept {
        std::lock_guard<std::mutex> lock(generation_mutex_);
        MarkFatalLocked(false);
    }

    void StopAndDrain() noexcept {
        std::lock_guard<std::mutex> stop_lock(stop_mutex_);
        if (stop_complete_) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(generation_mutex_);
            admission_open_.store(false, std::memory_order_release);
            CloseSubmissionGates();
        }
        {
            std::unique_lock<std::mutex> lock(submission_mutex_);
            submission_cv_.wait(lock, [this] {
                return SubmissionsDrained();
            });
        }
        {
            std::lock_guard<std::mutex> lock(generation_mutex_);
            stopping_.store(true, std::memory_order_release);
        }
        {
            std::lock_guard<std::mutex> lock(builder_mutex_);
            builder_stop_requested_ = true;
        }
        WakeAllWorkers();
        builder_cv_.notify_all();
        generation_cv_.notify_all();
        for (std::thread& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        if (builder_thread_.joinable()) {
            builder_thread_.join();
        }
        stop_complete_ = true;
    }

    RealtimeHistoryRuntimeConfigV1 config_{};
    std::unique_ptr<IntradayInstrumentStoreV1> store_;
    std::vector<std::unique_ptr<KLineAggregatorV1>>
        kline_aggregators_;
    std::vector<std::unique_ptr<SpscQueue<Command>>> queues_;
    std::vector<std::unique_ptr<HistoryHandoffPool>> handoff_pools_;
    std::vector<std::unique_ptr<WorkerSignal>> worker_signals_;
    std::vector<std::thread> workers_;
    std::thread builder_thread_;

    std::atomic<bool> admission_open_{true};
    std::atomic<bool> stopping_{false};
    std::atomic<bool> fatal_{false};
    std::atomic<bool> store_failed_{false};
    std::atomic<bool> kline_failed_{false};
    std::array<SourceOwnerState, kRealtimeHistorySourceCountV1>
        source_owner_states_{};

    std::mutex stop_mutex_;
    bool stop_complete_ = false;
    std::mutex submission_mutex_;
    std::condition_variable submission_cv_;

    mutable std::mutex generation_mutex_;
    std::condition_variable generation_cv_;
    std::unique_ptr<PendingGeneration> pending_;
    std::uint64_t building_generation_ = 0U;
    std::mutex builder_mutex_;
    std::condition_variable builder_cv_;
    std::unique_ptr<PendingGeneration> builder_job_;
    bool builder_stop_requested_ = false;
    std::uint64_t last_started_generation_ = 0U;
    std::shared_ptr<const IntradayInstrumentStoreGenerationV1>
        latest_generation_;
    std::shared_ptr<const RealtimeKLineGenerationV1>
        latest_kline_generation_;
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
        config.queue_capacity_per_source_worker >
            std::numeric_limits<std::size_t>::max() - 2U) {
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
    if (config.kline.enabled() &&
        config.kline.maximum_bars == 0U) {
        const std::uint64_t window_count =
            static_cast<std::uint64_t>(
                config.kline.windows.size());
        if (config.intraday_store.maximum_session_records == 0U ||
            window_count == 0U ||
            config.intraday_store.maximum_session_records >
                std::numeric_limits<std::uint64_t>::max() /
                    window_count) {
            return RealtimeHistoryCreateErrorV1::
                kInvalidConfiguration;
        }
        config.kline.maximum_bars =
            config.intraday_store.maximum_session_records *
            window_count;
    }
    try {
        std::unique_ptr<IntradayInstrumentStoreV1> store;
        const IntradayInstrumentStoreCreateErrorV1 error =
            IntradayInstrumentStoreV1::Create(
                config.intraday_store,
                config.worker_count,
                config.source_stream_ids,
                config.registry,
                &store);
        if (error != IntradayInstrumentStoreCreateErrorV1::kNone ||
            store == nullptr) {
            return RealtimeHistoryCreateErrorV1::kStoreCreateFailed;
        }
        std::vector<std::unique_ptr<KLineAggregatorV1>>
            kline_aggregators;
        if (config.kline.enabled()) {
            std::vector<std::size_t> worker_instrument_counts(
                config.worker_count, 0U);
            for (const InstrumentRegistryEntryV1& entry :
                 config.registry->entries()) {
                ++worker_instrument_counts[
                    entry.instrument_id % config.worker_count];
            }
            kline_aggregators.reserve(config.worker_count);
            for (std::uint32_t worker = 0U;
                 worker < config.worker_count;
                 ++worker) {
                KLineAggregatorConfigV1 worker_config =
                    config.kline;
                worker_config.instrument_capacity =
                    worker_instrument_counts[worker];
                std::unique_ptr<KLineAggregatorV1> aggregator;
                if (KLineAggregatorV1::Create(
                        std::move(worker_config), &aggregator) !=
                        KLineCreateErrorV1::kNone ||
                    aggregator == nullptr) {
                    return RealtimeHistoryCreateErrorV1::
                        kKLineCreateFailed;
                }
                kline_aggregators.push_back(
                    std::move(aggregator));
            }
        }
        auto impl = std::make_unique<Impl>(
            config,
            std::move(store),
            std::move(kline_aggregators));
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
    RealtimeHistoryEventInputV1&& input) noexcept {
    return impl_->Submit(std::move(input));
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
    std::shared_ptr<const IntradayInstrumentStoreGenerationV1>* output,
    std::shared_ptr<const RealtimeKLineGenerationV1>* kline_output)
    noexcept {
    return impl_->Wait(
        generation, timeout, output, kline_output);
}

std::shared_ptr<const IntradayInstrumentStoreGenerationV1>
RealtimeHistoryRuntimeV1::AcquireLatestGeneration() const noexcept {
    return std::atomic_load_explicit(
        &impl_->latest_generation_, std::memory_order_acquire);
}

std::shared_ptr<const RealtimeKLineGenerationV1>
RealtimeHistoryRuntimeV1::AcquireLatestKLineGeneration()
    const noexcept {
    return std::atomic_load_explicit(
        &impl_->latest_kline_generation_,
        std::memory_order_acquire);
}

IntradayInstrumentStoreSnapshotV1
RealtimeHistoryRuntimeV1::StoreSnapshot() const noexcept {
    // Serialize externally observable store health with factor commit and the
    // fatal transition. An append may discover failure before its worker can
    // acquire this lock, but no runtime health observer can then overtake a
    // commit that already owns the lock.
    std::lock_guard<std::mutex> lock(impl_->generation_mutex_);
    return impl_->store_->Snapshot();
}

bool RealtimeHistoryRuntimeV1::IsGenerationCurrentAndHealthy(
    const std::shared_ptr<
        const IntradayInstrumentStoreGenerationV1>& generation) const
    noexcept {
    std::lock_guard<std::mutex> lock(impl_->generation_mutex_);
    if (generation == nullptr || impl_->fatal_.load(std::memory_order_acquire) ||
        impl_->stopping_.load(std::memory_order_acquire) ||
        impl_->store_failed_.load(std::memory_order_acquire) ||
        impl_->store_->Snapshot().coverage_lost) {
        return false;
    }
    const auto latest = std::atomic_load_explicit(
        &impl_->latest_generation_, std::memory_order_acquire);
    return latest != nullptr && latest.get() == generation.get() &&
           !latest.owner_before(generation) &&
           !generation.owner_before(latest);
}

bool RealtimeHistoryRuntimeV1::CommitIfCurrentAndHealthy(
    const std::shared_ptr<
        const IntradayInstrumentStoreGenerationV1>& generation,
    RealtimeHistoryCommitActionV1 action,
    void* context) const noexcept {
    if (generation == nullptr || action == nullptr) {
        return false;
    }
    std::lock_guard<std::mutex> lock(impl_->generation_mutex_);
    if (impl_->fatal_.load(std::memory_order_acquire) ||
        impl_->stopping_.load(std::memory_order_acquire) ||
        impl_->store_failed_.load(std::memory_order_acquire) ||
        impl_->store_->Snapshot().coverage_lost) {
        return false;
    }
    const auto latest = std::atomic_load_explicit(
        &impl_->latest_generation_, std::memory_order_acquire);
    if (latest == nullptr || latest.get() != generation.get() ||
        latest.owner_before(generation) ||
        generation.owner_before(latest)) {
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

RealtimeHistoryGenerationErrorV1
RealtimeHistoryRuntimeV1::FailureError() const noexcept {
    return impl_->fatal_.load(std::memory_order_acquire)
               ? impl_->FatalGenerationError()
               : RealtimeHistoryGenerationErrorV1::kNone;
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
