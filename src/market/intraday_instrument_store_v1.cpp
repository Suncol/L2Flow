#include "l2flow/market/intraday_instrument_store_v1.h"

#include "l2flow/market/instrument_registry.h"
#include "l2flow/market/market_types_v1.h"
#include "l2flow/market/realtime_history_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace l2flow::market {
namespace {

static_assert(
    kIntradayInstrumentStoreSourceCountV1 ==
    kRealtimeHistorySourceCountV1);
static_assert(std::is_nothrow_move_constructible_v<ShanghaiSnapshotV1>);
static_assert(std::is_nothrow_move_constructible_v<ShanghaiTickV1>);
static_assert(std::is_nothrow_move_constructible_v<ShenzhenSnapshotV1>);
static_assert(std::is_nothrow_move_constructible_v<ShenzhenOrderV1>);
static_assert(
    std::is_nothrow_move_constructible_v<ShenzhenTransactionV1>);
static_assert(std::is_nothrow_move_constructible_v<DecodedMarketEventV1>);

[[nodiscard]] constexpr std::size_t Maximum(
    std::size_t left,
    std::size_t right) noexcept {
    return left > right ? left : right;
}

inline constexpr std::size_t kArenaStorageAlignment = Maximum(
    alignof(RealtimeHistoryRecordV1),
    Maximum(
        alignof(ShanghaiSnapshotV1),
        Maximum(
            alignof(ShanghaiTickV1),
            Maximum(
                alignof(ShenzhenSnapshotV1),
                Maximum(
                    alignof(ShenzhenOrderV1),
                    alignof(ShenzhenTransactionV1))))));

static_assert(
    kArenaStorageAlignment <= alignof(std::max_align_t),
    "arena payload alternatives must be supported by ordinary operator new");

[[nodiscard]] bool ValidDirection(
    IntradayInstrumentScanDirectionV1 direction) noexcept {
    switch (direction) {
        case IntradayInstrumentScanDirectionV1::kOldestFirst:
        case IntradayInstrumentScanDirectionV1::kNewestFirst:
            return true;
    }
    return false;
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

[[nodiscard]] bool TickStreamSequenceConsistent(
    MarketEventKindV1 kind,
    std::uint64_t ingress_sequence,
    std::uint64_t tick_stream_sequence) noexcept {
    if (IsSnapshotEventKindV1(kind)) {
        return tick_stream_sequence == 0U;
    }
    return IsTickEventKindV1(kind) &&
           tick_stream_sequence !=
               std::numeric_limits<std::uint64_t>::max() &&
           tick_stream_sequence <= ingress_sequence;
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

[[nodiscard]] bool CheckedAddSize(
    std::size_t left,
    std::size_t right,
    std::size_t* output) noexcept {
    if (output == nullptr ||
        left > std::numeric_limits<std::size_t>::max() - right) {
        return false;
    }
    *output = left + right;
    return true;
}

[[nodiscard]] constexpr std::size_t AlignUp(
    std::size_t value,
    std::size_t alignment) noexcept {
    return (value + alignment - 1U) & ~(alignment - 1U);
}

[[nodiscard]] constexpr std::size_t AlignDown(
    std::size_t value,
    std::size_t alignment) noexcept {
    return value & ~(alignment - 1U);
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

std::atomic<std::uint64_t> g_next_session_epoch{1U};

[[nodiscard]] std::uint64_t AcquireSessionEpoch() noexcept {
    std::uint64_t current =
        g_next_session_epoch.load(std::memory_order_relaxed);
    for (;;) {
        if (current == 0U ||
            current == std::numeric_limits<std::uint64_t>::max()) {
            return 0U;
        }
        if (g_next_session_epoch.compare_exchange_weak(
                current,
                current + 1U,
                std::memory_order_relaxed,
                std::memory_order_relaxed)) {
            return current;
        }
    }
}

struct ArenaSegment;

struct ArenaSegmentDeleter final {
    void operator()(ArenaSegment* segment) const noexcept;
};

using ArenaSegmentOwner =
    std::unique_ptr<ArenaSegment, ArenaSegmentDeleter>;

struct alignas(kArenaStorageAlignment) ArenaSegment final {
    ArenaSegment(
        std::size_t value_allocation_bytes,
        std::size_t value_storage_capacity) noexcept
        : allocation_bytes(value_allocation_bytes),
          payload_frontier(value_storage_capacity) {}

    ArenaSegmentOwner owned_next;
    ArenaSegment* previous = nullptr;
    std::size_t allocation_bytes = 0U;
    std::size_t payload_frontier = 0U;
    std::size_t header_count = 0U;
};

[[nodiscard]] constexpr std::size_t ArenaStorageOffset() noexcept {
    return AlignUp(sizeof(ArenaSegment), kArenaStorageAlignment);
}

[[nodiscard]] std::byte* ArenaStorage(
    ArenaSegment* segment) noexcept {
    return reinterpret_cast<std::byte*>(segment) + ArenaStorageOffset();
}

[[nodiscard]] const std::byte* ArenaStorage(
    const ArenaSegment* segment) noexcept {
    return reinterpret_cast<const std::byte*>(segment) +
           ArenaStorageOffset();
}

[[nodiscard]] RealtimeHistoryRecordV1* SegmentRecord(
    ArenaSegment* segment,
    std::size_t index) noexcept {
    return std::launder(
        reinterpret_cast<RealtimeHistoryRecordV1*>(
            ArenaStorage(segment) +
            index * sizeof(RealtimeHistoryRecordV1)));
}

[[nodiscard]] const RealtimeHistoryRecordV1* SegmentRecord(
    const ArenaSegment* segment,
    std::size_t index) noexcept {
    return std::launder(
        reinterpret_cast<const RealtimeHistoryRecordV1*>(
            ArenaStorage(segment) +
            index * sizeof(RealtimeHistoryRecordV1)));
}

void ArenaSegmentDeleter::operator()(
    ArenaSegment* segment) const noexcept {
    // The chain can be arbitrarily long for a hot symbol. Detach each link so
    // destruction remains iterative rather than recursing through unique_ptr.
    while (segment != nullptr) {
        ArenaSegment* const next = segment->owned_next.release();
        for (std::size_t index = 0U;
             index < segment->header_count;
             ++index) {
            std::destroy_at(SegmentRecord(segment, index));
        }
        segment->~ArenaSegment();
        ::operator delete(static_cast<void*>(segment));
        segment = next;
    }
}

[[nodiscard]] ArenaSegmentOwner AllocateArenaSegment(
    std::size_t allocation_bytes) {
    const std::size_t storage_offset = ArenaStorageOffset();
    if (allocation_bytes <= storage_offset) {
        throw std::bad_alloc();
    }
    void* const allocation = ::operator new(allocation_bytes);
    return ArenaSegmentOwner(
        ::new (allocation) ArenaSegment(
            allocation_bytes,
            allocation_bytes - storage_offset));
}

struct PayloadLayout final {
    std::size_t size = 0U;
    std::size_t alignment = 0U;
};

[[nodiscard]] PayloadLayout PayloadLayoutForKind(
    MarketEventKindV1 kind) noexcept {
    switch (kind) {
        case MarketEventKindV1::kShanghaiSnapshot:
            return PayloadLayout{
                sizeof(ShanghaiSnapshotV1),
                alignof(ShanghaiSnapshotV1)};
        case MarketEventKindV1::kShanghaiTick:
            return PayloadLayout{
                sizeof(ShanghaiTickV1),
                alignof(ShanghaiTickV1)};
        case MarketEventKindV1::kShenzhenSnapshot:
            return PayloadLayout{
                sizeof(ShenzhenSnapshotV1),
                alignof(ShenzhenSnapshotV1)};
        case MarketEventKindV1::kShenzhenOrder:
            return PayloadLayout{
                sizeof(ShenzhenOrderV1),
                alignof(ShenzhenOrderV1)};
        case MarketEventKindV1::kShenzhenTransaction:
            return PayloadLayout{
                sizeof(ShenzhenTransactionV1),
                alignof(ShenzhenTransactionV1)};
    }
    return {};
}

struct ArenaPlacement final {
    RealtimeHistoryRecordV1* header = nullptr;
    std::byte* payload = nullptr;
    std::size_t payload_frontier = 0U;
    std::uint32_t payload_delta = 0U;
};

[[nodiscard]] bool PlanArenaPlacement(
    ArenaSegment* segment,
    PayloadLayout payload,
    ArenaPlacement* output) noexcept {
    if (segment == nullptr || output == nullptr || payload.size == 0U ||
        payload.alignment == 0U ||
        (payload.alignment & (payload.alignment - 1U)) != 0U ||
        payload.size > segment->payload_frontier ||
        segment->header_count >
            std::numeric_limits<std::size_t>::max() /
                sizeof(RealtimeHistoryRecordV1)) {
        return false;
    }
    const std::size_t header_offset =
        segment->header_count * sizeof(RealtimeHistoryRecordV1);
    std::size_t header_end = 0U;
    if (!CheckedAddSize(
            header_offset,
            sizeof(RealtimeHistoryRecordV1),
            &header_end)) {
        return false;
    }
    const std::size_t payload_begin = AlignDown(
        segment->payload_frontier - payload.size,
        payload.alignment);
    if (header_end > payload_begin) {
        return false;
    }
    std::byte* const storage = ArenaStorage(segment);
    std::byte* const header_address = storage + header_offset;
    std::byte* const payload_address = storage + payload_begin;
    const std::size_t delta =
        static_cast<std::size_t>(payload_address - header_address);
    if (delta == 0U ||
        delta > std::numeric_limits<std::uint32_t>::max()) {
        return false;
    }
    *output = ArenaPlacement{
        reinterpret_cast<RealtimeHistoryRecordV1*>(header_address),
        payload_address,
        payload_begin,
        static_cast<std::uint32_t>(delta)};
    return true;
}

[[nodiscard]] bool RequiredSegmentAllocation(
    PayloadLayout payload,
    std::size_t target_bytes,
    std::size_t* output) noexcept {
    if (output == nullptr || payload.size == 0U ||
        payload.alignment == 0U) {
        return false;
    }
    std::size_t required = ArenaStorageOffset();
    if (!CheckedAddSize(
            required, sizeof(RealtimeHistoryRecordV1), &required) ||
        !CheckedAddSize(required, payload.size, &required) ||
        !CheckedAddSize(
            required, payload.alignment - 1U, &required)) {
        return false;
    }
    required = Maximum(required, target_bytes);
    if (required >
        kIntradayInstrumentStoreMaximumSegmentBytesV1) {
        return false;
    }
    *output = required;
    return true;
}

struct MutableLane final {
    MutableLane() = default;
    MutableLane(const MutableLane&) = delete;
    MutableLane& operator=(const MutableLane&) = delete;
    MutableLane(MutableLane&&) noexcept = default;
    MutableLane& operator=(MutableLane&&) noexcept = default;
    ~MutableLane() = default;

    ArenaSegmentOwner owned_head;
    ArenaSegment* head = nullptr;
    ArenaSegment* tail = nullptr;
    std::uint64_t record_count = 0U;
    std::uint64_t accounted_bytes = 0U;
    std::uint64_t allocated_segment_bytes = 0U;
    std::uint64_t segment_count = 0U;
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
    MutableInstrumentRow& operator=(MutableInstrumentRow&&) noexcept =
        default;

    std::uint32_t instrument_id = 0U;
    std::size_t ordinal = 0U;
    std::array<MutableLane, kIntradayInstrumentStoreSourceCountV1> lanes;
    const RealtimeHistoryRecordV1* latest_snapshot = nullptr;
    const RealtimeHistoryRecordV1* latest_tick = nullptr;
};

struct alignas(64) WorkerAccounting final {
    // Credits are normally touched by this worker only. Another worker may
    // atomically reclaim unused credits on the rare global-cap slow path.
    std::atomic<std::uint64_t> record_credits{0U};
    std::atomic<std::uint64_t> byte_credits{0U};
    std::atomic<std::uint64_t> appended_records{0U};
    std::atomic<std::uint64_t> accounted_record_bytes{0U};
    std::atomic<std::uint64_t> allocated_segment_bytes{0U};
    std::atomic<std::uint64_t> allocated_segments{0U};
};

struct WorkerState final {
    std::vector<MutableInstrumentRow> rows;
    std::uint64_t last_captured_generation = 0U;
    WorkerAccounting accounting;
};

struct OrdinalEntry final {
    std::uint32_t instrument_id = 0U;
    std::uint32_t worker = 0U;
    std::size_t local_index = 0U;
};

struct CapturedLane final {
    const ArenaSegment* head = nullptr;
    const ArenaSegment* tail = nullptr;
    std::size_t tail_used = 0U;
    std::uint64_t record_count = 0U;
    std::uint64_t accounted_bytes = 0U;
    std::uint64_t allocated_segment_bytes = 0U;
    std::uint64_t segment_count = 0U;
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
    std::uint64_t session_epoch = 0U;
    std::array<std::uint32_t, kIntradayInstrumentStoreSourceCountV1>
        source_stream_ids{};
    std::vector<std::unique_ptr<WorkerState>> workers;
    // Exact registry-ordinal order: ascending instrument_id.
    std::vector<OrdinalEntry> ordinals;
    // These authorities include both consumed quota and outstanding
    // worker-local credits. Hot appends consume local credits; the global
    // atomics and quota mutex are touched only on block refill/reclaim.
    std::atomic<std::uint64_t> issued_record_quota{0U};
    std::atomic<std::uint64_t> issued_byte_quota{0U};
    mutable std::mutex quota_mutex;
    std::atomic<std::uint64_t> failed_appends{0U};
    std::atomic<std::uint64_t> latest_generation{0U};
    std::atomic<bool> coverage_lost{false};
    std::uint64_t base_index_bytes = 0U;
    mutable std::mutex generation_mutex;
};

enum class QuotaKind : std::uint8_t {
    kRecords = 0U,
    kBytes,
};

inline constexpr std::uint64_t kRecordQuotaBlock = 4096U;
inline constexpr std::uint64_t kByteQuotaBlock = 4U * 1024U * 1024U;

[[nodiscard]] std::atomic<std::uint64_t>& WorkerCredits(
    WorkerState& worker,
    QuotaKind kind) noexcept {
    return kind == QuotaKind::kRecords
               ? worker.accounting.record_credits
               : worker.accounting.byte_credits;
}

[[nodiscard]] bool TryConsumeCredits(
    std::atomic<std::uint64_t>* credits,
    std::uint64_t amount) noexcept {
    std::uint64_t current = credits->load(std::memory_order_relaxed);
    while (current >= amount) {
        if (credits->compare_exchange_weak(
                current,
                current - amount,
                std::memory_order_relaxed,
                std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

[[nodiscard]] bool AcquireQuota(
    SessionState& session,
    WorkerState& worker,
    QuotaKind kind,
    std::uint64_t amount) noexcept {
    std::atomic<std::uint64_t>& local = WorkerCredits(worker, kind);
    if (amount == 0U || TryConsumeCredits(&local, amount)) {
        return true;
    }

    std::lock_guard<std::mutex> lock(session.quota_mutex);
    if (TryConsumeCredits(&local, amount)) {
        return true;
    }
    std::atomic<std::uint64_t>& issued =
        kind == QuotaKind::kRecords
            ? session.issued_record_quota
            : session.issued_byte_quota;
    const std::uint64_t maximum =
        kind == QuotaKind::kRecords
            ? session.config.maximum_session_records
            : session.config.maximum_session_accounted_bytes;
    const std::uint64_t block =
        kind == QuotaKind::kRecords ? kRecordQuotaBlock : kByteQuotaBlock;

    std::uint64_t issued_value =
        issued.load(std::memory_order_relaxed);
    if (issued_value > maximum) {
        return false;
    }
    if (maximum - issued_value < amount) {
        // Credits can be stranded on idle workers near a hard cap. Atomic
        // exchange makes a concurrent owner either consume a credit or yield
        // it, never both.
        std::uint64_t reclaimed = 0U;
        for (const std::unique_ptr<WorkerState>& candidate :
             session.workers) {
            const std::uint64_t value =
                WorkerCredits(*candidate, kind).exchange(
                    0U, std::memory_order_acq_rel);
            if (value >
                std::numeric_limits<std::uint64_t>::max() - reclaimed) {
                return false;
            }
            reclaimed += value;
        }
        if (reclaimed > issued_value) {
            return false;
        }
        issued_value -= reclaimed;
        issued.store(issued_value, std::memory_order_relaxed);
    }
    if (issued_value > maximum ||
        maximum - issued_value < amount) {
        return false;
    }
    const std::uint64_t desired =
        amount > block ? amount : block;
    const std::uint64_t grant =
        std::min(desired, maximum - issued_value);
    issued.store(issued_value + grant, std::memory_order_relaxed);
    local.fetch_add(grant, std::memory_order_relaxed);
    return TryConsumeCredits(&local, amount);
}

void ReturnQuota(
    WorkerState& worker,
    QuotaKind kind,
    std::uint64_t amount) noexcept {
    WorkerCredits(worker, kind).fetch_add(
        amount, std::memory_order_relaxed);
}

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
    std::uint8_t source,
    const RealtimeHistoryWatermarkV1& watermark) noexcept {
    if (lane.record_count == 0U) {
        return lane.head == nullptr && lane.tail == nullptr &&
               lane.tail_used == 0U && lane.accounted_bytes == 0U &&
               lane.allocated_segment_bytes == 0U &&
               lane.segment_count == 0U;
    }
    if (lane.head == nullptr || lane.tail == nullptr ||
        lane.tail_used == 0U ||
        lane.accounted_bytes == 0U ||
        lane.allocated_segment_bytes == 0U ||
        lane.segment_count == 0U ||
        lane.segment_count > lane.record_count) {
        return false;
    }
    const RealtimeHistoryRecordV1* const last =
        SegmentRecord(lane.tail, lane.tail_used - 1U);
    return last->source_slot() == source &&
           last->source_stream_id() ==
               watermark.sources[source].source_stream_id &&
           last->source_sequence() <
               watermark.sources[source].sequence_exclusive &&
           last->ingress_sequence() <
               watermark.ingress_sequence_exclusive;
}

struct LanePosition final {
    CapturedLane endpoint{};
    const ArenaSegment* segment = nullptr;
    std::size_t index = 0U;
    std::uint64_t remaining = 0U;
};

[[nodiscard]] std::size_t UsedInPositionSegment(
    const LanePosition& position) noexcept {
    if (position.segment == nullptr) {
        return 0U;
    }
    return position.segment == position.endpoint.tail
               ? position.endpoint.tail_used
               : position.segment->header_count;
}

class MergedInstrumentReader final {
public:
    MergedInstrumentReader(
        const CapturedInstrumentRow& row,
        IntradayInstrumentScanOptionsV1 options) noexcept
        : options_(options) {
        for (std::size_t source = 0U; source < positions_.size(); ++source) {
            LanePosition& position = positions_[source];
            position.endpoint = row.lanes[source];
            position.remaining = position.endpoint.record_count;
            if (options_.direction ==
                IntradayInstrumentScanDirectionV1::kOldestFirst) {
                position.segment = position.endpoint.head;
                position.index = 0U;
            } else {
                position.segment = position.endpoint.tail;
                position.index = position.endpoint.tail_used;
            }
            current_[source] = Normalize(position);
        }
        RefreshState();
    }

    [[nodiscard]] bool Pop(
        const RealtimeHistoryRecordV1** output) noexcept {
        if (output == nullptr || done_) {
            return false;
        }
        const std::size_t selected = SelectSource();
        if (selected >= current_.size()) {
            done_ = true;
            return false;
        }

        *output = current_[selected];
        Advance(positions_[selected]);
        ++emitted_;
        current_[selected] = Normalize(positions_[selected]);
        RefreshState();
        return true;
    }

    [[nodiscard]] bool done() const noexcept { return done_; }

private:
    [[nodiscard]] const RealtimeHistoryRecordV1* Current(
        const LanePosition& position) const noexcept {
        if (position.remaining == 0U || position.segment == nullptr) {
            return nullptr;
        }
        if (options_.direction ==
            IntradayInstrumentScanDirectionV1::kOldestFirst) {
            return position.index < UsedInPositionSegment(position)
                       ? SegmentRecord(position.segment, position.index)
                       : nullptr;
        }
        return position.index != 0U
                   ? SegmentRecord(position.segment, position.index - 1U)
                   : nullptr;
    }

    void Advance(LanePosition& position) noexcept {
        if (position.remaining == 0U || position.segment == nullptr) {
            return;
        }
        --position.remaining;
        if (options_.direction ==
            IntradayInstrumentScanDirectionV1::kOldestFirst) {
            ++position.index;
            if (position.remaining != 0U &&
                position.index >= UsedInPositionSegment(position)) {
                position.segment = position.segment->owned_next.get();
                position.index = 0U;
            }
            return;
        }

        --position.index;
        if (position.remaining != 0U && position.index == 0U) {
            position.segment = position.segment->previous;
            position.index =
                position.segment == nullptr
                    ? 0U
                    : position.segment->header_count;
        }
    }

    [[nodiscard]] const RealtimeHistoryRecordV1* Normalize(
        LanePosition& position) noexcept {
        while (position.remaining != 0U) {
            const RealtimeHistoryRecordV1* const record =
                Current(position);
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
                if (ingress >=
                    options_.ingress_sequence_end_exclusive) {
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

    [[nodiscard]] std::size_t SelectSource() const noexcept {
        if (active_count_ == 0U) {
            return current_.size();
        }
        if (active_count_ == 1U) {
            return active_sources_[0U];
        }
        if (active_count_ == 2U) {
            const std::size_t first = active_sources_[0U];
            const std::size_t second = active_sources_[1U];
            return Better(current_[second], current_[first])
                       ? second
                       : first;
        }
        std::size_t selected = active_sources_[0U];
        for (std::size_t index = 1U; index < active_count_; ++index) {
            const std::size_t candidate = active_sources_[index];
            if (Better(current_[candidate], current_[selected])) {
                selected = candidate;
            }
        }
        return selected;
    }

    void RefreshState() noexcept {
        active_count_ = 0U;
        for (std::size_t source = 0U; source < current_.size(); ++source) {
            if (current_[source] != nullptr) {
                active_sources_[active_count_] = source;
                ++active_count_;
            }
        }
        done_ = emitted_ >= options_.maximum_records ||
                active_count_ == 0U;
    }

    IntradayInstrumentScanOptionsV1 options_{};
    std::array<LanePosition, kIntradayInstrumentStoreSourceCountV1>
        positions_{};
    std::array<const RealtimeHistoryRecordV1*,
               kIntradayInstrumentStoreSourceCountV1>
        current_{};
    std::array<std::size_t, kIntradayInstrumentStoreSourceCountV1>
        active_sources_{};
    std::size_t active_count_ = 0U;
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
          reader(row, options) {}

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
                instrument_options);
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

std::uint64_t
IntradayInstrumentStoreGenerationV1::store_session_epoch()
    const noexcept {
    return impl_->data->session->session_epoch;
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
        auto cursor_impl =
            std::make_unique<IntradayInstrumentCursorV1::Impl>(
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
    std::array<std::uint32_t,
               kIntradayInstrumentStoreSourceCountV1>
        source_stream_ids,
    const InstrumentRegistryV1* registry,
    std::unique_ptr<IntradayInstrumentStoreV1>* output) noexcept {
    if (output == nullptr) {
        return IntradayInstrumentStoreCreateErrorV1::kNullOutput;
    }
    output->reset();
    bool valid_source_ids = true;
    for (std::size_t left = 0U; left < source_stream_ids.size(); ++left) {
        valid_source_ids =
            valid_source_ids && source_stream_ids[left] != 0U;
        for (std::size_t right = left + 1U;
             right < source_stream_ids.size();
             ++right) {
            valid_source_ids =
                valid_source_ids &&
                source_stream_ids[left] != source_stream_ids[right];
        }
    }
    if (worker_count == 0U || worker_count > 256U ||
        registry == nullptr || registry->empty() ||
        !valid_source_ids ||
        config.segment_target_bytes <
            kIntradayInstrumentStoreMinimumSegmentBytesV1 ||
        config.segment_target_bytes >
            kIntradayInstrumentStoreMaximumSegmentBytesV1 ||
        config.maximum_records_per_batch == 0U ||
        config.maximum_records_per_batch >
            kIntradayInstrumentStoreMaximumBatchRecordsV1 ||
        config.maximum_session_records == 0U ||
        config.maximum_session_accounted_bytes == 0U) {
        return IntradayInstrumentStoreCreateErrorV1::
            kInvalidConfiguration;
    }

    const std::uint64_t session_epoch = AcquireSessionEpoch();
    if (session_epoch == 0U) {
        return IntradayInstrumentStoreCreateErrorV1::
            kResourceExhausted;
    }

    try {
        auto session = std::make_shared<SessionState>();
        session->config = config;
        session->worker_count = worker_count;
        session->registry = registry;
        session->registry_version = registry->registry_version();
        session->registry_sha256 = registry->registry_sha256();
        session->session_epoch = session_epoch;
        session->source_stream_ids = source_stream_ids;
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
        for (std::size_t registry_ordinal = 0U;
             registry_ordinal < session->ordinals.size();
             ++registry_ordinal) {
            const OrdinalEntry& ordinal =
                session->ordinals[registry_ordinal];
            const InstrumentRegistryLookupResultV1 lookup =
                registry->LookupById(ordinal.instrument_id);
            if (!lookup.known() ||
                lookup.registry_ordinal != registry_ordinal) {
                return IntradayInstrumentStoreCreateErrorV1::
                    kInvalidConfiguration;
            }
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
        session->issued_byte_quota.store(
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

IntradayInstrumentStoreQueryErrorV1
IntradayInstrumentStoreV1::ResolveRouteToken(
    std::size_t registry_ordinal,
    std::uint32_t instrument_id,
    InstrumentRouteTokenV1* output) const noexcept {
    if (output == nullptr) {
        return IntradayInstrumentStoreQueryErrorV1::kNullOutput;
    }
    *output = InstrumentRouteTokenV1{};
    if (instrument_id == 0U) {
        return IntradayInstrumentStoreQueryErrorV1::kInvalidArgument;
    }
    const SessionState& session = *impl_->session;
    if (registry_ordinal >= session.ordinals.size() ||
        session.ordinals[registry_ordinal].instrument_id !=
            instrument_id) {
        return IntradayInstrumentStoreQueryErrorV1::kNotFound;
    }
    const OrdinalEntry& entry = session.ordinals[registry_ordinal];
    *output = InstrumentRouteTokenV1{
        entry.instrument_id,
        registry_ordinal,
        entry.worker,
        entry.local_index,
        session.session_epoch};
    return IntradayInstrumentStoreQueryErrorV1::kNone;
}

IntradayInstrumentStoreAppendErrorV1
IntradayInstrumentStoreV1::Append(
    std::uint32_t worker,
    const InstrumentRouteTokenV1& route,
    RealtimeHistoryEventInputV1&& input) noexcept {
    return Append(worker, route, std::move(input), nullptr);
}

IntradayInstrumentStoreAppendErrorV1
IntradayInstrumentStoreV1::Append(
    std::uint32_t worker,
    const InstrumentRouteTokenV1& route,
    RealtimeHistoryEventInputV1&& input,
    const RealtimeHistoryRecordV1** appended_record) noexcept {
    if (appended_record != nullptr) {
        *appended_record = nullptr;
    }
    SessionState& session = *impl_->session;
    if (session.coverage_lost.load(std::memory_order_acquire)) {
        return IntradayInstrumentStoreAppendErrorV1::kCoverageLost;
    }
    if (!input.valid() ||
        input.source_slot() >=
            kIntradayInstrumentStoreSourceCountV1 ||
        input.source_stream_id() == 0U ||
        input.source_sequence() == 0U ||
        input.source_sequence() ==
            std::numeric_limits<std::uint64_t>::max() ||
        input.ingress_sequence() == 0U ||
        input.ingress_sequence() ==
            std::numeric_limits<std::uint64_t>::max() ||
        input.instrument_id() == 0U ||
        input.registry_ordinal() ==
            std::numeric_limits<std::size_t>::max() ||
        input.accounted_record_bytes() == 0U ||
        !KindBelongsToSource(input.kind(), input.source_slot()) ||
        !TickStreamSequenceConsistent(
            input.kind(),
            input.ingress_sequence(),
            input.tick_stream_sequence()) ||
        input.source_stream_id() !=
            session.source_stream_ids[input.source_slot()] ||
        route.session_epoch != session.session_epoch ||
        route.registry_ordinal != input.registry_ordinal() ||
        route.instrument_id != input.instrument_id() ||
        route.registry_ordinal >= session.ordinals.size()) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::kInvalidRecord);
    }
    if (worker >= session.worker_count ||
        route.worker != worker) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::kWrongWorker);
    }

    const OrdinalEntry& ordinal =
        session.ordinals[route.registry_ordinal];
    if (ordinal.instrument_id != route.instrument_id ||
        ordinal.worker != route.worker ||
        ordinal.local_index != route.worker_local_row) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::kInvalidRecord);
    }
    WorkerState& worker_state = *session.workers[worker];
    if (route.worker_local_row >= worker_state.rows.size()) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::kWrongWorker);
    }
    MutableInstrumentRow& row =
        worker_state.rows[route.worker_local_row];
    if (row.instrument_id != input.instrument_id() ||
        row.ordinal != input.registry_ordinal()) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::kInvalidRecord);
    }
    MutableLane& lane = row.lanes[input.source_slot()];
    if (lane.record_count != 0U &&
        (input.source_sequence() <= lane.last_source_sequence ||
         input.ingress_sequence() <= lane.last_ingress_sequence)) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::
                kSequenceNotIncreasing);
    }

    const PayloadLayout payload = PayloadLayoutForKind(input.kind());
    ArenaPlacement placement{};
    const bool needs_segment =
        lane.tail == nullptr ||
        !PlanArenaPlacement(lane.tail, payload, &placement);
    std::size_t segment_allocation_bytes = 0U;
    if (needs_segment &&
        !RequiredSegmentAllocation(
            payload,
            session.config.segment_target_bytes,
            &segment_allocation_bytes)) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::
                kResourceExhausted);
    }

    std::uint64_t byte_reservation =
        input.accounted_record_bytes();
    if (needs_segment &&
        !CheckedAdd(
            byte_reservation,
            static_cast<std::uint64_t>(segment_allocation_bytes),
            &byte_reservation)) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::kByteCapacity);
    }
    if (!AcquireQuota(
            session, worker_state, QuotaKind::kRecords, 1U)) {
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::kRecordCapacity);
    }
    if (!AcquireQuota(
            session,
            worker_state,
            QuotaKind::kBytes,
            byte_reservation)) {
        ReturnQuota(worker_state, QuotaKind::kRecords, 1U);
        return impl_->FailAppend(
            IntradayInstrumentStoreAppendErrorV1::kByteCapacity);
    }

    ArenaSegmentOwner candidate;
    if (needs_segment) {
        try {
            candidate =
                AllocateArenaSegment(segment_allocation_bytes);
        } catch (...) {
            ReturnQuota(
                worker_state, QuotaKind::kBytes, byte_reservation);
            ReturnQuota(worker_state, QuotaKind::kRecords, 1U);
            return impl_->FailAppend(
                IntradayInstrumentStoreAppendErrorV1::
                    kResourceExhausted);
        }
        if (!PlanArenaPlacement(
                candidate.get(), payload, &placement)) {
            ReturnQuota(
                worker_state, QuotaKind::kBytes, byte_reservation);
            ReturnQuota(worker_state, QuotaKind::kRecords, 1U);
            return impl_->FailAppend(
                IntradayInstrumentStoreAppendErrorV1::
                    kResourceExhausted);
        }
    }

    const std::uint8_t source_slot = input.source_slot();
    const std::uint32_t source_stream_id =
        input.source_stream_id();
    const std::uint64_t source_sequence =
        input.source_sequence();
    const std::uint64_t ingress_sequence =
        input.ingress_sequence();
    const std::uint64_t tick_stream_sequence =
        input.tick_stream_sequence();
    const std::uint32_t instrument_id = input.instrument_id();
    const MarketEventKindV1 kind = input.kind();
    const std::int64_t event_time_ns = input.event_time_ns();
    const std::int64_t recv_realtime_ns =
        input.recv_realtime_ns();
    const std::int64_t recv_monotonic_ns =
        input.recv_monotonic_ns();
    const std::uint64_t accounted_record_bytes =
        input.accounted_record_bytes();

    std::visit(
        [&placement](auto&& value) noexcept {
            using Event = std::decay_t<decltype(value)>;
            static_assert(
                std::is_nothrow_move_constructible_v<Event>);
            ::new (static_cast<void*>(placement.payload))
                Event(std::move(value));
        },
        std::move(input).TakeEvent());
    ::new (static_cast<void*>(placement.header))
        RealtimeHistoryRecordV1(
            source_slot,
            source_stream_id,
            source_sequence,
            ingress_sequence,
            tick_stream_sequence,
            instrument_id,
            kind,
            event_time_ns,
            recv_realtime_ns,
            recv_monotonic_ns,
            placement.payload_delta);

    ArenaSegment* const appended_segment =
        needs_segment ? candidate.get() : lane.tail;
    appended_segment->payload_frontier =
        placement.payload_frontier;
    ++appended_segment->header_count;
    if (needs_segment) {
        candidate->previous = lane.tail;
        ArenaSegment* const raw = candidate.get();
        if (lane.tail == nullptr) {
            lane.head = raw;
            lane.owned_head = std::move(candidate);
        } else {
            lane.tail->owned_next = std::move(candidate);
        }
        lane.tail = raw;
        lane.allocated_segment_bytes +=
            static_cast<std::uint64_t>(segment_allocation_bytes);
        ++lane.segment_count;
        worker_state.accounting.allocated_segment_bytes.fetch_add(
            static_cast<std::uint64_t>(segment_allocation_bytes),
            std::memory_order_relaxed);
        worker_state.accounting.allocated_segments.fetch_add(
            1U, std::memory_order_relaxed);
    }

    ++lane.record_count;
    lane.accounted_bytes += accounted_record_bytes;
    lane.last_ingress_sequence = ingress_sequence;
    lane.last_source_sequence = source_sequence;

    const RealtimeHistoryRecordV1* const appended =
        placement.header;
    const RealtimeHistoryRecordV1*& latest =
        IsSnapshotEventKindV1(kind) ? row.latest_snapshot : row.latest_tick;
    if (latest == nullptr ||
        latest->ingress_sequence() < ingress_sequence) {
        latest = appended;
    }
    worker_state.accounting.appended_records.fetch_add(
        1U, std::memory_order_release);
    worker_state.accounting.accounted_record_bytes.fetch_add(
        accounted_record_bytes, std::memory_order_release);
    if (appended_record != nullptr) {
        *appended_record = appended;
    }
    return IntradayInstrumentStoreAppendErrorV1::kNone;
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
                    lane.tail == nullptr
                        ? 0U
                        : lane.tail->header_count,
                    lane.record_count,
                    lane.accounted_bytes,
                    lane.allocated_segment_bytes,
                    lane.segment_count};
            }
            slice.rows.push_back(captured);
        }
        auto slice_impl =
            std::make_unique<
                IntradayInstrumentStoreWorkerSliceV1::Impl>(
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
    for (std::size_t source = 0U;
         source < session.source_stream_ids.size();
         ++source) {
        if (watermark.sources[source].source_stream_id !=
            session.source_stream_ids[source]) {
            return IntradayInstrumentStoreGenerationErrorV1::
                kInvalidWatermark;
        }
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
        std::uint64_t total_segment_bytes = 0U;

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
                            lane.accounted_bytes,
                            &total_bytes) ||
                        !CheckedAdd(
                            total_segment_bytes,
                            lane.allocated_segment_bytes,
                            &total_segment_bytes)) {
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
        }
        std::uint64_t generation_index_bytes = 0U;
        if (!CheckedAdd(
                session.base_index_bytes,
                total_segment_bytes,
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
            built(
                new IntradayInstrumentStoreGenerationV1(
                    std::move(generation_impl)));
        *output = std::move(built);
        return IntradayInstrumentStoreGenerationErrorV1::kNone;
    } catch (...) {
        output->reset();
        return IntradayInstrumentStoreGenerationErrorV1::
            kResourceExhausted;
    }
}

IntradayInstrumentStoreGenerationErrorV1
IntradayInstrumentStoreV1::PublishGeneration(
    const std::shared_ptr<
        const IntradayInstrumentStoreGenerationV1>& generation)
    noexcept {
    if (generation == nullptr || generation->impl_ == nullptr) {
        return IntradayInstrumentStoreGenerationErrorV1::
            kInvalidGeneration;
    }
    SessionState& session = *impl_->session;
    if (session.coverage_lost.load(std::memory_order_acquire)) {
        return IntradayInstrumentStoreGenerationErrorV1::kCoverageLost;
    }
    const std::shared_ptr<const GenerationData>& data =
        generation->impl_->data;
    if (data == nullptr || data->session.get() != &session ||
        data->watermark.generation == 0U) {
        return IntradayInstrumentStoreGenerationErrorV1::
            kInvalidGeneration;
    }
    try {
        std::lock_guard<std::mutex> lock(session.generation_mutex);
        if (session.coverage_lost.load(std::memory_order_acquire)) {
            return IntradayInstrumentStoreGenerationErrorV1::
                kCoverageLost;
        }
        if (data->watermark.generation <=
            session.latest_generation.load(std::memory_order_acquire)) {
            return IntradayInstrumentStoreGenerationErrorV1::
                kInvalidGeneration;
        }
        session.latest_generation.store(
            data->watermark.generation, std::memory_order_release);
        return IntradayInstrumentStoreGenerationErrorV1::kNone;
    } catch (...) {
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
    result.allocated_index_bytes = session.base_index_bytes;
    for (const std::unique_ptr<WorkerState>& worker : session.workers) {
        const WorkerAccounting& accounting = worker->accounting;
        const std::uint64_t records =
            accounting.appended_records.load(std::memory_order_acquire);
        const std::uint64_t record_bytes =
            accounting.accounted_record_bytes.load(
                std::memory_order_acquire);
        const std::uint64_t segment_bytes =
            accounting.allocated_segment_bytes.load(
                std::memory_order_acquire);
        const std::uint64_t segments =
            accounting.allocated_segments.load(
                std::memory_order_acquire);
        result.appended_records += records;
        result.accounted_record_bytes += record_bytes;
        result.allocated_index_bytes += segment_bytes;
        result.allocated_segments += segments;
    }
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

bool IntradayInstrumentStoreV1::coverage_lost() const noexcept {
    return impl_->session->coverage_lost.load(
        std::memory_order_acquire);
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
