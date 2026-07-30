#include "l2flow/market/instrument_runtime_state_v2.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <span>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace l2flow::market {
namespace {

inline constexpr std::uint8_t kHasSnapshotV2 = 1U << 0U;
inline constexpr std::uint8_t kHasTickV2 = 1U << 1U;
inline constexpr std::size_t kFrozenVersionBlockEntriesV2 = 1'024U;

static_assert(std::is_nothrow_move_assignable_v<InstrumentKeyV1>);

[[nodiscard]] bool SpanShapeValid(
    std::span<const std::byte> value) noexcept {
    return value.empty() || value.data() != nullptr;
}

[[nodiscard]] bool MarketValid(MarketV1 value) noexcept {
    switch (value) {
        case MarketV1::kShanghai:
        case MarketV1::kShenzhen:
            return true;
        case MarketV1::kUnknown:
            return false;
    }
    return false;
}

[[nodiscard]] bool KeyValid(const InstrumentKeyViewV1& key) noexcept {
    return MarketValid(key.market) &&
           SpanShapeValid(key.security_id_source) &&
           SpanShapeValid(key.security_id) &&
           !key.security_id.empty();
}

[[nodiscard]] bool DataKindValid(
    InstrumentRuntimeDataKindV2 kind) noexcept {
    switch (kind) {
        case InstrumentRuntimeDataKindV2::kSnapshot:
        case InstrumentRuntimeDataKindV2::kTick:
            return true;
    }
    return false;
}

[[nodiscard]] std::uint8_t DataKindFlag(
    InstrumentRuntimeDataKindV2 kind) noexcept {
    return kind == InstrumentRuntimeDataKindV2::kSnapshot
               ? kHasSnapshotV2
               : kHasTickV2;
}

[[nodiscard]] InstrumentKeyViewV1 KeyView(
    const InstrumentKeyV1& key) noexcept {
    return InstrumentKeyViewV1{
        key.market, key.security_id_source, key.security_id};
}

enum class ByteOrderV2 : std::uint8_t {
    kLess = 0U,
    kEqual,
    kGreater,
};

[[nodiscard]] ByteOrderV2 CompareBytes(
    std::span<const std::byte> left,
    std::span<const std::byte> right) noexcept {
    const std::size_t common = std::min(left.size(), right.size());
    for (std::size_t index = 0U; index < common; ++index) {
        const std::uint8_t lhs =
            std::to_integer<std::uint8_t>(left[index]);
        const std::uint8_t rhs =
            std::to_integer<std::uint8_t>(right[index]);
        if (lhs < rhs) {
            return ByteOrderV2::kLess;
        }
        if (lhs > rhs) {
            return ByteOrderV2::kGreater;
        }
    }
    if (left.size() < right.size()) {
        return ByteOrderV2::kLess;
    }
    if (left.size() > right.size()) {
        return ByteOrderV2::kGreater;
    }
    return ByteOrderV2::kEqual;
}

[[nodiscard]] ByteOrderV2 CompareKey(
    const InstrumentKeyV1& left,
    const InstrumentKeyViewV1& right) noexcept {
    const std::uint8_t lhs =
        static_cast<std::uint8_t>(left.market);
    const std::uint8_t rhs =
        static_cast<std::uint8_t>(right.market);
    if (lhs < rhs) {
        return ByteOrderV2::kLess;
    }
    if (lhs > rhs) {
        return ByteOrderV2::kGreater;
    }
    const ByteOrderV2 source =
        CompareBytes(left.security_id_source, right.security_id_source);
    return source == ByteOrderV2::kEqual
               ? CompareBytes(left.security_id, right.security_id)
               : source;
}

struct FrozenEntryStateV2 final {
    InstrumentRuntimeDataStateV2 data_state =
        InstrumentRuntimeDataStateV2::kBoundNoData;
    std::uint8_t availability_flags = 0U;
    bool factor_eligible = false;
    std::uint64_t first_ingress_sequence = 0U;
    std::uint64_t last_ingress_sequence = 0U;
};

struct FrozenVersionNodeV2 final {
    std::uint64_t snapshot_epoch = 0U;
    FrozenEntryStateV2 state{};
    FrozenVersionNodeV2* previous = nullptr;
};

static_assert(std::is_trivially_destructible_v<FrozenVersionNodeV2>);

class FrozenVersionPoolV2 final {
public:
    explicit FrozenVersionPoolV2(std::size_t prewarm_node_count) {
        blocks_.reset(new Block);
        const std::size_t remainder =
            prewarm_node_count % kFrozenVersionBlockEntriesV2;
        const std::size_t block_count =
            prewarm_node_count /
                kFrozenVersionBlockEntriesV2 +
            (remainder == 0U ? 0U : 1U);
        for (std::size_t index = 1U;
             index < block_count;
             ++index) {
            std::unique_ptr<Block> standby(new Block);
            standby->next = std::move(standby_blocks_);
            standby_blocks_ = std::move(standby);
        }
        current_.store(blocks_.get(), std::memory_order_relaxed);
    }

    FrozenVersionPoolV2(const FrozenVersionPoolV2&) = delete;
    FrozenVersionPoolV2& operator=(
        const FrozenVersionPoolV2&) = delete;

    ~FrozenVersionPoolV2() {
        while (blocks_ != nullptr) {
            std::unique_ptr<Block> next =
                std::move(blocks_->next);
            blocks_->next.reset();
            blocks_ = std::move(next);
        }
        while (standby_blocks_ != nullptr) {
            std::unique_ptr<Block> next =
                std::move(standby_blocks_->next);
            standby_blocks_->next.reset();
            standby_blocks_ = std::move(next);
        }
    }

    [[nodiscard]] FrozenVersionNodeV2* Allocate(
        std::uint64_t snapshot_epoch,
        FrozenEntryStateV2 state,
        FrozenVersionNodeV2* previous) {
        for (;;) {
            Block* const block =
                current_.load(std::memory_order_acquire);
            std::size_t index =
                block->used.load(std::memory_order_relaxed);
            while (index < kFrozenVersionBlockEntriesV2) {
                if (block->used.compare_exchange_weak(
                        index,
                        index + 1U,
                        std::memory_order_acq_rel,
                        std::memory_order_relaxed)) {
                    void* const address =
                        static_cast<void*>(
                            block->storage +
                            index * sizeof(FrozenVersionNodeV2));
                    allocated_node_count_.fetch_add(
                        1U, std::memory_order_relaxed);
                    return ::new (address) FrozenVersionNodeV2{
                        snapshot_epoch, state, previous};
                }
            }
            AddBlockIfCurrent(block);
        }
    }

    [[nodiscard]] std::size_t allocated_node_count() const noexcept {
        return allocated_node_count_.load(std::memory_order_relaxed);
    }

private:
    struct Block final {
        Block() noexcept {
            std::fill_n(
                storage, sizeof(storage), std::byte{0U});
        }

        alignas(FrozenVersionNodeV2)
            std::byte storage[
                kFrozenVersionBlockEntriesV2 *
                sizeof(FrozenVersionNodeV2)];
        std::atomic<std::size_t> used{0U};
        std::unique_ptr<Block> next;
    };

    void AddBlockIfCurrent(Block* exhausted) {
        std::lock_guard<std::mutex> lock(block_mutex_);
        if (current_.load(std::memory_order_relaxed) != exhausted) {
            return;
        }
        std::unique_ptr<Block> next;
        if (standby_blocks_ != nullptr) {
            next = std::move(standby_blocks_);
            standby_blocks_ = std::move(next->next);
            next->next.reset();
        } else {
            next.reset(new Block);
        }
        Block* const next_raw = next.get();
        next->next = std::move(blocks_);
        blocks_ = std::move(next);
        current_.store(next_raw, std::memory_order_release);
    }

    std::mutex block_mutex_;
    std::unique_ptr<Block> blocks_;
    std::unique_ptr<Block> standby_blocks_;
    std::atomic<Block*> current_{nullptr};
    std::atomic<std::size_t> allocated_node_count_{0U};
};

// Writers enter with one CAS and never wait unless the O(1) snapshot
// publication boundary is active. AcquireSnapshot closes the gate only long
// enough to capture scalar state and publish a new epoch; it never visits a
// bound row.
class SnapshotMutationGateV2 final {
public:
    [[nodiscard]] bool Enter() noexcept {
        std::uint64_t observed =
            state_.load(std::memory_order_acquire);
        for (;;) {
            if ((observed & kClosedBit) != 0U) {
                std::this_thread::yield();
                observed = state_.load(std::memory_order_acquire);
                continue;
            }
            if (observed == kActiveMask) {
                return false;
            }
            if (state_.compare_exchange_weak(
                    observed,
                    observed + 1U,
                    std::memory_order_acquire,
                    std::memory_order_relaxed)) {
                return true;
            }
        }
    }

    void Leave() noexcept {
        state_.fetch_sub(1U, std::memory_order_release);
    }

    void CloseAndWait() noexcept {
        state_.fetch_or(kClosedBit, std::memory_order_acq_rel);
        while ((state_.load(std::memory_order_acquire) &
                kActiveMask) != 0U) {
            std::this_thread::yield();
        }
    }

    void Open() noexcept {
        state_.store(0U, std::memory_order_release);
    }

private:
    static constexpr std::uint64_t kClosedBit =
        std::uint64_t{1U} << 63U;
    static constexpr std::uint64_t kActiveMask = kClosedBit - 1U;
    std::atomic<std::uint64_t> state_{0U};
};

class SnapshotMutationLeaseV2 final {
public:
    explicit SnapshotMutationLeaseV2(
        SnapshotMutationGateV2* gate) noexcept
        : gate_(gate),
          acquired_(gate_ != nullptr && gate_->Enter()) {}

    SnapshotMutationLeaseV2(const SnapshotMutationLeaseV2&) = delete;
    SnapshotMutationLeaseV2& operator=(
        const SnapshotMutationLeaseV2&) = delete;

    ~SnapshotMutationLeaseV2() {
        if (acquired_) {
            gate_->Leave();
        }
    }

    [[nodiscard]] bool acquired() const noexcept {
        return acquired_;
    }

private:
    SnapshotMutationGateV2* gate_ = nullptr;
    bool acquired_ = false;
};

class ClosedSnapshotMutationGateV2 final {
public:
    explicit ClosedSnapshotMutationGateV2(
        SnapshotMutationGateV2* gate) noexcept
        : gate_(gate) {
        gate_->CloseAndWait();
    }

    ClosedSnapshotMutationGateV2(
        const ClosedSnapshotMutationGateV2&) = delete;
    ClosedSnapshotMutationGateV2& operator=(
        const ClosedSnapshotMutationGateV2&) = delete;

    ~ClosedSnapshotMutationGateV2() {
        gate_->Open();
    }

private:
    SnapshotMutationGateV2* gate_;
};

class AtomicFlagLockV2 final {
public:
    explicit AtomicFlagLockV2(
        std::atomic_flag* flag) noexcept
        : flag_(flag) {
        while (flag_->test_and_set(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }

    AtomicFlagLockV2(
        const AtomicFlagLockV2&) = delete;
    AtomicFlagLockV2& operator=(
        const AtomicFlagLockV2&) = delete;

    ~AtomicFlagLockV2() {
        flag_->clear(std::memory_order_release);
    }

private:
    std::atomic_flag* flag_;
};

struct StableSlotV2 final {
    std::atomic<InstrumentRuntimeDataStateV2> data_state{
        InstrumentRuntimeDataStateV2::kBoundNoData};
    std::uint32_t instrument_id = 0U;
    std::size_t ordinal = std::numeric_limits<std::size_t>::max();
    InstrumentKeyV1 key{};
    InstrumentRuntimeMetadataV2 metadata{};
    std::atomic<std::uint8_t> availability_flags{0U};
    std::atomic<bool> factor_eligible{false};
    std::atomic<std::uint64_t> first_ingress_sequence{0U};
    std::atomic<std::uint64_t> last_ingress_sequence{0U};
    mutable std::atomic_flag mutation_lock = ATOMIC_FLAG_INIT;
    mutable std::atomic_flag preservation_lock = ATOMIC_FLAG_INIT;
    std::atomic<std::uint64_t> last_preserved_snapshot_epoch{0U};
    FrozenVersionNodeV2* frozen_versions = nullptr;
    FrozenVersionNodeV2* reusable_versions = nullptr;
};

struct StableStorageV2 final {
    explicit StableStorageV2(
        std::size_t value_capacity,
        std::uint64_t value_session_epoch)
        : capacity(value_capacity),
          session_epoch(value_session_epoch),
          slots(std::make_unique<StableSlotV2[]>(
              value_capacity)),
          frozen_version_pool(value_capacity),
          live_snapshot_epochs(
              std::make_shared<const std::vector<std::uint64_t>>()) {}

    std::size_t capacity = 0U;
    std::uint64_t session_epoch = 0U;
    std::unique_ptr<StableSlotV2[]> slots;
    mutable std::mutex snapshot_mutex;
    SnapshotMutationGateV2 snapshot_mutation_gate;
    FrozenVersionPoolV2 frozen_version_pool;
    std::atomic<std::uint64_t> current_snapshot_epoch{0U};
    std::atomic<std::size_t> current_snapshot_bound_count{0U};
    std::uint64_t last_snapshot_epoch = 0U;
    std::atomic<bool> snapshot_dirty{true};
    std::weak_ptr<const DailyInstrumentCatalogSnapshotV2>
        published_snapshot;
    mutable std::mutex live_snapshot_mutex;
    std::shared_ptr<const std::vector<std::uint64_t>>
        live_snapshot_epochs;
    static constexpr std::uint64_t catalog_generation = 1U;
    std::atomic<std::uint64_t> data_state_generation{0U};
    l2flow::common::Sha256Digest catalog_digest{};
    std::atomic<std::size_t> available_count{0U};
    std::atomic<std::size_t> snapshot_available_count{0U};
    std::atomic<std::size_t> tick_available_count{0U};
    std::atomic<std::size_t> factor_eligible_count{0U};
    InstrumentCatalogScopeV2 catalog_scope =
        InstrumentCatalogScopeV2::kDeclaredDailyAShare;
    std::uint32_t trade_date = 0U;
    std::uint64_t catalog_version = 0U;
    bool coverage_complete = true;
};

[[nodiscard]] InstrumentRuntimeStateErrorV2
RegisterLiveSnapshotEpoch(
    StableStorageV2* storage,
    std::uint64_t snapshot_epoch) noexcept {
    if (storage == nullptr || snapshot_epoch == 0U) {
        return InstrumentRuntimeStateErrorV2::kUnexpectedFailure;
    }
    try {
        std::lock_guard<std::mutex> lock(
            storage->live_snapshot_mutex);
        const std::shared_ptr<
            const std::vector<std::uint64_t>>
            current = std::atomic_load_explicit(
                &storage->live_snapshot_epochs,
                std::memory_order_acquire);
        if (current == nullptr ||
            (!current->empty() &&
             current->back() >= snapshot_epoch)) {
            return InstrumentRuntimeStateErrorV2::
                kUnexpectedFailure;
        }
        if (current->size() ==
            std::numeric_limits<std::size_t>::max()) {
            return InstrumentRuntimeStateErrorV2::
                kResourceExhausted;
        }
        auto next =
            std::make_shared<std::vector<std::uint64_t>>();
        next->reserve(current->size() + 1U);
        next->insert(
            next->end(), current->begin(), current->end());
        next->push_back(snapshot_epoch);
        std::atomic_store_explicit(
            &storage->live_snapshot_epochs,
            std::shared_ptr<const std::vector<std::uint64_t>>(
                std::move(next)),
            std::memory_order_release);
        return InstrumentRuntimeStateErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return InstrumentRuntimeStateErrorV2::kResourceExhausted;
    } catch (const std::length_error&) {
        return InstrumentRuntimeStateErrorV2::kResourceExhausted;
    } catch (...) {
        return InstrumentRuntimeStateErrorV2::kUnexpectedFailure;
    }
}

void ReleaseLiveSnapshotEpoch(
    StableStorageV2* storage,
    std::uint64_t snapshot_epoch) noexcept {
    if (storage == nullptr || snapshot_epoch == 0U) {
        return;
    }
    try {
        std::lock_guard<std::mutex> lock(
            storage->live_snapshot_mutex);
        const std::shared_ptr<
            const std::vector<std::uint64_t>>
            current = std::atomic_load_explicit(
                &storage->live_snapshot_epochs,
                std::memory_order_acquire);
        if (current == nullptr) {
            return;
        }
        const auto found =
            std::lower_bound(
                current->begin(), current->end(), snapshot_epoch);
        if (found == current->end() || *found != snapshot_epoch) {
            return;
        }
        auto next =
            std::make_shared<std::vector<std::uint64_t>>();
        next->reserve(current->size() - 1U);
        next->insert(next->end(), current->begin(), found);
        next->insert(next->end(), found + 1, current->end());
        std::atomic_store_explicit(
            &storage->live_snapshot_epochs,
            std::shared_ptr<const std::vector<std::uint64_t>>(
                std::move(next)),
            std::memory_order_release);
    } catch (...) {
        // Failure to compact the advisory lifetime index can only retain
        // extra MVCC nodes. It cannot invalidate a live snapshot or expose
        // mutable state through an old one.
    }
}

[[nodiscard]] bool LiveSnapshotInInterval(
    const std::vector<std::uint64_t>& live_epochs,
    std::uint64_t lower_exclusive,
    std::uint64_t upper_inclusive) noexcept {
    const auto candidate =
        std::upper_bound(
            live_epochs.begin(), live_epochs.end(), lower_exclusive);
    return candidate != live_epochs.end() &&
           *candidate <= upper_inclusive;
}

void CompactFrozenVersions(
    StableSlotV2* slot,
    const std::vector<std::uint64_t>& live_epochs) noexcept {
    FrozenVersionNodeV2* retained_head = nullptr;
    FrozenVersionNodeV2* retained_tail = nullptr;
    FrozenVersionNodeV2* node = slot->frozen_versions;
    while (node != nullptr) {
        FrozenVersionNodeV2* const previous = node->previous;
        const std::uint64_t lower_exclusive =
            previous == nullptr ? 0U : previous->snapshot_epoch;
        if (LiveSnapshotInInterval(
                live_epochs,
                lower_exclusive,
                node->snapshot_epoch)) {
            if (retained_head == nullptr) {
                retained_head = node;
            } else {
                retained_tail->previous = node;
            }
            retained_tail = node;
        } else {
            node->previous = slot->reusable_versions;
            slot->reusable_versions = node;
        }
        node = previous;
    }
    if (retained_tail != nullptr) {
        retained_tail->previous = nullptr;
    }
    slot->frozen_versions = retained_head;
}

[[nodiscard]] FrozenEntryStateV2 CaptureLiveEntryState(
    const StableSlotV2& slot) noexcept {
    FrozenEntryStateV2 state{};
    state.data_state =
        slot.data_state.load(std::memory_order_acquire);
    state.availability_flags =
        slot.availability_flags.load(std::memory_order_acquire);
    state.factor_eligible =
        slot.factor_eligible.load(std::memory_order_acquire);
    state.first_ingress_sequence =
        slot.first_ingress_sequence.load(std::memory_order_acquire);
    state.last_ingress_sequence =
        slot.last_ingress_sequence.load(std::memory_order_acquire);
    return state;
}

[[nodiscard]] InstrumentRuntimeStateErrorV2
PreserveCurrentSnapshotState(
    StableStorageV2* storage,
    StableSlotV2* slot) noexcept {
    if (storage == nullptr || slot == nullptr) {
        return InstrumentRuntimeStateErrorV2::kUnexpectedFailure;
    }
    const std::uint64_t snapshot_epoch =
        storage->current_snapshot_epoch.load(std::memory_order_acquire);
    if (snapshot_epoch == 0U ||
        slot->last_preserved_snapshot_epoch.load(
            std::memory_order_acquire) == snapshot_epoch) {
        return InstrumentRuntimeStateErrorV2::kNone;
    }
    if (slot->ordinal >=
        storage->current_snapshot_bound_count.load(
            std::memory_order_acquire)) {
        slot->last_preserved_snapshot_epoch.store(
            snapshot_epoch, std::memory_order_release);
        return InstrumentRuntimeStateErrorV2::kNone;
    }

    try {
        const std::shared_ptr<
            const std::vector<std::uint64_t>>
            live_epochs = std::atomic_load_explicit(
                &storage->live_snapshot_epochs,
                std::memory_order_acquire);
        if (live_epochs == nullptr) {
            return InstrumentRuntimeStateErrorV2::
                kUnexpectedFailure;
        }
        const AtomicFlagLockV2 lock(&slot->preservation_lock);
        if (slot->last_preserved_snapshot_epoch.load(
                std::memory_order_relaxed) == snapshot_epoch) {
            return InstrumentRuntimeStateErrorV2::kNone;
        }
        CompactFrozenVersions(slot, *live_epochs);
        if (live_epochs->empty() ||
            live_epochs->front() > snapshot_epoch) {
            slot->last_preserved_snapshot_epoch.store(
                snapshot_epoch, std::memory_order_release);
            return InstrumentRuntimeStateErrorV2::kNone;
        }
        const FrozenEntryStateV2 state =
            CaptureLiveEntryState(*slot);
        FrozenVersionNodeV2* frozen = slot->reusable_versions;
        if (frozen != nullptr) {
            slot->reusable_versions = frozen->previous;
            *frozen = FrozenVersionNodeV2{
                snapshot_epoch, state, slot->frozen_versions};
        } else {
            frozen = storage->frozen_version_pool.Allocate(
                snapshot_epoch, state, slot->frozen_versions);
        }
        slot->frozen_versions = frozen;
        slot->last_preserved_snapshot_epoch.store(
            snapshot_epoch, std::memory_order_release);
        return InstrumentRuntimeStateErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return InstrumentRuntimeStateErrorV2::kResourceExhausted;
    } catch (...) {
        return InstrumentRuntimeStateErrorV2::kUnexpectedFailure;
    }
}

[[nodiscard]] FrozenEntryStateV2 FrozenEntryStateAtSnapshot(
    const StableSlotV2& slot,
    std::uint64_t snapshot_epoch) noexcept {
    const AtomicFlagLockV2 lock(&slot.preservation_lock);
    const FrozenVersionNodeV2* node = slot.frozen_versions;
    const FrozenVersionNodeV2* candidate = nullptr;
    while (node != nullptr &&
           node->snapshot_epoch >= snapshot_epoch) {
        candidate = node;
        if (node->snapshot_epoch == snapshot_epoch) {
            break;
        }
        node = node->previous;
    }
    return candidate != nullptr
               ? candidate->state
               : CaptureLiveEntryState(slot);
}

[[nodiscard]] InstrumentRuntimeStateErrorV2 FillLiveEntry(
    const StableStorageV2& storage,
    std::size_t ordinal,
    InstrumentRuntimeEntryViewV2* output) noexcept {
    if (output == nullptr) {
        return InstrumentRuntimeStateErrorV2::kNullOutput;
    }
    *output = InstrumentRuntimeEntryViewV2{};
    if (ordinal >= storage.capacity) {
        return InstrumentRuntimeStateErrorV2::kNotFound;
    }
    const StableSlotV2& slot = storage.slots[ordinal];
    output->instrument_id = slot.instrument_id;
    output->ordinal = slot.ordinal;
    output->key = KeyView(slot.key);
    output->metadata = slot.metadata;
    const std::uint8_t flags =
        slot.availability_flags.load(std::memory_order_acquire);
    output->has_snapshot = (flags & kHasSnapshotV2) != 0U;
    output->has_tick = (flags & kHasTickV2) != 0U;
    output->factor_eligible =
        slot.factor_eligible.load(std::memory_order_acquire);
    output->first_ingress_sequence =
        slot.first_ingress_sequence.load(std::memory_order_acquire);
    output->last_ingress_sequence =
        slot.last_ingress_sequence.load(std::memory_order_acquire);
    output->data_state =
        slot.data_state.load(std::memory_order_acquire);
    return output->bound()
               ? InstrumentRuntimeStateErrorV2::kNone
               : InstrumentRuntimeStateErrorV2::kIdentityMismatch;
}

void FillFrozenEntry(
    const StableStorageV2& storage,
    std::size_t ordinal,
    const FrozenEntryStateV2& frozen,
    InstrumentRuntimeEntryViewV2* output) noexcept {
    *output = InstrumentRuntimeEntryViewV2{};
    const StableSlotV2& slot = storage.slots[ordinal];
    output->instrument_id = slot.instrument_id;
    output->ordinal = slot.ordinal;
    output->key = KeyView(slot.key);
    output->metadata = slot.metadata;
    output->data_state = frozen.data_state;
    output->has_snapshot =
        (frozen.availability_flags & kHasSnapshotV2) != 0U;
    output->has_tick =
        (frozen.availability_flags & kHasTickV2) != 0U;
    output->factor_eligible = frozen.factor_eligible;
    output->first_ingress_sequence = frozen.first_ingress_sequence;
    output->last_ingress_sequence = frozen.last_ingress_sequence;
}

void AtomicMinimum(
    std::atomic<std::uint64_t>* value,
    std::uint64_t candidate) noexcept {
    std::uint64_t current = value->load(std::memory_order_relaxed);
    while ((current == 0U || candidate < current) &&
           !value->compare_exchange_weak(
               current,
               candidate,
               std::memory_order_release,
               std::memory_order_relaxed)) {
    }
}

void AtomicMaximum(
    std::atomic<std::uint64_t>* value,
    std::uint64_t candidate) noexcept {
    std::uint64_t current = value->load(std::memory_order_relaxed);
    while (candidate > current &&
           !value->compare_exchange_weak(
               current,
               candidate,
               std::memory_order_release,
               std::memory_order_relaxed)) {
    }
}

[[nodiscard]] bool ReserveDataStateGeneration(
    std::atomic<std::uint64_t>* generation) noexcept {
    std::uint64_t current =
        generation->load(std::memory_order_relaxed);
    while (current != std::numeric_limits<std::uint64_t>::max()) {
        if (generation->compare_exchange_weak(
                current,
                current + 1U,
                std::memory_order_acq_rel,
                std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

[[nodiscard]] bool SlotIdentityMatches(
    const StableStorageV2& storage,
    std::size_t ordinal,
    std::uint32_t instrument_id) noexcept {
    if (instrument_id == 0U || ordinal >= storage.capacity) {
        return false;
    }
    const StableSlotV2& slot = storage.slots[ordinal];
    const InstrumentRuntimeDataStateV2 state =
        slot.data_state.load(std::memory_order_acquire);
    return (state ==
                InstrumentRuntimeDataStateV2::kBoundNoData ||
            state == InstrumentRuntimeDataStateV2::kAvailable) &&
           slot.ordinal == ordinal &&
           slot.instrument_id == instrument_id;
}

[[nodiscard]] bool SnapshotCountsValid(
    const StableStorageV2& storage) noexcept {
    const std::size_t available =
        storage.available_count.load(std::memory_order_acquire);
    const std::size_t snapshot =
        storage.snapshot_available_count.load(
            std::memory_order_acquire);
    const std::size_t tick =
        storage.tick_available_count.load(std::memory_order_acquire);
    const std::size_t factor =
        storage.factor_eligible_count.load(std::memory_order_acquire);
    return factor <= snapshot && snapshot <= available &&
           tick <= available && available <= storage.capacity &&
           storage.catalog_generation == 1U &&
           storage.catalog_version != 0U &&
           storage.trade_date != 0U &&
           storage.coverage_complete &&
           storage.catalog_scope ==
               InstrumentCatalogScopeV2::
                   kDeclaredDailyAShare;
}

}  // namespace

class DailyInstrumentCatalogSnapshotV2::Impl final {
public:
    std::shared_ptr<const StableStorageV2> storage;
    std::uint64_t snapshot_epoch = 0U;
    bool live_epoch_registered = false;
    std::uint64_t catalog_generation = 0U;
    std::uint64_t data_state_generation = 0U;
    l2flow::common::Sha256Digest catalog_digest{};
    std::size_t bound_count = 0U;
    std::size_t available_count = 0U;
    std::size_t snapshot_available_count = 0U;
    std::size_t tick_available_count = 0U;
    std::size_t factor_eligible_count = 0U;
};

class InstrumentRuntimeStateV2::Impl final {
public:
    explicit Impl(std::shared_ptr<StableStorageV2> value) noexcept
        : storage(std::move(value)) {}

    std::shared_ptr<StableStorageV2> storage;
};

std::string_view InstrumentRuntimeStateErrorNameV2(
    InstrumentRuntimeStateErrorV2 error) noexcept {
    switch (error) {
        case InstrumentRuntimeStateErrorV2::kNone:
            return "none";
        case InstrumentRuntimeStateErrorV2::kNullOutput:
            return "null_output";
        case InstrumentRuntimeStateErrorV2::kInvalidConfiguration:
            return "invalid_configuration";
        case InstrumentRuntimeStateErrorV2::kInvalidDataKind:
            return "invalid_data_kind";
        case InstrumentRuntimeStateErrorV2::kInvalidKey:
            return "invalid_key";
        case InstrumentRuntimeStateErrorV2::kInvalidSequence:
            return "invalid_sequence";
        case InstrumentRuntimeStateErrorV2::kNotFound:
            return "not_found";
        case InstrumentRuntimeStateErrorV2::kIdentityMismatch:
            return "identity_mismatch";
        case InstrumentRuntimeStateErrorV2::kPrerequisiteUnavailable:
            return "prerequisite_unavailable";
        case InstrumentRuntimeStateErrorV2::kGenerationExhausted:
            return "generation_exhausted";
        case InstrumentRuntimeStateErrorV2::kResourceExhausted:
            return "resource_exhausted";
        case InstrumentRuntimeStateErrorV2::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "invalid_instrument_runtime_state_error_v2";
}

DailyInstrumentCatalogSnapshotV2::
    DailyInstrumentCatalogSnapshotV2(
        std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

DailyInstrumentCatalogSnapshotV2::
    ~DailyInstrumentCatalogSnapshotV2() {
    if (impl_ != nullptr && impl_->live_epoch_registered) {
        ReleaseLiveSnapshotEpoch(
            const_cast<StableStorageV2*>(impl_->storage.get()),
            impl_->snapshot_epoch);
    }
}

std::uint64_t
DailyInstrumentCatalogSnapshotV2::session_epoch() const noexcept {
    return impl_->storage->session_epoch;
}

std::uint32_t
DailyInstrumentCatalogSnapshotV2::trade_date() const noexcept {
    return impl_->storage->trade_date;
}

std::uint64_t
DailyInstrumentCatalogSnapshotV2::catalog_version() const noexcept {
    return impl_->storage->catalog_version;
}

std::size_t
DailyInstrumentCatalogSnapshotV2::capacity() const noexcept {
    return impl_->storage->capacity;
}

InstrumentCatalogScopeV2
DailyInstrumentCatalogSnapshotV2::catalog_scope() const noexcept {
    return impl_->storage->catalog_scope;
}

bool DailyInstrumentCatalogSnapshotV2::coverage_complete()
    const noexcept {
    return impl_->storage->coverage_complete;
}

std::uint64_t
DailyInstrumentCatalogSnapshotV2::catalog_generation()
    const noexcept {
    return impl_->catalog_generation;
}

std::uint64_t
DailyInstrumentCatalogSnapshotV2::data_state_generation()
    const noexcept {
    return impl_->data_state_generation;
}

const l2flow::common::Sha256Digest&
DailyInstrumentCatalogSnapshotV2::catalog_digest() const noexcept {
    return impl_->catalog_digest;
}

std::size_t
DailyInstrumentCatalogSnapshotV2::bound_count() const noexcept {
    return impl_->bound_count;
}

std::size_t
DailyInstrumentCatalogSnapshotV2::available_count() const noexcept {
    return impl_->available_count;
}

std::size_t
DailyInstrumentCatalogSnapshotV2::snapshot_available_count()
    const noexcept {
    return impl_->snapshot_available_count;
}

std::size_t
DailyInstrumentCatalogSnapshotV2::tick_available_count()
    const noexcept {
    return impl_->tick_available_count;
}

std::size_t
DailyInstrumentCatalogSnapshotV2::factor_eligible_count()
    const noexcept {
    return impl_->factor_eligible_count;
}

InstrumentRuntimeStateErrorV2
DailyInstrumentCatalogSnapshotV2::EntryAt(
    std::size_t ordinal,
    InstrumentRuntimeEntryViewV2* output) const noexcept {
    if (output == nullptr) {
        return InstrumentRuntimeStateErrorV2::kNullOutput;
    }
    *output = InstrumentRuntimeEntryViewV2{};
    if (ordinal >= impl_->bound_count ||
        impl_->snapshot_epoch == 0U) {
        return InstrumentRuntimeStateErrorV2::kNotFound;
    }
    const FrozenEntryStateV2 frozen =
        FrozenEntryStateAtSnapshot(
            impl_->storage->slots[ordinal],
            impl_->snapshot_epoch);
    FillFrozenEntry(
        *impl_->storage, ordinal, frozen, output);
    return output->bound()
               ? InstrumentRuntimeStateErrorV2::kNone
               : InstrumentRuntimeStateErrorV2::kUnexpectedFailure;
}

InstrumentRuntimeStateErrorV2
DailyInstrumentCatalogSnapshotV2::LookupById(
    std::uint32_t instrument_id,
    InstrumentRuntimeEntryViewV2* output) const noexcept {
    if (output == nullptr) {
        return InstrumentRuntimeStateErrorV2::kNullOutput;
    }
    *output = InstrumentRuntimeEntryViewV2{};
    if (instrument_id == 0U) {
        return InstrumentRuntimeStateErrorV2::kIdentityMismatch;
    }
    const std::size_t ordinal =
        static_cast<std::size_t>(instrument_id - 1U);
    return EntryAt(ordinal, output);
}

InstrumentRuntimeStateV2::InstrumentRuntimeStateV2(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

InstrumentRuntimeStateV2::~InstrumentRuntimeStateV2() = default;

InstrumentRuntimeStateErrorV2
InstrumentRuntimeStateV2::Create(
    const DailyInstrumentCatalogV2& catalog,
    std::unique_ptr<InstrumentRuntimeStateV2>* output) noexcept {
    if (output == nullptr) {
        return InstrumentRuntimeStateErrorV2::kNullOutput;
    }
    output->reset();
    if (!catalog.coverage_complete() ||
        catalog.trade_date() == 0U ||
        catalog.catalog_version() == 0U ||
        catalog.session_epoch() == 0U ||
        catalog.instrument_count() == 0U ||
        catalog.instrument_count() >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max())) {
        return InstrumentRuntimeStateErrorV2::
            kInvalidConfiguration;
    }
    try {
        auto storage = std::make_shared<StableStorageV2>(
            catalog.instrument_count(), catalog.session_epoch());
        storage->trade_date = catalog.trade_date();
        storage->catalog_version = catalog.catalog_version();
        storage->catalog_digest = catalog.catalog_digest();

        for (const DailyInstrumentCatalogEntryV2& source :
             catalog.entries()) {
            if (source.ordinal >= storage->capacity ||
                source.instrument_id !=
                    static_cast<std::uint32_t>(
                        source.ordinal + 1U)) {
                return InstrumentRuntimeStateErrorV2::
                    kIdentityMismatch;
            }
            StableSlotV2& slot = storage->slots[source.ordinal];
            slot.instrument_id = source.instrument_id;
            slot.ordinal = source.ordinal;
            slot.key = source.key;
            slot.metadata = source.metadata;
            slot.data_state.store(
                InstrumentRuntimeDataStateV2::kBoundNoData,
                std::memory_order_release);
        }
        storage->snapshot_dirty.store(true, std::memory_order_release);
        auto impl = std::make_unique<Impl>(std::move(storage));
        output->reset(
            new InstrumentRuntimeStateV2(std::move(impl)));
        return InstrumentRuntimeStateErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return InstrumentRuntimeStateErrorV2::kResourceExhausted;
    } catch (const std::length_error&) {
        return InstrumentRuntimeStateErrorV2::kResourceExhausted;
    } catch (...) {
        return InstrumentRuntimeStateErrorV2::kUnexpectedFailure;
    }
}

InstrumentRuntimeStateErrorV2
InstrumentRuntimeStateV2::LookupByKey(
    const InstrumentKeyViewV1& key,
    InstrumentRuntimeEntryViewV2* output) const noexcept {
    if (output == nullptr) {
        return InstrumentRuntimeStateErrorV2::kNullOutput;
    }
    *output = InstrumentRuntimeEntryViewV2{};
    if (!KeyValid(key)) {
        return InstrumentRuntimeStateErrorV2::kInvalidKey;
    }
    try {
        const StableStorageV2& storage = *impl_->storage;
        std::size_t first = 0U;
        std::size_t last = storage.capacity;
        while (first < last) {
            const std::size_t middle = first + (last - first) / 2U;
            if (CompareKey(storage.slots[middle].key, key) ==
                ByteOrderV2::kLess) {
                first = middle + 1U;
            } else {
                last = middle;
            }
        }
        if (first >= storage.capacity ||
            CompareKey(storage.slots[first].key, key) !=
                ByteOrderV2::kEqual) {
            return InstrumentRuntimeStateErrorV2::kNotFound;
        }
        return FillLiveEntry(storage, first, output);
    } catch (...) {
        return InstrumentRuntimeStateErrorV2::kUnexpectedFailure;
    }
}

InstrumentRuntimeStateErrorV2
InstrumentRuntimeStateV2::LookupByKey(
    const InstrumentKeyV1& key,
    InstrumentRuntimeEntryViewV2* output) const noexcept {
    return LookupByKey(KeyView(key), output);
}

InstrumentRuntimeStateErrorV2
InstrumentRuntimeStateV2::LookupById(
    std::uint32_t instrument_id,
    InstrumentRuntimeEntryViewV2* output) const noexcept {
    if (output == nullptr) {
        return InstrumentRuntimeStateErrorV2::kNullOutput;
    }
    *output = InstrumentRuntimeEntryViewV2{};
    if (instrument_id == 0U) {
        return InstrumentRuntimeStateErrorV2::kIdentityMismatch;
    }
    const std::size_t ordinal =
        static_cast<std::size_t>(instrument_id - 1U);
    return FillLiveEntry(*impl_->storage, ordinal, output);
}

InstrumentRuntimeStateErrorV2
InstrumentRuntimeStateV2::LookupByOrdinal(
    std::size_t ordinal,
    InstrumentRuntimeEntryViewV2* output) const noexcept {
    return FillLiveEntry(*impl_->storage, ordinal, output);
}

InstrumentRuntimeStateErrorV2
InstrumentRuntimeStateV2::ResolveBoundId(
    std::uint32_t instrument_id,
    std::size_t* ordinal) const noexcept {
    if (ordinal == nullptr) {
        return InstrumentRuntimeStateErrorV2::kNullOutput;
    }
    *ordinal = std::numeric_limits<std::size_t>::max();
    if (instrument_id == 0U) {
        return InstrumentRuntimeStateErrorV2::kIdentityMismatch;
    }
    const std::size_t candidate =
        static_cast<std::size_t>(instrument_id - 1U);
    const StableStorageV2& storage = *impl_->storage;
    if (candidate >= storage.capacity) {
        return InstrumentRuntimeStateErrorV2::kNotFound;
    }
    const StableSlotV2& slot = storage.slots[candidate];
    if (slot.instrument_id != instrument_id ||
        slot.ordinal != candidate) {
        return InstrumentRuntimeStateErrorV2::kIdentityMismatch;
    }
    *ordinal = candidate;
    return InstrumentRuntimeStateErrorV2::kNone;
}

InstrumentRuntimeStateErrorV2
InstrumentRuntimeStateV2::MarkApplied(
    std::size_t ordinal,
    std::uint32_t instrument_id,
    InstrumentRuntimeDataKindV2 kind,
    std::uint64_t ingress_sequence) noexcept {
    if (!DataKindValid(kind)) {
        return InstrumentRuntimeStateErrorV2::kInvalidDataKind;
    }
    if (ingress_sequence == 0U ||
        ingress_sequence ==
            std::numeric_limits<std::uint64_t>::max()) {
        return InstrumentRuntimeStateErrorV2::kInvalidSequence;
    }
    StableStorageV2& storage = *impl_->storage;
    if (!SlotIdentityMatches(storage, ordinal, instrument_id)) {
        return InstrumentRuntimeStateErrorV2::kIdentityMismatch;
    }
    StableSlotV2& slot = storage.slots[ordinal];
    const std::uint8_t flag = DataKindFlag(kind);

    try {
        const SnapshotMutationLeaseV2 mutation(
            &storage.snapshot_mutation_gate);
        if (!mutation.acquired()) {
            return InstrumentRuntimeStateErrorV2::
                kUnexpectedFailure;
        }
        const AtomicFlagLockV2 slot_lock(&slot.mutation_lock);
        if (!SlotIdentityMatches(storage, ordinal, instrument_id)) {
            return InstrumentRuntimeStateErrorV2::
                kIdentityMismatch;
        }
        const InstrumentRuntimeStateErrorV2 preserve_error =
            PreserveCurrentSnapshotState(&storage, &slot);
        if (preserve_error !=
            InstrumentRuntimeStateErrorV2::kNone) {
            return preserve_error;
        }

        const std::uint8_t observed_flags =
            slot.availability_flags.load(std::memory_order_acquire);
        if ((observed_flags & flag) != 0U &&
            slot.data_state.load(std::memory_order_acquire) ==
                InstrumentRuntimeDataStateV2::kAvailable) {
            AtomicMinimum(
                &slot.first_ingress_sequence, ingress_sequence);
            AtomicMaximum(
                &slot.last_ingress_sequence, ingress_sequence);
            storage.snapshot_dirty.store(
                true, std::memory_order_release);
            return InstrumentRuntimeStateErrorV2::kNone;
        }

        const std::uint8_t old_flags =
            slot.availability_flags.load(std::memory_order_relaxed);
        const bool first_available = old_flags == 0U;
        const bool first_kind = (old_flags & flag) == 0U;
        if (!first_available && !first_kind) {
            AtomicMinimum(
                &slot.first_ingress_sequence, ingress_sequence);
            AtomicMaximum(
                &slot.last_ingress_sequence, ingress_sequence);
            storage.snapshot_dirty.store(
                true, std::memory_order_release);
            return InstrumentRuntimeStateErrorV2::kNone;
        }
        if (!ReserveDataStateGeneration(
                &storage.data_state_generation)) {
            return InstrumentRuntimeStateErrorV2::
                kGenerationExhausted;
        }

        AtomicMinimum(&slot.first_ingress_sequence, ingress_sequence);
        AtomicMaximum(&slot.last_ingress_sequence, ingress_sequence);
        const std::uint8_t next_flags =
            static_cast<std::uint8_t>(old_flags | flag);
        slot.availability_flags.store(
            next_flags, std::memory_order_release);
        if (first_available) {
            ++storage.available_count;
        }
        if (first_kind &&
            kind == InstrumentRuntimeDataKindV2::kSnapshot) {
            ++storage.snapshot_available_count;
        }
        if (first_kind &&
            kind == InstrumentRuntimeDataKindV2::kTick) {
            ++storage.tick_available_count;
        }
        slot.data_state.store(
            InstrumentRuntimeDataStateV2::kAvailable,
            std::memory_order_release);
        storage.snapshot_dirty.store(true, std::memory_order_release);
        return InstrumentRuntimeStateErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return InstrumentRuntimeStateErrorV2::kResourceExhausted;
    } catch (...) {
        return InstrumentRuntimeStateErrorV2::kUnexpectedFailure;
    }
}

InstrumentRuntimeStateErrorV2
InstrumentRuntimeStateV2::SetFactorEligible(
    std::size_t ordinal,
    std::uint32_t instrument_id,
    bool eligible) noexcept {
    StableStorageV2& storage = *impl_->storage;
    if (!SlotIdentityMatches(storage, ordinal, instrument_id)) {
        return InstrumentRuntimeStateErrorV2::kIdentityMismatch;
    }
    StableSlotV2& fast_slot = storage.slots[ordinal];
    if (fast_slot.factor_eligible.load(std::memory_order_acquire) ==
        eligible) {
        return InstrumentRuntimeStateErrorV2::kNone;
    }
    try {
        const SnapshotMutationLeaseV2 mutation(
            &storage.snapshot_mutation_gate);
        if (!mutation.acquired()) {
            return InstrumentRuntimeStateErrorV2::
                kUnexpectedFailure;
        }
        const AtomicFlagLockV2 slot_lock(&fast_slot.mutation_lock);
        if (!SlotIdentityMatches(storage, ordinal, instrument_id)) {
            return InstrumentRuntimeStateErrorV2::
                kIdentityMismatch;
        }
        StableSlotV2& slot = fast_slot;
        const bool current =
            slot.factor_eligible.load(std::memory_order_acquire);
        if (current == eligible) {
            return InstrumentRuntimeStateErrorV2::kNone;
        }
        const std::uint8_t flags =
            slot.availability_flags.load(std::memory_order_acquire);
        if (eligible &&
            (slot.data_state.load(std::memory_order_acquire) !=
                 InstrumentRuntimeDataStateV2::kAvailable ||
             (flags & kHasSnapshotV2) == 0U)) {
            return InstrumentRuntimeStateErrorV2::
                kPrerequisiteUnavailable;
        }
        const InstrumentRuntimeStateErrorV2 preserve_error =
            PreserveCurrentSnapshotState(&storage, &slot);
        if (preserve_error !=
            InstrumentRuntimeStateErrorV2::kNone) {
            return preserve_error;
        }
        if (!ReserveDataStateGeneration(
                &storage.data_state_generation)) {
            return InstrumentRuntimeStateErrorV2::
                kGenerationExhausted;
        }
        if (eligible) {
            storage.factor_eligible_count.fetch_add(
                1U, std::memory_order_relaxed);
        } else {
            const std::size_t previous =
                storage.factor_eligible_count.fetch_sub(
                    1U, std::memory_order_relaxed);
            if (previous == 0U) {
                storage.factor_eligible_count.fetch_add(
                    1U, std::memory_order_relaxed);
                return InstrumentRuntimeStateErrorV2::
                    kUnexpectedFailure;
            }
        }
        slot.factor_eligible.store(eligible, std::memory_order_release);
        storage.snapshot_dirty.store(true, std::memory_order_release);
        return InstrumentRuntimeStateErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return InstrumentRuntimeStateErrorV2::kResourceExhausted;
    } catch (...) {
        return InstrumentRuntimeStateErrorV2::kUnexpectedFailure;
    }
}

InstrumentRuntimeStateErrorV2
InstrumentRuntimeStateV2::AcquireSnapshot(
    std::shared_ptr<const DailyInstrumentCatalogSnapshotV2>* output)
    const noexcept {
    if (output == nullptr) {
        return InstrumentRuntimeStateErrorV2::kNullOutput;
    }
    output->reset();
    try {
        const std::shared_ptr<StableStorageV2>& storage_owner =
            impl_->storage;
        StableStorageV2& storage = *storage_owner;
        std::lock_guard<std::mutex> snapshot_lock(
            storage.snapshot_mutex);
        if (!storage.snapshot_dirty.load(std::memory_order_acquire)) {
            std::shared_ptr<const
                DailyInstrumentCatalogSnapshotV2>
                published = storage.published_snapshot.lock();
            if (published != nullptr) {
                *output = std::move(published);
                return InstrumentRuntimeStateErrorV2::kNone;
            }
        }

        const ClosedSnapshotMutationGateV2 closed(
            &storage.snapshot_mutation_gate);
        if (!SnapshotCountsValid(storage)) {
            return InstrumentRuntimeStateErrorV2::
                kUnexpectedFailure;
        }

        // Reaching this point means there is no reusable live published
        // snapshot. Even unchanged state receives a fresh epoch so a dead
        // epoch can be removed from the live-version set and recycled.
        if (storage.last_snapshot_epoch ==
            std::numeric_limits<std::uint64_t>::max()) {
            return InstrumentRuntimeStateErrorV2::
                kGenerationExhausted;
        }
        const std::uint64_t snapshot_epoch =
            storage.last_snapshot_epoch + 1U;
        if (snapshot_epoch == 0U) {
            return InstrumentRuntimeStateErrorV2::
                kUnexpectedFailure;
        }

        auto snapshot_impl =
            std::make_unique<
                DailyInstrumentCatalogSnapshotV2::Impl>();
        snapshot_impl->storage =
            std::shared_ptr<const StableStorageV2>(storage_owner);
        snapshot_impl->snapshot_epoch = snapshot_epoch;
        snapshot_impl->catalog_generation =
            storage.catalog_generation;
        snapshot_impl->data_state_generation =
            storage.data_state_generation.load(
                std::memory_order_acquire);
        snapshot_impl->catalog_digest = storage.catalog_digest;
        snapshot_impl->bound_count = storage.capacity;
        snapshot_impl->available_count =
            storage.available_count.load(std::memory_order_acquire);
        snapshot_impl->snapshot_available_count =
            storage.snapshot_available_count.load(
                std::memory_order_acquire);
        snapshot_impl->tick_available_count =
            storage.tick_available_count.load(
                std::memory_order_acquire);
        snapshot_impl->factor_eligible_count =
            storage.factor_eligible_count.load(
                std::memory_order_acquire);

        auto* const snapshot_object =
            new DailyInstrumentCatalogSnapshotV2(
                std::move(snapshot_impl));
        std::shared_ptr<const DailyInstrumentCatalogSnapshotV2>
            snapshot(snapshot_object);
        const InstrumentRuntimeStateErrorV2 register_error =
            RegisterLiveSnapshotEpoch(&storage, snapshot_epoch);
        if (register_error !=
            InstrumentRuntimeStateErrorV2::kNone) {
            return register_error;
        }
        snapshot_object->impl_->live_epoch_registered = true;
        storage.last_snapshot_epoch = snapshot_epoch;
        storage.current_snapshot_bound_count.store(
            storage.capacity, std::memory_order_relaxed);
        storage.current_snapshot_epoch.store(
            snapshot_epoch, std::memory_order_release);
        storage.snapshot_dirty.store(false, std::memory_order_release);
        storage.published_snapshot = snapshot;
        *output = std::move(snapshot);
        return InstrumentRuntimeStateErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return InstrumentRuntimeStateErrorV2::kResourceExhausted;
    } catch (const std::length_error&) {
        return InstrumentRuntimeStateErrorV2::kResourceExhausted;
    } catch (...) {
        return InstrumentRuntimeStateErrorV2::kUnexpectedFailure;
    }
}

InstrumentRuntimeStateErrorV2
InstrumentRuntimeStateV2::SnapshotStorageStats(
    InstrumentRuntimeSnapshotStorageStatsV2* output)
    const noexcept {
    if (output == nullptr) {
        return InstrumentRuntimeStateErrorV2::kNullOutput;
    }
    *output = InstrumentRuntimeSnapshotStorageStatsV2{};
    try {
        const StableStorageV2& storage = *impl_->storage;
        const std::shared_ptr<
            const std::vector<std::uint64_t>>
            live_epochs = std::atomic_load_explicit(
                &storage.live_snapshot_epochs,
                std::memory_order_acquire);
        if (live_epochs == nullptr) {
            return InstrumentRuntimeStateErrorV2::
                kUnexpectedFailure;
        }
        output->live_snapshot_count = live_epochs->size();
        output->allocated_version_node_count =
            storage.frozen_version_pool.allocated_node_count();
        return InstrumentRuntimeStateErrorV2::kNone;
    } catch (...) {
        *output = InstrumentRuntimeSnapshotStorageStatsV2{};
        return InstrumentRuntimeStateErrorV2::kUnexpectedFailure;
    }
}

std::uint64_t
InstrumentRuntimeStateV2::session_epoch() const noexcept {
    return impl_->storage->session_epoch;
}

std::uint32_t
InstrumentRuntimeStateV2::trade_date() const noexcept {
    return impl_->storage->trade_date;
}

std::uint64_t
InstrumentRuntimeStateV2::catalog_version() const noexcept {
    return impl_->storage->catalog_version;
}

std::size_t InstrumentRuntimeStateV2::capacity() const noexcept {
    return impl_->storage->capacity;
}

}  // namespace l2flow::market
