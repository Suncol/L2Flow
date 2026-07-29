#include "l2flow/market/observed_instrument_directory_v2.h"

#include <algorithm>
#include <array>
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
#include <unordered_map>
#include <utility>
#include <vector>

namespace l2flow::market {
namespace {

inline constexpr std::string_view kCatalogGenesisHashDomainV2 =
    "L2FLOW_OBSERVED_INSTRUMENT_CATALOG_V2_GENESIS";
inline constexpr std::string_view kCatalogBindHashDomainV2 =
    "L2FLOW_OBSERVED_INSTRUMENT_CATALOG_V2_BIND";

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

[[nodiscard]] bool QuantityUnitValid(QuantityUnitV1 value) noexcept {
    switch (value) {
        case QuantityUnitV1::kUnknown:
        case QuantityUnitV1::kShare:
        case QuantityUnitV1::kFundUnit:
        case QuantityUnitV1::kLot:
        case QuantityUnitV1::kBondPiece:
        case QuantityUnitV1::kIndexUnit:
            return true;
    }
    return false;
}

[[nodiscard]] bool SecurityTypeValid(SecurityTypeV1 value) noexcept {
    switch (value) {
        case SecurityTypeV1::kUnknown:
        case SecurityTypeV1::kEquity:
        case SecurityTypeV1::kFund:
        case SecurityTypeV1::kBond:
        case SecurityTypeV1::kConvertibleBond:
        case SecurityTypeV1::kIndex:
        case SecurityTypeV1::kWarrant:
        case SecurityTypeV1::kOption:
            return true;
    }
    return false;
}

[[nodiscard]] bool AssetScopeValid(AssetScopeV1 value) noexcept {
    switch (value) {
        case AssetScopeV1::kUnknown:
        case AssetScopeV1::kDocumentedCore:
        case AssetScopeV1::kOutsideDocumentedCore:
            return true;
    }
    return false;
}

[[nodiscard]] bool KeyValid(const InstrumentKeyViewV1& key) noexcept {
    return MarketValid(key.market) &&
           SpanShapeValid(key.security_id_source) &&
           SpanShapeValid(key.security_id) &&
           !key.security_id.empty();
}

[[nodiscard]] bool MetadataValid(
    const ObservedInstrumentMetadataV2& metadata) noexcept {
    return QuantityUnitValid(metadata.quantity_unit) &&
           SecurityTypeValid(metadata.security_type) &&
           AssetScopeValid(metadata.asset_scope);
}

[[nodiscard]] bool MetadataEqual(
    const ObservedInstrumentMetadataV2& left,
    const ObservedInstrumentMetadataV2& right) noexcept {
    return left.quantity_unit == right.quantity_unit &&
           left.security_type == right.security_type &&
           left.asset_scope == right.asset_scope;
}

[[nodiscard]] bool DataKindValid(
    ObservedInstrumentDataKindV2 kind) noexcept {
    switch (kind) {
        case ObservedInstrumentDataKindV2::kSnapshot:
        case ObservedInstrumentDataKindV2::kTick:
            return true;
    }
    return false;
}

[[nodiscard]] std::uint8_t DataKindFlag(
    ObservedInstrumentDataKindV2 kind) noexcept {
    return kind == ObservedInstrumentDataKindV2::kSnapshot
               ? kHasSnapshotV2
               : kHasTickV2;
}

[[nodiscard]] bool BytesEqual(
    std::span<const std::byte> left,
    std::span<const std::byte> right) noexcept {
    return left.size() == right.size() &&
           std::equal(left.begin(), left.end(), right.begin());
}

[[nodiscard]] bool KeysEqual(
    const InstrumentKeyViewV1& left,
    const InstrumentKeyViewV1& right) noexcept {
    return left.market == right.market &&
           BytesEqual(
               left.security_id_source, right.security_id_source) &&
           BytesEqual(left.security_id, right.security_id);
}

[[nodiscard]] InstrumentKeyViewV1 KeyView(
    const InstrumentKeyV1& key) noexcept {
    return InstrumentKeyViewV1{
        key.market, key.security_id_source, key.security_id};
}

class InstrumentKeyHashV2 final {
public:
    using is_transparent = void;

    [[nodiscard]] std::size_t operator()(
        const InstrumentKeyV1& key) const noexcept {
        return Hash(KeyView(key));
    }

    [[nodiscard]] std::size_t operator()(
        const InstrumentKeyViewV1& key) const noexcept {
        return Hash(key);
    }

private:
    [[nodiscard]] static std::size_t Hash(
        const InstrumentKeyViewV1& key) noexcept {
        constexpr std::uint64_t kOffset = 14695981039346656037ULL;
        constexpr std::uint64_t kPrime = 1099511628211ULL;
        std::uint64_t value = kOffset;
        const auto mix_byte = [&value](std::uint8_t byte) noexcept {
            value ^= byte;
            value *= kPrime;
        };
        const auto mix_size = [&mix_byte](std::size_t size) noexcept {
            std::uint64_t narrowed = static_cast<std::uint64_t>(size);
            for (std::size_t index = 0U; index < 8U; ++index) {
                mix_byte(static_cast<std::uint8_t>(
                    (narrowed >> (index * 8U)) & 0xffU));
            }
        };
        mix_byte(static_cast<std::uint8_t>(key.market));
        mix_size(key.security_id_source.size());
        for (std::byte byte : key.security_id_source) {
            mix_byte(std::to_integer<std::uint8_t>(byte));
        }
        mix_size(key.security_id.size());
        for (std::byte byte : key.security_id) {
            mix_byte(std::to_integer<std::uint8_t>(byte));
        }
        if constexpr (sizeof(std::size_t) < sizeof(std::uint64_t)) {
            return static_cast<std::size_t>(
                value ^ (value >> (sizeof(std::size_t) * 8U)));
        }
        return static_cast<std::size_t>(value);
    }
};

class InstrumentKeyEqualV2 final {
public:
    using is_transparent = void;

    [[nodiscard]] bool operator()(
        const InstrumentKeyV1& left,
        const InstrumentKeyV1& right) const noexcept {
        return KeysEqual(KeyView(left), KeyView(right));
    }

    [[nodiscard]] bool operator()(
        const InstrumentKeyV1& left,
        const InstrumentKeyViewV1& right) const noexcept {
        return KeysEqual(KeyView(left), right);
    }

    [[nodiscard]] bool operator()(
        const InstrumentKeyViewV1& left,
        const InstrumentKeyV1& right) const noexcept {
        return KeysEqual(left, KeyView(right));
    }

    [[nodiscard]] bool operator()(
        const InstrumentKeyViewV1& left,
        const InstrumentKeyViewV1& right) const noexcept {
        return KeysEqual(left, right);
    }
};

[[nodiscard]] bool HashBytes(
    l2flow::common::Sha256Hasher* hasher,
    std::span<const std::byte> bytes) noexcept {
    return hasher != nullptr && hasher->Update(bytes);
}

[[nodiscard]] bool HashU8(
    l2flow::common::Sha256Hasher* hasher,
    std::uint8_t value) noexcept {
    const std::array<std::byte, 1U> bytes{
        static_cast<std::byte>(value)};
    return HashBytes(hasher, bytes);
}

[[nodiscard]] bool HashU32(
    l2flow::common::Sha256Hasher* hasher,
    std::uint32_t value) noexcept {
    std::array<std::byte, 4U> bytes{};
    for (std::size_t index = 0U; index < bytes.size(); ++index) {
        bytes[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
    return HashBytes(hasher, bytes);
}

[[nodiscard]] bool HashU64(
    l2flow::common::Sha256Hasher* hasher,
    std::uint64_t value) noexcept {
    std::array<std::byte, 8U> bytes{};
    for (std::size_t index = 0U; index < bytes.size(); ++index) {
        bytes[index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
    return HashBytes(hasher, bytes);
}

[[nodiscard]] bool HashSize(
    l2flow::common::Sha256Hasher* hasher,
    std::size_t value) noexcept {
    if constexpr (sizeof(std::size_t) > sizeof(std::uint64_t)) {
        if (value > static_cast<std::size_t>(
                        std::numeric_limits<std::uint64_t>::max())) {
            return false;
        }
    }
    return HashU64(hasher, static_cast<std::uint64_t>(value));
}

[[nodiscard]] bool HashDomain(
    l2flow::common::Sha256Hasher* hasher,
    std::string_view domain) noexcept {
    const std::span<const char> characters(
        domain.data(), domain.size());
    constexpr std::array<std::byte, 1U> separator{std::byte{0U}};
    return hasher != nullptr &&
           hasher->Update(std::as_bytes(characters)) &&
           hasher->Update(separator);
}

[[nodiscard]] bool ComputeGenesisDigest(
    const ObservedInstrumentDirectoryConfigV2& config,
    l2flow::common::Sha256Digest* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    l2flow::common::Sha256Hasher hasher;
    return HashDomain(&hasher, kCatalogGenesisHashDomainV2) &&
           HashU64(&hasher, config.session_epoch) &&
           HashSize(&hasher, config.capacity) &&
           HashU8(
               &hasher,
               static_cast<std::uint8_t>(
                   ObservedInstrumentCatalogScopeV2::kObservedOnly)) &&
           HashU8(&hasher, 0U) && hasher.Finalize(output);
}

[[nodiscard]] bool ComputeNextCatalogDigest(
    const l2flow::common::Sha256Digest& previous,
    std::uint64_t catalog_generation,
    std::size_t ordinal,
    std::uint32_t instrument_id,
    const InstrumentKeyViewV1& key,
    const ObservedInstrumentMetadataV2& metadata,
    std::uint64_t first_capture_sequence,
    l2flow::common::Sha256Digest* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    l2flow::common::Sha256Hasher hasher;
    return HashDomain(&hasher, kCatalogBindHashDomainV2) &&
           hasher.Update(previous) &&
           HashU64(&hasher, catalog_generation) &&
           HashSize(&hasher, ordinal) &&
           HashU32(&hasher, instrument_id) &&
           HashU64(&hasher, first_capture_sequence) &&
           HashU8(&hasher, static_cast<std::uint8_t>(key.market)) &&
           HashSize(&hasher, key.security_id_source.size()) &&
           HashBytes(&hasher, key.security_id_source) &&
           HashSize(&hasher, key.security_id.size()) &&
           HashBytes(&hasher, key.security_id) &&
           HashU8(
               &hasher,
               static_cast<std::uint8_t>(metadata.quantity_unit)) &&
           HashU8(
               &hasher,
               static_cast<std::uint8_t>(metadata.security_type)) &&
           HashU8(
               &hasher,
               static_cast<std::uint8_t>(metadata.asset_scope)) &&
           hasher.Finalize(output);
}

struct FrozenEntryStateV2 final {
    ObservedInstrumentBindingStateV2 binding_state =
        ObservedInstrumentBindingStateV2::kUnbound;
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

class SlotPreservationLockV2 final {
public:
    explicit SlotPreservationLockV2(
        std::atomic_flag* flag) noexcept
        : flag_(flag) {
        while (flag_->test_and_set(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }

    SlotPreservationLockV2(
        const SlotPreservationLockV2&) = delete;
    SlotPreservationLockV2& operator=(
        const SlotPreservationLockV2&) = delete;

    ~SlotPreservationLockV2() {
        flag_->clear(std::memory_order_release);
    }

private:
    std::atomic_flag* flag_;
};

struct StableSlotV2 final {
    std::atomic<ObservedInstrumentBindingStateV2> binding_state{
        ObservedInstrumentBindingStateV2::kUnbound};
    std::uint32_t instrument_id = 0U;
    std::size_t ordinal = std::numeric_limits<std::size_t>::max();
    InstrumentKeyV1 key{};
    ObservedInstrumentMetadataV2 metadata{};
    std::uint64_t first_capture_sequence = 0U;
    std::atomic<std::uint8_t> availability_flags{0U};
    std::atomic<bool> factor_eligible{false};
    std::atomic<std::uint64_t> first_ingress_sequence{0U};
    std::atomic<std::uint64_t> last_ingress_sequence{0U};
    mutable std::atomic_flag preservation_lock = ATOMIC_FLAG_INIT;
    std::atomic<std::uint64_t> last_preserved_snapshot_epoch{0U};
    FrozenVersionNodeV2* frozen_versions = nullptr;
    FrozenVersionNodeV2* reusable_versions = nullptr;
};

using KeyIndexV2 = std::unordered_map<
    InstrumentKeyV1,
    std::size_t,
    InstrumentKeyHashV2,
    InstrumentKeyEqualV2>;

struct StableStorageV2 final {
    explicit StableStorageV2(
        ObservedInstrumentDirectoryConfigV2 value_config)
        : config(value_config),
          slots(std::make_unique<StableSlotV2[]>(
              value_config.capacity)),
          frozen_version_pool(value_config.capacity),
          live_snapshot_epochs(
              std::make_shared<const std::vector<std::uint64_t>>()) {
        key_index.max_load_factor(0.7F);
        key_index.reserve(value_config.capacity);
    }

    ObservedInstrumentDirectoryConfigV2 config{};
    std::unique_ptr<StableSlotV2[]> slots;
    KeyIndexV2 key_index;
    mutable std::mutex state_mutex;
    mutable std::mutex snapshot_mutex;
    SnapshotMutationGateV2 snapshot_mutation_gate;
    FrozenVersionPoolV2 frozen_version_pool;
    std::atomic<std::uint64_t> current_snapshot_epoch{0U};
    std::atomic<std::size_t> current_snapshot_bound_count{0U};
    std::uint64_t last_snapshot_epoch = 0U;
    std::atomic<bool> snapshot_dirty{true};
    std::weak_ptr<const ObservedInstrumentCatalogSnapshotV2>
        published_snapshot;
    mutable std::mutex live_snapshot_mutex;
    std::shared_ptr<const std::vector<std::uint64_t>>
        live_snapshot_epochs;
    std::uint64_t last_bind_call_capture_sequence = 0U;
    std::uint64_t catalog_generation = 0U;
    std::uint64_t data_state_generation = 0U;
    l2flow::common::Sha256Digest catalog_digest{};
    std::size_t bound_count = 0U;
    std::size_t available_count = 0U;
    std::size_t snapshot_available_count = 0U;
    std::size_t tick_available_count = 0U;
    std::size_t factor_eligible_count = 0U;
};

[[nodiscard]] ObservedInstrumentDirectoryErrorV2
RegisterLiveSnapshotEpoch(
    StableStorageV2* storage,
    std::uint64_t snapshot_epoch) noexcept {
    if (storage == nullptr || snapshot_epoch == 0U) {
        return ObservedInstrumentDirectoryErrorV2::kUnexpectedFailure;
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
            return ObservedInstrumentDirectoryErrorV2::
                kUnexpectedFailure;
        }
        if (current->size() ==
            std::numeric_limits<std::size_t>::max()) {
            return ObservedInstrumentDirectoryErrorV2::
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
        return ObservedInstrumentDirectoryErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return ObservedInstrumentDirectoryErrorV2::kResourceExhausted;
    } catch (const std::length_error&) {
        return ObservedInstrumentDirectoryErrorV2::kResourceExhausted;
    } catch (...) {
        return ObservedInstrumentDirectoryErrorV2::kUnexpectedFailure;
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
    state.binding_state =
        slot.binding_state.load(std::memory_order_acquire);
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

[[nodiscard]] ObservedInstrumentDirectoryErrorV2
PreserveCurrentSnapshotState(
    StableStorageV2* storage,
    StableSlotV2* slot) noexcept {
    if (storage == nullptr || slot == nullptr) {
        return ObservedInstrumentDirectoryErrorV2::kUnexpectedFailure;
    }
    const std::uint64_t snapshot_epoch =
        storage->current_snapshot_epoch.load(std::memory_order_acquire);
    if (snapshot_epoch == 0U ||
        slot->last_preserved_snapshot_epoch.load(
            std::memory_order_acquire) == snapshot_epoch) {
        return ObservedInstrumentDirectoryErrorV2::kNone;
    }
    if (slot->ordinal >=
        storage->current_snapshot_bound_count.load(
            std::memory_order_acquire)) {
        slot->last_preserved_snapshot_epoch.store(
            snapshot_epoch, std::memory_order_release);
        return ObservedInstrumentDirectoryErrorV2::kNone;
    }

    try {
        const std::shared_ptr<
            const std::vector<std::uint64_t>>
            live_epochs = std::atomic_load_explicit(
                &storage->live_snapshot_epochs,
                std::memory_order_acquire);
        if (live_epochs == nullptr) {
            return ObservedInstrumentDirectoryErrorV2::
                kUnexpectedFailure;
        }
        const SlotPreservationLockV2 lock(&slot->preservation_lock);
        if (slot->last_preserved_snapshot_epoch.load(
                std::memory_order_relaxed) == snapshot_epoch) {
            return ObservedInstrumentDirectoryErrorV2::kNone;
        }
        CompactFrozenVersions(slot, *live_epochs);
        if (live_epochs->empty() ||
            live_epochs->front() > snapshot_epoch) {
            slot->last_preserved_snapshot_epoch.store(
                snapshot_epoch, std::memory_order_release);
            return ObservedInstrumentDirectoryErrorV2::kNone;
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
        return ObservedInstrumentDirectoryErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return ObservedInstrumentDirectoryErrorV2::kResourceExhausted;
    } catch (...) {
        return ObservedInstrumentDirectoryErrorV2::kUnexpectedFailure;
    }
}

[[nodiscard]] FrozenEntryStateV2 FrozenEntryStateAtSnapshot(
    const StableSlotV2& slot,
    std::uint64_t snapshot_epoch) noexcept {
    const SlotPreservationLockV2 lock(&slot.preservation_lock);
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

[[nodiscard]] ObservedInstrumentDirectoryErrorV2 FillLiveEntry(
    const StableStorageV2& storage,
    std::size_t ordinal,
    ObservedInstrumentEntryViewV2* output) noexcept {
    if (output == nullptr) {
        return ObservedInstrumentDirectoryErrorV2::kNullOutput;
    }
    *output = ObservedInstrumentEntryViewV2{};
    if (ordinal >= storage.config.capacity) {
        return ObservedInstrumentDirectoryErrorV2::kNotFound;
    }
    const StableSlotV2& slot = storage.slots[ordinal];
    const ObservedInstrumentBindingStateV2 initial_state =
        slot.binding_state.load(std::memory_order_acquire);
    output->binding_state = initial_state;
    if (initial_state ==
        ObservedInstrumentBindingStateV2::kBinding) {
        return ObservedInstrumentDirectoryErrorV2::kBindingInProgress;
    }
    if (initial_state ==
        ObservedInstrumentBindingStateV2::kUnbound) {
        return ObservedInstrumentDirectoryErrorV2::kNotFound;
    }

    output->instrument_id = slot.instrument_id;
    output->ordinal = slot.ordinal;
    output->key = KeyView(slot.key);
    output->metadata = slot.metadata;
    output->first_capture_sequence = slot.first_capture_sequence;
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
    output->binding_state =
        slot.binding_state.load(std::memory_order_acquire);
    return output->bound()
               ? ObservedInstrumentDirectoryErrorV2::kNone
               : ObservedInstrumentDirectoryErrorV2::kBindingInProgress;
}

void FillFrozenEntry(
    const StableStorageV2& storage,
    std::size_t ordinal,
    const FrozenEntryStateV2& frozen,
    ObservedInstrumentEntryViewV2* output) noexcept {
    *output = ObservedInstrumentEntryViewV2{};
    const StableSlotV2& slot = storage.slots[ordinal];
    output->instrument_id = slot.instrument_id;
    output->ordinal = slot.ordinal;
    output->key = KeyView(slot.key);
    output->metadata = slot.metadata;
    output->first_capture_sequence = slot.first_capture_sequence;
    output->binding_state = frozen.binding_state;
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

[[nodiscard]] bool SlotIdentityMatches(
    const StableStorageV2& storage,
    std::size_t ordinal,
    std::uint32_t instrument_id) noexcept {
    if (instrument_id == 0U || ordinal >= storage.config.capacity) {
        return false;
    }
    const StableSlotV2& slot = storage.slots[ordinal];
    const ObservedInstrumentBindingStateV2 state =
        slot.binding_state.load(std::memory_order_acquire);
    return (state ==
                ObservedInstrumentBindingStateV2::kBoundNoData ||
            state == ObservedInstrumentBindingStateV2::kAvailable) &&
           slot.ordinal == ordinal &&
           slot.instrument_id == instrument_id;
}

[[nodiscard]] bool SnapshotCountsValid(
    const StableStorageV2& storage) noexcept {
    return storage.factor_eligible_count <=
               storage.snapshot_available_count &&
           storage.snapshot_available_count <=
               storage.available_count &&
           storage.tick_available_count <= storage.available_count &&
           storage.available_count <= storage.bound_count &&
           storage.bound_count <= storage.config.capacity &&
           storage.catalog_generation ==
               static_cast<std::uint64_t>(storage.bound_count);
}

}  // namespace

class ObservedInstrumentCatalogSnapshotV2::Impl final {
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

class ObservedInstrumentDirectoryV2::Impl final {
public:
    explicit Impl(std::shared_ptr<StableStorageV2> value) noexcept
        : storage(std::move(value)) {}

    std::shared_ptr<StableStorageV2> storage;
};

std::string_view ObservedInstrumentDirectoryErrorNameV2(
    ObservedInstrumentDirectoryErrorV2 error) noexcept {
    switch (error) {
        case ObservedInstrumentDirectoryErrorV2::kNone:
            return "none";
        case ObservedInstrumentDirectoryErrorV2::kNullOutput:
            return "null_output";
        case ObservedInstrumentDirectoryErrorV2::kInvalidConfiguration:
            return "invalid_configuration";
        case ObservedInstrumentDirectoryErrorV2::kInvalidDataKind:
            return "invalid_data_kind";
        case ObservedInstrumentDirectoryErrorV2::kInvalidKey:
            return "invalid_key";
        case ObservedInstrumentDirectoryErrorV2::kInvalidMetadata:
            return "invalid_metadata";
        case ObservedInstrumentDirectoryErrorV2::kInvalidSequence:
            return "invalid_sequence";
        case ObservedInstrumentDirectoryErrorV2::kSequenceNotIncreasing:
            return "sequence_not_increasing";
        case ObservedInstrumentDirectoryErrorV2::kNotFound:
            return "not_found";
        case ObservedInstrumentDirectoryErrorV2::kBindingInProgress:
            return "binding_in_progress";
        case ObservedInstrumentDirectoryErrorV2::kMetadataConflict:
            return "metadata_conflict";
        case ObservedInstrumentDirectoryErrorV2::kIdentityMismatch:
            return "identity_mismatch";
        case ObservedInstrumentDirectoryErrorV2::kCapacityExhausted:
            return "capacity_exhausted";
        case ObservedInstrumentDirectoryErrorV2::kPrerequisiteUnavailable:
            return "prerequisite_unavailable";
        case ObservedInstrumentDirectoryErrorV2::kGenerationExhausted:
            return "generation_exhausted";
        case ObservedInstrumentDirectoryErrorV2::kHashFailure:
            return "hash_failure";
        case ObservedInstrumentDirectoryErrorV2::kResourceExhausted:
            return "resource_exhausted";
        case ObservedInstrumentDirectoryErrorV2::kUnexpectedFailure:
            return "unexpected_failure";
    }
    return "invalid_observed_instrument_directory_error_v2";
}

ObservedInstrumentCatalogSnapshotV2::
    ObservedInstrumentCatalogSnapshotV2(
        std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ObservedInstrumentCatalogSnapshotV2::
    ~ObservedInstrumentCatalogSnapshotV2() {
    if (impl_ != nullptr && impl_->live_epoch_registered) {
        ReleaseLiveSnapshotEpoch(
            const_cast<StableStorageV2*>(impl_->storage.get()),
            impl_->snapshot_epoch);
    }
}

std::uint64_t
ObservedInstrumentCatalogSnapshotV2::session_epoch() const noexcept {
    return impl_->storage->config.session_epoch;
}

std::size_t
ObservedInstrumentCatalogSnapshotV2::capacity() const noexcept {
    return impl_->storage->config.capacity;
}

ObservedInstrumentCatalogScopeV2
ObservedInstrumentCatalogSnapshotV2::catalog_scope() const noexcept {
    return ObservedInstrumentCatalogScopeV2::kObservedOnly;
}

bool ObservedInstrumentCatalogSnapshotV2::coverage_complete()
    const noexcept {
    return false;
}

std::uint64_t
ObservedInstrumentCatalogSnapshotV2::catalog_generation()
    const noexcept {
    return impl_->catalog_generation;
}

std::uint64_t
ObservedInstrumentCatalogSnapshotV2::data_state_generation()
    const noexcept {
    return impl_->data_state_generation;
}

const l2flow::common::Sha256Digest&
ObservedInstrumentCatalogSnapshotV2::catalog_digest() const noexcept {
    return impl_->catalog_digest;
}

std::size_t
ObservedInstrumentCatalogSnapshotV2::bound_count() const noexcept {
    return impl_->bound_count;
}

std::size_t
ObservedInstrumentCatalogSnapshotV2::available_count() const noexcept {
    return impl_->available_count;
}

std::size_t
ObservedInstrumentCatalogSnapshotV2::snapshot_available_count()
    const noexcept {
    return impl_->snapshot_available_count;
}

std::size_t
ObservedInstrumentCatalogSnapshotV2::tick_available_count()
    const noexcept {
    return impl_->tick_available_count;
}

std::size_t
ObservedInstrumentCatalogSnapshotV2::factor_eligible_count()
    const noexcept {
    return impl_->factor_eligible_count;
}

ObservedInstrumentDirectoryErrorV2
ObservedInstrumentCatalogSnapshotV2::EntryAt(
    std::size_t ordinal,
    ObservedInstrumentEntryViewV2* output) const noexcept {
    if (output == nullptr) {
        return ObservedInstrumentDirectoryErrorV2::kNullOutput;
    }
    *output = ObservedInstrumentEntryViewV2{};
    if (ordinal >= impl_->bound_count ||
        impl_->snapshot_epoch == 0U) {
        return ObservedInstrumentDirectoryErrorV2::kNotFound;
    }
    const FrozenEntryStateV2 frozen =
        FrozenEntryStateAtSnapshot(
            impl_->storage->slots[ordinal],
            impl_->snapshot_epoch);
    FillFrozenEntry(
        *impl_->storage, ordinal, frozen, output);
    return output->bound()
               ? ObservedInstrumentDirectoryErrorV2::kNone
               : ObservedInstrumentDirectoryErrorV2::kUnexpectedFailure;
}

ObservedInstrumentDirectoryErrorV2
ObservedInstrumentCatalogSnapshotV2::LookupById(
    std::uint32_t instrument_id,
    ObservedInstrumentEntryViewV2* output) const noexcept {
    if (output == nullptr) {
        return ObservedInstrumentDirectoryErrorV2::kNullOutput;
    }
    *output = ObservedInstrumentEntryViewV2{};
    if (instrument_id == 0U) {
        return ObservedInstrumentDirectoryErrorV2::kIdentityMismatch;
    }
    const std::size_t ordinal =
        static_cast<std::size_t>(instrument_id - 1U);
    return EntryAt(ordinal, output);
}

ObservedInstrumentDirectoryV2::ObservedInstrumentDirectoryV2(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ObservedInstrumentDirectoryV2::~ObservedInstrumentDirectoryV2() = default;

ObservedInstrumentDirectoryErrorV2
ObservedInstrumentDirectoryV2::Create(
    ObservedInstrumentDirectoryConfigV2 config,
    std::unique_ptr<ObservedInstrumentDirectoryV2>* output) noexcept {
    if (output == nullptr) {
        return ObservedInstrumentDirectoryErrorV2::kNullOutput;
    }
    output->reset();
    if (config.capacity == 0U || config.session_epoch == 0U ||
        config.capacity >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max())) {
        return ObservedInstrumentDirectoryErrorV2::
            kInvalidConfiguration;
    }
    try {
        auto storage = std::make_shared<StableStorageV2>(config);
        if (!ComputeGenesisDigest(
                config, &storage->catalog_digest)) {
            return ObservedInstrumentDirectoryErrorV2::kHashFailure;
        }
        auto impl = std::make_unique<Impl>(std::move(storage));
        output->reset(
            new ObservedInstrumentDirectoryV2(std::move(impl)));
        return ObservedInstrumentDirectoryErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return ObservedInstrumentDirectoryErrorV2::kResourceExhausted;
    } catch (const std::length_error&) {
        return ObservedInstrumentDirectoryErrorV2::kResourceExhausted;
    } catch (...) {
        return ObservedInstrumentDirectoryErrorV2::kUnexpectedFailure;
    }
}

ObservedInstrumentDirectoryErrorV2
ObservedInstrumentDirectoryV2::BindOrGet(
    const InstrumentKeyViewV1& key,
    ObservedInstrumentMetadataV2 metadata,
    std::uint64_t first_capture_sequence,
    ObservedInstrumentBindResultV2* output) noexcept {
    if (output == nullptr) {
        return ObservedInstrumentDirectoryErrorV2::kNullOutput;
    }
    *output = ObservedInstrumentBindResultV2{};
    if (!KeyValid(key)) {
        return ObservedInstrumentDirectoryErrorV2::kInvalidKey;
    }
    if (!MetadataValid(metadata)) {
        return ObservedInstrumentDirectoryErrorV2::kInvalidMetadata;
    }
    if (first_capture_sequence == 0U ||
        first_capture_sequence ==
            std::numeric_limits<std::uint64_t>::max()) {
        return ObservedInstrumentDirectoryErrorV2::kInvalidSequence;
    }

    try {
        StableStorageV2& storage = *impl_->storage;
        std::lock_guard<std::mutex> lock(storage.state_mutex);
        if (first_capture_sequence <=
            storage.last_bind_call_capture_sequence) {
            return ObservedInstrumentDirectoryErrorV2::
                kSequenceNotIncreasing;
        }

        const auto found = storage.key_index.find(key);
        if (found != storage.key_index.end()) {
            const std::size_t ordinal = found->second;
            if (ordinal >= storage.bound_count ||
                !MetadataEqual(
                    storage.slots[ordinal].metadata, metadata)) {
                return ordinal < storage.bound_count
                           ? ObservedInstrumentDirectoryErrorV2::
                                 kMetadataConflict
                           : ObservedInstrumentDirectoryErrorV2::
                                 kUnexpectedFailure;
            }
            const ObservedInstrumentDirectoryErrorV2 fill_error =
                FillLiveEntry(storage, ordinal, &output->entry);
            if (fill_error !=
                ObservedInstrumentDirectoryErrorV2::kNone) {
                *output = ObservedInstrumentBindResultV2{};
                return fill_error;
            }
            output->catalog_generation =
                storage.catalog_generation;
            output->bound_count = storage.bound_count;
            output->catalog_digest = storage.catalog_digest;
            storage.last_bind_call_capture_sequence =
                first_capture_sequence;
            return ObservedInstrumentDirectoryErrorV2::kNone;
        }

        if (storage.bound_count >= storage.config.capacity) {
            return ObservedInstrumentDirectoryErrorV2::
                kCapacityExhausted;
        }
        if (storage.catalog_generation ==
            std::numeric_limits<std::uint64_t>::max()) {
            return ObservedInstrumentDirectoryErrorV2::
                kGenerationExhausted;
        }

        const std::size_t ordinal = storage.bound_count;
        const std::uint32_t instrument_id =
            static_cast<std::uint32_t>(ordinal + 1U);
        const std::uint64_t next_generation =
            storage.catalog_generation + 1U;
        l2flow::common::Sha256Digest next_digest{};
        if (!ComputeNextCatalogDigest(
                storage.catalog_digest,
                next_generation,
                ordinal,
                instrument_id,
                key,
                metadata,
                first_capture_sequence,
                &next_digest)) {
            return ObservedInstrumentDirectoryErrorV2::kHashFailure;
        }

        InstrumentKeyV1 owned_key{};
        owned_key.market = key.market;
        owned_key.security_id_source.assign(
            key.security_id_source.begin(),
            key.security_id_source.end());
        owned_key.security_id.assign(
            key.security_id.begin(), key.security_id.end());
        const auto inserted =
            storage.key_index.emplace(owned_key, ordinal);
        if (!inserted.second) {
            return ObservedInstrumentDirectoryErrorV2::
                kUnexpectedFailure;
        }

        StableSlotV2& slot = storage.slots[ordinal];
        slot.instrument_id = instrument_id;
        slot.ordinal = ordinal;
        slot.key = std::move(owned_key);
        slot.metadata = metadata;
        slot.first_capture_sequence = first_capture_sequence;
        slot.binding_state.store(
            ObservedInstrumentBindingStateV2::kBinding,
            std::memory_order_release);

        storage.catalog_generation = next_generation;
        storage.catalog_digest = next_digest;
        ++storage.bound_count;
        storage.last_bind_call_capture_sequence =
            first_capture_sequence;
        slot.binding_state.store(
            ObservedInstrumentBindingStateV2::kBoundNoData,
            std::memory_order_release);
        storage.snapshot_dirty.store(true, std::memory_order_release);

        const ObservedInstrumentDirectoryErrorV2 fill_error =
            FillLiveEntry(storage, ordinal, &output->entry);
        if (fill_error !=
            ObservedInstrumentDirectoryErrorV2::kNone) {
            *output = ObservedInstrumentBindResultV2{};
            return fill_error;
        }
        output->newly_bound = true;
        output->catalog_generation = storage.catalog_generation;
        output->bound_count = storage.bound_count;
        output->catalog_digest = storage.catalog_digest;
        return ObservedInstrumentDirectoryErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return ObservedInstrumentDirectoryErrorV2::kResourceExhausted;
    } catch (const std::length_error&) {
        return ObservedInstrumentDirectoryErrorV2::kResourceExhausted;
    } catch (...) {
        return ObservedInstrumentDirectoryErrorV2::kUnexpectedFailure;
    }
}

ObservedInstrumentDirectoryErrorV2
ObservedInstrumentDirectoryV2::BindOrGet(
    const InstrumentKeyV1& key,
    ObservedInstrumentMetadataV2 metadata,
    std::uint64_t first_capture_sequence,
    ObservedInstrumentBindResultV2* output) noexcept {
    return BindOrGet(
        KeyView(key), metadata, first_capture_sequence, output);
}

ObservedInstrumentDirectoryErrorV2
ObservedInstrumentDirectoryV2::LookupByKey(
    const InstrumentKeyViewV1& key,
    ObservedInstrumentEntryViewV2* output) const noexcept {
    if (output == nullptr) {
        return ObservedInstrumentDirectoryErrorV2::kNullOutput;
    }
    *output = ObservedInstrumentEntryViewV2{};
    if (!KeyValid(key)) {
        return ObservedInstrumentDirectoryErrorV2::kInvalidKey;
    }
    try {
        const StableStorageV2& storage = *impl_->storage;
        std::lock_guard<std::mutex> lock(storage.state_mutex);
        const auto found = storage.key_index.find(key);
        if (found == storage.key_index.end()) {
            return ObservedInstrumentDirectoryErrorV2::kNotFound;
        }
        return FillLiveEntry(storage, found->second, output);
    } catch (...) {
        return ObservedInstrumentDirectoryErrorV2::kUnexpectedFailure;
    }
}

ObservedInstrumentDirectoryErrorV2
ObservedInstrumentDirectoryV2::LookupByKey(
    const InstrumentKeyV1& key,
    ObservedInstrumentEntryViewV2* output) const noexcept {
    return LookupByKey(KeyView(key), output);
}

ObservedInstrumentDirectoryErrorV2
ObservedInstrumentDirectoryV2::LookupById(
    std::uint32_t instrument_id,
    ObservedInstrumentEntryViewV2* output) const noexcept {
    if (output == nullptr) {
        return ObservedInstrumentDirectoryErrorV2::kNullOutput;
    }
    *output = ObservedInstrumentEntryViewV2{};
    if (instrument_id == 0U) {
        return ObservedInstrumentDirectoryErrorV2::kIdentityMismatch;
    }
    const std::size_t ordinal =
        static_cast<std::size_t>(instrument_id - 1U);
    return FillLiveEntry(*impl_->storage, ordinal, output);
}

ObservedInstrumentDirectoryErrorV2
ObservedInstrumentDirectoryV2::LookupByOrdinal(
    std::size_t ordinal,
    ObservedInstrumentEntryViewV2* output) const noexcept {
    return FillLiveEntry(*impl_->storage, ordinal, output);
}

ObservedInstrumentDirectoryErrorV2
ObservedInstrumentDirectoryV2::ResolveBoundId(
    std::uint32_t instrument_id,
    std::size_t* ordinal) const noexcept {
    if (ordinal == nullptr) {
        return ObservedInstrumentDirectoryErrorV2::kNullOutput;
    }
    *ordinal = std::numeric_limits<std::size_t>::max();
    if (instrument_id == 0U) {
        return ObservedInstrumentDirectoryErrorV2::kIdentityMismatch;
    }
    const std::size_t candidate =
        static_cast<std::size_t>(instrument_id - 1U);
    const StableStorageV2& storage = *impl_->storage;
    if (candidate >= storage.config.capacity) {
        return ObservedInstrumentDirectoryErrorV2::kNotFound;
    }
    const StableSlotV2& slot = storage.slots[candidate];
    const ObservedInstrumentBindingStateV2 state =
        slot.binding_state.load(std::memory_order_acquire);
    if (state == ObservedInstrumentBindingStateV2::kUnbound) {
        return ObservedInstrumentDirectoryErrorV2::kNotFound;
    }
    if (state == ObservedInstrumentBindingStateV2::kBinding) {
        return ObservedInstrumentDirectoryErrorV2::kBindingInProgress;
    }
    if (slot.instrument_id != instrument_id ||
        slot.ordinal != candidate) {
        return ObservedInstrumentDirectoryErrorV2::kIdentityMismatch;
    }
    *ordinal = candidate;
    return ObservedInstrumentDirectoryErrorV2::kNone;
}

ObservedInstrumentDirectoryErrorV2
ObservedInstrumentDirectoryV2::MarkApplied(
    std::size_t ordinal,
    std::uint32_t instrument_id,
    ObservedInstrumentDataKindV2 kind,
    std::uint64_t ingress_sequence) noexcept {
    if (!DataKindValid(kind)) {
        return ObservedInstrumentDirectoryErrorV2::kInvalidDataKind;
    }
    if (ingress_sequence == 0U ||
        ingress_sequence ==
            std::numeric_limits<std::uint64_t>::max()) {
        return ObservedInstrumentDirectoryErrorV2::kInvalidSequence;
    }
    StableStorageV2& storage = *impl_->storage;
    if (!SlotIdentityMatches(storage, ordinal, instrument_id)) {
        return ObservedInstrumentDirectoryErrorV2::kIdentityMismatch;
    }
    StableSlotV2& slot = storage.slots[ordinal];
    const std::uint8_t flag = DataKindFlag(kind);

    try {
        const SnapshotMutationLeaseV2 mutation(
            &storage.snapshot_mutation_gate);
        if (!mutation.acquired()) {
            return ObservedInstrumentDirectoryErrorV2::
                kUnexpectedFailure;
        }
        const ObservedInstrumentDirectoryErrorV2 preserve_error =
            PreserveCurrentSnapshotState(&storage, &slot);
        if (preserve_error !=
            ObservedInstrumentDirectoryErrorV2::kNone) {
            return preserve_error;
        }

        const std::uint8_t observed_flags =
            slot.availability_flags.load(std::memory_order_acquire);
        if ((observed_flags & flag) != 0U &&
            slot.binding_state.load(std::memory_order_acquire) ==
                ObservedInstrumentBindingStateV2::kAvailable) {
            AtomicMinimum(
                &slot.first_ingress_sequence, ingress_sequence);
            AtomicMaximum(
                &slot.last_ingress_sequence, ingress_sequence);
            storage.snapshot_dirty.store(
                true, std::memory_order_release);
            return ObservedInstrumentDirectoryErrorV2::kNone;
        }

        std::lock_guard<std::mutex> lock(storage.state_mutex);
        if (!SlotIdentityMatches(storage, ordinal, instrument_id)) {
            return ObservedInstrumentDirectoryErrorV2::
                kIdentityMismatch;
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
            return ObservedInstrumentDirectoryErrorV2::kNone;
        }
        if (storage.data_state_generation ==
            std::numeric_limits<std::uint64_t>::max()) {
            return ObservedInstrumentDirectoryErrorV2::
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
            kind == ObservedInstrumentDataKindV2::kSnapshot) {
            ++storage.snapshot_available_count;
        }
        if (first_kind &&
            kind == ObservedInstrumentDataKindV2::kTick) {
            ++storage.tick_available_count;
        }
        slot.binding_state.store(
            ObservedInstrumentBindingStateV2::kAvailable,
            std::memory_order_release);
        ++storage.data_state_generation;
        storage.snapshot_dirty.store(true, std::memory_order_release);
        return ObservedInstrumentDirectoryErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return ObservedInstrumentDirectoryErrorV2::kResourceExhausted;
    } catch (...) {
        return ObservedInstrumentDirectoryErrorV2::kUnexpectedFailure;
    }
}

ObservedInstrumentDirectoryErrorV2
ObservedInstrumentDirectoryV2::SetFactorEligible(
    std::size_t ordinal,
    std::uint32_t instrument_id,
    bool eligible) noexcept {
    StableStorageV2& storage = *impl_->storage;
    if (!SlotIdentityMatches(storage, ordinal, instrument_id)) {
        return ObservedInstrumentDirectoryErrorV2::kIdentityMismatch;
    }
    StableSlotV2& fast_slot = storage.slots[ordinal];
    if (fast_slot.factor_eligible.load(std::memory_order_acquire) ==
        eligible) {
        return ObservedInstrumentDirectoryErrorV2::kNone;
    }
    try {
        const SnapshotMutationLeaseV2 mutation(
            &storage.snapshot_mutation_gate);
        if (!mutation.acquired()) {
            return ObservedInstrumentDirectoryErrorV2::
                kUnexpectedFailure;
        }
        std::lock_guard<std::mutex> lock(storage.state_mutex);
        if (!SlotIdentityMatches(storage, ordinal, instrument_id)) {
            return ObservedInstrumentDirectoryErrorV2::
                kIdentityMismatch;
        }
        StableSlotV2& slot = fast_slot;
        const bool current =
            slot.factor_eligible.load(std::memory_order_acquire);
        if (current == eligible) {
            return ObservedInstrumentDirectoryErrorV2::kNone;
        }
        const std::uint8_t flags =
            slot.availability_flags.load(std::memory_order_acquire);
        if (eligible &&
            (slot.binding_state.load(std::memory_order_acquire) !=
                 ObservedInstrumentBindingStateV2::kAvailable ||
             (flags & kHasSnapshotV2) == 0U)) {
            return ObservedInstrumentDirectoryErrorV2::
                kPrerequisiteUnavailable;
        }
        if (storage.data_state_generation ==
            std::numeric_limits<std::uint64_t>::max()) {
            return ObservedInstrumentDirectoryErrorV2::
                kGenerationExhausted;
        }
        const ObservedInstrumentDirectoryErrorV2 preserve_error =
            PreserveCurrentSnapshotState(&storage, &slot);
        if (preserve_error !=
            ObservedInstrumentDirectoryErrorV2::kNone) {
            return preserve_error;
        }
        if (eligible) {
            ++storage.factor_eligible_count;
        } else {
            if (storage.factor_eligible_count == 0U) {
                return ObservedInstrumentDirectoryErrorV2::
                    kUnexpectedFailure;
            }
            --storage.factor_eligible_count;
        }
        slot.factor_eligible.store(eligible, std::memory_order_release);
        ++storage.data_state_generation;
        storage.snapshot_dirty.store(true, std::memory_order_release);
        return ObservedInstrumentDirectoryErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return ObservedInstrumentDirectoryErrorV2::kResourceExhausted;
    } catch (...) {
        return ObservedInstrumentDirectoryErrorV2::kUnexpectedFailure;
    }
}

ObservedInstrumentDirectoryErrorV2
ObservedInstrumentDirectoryV2::AcquireSnapshot(
    std::shared_ptr<const ObservedInstrumentCatalogSnapshotV2>* output)
    const noexcept {
    if (output == nullptr) {
        return ObservedInstrumentDirectoryErrorV2::kNullOutput;
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
                ObservedInstrumentCatalogSnapshotV2>
                published = storage.published_snapshot.lock();
            if (published != nullptr) {
                *output = std::move(published);
                return ObservedInstrumentDirectoryErrorV2::kNone;
            }
        }

        const ClosedSnapshotMutationGateV2 closed(
            &storage.snapshot_mutation_gate);
        std::lock_guard<std::mutex> state_lock(storage.state_mutex);
        if (!SnapshotCountsValid(storage)) {
            return ObservedInstrumentDirectoryErrorV2::
                kUnexpectedFailure;
        }

        // Reaching this point means there is no reusable live published
        // snapshot. Even unchanged state receives a fresh epoch so a dead
        // epoch can be removed from the live-version set and recycled.
        if (storage.last_snapshot_epoch ==
            std::numeric_limits<std::uint64_t>::max()) {
            return ObservedInstrumentDirectoryErrorV2::
                kGenerationExhausted;
        }
        const std::uint64_t snapshot_epoch =
            storage.last_snapshot_epoch + 1U;
        if (snapshot_epoch == 0U) {
            return ObservedInstrumentDirectoryErrorV2::
                kUnexpectedFailure;
        }

        auto snapshot_impl =
            std::make_unique<
                ObservedInstrumentCatalogSnapshotV2::Impl>();
        snapshot_impl->storage =
            std::shared_ptr<const StableStorageV2>(storage_owner);
        snapshot_impl->snapshot_epoch = snapshot_epoch;
        snapshot_impl->catalog_generation =
            storage.catalog_generation;
        snapshot_impl->data_state_generation =
            storage.data_state_generation;
        snapshot_impl->catalog_digest = storage.catalog_digest;
        snapshot_impl->bound_count = storage.bound_count;
        snapshot_impl->available_count = storage.available_count;
        snapshot_impl->snapshot_available_count =
            storage.snapshot_available_count;
        snapshot_impl->tick_available_count =
            storage.tick_available_count;
        snapshot_impl->factor_eligible_count =
            storage.factor_eligible_count;

        auto* const snapshot_object =
            new ObservedInstrumentCatalogSnapshotV2(
                std::move(snapshot_impl));
        std::shared_ptr<const ObservedInstrumentCatalogSnapshotV2>
            snapshot(snapshot_object);
        const ObservedInstrumentDirectoryErrorV2 register_error =
            RegisterLiveSnapshotEpoch(&storage, snapshot_epoch);
        if (register_error !=
            ObservedInstrumentDirectoryErrorV2::kNone) {
            return register_error;
        }
        snapshot_object->impl_->live_epoch_registered = true;
        storage.last_snapshot_epoch = snapshot_epoch;
        storage.current_snapshot_bound_count.store(
            storage.bound_count, std::memory_order_relaxed);
        storage.current_snapshot_epoch.store(
            snapshot_epoch, std::memory_order_release);
        storage.snapshot_dirty.store(false, std::memory_order_release);
        storage.published_snapshot = snapshot;
        *output = std::move(snapshot);
        return ObservedInstrumentDirectoryErrorV2::kNone;
    } catch (const std::bad_alloc&) {
        return ObservedInstrumentDirectoryErrorV2::kResourceExhausted;
    } catch (const std::length_error&) {
        return ObservedInstrumentDirectoryErrorV2::kResourceExhausted;
    } catch (...) {
        return ObservedInstrumentDirectoryErrorV2::kUnexpectedFailure;
    }
}

ObservedInstrumentDirectoryErrorV2
ObservedInstrumentDirectoryV2::SnapshotStorageStats(
    ObservedInstrumentSnapshotStorageStatsV2* output)
    const noexcept {
    if (output == nullptr) {
        return ObservedInstrumentDirectoryErrorV2::kNullOutput;
    }
    *output = ObservedInstrumentSnapshotStorageStatsV2{};
    try {
        const StableStorageV2& storage = *impl_->storage;
        const std::shared_ptr<
            const std::vector<std::uint64_t>>
            live_epochs = std::atomic_load_explicit(
                &storage.live_snapshot_epochs,
                std::memory_order_acquire);
        if (live_epochs == nullptr) {
            return ObservedInstrumentDirectoryErrorV2::
                kUnexpectedFailure;
        }
        output->live_snapshot_count = live_epochs->size();
        output->allocated_version_node_count =
            storage.frozen_version_pool.allocated_node_count();
        return ObservedInstrumentDirectoryErrorV2::kNone;
    } catch (...) {
        *output = ObservedInstrumentSnapshotStorageStatsV2{};
        return ObservedInstrumentDirectoryErrorV2::kUnexpectedFailure;
    }
}

std::uint64_t
ObservedInstrumentDirectoryV2::session_epoch() const noexcept {
    return impl_->storage->config.session_epoch;
}

std::size_t ObservedInstrumentDirectoryV2::capacity() const noexcept {
    return impl_->storage->config.capacity;
}

}  // namespace l2flow::market
