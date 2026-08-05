#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace l2flow::market::detail {

[[nodiscard]] inline std::uint64_t MixOrderKeyBitsV1(
    std::uint64_t value) noexcept {
    value ^= value >> 30U;
    value *= UINT64_C(0xbf58476d1ce4e5b9);
    value ^= value >> 27U;
    value *= UINT64_C(0x94d049bb133111eb);
    value ^= value >> 31U;
    return value;
}

// Both production order keys intentionally share these exact field names and
// comparison identity. Signed values are converted modulo 2^64, preserving
// every bit without invoking signed arithmetic.
struct OrderKeyHashV1 final {
    template <typename Key>
    [[nodiscard]] std::uint64_t operator()(
        const Key& key) const noexcept {
        std::uint64_t hash = MixOrderKeyBitsV1(
            static_cast<std::uint64_t>(key.trade_date));
        hash ^= MixOrderKeyBitsV1(
            static_cast<std::uint64_t>(key.instrument_id) +
            UINT64_C(0x9e3779b97f4a7c15));
        hash ^= MixOrderKeyBitsV1(
            static_cast<std::uint64_t>(key.channel) +
            UINT64_C(0x3c6ef372fe94f82a));
        hash ^= MixOrderKeyBitsV1(
            static_cast<std::uint64_t>(key.order_id) +
            UINT64_C(0xdaa66d2c7ddef743));
        return MixOrderKeyBitsV1(hash);
    }
};

template <typename Key>
struct OrderKeyLessV1 final {
    [[nodiscard]] bool operator()(
        const Key& left,
        const Key& right) const noexcept {
        return left < right;
    }
};

// Fixed-capacity lookup plus deterministic ordered index. Values live in one
// stable contiguous slab. Robin-Hood buckets provide allocation-free average
// O(1) lookup; an index-only deterministic treap preserves ascending OrderKey
// traversal for END/finalization without storing a second Value or allocating
// nodes. No erase is required because an order state lives for the session.
template <
    typename Key,
    typename Value,
    typename Hash = OrderKeyHashV1,
    typename Less = OrderKeyLessV1<Key>>
class BoundedOrderTableV1 final {
public:
    struct InsertResult final {
        Value* value = nullptr;
        bool inserted = false;
        bool capacity_exhausted = false;
        bool invariant_failure = false;
    };

    explicit BoundedOrderTableV1(std::size_t maximum_size)
        : nodes_(ValidatedMaximumSize(maximum_size)),
          buckets_(BucketCapacity(maximum_size), kInvalidIndex) {
        static_assert(std::is_nothrow_move_assignable_v<Key>);
        static_assert(std::is_nothrow_move_assignable_v<Value>);
    }

    BoundedOrderTableV1(const BoundedOrderTableV1&) = delete;
    BoundedOrderTableV1& operator=(const BoundedOrderTableV1&) = delete;

    [[nodiscard]] std::size_t size() const noexcept { return size_; }
    [[nodiscard]] std::size_t capacity() const noexcept {
        return nodes_.size();
    }

    [[nodiscard]] Value* Find(const Key& key) noexcept {
        const std::uint32_t index = FindIndex(key);
        return index == kInvalidIndex ? nullptr : &nodes_[index].value;
    }

    [[nodiscard]] const Value* Find(const Key& key) const noexcept {
        const std::uint32_t index = FindIndex(key);
        return index == kInvalidIndex ? nullptr : &nodes_[index].value;
    }

    [[nodiscard]] InsertResult Insert(
        Key key,
        Value value) noexcept {
        const std::uint32_t existing = FindIndex(key);
        if (existing != kInvalidIndex) {
            return InsertResult{&nodes_[existing].value, false, false, false};
        }
        if (size_ >= nodes_.size()) {
            return InsertResult{nullptr, false, true, false};
        }
        const std::uint32_t index = static_cast<std::uint32_t>(size_);
        Node& node = nodes_[index];
        node.key = std::move(key);
        node.value = std::move(value);
        node.hash = hash_(node.key);
        node.ordered_left = kInvalidIndex;
        node.ordered_right = kInvalidIndex;
        node.ordered_parent = kInvalidIndex;
        InsertOrdered(index);
        if (!InsertHash(index)) {
            return InsertResult{nullptr, false, false, true};
        }
        ++size_;
        return InsertResult{&node.value, true, false, false};
    }

    template <typename Visitor>
    [[nodiscard]] bool VisitOrdered(Visitor&& visitor) {
        std::uint32_t index = Minimum(ordered_root_);
        while (index != kInvalidIndex) {
            Node& node = nodes_[index];
            const std::uint32_t next = Successor(index);
            if (!visitor(
                    static_cast<const Key&>(node.key), node.value)) {
                return false;
            }
            index = next;
        }
        return true;
    }

    template <typename Visitor>
    [[nodiscard]] bool VisitOrdered(Visitor&& visitor) const {
        std::uint32_t index = Minimum(ordered_root_);
        while (index != kInvalidIndex) {
            const Node& node = nodes_[index];
            const std::uint32_t next = Successor(index);
            if (!visitor(node.key, static_cast<const Value&>(node.value))) {
                return false;
            }
            index = next;
        }
        return true;
    }

    template <typename Visitor>
    [[nodiscard]] bool VisitOrderedRangeInclusive(
        const Key& lower,
        const Key& upper,
        Visitor&& visitor) {
        std::uint32_t index = LowerBound(lower);
        while (index != kInvalidIndex &&
               !less_(upper, nodes_[index].key)) {
            Node& node = nodes_[index];
            const std::uint32_t next = Successor(index);
            if (!visitor(
                    static_cast<const Key&>(node.key), node.value)) {
                return false;
            }
            index = next;
        }
        return true;
    }

    template <typename Visitor>
    [[nodiscard]] bool VisitOrderedRangeInclusive(
        const Key& lower,
        const Key& upper,
        Visitor&& visitor) const {
        std::uint32_t index = LowerBound(lower);
        while (index != kInvalidIndex &&
               !less_(upper, nodes_[index].key)) {
            const Node& node = nodes_[index];
            const std::uint32_t next = Successor(index);
            if (!visitor(node.key, static_cast<const Value&>(node.value))) {
                return false;
            }
            index = next;
        }
        return true;
    }

private:
    static constexpr std::uint32_t kInvalidIndex =
        std::numeric_limits<std::uint32_t>::max();

    struct Node final {
        Key key{};
        Value value{};
        std::uint64_t hash = 0U;
        std::uint32_t ordered_left = kInvalidIndex;
        std::uint32_t ordered_right = kInvalidIndex;
        std::uint32_t ordered_parent = kInvalidIndex;
    };

    [[nodiscard]] static std::size_t ValidatedMaximumSize(
        std::size_t maximum_size) {
        if (maximum_size == 0U ||
            maximum_size >=
                static_cast<std::size_t>(
                    std::numeric_limits<std::uint32_t>::max()) ||
            maximum_size >
                std::numeric_limits<std::size_t>::max() / 2U) {
            throw std::length_error("invalid bounded order capacity");
        }
        return maximum_size;
    }

    [[nodiscard]] static std::size_t BucketCapacity(
        std::size_t maximum_size) {
        const std::size_t required =
            ValidatedMaximumSize(maximum_size) * 2U;
        std::size_t capacity = 2U;
        while (capacity < required) {
            if (capacity >
                std::numeric_limits<std::size_t>::max() / 2U) {
                throw std::length_error("bounded order hash overflow");
            }
            capacity *= 2U;
        }
        return capacity;
    }

    [[nodiscard]] std::uint32_t FindIndex(
        const Key& key) const noexcept {
        const std::uint64_t hash = hash_(key);
        const std::size_t mask = buckets_.size() - 1U;
        std::size_t bucket = static_cast<std::size_t>(hash) & mask;
        std::size_t distance = 0U;
        while (distance < buckets_.size()) {
            const std::uint32_t index = buckets_[bucket];
            if (index == kInvalidIndex) {
                return kInvalidIndex;
            }
            const Node& candidate = nodes_[index];
            const std::size_t ideal =
                static_cast<std::size_t>(candidate.hash) & mask;
            const std::size_t candidate_distance =
                (bucket - ideal) & mask;
            if (candidate_distance < distance) {
                return kInvalidIndex;
            }
            if (candidate.hash == hash && candidate.key == key) {
                return index;
            }
            bucket = (bucket + 1U) & mask;
            ++distance;
        }
        return kInvalidIndex;
    }

    [[nodiscard]] bool InsertHash(std::uint32_t index) noexcept {
        const std::size_t mask = buckets_.size() - 1U;
        std::size_t bucket =
            static_cast<std::size_t>(nodes_[index].hash) & mask;
        std::size_t distance = 0U;
        std::uint32_t moving = index;
        while (distance < buckets_.size()) {
            if (buckets_[bucket] == kInvalidIndex) {
                buckets_[bucket] = moving;
                return true;
            }
            const std::uint32_t resident = buckets_[bucket];
            const std::size_t resident_ideal =
                static_cast<std::size_t>(nodes_[resident].hash) & mask;
            const std::size_t resident_distance =
                (bucket - resident_ideal) & mask;
            if (resident_distance < distance) {
                std::swap(buckets_[bucket], moving);
                distance = resident_distance;
            }
            bucket = (bucket + 1U) & mask;
            ++distance;
        }
        return false;
    }

    [[nodiscard]] bool HigherPriority(
        std::uint32_t left,
        std::uint32_t right) const noexcept {
        if (nodes_[left].hash != nodes_[right].hash) {
            return nodes_[left].hash < nodes_[right].hash;
        }
        return less_(nodes_[left].key, nodes_[right].key);
    }

    void InsertOrdered(std::uint32_t index) noexcept {
        if (ordered_root_ == kInvalidIndex) {
            ordered_root_ = index;
            return;
        }
        std::uint32_t parent = ordered_root_;
        for (;;) {
            if (less_(nodes_[index].key, nodes_[parent].key)) {
                if (nodes_[parent].ordered_left == kInvalidIndex) {
                    nodes_[parent].ordered_left = index;
                    break;
                }
                parent = nodes_[parent].ordered_left;
            } else {
                if (nodes_[parent].ordered_right == kInvalidIndex) {
                    nodes_[parent].ordered_right = index;
                    break;
                }
                parent = nodes_[parent].ordered_right;
            }
        }
        nodes_[index].ordered_parent = parent;
        while (nodes_[index].ordered_parent != kInvalidIndex) {
            parent = nodes_[index].ordered_parent;
            if (!HigherPriority(index, parent)) {
                break;
            }
            if (nodes_[parent].ordered_left == index) {
                RotateRight(parent);
            } else {
                RotateLeft(parent);
            }
        }
    }

    void RotateLeft(std::uint32_t root) noexcept {
        const std::uint32_t replacement = nodes_[root].ordered_right;
        const std::uint32_t middle = nodes_[replacement].ordered_left;
        const std::uint32_t parent = nodes_[root].ordered_parent;
        nodes_[root].ordered_right = middle;
        if (middle != kInvalidIndex) {
            nodes_[middle].ordered_parent = root;
        }
        nodes_[replacement].ordered_left = root;
        nodes_[replacement].ordered_parent = parent;
        nodes_[root].ordered_parent = replacement;
        ReplaceOrderedChild(parent, root, replacement);
    }

    void RotateRight(std::uint32_t root) noexcept {
        const std::uint32_t replacement = nodes_[root].ordered_left;
        const std::uint32_t middle = nodes_[replacement].ordered_right;
        const std::uint32_t parent = nodes_[root].ordered_parent;
        nodes_[root].ordered_left = middle;
        if (middle != kInvalidIndex) {
            nodes_[middle].ordered_parent = root;
        }
        nodes_[replacement].ordered_right = root;
        nodes_[replacement].ordered_parent = parent;
        nodes_[root].ordered_parent = replacement;
        ReplaceOrderedChild(parent, root, replacement);
    }

    void ReplaceOrderedChild(
        std::uint32_t parent,
        std::uint32_t prior,
        std::uint32_t replacement) noexcept {
        if (parent == kInvalidIndex) {
            ordered_root_ = replacement;
        } else if (nodes_[parent].ordered_left == prior) {
            nodes_[parent].ordered_left = replacement;
        } else {
            nodes_[parent].ordered_right = replacement;
        }
    }

    [[nodiscard]] std::uint32_t Minimum(
        std::uint32_t index) const noexcept {
        if (index == kInvalidIndex) {
            return kInvalidIndex;
        }
        while (nodes_[index].ordered_left != kInvalidIndex) {
            index = nodes_[index].ordered_left;
        }
        return index;
    }

    [[nodiscard]] std::uint32_t Successor(
        std::uint32_t index) const noexcept {
        if (nodes_[index].ordered_right != kInvalidIndex) {
            return Minimum(nodes_[index].ordered_right);
        }
        std::uint32_t parent = nodes_[index].ordered_parent;
        while (parent != kInvalidIndex &&
               nodes_[parent].ordered_right == index) {
            index = parent;
            parent = nodes_[index].ordered_parent;
        }
        return parent;
    }

    [[nodiscard]] std::uint32_t LowerBound(
        const Key& key) const noexcept {
        std::uint32_t current = ordered_root_;
        std::uint32_t result = kInvalidIndex;
        while (current != kInvalidIndex) {
            if (!less_(nodes_[current].key, key)) {
                result = current;
                current = nodes_[current].ordered_left;
            } else {
                current = nodes_[current].ordered_right;
            }
        }
        return result;
    }

    std::vector<Node> nodes_;
    std::vector<std::uint32_t> buckets_;
    std::size_t size_ = 0U;
    std::uint32_t ordered_root_ = kInvalidIndex;
    Hash hash_{};
    Less less_{};
};

}  // namespace l2flow::market::detail
