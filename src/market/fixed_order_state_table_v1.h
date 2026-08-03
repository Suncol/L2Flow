#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace l2flow::market::internal {

[[nodiscard]] inline std::uint64_t MixOrderStateKeyV1(
    std::uint64_t value) noexcept {
    value ^= value >> 30U;
    value *= UINT64_C(0xbf58476d1ce4e5b9);
    value ^= value >> 27U;
    value *= UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31U);
}

template <typename Key>
struct OrderStateKeyHashV1 final {
    [[nodiscard]] std::size_t operator()(
        const Key& key) const noexcept {
        const std::uint64_t date_and_instrument =
            (static_cast<std::uint64_t>(key.trade_date) << 32U) |
            static_cast<std::uint64_t>(key.instrument_id);
        const std::uint64_t channel_and_order =
            (static_cast<std::uint64_t>(
                 static_cast<std::uint32_t>(key.channel))
             << 32U) ^
            static_cast<std::uint64_t>(key.order_id);
        return static_cast<std::size_t>(MixOrderStateKeyV1(
            date_and_instrument ^
            MixOrderStateKeyV1(channel_and_order)));
    }
};

// Single-writer, insert-only state table. All storage is acquired by the
// constructor; successful insertion cannot reallocate states or invalidate
// pointers returned by Find/Insert.
template <
    typename Key,
    typename State,
    typename KeyAccessor,
    typename Hasher = OrderStateKeyHashV1<Key>>
class FixedOrderStateTableV1 final {
public:
    struct InsertResult final {
        State* state = nullptr;
        bool inserted = false;
    };

    explicit FixedOrderStateTableV1(std::size_t maximum_size)
        : maximum_size_(maximum_size),
          states_(maximum_size),
          buckets_(BucketCount(maximum_size), kEmptyIndex) {
        sorted_indices_.reserve(maximum_size_);
    }

    [[nodiscard]] State* Find(const Key& key) noexcept {
        if (last_index_ != kEmptyIndex &&
            KeyAccessor{}(
                states_[static_cast<std::size_t>(last_index_)]) == key) {
            return &states_[static_cast<std::size_t>(last_index_)];
        }
        const std::uint32_t index = FindIndex(key);
        if (index == kEmptyIndex) {
            return nullptr;
        }
        last_index_ = index;
        return &states_[static_cast<std::size_t>(index)];
    }

    [[nodiscard]] const State* Find(const Key& key) const noexcept {
        const std::uint32_t index = FindIndex(key);
        return index == kEmptyIndex
                   ? nullptr
                   : &states_[static_cast<std::size_t>(index)];
    }

    [[nodiscard]] InsertResult Insert(
        const Key& key,
        State&& state) {
        if (last_index_ != kEmptyIndex) {
            State& last =
                states_[static_cast<std::size_t>(last_index_)];
            if (KeyAccessor{}(last) == key) {
                return {&last, false};
            }
        }
        std::size_t slot = hasher_(key) & (buckets_.size() - 1U);
        for (std::size_t probe = 0U;
             probe < buckets_.size();
             ++probe) {
            const std::uint32_t index = buckets_[slot];
            if (index == kEmptyIndex) {
                if (state_count_ >= maximum_size_) {
                    return {};
                }
                KeyAccessor{}(state) = key;
                states_[state_count_] = std::move(state);
                const auto inserted_index = static_cast<std::uint32_t>(
                    state_count_);
                ++state_count_;
                buckets_[slot] = inserted_index;
                last_index_ = inserted_index;
                return {
                    &states_[static_cast<std::size_t>(inserted_index)],
                    true};
            }
            State& existing =
                states_[static_cast<std::size_t>(index)];
            if (KeyAccessor{}(existing) == key) {
                last_index_ = index;
                return {&existing, false};
            }
            slot = (slot + 1U) & (buckets_.size() - 1U);
        }
        return {};
    }

    [[nodiscard]] InsertResult FindOrEmplaceDefault(const Key& key) {
        if (last_index_ != kEmptyIndex) {
            State& last =
                states_[static_cast<std::size_t>(last_index_)];
            if (KeyAccessor{}(last) == key) {
                return {&last, false};
            }
        }
        std::size_t slot = hasher_(key) & (buckets_.size() - 1U);
        for (std::size_t probe = 0U;
             probe < buckets_.size();
             ++probe) {
            const std::uint32_t index = buckets_[slot];
            if (index == kEmptyIndex) {
                if (state_count_ >= maximum_size_) {
                    return {};
                }
                State& state = states_[state_count_];
                KeyAccessor{}(state) = key;
                const auto inserted_index = static_cast<std::uint32_t>(
                    state_count_);
                ++state_count_;
                buckets_[slot] = inserted_index;
                last_index_ = inserted_index;
                return {&state, true};
            }
            State& existing =
                states_[static_cast<std::size_t>(index)];
            if (KeyAccessor{}(existing) == key) {
                last_index_ = index;
                return {&existing, false};
            }
            slot = (slot + 1U) & (buckets_.size() - 1U);
        }
        return {};
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return state_count_;
    }

    template <typename Predicate>
    [[nodiscard]] std::span<const std::uint32_t> SortedIndices(
        Predicate include) {
        sorted_indices_.clear();
        for (std::size_t index = 0U;
             index < state_count_;
             ++index) {
            if (include(states_[index])) {
                sorted_indices_.push_back(
                    static_cast<std::uint32_t>(index));
            }
        }
        std::sort(
            sorted_indices_.begin(),
            sorted_indices_.end(),
            [this](std::uint32_t lhs, std::uint32_t rhs) noexcept {
                return KeyAccessor{}(
                           states_[static_cast<std::size_t>(lhs)]) <
                       KeyAccessor{}(
                           states_[static_cast<std::size_t>(rhs)]);
            });
        return sorted_indices_;
    }

    [[nodiscard]] State& At(std::uint32_t index) noexcept {
        return states_[static_cast<std::size_t>(index)];
    }

private:
    static constexpr std::uint32_t kEmptyIndex =
        std::numeric_limits<std::uint32_t>::max();

    [[nodiscard]] static std::size_t BucketCount(
        std::size_t maximum_size) {
        if (maximum_size == 0U ||
            maximum_size >
                static_cast<std::size_t>(kEmptyIndex) ||
            maximum_size >
                std::numeric_limits<std::size_t>::max() / 2U) {
            throw std::length_error(
                "fixed order state table capacity is too large");
        }
        const std::size_t required = maximum_size * 2U;
        std::size_t result = 1U;
        while (result < required) {
            if (result >
                std::numeric_limits<std::size_t>::max() / 2U) {
                throw std::length_error(
                    "fixed order state bucket capacity overflow");
            }
            result *= 2U;
        }
        return result;
    }

    [[nodiscard]] std::uint32_t FindIndex(
        const Key& key) const noexcept {
        std::size_t slot = hasher_(key) & (buckets_.size() - 1U);
        for (std::size_t probe = 0U;
             probe < buckets_.size();
             ++probe) {
            const std::uint32_t index = buckets_[slot];
            if (index == kEmptyIndex) {
                return kEmptyIndex;
            }
            if (KeyAccessor{}(
                    states_[static_cast<std::size_t>(index)]) == key) {
                return index;
            }
            slot = (slot + 1U) & (buckets_.size() - 1U);
        }
        return kEmptyIndex;
    }

    std::size_t maximum_size_ = 0U;
    std::size_t state_count_ = 0U;
    std::uint32_t last_index_ = kEmptyIndex;
    std::vector<State> states_;
    std::vector<std::uint32_t> buckets_;
    std::vector<std::uint32_t> sorted_indices_;
    Hasher hasher_{};
};

}  // namespace l2flow::market::internal
