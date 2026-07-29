#pragma once

#include <atomic>
#include <cstdint>
#include <limits>

namespace l2flow::runtime::detail {

// Linearizes a nonblocking producer's publication against queue close.
//
// The closed bit and active-publication count share one atomic word. A
// producer that acquires a Lease before CloseAndWait() sets the closed bit is
// part of the prefix that close must wait through. Once the bit is set, no new
// producer can acquire a Lease. This prevents a consumer from observing
// "closed and empty" while an already-admitted producer has not yet published
// its queue tail.
class CloseablePublicationGate final {
public:
    class Lease final {
    public:
        Lease() noexcept = default;
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;

        Lease(Lease&& other) noexcept
            : owner_(other.owner_) {
            other.owner_ = nullptr;
        }

        Lease& operator=(Lease&& other) noexcept {
            if (this != &other) {
                Release();
                owner_ = other.owner_;
                other.owner_ = nullptr;
            }
            return *this;
        }

        ~Lease() { Release(); }

        [[nodiscard]] explicit operator bool() const noexcept {
            return owner_ != nullptr;
        }

    private:
        friend class CloseablePublicationGate;

        explicit Lease(CloseablePublicationGate* owner) noexcept
            : owner_(owner) {}

        void Release() noexcept {
            if (owner_ != nullptr) {
                owner_->Release();
                owner_ = nullptr;
            }
        }

        CloseablePublicationGate* owner_ = nullptr;
    };

    CloseablePublicationGate() noexcept = default;
    CloseablePublicationGate(const CloseablePublicationGate&) = delete;
    CloseablePublicationGate& operator=(
        const CloseablePublicationGate&) = delete;

    // Never waits. A valid Lease means this producer is ordered before close
    // and may publish one queue entry. An empty Lease means close already
    // linearized and the producer must reject the entry.
    [[nodiscard]] Lease TryAcquire() noexcept {
        std::uint64_t observed =
            state_.load(std::memory_order_acquire);
        for (;;) {
            if ((observed & kClosedBit) != 0U) {
                return Lease{};
            }
            // A queue cannot have this many simultaneous publishers. Keep the
            // arithmetic defined even if this primitive is reused elsewhere.
            if ((observed & kActiveMask) == kActiveMask) {
                return Lease{};
            }
            if (state_.compare_exchange_weak(
                    observed,
                    observed + 1U,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                return Lease(this);
            }
        }
    }

    // May wait for producers that acquired a Lease before the close
    // linearization point. It never waits for queue consumption or I/O.
    void CloseAndWait() noexcept {
        std::uint64_t observed =
            state_.fetch_or(kClosedBit, std::memory_order_acq_rel) |
            kClosedBit;
        while ((observed & kActiveMask) != 0U) {
            state_.wait(observed, std::memory_order_acquire);
            observed = state_.load(std::memory_order_acquire);
        }
    }

    [[nodiscard]] bool close_requested() const noexcept {
        return (state_.load(std::memory_order_acquire) &
                kClosedBit) != 0U;
    }

    [[nodiscard]] bool closed_and_quiesced() const noexcept {
        return state_.load(std::memory_order_acquire) == kClosedBit;
    }

private:
    void Release() noexcept {
        const std::uint64_t previous =
            state_.fetch_sub(1U, std::memory_order_acq_rel);
        if ((previous & kActiveMask) == 1U) {
            state_.notify_all();
        }
    }

    static constexpr std::uint64_t kClosedBit =
        std::uint64_t{1U}
        << (std::numeric_limits<std::uint64_t>::digits - 1U);
    static constexpr std::uint64_t kActiveMask = kClosedBit - 1U;

    std::atomic<std::uint64_t> state_{0U};
};

}  // namespace l2flow::runtime::detail
