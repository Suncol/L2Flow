#ifndef L2FLOW_OPS_FATAL_LATCH_H_
#define L2FLOW_OPS_FATAL_LATCH_H_

#include <atomic>
#include <cstdint>

namespace l2flow::ops {

enum class FatalReason : std::uint32_t {
    NONE = 0U,
    CALLBACK_REENTRY,
    CALLBACK_EXCEPTION,
    NULL_MESSAGE,
    NULL_VENDOR_HEAD,
    INVALID_VENDOR_HEADER,
    MESSAGE_TOO_LARGE,
    NULL_VENDOR_BODY,
    UNEXPECTED_SERVICE,
    INGRESS_SEQUENCE_EXHAUSTED,
    INGRESS_RING_OVERFLOW,
    RING_CORRUPTION,
    SHADOW_SINK_IO,
    MALFORMED_CONTROL_MESSAGE,
    RAW_WAL_IO,
    RAW_READINESS_OBSERVER,
    SOURCE_FRONTIER_FAILURE,
};

// A process-lifetime, first-writer-wins latch.  The first successful trip is
// never overwritten, so the initiating failure remains available even when
// shutdown races expose secondary errors.
class FatalLatch final {
public:
    FatalLatch() noexcept = default;

    FatalLatch(const FatalLatch&) = delete;
    FatalLatch& operator=(const FatalLatch&) = delete;

    [[nodiscard]] bool trip(FatalReason reason) noexcept {
        if (reason == FatalReason::NONE) {
            return false;
        }

        std::uint32_t expected =
            static_cast<std::uint32_t>(FatalReason::NONE);
        return reason_.compare_exchange_strong(
            expected,
            static_cast<std::uint32_t>(reason),
            std::memory_order_acq_rel,
            std::memory_order_acquire);
    }

    [[nodiscard]] bool tripped() const noexcept {
        return reason_.load(std::memory_order_acquire) !=
               static_cast<std::uint32_t>(FatalReason::NONE);
    }

    [[nodiscard]] FatalReason reason() const noexcept {
        return static_cast<FatalReason>(
            reason_.load(std::memory_order_acquire));
    }

private:
    static_assert(std::atomic<std::uint32_t>::is_always_lock_free);
    std::atomic<std::uint32_t> reason_{
        static_cast<std::uint32_t>(FatalReason::NONE)};
};

}  // namespace l2flow::ops

#endif  // L2FLOW_OPS_FATAL_LATCH_H_
