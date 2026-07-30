#pragma once

#include <cstdint>
#include <limits>

namespace l2flow::realtime {

struct ProcessingProgressV2 final {
    // Greatest contiguous capture sequence committed across the four source
    // decoder FIFOs. Each queue-tail publication is release-ordered after this
    // frontier, so no decoder can apply a sequence before it is accepted.
    std::uint64_t accepted_sequence = 0U;
    // Greatest contiguous capture prefix for which Store, every enabled
    // derived state, latest publication, and the required external applied
    // sink have all succeeded.
    std::uint64_t applied_sequence = 0U;

    [[nodiscard]] bool valid() const noexcept {
        return accepted_sequence !=
                   std::numeric_limits<std::uint64_t>::max() &&
               applied_sequence <= accepted_sequence;
    }

    [[nodiscard]] std::uint64_t processing_lag_records()
        const noexcept {
        return valid() ? accepted_sequence - applied_sequence : 0U;
    }
};

// Optional application-composition projection. Admission and completion may
// each publish a newer component. Implementations merge both monotonically.
// A false result makes the required external read projection unusable and the
// Pipeline fails closed.
class ProcessingProgressSinkV2 {
public:
    virtual ~ProcessingProgressSinkV2() = default;

    [[nodiscard]] virtual bool PublishProcessingProgress(
        ProcessingProgressV2 progress) noexcept = 0;
};

}  // namespace l2flow::realtime
