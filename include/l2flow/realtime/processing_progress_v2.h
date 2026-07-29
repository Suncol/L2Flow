#pragma once

#include <cstdint>
#include <limits>

namespace l2flow::realtime {

struct ProcessingProgressV2 final {
    // Greatest contiguous capture sequence admitted to the mandatory Journal
    // queue. In a healthy continuing session every such sequence then entered
    // the ordered processing queue before its callback returned successfully.
    // A processing-queue admission failure is terminal because Journal
    // admission cannot be rolled back. Neither processing nor Journal
    // durability is synchronous.
    std::uint64_t accepted_sequence = 0U;
    // Greatest capture sequence known to have passed the mandatory
    // journal's durability boundary.
    std::uint64_t durable_sequence = 0U;
    // Greatest contiguous capture prefix for which Store, every enabled
    // derived state, latest publication, and the required external applied
    // sink have all succeeded.
    std::uint64_t applied_sequence = 0U;

    [[nodiscard]] bool valid() const noexcept {
        return accepted_sequence !=
                   std::numeric_limits<std::uint64_t>::max() &&
               durable_sequence <= accepted_sequence &&
               applied_sequence <= accepted_sequence;
    }

    [[nodiscard]] std::uint64_t processing_lag_records()
        const noexcept {
        return valid() ? accepted_sequence - applied_sequence : 0U;
    }

    [[nodiscard]] std::uint64_t durability_lag_records()
        const noexcept {
        return valid() ? accepted_sequence - durable_sequence : 0U;
    }
};

// Optional application-composition projection. Admission, Journal durability,
// and processing may each publish a newer component. Implementations merge
// every component monotonically and never infer an ordering between durable
// and applied. A false result makes the required external read projection
// unusable and the Pipeline fails closed.
class ProcessingProgressSinkV2 {
public:
    virtual ~ProcessingProgressSinkV2() = default;

    [[nodiscard]] virtual bool PublishProcessingProgress(
        ProcessingProgressV2 progress) noexcept = 0;
};

}  // namespace l2flow::realtime
