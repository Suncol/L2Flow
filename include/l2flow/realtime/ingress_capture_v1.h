#pragma once

#include "l2flow/realtime/owned_ingress_message_v1.h"

#include <cstdint>

namespace l2flow::realtime {

// Optional lossless copy boundary used by online recovery.  The inspection
// remains valid only for the duration of Capture(); implementations that
// retain it must copy both the vendor header and the complete body before
// returning.  A true return means that the copy owns the logical journal
// capacity needed for asynchronous commit; it does not mean that fdatasync
// has already completed.  The journal's committed frontier is the durability
// boundary.  A false return means that the callback could not be independently
// retained with that reservation (for example a bounded queue or logical disk
// budget was exhausted) and therefore fails the owning FAST pipeline closed.
//
// The ordinary from-open path leaves this sink null.  RealtimePipelineV1 then
// executes its original callback path without an additional inspection,
// clock read, allocation, virtual call, or queue operation.
struct RealtimeIngressCaptureInputV1 final {
    const OwnedIngressMessageInspectionV1* inspection = nullptr;
    std::uint64_t recv_realtime_ns = 0U;
    std::uint64_t recv_monotonic_ns = 0U;
};

class RealtimeIngressCaptureSinkV1 {
public:
    virtual ~RealtimeIngressCaptureSinkV1() = default;

    [[nodiscard]] virtual bool Capture(
        const RealtimeIngressCaptureInputV1& input) noexcept = 0;
};

}  // namespace l2flow::realtime
