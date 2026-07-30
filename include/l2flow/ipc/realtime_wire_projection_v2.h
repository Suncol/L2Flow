#pragma once

#include "l2flow/ipc/realtime_wire_v2.h"
#include "l2flow/market/realtime_history_v1.h"

#include <cstddef>

namespace l2flow::ipc {

// Projects one Store-owned tick record into the stable Wire V2 payload without
// publishing it.  The projection is deterministic for the supplied record and
// ordinal and performs no allocation.  Arrival metadata remains unchanged; a
// caller constructing a semantic duplicate fingerprint must explicitly remove
// the arrival-only fields it does not intend to compare.
[[nodiscard]] bool ProjectRealtimeWireTickPayloadV2(
    const l2flow::market::RealtimeHistoryRecordV1& record,
    std::size_t ordinal,
    RealtimeWireTickPayloadV2* output) noexcept;

}  // namespace l2flow::ipc
