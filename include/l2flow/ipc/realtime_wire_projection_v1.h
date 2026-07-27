#pragma once

#include "l2flow/ipc/realtime_wire_v1.h"
#include "l2flow/market/kline_types_v1.h"
#include "l2flow/market/realtime_history_v1.h"

#include <cstddef>
#include <cstdint>

namespace l2flow::ipc {

// Pure, allocation-free projections from retained C++ market objects to the
// stable wire schema. The destination is changed only on success.
[[nodiscard]] bool ProjectSnapshotWireV1(
    const l2flow::market::RealtimeHistoryRecordV1& record,
    std::size_t registry_ordinal,
    RealtimeWireSnapshotPayloadV1* output) noexcept;

[[nodiscard]] bool ProjectTickWireV1(
    const l2flow::market::RealtimeHistoryRecordV1& record,
    std::size_t registry_ordinal,
    RealtimeWireTickPayloadV1* output) noexcept;

[[nodiscard]] bool ProjectKLineWireV1(
    std::uint64_t generation,
    const l2flow::market::KLineBarV1& bar,
    RealtimeWireKLinePayloadV1* output) noexcept;

}  // namespace l2flow::ipc
