#pragma once

#include "l2flow/ipc/realtime_wire_v2.h"
#include "l2flow/market/shanghai_order_event_aggregator_v1.h"
#include "l2flow/market/shenzhen_order_event_projector_v1.h"

#include <cstdint>
#include <string_view>

namespace l2flow::ipc {

// This adapter is the one-way IPC -> market boundary.  The reconstruction
// cores deliberately have no dependency on the shared-memory ABI.
//
// RealtimeWireTickPayloadV2 does not retain DecodedMarketCommonV1::md_stream_id
// text.  common.source_stream_id is the pipeline source-stream identity, not
// MDStreamID, and these functions never substitute one for the other.
//
// Likewise, quantity_unit is validated as wire metadata but is not converted:
// the V1 order cores consume exact native, scale-zero quantities.  Callers
// which expose a unit to users must retain the instrument-directory metadata
// alongside the derived events.
enum class WireOrderEventProjectionResultV2 : std::uint8_t {
    kProjected = 0U,
    kNotTargetEvent,
    kNullOutput,
    kInvalidEnvelope,
    kInvalidFieldEncoding,
    kInvalidEventContract,
};

[[nodiscard]] std::string_view
WireOrderEventProjectionResultNameV2(
    WireOrderEventProjectionResultV2 result) noexcept;

// On every result other than kNullOutput, output is first reset to its
// value-initialized state.  A caller may therefore safely reject a record
// without retaining a previous projection.
//
// Exchange event time is accepted only when the validity bit, nanoseconds
// since midnight, trade date, and UTC+08:00 Unix projection agree exactly.
// SDK LocalTime has no capture date in Wire V2: only a syntactically valid
// hhmmssmmm time-of-day is recovered, never a Unix timestamp.
[[nodiscard]] WireOrderEventProjectionResultV2
ProjectShanghaiOrderEventInputFromWireV2(
    const RealtimeWireTickPayloadV2& payload,
    market::ShanghaiOrderEventInputV1* output) noexcept;

[[nodiscard]] WireOrderEventProjectionResultV2
ProjectShenzhenOrderEventInputFromWireV2(
    const RealtimeWireTickPayloadV2& payload,
    market::ShenzhenOrderEventInputV1* output) noexcept;

}  // namespace l2flow::ipc
