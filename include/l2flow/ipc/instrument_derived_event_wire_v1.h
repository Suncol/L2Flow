#pragma once

#include "l2flow/ipc/instrument_derived_event_history_c_v1.h"
#include "l2flow/ipc/instrument_derived_event_history_v1.h"

namespace l2flow::ipc {

// Losslessly flattens the public C++ Event variant into record schema 2 of the
// already-frozen 320-byte C row ABI. The source sequence is retained; this
// function never substitutes a derived/canonical sequence for any native
// anchor or source_tick_event_ordinal. A source-free row is accepted only with
// ordinal_valid=false, ordinal=0, tick_stream_sequence=0, and an order
// revision whose operation is Finalize.
[[nodiscard]] bool ProjectInstrumentDerivedEventWireV1(
    const InstrumentDerivedEventV1& source,
    l2flow_instrument_derived_event_row_v1* output) noexcept;

// Serial producer fast paths for source-backed tick variants. They construct
// the frozen row directly and avoid first wrapping a market event in the much
// larger cross-market InstrumentDerivedEventV1 variant.
[[nodiscard]] bool ProjectInstrumentDerivedEventWireV1(
    std::uint64_t derived_event_sequence,
    std::uint32_t source_tick_event_ordinal,
    const market::ShanghaiOrderEventV1& source,
    l2flow_instrument_derived_event_row_v1* output) noexcept;
[[nodiscard]] bool ProjectInstrumentDerivedEventWireV1(
    std::uint64_t derived_event_sequence,
    std::uint32_t source_tick_event_ordinal,
    const market::ShenzhenOrderEventV1& source,
    l2flow_instrument_derived_event_row_v1* output) noexcept;

// Owner-private Shenzhen fast path. These overloads preserve the exact frozen
// row representation without first copying a snapshot into an Event variant.
[[nodiscard]] bool ProjectInstrumentDerivedEventWireV1(
    std::uint64_t derived_event_sequence,
    std::uint32_t source_tick_event_ordinal,
    market::ShenzhenOrderDeltaOperationV1 operation,
    const market::ShenzhenEventSourceAnchorV1& source_anchor,
    const market::ShenzhenOrderSnapshotV1& order,
    l2flow_instrument_derived_event_row_v1* output) noexcept;
[[nodiscard]] bool ProjectInstrumentDerivedEventWireV1(
    std::uint64_t derived_event_sequence,
    std::uint32_t source_tick_event_ordinal,
    const market::ShenzhenTradeEventV1& source,
    l2flow_instrument_derived_event_row_v1* output) noexcept;
[[nodiscard]] bool ProjectInstrumentDerivedEventWireV1(
    std::uint64_t derived_event_sequence,
    std::uint32_t source_tick_event_ordinal,
    const market::ShenzhenCancelEventV1& source,
    l2flow_instrument_derived_event_row_v1* output) noexcept;

}  // namespace l2flow::ipc
