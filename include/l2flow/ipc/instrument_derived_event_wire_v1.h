#pragma once

#include "l2flow/ipc/instrument_derived_event_history_c_v1.h"
#include "l2flow/ipc/instrument_derived_event_history_v1.h"

namespace l2flow::ipc {

// Losslessly flattens the public C++ Event variant into the already-frozen
// 320-byte C row ABI.  The source sequence is retained; this function never
// substitutes a canonical apply sequence for any native/arrival anchor.
[[nodiscard]] bool ProjectInstrumentDerivedEventWireV1(
    const InstrumentDerivedEventV1& source,
    l2flow_instrument_derived_event_row_v1* output) noexcept;

}  // namespace l2flow::ipc
