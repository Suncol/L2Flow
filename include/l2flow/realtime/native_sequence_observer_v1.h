#pragma once

#include "l2flow/realtime/native_sequence_recovery_v1.h"

#include <cstdint>

namespace l2flow::realtime {

// One exchange-native sequence observation made by the serialized SDK
// admission path. Target observations are published only as part of a
// successful decoder-queue commit; filtered observations represent a
// well-formed non-A-share message which occupies the same exchange sequence
// domain but intentionally has no Store record.
struct NativeSequenceObservationV1 final {
    NativeSequenceDescriptorV1 descriptor{};
    l2flow::sdk::MessageKey message_key{};
    NativeSequenceRecoveryRecordClassV1 record_class =
        NativeSequenceRecoveryRecordClassV1::kTarget;
    // Nonzero only for kTarget. It is the immutable Store correlation key and
    // remains an arrival-order identity, never a native sequence substitute.
    std::uint64_t ingress_sequence = 0U;
};

enum class NativeSequenceObservationFailureV1 : std::uint8_t {
    // A tracked tick tuple did not contain a valid native sequence domain.
    kExtraction = 1U,
    // The observer's bounded, nonblocking handoff could not retain an
    // observation. The observer must freeze CERTIFIED conservatively.
    kHandoff = 2U,
};

// Optional best-effort certification tap. Implementations must be
// allocation-free, nonblocking, and noexcept on Observe. Pipeline deliberately
// ignores every observer outcome: this boundary is never allowed to reject,
// stop, or mark coverage lost on the independent FAST path.
class NativeSequenceObservationSinkV1 {
public:
    virtual ~NativeSequenceObservationSinkV1() = default;

    virtual void ObserveNativeSequence(
        const NativeSequenceObservationV1& observation) noexcept = 0;

    virtual void MarkNativeSequenceObservationFailure(
        NativeSequenceObservationFailureV1 failure,
        const l2flow::sdk::MessageKey& message_key) noexcept = 0;
};

}  // namespace l2flow::realtime
