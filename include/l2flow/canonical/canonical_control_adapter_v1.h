#pragma once

#include "l2flow/canonical/canonical_normalizer_v1.h"
#include "l2flow/control/control_record_v1.h"

#include <cstdint>
#include <string_view>

namespace l2flow::canonical {

enum class CanonicalControlAdapterErrorV1 : std::uint8_t {
    kNone = 0U,
    kNullOutput,
    kInvalidContext,
    kInvalidControlRecord,
    kContextMismatch,
    kUnsupportedControlFlags,
    kCanonicalValidationFailure,
};

[[nodiscard]] std::string_view CanonicalControlAdapterErrorNameV1(
    CanonicalControlAdapterErrorV1 error) noexcept;

// Low-level field converter.  Production runtime uses
// CanonicalNormalizerV1::PrepareControl so shard_event_id is allocated by the
// same transactional family state; direct calls are for validation/tests.
// Converts the logical Phase-3 control object field-by-field.  It never
// reinterprets the unrelated 256-byte Phase-3 wire record as a Canonical
// record.  Capture namespace and clock identity stay in segment context;
// trade_date remains a separate business partition key.
[[nodiscard]] CanonicalControlAdapterErrorV1
MakeCanonicalControlRecordV1(
    const CanonicalRawContextV1& raw,
    const l2flow::control::ControlRecordV1& control,
    std::int64_t recv_realtime_ns,
    std::int64_t recv_monotonic_ns,
    std::uint64_t shard_event_id,
    CanonicalControlRecordV1* output) noexcept;

}  // namespace l2flow::canonical
