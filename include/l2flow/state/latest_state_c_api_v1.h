#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
#define L2FLOW_LATEST_STATE_NODISCARD_V1 [[nodiscard]]
#define L2FLOW_LATEST_STATE_NOEXCEPT_V1 noexcept
extern "C" {
#else
#define L2FLOW_LATEST_STATE_NODISCARD_V1
#define L2FLOW_LATEST_STATE_NOEXCEPT_V1
#endif

enum l2flow_latest_state_storage_v1 {
    L2FLOW_LATEST_STATE_SLOT_BYTES_V1 = 4096,
    L2FLOW_LATEST_STATE_SLOT_ALIGNMENT_V1 = 64,
};

enum l2flow_latest_state_stable_copy_result_v1 {
    L2FLOW_LATEST_STATE_STABLE_COPY_OK_V1 = 0,
    L2FLOW_LATEST_STATE_STABLE_COPY_INVALID_V1 = 1,
    L2FLOW_LATEST_STATE_STABLE_COPY_BUSY_V1 = 2,
};

// Returns true only when the host can provide the lock-free 64-bit atomics
// required by the opaque Latest State slot ABI.
L2FLOW_LATEST_STATE_NODISCARD_V1 bool
l2flow_latest_state_atomic_u64_lock_free_v1(void)
    L2FLOW_LATEST_STATE_NOEXCEPT_V1;

// Copies one stable 4096-byte slot image.  slot_bytes must be 64-byte aligned;
// the source and destination ranges must not overlap.  The return value is one
// of enum l2flow_latest_state_stable_copy_result_v1.
L2FLOW_LATEST_STATE_NODISCARD_V1 uint32_t
l2flow_latest_state_copy_stable_v1(
    const void* slot_bytes,
    size_t slot_size,
    void* output_bytes,
    size_t output_size,
    uint32_t max_attempts) L2FLOW_LATEST_STATE_NOEXCEPT_V1;

#ifdef __cplusplus
}
#endif

#undef L2FLOW_LATEST_STATE_NODISCARD_V1
#undef L2FLOW_LATEST_STATE_NOEXCEPT_V1
