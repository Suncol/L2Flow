#include "l2flow/state/latest_state_c_api_v1.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

_Static_assert(sizeof(uint64_t) == 8U, "Phase 6 C ABI requires uint64_t");
_Static_assert(
    L2FLOW_LATEST_STATE_SLOT_BYTES_V1 == 4096,
    "Latest State slot size changed");
_Static_assert(
    L2FLOW_LATEST_STATE_SLOT_ALIGNMENT_V1 == 64,
    "Latest State slot alignment changed");

int main(void) {
    _Alignas(L2FLOW_LATEST_STATE_SLOT_ALIGNMENT_V1)
        uint64_t slot[L2FLOW_LATEST_STATE_SLOT_BYTES_V1 / sizeof(uint64_t)];
    uint8_t copy[L2FLOW_LATEST_STATE_SLOT_BYTES_V1];

    memset(slot, 0, sizeof(slot));
    memset(copy, 0xff, sizeof(copy));
    slot[0] = 2U;
    slot[1] = UINT64_C(0x0123456789abcdef);

    if (!l2flow_latest_state_atomic_u64_lock_free_v1()) {
        return 1;
    }
    if (l2flow_latest_state_copy_stable_v1(
            slot, sizeof(slot), copy, sizeof(copy), 8U) !=
        L2FLOW_LATEST_STATE_STABLE_COPY_OK_V1) {
        return 2;
    }
    if (memcmp(slot, copy, sizeof(slot)) != 0) {
        return 3;
    }
    if (l2flow_latest_state_copy_stable_v1(
            slot, sizeof(slot) - 1U, copy, sizeof(copy), 8U) !=
        L2FLOW_LATEST_STATE_STABLE_COPY_INVALID_V1) {
        return 4;
    }
    if (l2flow_latest_state_copy_stable_v1(
            slot, sizeof(slot), slot, sizeof(slot), 8U) !=
        L2FLOW_LATEST_STATE_STABLE_COPY_INVALID_V1) {
        return 5;
    }

    slot[0] = 3U;
    if (l2flow_latest_state_copy_stable_v1(
            slot, sizeof(slot), copy, sizeof(copy), 2U) !=
        L2FLOW_LATEST_STATE_STABLE_COPY_BUSY_V1) {
        return 6;
    }
    return 0;
}
