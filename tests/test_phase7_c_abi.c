#include "l2flow/consumer/consumer_c_api_v1.h"
#include "l2flow/factor/latest_factor_c_api_v1.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

_Static_assert(sizeof(uint64_t) == 8U, "Phase 7 C ABI requires uint64_t");

static void fill_nonzero(uint8_t* bytes, size_t size, uint8_t seed) {
    size_t index = 0U;
    for (index = 0U; index < size; ++index) {
        bytes[index] = (uint8_t)(seed + (uint8_t)index);
    }
}

int main(void) {
    struct l2flow_consumer_attach_identity_v1 expected;
    struct l2flow_consumer_attach_identity_v1 actual;
    _Alignas(64) uint8_t factor_slot[4096];
    struct l2flow_latest_factor_value_v1 value;
    size_t descriptor_size = 0U;

    memset(&expected, 0, sizeof(expected));
    expected.event_type = 2U;
    expected.record_size = 192U;
    expected.registry_version = 1U;
    fill_nonzero(expected.schema_sha256, sizeof(expected.schema_sha256), 1U);
    fill_nonzero(expected.dtype_sha256, sizeof(expected.dtype_sha256), 2U);
    fill_nonzero(expected.registry_sha256, sizeof(expected.registry_sha256), 3U);
    actual = expected;
    if (l2flow_consumer_validate_attach_v1(&expected, &actual) !=
        L2FLOW_CONSUMER_C_OK_V1) {
        return 1;
    }

    memset(factor_slot, 0, sizeof(factor_slot));
    if (l2flow_latest_factor_atomics_lock_free_v1() != 1 ||
        l2flow_latest_factor_initialize_v1(
            factor_slot, sizeof(factor_slot)) !=
            L2FLOW_LATEST_FACTOR_NONE_V1) {
        return 2;
    }
    memset(&value, 0, sizeof(value));
    if (l2flow_latest_factor_read_v1(
            factor_slot, sizeof(factor_slot), &value, NULL) !=
        L2FLOW_LATEST_FACTOR_EMPTY_V1) {
        return 3;
    }
    if (l2flow_latest_factor_schema_descriptor_v1(&descriptor_size) == NULL ||
        descriptor_size == 0U) {
        return 4;
    }
    return 0;
}
