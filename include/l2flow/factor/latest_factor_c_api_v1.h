#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum l2flow_latest_factor_error_v1 {
    L2FLOW_LATEST_FACTOR_NONE_V1 = 0,
    L2FLOW_LATEST_FACTOR_NULL_ARGUMENT_V1 = 1,
    L2FLOW_LATEST_FACTOR_INVALID_STORAGE_V1 = 2,
    L2FLOW_LATEST_FACTOR_UNSUPPORTED_HOST_V1 = 3,
    L2FLOW_LATEST_FACTOR_ATOMICS_NOT_LOCK_FREE_V1 = 4,
    L2FLOW_LATEST_FACTOR_INVALID_VALUE_V1 = 5,
    L2FLOW_LATEST_FACTOR_EMPTY_V1 = 6,
    L2FLOW_LATEST_FACTOR_BUSY_V1 = 7,
    L2FLOW_LATEST_FACTOR_SEQUENCE_EXHAUSTED_V1 = 8,
    L2FLOW_LATEST_FACTOR_SLOT_IDENTITY_MISMATCH_V1 = 9,
    L2FLOW_LATEST_FACTOR_OLD_OUTPUT_V1 = 10,
    L2FLOW_LATEST_FACTOR_OUTPUT_CONFLICT_V1 = 11,
    L2FLOW_LATEST_FACTOR_CORRUPT_SLOT_V1 = 12,
    L2FLOW_LATEST_FACTOR_ALREADY_INITIALIZED_V1 = 13,
};

enum l2flow_latest_factor_publish_disposition_v1 {
    L2FLOW_LATEST_FACTOR_PUBLISHED_V1 = 1,
    L2FLOW_LATEST_FACTOR_IDEMPOTENT_V1 = 2,
};

struct l2flow_latest_factor_value_v1 {
    uint8_t factor_id_sha256[32];
    uint8_t factor_version_sha256[32];
    uint32_t instrument_id;
    int64_t asof_ns;
    double value;
    uint8_t valid;
    uint8_t reserved0[7];
    uint64_t input_quality_flags;
    uint64_t watermark_set_id;
    uint8_t input_identity_sha256[32];
    uint64_t calculation_latency_ns;
    uint32_t clock_epoch_algorithm;
    uint8_t clock_epoch_digest[32];
    uint64_t clock_epoch_label;
};

int l2flow_latest_factor_atomics_lock_free_v1(void);

// storage must address exactly one 4096-byte, 64-byte-aligned opaque slot.
// Initialize is a single-owner construction operation, not a concurrent reset.
int l2flow_latest_factor_initialize_v1(void* storage, size_t storage_size);

// The value/disposition objects must not overlap each other or the opaque
// slot. Mixing ordinary C loads/stores with the slot's atomic word accesses
// is invalid and is rejected before publication begins.
int l2flow_latest_factor_publish_v1(
    void* storage,
    size_t storage_size,
    const struct l2flow_latest_factor_value_v1* value,
    uint8_t* disposition);

// The value/stable_sequence outputs must not overlap each other or the opaque
// slot. stable_sequence may be NULL.
int l2flow_latest_factor_read_v1(
    const void* storage,
    size_t storage_size,
    struct l2flow_latest_factor_value_v1* value,
    uint64_t* stable_sequence);

const char* l2flow_latest_factor_schema_descriptor_v1(size_t* byte_count);

int l2flow_latest_factor_schema_sha256_v1(uint8_t output[32]);

#ifdef __cplusplus
}
#endif
