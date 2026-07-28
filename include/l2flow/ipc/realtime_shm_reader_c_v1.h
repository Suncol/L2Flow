#pragma once

#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32)
#define L2FLOW_SHM_READER_API_V1
#elif defined(__GNUC__) || defined(__clang__)
#define L2FLOW_SHM_READER_API_V1 \
    __attribute__((visibility("default")))
#else
#define L2FLOW_SHM_READER_API_V1
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct l2flow_shm_reader_v1 l2flow_shm_reader_v1;

enum l2flow_shm_reader_error_v1 {
    L2FLOW_SHM_READER_OK_V1 = 0,
    L2FLOW_SHM_READER_INVALID_ARGUMENT_V1 = 1,
    L2FLOW_SHM_READER_SYSTEM_ERROR_V1 = 2,
    L2FLOW_SHM_READER_ABI_MISMATCH_V1 = 3,
    L2FLOW_SHM_READER_LAYOUT_INVALID_V1 = 4,
    L2FLOW_SHM_READER_UNAVAILABLE_V1 = 5,
    L2FLOW_SHM_READER_OVERRUN_V1 = 6,
    L2FLOW_SHM_READER_BUFFER_TOO_SMALL_V1 = 7,
    L2FLOW_SHM_READER_INCONSISTENT_READ_V1 = 8,
};

enum l2flow_latest_status_v1 {
    L2FLOW_LATEST_AVAILABLE_V1 = 0,
    L2FLOW_LATEST_NOT_YET_OBSERVED_V1 = 1,
    L2FLOW_LATEST_UNKNOWN_INSTRUMENT_V1 = 2,
    L2FLOW_LATEST_INVALID_INSTRUMENT_ID_V1 = 3,
    L2FLOW_LATEST_UNKNOWN_WINDOW_V1 = 4,
    L2FLOW_LATEST_INVALID_WINDOW_ID_V1 = 5,
};

enum l2flow_instrument_lookup_status_v1 {
    L2FLOW_INSTRUMENT_LOOKUP_FOUND_V1 = 0,
    L2FLOW_INSTRUMENT_LOOKUP_UNKNOWN_V1 = 1,
    L2FLOW_INSTRUMENT_LOOKUP_INVALID_MARKET_V1 = 2,
    L2FLOW_INSTRUMENT_LOOKUP_EMPTY_SECURITY_ID_V1 = 3,
};

typedef struct l2flow_shm_session_info_v1 {
    uint8_t run_id[16];
    uint64_t session_epoch;
    uint64_t registry_version;
    uint8_t registry_sha256[32];
    uint64_t tick_ring_capacity;
    uint64_t tick_highest_published_sequence;
    uint64_t tick_contiguous_published_sequence;
    uint64_t kline_generation;
    uint64_t heartbeat_monotonic_ns;
    uint32_t trade_date;
    uint32_t server_state;
    uint32_t flags;
    uint32_t instrument_count;
    uint32_t window_count;
    uint32_t reserved;
} l2flow_shm_session_info_v1;

// Maps fd read-only and validates the complete V1 layout and required memfd
// seals. The caller retains ownership of fd and may close it immediately
// after this function returns.
L2FLOW_SHM_READER_API_V1 int l2flow_shm_reader_open_fd_v1(
    int fd,
    l2flow_shm_reader_v1** output);
// The caller owns external synchronization: close is permitted only after
// every in-flight call using this reader has returned.
L2FLOW_SHM_READER_API_V1 void l2flow_shm_reader_close_v1(
    l2flow_shm_reader_v1* reader);

L2FLOW_SHM_READER_API_V1 int l2flow_shm_reader_session_v1(
    const l2flow_shm_reader_v1* reader,
    l2flow_shm_session_info_v1* output);

// Copies the fixed 64-byte registry row and its two opaque key byte strings.
// Required lengths are always returned. A null byte buffer is legal only
// when its capacity is zero; BUFFER_TOO_SMALL leaves both byte buffers
// untouched.
L2FLOW_SHM_READER_API_V1 int l2flow_shm_reader_instrument_v1(
    const l2flow_shm_reader_v1* reader,
    uint32_t instrument_id,
    void* row_output,
    size_t row_output_bytes,
    uint8_t* security_id_source_output,
    size_t security_id_source_capacity,
    size_t* security_id_source_written,
    uint8_t* security_id_output,
    size_t security_id_capacity,
    size_t* security_id_written);

// Resolves exact opaque-byte registry keys. The composite key is
// (market, security_id_source, security_id); the reader never trims,
// case-folds, transcodes, or infers any component. Input order and duplicate
// keys are preserved. A null per-item byte pointer is legal only when its
// corresponding length is zero. Non-FOUND items always return instrument ID
// zero and are described by their per-item status. Input arrays, output
// arrays, and the two output arrays must not overlap. As with every reader
// call, the caller must finish all in-flight calls before closing the reader.
L2FLOW_SHM_READER_API_V1 int
l2flow_shm_reader_resolve_instruments_v1(
    const l2flow_shm_reader_v1* reader,
    const uint8_t* markets,
    const uint8_t* const* security_id_sources,
    const size_t* security_id_source_lengths,
    const uint8_t* const* security_ids,
    const size_t* security_id_lengths,
    size_t count,
    uint32_t* instrument_ids,
    uint8_t* item_statuses);

// outputs is count fixed-stride records. Each successful item is a consistent
// client-owned copy. Item status preserves input order and duplicate IDs.
L2FLOW_SHM_READER_API_V1 int
l2flow_shm_reader_latest_snapshots_v1(
    const l2flow_shm_reader_v1* reader,
    const uint32_t* instrument_ids,
    size_t count,
    void* outputs,
    size_t output_stride,
    uint8_t* item_statuses);
L2FLOW_SHM_READER_API_V1 int l2flow_shm_reader_latest_ticks_v1(
    const l2flow_shm_reader_v1* reader,
    const uint32_t* instrument_ids,
    size_t count,
    void* outputs,
    size_t output_stride,
    uint8_t* item_statuses);

L2FLOW_SHM_READER_API_V1 int
l2flow_shm_reader_latest_klines_v1(
    const l2flow_shm_reader_v1* reader,
    const uint32_t* instrument_ids,
    const uint32_t* window_ids,
    size_t count,
    void* outputs,
    size_t output_stride,
    uint8_t* item_statuses);

// Reads the global mixed tick stream from expected_sequence. A successful
// zero-row result means the contiguous prefix has not advanced yet. Overrun
// never changes the requested cursor and returns the observed slot sequence
// (or the current oldest candidate) through observed_sequence.
L2FLOW_SHM_READER_API_V1 int l2flow_shm_reader_ticks_v1(
    const l2flow_shm_reader_v1* reader,
    uint64_t expected_sequence,
    void* outputs,
    size_t output_stride,
    size_t maximum_records,
    size_t* written,
    uint64_t* next_sequence,
    uint64_t* observed_sequence);

#ifdef __cplusplus
}
#endif

#undef L2FLOW_SHM_READER_API_V1
