#pragma once

#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32)
#define L2FLOW_SHM_READER_API_V2
#elif defined(__GNUC__) || defined(__clang__)
#define L2FLOW_SHM_READER_API_V2 \
    __attribute__((visibility("default")))
#else
#define L2FLOW_SHM_READER_API_V2
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct l2flow_shm_reader_v2 l2flow_shm_reader_v2;

enum l2flow_shm_reader_error_v2 {
    L2FLOW_SHM_READER_OK_V2 = 0,
    L2FLOW_SHM_READER_INVALID_ARGUMENT_V2 = 1,
    L2FLOW_SHM_READER_SYSTEM_ERROR_V2 = 2,
    L2FLOW_SHM_READER_ABI_MISMATCH_V2 = 3,
    L2FLOW_SHM_READER_LAYOUT_INVALID_V2 = 4,
    L2FLOW_SHM_READER_UNAVAILABLE_V2 = 5,
    L2FLOW_SHM_READER_OVERRUN_V2 = 6,
    L2FLOW_SHM_READER_BUFFER_TOO_SMALL_V2 = 7,
    L2FLOW_SHM_READER_INCONSISTENT_READ_V2 = 8,
};

enum l2flow_instrument_status_v2 {
    L2FLOW_INSTRUMENT_AVAILABLE_V2 = 0,
    L2FLOW_INSTRUMENT_BOUND_NO_DATA_V2 = 1,
    L2FLOW_INSTRUMENT_UNBOUND_V2 = 2,
    L2FLOW_INSTRUMENT_INVALID_ID_V2 = 3,
};

enum l2flow_latest_status_v2 {
    L2FLOW_LATEST_AVAILABLE_V2 = 0,
    L2FLOW_LATEST_BOUND_NO_DATA_V2 = 1,
    L2FLOW_LATEST_TYPE_UNAVAILABLE_V2 = 2,
    L2FLOW_LATEST_UNBOUND_V2 = 3,
    L2FLOW_LATEST_INVALID_INSTRUMENT_ID_V2 = 4,
    L2FLOW_LATEST_UNKNOWN_WINDOW_V2 = 5,
    L2FLOW_LATEST_INVALID_WINDOW_ID_V2 = 6,
};

enum l2flow_instrument_lookup_status_v2 {
    L2FLOW_INSTRUMENT_LOOKUP_FOUND_V2 = 0,
    L2FLOW_INSTRUMENT_LOOKUP_UNKNOWN_V2 = 1,
    L2FLOW_INSTRUMENT_LOOKUP_INVALID_MARKET_V2 = 2,
    L2FLOW_INSTRUMENT_LOOKUP_EMPTY_SECURITY_ID_V2 = 3,
};

enum l2flow_selection_scope_v2 {
    L2FLOW_SELECTION_BOUND_V2 = 1,
    L2FLOW_SELECTION_OBSERVED_ANY_V2 = 2,
    L2FLOW_SELECTION_SNAPSHOT_AVAILABLE_V2 = 3,
    L2FLOW_SELECTION_TICK_AVAILABLE_V2 = 4,
    L2FLOW_SELECTION_FACTOR_ELIGIBLE_V2 = 5,
};

// All IDs in this ABI are scoped by session_epoch. Carrying an instrument ID
// into another session is invalid even when the numeric value is unchanged.
typedef struct l2flow_shm_session_info_v2 {
    uint8_t run_id[16];
    uint8_t layout_digest[32];
    uint8_t catalog_digest[32];
    uint64_t session_epoch;
    uint64_t catalog_generation;
    uint64_t data_state_generation;
    uint64_t accepted_sequence;
    uint64_t applied_sequence;
    uint64_t processing_lag_records;
    uint64_t tick_ring_capacity;
    uint64_t tick_highest_published_sequence;
    uint64_t tick_contiguous_published_sequence;
    uint64_t kline_generation;
    uint64_t heartbeat_monotonic_ns;
    uint64_t published_records;
    uint32_t trade_date;
    uint32_t server_state;
    uint32_t flags;
    uint32_t capacity;
    uint32_t window_count;
    uint32_t catalog_scope;
    uint32_t coverage_complete;
    uint32_t bound_count;
    uint32_t available_count;
    uint32_t snapshot_available_count;
    uint32_t tick_available_count;
    uint32_t factor_eligible_count;
    uint32_t reserved[4];
} l2flow_shm_session_info_v2;

// Minimal throttled Python health sample. Unlike session_v2, this never
// copies the catalog/count/progress status seqcount.
typedef struct l2flow_shm_health_v2 {
    uint64_t session_epoch;
    uint64_t heartbeat_monotonic_ns;
    uint32_t server_state;
    uint32_t flags;
    uint32_t reserved[2];
} l2flow_shm_health_v2;

// A selection envelope and its ID array describe one stable structural cut.
// The catalog/data-state identity and selected rows are validated together;
// accepted/applied progress comes from one coherent status read taken after
// that validation. Progress-only changes do not restart the row scan.
// returned_row_count is the logical number of selected rows and therefore
// also the required ID-buffer length. It is populated on BUFFER_TOO_SMALL;
// the undersized ID buffer itself is left untouched.
typedef struct l2flow_selection_envelope_v2 {
    uint8_t run_id[16];
    uint8_t catalog_digest[32];
    uint64_t session_epoch;
    uint64_t catalog_generation;
    uint64_t data_state_generation;
    uint64_t accepted_sequence;
    uint64_t applied_sequence;
    uint64_t processing_lag_records;
    uint32_t capacity;
    uint32_t catalog_scope;
    uint32_t coverage_complete;
    uint32_t bound_count;
    uint32_t available_count;
    uint32_t snapshot_available_count;
    uint32_t tick_available_count;
    uint32_t factor_eligible_count;
    uint32_t selection_scope;
    uint32_t returned_row_count;
    uint32_t reserved[2];
} l2flow_selection_envelope_v2;

// Maps fd read-only and accepts only the sealed Wire V2.1 layout. The caller
// retains ownership of fd and may close it immediately after this function
// returns.
L2FLOW_SHM_READER_API_V2 int l2flow_shm_reader_open_fd_v2(
    int fd,
    l2flow_shm_reader_v2** output);
// The caller owns close synchronization: every in-flight call must return
// before close.
L2FLOW_SHM_READER_API_V2 void l2flow_shm_reader_close_v2(
    l2flow_shm_reader_v2* reader);

L2FLOW_SHM_READER_API_V2 int l2flow_shm_reader_session_v2(
    const l2flow_shm_reader_v2* reader,
    l2flow_shm_session_info_v2* output);
L2FLOW_SHM_READER_API_V2 int l2flow_shm_reader_health_v2(
    const l2flow_shm_reader_v2* reader,
    l2flow_shm_health_v2* output);

// Looks up instrument_id in O(1) as ordinal=instrument_id-1 and copies one
// stable 128-byte row plus its exact opaque key bytes when bound. Required
// lengths and item_status are returned for every semantic point result.
// BUFFER_TOO_SMALL leaves the row and both byte buffers untouched.
L2FLOW_SHM_READER_API_V2 int l2flow_shm_reader_instrument_v2(
    const l2flow_shm_reader_v2* reader,
    uint32_t instrument_id,
    void* row_output,
    size_t row_output_bytes,
    uint8_t* security_id_source_output,
    size_t security_id_source_capacity,
    size_t* security_id_source_written,
    uint8_t* security_id_output,
    size_t security_id_capacity,
    size_t* security_id_written,
    uint8_t* item_status);

// Resolves exact opaque-byte keys. The reader-owned sorted index is rebuilt
// lazily under its mutex only when catalog_generation changes. Latest-by-ID
// APIs never enter this path.
L2FLOW_SHM_READER_API_V2 int
l2flow_shm_reader_resolve_instruments_v2(
    const l2flow_shm_reader_v2* reader,
    const uint8_t* markets,
    const uint8_t* const* security_id_sources,
    const size_t* security_id_source_lengths,
    const uint8_t* const* security_ids,
    const size_t* security_id_lengths,
    size_t count,
    uint32_t* instrument_ids,
    uint8_t* item_statuses);

L2FLOW_SHM_READER_API_V2 int
l2flow_shm_reader_latest_snapshots_v2(
    const l2flow_shm_reader_v2* reader,
    const uint32_t* instrument_ids,
    size_t count,
    void* outputs,
    size_t output_stride,
    uint8_t* item_statuses);

L2FLOW_SHM_READER_API_V2 int
l2flow_shm_reader_latest_ticks_v2(
    const l2flow_shm_reader_v2* reader,
    const uint32_t* instrument_ids,
    size_t count,
    void* outputs,
    size_t output_stride,
    uint8_t* item_statuses);

L2FLOW_SHM_READER_API_V2 int
l2flow_shm_reader_latest_klines_v2(
    const l2flow_shm_reader_v2* reader,
    const uint32_t* instrument_ids,
    const uint32_t* window_ids,
    size_t count,
    void* outputs,
    size_t output_stride,
    uint8_t* item_statuses);

// Reads the global mixed tick stream from expected_sequence. A successful
// zero-row result means the contiguous prefix has not advanced. Overrun
// leaves the requested cursor unchanged and returns the observed sequence.
L2FLOW_SHM_READER_API_V2 int l2flow_shm_reader_ticks_v2(
    const l2flow_shm_reader_v2* reader,
    uint64_t expected_sequence,
    void* outputs,
    size_t output_stride,
    size_t maximum_records,
    size_t* written,
    uint64_t* next_sequence,
    uint64_t* observed_sequence);

// Scans the committed bound prefix against one coherent header status cut.
// On success IDs are ordinal ordered and required_count equals
// envelope.returned_row_count and the selected scope's header count.
L2FLOW_SHM_READER_API_V2 int
l2flow_shm_reader_select_instruments_v2(
    const l2flow_shm_reader_v2* reader,
    uint32_t selection_scope,
    uint32_t* instrument_ids,
    size_t instrument_id_capacity,
    size_t* required_count,
    l2flow_selection_envelope_v2* envelope);

#ifdef __cplusplus
}
#endif

#undef L2FLOW_SHM_READER_API_V2
