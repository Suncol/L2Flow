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
typedef struct l2flow_instrument_raw_event_history_session_v2
    l2flow_instrument_raw_event_history_session_v2;
typedef struct l2flow_instrument_raw_event_history_cursor_v2
    l2flow_instrument_raw_event_history_cursor_v2;

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

// Wire V2.4 server states. LIVE_PARTIAL permits point-in-time latest reads and
// an explicitly enabled standalone service may also publish immutable
// process-start History/tick-delta generations. KLine is readable only when
// explicitly enabled and always retains process-start coverage metadata; it
// cannot be treated as full-day. Factor/CERTIFIED remain unavailable.
enum l2flow_shm_server_state_v2 {
    L2FLOW_SHM_SERVER_INITIALIZING_V2 = 1,
    L2FLOW_SHM_SERVER_ACTIVE_V2 = 2,
    L2FLOW_SHM_SERVER_DRAINING_V2 = 3,
    L2FLOW_SHM_SERVER_STOPPED_CLEAN_V2 = 4,
    L2FLOW_SHM_SERVER_FAILED_V2 = 5,
    L2FLOW_SHM_SERVER_LIVE_PARTIAL_V2 = 6,
};

enum l2flow_shm_header_flag_v2 {
    L2FLOW_SHM_HEADER_COVERAGE_LOST_V2 = 1U << 0U,
    L2FLOW_SHM_HEADER_KLINE_ENABLED_V2 = 1U << 1U,
    L2FLOW_SHM_HEADER_COVERAGE_FROM_OPEN_V2 = 1U << 2U,
    L2FLOW_SHM_HEADER_STARTUP_PREFIX_RECOVERED_V2 = 1U << 3U,
    L2FLOW_SHM_HEADER_FULL_DAY_KLINE_VALID_V2 = 1U << 4U,
    L2FLOW_SHM_HEADER_FULL_DAY_FACTOR_VALID_V2 = 1U << 5U,
    L2FLOW_SHM_HEADER_CERTIFIED_PREFIX_VALID_V2 = 1U << 6U,
};

enum l2flow_kline_coverage_kind_v2 {
    L2FLOW_KLINE_COVERAGE_DISABLED_V2 = 0,
    L2FLOW_KLINE_COVERAGE_FROM_OPEN_V2 = 1,
    L2FLOW_KLINE_COVERAGE_PROCESS_START_PARTIAL_V2 = 2,
};

enum l2flow_kline_coverage_flag_v2 {
    L2FLOW_KLINE_PROCESS_START_PARTIAL_V2 = 1U << 0U,
    L2FLOW_KLINE_NATURAL_WINDOW_LEFT_TRUNCATED_V2 = 1U << 1U,
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
    L2FLOW_SELECTION_CATALOG_ALL_V2 = 1,
    L2FLOW_SELECTION_AVAILABLE_ANY_V2 = 2,
    L2FLOW_SELECTION_SNAPSHOT_AVAILABLE_V2 = 3,
    L2FLOW_SELECTION_TICK_AVAILABLE_V2 = 4,
    L2FLOW_SELECTION_FACTOR_ELIGIBLE_V2 = 5,
    L2FLOW_SELECTION_BOUND_V2 = L2FLOW_SELECTION_CATALOG_ALL_V2,
    L2FLOW_SELECTION_OBSERVED_ANY_V2 =
        L2FLOW_SELECTION_AVAILABLE_ANY_V2,
};

enum l2flow_catalog_scope_v2 {
    L2FLOW_CATALOG_DECLARED_DAILY_A_SHARE_V2 = 2,
};

// Stable error set for the stateful instrument raw-event history API. This
// interface returns normalized Wire V2 tick inputs retained by the Store; it
// does not return derived/canonical ORDER/TRADE/CANCEL lifecycle rows.
// Protocol and ABI errors are fail-closed: the owning session can no longer
// be reused.
enum l2flow_instrument_raw_event_history_error_v2 {
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_OK_V2 = 0,
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_INVALID_ARGUMENT_V2 = 1,
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_SYSTEM_ERROR_V2 = 2,
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_PROTOCOL_ERROR_V2 = 3,
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_ABI_MISMATCH_V2 = 4,
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_UNAVAILABLE_V2 = 5,
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_NOT_FOUND_V2 = 6,
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_RESOURCE_EXHAUSTED_V2 = 7,
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_CHECKPOINT_MISMATCH_V2 = 8,
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_NOT_READY_V2 = 9,
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_CLOSED_V2 = 10,
};

// C projection of one immutable Store-generation endpoint. This is an exact
// process/session cut, not a claim that the upstream exchange feed was
// complete.
typedef struct l2flow_instrument_raw_event_history_endpoint_v2 {
    uint8_t run_id[16];
    uint64_t session_epoch;
    uint64_t generation;
    uint64_t catalog_generation;
    uint64_t data_state_generation;
    uint64_t ingress_sequence_exclusive;
    uint64_t tick_stream_sequence_exclusive;
    uint64_t recv_monotonic_cut_ns;
    uint64_t history_published_monotonic_ns;
    uint64_t accepted_sequence;
    uint64_t applied_sequence;
    uint8_t catalog_digest[32];
    uint8_t input_identity_sha256[32];
    uint32_t source_stream_ids[4];
    uint64_t source_sequence_exclusive[4];
    uint32_t trade_date;
    uint32_t capacity;
    uint32_t bound_count;
    uint32_t available_count;
    uint32_t snapshot_available_count;
    uint32_t tick_available_count;
    uint32_t factor_eligible_count;
    uint32_t catalog_scope;
    uint32_t coverage_complete;
    uint32_t flags;
} l2flow_instrument_raw_event_history_endpoint_v2;

// A rolling checkpoint is instrument-local and target-generation exact.
// Source slots 1 and 3 are the Shanghai and Shenzhen tick lanes; slots 0 and
// 2 are always zero. A checkpoint obtained before explicit EOF is unverified
// and is therefore never returned by the public cursor API.
typedef struct l2flow_instrument_raw_event_history_checkpoint_v2 {
    l2flow_instrument_raw_event_history_endpoint_v2 generation;
    uint32_t instrument_id;
    uint32_t ordinal;
    uint64_t instrument_event_source_record_counts[4];
    uint64_t instrument_event_record_count;
    uint32_t payload_projection;
    uint32_t flags;
    uint8_t reserved[8];
} l2flow_instrument_raw_event_history_checkpoint_v2;

enum l2flow_instrument_raw_event_history_base_kind_v2 {
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_ORIGIN_V2 = 1,
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_CHECKPOINT_V2 = 2,
};

enum l2flow_instrument_raw_event_history_endpoint_flag_v2 {
    // The process was continuously healthy from its asserted session-open
    // boundary. This is not an upstream/exchange completeness claim.
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_COVERAGE_FROM_OPEN_V2 =
        1 << 0,
    // The immutable Store cut accounts for every accepted process record.
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_RECORD_COVERAGE_COMPLETE_V2 =
        1 << 1,
};

enum l2flow_instrument_raw_event_history_stream_flag_v2 {
    // Every normalized Wire event retained in source slots 1 and 3 is
    // represented between the declared base and target boundaries.
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_EVENT_COVERAGE_COMPLETE_V2 =
        1 << 0,
};

enum l2flow_instrument_raw_event_history_projection_v2 {
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_CORE_WIRE_V2 = 1,
};

enum l2flow_instrument_raw_event_history_source_v2 {
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_SHANGHAI_SOURCE_SLOT_V2 = 1,
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_SHENZHEN_SOURCE_SLOT_V2 = 3,
    L2FLOW_INSTRUMENT_RAW_EVENT_HISTORY_SOURCE_MASK_V2 =
        (1 << 1) | (1 << 3),
};

typedef struct l2flow_instrument_raw_event_history_metadata_v2 {
    uint32_t base_kind;
    uint32_t selected_source_mask;
    l2flow_instrument_raw_event_history_checkpoint_v2 base_checkpoint;
    l2flow_instrument_raw_event_history_checkpoint_v2 target_checkpoint;
    uint64_t delta_event_source_record_counts[4];
    uint64_t delta_event_record_count;
    uint64_t ingress_sequence_begin_inclusive;
    uint64_t ingress_sequence_end_exclusive;
    uint64_t tick_stream_sequence_begin_inclusive;
    uint64_t tick_stream_sequence_end_exclusive;
    uint32_t flags;
    uint32_t payload_projection;
    uint8_t reserved[8];
} l2flow_instrument_raw_event_history_metadata_v2;

// Borrowed zero-copy view of one immutable event page. event_records addresses
// event_record_count consecutive Wire V2 RealtimeWireTickPayloadV2 records
// using event_record_stride bytes. The pointer remains valid only until the
// next read on this cursor or cursor close. EOF is an explicit successful
// zero-record page; only then may verified_checkpoint be requested.
typedef struct l2flow_instrument_raw_event_history_page_v2 {
    const void* event_records;
    size_t event_record_count;
    size_t event_record_stride;
    uint64_t mapping_bytes;
    uint64_t page_index;
    uint64_t first_ingress_sequence;
    uint64_t last_ingress_sequence;
    uint64_t first_tick_stream_sequence;
    uint64_t last_tick_stream_sequence;
    uint64_t cumulative_record_count;
    uint64_t cumulative_source_record_counts[4];
    uint8_t eof;
    uint8_t reserved[7];
} l2flow_instrument_raw_event_history_page_v2;

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
    uint32_t catalog_trade_date;
    uint32_t reserved_catalog;
    uint64_t catalog_version;
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

// Fixed additive ABI for the KLine temporal-coverage contract. The existing
// 240-byte session_info_v2 structure remains unchanged. A process-start
// KLine service returns UNAVAILABLE until its nonzero boundary has been
// prepared; disabled/from-open KLine always reports a zero boundary.
typedef struct l2flow_kline_coverage_info_v2 {
    uint64_t session_epoch;
    uint64_t coverage_start_unix_ns;
    uint32_t coverage_kind;
    uint32_t reserved0;
    uint64_t reserved[1];
} l2flow_kline_coverage_info_v2;

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

// Maps fd read-only and accepts only the sealed Wire V2.4 layout. The caller
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
L2FLOW_SHM_READER_API_V2 int l2flow_shm_reader_kline_coverage_v2(
    const l2flow_shm_reader_v2* reader,
    l2flow_kline_coverage_info_v2* output);

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

// Session and cursor handles are not safe for concurrent calls. The caller
// must serialize every operation, including close, that touches a related
// session/cursor pair.
//
// Opens one stateful control connection and pins its latest immutable target
// generation. expected_session must be a successful session_v2 result from
// the matching shared-memory mapping. Its run/session/day/capacity and frozen
// daily-catalog scope, coverage, generation, bound count, and digest are
// checked against the pinned generation. catalog_trade_date/catalog_version
// must also be canonical; catalog_digest binds their catalog definition.
// expected_generation==0 selects the latest published generation, while a
// nonzero value requires that exact latest generation. timeout_ms==0 leaves
// socket operations blocking.
L2FLOW_SHM_READER_API_V2 int
l2flow_instrument_raw_event_history_session_open_v2(
    const char* absolute_control_socket_path,
    const l2flow_shm_session_info_v2* expected_session,
    uint64_t expected_generation,
    uint32_t timeout_ms,
    l2flow_instrument_raw_event_history_session_v2** output);

// The caller owns close synchronization. Closing a session with an active
// cursor invalidates that cursor; a cursor closed before EOF also fail-closes
// its session because the server-side stateful stream cannot be resumed.
L2FLOW_SHM_READER_API_V2 void
l2flow_instrument_raw_event_history_session_close_v2(
    l2flow_instrument_raw_event_history_session_v2* session);

L2FLOW_SHM_READER_API_V2 int
l2flow_instrument_raw_event_history_session_target_v2(
    const l2flow_instrument_raw_event_history_session_v2* session,
    l2flow_instrument_raw_event_history_endpoint_v2* output);

// Full retained normalized Wire tick-event history for one instrument:
// [origin, target). requested_page_records is a positive upper bound; the
// service may return smaller pages. At most one cursor may be active on a
// session.
L2FLOW_SHM_READER_API_V2 int
l2flow_instrument_raw_event_history_open_full_v2(
    l2flow_instrument_raw_event_history_session_v2* session,
    uint32_t instrument_id,
    uint32_t requested_page_records,
    l2flow_instrument_raw_event_history_cursor_v2** output);

// Rolling update for one instrument: [base checkpoint, target generation).
// The base must be a verified checkpoint from an earlier read in the same
// retained run/session and for the same instrument.
L2FLOW_SHM_READER_API_V2 int
l2flow_instrument_raw_event_history_open_update_v2(
    l2flow_instrument_raw_event_history_session_v2* session,
    uint32_t instrument_id,
    uint32_t requested_page_records,
    const l2flow_instrument_raw_event_history_checkpoint_v2*
        base_checkpoint,
    l2flow_instrument_raw_event_history_cursor_v2** output);

L2FLOW_SHM_READER_API_V2 void
l2flow_instrument_raw_event_history_cursor_close_v2(
    l2flow_instrument_raw_event_history_cursor_v2* cursor);

L2FLOW_SHM_READER_API_V2 int
l2flow_instrument_raw_event_history_cursor_metadata_v2(
    const l2flow_instrument_raw_event_history_cursor_v2* cursor,
    l2flow_instrument_raw_event_history_metadata_v2* output);

// On every call the previous borrowed page is invalidated. A successful
// nonterminal result has eof=0 and at least one record. A successful terminal
// result has eof=1 and zero records. Calling again after EOF returns the same
// logical EOF without network I/O.
L2FLOW_SHM_READER_API_V2 int
l2flow_instrument_raw_event_history_cursor_read_v2(
    l2flow_instrument_raw_event_history_cursor_v2* cursor,
    l2flow_instrument_raw_event_history_page_v2* output);

// Returns NOT_READY until a canonical explicit EOF has reconciled the total
// and per-source record counts. This rule prevents a partially consumed page
// stream from being used as the base of a later rolling update.
L2FLOW_SHM_READER_API_V2 int
l2flow_instrument_raw_event_history_cursor_verified_checkpoint_v2(
    const l2flow_instrument_raw_event_history_cursor_v2* cursor,
    l2flow_instrument_raw_event_history_checkpoint_v2* output);

L2FLOW_SHM_READER_API_V2 const char*
l2flow_instrument_raw_event_history_error_name_v2(int error);

#ifdef __cplusplus
}
#endif

#undef L2FLOW_SHM_READER_API_V2
