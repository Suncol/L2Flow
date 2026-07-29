#pragma once

#include "l2flow/ipc/realtime_wire_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace l2flow::ipc {

// Instrument tick-delta control messages share the realtime control magic and
// protocol version, but use independent opcodes and an independent page
// magic. One SOCK_SEQPACKET connection first pins one immutable target Store
// generation, then may open and exhaust multiple instrument cursors against
// that same target.
inline constexpr std::array<std::uint8_t, 8U>
    kRealtimeInstrumentTickDeltaPageMagicV2{
        'L', '2', 'F', 'I', 'D', 'T', '2', '\0'};
inline constexpr std::size_t
    kRealtimeInstrumentTickDeltaPageHeaderBytesV2 = 4096U;
inline constexpr std::uint32_t
    kRealtimeInstrumentTickDeltaSourceMaskV2 =
        (1U << 1U) | (1U << 3U);

enum class RealtimeInstrumentTickDeltaControlOpcodeV2 :
    std::uint16_t {
    kOpenDeltaSession = 4U,
    kOpenInstrumentDelta = 5U,
    kReadInstrumentDelta = 6U,
};

// Values 0..6 deliberately retain the realtime/history control status
// meanings. Checkpoint mismatch is separate from malformed wire input so a
// client can discard or rebuild stale rolling state without treating the
// service as corrupt.
enum class RealtimeInstrumentTickDeltaControlStatusV2 :
    std::uint16_t {
    kOk = 0U,
    kInvalidRequest = 1U,
    kUnsupportedVersion = 2U,
    kUnavailable = 3U,
    kNotFound = 4U,
    kResourceExhausted = 5U,
    kInternalFailure = 6U,
    kCheckpointMismatch = 7U,
};

enum class RealtimeInstrumentTickDeltaBaseKindV2 :
    std::uint32_t {
    // The canonical retained-session origin has every exclusive sequence set
    // to one and every instrument count set to zero. It is not generation 0;
    // the wire base_checkpoint is all-zero and the canonical bounds are
    // carried by delta metadata.
    kOrigin = 1U,
    // A complete checkpoint previously returned as target_checkpoint.
    kCheckpoint = 2U,
};

enum class RealtimeInstrumentTickDeltaPayloadProjectionV2 :
    std::uint32_t {
    // Reuses RealtimeWireTickPayloadV1. Every selected Store tick has one
    // output row, but the projection is not field-complete.
    kCoreV1 = 1U,
};

enum RealtimeInstrumentTickDeltaEndpointFlagV2 : std::uint32_t {
    kRealtimeInstrumentTickDeltaCoverageFromOpenV2 = 1U << 0U,
    // Complete accepted-process record coverage for the selected tick lanes.
    // This is not a claim of snapshot coverage or upstream feed completeness.
    kRealtimeInstrumentTickDeltaTickRecordCoverageCompleteV2 =
        1U << 1U,
    // Reserved for a future lossless StoredMarketEvent projection. A CoreV1
    // producer MUST leave this bit clear.
    kRealtimeInstrumentTickDeltaFieldCompleteV2 = 1U << 2U,
};

enum RealtimeInstrumentTickDeltaResponseFlagV2 : std::uint16_t {
    kRealtimeInstrumentTickDeltaResponseTerminalV2 = 1U << 0U,
};

// Complete global identity of one immutable Store generation cut.
// tick_stream_sequence_exclusive is checked against
// 1 + (source[1].exclusive - 1) + (source[3].exclusive - 1).
// input_identity_sha256 identifies the accepted-process cut and registry; it
// is not a digest of serialized payload bytes.
struct RealtimeInstrumentTickDeltaGenerationEndpointV2 final {
    std::array<std::uint8_t, 16U> run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint64_t generation = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t instrument_count = 0U;
    std::uint64_t ingress_sequence_exclusive = 0U;
    std::uint64_t tick_stream_sequence_exclusive = 0U;
    std::uint64_t recv_monotonic_cut_ns = 0U;
    std::uint64_t registry_version = 0U;
    std::array<std::uint8_t, 32U> registry_sha256{};
    std::array<std::uint8_t, 32U> input_identity_sha256{};
    std::array<std::uint32_t, 4U> source_stream_ids{};
    std::array<std::uint64_t, 4U> source_sequence_exclusive{};
    std::uint32_t flags = 0U;
    std::uint32_t payload_projection = 0U;
    std::array<std::uint8_t, 64U> reserved{};
};
static_assert(
    sizeof(RealtimeInstrumentTickDeltaGenerationEndpointV2) ==
    256U);
static_assert(
    std::is_standard_layout_v<
        RealtimeInstrumentTickDeltaGenerationEndpointV2>);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaGenerationEndpointV2,
        ingress_sequence_exclusive) == 40U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaGenerationEndpointV2,
        tick_stream_sequence_exclusive) == 48U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaGenerationEndpointV2,
        source_stream_ids) == 136U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaGenerationEndpointV2,
        source_sequence_exclusive) == 152U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaGenerationEndpointV2,
        flags) == 184U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaGenerationEndpointV2,
        reserved) == 192U);

// Instrument-local rolling checkpoint at one generation endpoint. Slots 0
// and 2 of instrument_tick_source_record_counts are always zero. The total is
// exactly the checked sum of slots 1 and 3.
struct RealtimeInstrumentTickDeltaCheckpointV2 final {
    RealtimeInstrumentTickDeltaGenerationEndpointV2 generation{};
    std::uint32_t instrument_id = 0U;
    std::uint32_t registry_ordinal = 0U;
    std::array<std::uint64_t, 4U>
        instrument_tick_source_record_counts{};
    std::uint64_t instrument_tick_record_count = 0U;
    std::array<std::uint8_t, 16U> reserved{};
};
static_assert(
    sizeof(RealtimeInstrumentTickDeltaCheckpointV2) == 320U);
static_assert(
    std::is_standard_layout_v<
        RealtimeInstrumentTickDeltaCheckpointV2>);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaCheckpointV2,
        instrument_id) == 256U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaCheckpointV2,
        instrument_tick_source_record_counts) == 264U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaCheckpointV2,
        instrument_tick_record_count) == 296U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaCheckpointV2,
        reserved) == 304U);

// Immutable authority for one finite instrument delta. The redundant bounds
// and counts are intentional: readers can verify both endpoints and the
// subtraction without deriving a frontier from the last sparse output row.
struct RealtimeInstrumentTickDeltaMetadataV2 final {
    std::uint32_t base_kind = 0U;
    std::uint32_t selected_source_mask = 0U;
    // All-zero for kOrigin; complete and nonzero for kCheckpoint.
    RealtimeInstrumentTickDeltaCheckpointV2 base_checkpoint{};
    RealtimeInstrumentTickDeltaCheckpointV2 target_checkpoint{};
    std::array<std::uint64_t, 4U> delta_tick_source_record_counts{};
    std::uint64_t delta_tick_record_count = 0U;
    std::uint64_t ingress_sequence_begin_inclusive = 0U;
    std::uint64_t ingress_sequence_end_exclusive = 0U;
    std::uint64_t tick_stream_sequence_begin_inclusive = 0U;
    std::uint64_t tick_stream_sequence_end_exclusive = 0U;
    std::uint32_t flags = 0U;
    std::uint32_t payload_projection = 0U;
    std::array<std::uint8_t, 8U> reserved{};
};
static_assert(
    sizeof(RealtimeInstrumentTickDeltaMetadataV2) == 736U);
static_assert(
    std::is_standard_layout_v<
        RealtimeInstrumentTickDeltaMetadataV2>);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaMetadataV2,
        base_checkpoint) == 8U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaMetadataV2,
        target_checkpoint) == 328U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaMetadataV2,
        delta_tick_source_record_counts) == 648U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaMetadataV2,
        delta_tick_record_count) == 680U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaMetadataV2,
        ingress_sequence_begin_inclusive) == 688U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaMetadataV2,
        tick_stream_sequence_begin_inclusive) == 704U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaMetadataV2,
        flags) == 720U);

struct RealtimeInstrumentTickDeltaOpenSessionRequestV2 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t protocol_major = 0U;
    std::uint16_t protocol_minor = 0U;
    std::uint16_t opcode = 0U;
    std::uint16_t reserved0 = 0U;
    std::uint32_t message_bytes = 0U;
    std::uint32_t flags = 0U;
    std::uint64_t request_id = 0U;
    std::uint64_t reserved1 = 0U;
};
static_assert(
    sizeof(RealtimeInstrumentTickDeltaOpenSessionRequestV2) == 40U);
static_assert(
    std::is_standard_layout_v<
        RealtimeInstrumentTickDeltaOpenSessionRequestV2>);

struct RealtimeInstrumentTickDeltaOpenSessionResponseV2 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t protocol_major = 0U;
    std::uint16_t protocol_minor = 0U;
    std::uint16_t status = 0U;
    std::uint16_t flags = 0U;
    std::uint32_t message_bytes = 0U;
    std::uint32_t reserved0 = 0U;
    std::uint64_t request_id = 0U;
    RealtimeInstrumentTickDeltaGenerationEndpointV2
        target_generation{};
    // Nonzero unpredictable value required by every instrument OPEN on this
    // connection. It is distinct from each cursor's rotating read token.
    std::uint64_t delta_session_token = 0U;
};
static_assert(
    sizeof(RealtimeInstrumentTickDeltaOpenSessionResponseV2) ==
    296U);
static_assert(
    std::is_standard_layout_v<
        RealtimeInstrumentTickDeltaOpenSessionResponseV2>);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaOpenSessionResponseV2,
        target_generation) == 32U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaOpenSessionResponseV2,
        delta_session_token) == 288U);

struct RealtimeInstrumentTickDeltaOpenInstrumentRequestV2 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t protocol_major = 0U;
    std::uint16_t protocol_minor = 0U;
    std::uint16_t opcode = 0U;
    std::uint16_t reserved0 = 0U;
    std::uint32_t message_bytes = 0U;
    std::uint32_t flags = 0U;
    std::uint64_t request_id = 0U;
    std::uint32_t instrument_id = 0U;
    std::uint32_t requested_page_records = 0U;
    std::uint32_t base_kind = 0U;
    std::uint32_t reserved1 = 0U;
    std::uint64_t delta_session_token = 0U;
    RealtimeInstrumentTickDeltaCheckpointV2 base_checkpoint{};
    std::array<std::uint8_t, 8U> reserved2{};
};
static_assert(
    sizeof(RealtimeInstrumentTickDeltaOpenInstrumentRequestV2) ==
    384U);
static_assert(
    std::is_standard_layout_v<
        RealtimeInstrumentTickDeltaOpenInstrumentRequestV2>);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaOpenInstrumentRequestV2,
        instrument_id) == 32U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaOpenInstrumentRequestV2,
        delta_session_token) == 48U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaOpenInstrumentRequestV2,
        base_checkpoint) == 56U);

struct RealtimeInstrumentTickDeltaOpenInstrumentResponseV2 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t protocol_major = 0U;
    std::uint16_t protocol_minor = 0U;
    std::uint16_t status = 0U;
    std::uint16_t flags = 0U;
    std::uint32_t message_bytes = 0U;
    std::uint32_t reserved0 = 0U;
    std::uint64_t request_id = 0U;
    std::uint64_t initial_read_token = 0U;
    RealtimeInstrumentTickDeltaMetadataV2 metadata{};
};
static_assert(
    sizeof(
        RealtimeInstrumentTickDeltaOpenInstrumentResponseV2) ==
    776U);
static_assert(
    std::is_standard_layout_v<
        RealtimeInstrumentTickDeltaOpenInstrumentResponseV2>);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaOpenInstrumentResponseV2,
        initial_read_token) == 32U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaOpenInstrumentResponseV2,
        metadata) == 40U);

struct RealtimeInstrumentTickDeltaReadRequestV2 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t protocol_major = 0U;
    std::uint16_t protocol_minor = 0U;
    std::uint16_t opcode = 0U;
    std::uint16_t reserved0 = 0U;
    std::uint32_t message_bytes = 0U;
    std::uint32_t flags = 0U;
    std::uint64_t request_id = 0U;
    std::uint64_t expected_page_index = 0U;
    std::uint64_t read_token = 0U;
};
static_assert(
    sizeof(RealtimeInstrumentTickDeltaReadRequestV2) == 48U);
static_assert(
    std::is_standard_layout_v<
        RealtimeInstrumentTickDeltaReadRequestV2>);

struct RealtimeInstrumentTickDeltaReadResponseV2 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t protocol_major = 0U;
    std::uint16_t protocol_minor = 0U;
    std::uint16_t status = 0U;
    std::uint16_t flags = 0U;
    std::uint32_t message_bytes = 0U;
    std::uint32_t record_count = 0U;
    std::uint64_t request_id = 0U;
    std::uint64_t page_mapping_bytes = 0U;
    std::uint64_t page_index = 0U;
    std::uint64_t target_generation = 0U;
    std::uint64_t next_read_token = 0U;
};
static_assert(
    sizeof(RealtimeInstrumentTickDeltaReadResponseV2) == 64U);
static_assert(
    std::is_standard_layout_v<
        RealtimeInstrumentTickDeltaReadResponseV2>);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaReadResponseV2,
        record_count) == 20U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaReadResponseV2,
        page_mapping_bytes) == 32U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaReadResponseV2,
        next_read_token) == 56U);

// Every data page is one dense oldest-first RealtimeWireTickPayloadV1 array.
// A terminal response has zero rows and carries no page descriptor.
struct alignas(4096) RealtimeInstrumentTickDeltaPageHeaderV2 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t abi_major = 0U;
    std::uint16_t abi_minor = 0U;
    std::uint32_t header_bytes = 0U;
    std::uint32_t endian_marker = 0U;
    std::uint32_t flags = 0U;
    std::uint64_t total_mapping_bytes = 0U;
    std::uint64_t page_index = 0U;
    std::uint32_t record_count = 0U;
    std::uint32_t tick_payload_bytes = 0U;
    std::uint64_t tick_payloads_offset = 0U;
    std::uint64_t first_ingress_sequence = 0U;
    std::uint64_t last_ingress_sequence = 0U;
    std::uint64_t first_tick_stream_sequence = 0U;
    std::uint64_t last_tick_stream_sequence = 0U;
    RealtimeInstrumentTickDeltaMetadataV2 metadata{};
    std::array<std::uint8_t, 3272U> reserved{};
};
static_assert(
    sizeof(RealtimeInstrumentTickDeltaPageHeaderV2) ==
    kRealtimeInstrumentTickDeltaPageHeaderBytesV2);
static_assert(
    alignof(RealtimeInstrumentTickDeltaPageHeaderV2) == 4096U);
static_assert(
    std::is_standard_layout_v<
        RealtimeInstrumentTickDeltaPageHeaderV2>);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaPageHeaderV2,
        tick_payloads_offset) == 48U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaPageHeaderV2,
        first_ingress_sequence) == 56U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaPageHeaderV2,
        first_tick_stream_sequence) == 72U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaPageHeaderV2,
        metadata) == 88U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaPageHeaderV2,
        reserved) == 824U);

}  // namespace l2flow::ipc
