#pragma once

#include "l2flow/ipc/realtime_wire_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace l2flow::ipc {

// History queries use the existing realtime control magic and protocol
// version on a dedicated SOCK_SEQPACKET connection. Page memfds use their own
// magic so they can never be mistaken for the mutable latest-value mapping.
inline constexpr std::array<std::uint8_t, 8U> kRealtimeHistoryMagicV1{
    'L', '2', 'F', 'H', 'S', 'T', '1', '\0'};
inline constexpr std::size_t kRealtimeHistoryPageHeaderBytesV1 = 4096U;

enum class RealtimeHistoryControlOpcodeV1 : std::uint16_t {
    kOpenHistory = 2U,
    kReadHistory = 3U,
};

// Values 0..3 deliberately retain RealtimeControlStatusV1 semantics.
enum class RealtimeHistoryControlStatusV1 : std::uint16_t {
    kOk = 0U,
    kInvalidRequest = 1U,
    kUnsupportedVersion = 2U,
    kUnavailable = 3U,
    kNotFound = 4U,
    kResourceExhausted = 5U,
    kInternalFailure = 6U,
};

enum class RealtimeHistoryPayloadKindV1 : std::uint8_t {
    kSnapshot = 1U,
    kTick = 2U,
};

enum class RealtimeHistoryPayloadProjectionV1 : std::uint32_t {
    // Reuses RealtimeWireSnapshotPayloadV1 and
    // RealtimeWireTickPayloadV1. It preserves one output record for every
    // Store record but is intentionally not an exact serialization of every
    // field retained by StoredMarketEventViewV1.
    kCoreV1 = 1U,
};

enum RealtimeHistoryGenerationFlagV1 : std::uint32_t {
    kRealtimeHistoryCoverageFromOpenV1 = 1U << 0U,
    // The Store generation is a verified complete accepted-process prefix.
    // This does not by itself assert that the process covered market open or
    // that the upstream vendor feed was complete.
    kRealtimeHistoryRecordCoverageCompleteV1 = 1U << 1U,
    // Reserved for a future lossless StoredMarketEvent wire projection.
    // A CoreV1 producer MUST leave this bit clear.
    kRealtimeHistoryFieldCompleteV1 = 1U << 2U,
};

enum RealtimeHistoryRecordProjectionFlagV1 : std::uint32_t {
    // CoreV1 keeps SH raw strings inline up to the existing 32-byte tick
    // capacity. A longer retained string is represented by an empty inline
    // value plus the corresponding explicit omission flag; the record itself
    // must never be skipped.
    kRealtimeHistoryRawTypeOmittedV1 =
        kRealtimeWireTickRawTypeOmittedV1,
    kRealtimeHistoryRawTickFlagOmittedV1 =
        kRealtimeWireTickRawTickFlagOmittedV1,
};

enum RealtimeHistoryResponseFlagV1 : std::uint16_t {
    kRealtimeHistoryResponseTerminalV1 = 1U << 0U,
};

// Immutable identity and count authority for one instrument in one pinned
// Store generation. input_identity_sha256 identifies the generation cut and
// registry; it is not a digest of serialized record payload bytes.
struct RealtimeHistoryGenerationInfoV1 final {
    std::array<std::uint8_t, 16U> run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint64_t generation = 0U;
    std::uint32_t trade_date = 0U;
    std::uint32_t instrument_id = 0U;
    std::uint32_t registry_ordinal = 0U;
    std::uint32_t instrument_count = 0U;
    std::uint64_t ingress_sequence_exclusive = 0U;
    std::uint64_t recv_monotonic_cut_ns = 0U;
    std::uint64_t registry_version = 0U;
    std::array<std::uint8_t, 32U> registry_sha256{};
    std::array<std::uint8_t, 32U> input_identity_sha256{};
    std::array<std::uint32_t, 4U> source_stream_ids{};
    std::array<std::uint64_t, 4U> source_sequence_exclusive{};
    std::array<std::uint64_t, 4U> instrument_source_record_counts{};
    std::uint64_t instrument_record_count = 0U;
    std::uint32_t flags = 0U;
    std::uint32_t payload_projection = 0U;
    std::array<std::uint8_t, 24U> reserved{};
};
static_assert(sizeof(RealtimeHistoryGenerationInfoV1) == 256U);
static_assert(
    std::is_standard_layout_v<RealtimeHistoryGenerationInfoV1>);
static_assert(
    offsetof(
        RealtimeHistoryGenerationInfoV1,
        source_sequence_exclusive) == 152U);
static_assert(
    offsetof(
        RealtimeHistoryGenerationInfoV1,
        source_stream_ids) == 136U);
static_assert(
    offsetof(
        RealtimeHistoryGenerationInfoV1,
        instrument_source_record_counts) == 184U);
static_assert(
    offsetof(
        RealtimeHistoryGenerationInfoV1,
        instrument_record_count) == 216U);
static_assert(
    offsetof(RealtimeHistoryGenerationInfoV1, flags) == 224U);
static_assert(
    offsetof(
        RealtimeHistoryGenerationInfoV1,
        payload_projection) == 228U);
static_assert(
    offsetof(RealtimeHistoryGenerationInfoV1, reserved) == 232U);

// Open binds the connection to one immutable generation and one instrument
// cursor. V1 deliberately exposes only the complete oldest-first scan over
// [1, UINT64_MAX): a successful terminal response is therefore a complete
// record-coverage proof rather than a range/limit truncation.
struct RealtimeHistoryOpenRequestV1 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t protocol_major = 0U;
    std::uint16_t protocol_minor = 0U;
    std::uint16_t opcode = 0U;
    std::uint16_t reserved0 = 0U;
    std::uint32_t message_bytes = 0U;
    std::uint32_t flags = 0U;
    std::uint64_t request_id = 0U;
    std::uint64_t reserved1 = 0U;
    std::uint32_t instrument_id = 0U;
    std::uint32_t requested_page_records = 0U;
    std::array<std::uint64_t, 2U> reserved2{};
};
static_assert(sizeof(RealtimeHistoryOpenRequestV1) == 64U);
static_assert(std::is_standard_layout_v<RealtimeHistoryOpenRequestV1>);
static_assert(
    offsetof(RealtimeHistoryOpenRequestV1, instrument_id) == 40U);
static_assert(
    offsetof(
        RealtimeHistoryOpenRequestV1,
        requested_page_records) == 44U);
static_assert(
    offsetof(RealtimeHistoryOpenRequestV1, reserved2) == 48U);

// Response prefix matches RealtimeControlResponseV1 through request_id.
struct RealtimeHistoryOpenResponseV1 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t protocol_major = 0U;
    std::uint16_t protocol_minor = 0U;
    std::uint16_t status = 0U;
    std::uint16_t flags = 0U;
    std::uint32_t message_bytes = 0U;
    std::uint32_t reserved0 = 0U;
    std::uint64_t request_id = 0U;
    // Unpredictable per-cursor token required by the first READ. Rotating the
    // token on every data response rejects a future READ queued before the
    // current response unless the client guesses the 64-bit token.
    std::uint64_t initial_read_token = 0U;
    RealtimeHistoryGenerationInfoV1 generation{};
};
static_assert(sizeof(RealtimeHistoryOpenResponseV1) == 296U);
static_assert(std::is_standard_layout_v<RealtimeHistoryOpenResponseV1>);
static_assert(
    offsetof(
        RealtimeHistoryOpenResponseV1,
        initial_read_token) == 32U);
static_assert(
    offsetof(RealtimeHistoryOpenResponseV1, generation) == 40U);

// Read operates on the cursor bound to this connection. The expected page
// index makes retries, duplicates, and out-of-order requests fail explicitly.
struct RealtimeHistoryReadRequestV1 final {
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
static_assert(sizeof(RealtimeHistoryReadRequestV1) == 48U);
static_assert(std::is_standard_layout_v<RealtimeHistoryReadRequestV1>);
static_assert(
    offsetof(
        RealtimeHistoryReadRequestV1,
        expected_page_index) == 32U);
static_assert(
    offsetof(RealtimeHistoryReadRequestV1, read_token) == 40U);

// A successful nonterminal read passes exactly one sealed read-only page
// memfd with SCM_RIGHTS. The explicit terminal response has zero rows/bytes,
// sets kRealtimeHistoryResponseTerminalV1, and carries no descriptor.
struct RealtimeHistoryReadResponseV1 final {
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
    std::uint64_t generation = 0U;
    // Nonterminal success rotates to a nonzero unpredictable token for the
    // next READ. Terminal and error responses set it to zero.
    std::uint64_t next_read_token = 0U;
};
static_assert(sizeof(RealtimeHistoryReadResponseV1) == 64U);
static_assert(std::is_standard_layout_v<RealtimeHistoryReadResponseV1>);
static_assert(
    offsetof(
        RealtimeHistoryReadResponseV1,
        record_count) == 20U);
static_assert(
    offsetof(
        RealtimeHistoryReadResponseV1,
        page_mapping_bytes) == 32U);
static_assert(
    offsetof(RealtimeHistoryReadResponseV1, page_index) == 40U);
static_assert(
    offsetof(RealtimeHistoryReadResponseV1, generation) == 48U);
static_assert(
    offsetof(
        RealtimeHistoryReadResponseV1,
        next_read_token) == 56U);

// Descriptors preserve the sparse process sequences. payload_index addresses
// the dense array selected by payload_kind; it is not a sequence number.
struct RealtimeHistoryRecordDescriptorV1 final {
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t source_sequence = 0U;
    std::uint64_t tick_stream_sequence = 0U;
    std::uint32_t payload_index = 0U;
    std::uint32_t projection_flags = 0U;
    std::uint8_t payload_kind = 0U;
    std::uint8_t event_kind = 0U;
    std::uint8_t source_slot = 0U;
    std::uint8_t reserved0 = 0U;
    std::uint32_t reserved1 = 0U;
};
static_assert(sizeof(RealtimeHistoryRecordDescriptorV1) == 40U);
static_assert(
    std::is_standard_layout_v<RealtimeHistoryRecordDescriptorV1>);
static_assert(
    offsetof(RealtimeHistoryRecordDescriptorV1, payload_kind) == 32U);
static_assert(
    offsetof(
        RealtimeHistoryRecordDescriptorV1,
        projection_flags) == 28U);
static_assert(
    offsetof(RealtimeHistoryRecordDescriptorV1, reserved1) == 36U);

struct alignas(4096) RealtimeHistoryPageHeaderV1 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t abi_major = 0U;
    std::uint16_t abi_minor = 0U;
    std::uint32_t header_bytes = 0U;
    std::uint32_t endian_marker = 0U;
    std::uint32_t flags = 0U;
    std::uint64_t total_mapping_bytes = 0U;
    std::uint64_t page_index = 0U;
    std::uint32_t record_count = 0U;
    std::uint32_t record_descriptor_bytes = 0U;
    std::uint64_t record_descriptors_offset = 0U;
    std::uint64_t snapshot_payloads_offset = 0U;
    std::uint32_t snapshot_count = 0U;
    std::uint32_t snapshot_payload_bytes = 0U;
    std::uint64_t tick_payloads_offset = 0U;
    std::uint32_t tick_count = 0U;
    std::uint32_t tick_payload_bytes = 0U;
    std::uint64_t first_ingress_sequence = 0U;
    std::uint64_t last_ingress_sequence = 0U;
    RealtimeHistoryGenerationInfoV1 generation{};
    std::array<std::uint8_t, 3736U> reserved{};
};
static_assert(
    sizeof(RealtimeHistoryPageHeaderV1) ==
    kRealtimeHistoryPageHeaderBytesV1);
static_assert(alignof(RealtimeHistoryPageHeaderV1) == 4096U);
static_assert(std::is_standard_layout_v<RealtimeHistoryPageHeaderV1>);
static_assert(
    offsetof(
        RealtimeHistoryPageHeaderV1,
        record_descriptors_offset) == 48U);
static_assert(
    offsetof(
        RealtimeHistoryPageHeaderV1,
        snapshot_payloads_offset) == 56U);
static_assert(
    offsetof(
        RealtimeHistoryPageHeaderV1,
        tick_payloads_offset) == 72U);
static_assert(
    offsetof(
        RealtimeHistoryPageHeaderV1,
        first_ingress_sequence) == 88U);
static_assert(
    offsetof(RealtimeHistoryPageHeaderV1, generation) == 104U);
static_assert(
    offsetof(RealtimeHistoryPageHeaderV1, reserved) == 360U);

}  // namespace l2flow::ipc
