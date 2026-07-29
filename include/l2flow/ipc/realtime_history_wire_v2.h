#pragma once

#include "l2flow/ipc/realtime_wire_v2.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

namespace l2flow::ipc {

// Complete-history control messages use the Wire V2 control magic and
// protocol version on one stateful SOCK_SEQPACKET connection. OPEN pins one
// immutable Store generation and one instrument cursor. Every successful
// nonterminal READ returns one independently sealed, read-only page memfd.
// The terminal READ is an explicit zero-record response and carries no fd.
inline constexpr std::array<std::uint8_t, 8U>
    kRealtimeHistoryPageMagicV2{
        'L', '2', 'F', 'H', 'S', 'T', '2', '\0'};
inline constexpr std::size_t kRealtimeHistoryPageHeaderBytesV2 = 4096U;

enum class RealtimeHistoryControlOpcodeV2 : std::uint16_t {
    kOpenHistory = 2U,
    kReadHistory = 3U,
};

// Values 0..3 deliberately match RealtimeControlStatusV2. History-specific
// failures extend that closed set; there is no V1 status translation.
enum class RealtimeHistoryControlStatusV2 : std::uint16_t {
    kOk = 0U,
    kInvalidRequest = 1U,
    kUnsupportedVersion = 2U,
    kUnavailable = 3U,
    kNotFound = 4U,
    kResourceExhausted = 5U,
    kInternalFailure = 6U,
    kGenerationChanged = 7U,
};

enum class RealtimeHistoryPayloadKindV2 : std::uint8_t {
    kSnapshot = 1U,
    kTick = 2U,
};

enum class RealtimeHistoryPayloadProjectionV2 : std::uint32_t {
    // Reuses RealtimeWireSnapshotPayloadV2 and
    // RealtimeWireTickPayloadV2. It preserves one output descriptor for every
    // retained Store record, but it is not a lossless serialization of every
    // field in StoredMarketEventViewV1.
    kCoreV2 = 1U,
};

// These flags describe the retained process history represented by an exact
// immutable generation. They never assert authoritative exchange-universe or
// upstream-feed completeness.
enum RealtimeGenerationEndpointFlagV2 : std::uint32_t {
    kRealtimeGenerationCoverageFromOpenV2 = 1U << 0U,
    kRealtimeGenerationRecordCoverageCompleteV2 = 1U << 1U,
};

enum RealtimeHistoryRecordProjectionFlagV2 : std::uint32_t {
    kRealtimeHistoryRawTypeOmittedV2 = 1U << 0U,
    kRealtimeHistoryRawTickFlagOmittedV2 = 1U << 1U,
};

enum RealtimeHistoryResponseFlagV2 : std::uint16_t {
    kRealtimeHistoryResponseTerminalV2 = 1U << 0U,
};

namespace realtime_history_wire_v2_detail {

template <typename T, std::size_t Size>
[[nodiscard]] constexpr bool AllZero(
    const std::array<T, Size>& values) noexcept {
    for (const T value : values) {
        if (value != T{}) {
            return false;
        }
    }
    return true;
}

template <typename T, std::size_t Size>
[[nodiscard]] constexpr bool AnyNonzero(
    const std::array<T, Size>& values) noexcept {
    return !AllZero(values);
}

[[nodiscard]] constexpr bool CheckedAdd(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t& output) noexcept {
    if (right >
        std::numeric_limits<std::uint64_t>::max() - left) {
        return false;
    }
    output = left + right;
    return true;
}

}  // namespace realtime_history_wire_v2_detail

// One immutable observed-universe Store generation endpoint. The independent
// accepted/durable/applied fields deliberately do not impose an ordering
// between durable and applied. history_published_monotonic_ns is the t2
// release-publication boundary for callback-to-history latency measurement.
// input_identity_sha256 identifies the accepted-process cut and catalog; it
// is not a digest of serialized page payload bytes.
struct RealtimeGenerationEndpointV2 final {
    std::array<std::uint8_t, 16U> run_id{};
    std::uint64_t session_epoch = 0U;
    std::uint64_t generation = 0U;
    std::uint64_t catalog_generation = 0U;
    std::uint64_t data_state_generation = 0U;
    std::uint64_t ingress_sequence_exclusive = 0U;
    std::uint64_t tick_stream_sequence_exclusive = 0U;
    std::uint64_t recv_monotonic_cut_ns = 0U;
    std::uint64_t history_published_monotonic_ns = 0U;
    std::uint64_t accepted_sequence = 0U;
    std::uint64_t durable_sequence = 0U;
    std::uint64_t applied_sequence = 0U;
    std::array<std::uint8_t, 32U> catalog_digest{};
    std::array<std::uint8_t, 32U> input_identity_sha256{};
    std::array<std::uint32_t, 4U> source_stream_ids{};
    std::array<std::uint64_t, 4U> source_sequence_exclusive{};
    std::uint32_t trade_date = 0U;
    std::uint32_t capacity = 0U;
    std::uint32_t bound_count = 0U;
    std::uint32_t available_count = 0U;
    std::uint32_t snapshot_available_count = 0U;
    std::uint32_t tick_available_count = 0U;
    std::uint32_t factor_eligible_count = 0U;
    std::uint32_t catalog_scope = 0U;
    std::uint32_t coverage_complete = 0U;
    std::uint32_t flags = 0U;
};
static_assert(sizeof(RealtimeGenerationEndpointV2) == 256U);
static_assert(std::is_standard_layout_v<RealtimeGenerationEndpointV2>);
static_assert(offsetof(RealtimeGenerationEndpointV2, run_id) == 0U);
static_assert(
    offsetof(RealtimeGenerationEndpointV2, session_epoch) == 16U);
static_assert(offsetof(RealtimeGenerationEndpointV2, generation) == 24U);
static_assert(
    offsetof(RealtimeGenerationEndpointV2, catalog_generation) == 32U);
static_assert(
    offsetof(RealtimeGenerationEndpointV2, data_state_generation) == 40U);
static_assert(
    offsetof(
        RealtimeGenerationEndpointV2,
        ingress_sequence_exclusive) == 48U);
static_assert(
    offsetof(
        RealtimeGenerationEndpointV2,
        tick_stream_sequence_exclusive) == 56U);
static_assert(
    offsetof(RealtimeGenerationEndpointV2, recv_monotonic_cut_ns) == 64U);
static_assert(
    offsetof(
        RealtimeGenerationEndpointV2,
        history_published_monotonic_ns) == 72U);
static_assert(
    offsetof(RealtimeGenerationEndpointV2, accepted_sequence) == 80U);
static_assert(
    offsetof(RealtimeGenerationEndpointV2, durable_sequence) == 88U);
static_assert(
    offsetof(RealtimeGenerationEndpointV2, applied_sequence) == 96U);
static_assert(
    offsetof(RealtimeGenerationEndpointV2, catalog_digest) == 104U);
static_assert(
    offsetof(
        RealtimeGenerationEndpointV2,
        input_identity_sha256) == 136U);
static_assert(
    offsetof(RealtimeGenerationEndpointV2, source_stream_ids) == 168U);
static_assert(
    offsetof(
        RealtimeGenerationEndpointV2,
        source_sequence_exclusive) == 184U);
static_assert(
    offsetof(RealtimeGenerationEndpointV2, trade_date) == 216U);
static_assert(offsetof(RealtimeGenerationEndpointV2, capacity) == 220U);
static_assert(
    offsetof(RealtimeGenerationEndpointV2, bound_count) == 224U);
static_assert(
    offsetof(RealtimeGenerationEndpointV2, available_count) == 228U);
static_assert(
    offsetof(
        RealtimeGenerationEndpointV2,
        snapshot_available_count) == 232U);
static_assert(
    offsetof(
        RealtimeGenerationEndpointV2,
        tick_available_count) == 236U);
static_assert(
    offsetof(
        RealtimeGenerationEndpointV2,
        factor_eligible_count) == 240U);
static_assert(
    offsetof(RealtimeGenerationEndpointV2, catalog_scope) == 244U);
static_assert(
    offsetof(RealtimeGenerationEndpointV2, coverage_complete) == 248U);
static_assert(offsetof(RealtimeGenerationEndpointV2, flags) == 252U);

// Canonical immutable-generation identity. A generation is an applied Store
// cut, so its accepted/applied frontier is exactly ingress_exclusive - 1.
// Journal durability remains independent and may trail that frontier.
[[nodiscard]] constexpr bool RealtimeGenerationEndpointCanonicalV2(
    const RealtimeGenerationEndpointV2& endpoint) noexcept {
    constexpr std::uint32_t known_flags =
        kRealtimeGenerationCoverageFromOpenV2 |
        kRealtimeGenerationRecordCoverageCompleteV2;
    if (!realtime_history_wire_v2_detail::AnyNonzero(
            endpoint.run_id) ||
        endpoint.session_epoch == 0U || endpoint.generation == 0U ||
        endpoint.trade_date == 0U || endpoint.capacity == 0U ||
        endpoint.catalog_scope !=
            static_cast<std::uint32_t>(
                RealtimeCatalogScopeV2::kObservedOnly) ||
        endpoint.coverage_complete != 0U ||
        (endpoint.flags & ~known_flags) != 0U ||
        (endpoint.flags &
         kRealtimeGenerationRecordCoverageCompleteV2) == 0U ||
        !realtime_history_wire_v2_detail::AnyNonzero(
            endpoint.catalog_digest) ||
        !realtime_history_wire_v2_detail::AnyNonzero(
            endpoint.input_identity_sha256) ||
        endpoint.ingress_sequence_exclusive == 0U ||
        endpoint.tick_stream_sequence_exclusive == 0U ||
        endpoint.recv_monotonic_cut_ns == 0U ||
        endpoint.history_published_monotonic_ns == 0U ||
        endpoint.history_published_monotonic_ns <
            endpoint.recv_monotonic_cut_ns ||
        endpoint.accepted_sequence !=
            endpoint.ingress_sequence_exclusive - 1U ||
        endpoint.applied_sequence != endpoint.accepted_sequence ||
        endpoint.durable_sequence > endpoint.accepted_sequence ||
        endpoint.catalog_generation != endpoint.bound_count ||
        endpoint.bound_count > endpoint.capacity ||
        endpoint.available_count > endpoint.bound_count ||
        endpoint.snapshot_available_count >
            endpoint.available_count ||
        endpoint.tick_available_count > endpoint.available_count ||
        endpoint.factor_eligible_count >
            endpoint.snapshot_available_count) {
        return false;
    }

    std::uint64_t ingress_count = 0U;
    for (std::size_t source = 0U;
         source < endpoint.source_stream_ids.size();
         ++source) {
        if (endpoint.source_stream_ids[source] == 0U ||
            endpoint.source_sequence_exclusive[source] == 0U) {
            return false;
        }
        for (std::size_t prior = 0U; prior < source; ++prior) {
            if (endpoint.source_stream_ids[source] ==
                endpoint.source_stream_ids[prior]) {
                return false;
            }
        }
        if (!realtime_history_wire_v2_detail::CheckedAdd(
                ingress_count,
                endpoint.source_sequence_exclusive[source] - 1U,
                ingress_count)) {
            return false;
        }
    }
    if (ingress_count != endpoint.accepted_sequence) {
        return false;
    }

    std::uint64_t tick_count =
        endpoint.source_sequence_exclusive[1U] - 1U;
    return realtime_history_wire_v2_detail::CheckedAdd(
               tick_count,
               endpoint.source_sequence_exclusive[3U] - 1U,
               tick_count) &&
           endpoint.tick_stream_sequence_exclusive - 1U ==
               tick_count;
}

// Exact count authority for one instrument in one pinned generation. Source
// counts cover all four Store lanes. record_count equals their checked sum;
// snapshot_count + tick_count equals record_count.
struct RealtimeHistoryGenerationInfoV2 final {
    RealtimeGenerationEndpointV2 endpoint{};
    std::uint32_t instrument_id = 0U;
    std::uint32_t ordinal = 0U;
    std::array<std::uint64_t, 4U> instrument_source_record_counts{};
    std::uint64_t instrument_record_count = 0U;
    std::uint64_t snapshot_record_count = 0U;
    std::uint64_t tick_record_count = 0U;
    std::uint32_t payload_projection = 0U;
    std::uint32_t reserved0 = 0U;
    std::array<std::uint8_t, 8U> reserved{};
};
static_assert(sizeof(RealtimeHistoryGenerationInfoV2) == 336U);
static_assert(
    std::is_standard_layout_v<RealtimeHistoryGenerationInfoV2>);
static_assert(
    offsetof(RealtimeHistoryGenerationInfoV2, endpoint) == 0U);
static_assert(
    offsetof(RealtimeHistoryGenerationInfoV2, instrument_id) == 256U);
static_assert(
    offsetof(RealtimeHistoryGenerationInfoV2, ordinal) == 260U);
static_assert(
    offsetof(
        RealtimeHistoryGenerationInfoV2,
        instrument_source_record_counts) == 264U);
static_assert(
    offsetof(
        RealtimeHistoryGenerationInfoV2,
        instrument_record_count) == 296U);
static_assert(
    offsetof(
        RealtimeHistoryGenerationInfoV2,
        snapshot_record_count) == 304U);
static_assert(
    offsetof(
        RealtimeHistoryGenerationInfoV2,
        tick_record_count) == 312U);
static_assert(
    offsetof(
        RealtimeHistoryGenerationInfoV2,
        payload_projection) == 320U);
static_assert(
    offsetof(RealtimeHistoryGenerationInfoV2, reserved0) == 324U);
static_assert(
    offsetof(RealtimeHistoryGenerationInfoV2, reserved) == 328U);

[[nodiscard]] constexpr bool RealtimeHistoryGenerationInfoCanonicalV2(
    const RealtimeHistoryGenerationInfoV2& info) noexcept {
    if (!RealtimeGenerationEndpointCanonicalV2(info.endpoint) ||
        info.instrument_id == 0U ||
        info.ordinal != info.instrument_id - 1U ||
        info.ordinal >= info.endpoint.capacity ||
        info.ordinal >= info.endpoint.bound_count ||
        info.payload_projection !=
            static_cast<std::uint32_t>(
                RealtimeHistoryPayloadProjectionV2::kCoreV2) ||
        info.reserved0 != 0U ||
        !realtime_history_wire_v2_detail::AllZero(info.reserved)) {
        return false;
    }

    std::uint64_t total = 0U;
    for (std::size_t source = 0U;
         source < info.instrument_source_record_counts.size();
         ++source) {
        const std::uint64_t count =
            info.instrument_source_record_counts[source];
        if (count >
                info.endpoint.source_sequence_exclusive[source] - 1U ||
            !realtime_history_wire_v2_detail::CheckedAdd(
                total, count, total)) {
            return false;
        }
    }

    std::uint64_t projected_total = info.snapshot_record_count;
    if (!realtime_history_wire_v2_detail::CheckedAdd(
            projected_total,
            info.tick_record_count,
            projected_total)) {
        return false;
    }
    return total == info.instrument_record_count &&
           projected_total == total &&
           info.snapshot_record_count ==
               info.instrument_source_record_counts[0U] +
                   info.instrument_source_record_counts[2U] &&
           info.tick_record_count ==
               info.instrument_source_record_counts[1U] +
                   info.instrument_source_record_counts[3U];
}

// expected_generation==0 pins the latest published history generation.
// A nonzero value requests that exact latest generation and fails with
// kGenerationChanged rather than silently opening another cut.
struct RealtimeHistoryOpenRequestV2 final {
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
    std::uint64_t expected_generation = 0U;
    std::array<std::uint64_t, 2U> reserved{};
};
static_assert(sizeof(RealtimeHistoryOpenRequestV2) == 64U);
static_assert(std::is_standard_layout_v<RealtimeHistoryOpenRequestV2>);
static_assert(
    offsetof(RealtimeHistoryOpenRequestV2, instrument_id) == 32U);
static_assert(
    offsetof(
        RealtimeHistoryOpenRequestV2,
        requested_page_records) == 36U);
static_assert(
    offsetof(RealtimeHistoryOpenRequestV2, expected_generation) == 40U);
static_assert(
    offsetof(RealtimeHistoryOpenRequestV2, reserved) == 48U);

struct RealtimeHistoryOpenResponseV2 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t protocol_major = 0U;
    std::uint16_t protocol_minor = 0U;
    std::uint16_t status = 0U;
    std::uint16_t flags = 0U;
    std::uint32_t message_bytes = 0U;
    std::uint32_t reserved0 = 0U;
    std::uint64_t request_id = 0U;
    // Unpredictable and nonzero on success. Every nonterminal READ rotates
    // this token; terminal and error responses return no successor token.
    std::uint64_t initial_read_token = 0U;
    RealtimeHistoryGenerationInfoV2 generation{};
};
static_assert(sizeof(RealtimeHistoryOpenResponseV2) == 376U);
static_assert(std::is_standard_layout_v<RealtimeHistoryOpenResponseV2>);
static_assert(
    offsetof(
        RealtimeHistoryOpenResponseV2,
        initial_read_token) == 32U);
static_assert(
    offsetof(RealtimeHistoryOpenResponseV2, generation) == 40U);

struct RealtimeHistoryReadRequestV2 final {
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
static_assert(sizeof(RealtimeHistoryReadRequestV2) == 48U);
static_assert(std::is_standard_layout_v<RealtimeHistoryReadRequestV2>);
static_assert(
    offsetof(
        RealtimeHistoryReadRequestV2,
        expected_page_index) == 32U);
static_assert(
    offsetof(RealtimeHistoryReadRequestV2, read_token) == 40U);

// A successful nonterminal response carries exactly one sealed read-only page
// memfd. Explicit EOF is status=kOk, terminal flag set, record_count=0,
// page_mapping_bytes=0, next_read_token=0, and no attached descriptor.
// Every error response has zero flags and zero success-only fields and carries
// no descriptor, so a Reader never interprets stale cursor metadata.
struct RealtimeHistoryReadResponseV2 final {
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
    std::uint64_t next_read_token = 0U;
};
static_assert(sizeof(RealtimeHistoryReadResponseV2) == 64U);
static_assert(std::is_standard_layout_v<RealtimeHistoryReadResponseV2>);
static_assert(
    offsetof(RealtimeHistoryReadResponseV2, record_count) == 20U);
static_assert(
    offsetof(
        RealtimeHistoryReadResponseV2,
        page_mapping_bytes) == 32U);
static_assert(
    offsetof(RealtimeHistoryReadResponseV2, page_index) == 40U);
static_assert(
    offsetof(RealtimeHistoryReadResponseV2, generation) == 48U);
static_assert(
    offsetof(
        RealtimeHistoryReadResponseV2,
        next_read_token) == 56U);

// Descriptors preserve sparse process sequences. payload_index addresses the
// dense array selected by payload_kind and is never a process sequence.
struct RealtimeHistoryRecordDescriptorV2 final {
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
static_assert(sizeof(RealtimeHistoryRecordDescriptorV2) == 40U);
static_assert(
    std::is_standard_layout_v<RealtimeHistoryRecordDescriptorV2>);
static_assert(
    offsetof(
        RealtimeHistoryRecordDescriptorV2,
        projection_flags) == 28U);
static_assert(
    offsetof(
        RealtimeHistoryRecordDescriptorV2,
        payload_kind) == 32U);
static_assert(
    offsetof(RealtimeHistoryRecordDescriptorV2, reserved1) == 36U);

// One nonterminal page has:
//   4096-byte header
//   record_count dense RealtimeHistoryRecordDescriptorV2 rows
//   snapshot_count dense RealtimeWireSnapshotPayloadV2 rows
//   tick_count dense RealtimeWireTickPayloadV2 rows
// Offsets are explicit and all page bytes are immutable before SCM_RIGHTS.
struct alignas(4096) RealtimeHistoryPageHeaderV2 final {
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
    RealtimeHistoryGenerationInfoV2 generation{};
    std::array<std::uint8_t, 3656U> reserved{};
};
static_assert(
    sizeof(RealtimeHistoryPageHeaderV2) ==
    kRealtimeHistoryPageHeaderBytesV2);
static_assert(alignof(RealtimeHistoryPageHeaderV2) == 4096U);
static_assert(std::is_standard_layout_v<RealtimeHistoryPageHeaderV2>);
static_assert(
    offsetof(
        RealtimeHistoryPageHeaderV2,
        record_descriptors_offset) == 48U);
static_assert(
    offsetof(
        RealtimeHistoryPageHeaderV2,
        snapshot_payloads_offset) == 56U);
static_assert(
    offsetof(
        RealtimeHistoryPageHeaderV2,
        tick_payloads_offset) == 72U);
static_assert(
    offsetof(
        RealtimeHistoryPageHeaderV2,
        first_ingress_sequence) == 88U);
static_assert(
    offsetof(RealtimeHistoryPageHeaderV2, generation) == 104U);
static_assert(
    offsetof(RealtimeHistoryPageHeaderV2, reserved) == 440U);

}  // namespace l2flow::ipc
