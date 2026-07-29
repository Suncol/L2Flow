#pragma once

#include "l2flow/ipc/realtime_history_wire_v2.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace l2flow::ipc {

// One stateful SOCK_SEQPACKET connection first pins one immutable target
// generation, then opens and exhausts at most one instrument cursor at a
// time. Every nonterminal READ returns one independently sealed page memfd.
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
    // The canonical retained-session origin has all exclusive sequences set
    // to one and instrument-local counts set to zero. Its wire checkpoint is
    // all-zero; it is not a fabricated generation zero.
    kOrigin = 1U,
    kCheckpoint = 2U,
};

enum class RealtimeInstrumentTickDeltaPayloadProjectionV2 :
    std::uint32_t {
    kCoreV2 = 1U,
};

enum RealtimeInstrumentTickDeltaFlagV2 : std::uint32_t {
    // Complete accepted-process coverage for selected source slots 1 and 3.
    // This is not a snapshot or authoritative upstream-feed claim.
    kRealtimeInstrumentTickDeltaTickRecordCoverageCompleteV2 =
        1U << 0U,
};

enum RealtimeInstrumentTickDeltaResponseFlagV2 : std::uint16_t {
    kRealtimeInstrumentTickDeltaResponseTerminalV2 = 1U << 0U,
};

// Instrument-local rolling endpoint at one exact global generation. Slots 0
// and 2 of instrument_tick_source_record_counts are always zero; their checked
// sum is instrument_tick_record_count.
struct RealtimeInstrumentTickDeltaCheckpointV2 final {
    RealtimeGenerationEndpointV2 generation{};
    std::uint32_t instrument_id = 0U;
    std::uint32_t ordinal = 0U;
    std::array<std::uint64_t, 4U>
        instrument_tick_source_record_counts{};
    std::uint64_t instrument_tick_record_count = 0U;
    std::uint32_t payload_projection = 0U;
    std::uint32_t flags = 0U;
    std::array<std::uint8_t, 8U> reserved{};
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
        ordinal) == 260U);
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
        payload_projection) == 304U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaCheckpointV2,
        flags) == 308U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaCheckpointV2,
        reserved) == 312U);

[[nodiscard]] constexpr bool
RealtimeInstrumentTickDeltaCheckpointCanonicalV2(
    const RealtimeInstrumentTickDeltaCheckpointV2&
        checkpoint) noexcept {
    if (!RealtimeGenerationEndpointCanonicalV2(
        checkpoint.generation) ||
        checkpoint.instrument_id == 0U ||
        checkpoint.ordinal != checkpoint.instrument_id - 1U ||
        checkpoint.ordinal >= checkpoint.generation.capacity ||
        checkpoint.ordinal >= checkpoint.generation.bound_count ||
        checkpoint.instrument_tick_source_record_counts[0U] != 0U ||
        checkpoint.instrument_tick_source_record_counts[2U] != 0U ||
        checkpoint.payload_projection !=
            static_cast<std::uint32_t>(
                RealtimeInstrumentTickDeltaPayloadProjectionV2::
                    kCoreV2) ||
        checkpoint.flags !=
            kRealtimeInstrumentTickDeltaTickRecordCoverageCompleteV2 ||
        !realtime_history_wire_v2_detail::AllZero(
            checkpoint.reserved)) {
        return false;
    }

    std::uint64_t total = 0U;
    for (std::size_t source = 0U;
         source <
         checkpoint.instrument_tick_source_record_counts.size();
         ++source) {
        const std::uint64_t count =
            checkpoint
                .instrument_tick_source_record_counts[source];
        if (count >
                checkpoint.generation
                        .source_sequence_exclusive[source] -
                    1U ||
            !realtime_history_wire_v2_detail::CheckedAdd(
                total, count, total)) {
            return false;
        }
    }
    return total == checkpoint.instrument_tick_record_count &&
           total <=
               checkpoint.generation.tick_stream_sequence_exclusive -
                   1U;
}

// Redundant endpoints, local counts, and half-open global bounds are
// intentional. EOF reconciliation proves the finite delta without deriving a
// frontier from its last sparse output row.
struct RealtimeInstrumentTickDeltaMetadataV2 final {
    std::uint32_t base_kind = 0U;
    std::uint32_t selected_source_mask = 0U;
    // All-zero for kOrigin; complete and nonzero for kCheckpoint.
    RealtimeInstrumentTickDeltaCheckpointV2 base_checkpoint{};
    RealtimeInstrumentTickDeltaCheckpointV2 target_checkpoint{};
    std::array<std::uint64_t, 4U>
        delta_tick_source_record_counts{};
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
        ingress_sequence_end_exclusive) == 696U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaMetadataV2,
        tick_stream_sequence_begin_inclusive) == 704U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaMetadataV2,
        tick_stream_sequence_end_exclusive) == 712U);
static_assert(
    offsetof(RealtimeInstrumentTickDeltaMetadataV2, flags) == 720U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaMetadataV2,
        payload_projection) == 724U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaMetadataV2,
        reserved) == 728U);

namespace realtime_instrument_tick_delta_wire_v2_detail {

[[nodiscard]] constexpr bool EndpointAllZero(
    const RealtimeGenerationEndpointV2& endpoint) noexcept {
    return realtime_history_wire_v2_detail::AllZero(endpoint.run_id) &&
           endpoint.session_epoch == 0U &&
           endpoint.generation == 0U &&
           endpoint.catalog_generation == 0U &&
           endpoint.data_state_generation == 0U &&
           endpoint.ingress_sequence_exclusive == 0U &&
           endpoint.tick_stream_sequence_exclusive == 0U &&
           endpoint.recv_monotonic_cut_ns == 0U &&
           endpoint.history_published_monotonic_ns == 0U &&
           endpoint.accepted_sequence == 0U &&
           endpoint.durable_sequence == 0U &&
           endpoint.applied_sequence == 0U &&
           realtime_history_wire_v2_detail::AllZero(
               endpoint.catalog_digest) &&
           realtime_history_wire_v2_detail::AllZero(
               endpoint.input_identity_sha256) &&
           realtime_history_wire_v2_detail::AllZero(
               endpoint.source_stream_ids) &&
           realtime_history_wire_v2_detail::AllZero(
               endpoint.source_sequence_exclusive) &&
           endpoint.trade_date == 0U && endpoint.capacity == 0U &&
           endpoint.bound_count == 0U &&
           endpoint.available_count == 0U &&
           endpoint.snapshot_available_count == 0U &&
           endpoint.tick_available_count == 0U &&
           endpoint.factor_eligible_count == 0U &&
           endpoint.catalog_scope == 0U &&
           endpoint.coverage_complete == 0U && endpoint.flags == 0U;
}

[[nodiscard]] constexpr bool CheckpointAllZero(
    const RealtimeInstrumentTickDeltaCheckpointV2&
        checkpoint) noexcept {
    return EndpointAllZero(checkpoint.generation) &&
           checkpoint.instrument_id == 0U &&
           checkpoint.ordinal == 0U &&
           realtime_history_wire_v2_detail::AllZero(
               checkpoint.instrument_tick_source_record_counts) &&
           checkpoint.instrument_tick_record_count == 0U &&
           checkpoint.payload_projection == 0U &&
           checkpoint.flags == 0U &&
           realtime_history_wire_v2_detail::AllZero(
               checkpoint.reserved);
}

[[nodiscard]] constexpr bool EndpointsEqual(
    const RealtimeGenerationEndpointV2& left,
    const RealtimeGenerationEndpointV2& right) noexcept {
    return left.run_id == right.run_id &&
           left.session_epoch == right.session_epoch &&
           left.generation == right.generation &&
           left.catalog_generation == right.catalog_generation &&
           left.data_state_generation == right.data_state_generation &&
           left.ingress_sequence_exclusive ==
               right.ingress_sequence_exclusive &&
           left.tick_stream_sequence_exclusive ==
               right.tick_stream_sequence_exclusive &&
           left.recv_monotonic_cut_ns ==
               right.recv_monotonic_cut_ns &&
           left.history_published_monotonic_ns ==
               right.history_published_monotonic_ns &&
           left.accepted_sequence == right.accepted_sequence &&
           left.durable_sequence == right.durable_sequence &&
           left.applied_sequence == right.applied_sequence &&
           left.catalog_digest == right.catalog_digest &&
           left.input_identity_sha256 ==
               right.input_identity_sha256 &&
           left.source_stream_ids == right.source_stream_ids &&
           left.source_sequence_exclusive ==
               right.source_sequence_exclusive &&
           left.trade_date == right.trade_date &&
           left.capacity == right.capacity &&
           left.bound_count == right.bound_count &&
           left.available_count == right.available_count &&
           left.snapshot_available_count ==
               right.snapshot_available_count &&
           left.tick_available_count ==
               right.tick_available_count &&
           left.factor_eligible_count ==
               right.factor_eligible_count &&
           left.catalog_scope == right.catalog_scope &&
           left.coverage_complete == right.coverage_complete &&
           left.flags == right.flags;
}

[[nodiscard]] constexpr bool CheckpointsEqual(
    const RealtimeInstrumentTickDeltaCheckpointV2& left,
    const RealtimeInstrumentTickDeltaCheckpointV2& right) noexcept {
    return EndpointsEqual(left.generation, right.generation) &&
           left.instrument_id == right.instrument_id &&
           left.ordinal == right.ordinal &&
           left.instrument_tick_source_record_counts ==
               right.instrument_tick_source_record_counts &&
           left.instrument_tick_record_count ==
               right.instrument_tick_record_count &&
           left.payload_projection == right.payload_projection &&
           left.flags == right.flags &&
           left.reserved == right.reserved;
}

// A predecessor may have an earlier observed catalog. Catalog digests may
// differ only when catalog_generation advances, while accepted-cut digests
// need not match. Session/layout identity and source identities are stable,
// while catalog/data watermarks, durable and applied frontiers, source
// frontiers, and monotonic observed counts cannot move backwards.
// factor_eligible_count is intentionally excluded because it is a live
// eligibility count and is not monotonic.
[[nodiscard]] constexpr bool EndpointPrecedes(
    const RealtimeGenerationEndpointV2& base,
    const RealtimeGenerationEndpointV2& target) noexcept {
    if (base.run_id != target.run_id ||
        base.session_epoch != target.session_epoch ||
        base.trade_date != target.trade_date ||
        base.capacity != target.capacity ||
        base.source_stream_ids != target.source_stream_ids ||
        base.flags != target.flags ||
        target.generation < base.generation ||
        target.catalog_generation < base.catalog_generation ||
        target.data_state_generation < base.data_state_generation ||
        target.ingress_sequence_exclusive <
            base.ingress_sequence_exclusive ||
        target.tick_stream_sequence_exclusive <
            base.tick_stream_sequence_exclusive ||
        (target.catalog_generation == base.catalog_generation &&
         target.catalog_digest != base.catalog_digest) ||
        target.recv_monotonic_cut_ns <
            base.recv_monotonic_cut_ns ||
        target.history_published_monotonic_ns <
            base.history_published_monotonic_ns ||
        target.accepted_sequence < base.accepted_sequence ||
        target.durable_sequence < base.durable_sequence ||
        target.applied_sequence < base.applied_sequence ||
        target.bound_count < base.bound_count ||
        target.available_count < base.available_count ||
        target.snapshot_available_count <
            base.snapshot_available_count ||
        target.tick_available_count < base.tick_available_count) {
        return false;
    }
    for (std::size_t source = 0U;
         source < base.source_sequence_exclusive.size();
         ++source) {
        if (target.source_sequence_exclusive[source] <
            base.source_sequence_exclusive[source]) {
            return false;
        }
    }
    return true;
}

}  // namespace realtime_instrument_tick_delta_wire_v2_detail

[[nodiscard]] constexpr bool
RealtimeInstrumentTickDeltaMetadataCanonicalV2(
    const RealtimeInstrumentTickDeltaMetadataV2&
        metadata) noexcept {
    const bool origin =
        metadata.base_kind ==
        static_cast<std::uint32_t>(
            RealtimeInstrumentTickDeltaBaseKindV2::kOrigin);
    const bool checkpoint_base =
        metadata.base_kind ==
        static_cast<std::uint32_t>(
            RealtimeInstrumentTickDeltaBaseKindV2::kCheckpoint);
    if ((!origin && !checkpoint_base) ||
        metadata.selected_source_mask !=
            kRealtimeInstrumentTickDeltaSourceMaskV2 ||
        !RealtimeInstrumentTickDeltaCheckpointCanonicalV2(
            metadata.target_checkpoint) ||
        metadata.delta_tick_source_record_counts[0U] != 0U ||
        metadata.delta_tick_source_record_counts[2U] != 0U ||
        metadata.flags !=
            kRealtimeInstrumentTickDeltaTickRecordCoverageCompleteV2 ||
        metadata.flags != metadata.target_checkpoint.flags ||
        metadata.payload_projection !=
            static_cast<std::uint32_t>(
                RealtimeInstrumentTickDeltaPayloadProjectionV2::
                    kCoreV2) ||
        metadata.payload_projection !=
            metadata.target_checkpoint.payload_projection ||
        metadata.ingress_sequence_end_exclusive !=
            metadata.target_checkpoint.generation
                .ingress_sequence_exclusive ||
        metadata.tick_stream_sequence_end_exclusive !=
            metadata.target_checkpoint.generation
                .tick_stream_sequence_exclusive ||
        !realtime_history_wire_v2_detail::AllZero(
            metadata.reserved)) {
        return false;
    }

    std::array<std::uint64_t, 4U> base_counts{};
    std::array<std::uint64_t, 4U> base_source_endpoints{
        1U, 1U, 1U, 1U};
    if (origin) {
        if (!realtime_instrument_tick_delta_wire_v2_detail::
                CheckpointAllZero(metadata.base_checkpoint) ||
            metadata.ingress_sequence_begin_inclusive != 1U ||
            metadata.tick_stream_sequence_begin_inclusive != 1U) {
            return false;
        }
    } else {
        const auto& base = metadata.base_checkpoint;
        const auto& target = metadata.target_checkpoint;
        if (!RealtimeInstrumentTickDeltaCheckpointCanonicalV2(base) ||
            base.instrument_id != target.instrument_id ||
            base.ordinal != target.ordinal ||
            !realtime_instrument_tick_delta_wire_v2_detail::
                EndpointPrecedes(base.generation, target.generation) ||
            metadata.ingress_sequence_begin_inclusive !=
                base.generation.ingress_sequence_exclusive ||
            metadata.tick_stream_sequence_begin_inclusive !=
                base.generation.tick_stream_sequence_exclusive ||
            (base.generation.generation ==
                 target.generation.generation &&
             !realtime_instrument_tick_delta_wire_v2_detail::
                 CheckpointsEqual(base, target))) {
            return false;
        }
        for (std::size_t source = 0U;
             source < base_counts.size();
             ++source) {
            base_counts[source] =
                base.instrument_tick_source_record_counts[source];
            base_source_endpoints[source] =
                base.generation.source_sequence_exclusive[source];
        }
    }

    std::uint64_t delta_total = 0U;
    for (std::size_t source = 0U;
         source <
         metadata.delta_tick_source_record_counts.size();
         ++source) {
        const std::uint64_t target_count =
            metadata.target_checkpoint
                .instrument_tick_source_record_counts[source];
        const std::uint64_t target_source =
            metadata.target_checkpoint.generation
                .source_sequence_exclusive[source];
        if (target_count < base_counts[source] ||
            target_source < base_source_endpoints[source]) {
            return false;
        }
        const std::uint64_t local_delta =
            target_count - base_counts[source];
        if (metadata.delta_tick_source_record_counts[source] !=
                local_delta ||
            local_delta >
                target_source - base_source_endpoints[source] ||
            !realtime_history_wire_v2_detail::CheckedAdd(
                delta_total, local_delta, delta_total)) {
            return false;
        }
    }
    if (delta_total != metadata.delta_tick_record_count ||
        metadata.ingress_sequence_begin_inclusive >
            metadata.ingress_sequence_end_exclusive ||
        metadata.tick_stream_sequence_begin_inclusive >
            metadata.tick_stream_sequence_end_exclusive) {
        return false;
    }
    const std::uint64_t ingress_delta =
        metadata.ingress_sequence_end_exclusive -
        metadata.ingress_sequence_begin_inclusive;
    const std::uint64_t tick_delta =
        metadata.tick_stream_sequence_end_exclusive -
        metadata.tick_stream_sequence_begin_inclusive;
    return delta_total <= tick_delta && tick_delta <= ingress_delta;
}

struct RealtimeInstrumentTickDeltaOpenSessionRequestV2 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t protocol_major = 0U;
    std::uint16_t protocol_minor = 0U;
    std::uint16_t opcode = 0U;
    std::uint16_t reserved0 = 0U;
    std::uint32_t message_bytes = 0U;
    std::uint32_t flags = 0U;
    std::uint64_t request_id = 0U;
    std::uint64_t expected_generation = 0U;
};
static_assert(
    sizeof(RealtimeInstrumentTickDeltaOpenSessionRequestV2) == 40U);
static_assert(
    std::is_standard_layout_v<
        RealtimeInstrumentTickDeltaOpenSessionRequestV2>);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaOpenSessionRequestV2,
        expected_generation) == 32U);

struct RealtimeInstrumentTickDeltaOpenSessionResponseV2 final {
    std::array<std::uint8_t, 8U> magic{};
    std::uint16_t protocol_major = 0U;
    std::uint16_t protocol_minor = 0U;
    std::uint16_t status = 0U;
    std::uint16_t flags = 0U;
    std::uint32_t message_bytes = 0U;
    std::uint32_t reserved0 = 0U;
    std::uint64_t request_id = 0U;
    RealtimeGenerationEndpointV2 target_generation{};
    // Nonzero unpredictable value required by every instrument OPEN on this
    // connection. It is independent of each cursor's rotating read token.
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
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaOpenInstrumentRequestV2,
        reserved2) == 376U);

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
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaReadRequestV2,
        expected_page_index) == 32U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaReadRequestV2,
        read_token) == 40U);

// Error responses carry no descriptor and keep flags plus every success-only
// field zero. Successful terminal responses carry the expected page and target
// generation, but zero records, mapping bytes, and successor token.
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
        page_index) == 40U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaReadResponseV2,
        target_generation) == 48U);
static_assert(
    offsetof(
        RealtimeInstrumentTickDeltaReadResponseV2,
        next_read_token) == 56U);

// Every nonterminal page is one dense oldest-first
// RealtimeWireTickPayloadV2 array. Explicit EOF is a terminal zero-record
// response with no page descriptor.
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
