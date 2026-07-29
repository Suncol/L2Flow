#include "l2flow/ipc/realtime_history_wire_v2.h"
#include "l2flow/ipc/realtime_instrument_tick_delta_wire_v2.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace {

namespace ipc = l2flow::ipc;

[[nodiscard]] constexpr ipc::RealtimeGenerationEndpointV2
MakeEndpoint(bool target) noexcept {
    ipc::RealtimeGenerationEndpointV2 endpoint{};
    endpoint.run_id[0U] = 0xA5U;
    endpoint.session_epoch = 9U;
    endpoint.generation = target ? 3U : 2U;
    endpoint.catalog_generation = target ? 2U : 1U;
    endpoint.data_state_generation = target ? 4U : 2U;
    endpoint.ingress_sequence_exclusive = target ? 11U : 5U;
    endpoint.tick_stream_sequence_exclusive = target ? 8U : 4U;
    endpoint.recv_monotonic_cut_ns = target ? 120U : 80U;
    endpoint.history_published_monotonic_ns =
        target ? 150U : 90U;
    endpoint.accepted_sequence = target ? 10U : 4U;
    endpoint.durable_sequence = target ? 8U : 3U;
    endpoint.applied_sequence = target ? 10U : 4U;
    endpoint.catalog_digest[0U] = target ? 0x22U : 0x11U;
    endpoint.input_identity_sha256[0U] =
        target ? 0x44U : 0x33U;
    endpoint.source_stream_ids = {101U, 102U, 103U, 104U};
    endpoint.source_sequence_exclusive =
        target ? std::array<std::uint64_t, 4U>{3U, 4U, 2U, 5U}
               : std::array<std::uint64_t, 4U>{2U, 3U, 1U, 2U};
    endpoint.trade_date = 20260729U;
    endpoint.capacity = ipc::kRealtimeDefaultInstrumentCapacityV2;
    endpoint.bound_count = target ? 2U : 1U;
    endpoint.available_count = target ? 2U : 1U;
    endpoint.snapshot_available_count = 1U;
    endpoint.tick_available_count = target ? 2U : 1U;
    endpoint.factor_eligible_count = 1U;
    endpoint.catalog_scope = static_cast<std::uint32_t>(
        ipc::RealtimeCatalogScopeV2::kObservedOnly);
    endpoint.coverage_complete = 0U;
    endpoint.flags =
        ipc::kRealtimeGenerationCoverageFromOpenV2 |
        ipc::kRealtimeGenerationRecordCoverageCompleteV2;
    return endpoint;
}

[[nodiscard]] constexpr ipc::RealtimeHistoryGenerationInfoV2
MakeHistoryInfo() noexcept {
    ipc::RealtimeHistoryGenerationInfoV2 info{};
    info.endpoint = MakeEndpoint(true);
    info.instrument_id = 1U;
    info.ordinal = 0U;
    info.instrument_source_record_counts = {1U, 2U, 0U, 1U};
    info.instrument_record_count = 4U;
    info.snapshot_record_count = 1U;
    info.tick_record_count = 3U;
    info.payload_projection = static_cast<std::uint32_t>(
        ipc::RealtimeHistoryPayloadProjectionV2::kCoreV2);
    return info;
}

[[nodiscard]] constexpr
ipc::RealtimeInstrumentTickDeltaCheckpointV2 MakeCheckpoint(
    bool target) noexcept {
    ipc::RealtimeInstrumentTickDeltaCheckpointV2 checkpoint{};
    checkpoint.generation = MakeEndpoint(target);
    checkpoint.instrument_id = 1U;
    checkpoint.ordinal = 0U;
    checkpoint.instrument_tick_source_record_counts =
        target ? std::array<std::uint64_t, 4U>{0U, 2U, 0U, 1U}
               : std::array<std::uint64_t, 4U>{0U, 1U, 0U, 1U};
    checkpoint.instrument_tick_record_count = target ? 3U : 2U;
    checkpoint.payload_projection = static_cast<std::uint32_t>(
        ipc::RealtimeInstrumentTickDeltaPayloadProjectionV2::
            kCoreV2);
    checkpoint.flags =
        ipc::
            kRealtimeInstrumentTickDeltaTickRecordCoverageCompleteV2;
    return checkpoint;
}

[[nodiscard]] constexpr ipc::RealtimeInstrumentTickDeltaMetadataV2
MakeCheckpointDelta() noexcept {
    ipc::RealtimeInstrumentTickDeltaMetadataV2 metadata{};
    metadata.base_kind = static_cast<std::uint32_t>(
        ipc::RealtimeInstrumentTickDeltaBaseKindV2::kCheckpoint);
    metadata.selected_source_mask =
        ipc::kRealtimeInstrumentTickDeltaSourceMaskV2;
    metadata.base_checkpoint = MakeCheckpoint(false);
    metadata.target_checkpoint = MakeCheckpoint(true);
    metadata.delta_tick_source_record_counts = {0U, 1U, 0U, 0U};
    metadata.delta_tick_record_count = 1U;
    metadata.ingress_sequence_begin_inclusive = 5U;
    metadata.ingress_sequence_end_exclusive = 11U;
    metadata.tick_stream_sequence_begin_inclusive = 4U;
    metadata.tick_stream_sequence_end_exclusive = 8U;
    metadata.flags =
        ipc::
            kRealtimeInstrumentTickDeltaTickRecordCoverageCompleteV2;
    metadata.payload_projection = static_cast<std::uint32_t>(
        ipc::RealtimeInstrumentTickDeltaPayloadProjectionV2::
            kCoreV2);
    return metadata;
}

[[nodiscard]] constexpr ipc::RealtimeInstrumentTickDeltaMetadataV2
MakeOriginDelta() noexcept {
    auto metadata = MakeCheckpointDelta();
    metadata.base_kind = static_cast<std::uint32_t>(
        ipc::RealtimeInstrumentTickDeltaBaseKindV2::kOrigin);
    metadata.base_checkpoint = {};
    metadata.delta_tick_source_record_counts = {0U, 2U, 0U, 1U};
    metadata.delta_tick_record_count = 3U;
    metadata.ingress_sequence_begin_inclusive = 1U;
    metadata.tick_stream_sequence_begin_inclusive = 1U;
    return metadata;
}

static_assert(
    sizeof(ipc::RealtimeGenerationEndpointV2) == 256U);
static_assert(
    sizeof(ipc::RealtimeHistoryGenerationInfoV2) == 336U);
static_assert(
    sizeof(ipc::RealtimeHistoryPageHeaderV2) == 4096U);
static_assert(
    sizeof(ipc::RealtimeInstrumentTickDeltaCheckpointV2) == 320U);
static_assert(
    sizeof(ipc::RealtimeInstrumentTickDeltaMetadataV2) == 736U);
static_assert(
    sizeof(ipc::RealtimeInstrumentTickDeltaPageHeaderV2) == 4096U);
static_assert(
    sizeof(ipc::RealtimeWireTickPayloadV2) == 336U);
static_assert(
    sizeof(ipc::RealtimeWireSnapshotPayloadV2) == 3104U);
static_assert(
    sizeof(ipc::RealtimeHistoryRecordDescriptorV2) == 40U);

static_assert(
    static_cast<std::uint16_t>(
        ipc::RealtimeHistoryControlOpcodeV2::kOpenHistory) == 2U);
static_assert(
    static_cast<std::uint16_t>(
        ipc::RealtimeHistoryControlOpcodeV2::kReadHistory) == 3U);
static_assert(
    static_cast<std::uint16_t>(
        ipc::RealtimeInstrumentTickDeltaControlOpcodeV2::
            kOpenDeltaSession) == 4U);
static_assert(
    static_cast<std::uint16_t>(
        ipc::RealtimeInstrumentTickDeltaControlOpcodeV2::
            kOpenInstrumentDelta) == 5U);
static_assert(
    static_cast<std::uint16_t>(
        ipc::RealtimeInstrumentTickDeltaControlOpcodeV2::
            kReadInstrumentDelta) == 6U);

static_assert(
    std::is_trivially_copyable_v<ipc::RealtimeGenerationEndpointV2>);
static_assert(
    std::is_trivially_copyable_v<
        ipc::RealtimeHistoryGenerationInfoV2>);
static_assert(
    std::is_trivially_copyable_v<ipc::RealtimeHistoryPageHeaderV2>);
static_assert(
    std::is_trivially_copyable_v<
        ipc::RealtimeInstrumentTickDeltaCheckpointV2>);
static_assert(
    std::is_trivially_copyable_v<
        ipc::RealtimeInstrumentTickDeltaMetadataV2>);
static_assert(
    std::is_trivially_copyable_v<
        ipc::RealtimeInstrumentTickDeltaPageHeaderV2>);

static_assert(
    ipc::RealtimeGenerationEndpointCanonicalV2(MakeEndpoint(false)));
static_assert(
    ipc::RealtimeGenerationEndpointCanonicalV2(MakeEndpoint(true)));
static_assert(
    ipc::RealtimeHistoryGenerationInfoCanonicalV2(
        MakeHistoryInfo()));
static_assert(
    ipc::RealtimeInstrumentTickDeltaCheckpointCanonicalV2(
        MakeCheckpoint(false)));
static_assert(
    ipc::RealtimeInstrumentTickDeltaCheckpointCanonicalV2(
        MakeCheckpoint(true)));
static_assert(
    ipc::RealtimeInstrumentTickDeltaMetadataCanonicalV2(
        MakeCheckpointDelta()));
static_assert(
    ipc::RealtimeInstrumentTickDeltaMetadataCanonicalV2(
        MakeOriginDelta()));

static_assert([]() constexpr {
    auto endpoint = MakeEndpoint(true);
    endpoint.catalog_scope = 0U;
    return !ipc::RealtimeGenerationEndpointCanonicalV2(endpoint);
}());
static_assert([]() constexpr {
    auto endpoint = MakeEndpoint(true);
    endpoint.coverage_complete = 1U;
    return !ipc::RealtimeGenerationEndpointCanonicalV2(endpoint);
}());
static_assert([]() constexpr {
    auto endpoint = MakeEndpoint(true);
    endpoint.available_count = endpoint.bound_count + 1U;
    return !ipc::RealtimeGenerationEndpointCanonicalV2(endpoint);
}());
static_assert([]() constexpr {
    auto endpoint = MakeEndpoint(true);
    ++endpoint.catalog_generation;
    return !ipc::RealtimeGenerationEndpointCanonicalV2(endpoint);
}());
static_assert([]() constexpr {
    auto endpoint = MakeEndpoint(true);
    --endpoint.applied_sequence;
    return !ipc::RealtimeGenerationEndpointCanonicalV2(endpoint);
}());
static_assert([]() constexpr {
    auto endpoint = MakeEndpoint(true);
    endpoint.durable_sequence = endpoint.accepted_sequence + 1U;
    return !ipc::RealtimeGenerationEndpointCanonicalV2(endpoint);
}());
static_assert([]() constexpr {
    auto endpoint = MakeEndpoint(true);
    endpoint.source_stream_ids[3U] =
        endpoint.source_stream_ids[1U];
    return !ipc::RealtimeGenerationEndpointCanonicalV2(endpoint);
}());
static_assert([]() constexpr {
    auto endpoint = MakeEndpoint(true);
    ++endpoint.tick_stream_sequence_exclusive;
    return !ipc::RealtimeGenerationEndpointCanonicalV2(endpoint);
}());
static_assert([]() constexpr {
    auto endpoint = MakeEndpoint(true);
    endpoint.catalog_digest = {};
    return !ipc::RealtimeGenerationEndpointCanonicalV2(endpoint);
}());
static_assert([]() constexpr {
    auto endpoint = MakeEndpoint(true);
    endpoint.flags =
        ipc::kRealtimeGenerationCoverageFromOpenV2;
    return !ipc::RealtimeGenerationEndpointCanonicalV2(endpoint);
}());
static_assert([]() constexpr {
    auto info = MakeHistoryInfo();
    ++info.ordinal;
    return !ipc::RealtimeHistoryGenerationInfoCanonicalV2(info);
}());
static_assert([]() constexpr {
    auto info = MakeHistoryInfo();
    ++info.tick_record_count;
    return !ipc::RealtimeHistoryGenerationInfoCanonicalV2(info);
}());
static_assert([]() constexpr {
    auto info = MakeHistoryInfo();
    info.reserved[0U] = 1U;
    return !ipc::RealtimeHistoryGenerationInfoCanonicalV2(info);
}());
static_assert([]() constexpr {
    auto checkpoint = MakeCheckpoint(true);
    ++checkpoint.ordinal;
    return !ipc::
        RealtimeInstrumentTickDeltaCheckpointCanonicalV2(checkpoint);
}());
static_assert([]() constexpr {
    auto checkpoint = MakeCheckpoint(true);
    checkpoint.instrument_tick_source_record_counts[0U] = 1U;
    ++checkpoint.instrument_tick_record_count;
    return !ipc::
        RealtimeInstrumentTickDeltaCheckpointCanonicalV2(checkpoint);
}());
static_assert([]() constexpr {
    auto metadata = MakeCheckpointDelta();
    metadata.selected_source_mask = 0U;
    return !ipc::
        RealtimeInstrumentTickDeltaMetadataCanonicalV2(metadata);
}());
static_assert([]() constexpr {
    auto metadata = MakeCheckpointDelta();
    // Dynamic observed-catalog identity is allowed to change.
    metadata.base_checkpoint.generation.catalog_digest[0U] =
        metadata.target_checkpoint.generation.catalog_digest[0U];
    return ipc::RealtimeInstrumentTickDeltaMetadataCanonicalV2(
        metadata);
}());
static_assert([]() constexpr {
    auto metadata = MakeCheckpointDelta();
    metadata.base_checkpoint.generation.catalog_generation =
        metadata.target_checkpoint.generation.catalog_generation;
    metadata.base_checkpoint.generation.bound_count =
        metadata.target_checkpoint.generation.bound_count;
    // The same catalog generation cannot name two catalog identities.
    metadata.base_checkpoint.generation.catalog_digest[0U] ^=
        0xffU;
    return !ipc::
        RealtimeInstrumentTickDeltaMetadataCanonicalV2(metadata);
}());
static_assert([]() constexpr {
    auto metadata = MakeCheckpointDelta();
    metadata.base_checkpoint.generation.flags =
        ipc::kRealtimeGenerationRecordCoverageCompleteV2;
    return !ipc::
        RealtimeInstrumentTickDeltaMetadataCanonicalV2(metadata);
}());
static_assert([]() constexpr {
    auto metadata = MakeCheckpointDelta();
    metadata.base_checkpoint.generation.durable_sequence =
        metadata.target_checkpoint.generation.durable_sequence + 1U;
    return !ipc::
        RealtimeInstrumentTickDeltaMetadataCanonicalV2(metadata);
}());
static_assert([]() constexpr {
    auto metadata = MakeOriginDelta();
    metadata.base_checkpoint.instrument_id = 1U;
    return !ipc::
        RealtimeInstrumentTickDeltaMetadataCanonicalV2(metadata);
}());
static_assert([]() constexpr {
    auto metadata = MakeCheckpointDelta();
    ++metadata.delta_tick_source_record_counts[1U];
    ++metadata.delta_tick_record_count;
    return !ipc::
        RealtimeInstrumentTickDeltaMetadataCanonicalV2(metadata);
}());

[[nodiscard]] bool ExactMagic(
    const std::array<std::uint8_t, 8U>& actual,
    const std::array<std::uint8_t, 8U>& expected) noexcept {
    return actual == expected && actual.back() == 0U;
}

}  // namespace

int main() {
    const ipc::RealtimeGenerationEndpointV2 endpoint{};
    const ipc::RealtimeHistoryGenerationInfoV2 history{};
    const ipc::RealtimeInstrumentTickDeltaCheckpointV2 checkpoint{};
    const ipc::RealtimeInstrumentTickDeltaMetadataV2 metadata{};
    const ipc::RealtimeHistoryReadResponseV2 history_eof{};
    const ipc::RealtimeInstrumentTickDeltaReadResponseV2 delta_eof{};

    const bool zero_defaults =
        std::all_of(
            endpoint.run_id.begin(),
            endpoint.run_id.end(),
            [](std::uint8_t value) noexcept { return value == 0U; }) &&
        history.instrument_record_count == 0U &&
        checkpoint.instrument_tick_record_count == 0U &&
        metadata.delta_tick_record_count == 0U &&
        history_eof.record_count == 0U &&
        history_eof.page_mapping_bytes == 0U &&
        history_eof.next_read_token == 0U &&
        delta_eof.record_count == 0U &&
        delta_eof.page_mapping_bytes == 0U &&
        delta_eof.next_read_token == 0U;
    const bool magics =
        ExactMagic(
            ipc::kRealtimeHistoryPageMagicV2,
            std::array<std::uint8_t, 8U>{
                'L', '2', 'F', 'H', 'S', 'T', '2', '\0'}) &&
        ExactMagic(
            ipc::kRealtimeInstrumentTickDeltaPageMagicV2,
            std::array<std::uint8_t, 8U>{
                'L', '2', 'F', 'I', 'D', 'T', '2', '\0'});
    const bool source_mask =
        ipc::kRealtimeInstrumentTickDeltaSourceMaskV2 ==
        ((1U << 1U) | (1U << 3U));
    auto zero_clock_endpoint = MakeEndpoint(true);
    zero_clock_endpoint.recv_monotonic_cut_ns = 0U;
    zero_clock_endpoint.history_published_monotonic_ns = 0U;
    const bool zero_clocks_rejected =
        !ipc::RealtimeGenerationEndpointCanonicalV2(
            zero_clock_endpoint);
    return zero_defaults && magics && source_mask &&
                   zero_clocks_rejected
               ? 0
               : 1;
}
