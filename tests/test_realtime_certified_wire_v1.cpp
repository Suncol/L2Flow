#include "l2flow/ipc/realtime_certified_wire_v1.h"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string_view>
#include <type_traits>

namespace {

namespace ipc = l2flow::ipc;

[[nodiscard]] constexpr ipc::RealtimeCertifiedHeaderV1
MakeHeader() noexcept {
    ipc::RealtimeCertifiedHeaderV1 header{};
    header.magic = ipc::kRealtimeCertifiedShmMagicV1;
    header.abi_major = ipc::kRealtimeCertifiedWireMajorV1;
    header.abi_minor = ipc::kRealtimeCertifiedWireMinorV1;
    header.header_bytes = ipc::kRealtimeCertifiedHeaderBytesV1;
    header.endian_marker =
        ipc::kRealtimeCertifiedLittleEndianMarkerV1;
    header.run_id[0U] = 0xA5U;
    header.session_epoch = 7U;
    header.trade_date = 20260730U;

    header.latest_capacity = 4U;
    header.certified_ring_capacity = 8U;
    header.channel_state_capacity = 4U;
    header.latest_offset = ipc::kRealtimeCertifiedHeaderBytesV1;
    header.certified_ring_offset =
        header.latest_offset +
        header.latest_capacity *
            ipc::kRealtimeCertifiedTickSlotBytesV1;
    header.channel_state_offset =
        header.certified_ring_offset +
        header.certified_ring_capacity *
            ipc::kRealtimeCertifiedTickSlotBytesV1;
    header.total_mapping_bytes =
        header.channel_state_offset +
        header.channel_state_capacity *
            ipc::kRealtimeCertifiedChannelStateBytesV1;

    header.status_publish_tag = 2U;
    header.heartbeat_monotonic_ns = 200U;
    header.canonical_apply_frontier = 3U;
    header.correction_epoch = 1U;
    header.observed_native_message_count = 4U;
    header.certified_tick_count = 3U;
    header.exact_duplicate_message_count = 1U;
    header.gap_opened_count = 1U;
    header.gap_recovered_count = 1U;
    header.aggregate_state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kContiguous);
    header.channel_state_count = 1U;
    return header;
}

[[nodiscard]] constexpr ipc::RealtimeCertifiedChannelStateV1
MakeChannelState() noexcept {
    ipc::RealtimeCertifiedChannelStateV1 row{};
    row.publish_tag = 2U;
    row.feed_epoch = 1U;
    row.origin_sequence = 10;
    row.observed_contiguous_frontier = 12;
    row.certified_published_frontier = 12;
    row.highest_observed_sequence = 12;
    row.canonical_apply_frontier = 3U;
    row.observed_native_message_count = 3U;
    row.certified_tick_count = 3U;
    row.state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kContiguous);
    row.trade_date = 20260730U;
    row.channel = 1;
    row.market = 1U;
    row.channel_correction_epoch = 1U;
    return row;
}

[[nodiscard]] constexpr ipc::RealtimeWireTickPayloadV2
MakeShanghaiPayload() noexcept {
    ipc::RealtimeWireTickPayloadV2 payload{};
    payload.common.record_schema_version = 2U;
    payload.common.record_bytes = sizeof(payload);
    payload.common.instrument_id = 1U;
    payload.common.ordinal = 0U;
    payload.common.source_sequence = 5U;
    payload.common.ingress_sequence = 10U;
    payload.common.tick_stream_sequence = 7U;
    payload.common.source_stream_id = 101U;
    payload.common.trade_date = 20260730U;
    payload.common.source_slot = 1U;
    payload.common.event_kind = 2U;
    payload.common.market = 1U;
    payload.channel = 1;
    payload.native_event_sequence = 12;
    return payload;
}

[[nodiscard]] constexpr ipc::RealtimeCertifiedTickEnvelopeV1
MakeEnvelope() noexcept {
    ipc::RealtimeCertifiedTickEnvelopeV1 envelope{};
    envelope.canonical_apply_sequence = 3U;
    envelope.correction_epoch = 1U;
    envelope.feed_epoch = 1U;
    envelope.certified_monotonic_ns = 100U;
    envelope.payload = MakeShanghaiPayload();
    return envelope;
}

[[nodiscard]] constexpr ipc::RealtimeCertifiedTickSlotV1
MakeTickSlot() noexcept {
    ipc::RealtimeCertifiedTickSlotV1 slot{};
    slot.publish_tag = 2U;
    const auto words = std::bit_cast<
        std::array<
            std::uint64_t,
            ipc::kRealtimeCertifiedTickEnvelopeWordsV1>>(
        MakeEnvelope());
    for (std::size_t index = 0U; index < words.size(); ++index) {
        slot.payload_words[index] = words[index];
    }
    return slot;
}

static_assert(
    ipc::kRealtimeCertifiedShmMagicV1 ==
    std::array<std::uint8_t, 8U>{
        'L', '2', 'F', 'C', 'E', 'R', 'T', '1'});
static_assert(ipc::kRealtimeCertifiedWireMajorV1 == 1U);
static_assert(ipc::kRealtimeCertifiedWireMinorV1 == 1U);
static_assert(
    ipc::kRealtimeCertifiedLittleEndianMarkerV1 ==
    0x01020304U);

static_assert(sizeof(ipc::RealtimeCertifiedHeaderV1) == 4096U);
static_assert(alignof(ipc::RealtimeCertifiedHeaderV1) == 4096U);
static_assert(
    sizeof(ipc::RealtimeCertifiedChannelStateV1) == 128U);
static_assert(
    alignof(ipc::RealtimeCertifiedChannelStateV1) == 64U);
static_assert(
    sizeof(ipc::RealtimeCertifiedTickEnvelopeV1) == 368U);
static_assert(sizeof(ipc::RealtimeCertifiedTickSlotV1) == 512U);
static_assert(alignof(ipc::RealtimeCertifiedTickSlotV1) == 64U);
static_assert(sizeof(ipc::RealtimeWireTickPayloadV2) == 336U);

static_assert(
    std::is_standard_layout_v<ipc::RealtimeCertifiedHeaderV1>);
static_assert(
    std::is_trivially_copyable_v<
        ipc::RealtimeCertifiedHeaderV1>);
static_assert(
    std::is_standard_layout_v<
        ipc::RealtimeCertifiedChannelStateV1>);
static_assert(
    std::is_trivially_copyable_v<
        ipc::RealtimeCertifiedChannelStateV1>);
static_assert(
    std::is_standard_layout_v<
        ipc::RealtimeCertifiedTickEnvelopeV1>);
static_assert(
    std::is_trivially_copyable_v<
        ipc::RealtimeCertifiedTickEnvelopeV1>);
static_assert(
    std::is_standard_layout_v<
        ipc::RealtimeCertifiedTickSlotV1>);
static_assert(
    std::is_trivially_copyable_v<
        ipc::RealtimeCertifiedTickSlotV1>);

static_assert(
    offsetof(ipc::RealtimeCertifiedHeaderV1, latest_offset) ==
    64U);
static_assert(
    offsetof(
        ipc::RealtimeCertifiedHeaderV1,
        status_publish_tag) == 128U);
static_assert(
    offsetof(
        ipc::RealtimeCertifiedHeaderV1,
        canonical_apply_frontier) == 144U);
static_assert(
    offsetof(
        ipc::RealtimeCertifiedHeaderV1,
        aggregate_state) == 224U);
static_assert(
    offsetof(
        ipc::RealtimeCertifiedChannelStateV1,
        origin_sequence) == 16U);
static_assert(
    offsetof(
        ipc::RealtimeCertifiedChannelStateV1,
        canonical_apply_frontier) == 48U);
static_assert(
    offsetof(
        ipc::RealtimeCertifiedChannelStateV1,
        channel) == 112U);
static_assert(
    offsetof(
        ipc::RealtimeCertifiedChannelStateV1,
        channel_correction_epoch) == 120U);
static_assert(
    offsetof(ipc::RealtimeCertifiedTickEnvelopeV1, payload) ==
    32U);
static_assert(
    offsetof(ipc::RealtimeCertifiedTickSlotV1, payload_words) ==
    64U);

static_assert(
    static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kDisabled) == 1U);
static_assert(
    static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kNoData) == 2U);
static_assert(
    static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kContiguous) == 3U);
static_assert(
    static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kGapOpen) == 4U);
static_assert(
    static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kCatchingUp) == 5U);
static_assert(
    static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kFrozenConflict) == 6U);
static_assert(
    static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kFrozenResource) == 7U);
static_assert(
    static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kStopped) == 8U);
static_assert(
    static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kDegraded) == 9U);

static_assert(ipc::RealtimeCertifiedHeaderCanonicalV1(MakeHeader()));
static_assert(
    ipc::RealtimeCertifiedChannelStateCanonicalV1(
        MakeChannelState()));
static_assert(
    ipc::RealtimeCertifiedTickPayloadCanonicalV1(
        MakeShanghaiPayload()));
static_assert(
    ipc::RealtimeCertifiedTickEnvelopeCanonicalV1(
        MakeEnvelope()));
static_assert(
    ipc::RealtimeCertifiedTickSlotPublishedV1(MakeTickSlot()));
static_assert(
    ipc::RealtimeCertifiedTickMatchesHeaderV1(
        MakeEnvelope(), MakeHeader()));
static_assert(
    ipc::RealtimeCertifiedTickMatchesChannelStateV1(
        MakeEnvelope(), MakeChannelState()));
static_assert([]() constexpr {
    auto row = MakeChannelState();
    row.market = 2U;
    row.channel = 0;
    return ipc::RealtimeCertifiedChannelStateCanonicalV1(row);
}());
static_assert([]() constexpr {
    auto payload = MakeShanghaiPayload();
    payload.common.source_slot = 3U;
    payload.common.event_kind = 4U;
    payload.common.market = 2U;
    payload.channel = 0;
    return ipc::RealtimeCertifiedTickPayloadCanonicalV1(payload);
}());

static_assert([]() constexpr {
    auto header = MakeHeader();
    header.magic[0U] = 0U;
    return !ipc::RealtimeCertifiedHeaderCanonicalV1(header);
}());
static_assert([]() constexpr {
    auto header = MakeHeader();
    ++header.abi_minor;
    return !ipc::RealtimeCertifiedHeaderCanonicalV1(header);
}());
static_assert([]() constexpr {
    auto header = MakeHeader();
    header.endian_marker = 0U;
    return !ipc::RealtimeCertifiedHeaderCanonicalV1(header);
}());
static_assert([]() constexpr {
    auto header = MakeHeader();
    ++header.certified_ring_offset;
    return !ipc::RealtimeCertifiedHeaderCanonicalV1(header);
}());
static_assert([]() constexpr {
    auto header = MakeHeader();
    header.latest_capacity = 0U;
    return !ipc::RealtimeCertifiedHeaderCanonicalV1(header);
}());
static_assert([]() constexpr {
    auto header = MakeHeader();
    header.status_publish_tag = 3U;
    return !ipc::RealtimeCertifiedHeaderCanonicalV1(header);
}());
static_assert([]() constexpr {
    auto header = MakeHeader();
    header.certified_tick_count = 2U;
    return !ipc::RealtimeCertifiedHeaderCanonicalV1(header);
}());
static_assert([]() constexpr {
    auto header = MakeHeader();
    header.gap_recovered_count =
        header.gap_opened_count + 1U;
    return !ipc::RealtimeCertifiedHeaderCanonicalV1(header);
}());
static_assert([]() constexpr {
    auto header = MakeHeader();
    header.pending_token_count = 1U;
    return !ipc::RealtimeCertifiedHeaderCanonicalV1(header);
}());
static_assert([]() constexpr {
    auto header = MakeHeader();
    header.flags = 1U;
    return !ipc::RealtimeCertifiedHeaderCanonicalV1(header);
}());
static_assert([]() constexpr {
    auto header = MakeHeader();
    header.aggregate_state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kDegraded);
    header.frozen_channel_count = 1U;
    return ipc::RealtimeCertifiedHeaderCanonicalV1(header);
}());
static_assert([]() constexpr {
    auto header = MakeHeader();
    header.reserved_identity = 1U;
    return !ipc::RealtimeCertifiedHeaderCanonicalV1(header);
}());
static_assert([]() constexpr {
    auto header = MakeHeader();
    header.reserved_layout0 = 1U;
    return !ipc::RealtimeCertifiedHeaderCanonicalV1(header);
}());
static_assert([]() constexpr {
    auto header = MakeHeader();
    header.reserved_layout[0U] = 1U;
    return !ipc::RealtimeCertifiedHeaderCanonicalV1(header);
}());
static_assert([]() constexpr {
    auto header = MakeHeader();
    header.reserved_status = 1U;
    return !ipc::RealtimeCertifiedHeaderCanonicalV1(header);
}());
static_assert([]() constexpr {
    auto header = MakeHeader();
    header.reserved[0U] = 1U;
    return !ipc::RealtimeCertifiedHeaderCanonicalV1(header);
}());

static_assert([]() constexpr {
    auto row = MakeChannelState();
    row.observed_contiguous_frontier =
        row.origin_sequence - 2;
    return !ipc::RealtimeCertifiedChannelStateCanonicalV1(row);
}());
static_assert([]() constexpr {
    auto row = MakeChannelState();
    row.certified_published_frontier =
        row.observed_contiguous_frontier - 1;
    return !ipc::RealtimeCertifiedChannelStateCanonicalV1(row);
}());
static_assert([]() constexpr {
    auto row = MakeChannelState();
    row.canonical_apply_frontier =
        row.certified_tick_count - 1U;
    return !ipc::RealtimeCertifiedChannelStateCanonicalV1(row);
}());
static_assert([]() constexpr {
    auto row = MakeChannelState();
    row.market = 0U;
    return !ipc::RealtimeCertifiedChannelStateCanonicalV1(row);
}());
static_assert([]() constexpr {
    auto row = MakeChannelState();
    row.reserved[0U] = 1U;
    return !ipc::RealtimeCertifiedChannelStateCanonicalV1(row);
}());
static_assert([]() constexpr {
    auto row = MakeChannelState();
    row.origin_sequence = 0;
    row.observed_contiguous_frontier = 0;
    row.certified_published_frontier = 0;
    row.highest_observed_sequence = 99;
    row.canonical_apply_frontier = 0U;
    row.observed_native_message_count = 2U;
    row.certified_tick_count = 0U;
    row.pending_token_count = 2U;
    row.state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kGapOpen);
    return ipc::RealtimeCertifiedChannelStateCanonicalV1(row);
}());
static_assert([]() constexpr {
    auto row = MakeChannelState();
    row.origin_sequence = 0;
    row.observed_contiguous_frontier = 0;
    row.certified_published_frontier = 0;
    row.highest_observed_sequence = 100;
    row.canonical_apply_frontier = 0U;
    row.observed_native_message_count = 1U;
    row.certified_tick_count = 0U;
    row.pending_token_count = 1U;
    row.state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kFrozenResource);
    return ipc::RealtimeCertifiedChannelStateCanonicalV1(row);
}());
static_assert([]() constexpr {
    auto row = MakeChannelState();
    row.origin_sequence = 0;
    row.observed_contiguous_frontier = 0;
    row.certified_published_frontier = 0;
    row.highest_observed_sequence = 0;
    row.canonical_apply_frontier = 0U;
    row.observed_native_message_count = 0U;
    row.certified_tick_count = 0U;
    row.pending_token_count = 1U;
    row.state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kFrozenConflict);
    return ipc::RealtimeCertifiedChannelStateCanonicalV1(row);
}());
static_assert([]() constexpr {
    auto row = MakeChannelState();
    row.origin_sequence = 0;
    row.observed_contiguous_frontier = 0;
    row.certified_published_frontier = 0;
    row.highest_observed_sequence = 0;
    row.canonical_apply_frontier = 0U;
    row.observed_native_message_count = 0U;
    row.certified_tick_count = 0U;
    row.pending_token_count = 0U;
    row.state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kFrozenResource);
    return ipc::RealtimeCertifiedChannelStateCanonicalV1(row);
}());
static_assert([]() constexpr {
    auto row = MakeChannelState();
    row.origin_sequence = 0;
    row.observed_contiguous_frontier = 0;
    row.certified_published_frontier = 0;
    row.highest_observed_sequence = 0;
    row.canonical_apply_frontier = 0U;
    row.observed_native_message_count = 1U;
    row.certified_tick_count = 0U;
    row.pending_token_count = 1U;
    row.state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kFrozenResource);
    return !ipc::RealtimeCertifiedChannelStateCanonicalV1(row);
}());
static_assert([]() constexpr {
    auto row = MakeChannelState();
    row.channel_correction_epoch = 0U;
    return !ipc::RealtimeCertifiedChannelStateCanonicalV1(row);
}());

static_assert([]() constexpr {
    auto payload = MakeShanghaiPayload();
    payload.native_event_sequence = 0;
    return !ipc::RealtimeCertifiedTickPayloadCanonicalV1(payload);
}());
static_assert([]() constexpr {
    auto payload = MakeShanghaiPayload();
    payload.channel = 0;
    return !ipc::RealtimeCertifiedTickPayloadCanonicalV1(payload);
}());
static_assert([]() constexpr {
    auto payload = MakeShanghaiPayload();
    payload.common.market = 2U;
    return !ipc::RealtimeCertifiedTickPayloadCanonicalV1(payload);
}());
static_assert([]() constexpr {
    auto payload = MakeShanghaiPayload();
    payload.common.event_kind = 1U;
    return !ipc::RealtimeCertifiedTickPayloadCanonicalV1(payload);
}());
static_assert([]() constexpr {
    auto payload = MakeShanghaiPayload();
    payload.common.reserved[0U] = 1U;
    return !ipc::RealtimeCertifiedTickPayloadCanonicalV1(payload);
}());
static_assert([]() constexpr {
    auto payload = MakeShanghaiPayload();
    payload.common.reserved0 = 1U;
    return !ipc::RealtimeCertifiedTickPayloadCanonicalV1(payload);
}());
static_assert([]() constexpr {
    auto payload = MakeShanghaiPayload();
    payload.reserved0 = 1U;
    return !ipc::RealtimeCertifiedTickPayloadCanonicalV1(payload);
}());
static_assert([]() constexpr {
    auto payload = MakeShanghaiPayload();
    payload.price.reserved[0U] = 1U;
    return !ipc::RealtimeCertifiedTickPayloadCanonicalV1(payload);
}());
static_assert([]() constexpr {
    auto payload = MakeShanghaiPayload();
    payload.raw_type[0U] = 1U;
    return !ipc::RealtimeCertifiedTickPayloadCanonicalV1(payload);
}());
static_assert([]() constexpr {
    auto envelope = MakeEnvelope();
    envelope.feed_epoch = 0U;
    return !ipc::RealtimeCertifiedTickEnvelopeCanonicalV1(
        envelope);
}());
static_assert([]() constexpr {
    auto slot = MakeTickSlot();
    slot.publish_tag = 3U;
    return !ipc::RealtimeCertifiedTickSlotPublishedV1(slot);
}());
static_assert([]() constexpr {
    auto slot = MakeTickSlot();
    slot.reserved0[0U] = 1U;
    return !ipc::RealtimeCertifiedTickSlotPublishedV1(slot);
}());
static_assert([]() constexpr {
    auto slot = MakeTickSlot();
    slot.payload_words[
        ipc::kRealtimeCertifiedTickEnvelopeWordsV1] = 1U;
    return !ipc::RealtimeCertifiedTickSlotPublishedV1(slot);
}());

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

bool TestAggregateStates() {
    bool ok = true;

    auto disabled = MakeHeader();
    disabled.aggregate_state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kDisabled);
    disabled.canonical_apply_frontier = 0U;
    disabled.correction_epoch = 0U;
    disabled.observed_native_message_count = 0U;
    disabled.certified_tick_count = 0U;
    disabled.exact_duplicate_message_count = 0U;
    disabled.gap_opened_count = 0U;
    disabled.gap_recovered_count = 0U;
    disabled.channel_state_count = 0U;
    ok &= Expect(
        ipc::RealtimeCertifiedHeaderCanonicalV1(disabled),
        "disabled state is an explicit readable sidecar state");

    auto no_data = disabled;
    no_data.aggregate_state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kNoData);
    no_data.correction_epoch = 1U;
    ok &= Expect(
        ipc::RealtimeCertifiedHeaderCanonicalV1(no_data),
        "enabled sidecar may publish NO_DATA before first domain");

    auto gap = MakeHeader();
    gap.aggregate_state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kGapOpen);
    gap.gap_open_channel_count = 1U;
    gap.pending_token_count = 2U;
    ok &= Expect(
        ipc::RealtimeCertifiedHeaderCanonicalV1(gap),
        "open gap remains a valid nonblocking read state");

    auto catching_up = MakeHeader();
    catching_up.aggregate_state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kCatchingUp);
    catching_up.catching_up_channel_count = 1U;
    catching_up.pending_token_count = 2U;
    ok &= Expect(
        ipc::RealtimeCertifiedHeaderCanonicalV1(catching_up),
        "catch-up remains a valid nonblocking read state");

    auto conflict = MakeHeader();
    conflict.aggregate_state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kFrozenConflict);
    conflict.conflicting_duplicate_count = 1U;
    ok &= Expect(
        ipc::RealtimeCertifiedHeaderCanonicalV1(conflict),
        "source-wide conflict may freeze before a row is available");

    auto resource = MakeHeader();
    resource.aggregate_state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kFrozenResource);
    resource.resource_exhaustion_count = 1U;
    ok &= Expect(
        ipc::RealtimeCertifiedHeaderCanonicalV1(resource),
        "source-wide resource freeze need not invent a channel row");

    auto stopped = MakeHeader();
    stopped.aggregate_state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kStopped);
    ok &= Expect(
        ipc::RealtimeCertifiedHeaderCanonicalV1(stopped),
        "stopped mapping retains its last coherent prefix");
    return ok;
}

bool TestChannelStates() {
    bool ok = true;

    auto no_data = MakeChannelState();
    no_data.state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kNoData);
    no_data.origin_sequence = 0;
    no_data.observed_contiguous_frontier = 0;
    no_data.certified_published_frontier = 0;
    no_data.highest_observed_sequence = 0;
    no_data.canonical_apply_frontier = 0U;
    no_data.observed_native_message_count = 0U;
    no_data.certified_tick_count = 0U;
    ok &= Expect(
        ipc::RealtimeCertifiedChannelStateCanonicalV1(no_data),
        "preprovisioned channel identity may be NO_DATA");

    auto gap = MakeChannelState();
    gap.state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kGapOpen);
    gap.observed_contiguous_frontier = 10;
    gap.certified_published_frontier = 10;
    gap.highest_observed_sequence = 12;
    gap.canonical_apply_frontier = 1U;
    gap.certified_tick_count = 1U;
    gap.pending_token_count = 2U;
    gap.gap_opened_count = 1U;
    ok &= Expect(
        ipc::RealtimeCertifiedChannelStateCanonicalV1(gap),
        "channel gap preserves its certified frontier");

    auto catching_up = MakeChannelState();
    catching_up.state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kCatchingUp);
    catching_up.certified_published_frontier = 10;
    catching_up.canonical_apply_frontier = 1U;
    catching_up.certified_tick_count = 1U;
    catching_up.pending_token_count = 2U;
    catching_up.gap_opened_count = 1U;
    catching_up.gap_recovered_count = 1U;
    ok &= Expect(
        ipc::RealtimeCertifiedChannelStateCanonicalV1(catching_up),
        "filled gap may catch up without blocking reads");

    auto conflict = gap;
    conflict.state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kFrozenConflict);
    ok &= Expect(
        ipc::RealtimeCertifiedChannelStateCanonicalV1(conflict),
        "conflicting duplicate freezes a channel at last good data");

    auto resource = gap;
    resource.state = static_cast<std::uint32_t>(
        ipc::RealtimeCertifiedStateV1::kFrozenResource);
    ok &= Expect(
        ipc::RealtimeCertifiedChannelStateCanonicalV1(resource),
        "bounded-capacity failure freezes a channel");
    return ok;
}

bool TestPayloadAuthorityAndRingIndex() {
    bool ok = true;

    auto shenzhen_order = MakeShanghaiPayload();
    shenzhen_order.common.source_slot = 3U;
    shenzhen_order.common.event_kind = 4U;
    shenzhen_order.common.market = 2U;
    shenzhen_order.channel = 0;
    ok &= Expect(
        ipc::RealtimeCertifiedTickPayloadCanonicalV1(
            shenzhen_order),
        "Shenzhen order accepts documented ChannelNo zero");

    auto shenzhen_transaction = shenzhen_order;
    shenzhen_transaction.common.event_kind = 5U;
    ok &= Expect(
        ipc::RealtimeCertifiedTickPayloadCanonicalV1(
            shenzhen_transaction),
        "Shenzhen transaction takes native identity from payload");

    auto bad_projection = shenzhen_order;
    bad_projection.projection_flags =
        ipc::kRealtimeWireTickRawTypeOmittedV2;
    ok &= Expect(
        !ipc::RealtimeCertifiedTickPayloadCanonicalV1(
            bad_projection),
        "Shenzhen payload rejects Shanghai-only raw projection flags");

    std::uint32_t index = 99U;
    ok &= Expect(
        ipc::RealtimeCertifiedRingSlotIndexV1(1U, 8U, &index) &&
            index == 0U,
        "first dense sequence maps to first ring slot");
    ok &= Expect(
        ipc::RealtimeCertifiedRingSlotIndexV1(8U, 8U, &index) &&
            index == 7U,
        "capacity boundary maps to final ring slot");
    ok &= Expect(
        ipc::RealtimeCertifiedRingSlotIndexV1(9U, 8U, &index) &&
            index == 0U,
        "next sequence wraps the dense ring");
    index = 99U;
    ok &= Expect(
        !ipc::RealtimeCertifiedRingSlotIndexV1(0U, 8U, &index) &&
            index == 99U,
        "sequence zero is rejected without modifying output");
    ok &= Expect(
        ipc::RealtimeCertifiedRingSlotIndexV1(
            std::numeric_limits<std::uint64_t>::max(),
            8U,
            &index),
        "maximum representable published sequence indexes safely");
    return ok;
}

}  // namespace

int main() {
    bool ok = true;
    ok &= TestAggregateStates();
    ok &= TestChannelStates();
    ok &= TestPayloadAuthorityAndRingIndex();
    if (ok) {
        std::cout << "PASS: realtime certified wire v1\n";
        return 0;
    }
    return 1;
}
