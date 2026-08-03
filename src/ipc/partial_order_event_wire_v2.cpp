#include "l2flow/ipc/partial_order_event_wire_v2.h"

#include "l2flow/common/crc32c.h"
#include "l2flow/ipc/certified_order_event_wire_v1.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>

namespace l2flow::ipc {
namespace {

[[nodiscard]] bool ValidTradeDate(std::uint32_t value) noexcept {
    const std::uint32_t year = value / 10'000U;
    const std::uint32_t month = (value / 100U) % 100U;
    const std::uint32_t day = value % 100U;
    if (year < 1992U || year > 2200U || month == 0U ||
        month > 12U || day == 0U) {
        return false;
    }
    constexpr std::array<std::uint32_t, 12U> days_by_month{
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U};
    std::uint32_t maximum_day = days_by_month[month - 1U];
    const bool leap =
        (year % 4U == 0U && year % 100U != 0U) ||
        year % 400U == 0U;
    if (month == 2U && leap) {
        maximum_day = 29U;
    }
    return day <= maximum_day;
}

[[nodiscard]] bool AnyNonzero(
    const std::array<std::uint8_t, 16U>& value) noexcept {
    return std::any_of(
        value.begin(), value.end(), [](std::uint8_t byte) {
            return byte != 0U;
        });
}

[[nodiscard]] bool ValidState(
    PartialOrderEventServiceStateV2 state) noexcept {
    return state >= PartialOrderEventServiceStateV2::kInitializing &&
           state <= PartialOrderEventServiceStateV2::kStoppedClean;
}

[[nodiscard]] bool ValidLastError(
    PartialOrderEventLastErrorV2 error) noexcept {
    return error >= PartialOrderEventLastErrorV2::kNone &&
           error <=
               PartialOrderEventLastErrorV2::kPublicationInvariant;
}

[[nodiscard]] bool StateRequiresStale(
    PartialOrderEventServiceStateV2 state) noexcept {
    return state == PartialOrderEventServiceStateV2::kReordering ||
           state == PartialOrderEventServiceStateV2::kCatchingUp ||
           state ==
               PartialOrderEventServiceStateV2::kFrozenConflict ||
           state ==
               PartialOrderEventServiceStateV2::kFrozenResource ||
           state == PartialOrderEventServiceStateV2::kRestarting ||
           state ==
               PartialOrderEventServiceStateV2::kCorrectionPending;
}

}  // namespace

bool PartialOrderEventHeaderLayoutCanonicalV2(
    const PartialOrderEventHeaderV2& header) noexcept {
    using namespace partial_order_event_wire_v2_detail;

    if (header.magic != kPartialOrderEventMagicV2 ||
        header.abi_major != kPartialOrderEventWireMajorV2 ||
        header.abi_minor != kPartialOrderEventWireMinorV2 ||
        header.header_bytes != kPartialOrderEventHeaderBytesV2 ||
        header.endian_marker != kPartialOrderEventEndianMarkerV2 ||
        header.temporal_coverage !=
            PartialOrderEventTemporalCoverageV2::kProcessStart ||
        !AnyNonzero(header.run_id) || header.session_epoch == 0U ||
        !ValidTradeDate(header.trade_date) ||
        header.reserved_identity != 0U ||
        header.publication_generation == 0U ||
        header.correction_epoch == 0U ||
        header.coverage_start_unix_ns == 0U ||
        header.ordering_quality !=
            PartialOrderEventOrderingQualityV2::
                kBoundedReorderedPartial ||
        header.reserved_quality != 0U ||
        header.slots_offset != kPartialOrderEventHeaderBytesV2 ||
        header.event_capacity == 0U ||
        header.slot_stride != kPartialOrderEventSlotBytesV2 ||
        header.region_alignment != kPartialOrderEventAlignmentV2 ||
        header.channel_capacity == 0U ||
        header.channel_stride !=
            kPartialOrderEventChannelHealthBytesV2 ||
        header.order_state_capacity == 0U ||
        (header.order_state_capacity &
             (header.order_state_capacity - 1U)) != 0U ||
        header.order_state_stride !=
            kPartialOrderEventOrderStateSlotBytesV2 ||
        header.reserved_order_state != 0U ||
        header.extension_offset != 0U ||
        header.extension_bytes != 0U ||
        !AllZero(header.reserved_layout) ||
        !AllZero(header.reserved)) {
        return false;
    }

    std::uint64_t event_bytes = 0U;
    std::uint64_t event_end = 0U;
    std::uint64_t expected_bank0 = 0U;
    std::uint64_t channel_bytes = 0U;
    std::uint64_t expected_bank_bytes = 0U;
    std::uint64_t expected_bank1 = 0U;
    std::uint64_t expected_order_offset = 0U;
    std::uint64_t order_bytes = 0U;
    std::uint64_t logical_end = 0U;
    std::uint64_t expected_total = 0U;
    return CheckedMultiply(
               header.event_capacity,
               kPartialOrderEventSlotBytesV2,
               &event_bytes) &&
           CheckedAdd(header.slots_offset, event_bytes, &event_end) &&
           AlignUp(event_end, 4096U, &expected_bank0) &&
           CheckedMultiply(
               header.channel_capacity,
               kPartialOrderEventChannelHealthBytesV2,
               &channel_bytes) &&
           AlignUp(channel_bytes, 4096U, &expected_bank_bytes) &&
           CheckedAdd(
               expected_bank0,
               expected_bank_bytes,
               &expected_bank1) &&
           CheckedAdd(
               expected_bank1,
               expected_bank_bytes,
               &expected_order_offset) &&
           CheckedMultiply(
               header.order_state_capacity,
               kPartialOrderEventOrderStateSlotBytesV2,
               &order_bytes) &&
           CheckedAdd(
               expected_order_offset, order_bytes, &logical_end) &&
           AlignUp(logical_end, 4096U, &expected_total) &&
           header.channel_bank_offsets[0U] == expected_bank0 &&
           header.channel_bank_offsets[1U] == expected_bank1 &&
           header.channel_bank_bytes == expected_bank_bytes &&
           header.order_states_offset == expected_order_offset &&
           header.total_mapping_bytes == expected_total;
}

std::uint32_t PartialOrderEventCommitCutCrc32cV2(
    const PartialOrderEventCommitCutV2& cut) noexcept {
    PartialOrderEventCommitCutV2 normalized = cut;
    normalized.publish_tag = 0U;
    normalized.cut_crc32c = 0U;
    return common::ComputeCrc32c(
        std::as_bytes(std::span{&normalized, std::size_t{1U}}));
}

bool PartialOrderEventCommitCutCanonicalV2(
    const PartialOrderEventHeaderV2& header,
    const PartialOrderEventCommitCutV2& cut,
    std::uint32_t expected_bank) noexcept {
    if (expected_bank > 1U || cut.commit_sequence == 0U ||
        cut.commit_sequence >
            std::numeric_limits<std::uint64_t>::max() / 2U ||
        cut.publish_tag != cut.commit_sequence * 2U ||
        cut.heartbeat_monotonic_ns == 0U ||
        cut.history_generation != cut.canonical_apply_frontier ||
        cut.event_published_frontier > header.event_capacity ||
        cut.order_state_generation !=
            header.publication_generation ||
        cut.order_state_canonical_frontier !=
            cut.canonical_apply_frontier ||
        cut.committed_event_region_bytes <
            kPartialOrderEventHeaderBytesV2 ||
        cut.committed_event_region_bytes >
            header.channel_bank_offsets[0U] ||
        cut.committed_event_region_bytes % 4096U != 0U ||
        cut.pending_count > cut.reorder_high_water ||
        cut.affected_channel_count > cut.channel_health_count ||
        cut.channel_health_count > header.channel_capacity ||
        !ValidState(cut.state) || cut.stale > 1U ||
        !ValidLastError(cut.last_error) ||
        cut.active_channel_bank != expected_bank ||
        !partial_order_event_wire_v2_detail::AllZero(cut.reserved) ||
        cut.cut_crc32c != PartialOrderEventCommitCutCrc32cV2(cut)) {
        return false;
    }

    std::uint64_t visible_event_bytes = 0U;
    std::uint64_t visible_event_end = 0U;
    if (!partial_order_event_wire_v2_detail::CheckedMultiply(
            cut.event_published_frontier,
            kPartialOrderEventSlotBytesV2,
            &visible_event_bytes) ||
        !partial_order_event_wire_v2_detail::CheckedAdd(
            header.slots_offset,
            visible_event_bytes,
            &visible_event_end) ||
        visible_event_end > cut.committed_event_region_bytes) {
        return false;
    }

    if (cut.state == PartialOrderEventServiceStateV2::kContiguous &&
        (cut.stale != 0U || cut.pending_count != 0U ||
         cut.affected_channel_count != 0U ||
         cut.oldest_gap_age_ns != 0U ||
         cut.last_error != PartialOrderEventLastErrorV2::kNone)) {
        return false;
    }
    if (StateRequiresStale(cut.state) && cut.stale == 0U) {
        return false;
    }
    return true;
}

bool PartialOrderEventChannelHealthCanonicalV2(
    const PartialOrderEventChannelHealthV2& health,
    std::uint64_t expected_commit_sequence) noexcept {
    if (expected_commit_sequence == 0U ||
        health.commit_sequence != expected_commit_sequence ||
        (health.market !=
             L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1 &&
         health.market !=
             L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1) ||
        (health.market ==
             L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1 &&
         health.channel <= 0) ||
        health.channel < 0 ||
        (health.flags & ~kPartialOrderEventKnownChannelFlagsV2) != 0U ||
        !ValidState(health.state) ||
        !ValidLastError(health.last_error) ||
        !partial_order_event_wire_v2_detail::AllZero(
            health.reserved)) {
        return false;
    }

    const bool origin_established =
        (health.flags &
         kPartialOrderEventChannelOriginEstablishedV2) != 0U;
    if (origin_established &&
        (health.expected_native_sequence <= 0 ||
         health.contiguous_native_sequence < 0 ||
         health.contiguous_native_sequence ==
             std::numeric_limits<std::int64_t>::max() ||
         health.expected_native_sequence !=
             health.contiguous_native_sequence + 1 ||
         health.highest_observed_native_sequence <
             health.contiguous_native_sequence ||
         health.oldest_missing_native_sequence < 0)) {
        return false;
    }
    const bool affected =
        (health.flags & kPartialOrderEventChannelAffectedV2) != 0U;
    const bool stale =
        (health.flags & kPartialOrderEventChannelStaleV2) != 0U;
    if (affected && !stale) {
        return false;
    }
    if (health.state ==
            PartialOrderEventServiceStateV2::kContiguous &&
        (affected || stale || health.pending_count != 0U ||
         health.oldest_gap_age_ns != 0U ||
         health.oldest_missing_native_sequence != 0 ||
         health.last_error != PartialOrderEventLastErrorV2::kNone)) {
        return false;
    }
    if (StateRequiresStale(health.state) && !stale) {
        return false;
    }
    return true;
}

bool PartialOrderEventEnvelopeCanonicalV2(
    const PartialOrderEventEnvelopeV2& envelope,
    std::uint32_t expected_trade_date,
    std::uint64_t expected_derived_event_sequence) noexcept {
    return envelope.canonical_apply_sequence != 0U &&
           CertifiedOrderEventRowCanonicalV1(
               envelope.event,
               expected_trade_date,
               expected_derived_event_sequence);
}

bool PartialOrderEventOrderStateCanonicalV2(
    const PartialOrderEventOrderStateV2& state,
    std::uint32_t expected_trade_date,
    std::uint32_t expected_market,
    std::uint32_t expected_instrument_id,
    std::int64_t expected_channel,
    std::int64_t expected_order_id) noexcept {
    return state.canonical_apply_sequence != 0U &&
           state.order_revision.derived_event_sequence != 0U &&
           CertifiedOrderEventRowCanonicalV1(
               state.order_revision,
               expected_trade_date,
               state.order_revision.derived_event_sequence) &&
           state.order_revision.event_kind ==
               L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1 &&
           state.order_revision.market == expected_market &&
           state.order_revision.instrument_id ==
               expected_instrument_id &&
           state.order_revision.channel == expected_channel &&
           state.order_revision.order_id == expected_order_id;
}

}  // namespace l2flow::ipc
