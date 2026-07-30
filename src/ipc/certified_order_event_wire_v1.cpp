#include "l2flow/ipc/certified_order_event_wire_v1.h"

#include <algorithm>
#include <cstdint>
#include <limits>

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

[[nodiscard]] bool BoolByte(std::uint8_t value) noexcept {
    return value <= 1U;
}

[[nodiscard]] bool AnyNonzero(
    const std::array<std::uint8_t, 16U>& value) noexcept {
    return std::any_of(
        value.begin(), value.end(), [](std::uint8_t byte) {
            return byte != 0U;
        });
}

}  // namespace

bool CertifiedOrderEventHeaderCanonicalV1(
    const CertifiedOrderEventHeaderV1& header) noexcept {
    using namespace certified_order_event_wire_v1_detail;

    if (header.magic != kCertifiedOrderEventMagicV1 ||
        header.abi_major != kCertifiedOrderEventWireMajorV1 ||
        header.abi_minor != kCertifiedOrderEventWireMinorV1 ||
        header.header_bytes != kCertifiedOrderEventHeaderBytesV1 ||
        header.endian_marker !=
            kCertifiedOrderEventEndianMarkerV1 ||
        header.flags != 0U || !AnyNonzero(header.run_id) ||
        header.session_epoch == 0U ||
        !ValidTradeDate(header.trade_date) ||
        header.reserved_identity != 0U ||
        header.slots_offset !=
            kCertifiedOrderEventHeaderBytesV1 ||
        header.event_capacity == 0U ||
        header.slot_stride != kCertifiedOrderEventSlotBytesV1 ||
        header.region_alignment !=
            kCertifiedOrderEventAlignmentV1 ||
        !AllZero(header.reserved_layout) ||
        header.status_publish_tag == 0U ||
        (header.status_publish_tag & 1U) != 0U ||
        header.heartbeat_monotonic_ns == 0U ||
        header.generation != header.canonical_apply_frontier ||
        header.event_published_sequence > header.event_capacity ||
        header.committed_mapping_bytes <
            kCertifiedOrderEventHeaderBytesV1 ||
        header.committed_mapping_bytes >
            header.total_mapping_bytes ||
        header.committed_mapping_bytes % 4096U != 0U ||
        header.reserved_status != 0U ||
        !AllZero(header.reserved)) {
        return false;
    }

    std::uint64_t slots_bytes = 0U;
    std::uint64_t logical_end = 0U;
    std::uint64_t expected_bytes = 0U;
    return CheckedMultiply(
               header.event_capacity,
               kCertifiedOrderEventSlotBytesV1,
               &slots_bytes) &&
           CheckedAdd(
               header.slots_offset,
               slots_bytes,
               &logical_end) &&
           AlignUp(logical_end, 4096U, &expected_bytes) &&
           expected_bytes == header.total_mapping_bytes;
}

bool CertifiedOrderEventRowCanonicalV1(
    const l2flow_instrument_derived_event_row_v1& row,
    std::uint32_t expected_trade_date,
    std::uint64_t expected_derived_event_sequence) noexcept {
    if (!ValidTradeDate(expected_trade_date) ||
        expected_derived_event_sequence == 0U ||
        row.record_schema_version != 1U ||
        row.record_bytes != sizeof(row) ||
        row.derived_event_sequence !=
            expected_derived_event_sequence ||
        row.trade_date != expected_trade_date ||
        row.instrument_id == 0U || row.channel < 0 ||
        row.native_event_sequence <= 0 ||
        row.source_sequence == 0U ||
        row.source_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        row.ingress_sequence == 0U ||
        row.ingress_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        row.tick_stream_sequence == 0U ||
        row.tick_stream_sequence ==
            std::numeric_limits<std::uint64_t>::max() ||
        row.tick_stream_sequence > row.ingress_sequence ||
        row.reserved0 != 0U ||
        !std::all_of(
            std::begin(row.reserved1),
            std::end(row.reserved1),
            [](std::uint8_t value) { return value == 0U; }) ||
        (row.market !=
             L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1 &&
         row.market !=
             L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1) ||
        row.event_kind <
            L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1 ||
        row.event_kind >
            L2FLOW_INSTRUMENT_DERIVED_EVENT_STATUS_V1 ||
        row.operation > 2U || row.finality > 2U ||
        row.side > 4U || row.side_source > 2U ||
        row.aggressor > 3U || row.phase > 7U ||
        row.phase_at_first > 7U || row.phase_at_add > 7U ||
        row.phase_at_last > 7U || row.order_type > 3U ||
        row.order_source > 2U || row.price_source > 3U ||
        row.original_quantity_status > 2U ||
        !BoolByte(row.price_valid) ||
        !BoolByte(row.execution_boundary_price_valid) ||
        !BoolByte(row.trade_amount_valid) ||
        !BoolByte(row.published_quantity_valid) ||
        !BoolByte(row.original_quantity_valid) ||
        !BoolByte(row.remaining_quantity_valid) ||
        !BoolByte(row.source_matched_quantity_valid) ||
        !BoolByte(row.add_seen) ||
        !BoolByte(row.apply_to_book) ||
        !BoolByte(row.referenced_order_found) ||
        !BoolByte(row.side_from_order) ||
        !BoolByte(row.event_time_valid) ||
        !BoolByte(row.event_time_unix_ns_valid) ||
        !BoolByte(row.vendor_local_time_valid)) {
        return false;
    }

    // SDK documentation requires a positive Shanghai Channel but does not
    // prohibit Shenzhen ChannelNo zero; preserve that documented asymmetry.
    if (row.market ==
            L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1 &&
        row.channel <= 0) {
        return false;
    }

    switch (row.event_kind) {
        case L2FLOW_INSTRUMENT_DERIVED_EVENT_ORDER_REVISION_V1:
            return row.order_id > 0 && row.revision > 0U;
        case L2FLOW_INSTRUMENT_DERIVED_EVENT_TRADE_V1:
            return row.quantity > 0 && row.price_valid == 1U;
        case L2FLOW_INSTRUMENT_DERIVED_EVENT_CANCEL_V1:
            return row.order_id > 0 && row.quantity > 0;
        case L2FLOW_INSTRUMENT_DERIVED_EVENT_STATUS_V1:
            return row.market ==
                   L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1;
        default:
            return false;
    }
}

bool CertifiedOrderEventEnvelopeCanonicalV1(
    const CertifiedOrderEventEnvelopeV1& envelope,
    std::uint32_t expected_trade_date,
    std::uint64_t expected_derived_event_sequence) noexcept {
    return envelope.canonical_apply_sequence != 0U &&
           CertifiedOrderEventRowCanonicalV1(
               envelope.event,
               expected_trade_date,
               expected_derived_event_sequence);
}

}  // namespace l2flow::ipc
