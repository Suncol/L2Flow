#pragma once

#include "l2flow/ingress/raw_control_page.h"
#include "l2flow/ingress/raw_v1.h"

#include <cstdint>

namespace l2flow::control {

// Validates an ordered pair of stream-day Raw cursors when one or more
// segment transitions may lie between them. A transition advances WAL by one
// 4096-byte segment header without advancing ingress_sequence; every sequence
// increment, on the other hand, requires at least one minimum-sized Raw
// record. This is a necessary arithmetic condition, not proof that either
// cursor is an actual validated record boundary.
[[nodiscard]] constexpr bool RawCursorAdvancePlausibleV1(
    std::uint64_t earlier_wal_pos,
    std::uint64_t earlier_ingress_sequence,
    std::uint64_t later_wal_pos,
    std::uint64_t later_ingress_sequence) noexcept {
    if ((earlier_wal_pos %
         l2flow::ingress::kRawV1RecordAlignment) != 0U ||
        (later_wal_pos %
         l2flow::ingress::kRawV1RecordAlignment) != 0U ||
        later_wal_pos < earlier_wal_pos ||
        later_ingress_sequence < earlier_ingress_sequence) {
        return false;
    }
    const std::uint64_t wal_delta = later_wal_pos - earlier_wal_pos;
    const std::uint64_t sequence_delta =
        later_ingress_sequence - earlier_ingress_sequence;
    if (wal_delta == 0U) {
        return sequence_delta == 0U;
    }
    if (sequence_delta == 0U) {
        return (wal_delta %
                l2flow::ingress::kRawV1SegmentHeaderBytes) == 0U;
    }
    constexpr std::uint64_t kMinimumRecordBytes =
        l2flow::ingress::kRawV1RecordHeaderBytes +
        l2flow::ingress::kRawV1RecordTrailerBytes;
    return sequence_delta <= wal_delta / kMinimumRecordBytes;
}

// Checks the arithmetic invariants of the append/durable cursor pair carried
// by one Raw control-page snapshot.  Both cursors are in the same segment
// coordinate system.  Consequently every newly appended Raw record advances
// both ingress_sequence and WAL position, while a segment header advances
// neither side of an append-versus-durable pair independently.
[[nodiscard]] constexpr bool RawFrontierCursorShapeValidV1(
    const l2flow::ingress::RawControlSnapshot& frontier) noexcept {
    if (frontier.segment_sequence == 0U ||
        frontier.append_segment_offset <
            l2flow::ingress::kRawV1SegmentHeaderBytes ||
        frontier.durable_segment_offset <
            l2flow::ingress::kRawV1SegmentHeaderBytes ||
        (frontier.append_global_wal_pos %
         l2flow::ingress::kRawV1RecordAlignment) != 0U ||
        (frontier.durable_global_wal_pos %
         l2flow::ingress::kRawV1RecordAlignment) != 0U ||
        (frontier.append_segment_offset %
         l2flow::ingress::kRawV1RecordAlignment) != 0U ||
        (frontier.durable_segment_offset %
         l2flow::ingress::kRawV1RecordAlignment) != 0U ||
        frontier.append_global_wal_pos <
            frontier.append_segment_offset ||
        frontier.durable_global_wal_pos <
            frontier.durable_segment_offset ||
        frontier.durable_segment_offset >
            frontier.append_segment_offset ||
        frontier.durable_global_wal_pos >
            frontier.append_global_wal_pos ||
        frontier.durable_ingress_sequence >
            frontier.append_ingress_sequence) {
        return false;
    }

    const std::uint64_t segment_base_wal_pos =
        frontier.append_global_wal_pos -
        frontier.append_segment_offset;
    const std::uint64_t durable_segment_base_wal_pos =
        frontier.durable_global_wal_pos -
        frontier.durable_segment_offset;
    const std::uint64_t minimum_segment_base_wal_pos =
        static_cast<std::uint64_t>(
            frontier.segment_sequence - 1U) *
        l2flow::ingress::kRawV1SegmentHeaderBytes;
    if (segment_base_wal_pos != durable_segment_base_wal_pos ||
        segment_base_wal_pos < minimum_segment_base_wal_pos ||
        (frontier.segment_sequence == 1U &&
         segment_base_wal_pos != 0U)) {
        return false;
    }

    const std::uint64_t appended_bytes =
        frontier.append_global_wal_pos -
        frontier.durable_global_wal_pos;
    const std::uint64_t appended_records =
        frontier.append_ingress_sequence -
        frontier.durable_ingress_sequence;
    constexpr std::uint64_t kMinimumRecordBytes =
        l2flow::ingress::kRawV1RecordHeaderBytes +
        l2flow::ingress::kRawV1RecordTrailerBytes;
    return (appended_bytes == 0U) == (appended_records == 0U) &&
           appended_records <= appended_bytes / kMinimumRecordBytes;
}

}  // namespace l2flow::control
