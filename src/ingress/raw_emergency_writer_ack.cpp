#include "l2flow/ingress/raw_emergency_writer_ack.h"

#include <limits>

namespace l2flow::ingress {
namespace {

[[nodiscard]] bool CheckedAdd(
    std::uint64_t left,
    std::uint64_t right,
    std::uint64_t* result) noexcept {
    if (result == nullptr ||
        right >
            std::numeric_limits<std::uint64_t>::max() -
                left) {
        return false;
    }
    *result = left + right;
    return true;
}

}  // namespace

bool RawEmergencyWriterAckFactsV1::exact()
    const noexcept {
    if (!sdk_shutdown_returned ||
        !callback_quiesced ||
        !regular_writer_mutation_stopped ||
        !same_process_ring_suffix_retained ||
        callback.callback_inflight ||
        !capture.startup_complete ||
        !capture.startup_succeeded ||
        capture.finished ||
        capture.stop_requested ||
        !capture.emergency_pause_requested ||
        !capture.emergency_paused ||
        capture.emergency_abandoned ||
        capture.failure_kind !=
            RawCaptureWorkerFailureKind::kNone ||
        !wal.initialized || wal.sealed || wal.closed ||
        wal.fatal ||
        writer.writer_instance !=
            started_runtime.writer_instance ||
        writer.stream_day_id !=
            started_runtime.stream_day_id ||
        writer.source_stream_id !=
            started_runtime.source_stream_id ||
        writer.capture_date !=
            started_runtime.capture_date ||
        wal.append.global_wal_pos <
            wal.append.segment_offset ||
        writer.segment_base_wal_pos !=
            wal.append.global_wal_pos -
                wal.append.segment_offset ||
        callback.captured_records <
            capture.append.records ||
        callback.captured_vendor_bytes <
            capture.append.vendor_bytes ||
        callback.captured_framed_wal_bytes <
            capture.append.framed_wal_bytes ||
        capture.append.records <
            capture.durable.records ||
        capture.append.vendor_bytes <
            capture.durable.vendor_bytes ||
        capture.append.framed_wal_bytes <
            capture.durable.framed_wal_bytes ||
        callback.captured_records -
                capture.append.records !=
            queued_record_count ||
        callback.captured_framed_wal_bytes -
                capture.append.framed_wal_bytes !=
            queued_framed_wal_bytes ||
        ring_published_position <
            ring_consumed_position ||
        ring_published_position -
                ring_consumed_position !=
            ring_used_bytes ||
        (queued_record_count == 0U) !=
            (ring_used_bytes == 0U) ||
        (queued_record_count == 0U) !=
            (queued_framed_wal_bytes == 0U)) {
        return false;
    }

    std::uint64_t expected_callback_sequence = 0U;
    std::uint64_t expected_append_sequence = 0U;
    std::uint64_t expected_durable_sequence = 0U;
    if (!CheckedAdd(
            started_runtime.append.ingress_sequence,
            callback.captured_records,
            &expected_callback_sequence) ||
        !CheckedAdd(
            started_runtime.append.ingress_sequence,
            capture.append.records,
            &expected_append_sequence) ||
        !CheckedAdd(
            started_runtime.durable.ingress_sequence,
            capture.durable.records,
            &expected_durable_sequence) ||
        wal.append.ingress_sequence !=
            expected_append_sequence ||
        wal.durable.ingress_sequence !=
            expected_durable_sequence ||
        capture.append.last_ingress_sequence !=
            expected_append_sequence ||
        capture.durable.last_ingress_sequence !=
            expected_durable_sequence) {
        return false;
    }
    if (callback.captured_records == 0U) {
        return callback.captured_ingress_sequence == 0U;
    }
    return callback.captured_ingress_sequence ==
           expected_callback_sequence;
}

}  // namespace l2flow::ingress
