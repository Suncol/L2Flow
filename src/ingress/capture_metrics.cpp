#include "l2flow/ingress/capture_metrics.h"

#include <bit>
#include <iomanip>
#include <sstream>
#include <string_view>

namespace l2flow::ingress {
namespace {

std::size_t LatencyBucket(std::uint64_t value) noexcept {
    if (value <= 1U) {
        return 0U;
    }
    const std::size_t bucket =
        static_cast<std::size_t>(
            std::bit_width(value) - 1U);
    return bucket < kCallbackLatencyBucketCount
               ? bucket
               : kCallbackLatencyBucketCount - 1U;
}

const char* InvalidReasonName(std::size_t index) noexcept {
    static constexpr const char* names[kInvalidMessageReasonCount] = {
        "null_message",
        "null_head",
        "wrong_head_size",
        "message_smaller_than_head",
        "message_too_large",
        "null_body",
        "unexpected_service",
    };
    return index < kInvalidMessageReasonCount ? names[index] : "unknown";
}

void AppendLabelValue(
    std::ostringstream& output,
    std::string_view value) {
    output << '"';
    for (const char character : value) {
        switch (character) {
        case '\\':
            output << "\\\\";
            break;
        case '"':
            output << "\\\"";
            break;
        case '\n':
            output << "\\n";
            break;
        default:
            output << character;
            break;
        }
    }
    output << '"';
}

}  // namespace

CaptureMetrics::CaptureMetrics() noexcept {
    for (auto& value : invalid_messages_) {
        value.store(0U, std::memory_order_relaxed);
    }
    for (auto& value : latency_ns_) {
        value.store(0U, std::memory_order_relaxed);
    }
}

void CaptureMetrics::IncrementCallbackInvocations() noexcept {
    callback_invocations_.fetch_add(1U, std::memory_order_relaxed);
}

void CaptureMetrics::IncrementCaptured(
    std::uint32_t vendor_message_bytes,
    std::uint64_t ingress_sequence,
    std::uint64_t framed_wal_bytes) noexcept {
    captured_publication_version_.fetch_add(
        1U, std::memory_order_acq_rel);
    captured_records_.fetch_add(1U, std::memory_order_relaxed);
    captured_vendor_bytes_.fetch_add(vendor_message_bytes,
                                     std::memory_order_relaxed);
    captured_framed_wal_bytes_.fetch_add(
        framed_wal_bytes, std::memory_order_relaxed);
    captured_ingress_sequence_.store(
        ingress_sequence, std::memory_order_relaxed);
    captured_publication_version_.fetch_add(
        1U, std::memory_order_release);
}

void CaptureMetrics::IncrementReentry() noexcept {
    callback_reentry_.fetch_add(1U, std::memory_order_relaxed);
}

void CaptureMetrics::IncrementException() noexcept {
    callback_exceptions_.fetch_add(1U, std::memory_order_relaxed);
}

void CaptureMetrics::IncrementAfterStop() noexcept {
    callbacks_after_stop_.fetch_add(1U, std::memory_order_relaxed);
}

void CaptureMetrics::IncrementAfterFatal() noexcept {
    callbacks_after_fatal_.fetch_add(1U, std::memory_order_relaxed);
}

void CaptureMetrics::IncrementRingOverflow() noexcept {
    ring_overflow_.fetch_add(1U, std::memory_order_relaxed);
}

void CaptureMetrics::IncrementInvalid(InvalidMessageReason reason) noexcept {
    const std::size_t index = static_cast<std::size_t>(reason);
    if (index < invalid_messages_.size()) {
        invalid_messages_[index].fetch_add(1U, std::memory_order_relaxed);
    }
}

void CaptureMetrics::SetInflight(bool value) noexcept {
    callback_inflight_.store(value, std::memory_order_release);
}

void CaptureMetrics::RecordLatencyNanoseconds(std::uint64_t value) noexcept {
    latency_ns_[LatencyBucket(value)].fetch_add(
        1U, std::memory_order_relaxed);
}

CaptureMetricsSnapshot CaptureMetrics::Snapshot() const noexcept {
    CaptureMetricsSnapshot result;
    // Read the captured tuple under a tiny sequence lock. A plain collection
    // of relaxed loads can otherwise pair a new sequence with old counts (or
    // observe counts from a callback whose sequence is not published yet).
    for (;;) {
        const std::uint64_t before =
            captured_publication_version_.load(
                std::memory_order_acquire);
        if ((before & 1U) != 0U) {
            continue;
        }
        result.captured_records =
            captured_records_.load(std::memory_order_relaxed);
        result.captured_vendor_bytes =
            captured_vendor_bytes_.load(std::memory_order_relaxed);
        result.captured_framed_wal_bytes =
            captured_framed_wal_bytes_.load(
                std::memory_order_relaxed);
        result.captured_ingress_sequence =
            captured_ingress_sequence_.load(
                std::memory_order_relaxed);
        const std::uint64_t after =
            captured_publication_version_.load(
                std::memory_order_acquire);
        if (before == after) {
            break;
        }
    }
    result.callback_invocations =
        callback_invocations_.load(std::memory_order_relaxed);
    result.callback_reentry =
        callback_reentry_.load(std::memory_order_relaxed);
    result.callback_exceptions =
        callback_exceptions_.load(std::memory_order_relaxed);
    result.callbacks_after_stop =
        callbacks_after_stop_.load(std::memory_order_relaxed);
    result.callbacks_after_fatal =
        callbacks_after_fatal_.load(std::memory_order_relaxed);
    result.ring_overflow = ring_overflow_.load(std::memory_order_relaxed);
    result.callback_inflight =
        callback_inflight_.load(std::memory_order_acquire);
    for (std::size_t index = 0U; index < invalid_messages_.size(); ++index) {
        result.invalid_messages[index] =
            invalid_messages_[index].load(std::memory_order_relaxed);
    }
    for (std::size_t index = 0U; index < latency_ns_.size(); ++index) {
        result.latency_ns[index] =
            latency_ns_[index].load(std::memory_order_relaxed);
    }
    return result;
}

std::string RenderPrometheusMetrics(
    std::string_view service_name,
    std::uint32_t source_stream_id,
    const CaptureMetricsSnapshot& capture,
    std::uint64_t ring_used_bytes,
    std::uint64_t ring_capacity_bytes,
    bool fatal) {
    std::ostringstream output;
    const auto label = [&output, service_name, source_stream_id]() {
        output << "{service=";
        AppendLabelValue(output, service_name);
        output << ",source_stream_id=\"" << source_stream_id << "\"}";
    };
    output << "mdl_callback_invocations_total";
    label();
    output << ' ' << capture.callback_invocations << '\n';
    output << "mdl_callback_captured_records_total";
    label();
    output << ' ' << capture.captured_records << '\n';
    output << "mdl_callback_captured_vendor_bytes_total";
    label();
    output << ' ' << capture.captured_vendor_bytes << '\n';
    output << "mdl_callback_captured_framed_wal_bytes_total";
    label();
    output << ' ' << capture.captured_framed_wal_bytes
           << '\n';
    output << "mdl_callback_reentry_total";
    label();
    output << ' ' << capture.callback_reentry << '\n';
    output << "mdl_callback_exceptions_total";
    label();
    output << ' ' << capture.callback_exceptions << '\n';
    output << "mdl_callback_after_stop_total";
    label();
    output << ' ' << capture.callbacks_after_stop << '\n';
    output << "mdl_callback_after_fatal_total";
    label();
    output << ' ' << capture.callbacks_after_fatal << '\n';
    output << "mdl_ingress_ring_overflow_total";
    label();
    output << ' ' << capture.ring_overflow << '\n';
    output << "mdl_callback_inflight";
    label();
    output << ' ' << (capture.callback_inflight ? 1 : 0) << '\n';
    output << "mdl_captured_ingress_sequence";
    label();
    output << ' ' << capture.captured_ingress_sequence << '\n';
    output << "mdl_ingress_ring_used_bytes";
    label();
    output << ' ' << ring_used_bytes << '\n';
    output << "mdl_ingress_ring_capacity_bytes";
    label();
    output << ' ' << ring_capacity_bytes << '\n';
    output << "mdl_ingress_ring_utilization_ratio";
    label();
    output << ' ' << std::setprecision(17)
           << (ring_capacity_bytes == 0U
                   ? 0.0
                   : static_cast<double>(ring_used_bytes) /
                         static_cast<double>(ring_capacity_bytes))
           << '\n';
    output << "mdl_ingress_fatal";
    label();
    output << ' ' << (fatal ? 1 : 0) << '\n';
    for (std::size_t index = 0U; index < kInvalidMessageReasonCount; ++index) {
        output << "mdl_callback_invalid_message_total{service=";
        AppendLabelValue(output, service_name);
        output << ",source_stream_id=\""
               << source_stream_id << "\",reason=\""
               << InvalidReasonName(index) << "\"} "
               << capture.invalid_messages[index] << '\n';
    }
    for (std::size_t index = 0U; index < kCallbackLatencyBucketCount;
         ++index) {
        output << "mdl_callback_duration_bucket_total{service=";
        AppendLabelValue(output, service_name);
        output << ",source_stream_id=\""
               << source_stream_id << "\",log2_ns=\"" << index << "\"} "
               << capture.latency_ns[index] << '\n';
    }
    return output.str();
}

}  // namespace l2flow::ingress
