#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace l2flow::ingress {

enum class InvalidMessageReason : std::size_t {
    NullMessage = 0,
    NullHead,
    WrongHeadSize,
    MessageSmallerThanHead,
    MessageTooLarge,
    NullBody,
    UnexpectedService,
    Count,
};

inline constexpr std::size_t kInvalidMessageReasonCount =
    static_cast<std::size_t>(InvalidMessageReason::Count);
inline constexpr std::size_t kCallbackLatencyBucketCount = 32U;

struct CaptureMetricsSnapshot {
    std::uint64_t callback_invocations = 0U;
    std::uint64_t captured_records = 0U;
    std::uint64_t captured_vendor_bytes = 0U;
    // Sum of the exact RawRecordV1 record_size values corresponding to
    // callback-published ring records. This excludes segment headers.
    std::uint64_t captured_framed_wal_bytes = 0U;
    std::uint64_t callback_reentry = 0U;
    std::uint64_t callback_exceptions = 0U;
    std::uint64_t callbacks_after_stop = 0U;
    std::uint64_t callbacks_after_fatal = 0U;
    std::uint64_t ring_overflow = 0U;
    std::uint64_t captured_ingress_sequence = 0U;
    bool callback_inflight = false;
    std::array<std::uint64_t, kInvalidMessageReasonCount> invalid_messages{};
    std::array<std::uint64_t, kCallbackLatencyBucketCount> latency_ns{};
};

class CaptureMetrics {
public:
    CaptureMetrics() noexcept;

    void IncrementCallbackInvocations() noexcept;
    void IncrementCaptured(std::uint32_t vendor_message_bytes,
                           std::uint64_t ingress_sequence,
                           std::uint64_t framed_wal_bytes = 0U) noexcept;
    void IncrementReentry() noexcept;
    void IncrementException() noexcept;
    void IncrementAfterStop() noexcept;
    void IncrementAfterFatal() noexcept;
    void IncrementRingOverflow() noexcept;
    void IncrementInvalid(InvalidMessageReason reason) noexcept;
    void SetInflight(bool value) noexcept;
    void RecordLatencyNanoseconds(std::uint64_t value) noexcept;

    [[nodiscard]] CaptureMetricsSnapshot Snapshot() const noexcept;

private:
    // Even values are quiescent publications; odd values mean the
    // records/bytes/sequence tuple is being updated. This is deliberately
    // separate from the callback gate so telemetry readers never touch
    // callback-owned non-atomic state.
    std::atomic<std::uint64_t> captured_publication_version_{0U};
    std::atomic<std::uint64_t> callback_invocations_{0U};
    std::atomic<std::uint64_t> captured_records_{0U};
    std::atomic<std::uint64_t> captured_vendor_bytes_{0U};
    std::atomic<std::uint64_t>
        captured_framed_wal_bytes_{0U};
    std::atomic<std::uint64_t> callback_reentry_{0U};
    std::atomic<std::uint64_t> callback_exceptions_{0U};
    std::atomic<std::uint64_t> callbacks_after_stop_{0U};
    std::atomic<std::uint64_t> callbacks_after_fatal_{0U};
    std::atomic<std::uint64_t> ring_overflow_{0U};
    std::atomic<std::uint64_t> captured_ingress_sequence_{0U};
    std::atomic<bool> callback_inflight_{false};
    std::array<std::atomic<std::uint64_t>, kInvalidMessageReasonCount>
        invalid_messages_;
    std::array<std::atomic<std::uint64_t>, kCallbackLatencyBucketCount>
        latency_ns_;
};

std::string RenderPrometheusMetrics(
    std::string_view service_name,
    std::uint32_t source_stream_id,
    const CaptureMetricsSnapshot& capture,
    std::uint64_t ring_used_bytes,
    std::uint64_t ring_capacity_bytes,
    bool fatal);

}  // namespace l2flow::ingress
