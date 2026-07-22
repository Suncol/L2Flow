#include "l2flow/ingress/callback_handler.h"

#include "l2flow/ingress/raw_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <thread>

#include <time.h>

namespace l2flow::ingress {
namespace {

std::uint64_t DurationClockNanoseconds() noexcept {
    struct timespec value {};
    if (::clock_gettime(CLOCK_MONOTONIC, &value) != 0 ||
        value.tv_sec < 0 || value.tv_nsec < 0) {
        return 0U;
    }
    constexpr std::uint64_t billion = 1'000'000'000ULL;
    const auto seconds = static_cast<std::uint64_t>(value.tv_sec);
    if (seconds >
        (std::numeric_limits<std::uint64_t>::max() -
         static_cast<std::uint64_t>(value.tv_nsec)) /
            billion) {
        return 0U;
    }
    return seconds * billion + static_cast<std::uint64_t>(value.tv_nsec);
}

bool ComputeRawFramedRecordBytes(
    std::size_t vendor_body_size,
    std::uint64_t* framed_bytes) noexcept {
    if (framed_bytes == nullptr ||
        vendor_body_size >
            static_cast<std::size_t>(
                std::numeric_limits<std::uint32_t>::max()) -
                kVendorMessageHeadBytes) {
        return false;
    }
    const std::uint64_t payload_end =
        static_cast<std::uint64_t>(
            kRawV1RecordHeaderBytes) +
        static_cast<std::uint64_t>(
            kVendorMessageHeadBytes) +
        static_cast<std::uint64_t>(vendor_body_size);
    const std::uint64_t padding =
        (static_cast<std::uint64_t>(
             kRawV1RecordAlignment) -
         (payload_end %
          static_cast<std::uint64_t>(
              kRawV1RecordAlignment))) %
        static_cast<std::uint64_t>(
            kRawV1RecordAlignment);
    const std::uint64_t record_size =
        payload_end + padding +
        static_cast<std::uint64_t>(
            kRawV1RecordTrailerBytes);
    if (record_size >
        static_cast<std::uint64_t>(
            std::numeric_limits<std::uint32_t>::max())) {
        return false;
    }
    *framed_bytes = record_size;
    return true;
}

class CallbackExit {
public:
    CallbackExit(std::atomic_flag& gate,
                 std::atomic<bool>& inflight,
                 CaptureMetrics& metrics,
                 std::uint64_t started_ns) noexcept
        : gate_(gate),
          inflight_(inflight),
          metrics_(metrics),
          started_ns_(started_ns) {}

    ~CallbackExit() noexcept {
        const std::uint64_t finished_ns = DurationClockNanoseconds();
        if (started_ns_ != 0U && finished_ns >= started_ns_) {
            metrics_.RecordLatencyNanoseconds(finished_ns - started_ns_);
        }
        metrics_.SetInflight(false);
        inflight_.store(false, std::memory_order_release);
        gate_.clear(std::memory_order_release);
    }

    CallbackExit(const CallbackExit&) = delete;
    CallbackExit& operator=(const CallbackExit&) = delete;

private:
    std::atomic_flag& gate_;
    std::atomic<bool>& inflight_;
    CaptureMetrics& metrics_;
    std::uint64_t started_ns_;
};

}  // namespace

CallbackHandler::CallbackHandler(CallbackHandlerConfig config,
                                 ByteRing& ring,
                                 CaptureClock& clock,
                                 l2flow::ops::FatalLatch& fatal,
                                 CaptureMetrics& metrics)
    : config_(config),
      ring_(ring),
      clock_(clock),
      fatal_(fatal),
      metrics_(metrics),
      capture_date_(config.capture_date),
      next_ingress_sequence_(config.first_ingress_sequence),
      captured_sequence_(
          config.first_ingress_sequence == 0U
              ? 0U
              : config.first_ingress_sequence - 1U) {
    const bool frontier_enabled = config_.source_frontier != nullptr;
    if (config_.source_stream_id == 0U ||
        (config_.market_service_id !=
             static_cast<std::uint8_t>(datayes::mdl::MDLSID_MDL_SHL2) &&
         config_.market_service_id !=
             static_cast<std::uint8_t>(datayes::mdl::MDLSID_MDL_SZL2)) ||
        config_.max_message_bytes < l2flow::sdk::kVendorHeadBytes ||
        config_.max_message_bytes != ring_.max_message_bytes() ||
        config_.capture_date == 0U ||
        config_.first_ingress_sequence == 0U ||
        (frontier_enabled &&
         (l2flow::common::IsZeroIdentity(
              config_.frontier_writer_instance) ||
          config_.frontier_generation == 0U ||
          config_.source_frontier_busy_timeout.count() <= 0 ||
          config_.source_frontier_busy_timeout > l2flow::canonical::
              kSourceFrontierMaximumBusyTimeoutV1)) ||
        (!frontier_enabled &&
         (!l2flow::common::IsZeroIdentity(
              config_.frontier_writer_instance) ||
          config_.frontier_generation != 0U))) {
        throw std::invalid_argument("invalid callback handler configuration");
    }
    if (frontier_enabled) {
        l2flow::canonical::SourceFrontierV1 frontier{};
        if (l2flow::canonical::ReadSourceFrontierV1(
                *config_.source_frontier, &frontier) !=
                l2flow::canonical::SourceFrontierErrorV1::kNone ||
            frontier.source_stream_id != config_.source_stream_id ||
            frontier.capture_date != config_.capture_date ||
            frontier.writer_instance !=
                config_.frontier_writer_instance ||
            frontier.generation != config_.frontier_generation ||
            frontier.captured_ingress_sequence !=
                config_.first_ingress_sequence - 1U ||
            frontier.source_state ==
                l2flow::canonical::SourceStateV1::kFatal) {
            throw std::invalid_argument(
                "callback SourceFrontier identity mismatch");
        }
    }
}

void CallbackHandler::OnMDLAPIMessage(
    const datayes::mdl::MDLMessage* message) noexcept {
    CaptureMessage(message);
}

void CallbackHandler::OnMDLSysMessage(
    const datayes::mdl::MDLMessage* message) noexcept {
    CaptureMessage(message);
}

void CallbackHandler::OnMDLSHL2Message(
    const datayes::mdl::MDLMessage* message) noexcept {
    CaptureMessage(message);
}

void CallbackHandler::OnMDLSZL2Message(
    const datayes::mdl::MDLMessage* message) noexcept {
    CaptureMessage(message);
}

void CallbackHandler::SetConnectionEpochHint(std::uint32_t value) noexcept {
    connection_epoch_hint_.store(value, std::memory_order_relaxed);
}

void CallbackHandler::SetCaptureDate(std::uint32_t value) noexcept {
    capture_date_.store(value, std::memory_order_relaxed);
}

void CallbackHandler::BeginStopping() noexcept {
    accepting_.store(false, std::memory_order_release);
}

bool CallbackHandler::Quiesce(std::chrono::milliseconds timeout) noexcept {
    if (timeout < std::chrono::milliseconds::zero()) {
        return false;
    }
    using Clock = std::chrono::steady_clock;
    const Clock::time_point now = Clock::now();
    const auto remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            Clock::time_point::max() - now);
    const Clock::time_point deadline =
        timeout >= remaining
            ? Clock::time_point::max()
            : now + std::chrono::duration_cast<Clock::duration>(
                        timeout);
    for (;;) {
        if (!callback_gate_.test_and_set(std::memory_order_acquire)) {
            const bool is_inflight =
                callback_inflight_.load(std::memory_order_acquire);
            callback_gate_.clear(std::memory_order_release);
            return !is_inflight;
        }
        if (Clock::now() >= deadline) {
            return false;
        }
        std::this_thread::yield();
    }
}

bool CallbackHandler::callback_inflight() const noexcept {
    return callback_inflight_.load(std::memory_order_acquire);
}

std::uint64_t CallbackHandler::captured_sequence() const noexcept {
    return captured_sequence_.load(std::memory_order_acquire);
}

std::uint64_t
CallbackHandler::next_ingress_sequence_when_quiescent() const noexcept {
    return next_ingress_sequence_;
}

void CallbackHandler::CaptureMessage(
    const datayes::mdl::MDLMessage* message) noexcept {
    // This must remain the first interaction with handler state. In
    // particular, a losing callback must not read the sequence or ring.
    if (callback_gate_.test_and_set(std::memory_order_acquire)) {
        metrics_.IncrementReentry();
        static_cast<void>(
            fatal_.trip(l2flow::ops::FatalReason::CALLBACK_REENTRY));
        if (config_.source_frontier != nullptr) {
            static_cast<void>(l2flow::canonical::PublishSourceStateV1(
                config_.source_frontier,
                config_.frontier_writer_instance,
                config_.frontier_generation,
                l2flow::canonical::SourceStateV1::kFatal,
                0U));
        }
        return;
    }

    callback_inflight_.store(true, std::memory_order_release);
    metrics_.SetInflight(true);
    const std::uint64_t started_ns = DurationClockNanoseconds();
    CallbackExit on_exit(
        callback_gate_, callback_inflight_, metrics_, started_ns);

    if (!accepting_.load(std::memory_order_acquire)) {
        metrics_.IncrementAfterStop();
        return;
    }
    if (fatal_.tripped()) {
        metrics_.IncrementAfterFatal();
        return;
    }

    metrics_.IncrementCallbackInvocations();
    std::optional<l2flow::canonical::SourceFrontierCallbackGuardV1>
        frontier_callback;
    if (config_.source_frontier != nullptr) {
        frontier_callback.emplace(
            config_.source_frontier,
            config_.frontier_writer_instance,
            config_.frontier_generation,
            config_.source_frontier_busy_timeout);
        if (frontier_callback->error() !=
            l2flow::canonical::SourceFrontierErrorV1::kNone) {
            static_cast<void>(fatal_.trip(
                l2flow::ops::FatalReason::SOURCE_FRONTIER_FAILURE));
            // This may fail when the identity itself changed, but it is safe
            // to attempt and closes the route-visible page when contention or
            // a state transition prevented callback admission.
            static_cast<void>(l2flow::canonical::PublishSourceStateV1(
                config_.source_frontier,
                config_.frontier_writer_instance,
                config_.frontier_generation,
                l2flow::canonical::SourceStateV1::kFatal,
                0U));
            return;
        }
    }
    try {
        std::uint64_t captured_sequence = 0U;
        if (!CaptureMessageImpl(message, &captured_sequence)) {
            return;
        }
        if (frontier_callback.has_value() &&
            frontier_callback->CompleteCaptured(captured_sequence) !=
                l2flow::canonical::SourceFrontierErrorV1::kNone) {
            static_cast<void>(fatal_.trip(
                l2flow::ops::FatalReason::SOURCE_FRONTIER_FAILURE));
        }
    } catch (...) {
        metrics_.IncrementException();
        static_cast<void>(
            fatal_.trip(l2flow::ops::FatalReason::CALLBACK_EXCEPTION));
    }
}

bool CallbackHandler::CaptureMessageImpl(
    const datayes::mdl::MDLMessage* message,
    std::uint64_t* captured_sequence) {
    if (captured_sequence == nullptr || message == nullptr) {
        metrics_.IncrementInvalid(InvalidMessageReason::NullMessage);
        static_cast<void>(
            fatal_.trip(l2flow::ops::FatalReason::NULL_MESSAGE));
        return false;
    }

    const datayes::mdl::MDLMessageHead* vendor_head = message->GetHead();
    if (vendor_head == nullptr) {
        metrics_.IncrementInvalid(InvalidMessageReason::NullHead);
        static_cast<void>(
            fatal_.trip(l2flow::ops::FatalReason::NULL_VENDOR_HEAD));
        return false;
    }

    l2flow::sdk::VendorHeadBytes head_bytes{};
    std::memcpy(head_bytes.data(), vendor_head, head_bytes.size());
    const l2flow::sdk::VendorHeadView head(head_bytes);
    if (head.head_size() != l2flow::sdk::kVendorHeadBytes) {
        metrics_.IncrementInvalid(InvalidMessageReason::WrongHeadSize);
        static_cast<void>(
            fatal_.trip(l2flow::ops::FatalReason::INVALID_VENDOR_HEADER));
        return false;
    }
    if (head.message_size() < head.head_size()) {
        metrics_.IncrementInvalid(
            InvalidMessageReason::MessageSmallerThanHead);
        static_cast<void>(
            fatal_.trip(l2flow::ops::FatalReason::INVALID_VENDOR_HEADER));
        return false;
    }
    if (head.message_size() > config_.max_message_bytes) {
        metrics_.IncrementInvalid(InvalidMessageReason::MessageTooLarge);
        static_cast<void>(
            fatal_.trip(l2flow::ops::FatalReason::MESSAGE_TOO_LARGE));
        return false;
    }
    if (head.service_id() !=
            static_cast<std::uint8_t>(datayes::mdl::MDLSID_MDL_API) &&
        head.service_id() !=
            static_cast<std::uint8_t>(datayes::mdl::MDLSID_MDL_SYS) &&
        head.service_id() != config_.market_service_id) {
        metrics_.IncrementInvalid(InvalidMessageReason::UnexpectedService);
        static_cast<void>(
            fatal_.trip(l2flow::ops::FatalReason::UNEXPECTED_SERVICE));
        return false;
    }

    // The subtraction is deliberately after both header-size checks.
    const std::uint32_t body_size =
        head.message_size() - static_cast<std::uint32_t>(head.head_size());
    const char* body_pointer = nullptr;
    if (body_size != 0U) {
        body_pointer = message->GetBody();
        if (body_pointer == nullptr) {
            metrics_.IncrementInvalid(InvalidMessageReason::NullBody);
            static_cast<void>(
                fatal_.trip(l2flow::ops::FatalReason::NULL_VENDOR_BODY));
            return false;
        }
    }

    // UINT64_MAX is kept as the exhaustion sentinel so this counter can never
    // wrap and falsely satisfy strict monotonicity after a long run.
    if (next_ingress_sequence_ ==
        std::numeric_limits<std::uint64_t>::max()) {
        static_cast<void>(fatal_.trip(
            l2flow::ops::FatalReason::INGRESS_SEQUENCE_EXHAUSTED));
        return false;
    }

    CaptureMetaV1 metadata{};
    metadata.source_stream_id = config_.source_stream_id;
    metadata.connection_epoch_hint =
        connection_epoch_hint_.load(std::memory_order_relaxed);
    metadata.ingress_sequence = next_ingress_sequence_;
    metadata.recv_monotonic_ns = clock_.MonotonicRawNanoseconds();
    metadata.recv_realtime_ns = clock_.RealtimeNanoseconds();
    metadata.capture_date = capture_date_.load(std::memory_order_relaxed);
    metadata.flags = 0U;

    const std::span<const std::byte> body =
        body_size == 0U
            ? std::span<const std::byte>{}
            : std::span<const std::byte>{
                  reinterpret_cast<const std::byte*>(body_pointer),
                  static_cast<std::size_t>(body_size)};
    std::uint64_t framed_record_bytes = 0U;
    if (!ComputeRawFramedRecordBytes(
            body.size(), &framed_record_bytes) ||
        framed_record_bytes == 0U) {
        static_cast<void>(fatal_.trip(
            l2flow::ops::FatalReason::RING_CORRUPTION));
        return false;
    }
    const ByteRingPushResult pushed = ring_.try_push_copy(
        metadata,
        std::span<const std::byte, l2flow::sdk::kVendorHeadBytes>(head_bytes),
        body);
    if (pushed != ByteRingPushResult::PUBLISHED) {
        if (pushed == ByteRingPushResult::FULL) {
            metrics_.IncrementRingOverflow();
            static_cast<void>(
                fatal_.trip(l2flow::ops::FatalReason::INGRESS_RING_OVERFLOW));
        } else if (pushed == ByteRingPushResult::MESSAGE_TOO_LARGE) {
            metrics_.IncrementInvalid(InvalidMessageReason::MessageTooLarge);
            static_cast<void>(
                fatal_.trip(l2flow::ops::FatalReason::MESSAGE_TOO_LARGE));
        } else {
            static_cast<void>(
                fatal_.trip(l2flow::ops::FatalReason::RING_CORRUPTION));
        }
        return false;
    }

    // Linearization order: ring commit/release first, then publish the
    // captured sequence. A failed push never claims an uncaptured sequence.
    ++next_ingress_sequence_;
    captured_sequence_.store(metadata.ingress_sequence,
                             std::memory_order_release);
    metrics_.IncrementCaptured(
        head.message_size(),
        metadata.ingress_sequence,
        framed_record_bytes);
    *captured_sequence = metadata.ingress_sequence;
    return true;
}

}  // namespace l2flow::ingress
