#include "l2flow/ingress/callback_handler.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>

namespace ingress = l2flow::ingress;
namespace mdl = datayes::mdl;
namespace ops = l2flow::ops;

namespace {

constexpr std::uint64_t kCallbackCount = 10'000'000U;

class FastClock final : public ingress::CaptureClock {
public:
    std::uint64_t MonotonicRawNanoseconds() override {
        return next_++;
    }

    std::uint64_t RealtimeNanoseconds() override {
        return 1'000'000'000U + next_++;
    }

private:
    std::uint64_t next_ = 1U;
};

class ZeroBodyMessage final : public mdl::MDLMessage {
public:
    ZeroBodyMessage() {
        std::memset(&head_, 0, sizeof(head_));
        head_.HeadSize =
            static_cast<std::uint8_t>(
                l2flow::sdk::kVendorHeadBytes);
        head_.MessageSize =
            static_cast<std::uint32_t>(
                l2flow::sdk::kVendorHeadBytes);
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID =
            static_cast<std::uint8_t>(mdl::MDLSID_MDL_SHL2);
        head_.ServiceVersion = 101U;
        head_.MessageID = 24U;
        head_.SequenceID = 1U;
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }

    mdl::MDLMessageHead* GetHead() const override {
        return const_cast<mdl::MDLMessageHead*>(&head_);
    }

    char* GetBody() const override {
        body_was_requested_.store(true, std::memory_order_relaxed);
        return nullptr;
    }

    mdl::MDLMessage* _Copy() const override {
        return nullptr;
    }

    [[nodiscard]] bool body_was_requested() const noexcept {
        return body_was_requested_.load(std::memory_order_relaxed);
    }

private:
    mdl::MDLMessageHead head_{};
    mutable std::atomic<bool> body_was_requested_{false};
};

}  // namespace

int main() {
    constexpr std::size_t ring_bytes = 16U * 1024U * 1024U;
    constexpr std::uint32_t message_bytes =
        static_cast<std::uint32_t>(
            l2flow::sdk::kVendorHeadBytes);

    ingress::ByteRing ring(ring_bytes, message_bytes);
    FastClock clock;
    ops::FatalLatch fatal;
    ingress::CaptureMetrics metrics;
    ingress::CallbackHandlerConfig config;
    config.source_stream_id = 1001U;
    config.market_service_id =
        static_cast<std::uint8_t>(mdl::MDLSID_MDL_SHL2);
    config.max_message_bytes = message_bytes;
    config.capture_date = 20260718U;
    config.first_ingress_sequence = 1U;
    ingress::CallbackHandler handler(
        config, ring, clock, fatal, metrics);
    ZeroBodyMessage message;

    std::atomic<bool> producer_done{false};
    std::atomic<bool> consumer_failed{false};
    std::atomic<std::uint64_t> consumed_records{0U};
    std::thread consumer([&] {
        ingress::ByteRingRecord record(0U);
        std::uint64_t expected_sequence = 1U;
        for (;;) {
            const ingress::ByteRingPopResult result =
                ring.try_pop(record);
            if (result == ingress::ByteRingPopResult::RECORD) {
                const l2flow::sdk::VendorHeadView head(record.head);
                if (record.meta.ingress_sequence !=
                        expected_sequence ||
                    record.meta.source_stream_id != 1001U ||
                    head.head_size() !=
                        l2flow::sdk::kVendorHeadBytes ||
                    head.message_size() != message_bytes ||
                    head.service_id() !=
                        static_cast<std::uint8_t>(
                            mdl::MDLSID_MDL_SHL2) ||
                    !record.body.empty()) {
                    consumer_failed.store(
                        true, std::memory_order_release);
                    return;
                }
                ++expected_sequence;
                consumed_records.fetch_add(
                    1U, std::memory_order_relaxed);
                continue;
            }
            if (result != ingress::ByteRingPopResult::EMPTY) {
                consumer_failed.store(
                    true, std::memory_order_release);
                return;
            }
            if (producer_done.load(std::memory_order_acquire) &&
                ring.used_bytes() == 0U) {
                return;
            }
            std::this_thread::yield();
        }
    });

    std::uint64_t produced = 0U;
    for (; produced < kCallbackCount; ++produced) {
        while (ring.pressure() != ingress::ByteRingPressure::NORMAL &&
               !consumer_failed.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        if (consumer_failed.load(std::memory_order_acquire) ||
            fatal.tripped()) {
            break;
        }
        handler.OnMDLSHL2Message(&message);
    }
    producer_done.store(true, std::memory_order_release);
    consumer.join();
    handler.BeginStopping();

    const ingress::CaptureMetricsSnapshot snapshot =
        metrics.Snapshot();
    const bool passed =
        produced == kCallbackCount &&
        !consumer_failed.load(std::memory_order_acquire) &&
        !fatal.tripped() &&
        !message.body_was_requested() &&
        consumed_records.load(std::memory_order_relaxed) ==
            kCallbackCount &&
        handler.Quiesce(std::chrono::milliseconds(100)) &&
        handler.captured_sequence() == kCallbackCount &&
        handler.next_ingress_sequence_when_quiescent() ==
            kCallbackCount + 1U &&
        snapshot.callback_invocations == kCallbackCount &&
        snapshot.captured_records == kCallbackCount &&
        snapshot.captured_ingress_sequence == kCallbackCount &&
        snapshot.captured_vendor_bytes ==
            kCallbackCount *
                static_cast<std::uint64_t>(message_bytes) &&
        snapshot.callback_reentry == 0U &&
        snapshot.callback_exceptions == 0U &&
        snapshot.ring_overflow == 0U &&
        ring.used_bytes() == 0U;
    if (!passed) {
        std::cerr
            << "10M callback stress failed: produced=" << produced
            << " consumed="
            << consumed_records.load(std::memory_order_relaxed)
            << " fatal=" << static_cast<unsigned>(fatal.reason())
            << " callback_count=" << snapshot.callback_invocations
            << " captured=" << snapshot.captured_records
            << " sequence=" << snapshot.captured_ingress_sequence
            << " ring_used=" << ring.used_bytes() << '\n';
        return 1;
    }

    std::cout
        << "10M serial callbacks preserved exact sequence, bytes, "
           "and SPSC entries\n";
    return 0;
}
