#include "l2flow/ingress/callback_handler.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace ingress = l2flow::ingress;
namespace canonical = l2flow::canonical;
namespace ops = l2flow::ops;
namespace mdl = datayes::mdl;

namespace {

struct TestContext {
    void Expect(bool condition, const std::string& description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }
    int failures = 0;
};

class FakeMessage final : public mdl::MDLMessage {
public:
    FakeMessage(std::uint8_t service_id,
                std::uint16_t message_id,
                std::size_t body_size)
        : body_(body_size, std::byte{0x5a}) {
        std::memset(&head_, 0, sizeof(head_));
        head_.HeadSize =
            static_cast<std::uint8_t>(l2flow::sdk::kVendorHeadBytes);
        head_.MessageSize = static_cast<std::uint32_t>(
            l2flow::sdk::kVendorHeadBytes + body_size);
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID = service_id;
        head_.ServiceVersion = 101U;
        head_.MessageID = message_id;
        head_.SequenceID = 77U;
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }

    mdl::MDLMessageHead* GetHead() const override {
        head_calls.fetch_add(1U, std::memory_order_relaxed);
        if (throw_head) {
            throw std::runtime_error("injected GetHead failure");
        }
        return null_head ? nullptr
                         : const_cast<mdl::MDLMessageHead*>(&head_);
    }

    char* GetBody() const override {
        body_calls.fetch_add(1U, std::memory_order_relaxed);
        if (throw_body) {
            throw std::runtime_error("injected GetBody failure");
        }
        if (null_body || body_.empty()) {
            return nullptr;
        }
        return reinterpret_cast<char*>(
            const_cast<std::byte*>(body_.data()));
    }

    mdl::MDLMessage* _Copy() const override { return nullptr; }

    mdl::MDLMessageHead& head() { return head_; }
    std::vector<std::byte>& body() { return body_; }

    mutable std::atomic<std::uint64_t> head_calls{0U};
    mutable std::atomic<std::uint64_t> body_calls{0U};
    bool null_head = false;
    bool null_body = false;
    bool throw_head = false;
    bool throw_body = false;

private:
    mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
};

class DeterministicClock : public ingress::CaptureClock {
public:
    std::uint64_t MonotonicRawNanoseconds() override {
        return next.fetch_add(2U, std::memory_order_relaxed);
    }
    std::uint64_t RealtimeNanoseconds() override {
        return next.fetch_add(2U, std::memory_order_relaxed) + 1'000'000U;
    }
    std::atomic<std::uint64_t> next{100U};
};

class ThrowingClock final : public ingress::CaptureClock {
public:
    std::uint64_t MonotonicRawNanoseconds() override {
        throw std::runtime_error("injected clock failure");
    }
    std::uint64_t RealtimeNanoseconds() override { return 0U; }
};

class BlockingClock final : public ingress::CaptureClock {
public:
    std::uint64_t MonotonicRawNanoseconds() override {
        std::unique_lock<std::mutex> lock(mutex_);
        entered_ = true;
        condition_.notify_all();
        condition_.wait(lock, [this] { return released_; });
        return 200U;
    }
    std::uint64_t RealtimeNanoseconds() override { return 300U; }

    void WaitUntilEntered() {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_.wait(lock, [this] { return entered_; });
    }
    void Release() {
        std::lock_guard<std::mutex> lock(mutex_);
        released_ = true;
        condition_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable condition_;
    bool entered_ = false;
    bool released_ = false;
};

ingress::CallbackHandlerConfig HandlerConfig(
    std::uint32_t max_message_bytes = 256U,
    std::uint64_t first_sequence = 1U) {
    return {
        1001U,
        static_cast<std::uint8_t>(mdl::MDLSID_MDL_SHL2),
        max_message_bytes,
        20260717U,
        first_sequence,
    };
}

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t seed) {
    std::array<std::byte, Size> value{};
    for (std::size_t index = 0U; index < Size; ++index) {
        value[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(seed + index));
    }
    return value;
}

canonical::SourceFrontierConfigV1 FrontierConfig() {
    canonical::SourceFrontierConfigV1 config;
    config.source_stream_id = 1001U;
    config.capture_date = 20260717U;
    config.stream_day_id = Pattern<16U>(0x10U);
    config.clock_epoch.algorithm = 1U;
    config.clock_epoch.digest = Pattern<32U>(0x20U);
    config.clock_epoch.label = 0x1234U;
    config.writer_instance = Pattern<16U>(0x40U);
    config.generation = 7U;
    config.initial_state = canonical::SourceStateV1::kHealthy;
    return config;
}

canonical::SourceFrontierV1 ReadFrontier(
    TestContext* test,
    const canonical::SourceFrontierPageV1& page) {
    canonical::SourceFrontierV1 frontier;
    test->Expect(
        canonical::ReadSourceFrontierV1(page, &frontier) ==
            canonical::SourceFrontierErrorV1::kNone,
        "SourceFrontier page remains coherently readable");
    return frontier;
}

struct Fixture {
    explicit Fixture(std::uint32_t max_message_bytes = 256U,
                     std::size_t ring_bytes = 4096U,
                     std::uint64_t first_sequence = 1U)
        : ring(ring_bytes, max_message_bytes),
          handler(HandlerConfig(max_message_bytes, first_sequence),
                  ring, clock, fatal, metrics) {}

    ingress::ByteRing ring;
    DeterministicClock clock;
    ops::FatalLatch fatal;
    ingress::CaptureMetrics metrics;
    ingress::CallbackHandler handler;
};

void CheckUnifiedCaptureAndOrder(TestContext* test) {
    Fixture fixture;
    FakeMessage api(
        static_cast<std::uint8_t>(mdl::MDLSID_MDL_API), 1U, 0U);
    FakeMessage sys(
        static_cast<std::uint8_t>(mdl::MDLSID_MDL_SYS), 2U, 3U);
    FakeMessage market(
        static_cast<std::uint8_t>(mdl::MDLSID_MDL_SHL2), 4U, 5U);

    fixture.handler.OnMDLAPIMessage(&api);
    fixture.handler.OnMDLSysMessage(&sys);
    fixture.handler.OnMDLSHL2Message(&market);

    test->Expect(!fixture.fatal.tripped(),
                 "valid API/SYS/market callbacks do not trip fatal");
    test->Expect(fixture.handler.captured_sequence() == 3U,
                 "all callback families share one ingress sequence");

    ingress::ByteRingRecord record(256U);
    const std::uint8_t expected_services[] = {
        static_cast<std::uint8_t>(mdl::MDLSID_MDL_API),
        static_cast<std::uint8_t>(mdl::MDLSID_MDL_SYS),
        static_cast<std::uint8_t>(mdl::MDLSID_MDL_SHL2),
    };
    const std::size_t expected_bodies[] = {0U, 3U, 5U};
    for (std::size_t index = 0U; index < 3U; ++index) {
        test->Expect(fixture.ring.try_pop(record) ==
                         ingress::ByteRingPopResult::RECORD,
                     "captured record is readable in callback order");
        const l2flow::sdk::VendorHeadView head(record.head);
        test->Expect(head.service_id() == expected_services[index],
                     "captured service order is exact");
        test->Expect(record.meta.ingress_sequence == index + 1U,
                     "ring metadata sequence is exact");
        test->Expect(record.body.size() == expected_bodies[index],
                     "complete body length is preserved");
        test->Expect(record.meta.recv_monotonic_ns <
                         record.meta.recv_realtime_ns,
                     "both configured clock observations are captured");
    }
    test->Expect(fixture.ring.try_pop(record) ==
                     ingress::ByteRingPopResult::EMPTY,
                 "ring is empty after consuming every callback");

    const ingress::CaptureMetricsSnapshot metrics =
        fixture.metrics.Snapshot();
    test->Expect(metrics.callback_invocations == 3U &&
                     metrics.captured_records == 3U,
                 "callback and capture counters agree");
    test->Expect(metrics.captured_vendor_bytes ==
                     (23U + 26U + 28U),
                 "vendor byte metric counts head plus complete body");
}

void CheckMalformedMessages(TestContext* test) {
    {
        Fixture fixture;
        fixture.handler.OnMDLSHL2Message(nullptr);
        test->Expect(fixture.fatal.reason() == ops::FatalReason::NULL_MESSAGE,
                     "null message is an explicit fatal");
        test->Expect(fixture.ring.published_position() == 0U,
                     "null message cannot touch the ring");
    }
    {
        Fixture fixture;
        FakeMessage message(4U, 4U, 0U);
        message.null_head = true;
        fixture.handler.OnMDLSHL2Message(&message);
        test->Expect(
            fixture.fatal.reason() == ops::FatalReason::NULL_VENDOR_HEAD,
            "null vendor head is an explicit fatal");
        test->Expect(message.body_calls.load() == 0U,
                     "null head never reads body");
    }
    {
        Fixture fixture;
        FakeMessage message(4U, 4U, 5U);
        message.head().HeadSize = 22U;
        fixture.handler.OnMDLSHL2Message(&message);
        test->Expect(
            fixture.fatal.reason() == ops::FatalReason::INVALID_VENDOR_HEADER,
            "HeadSize != 23 is rejected");
        test->Expect(message.body_calls.load() == 0U,
                     "wrong HeadSize never reads body");
    }
    {
        Fixture fixture;
        FakeMessage message(4U, 4U, 0U);
        message.head().MessageSize = 22U;
        fixture.handler.OnMDLSHL2Message(&message);
        test->Expect(
            fixture.fatal.reason() == ops::FatalReason::INVALID_VENDOR_HEADER,
            "MessageSize below HeadSize is rejected before subtraction");
        test->Expect(message.body_calls.load() == 0U,
                     "underflow candidate never reads body");
    }
    {
        Fixture fixture(64U);
        FakeMessage message(4U, 4U, 42U);
        message.head().MessageSize = 65U;
        fixture.handler.OnMDLSHL2Message(&message);
        test->Expect(
            fixture.fatal.reason() == ops::FatalReason::MESSAGE_TOO_LARGE,
            "configured maximum plus one is rejected");
        test->Expect(message.body_calls.load() == 0U,
                     "oversize message never reads body");
    }
    {
        Fixture fixture;
        FakeMessage message(4U, 4U, 1U);
        message.null_body = true;
        fixture.handler.OnMDLSHL2Message(&message);
        test->Expect(
            fixture.fatal.reason() == ops::FatalReason::NULL_VENDOR_BODY,
            "non-zero body with null pointer is rejected");
    }
    {
        Fixture fixture;
        FakeMessage message(
            static_cast<std::uint8_t>(mdl::MDLSID_MDL_SZL2), 28U, 0U);
        fixture.handler.OnMDLSZL2Message(&message);
        test->Expect(
            fixture.fatal.reason() == ops::FatalReason::UNEXPECTED_SERVICE,
            "other market service cannot enter this stream");
        test->Expect(message.body_calls.load() == 0U,
                     "unexpected service never reads body");
    }
}

void CheckZeroAndMaximumBodies(TestContext* test) {
    Fixture fixture(64U);
    FakeMessage zero(4U, 4U, 0U);
    zero.null_body = true;
    fixture.handler.OnMDLSHL2Message(&zero);
    test->Expect(!fixture.fatal.tripped(),
                 "zero body with null pointer is legal");
    test->Expect(zero.body_calls.load() == 0U,
                 "zero body does not call GetBody");

    FakeMessage maximum(4U, 4U, 64U - 23U);
    fixture.handler.OnMDLSHL2Message(&maximum);
    test->Expect(!fixture.fatal.tripped(),
                 "MessageSize equal to configured maximum is legal");

    ingress::ByteRingRecord record(64U);
    test->Expect(fixture.ring.try_pop(record) ==
                     ingress::ByteRingPopResult::RECORD &&
                     record.body.empty(),
                 "zero body round-trips as an empty span");
    test->Expect(fixture.ring.try_pop(record) ==
                     ingress::ByteRingPopResult::RECORD &&
                     record.body.size() == 41U,
                 "maximum legal body round-trips completely");
}

void CheckExceptionsDoNotCrossAbi(TestContext* test) {
    ingress::ByteRing ring(4096U, 256U);
    ThrowingClock clock;
    ops::FatalLatch fatal;
    ingress::CaptureMetrics metrics;
    ingress::CallbackHandler handler(
        HandlerConfig(), ring, clock, fatal, metrics);
    FakeMessage message(4U, 4U, 0U);

    bool escaped = false;
    try {
        handler.OnMDLSHL2Message(&message);
    } catch (...) {
        escaped = true;
    }
    test->Expect(!escaped, "clock exception never crosses callback ABI");
    test->Expect(fatal.reason() == ops::FatalReason::CALLBACK_EXCEPTION,
                 "clock exception trips CALLBACK_EXCEPTION");
    test->Expect(ring.published_position() == 0U,
                 "exception before push does not publish a record");

    Fixture vendor_exception;
    FakeMessage throwing(4U, 4U, 0U);
    throwing.throw_head = true;
    vendor_exception.handler.OnMDLSHL2Message(&throwing);
    test->Expect(
        vendor_exception.fatal.reason() ==
            ops::FatalReason::CALLBACK_EXCEPTION,
        "vendor accessor exception is contained");
}

void CheckReentryGate(TestContext* test) {
    ingress::ByteRing ring(4096U, 256U);
    BlockingClock clock;
    ops::FatalLatch fatal;
    ingress::CaptureMetrics metrics;
    ingress::CallbackHandler handler(
        HandlerConfig(), ring, clock, fatal, metrics);
    FakeMessage winner(4U, 4U, 0U);
    FakeMessage loser(4U, 4U, 0U);

    std::thread first([&] { handler.OnMDLSHL2Message(&winner); });
    clock.WaitUntilEntered();
    const std::uint64_t published_before = ring.published_position();
    handler.OnMDLSHL2Message(&loser);

    test->Expect(loser.head_calls.load(std::memory_order_relaxed) == 0U,
                 "reentrant loser does not read the vendor message");
    test->Expect(ring.published_position() == published_before,
                 "reentrant loser does not touch SPSC producer position");
    test->Expect(
        fatal.reason() == ops::FatalReason::CALLBACK_REENTRY,
        "reentry trips the first-wins fatal latch");

    clock.Release();
    first.join();
    test->Expect(handler.captured_sequence() == 1U,
                 "only the gate winner consumes an ingress sequence");
    test->Expect(handler.next_ingress_sequence_when_quiescent() == 2U,
                 "reentrant loser cannot modify next sequence");
    test->Expect(metrics.Snapshot().callback_reentry == 1U,
                 "reentry metric is exact");
}

void CheckOverflowAndSequenceExhaustion(TestContext* test) {
    constexpr std::size_t one_zero_body_entry =
        sizeof(ingress::CaptureMetaV1) + 23U + sizeof(std::uint32_t);
    Fixture full(23U, one_zero_body_entry);
    FakeMessage first(4U, 4U, 0U);
    FakeMessage second(4U, 4U, 0U);
    full.handler.OnMDLSHL2Message(&first);
    const std::uint64_t committed = full.ring.published_position();
    full.handler.OnMDLSHL2Message(&second);
    test->Expect(
        full.fatal.reason() == ops::FatalReason::INGRESS_RING_OVERFLOW,
        "full byte ring is fail-stop");
    test->Expect(full.ring.published_position() == committed,
                 "overflow cannot overwrite or advance the ring");
    test->Expect(full.handler.captured_sequence() == 1U &&
                     full.handler.next_ingress_sequence_when_quiescent() == 2U,
                 "failed push never publishes or consumes a sequence");

    Fixture exhausted(
        23U, 256U, std::numeric_limits<std::uint64_t>::max() - 1U);
    FakeMessage penultimate(4U, 4U, 0U);
    FakeMessage sentinel(4U, 4U, 0U);
    exhausted.handler.OnMDLSHL2Message(&penultimate);
    exhausted.handler.OnMDLSHL2Message(&sentinel);
    test->Expect(
        exhausted.fatal.reason() ==
            ops::FatalReason::INGRESS_SEQUENCE_EXHAUSTED,
        "sequence fails before uint64 wrap");
    test->Expect(
        exhausted.handler.captured_sequence() ==
            std::numeric_limits<std::uint64_t>::max() - 1U,
        "last published sequence remains strictly below exhaustion sentinel");
}

void CheckStoppingAndQuiescence(TestContext* test) {
    Fixture fixture;
    fixture.handler.BeginStopping();
    FakeMessage after_stop(4U, 4U, 0U);
    fixture.handler.OnMDLSHL2Message(&after_stop);
    test->Expect(after_stop.head_calls.load() == 0U,
                 "callback after STOPPING does not touch vendor data");
    test->Expect(fixture.handler.Quiesce(std::chrono::milliseconds(50)),
                 "quiescence succeeds after callbacks stop");
    test->Expect(
        fixture.handler.Quiesce(
            std::chrono::milliseconds::max()),
        "maximum quiescence timeout saturates without time-point overflow");
    test->Expect(!fixture.handler.callback_inflight(),
                 "no callback remains inflight after quiescence");
    test->Expect(fixture.metrics.Snapshot().callbacks_after_stop == 1U,
                 "callback after stop is visible in metrics");

    ingress::ByteRing ring(4096U, 256U);
    BlockingClock clock;
    ops::FatalLatch fatal;
    ingress::CaptureMetrics metrics;
    ingress::CallbackHandler handler(
        HandlerConfig(), ring, clock, fatal, metrics);
    FakeMessage in_flight(4U, 4U, 0U);
    std::thread callback([&] { handler.OnMDLSHL2Message(&in_flight); });
    clock.WaitUntilEntered();
    handler.BeginStopping();
    test->Expect(!handler.Quiesce(std::chrono::milliseconds(1)),
                 "quiescence times out while a callback is still active");
    clock.Release();
    callback.join();
    test->Expect(handler.Quiesce(std::chrono::milliseconds(50)),
                 "quiescence succeeds after in-flight callback returns");
}

void CheckSourceFrontierCallbackContract(TestContext* test) {
    const canonical::SourceFrontierConfigV1 frontier_config =
        FrontierConfig();
    canonical::SourceFrontierPageV1 success_page{};
    test->Expect(
        canonical::InitializeSourceFrontierPageV1(
            frontier_config, &success_page) ==
            canonical::SourceFrontierErrorV1::kNone,
        "callback test initializes a real SourceFrontier page");

    ingress::ByteRing success_ring(4096U, 256U);
    DeterministicClock success_clock;
    ops::FatalLatch success_fatal;
    ingress::CaptureMetrics success_metrics;
    ingress::CallbackHandlerConfig success_config = HandlerConfig();
    success_config.source_frontier = &success_page;
    success_config.frontier_writer_instance =
        frontier_config.writer_instance;
    success_config.frontier_generation = frontier_config.generation;
    ingress::CallbackHandler success_handler(
        success_config,
        success_ring,
        success_clock,
        success_fatal,
        success_metrics);
    FakeMessage valid(
        static_cast<std::uint8_t>(mdl::MDLSID_MDL_SHL2),
        4U,
        5U);
    success_handler.OnMDLSHL2Message(&valid);

    const canonical::SourceFrontierV1 success =
        ReadFrontier(test, success_page);
    test->Expect(
        !success_fatal.tripped() &&
            success_handler.captured_sequence() == 1U &&
            success.captured_ingress_sequence == 1U &&
            success.callback_inflight == 0U &&
            success.callback_generation == 2U &&
            success.source_state ==
                canonical::SourceStateV1::kHealthy,
        "successful callback publishes captured before clearing inflight");

    canonical::SourceFrontierPageV1 invalid_page{};
    test->Expect(
        canonical::InitializeSourceFrontierPageV1(
            frontier_config, &invalid_page) ==
            canonical::SourceFrontierErrorV1::kNone,
        "invalid-message frontier page initializes");
    ingress::ByteRing invalid_ring(4096U, 256U);
    DeterministicClock invalid_clock;
    ops::FatalLatch invalid_fatal;
    ingress::CaptureMetrics invalid_metrics;
    ingress::CallbackHandlerConfig invalid_config = HandlerConfig();
    invalid_config.source_frontier = &invalid_page;
    invalid_config.frontier_writer_instance =
        frontier_config.writer_instance;
    invalid_config.frontier_generation = frontier_config.generation;
    ingress::CallbackHandler invalid_handler(
        invalid_config,
        invalid_ring,
        invalid_clock,
        invalid_fatal,
        invalid_metrics);
    invalid_handler.OnMDLSHL2Message(nullptr);

    const canonical::SourceFrontierV1 invalid =
        ReadFrontier(test, invalid_page);
    test->Expect(
        invalid_fatal.reason() == ops::FatalReason::NULL_MESSAGE &&
            invalid.captured_ingress_sequence == 0U &&
            invalid.callback_inflight == 0U &&
            invalid.source_state == canonical::SourceStateV1::kFatal,
        "invalid callback cannot leave an apparently healthy frontier");

    canonical::SourceFrontierPageV1 exception_page{};
    test->Expect(
        canonical::InitializeSourceFrontierPageV1(
            frontier_config, &exception_page) ==
            canonical::SourceFrontierErrorV1::kNone,
        "unfinished-callback frontier page initializes");
    ingress::ByteRing exception_ring(4096U, 256U);
    ThrowingClock exception_clock;
    ops::FatalLatch exception_fatal;
    ingress::CaptureMetrics exception_metrics;
    ingress::CallbackHandlerConfig exception_config = HandlerConfig();
    exception_config.source_frontier = &exception_page;
    exception_config.frontier_writer_instance =
        frontier_config.writer_instance;
    exception_config.frontier_generation = frontier_config.generation;
    ingress::CallbackHandler exception_handler(
        exception_config,
        exception_ring,
        exception_clock,
        exception_fatal,
        exception_metrics);
    FakeMessage unfinished(
        static_cast<std::uint8_t>(mdl::MDLSID_MDL_SHL2),
        4U,
        0U);
    exception_handler.OnMDLSHL2Message(&unfinished);

    const canonical::SourceFrontierV1 exception =
        ReadFrontier(test, exception_page);
    test->Expect(
        exception_fatal.reason() ==
                ops::FatalReason::CALLBACK_EXCEPTION &&
            exception.captured_ingress_sequence == 0U &&
            exception.callback_inflight == 0U &&
            exception.source_state == canonical::SourceStateV1::kFatal,
        "callback exiting without CompleteCaptured fail-stops the source");
}

}  // namespace

int main() {
    TestContext test;
    CheckUnifiedCaptureAndOrder(&test);
    CheckMalformedMessages(&test);
    CheckZeroAndMaximumBodies(&test);
    CheckExceptionsDoNotCrossAbi(&test);
    CheckReentryGate(&test);
    CheckOverflowAndSequenceExhaustion(&test);
    CheckStoppingAndQuiescence(&test);
    CheckSourceFrontierCallbackContract(&test);

    if (test.failures != 0) {
        std::cerr << test.failures << " phase-1 callback test(s) failed\n";
        return 1;
    }
    std::cout << "phase-1 callback checks passed\n";
    return 0;
}
