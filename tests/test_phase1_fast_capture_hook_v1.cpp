#include "l2flow/ingress/callback_handler.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <span>
#include <string_view>

namespace ingress = l2flow::ingress;
namespace canonical = l2flow::canonical;
namespace mdl = datayes::mdl;
namespace ops = l2flow::ops;

namespace {

constexpr std::uint32_t kSourceStreamId = 1001U;
constexpr std::uint32_t kCaptureDate = 20260723U;
constexpr std::size_t kBodyBytes = 4U;
constexpr std::uint32_t kMessageBytes =
    static_cast<std::uint32_t>(
        l2flow::sdk::kVendorHeadBytes + kBodyBytes);
constexpr std::size_t kOneEntryBytes =
    sizeof(ingress::CaptureMetaV1) + kMessageBytes +
    ingress::kEntryCommitLengthBytes;

struct TestContext final {
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    int failures = 0;
};

class FixedClock final : public ingress::CaptureClock {
public:
    std::uint64_t MonotonicRawNanoseconds() override {
        return 123U;
    }

    std::uint64_t RealtimeNanoseconds() override {
        return 456U;
    }
};

class FatalFrontierClock final : public ingress::CaptureClock {
public:
    FatalFrontierClock(
        canonical::SourceFrontierPageV1* page,
        l2flow::common::Identity128 writer_instance,
        std::uint64_t generation) noexcept
        : page_(page),
          writer_instance_(writer_instance),
          generation_(generation) {}

    std::uint64_t MonotonicRawNanoseconds() override {
        static_cast<void>(canonical::PublishSourceStateV1(
            page_,
            writer_instance_,
            generation_,
            canonical::SourceStateV1::kFatal,
            0U));
        return 123U;
    }

    std::uint64_t RealtimeNanoseconds() override {
        return 456U;
    }

private:
    canonical::SourceFrontierPageV1* page_ = nullptr;
    l2flow::common::Identity128 writer_instance_{};
    std::uint64_t generation_ = 0U;
};

class FakeMessage final : public mdl::MDLMessage {
public:
    explicit FakeMessage(std::uint8_t service_id) {
        std::memset(&head_, 0, sizeof(head_));
        head_.HeadSize = static_cast<std::uint8_t>(
            l2flow::sdk::kVendorHeadBytes);
        head_.MessageSize = kMessageBytes;
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID = service_id;
        head_.ServiceVersion = 101U;
        head_.MessageID = 24U;
        head_.SequenceID = 77U;
        body_ = {
            std::byte{0x11},
            std::byte{0x22},
            std::byte{0x33},
            std::byte{0x44},
        };
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }

    mdl::MDLMessageHead* GetHead() const override {
        return const_cast<mdl::MDLMessageHead*>(&head_);
    }

    char* GetBody() const override {
        return reinterpret_cast<char*>(
            const_cast<std::byte*>(body_.data()));
    }

    mdl::MDLMessage* _Copy() const override { return nullptr; }

    [[nodiscard]] std::array<std::byte,
                             l2flow::sdk::kVendorHeadBytes>
    HeadBytes() const noexcept {
        std::array<std::byte, l2flow::sdk::kVendorHeadBytes> bytes{};
        std::memcpy(bytes.data(), &head_, bytes.size());
        return bytes;
    }

    [[nodiscard]] const std::array<std::byte, kBodyBytes>&
    body() const noexcept {
        return body_;
    }

private:
    mdl::MDLMessageHead head_{};
    std::array<std::byte, kBodyBytes> body_{};
};

struct SinkState final {
    ingress::ByteRing* raw_ring = nullptr;
    ingress::FastCapturePublishResultV1 result =
        ingress::FastCapturePublishResultV1::kPublished;
    std::uint64_t calls = 0U;
    std::uint64_t invalidations = 0U;
    bool raw_commit_seen = false;
    const canonical::SourceFrontierPageV1* source_frontier = nullptr;
    bool source_frontier_commit_seen = false;
    ingress::CaptureMetaV1 meta{};
    std::array<std::byte, l2flow::sdk::kVendorHeadBytes> head{};
    std::array<std::byte, kBodyBytes> body{};
    std::size_t body_size = 0U;
};

ingress::FastCapturePublishResultV1 CaptureFastCopy(
    void* context,
    const ingress::CaptureMetaV1& metadata,
    std::span<const std::byte, l2flow::sdk::kVendorHeadBytes> head,
    std::span<const std::byte> body) noexcept {
    auto& state = *static_cast<SinkState*>(context);
    ++state.calls;
    state.raw_commit_seen =
        state.raw_ring != nullptr &&
        state.raw_ring->published_position() == kOneEntryBytes;
    canonical::SourceFrontierV1 frontier{};
    state.source_frontier_commit_seen =
        state.source_frontier != nullptr &&
        canonical::ReadSourceFrontierV1(
            *state.source_frontier, &frontier) ==
            canonical::SourceFrontierErrorV1::kNone &&
        frontier.captured_ingress_sequence ==
            metadata.ingress_sequence &&
        frontier.callback_inflight == 0U;
    state.meta = metadata;
    std::memcpy(state.head.data(), head.data(), head.size());
    state.body_size = body.size();
    if (body.size() == state.body.size()) {
        std::memcpy(state.body.data(), body.data(), body.size());
    }
    return state.result;
}

void InvalidateFastGeneration(
    void* context,
    std::uint32_t,
    std::uint64_t) noexcept {
    auto& state = *static_cast<SinkState*>(context);
    ++state.invalidations;
}

ingress::CallbackHandlerConfig HandlerConfig(SinkState* sink) {
    ingress::CallbackHandlerConfig config;
    config.source_stream_id = kSourceStreamId;
    config.market_service_id =
        static_cast<std::uint8_t>(mdl::MDLSID_MDL_SHL2);
    config.max_message_bytes = kMessageBytes;
    config.capture_date = kCaptureDate;
    config.first_ingress_sequence = 41U;
    config.fast_capture_sink = {
        sink,
        &CaptureFastCopy,
        &InvalidateFastGeneration,
    };
    return config;
}

void CheckExactCopyAfterRawCommit(TestContext* test) {
    canonical::SourceFrontierConfigV1 frontier_config{};
    frontier_config.source_stream_id = kSourceStreamId;
    frontier_config.capture_date = kCaptureDate;
    frontier_config.stream_day_id[0U] = std::byte{1U};
    frontier_config.clock_epoch.algorithm = 1U;
    frontier_config.clock_epoch.digest[0U] = std::byte{2U};
    frontier_config.clock_epoch.label = 3U;
    frontier_config.writer_instance[0U] = std::byte{4U};
    frontier_config.generation = 1U;
    frontier_config.initial_ingress_sequence = 40U;
    frontier_config.initial_global_wal_pos = 1U;
    frontier_config.initial_state = canonical::SourceStateV1::kHealthy;
    canonical::SourceFrontierPageV1 frontier_page{};
    test->Expect(
        canonical::InitializeSourceFrontierPageV1(
            frontier_config, &frontier_page) ==
            canonical::SourceFrontierErrorV1::kNone,
        "Fast hook test initializes SourceFrontier");

    ingress::ByteRing ring(4096U, kMessageBytes);
    FixedClock clock;
    ops::FatalLatch fatal;
    ingress::CaptureMetrics metrics;
    SinkState sink{&ring};
    sink.source_frontier = &frontier_page;
    ingress::CallbackHandlerConfig config = HandlerConfig(&sink);
    config.source_frontier = &frontier_page;
    config.frontier_writer_instance = frontier_config.writer_instance;
    config.frontier_generation = frontier_config.generation;
    ingress::CallbackHandler handler(
        config, ring, clock, fatal, metrics);
    handler.SetConnectionEpochHint(9U);

    FakeMessage message(
        static_cast<std::uint8_t>(mdl::MDLSID_MDL_SHL2));
    const auto expected_head = message.HeadBytes();
    const auto expected_body = message.body();
    handler.OnMDLSHL2Message(&message);

    test->Expect(!fatal.tripped(), "valid callback remains healthy");
    test->Expect(sink.calls == 1U, "valid callback invokes Fast sink once");
    test->Expect(
        sink.raw_commit_seen,
        "Fast sink observes the Raw ring commit before publication");
    test->Expect(
        sink.source_frontier_commit_seen,
        "Fast sink observes SourceFrontier capture commit before publication");
    test->Expect(
        sink.meta.source_stream_id == kSourceStreamId &&
            sink.meta.connection_epoch_hint == 9U &&
            sink.meta.ingress_sequence == 41U &&
            sink.meta.recv_monotonic_ns == 123U &&
            sink.meta.recv_realtime_ns == 456U &&
            sink.meta.capture_date == kCaptureDate &&
            sink.meta.flags == 0U,
        "Fast sink receives exact capture metadata");
    test->Expect(sink.head == expected_head,
                 "Fast sink receives the exact vendor head");
    test->Expect(
        sink.body_size == expected_body.size() &&
            sink.body == expected_body,
        "Fast sink receives the exact vendor body");

    ingress::ByteRingRecord raw_record(kBodyBytes);
    test->Expect(
        ring.try_pop(raw_record) ==
                ingress::ByteRingPopResult::RECORD &&
            raw_record.meta.ingress_sequence == 41U &&
            raw_record.head == expected_head &&
            raw_record.body.size() == expected_body.size() &&
            std::memcmp(raw_record.body.data(),
                        expected_body.data(),
                        expected_body.size()) == 0,
        "Raw ring retains the same committed message");
}

void CheckRejectedMessagesSkipFastSink(TestContext* test) {
    {
        ingress::ByteRing ring(4096U, kMessageBytes);
        FixedClock clock;
        ops::FatalLatch fatal;
        ingress::CaptureMetrics metrics;
        SinkState sink{&ring};
        ingress::CallbackHandler handler(
            HandlerConfig(&sink), ring, clock, fatal, metrics);
        FakeMessage invalid(
            static_cast<std::uint8_t>(mdl::MDLSID_MDL_SZL2));

        handler.OnMDLSHL2Message(&invalid);

        test->Expect(
            sink.calls == 0U &&
                sink.invalidations == 1U &&
                handler.captured_sequence() == 40U &&
                ring.published_position() == 0U,
            "invalid message invalidates Fast without publishing bytes");
    }

    {
        ingress::ByteRing ring(kOneEntryBytes, kMessageBytes);
        FixedClock clock;
        ops::FatalLatch fatal;
        ingress::CaptureMetrics metrics;
        SinkState sink{&ring};
        ingress::CallbackHandler handler(
            HandlerConfig(&sink), ring, clock, fatal, metrics);
        FakeMessage first(
            static_cast<std::uint8_t>(mdl::MDLSID_MDL_SHL2));
        FakeMessage overflow(
            static_cast<std::uint8_t>(mdl::MDLSID_MDL_SHL2));

        handler.OnMDLSHL2Message(&first);
        handler.OnMDLSHL2Message(&overflow);

        test->Expect(
            sink.calls == 1U &&
                sink.invalidations == 1U &&
                handler.captured_sequence() == 41U &&
                ring.published_position() == kOneEntryBytes,
            "Raw ring full invalidates Fast without publishing rejected bytes");
    }
}

void CheckFrontierFailureInvalidatesFast(TestContext* test) {
    canonical::SourceFrontierConfigV1 frontier_config{};
    frontier_config.source_stream_id = kSourceStreamId;
    frontier_config.capture_date = kCaptureDate;
    frontier_config.stream_day_id[0U] = std::byte{1U};
    frontier_config.clock_epoch.algorithm = 1U;
    frontier_config.clock_epoch.digest[0U] = std::byte{2U};
    frontier_config.writer_instance[0U] = std::byte{4U};
    frontier_config.generation = 1U;
    frontier_config.initial_ingress_sequence = 40U;
    frontier_config.initial_global_wal_pos = 1U;
    frontier_config.initial_state = canonical::SourceStateV1::kHealthy;
    canonical::SourceFrontierPageV1 frontier_page{};
    test->Expect(
        canonical::InitializeSourceFrontierPageV1(
            frontier_config, &frontier_page) ==
            canonical::SourceFrontierErrorV1::kNone,
        "frontier-failure test initializes SourceFrontier");

    ingress::ByteRing ring(4096U, kMessageBytes);
    FatalFrontierClock clock(
        &frontier_page,
        frontier_config.writer_instance,
        frontier_config.generation);
    ops::FatalLatch fatal;
    ingress::CaptureMetrics metrics;
    SinkState sink{&ring};
    ingress::CallbackHandlerConfig config = HandlerConfig(&sink);
    config.source_frontier = &frontier_page;
    config.frontier_writer_instance = frontier_config.writer_instance;
    config.frontier_generation = frontier_config.generation;
    ingress::CallbackHandler handler(
        config, ring, clock, fatal, metrics);
    FakeMessage message(
        static_cast<std::uint8_t>(mdl::MDLSID_MDL_SHL2));

    handler.OnMDLSHL2Message(&message);

    canonical::SourceFrontierV1 frontier{};
    test->Expect(
        fatal.tripped() &&
            canonical::ReadSourceFrontierV1(
                frontier_page, &frontier) ==
                canonical::SourceFrontierErrorV1::kNone &&
            frontier.source_state == canonical::SourceStateV1::kFatal &&
            frontier.captured_ingress_sequence == 40U,
        "SourceFrontier completion failure fail-stops the Raw source");
    test->Expect(
        sink.calls == 0U && sink.invalidations != 0U &&
            ring.published_position() == kOneEntryBytes,
        "committed Raw bytes are not exposed through a failed Fast generation");
}

void CheckFastFullPreservesRawCapture(TestContext* test) {
    ingress::ByteRing ring(4096U, kMessageBytes);
    FixedClock clock;
    ops::FatalLatch fatal;
    ingress::CaptureMetrics metrics;
    SinkState sink{&ring};
    sink.result = ingress::FastCapturePublishResultV1::kFull;
    ingress::CallbackHandler handler(
        HandlerConfig(&sink), ring, clock, fatal, metrics);
    FakeMessage message(
        static_cast<std::uint8_t>(mdl::MDLSID_MDL_SHL2));

    handler.OnMDLSHL2Message(&message);

    const ingress::CaptureMetricsSnapshot snapshot = metrics.Snapshot();
    ingress::ByteRingRecord record(kBodyBytes);
    test->Expect(
        sink.calls == 1U && !fatal.tripped(),
        "Fast sink full is isolated from Raw fatal state");
    test->Expect(
        handler.captured_sequence() == 41U &&
            handler.next_ingress_sequence_when_quiescent() == 42U &&
            snapshot.captured_records == 1U &&
            snapshot.captured_ingress_sequence == 41U,
        "Fast sink full preserves Raw captured sequence and metrics");
    test->Expect(
        ring.try_pop(record) == ingress::ByteRingPopResult::RECORD &&
            record.meta.ingress_sequence == 41U,
        "Fast sink full preserves the committed Raw record");
}

}  // namespace

int main() {
    TestContext test;
    CheckExactCopyAfterRawCommit(&test);
    CheckRejectedMessagesSkipFastSink(&test);
    CheckFrontierFailureInvalidatesFast(&test);
    CheckFastFullPreservesRawCapture(&test);

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Fast capture hook test(s) failed\n";
        return 1;
    }
    std::cout << "phase-1 Fast capture hook checks passed\n";
    return 0;
}
