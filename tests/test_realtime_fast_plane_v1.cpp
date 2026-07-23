#include "l2flow/ingress/capture_meta.h"
#include "l2flow/ingress/fast_capture_sink_v1.h"
#include "l2flow/market/instrument_history_v1.h"
#include "l2flow/market/instrument_registry.h"
#include "l2flow/market/market_types_v1.h"
#include "l2flow/runtime/realtime_fast_plane_v1.h"
#include "l2flow/sdk/subscription_manifest.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace ingress = l2flow::ingress;
namespace market = l2flow::market;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;

namespace {

using namespace std::chrono_literals;

constexpr std::uint32_t kTradeDate = 20260723U;
constexpr std::uint32_t kShanghaiTickSource = 1002U;
constexpr std::uint32_t kShenzhenTickSource = 2002U;
constexpr std::uint32_t kShanghaiInstrument = 16U;
constexpr std::uint32_t kSecondShanghaiInstrument = 17U;
constexpr std::uint32_t kShenzhenInstrument = 18U;
constexpr std::size_t kShanghaiTickFixedBytes = 70U;
constexpr std::size_t kShenzhenOrderFixedBytes = 58U;

class TestContext final {
public:
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    [[nodiscard]] int failures() const noexcept {
        return failures_;
    }

private:
    int failures_ = 0;
};

class WireWriter final {
public:
    explicit WireWriter(std::size_t fixed_bytes)
        : bytes_(fixed_bytes, std::byte{0U}) {}

    void StoreU16(std::size_t offset, std::uint16_t value) {
        StoreUnsigned(offset, value);
    }

    void StoreU32(std::size_t offset, std::uint32_t value) {
        StoreUnsigned(offset, value);
    }

    void StoreU64(std::size_t offset, std::uint64_t value) {
        StoreUnsigned(offset, value);
    }

    void StoreString(std::size_t descriptor, std::string_view value) {
        const std::size_t start = bytes_.size();
        StoreU16(
            descriptor,
            static_cast<std::uint16_t>(value.size()));
        StoreU32(
            descriptor + sizeof(std::uint16_t),
            value.empty()
                ? 0U
                : static_cast<std::uint32_t>(start - descriptor));
        const std::span<const char> characters(value.data(), value.size());
        const std::span<const std::byte> encoded =
            std::as_bytes(characters);
        bytes_.insert(bytes_.end(), encoded.begin(), encoded.end());
    }

    [[nodiscard]] std::vector<std::byte> Take() && {
        return std::move(bytes_);
    }

private:
    template <typename Unsigned>
    void StoreUnsigned(std::size_t offset, Unsigned value) {
        static_assert(std::is_unsigned_v<Unsigned>);
        for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
            bytes_[offset + index] = static_cast<std::byte>(
                (value >> (index * 8U)) & static_cast<Unsigned>(0xffU));
        }
    }

    std::vector<std::byte> bytes_;
};

[[nodiscard]] std::vector<std::byte> Bytes(std::string_view value) {
    const std::span<const char> characters(value.data(), value.size());
    const std::span<const std::byte> encoded = std::as_bytes(characters);
    return std::vector<std::byte>(encoded.begin(), encoded.end());
}

[[nodiscard]] std::unique_ptr<market::InstrumentRegistryV1>
MakeRegistry(TestContext* test) {
    std::array<market::InstrumentRegistryEntryV1, 3U> entries{};

    entries[0].instrument_id = kShanghaiInstrument;
    entries[0].key.market = market::MarketV1::kShanghai;
    entries[0].key.security_id = Bytes("600000");
    entries[0].quantity_unit = market::QuantityUnitV1::kShare;
    entries[0].security_type = market::SecurityTypeV1::kEquity;
    entries[0].asset_scope = market::AssetScopeV1::kDocumentedCore;

    entries[1].instrument_id = kSecondShanghaiInstrument;
    entries[1].key.market = market::MarketV1::kShanghai;
    entries[1].key.security_id = Bytes("600001");
    entries[1].quantity_unit = market::QuantityUnitV1::kShare;
    entries[1].security_type = market::SecurityTypeV1::kEquity;
    entries[1].asset_scope = market::AssetScopeV1::kDocumentedCore;

    entries[2].instrument_id = kShenzhenInstrument;
    entries[2].key.market = market::MarketV1::kShenzhen;
    entries[2].key.security_id_source = Bytes("102");
    entries[2].key.security_id = Bytes("000001");
    entries[2].quantity_unit = market::QuantityUnitV1::kShare;
    entries[2].security_type = market::SecurityTypeV1::kEquity;
    entries[2].asset_scope = market::AssetScopeV1::kDocumentedCore;

    std::unique_ptr<market::InstrumentRegistryV1> registry;
    const auto error = market::InstrumentRegistryV1::Create(
        1U, entries, &registry);
    test->Expect(
        error == market::InstrumentRegistryCreateErrorV1::kNone &&
            registry != nullptr,
        "instrument registry creates");
    return registry;
}

[[nodiscard]] runtime::RealtimeFastPlaneConfigV1 FastConfig(
    std::size_t input_ring_capacity = 1U << 20U,
    std::uint32_t max_message_bytes = 4096U) {
    runtime::RealtimeFastPlaneConfigV1 config{};
    config.capture_date = kTradeDate;
    config.trade_date = kTradeDate;
    config.max_message_bytes = max_message_bytes;
    config.input_ring_capacity_bytes_per_source = input_ring_capacity;
    for (std::size_t slot = 0U; slot < config.source_kinds.size(); ++slot) {
        config.history.source_stream_ids[slot] =
            sdk::GetIngressSpec(config.source_kinds[slot]).source_stream_id;
    }
    config.history.physical_worker_count = 2U;
    config.history.queue_capacity = 64U;
    config.history.maximum_inflight_per_source = 1024U;
    config.history.chunk_record_capacity = 8U;
    config.history.maximum_records_per_query = 1024U;
    config.history.maximum_records_per_logical_shard = 100'000U;
    config.history.maximum_instruments_per_logical_shard = 1024U;
    config.history.maximum_owned_payload_bytes_per_logical_shard =
        64U * 1024U * 1024U;
    return config;
}

[[nodiscard]] std::unique_ptr<runtime::RealtimeFastPlaneRuntimeV1>
MakeRuntime(
    TestContext* test,
    const market::InstrumentRegistryV1* registry,
    runtime::RealtimeFastPlaneConfigV1 config = FastConfig()) {
    std::unique_ptr<runtime::RealtimeFastPlaneRuntimeV1> fast_plane;
    const auto error = runtime::RealtimeFastPlaneRuntimeV1::Create(
        std::move(config), registry, &fast_plane);
    test->Expect(
        error == runtime::RealtimeFastPlaneCreateErrorV1::kNone &&
            fast_plane != nullptr,
        "realtime fast plane creates");
    return fast_plane;
}

[[nodiscard]] std::vector<std::byte> ShanghaiTickBody(
    std::uint64_t sequence,
    std::string_view security_id = "600000") {
    WireWriter writer(kShanghaiTickFixedBytes);
    writer.StoreU64(0U, sequence);
    writer.StoreU32(8U, 1U);
    writer.StoreU32(18U, 93'000'000U);
    writer.StoreU64(28U, 10'000U + sequence);
    writer.StoreU64(36U, 0U);
    writer.StoreU32(44U, 12'345U);
    writer.StoreU64(48U, 100U + sequence);
    writer.StoreU64(56U, 0U);
    writer.StoreString(12U, security_id);
    writer.StoreString(22U, "A");
    writer.StoreString(64U, "B");
    return std::move(writer).Take();
}

[[nodiscard]] std::vector<std::byte> ShenzhenOrderBody(
    std::uint64_t sequence) {
    WireWriter writer(kShenzhenOrderFixedBytes);
    writer.StoreU32(0U, 12U);
    writer.StoreU64(4U, 20'000U + sequence);
    writer.StoreU64(30U, 123'456U);
    writer.StoreU64(38U, 200U + sequence);
    writer.StoreU32(46U, 49U);
    writer.StoreU32(50U, 93'000'123U);
    writer.StoreU32(54U, 50U);
    writer.StoreString(12U, "010");
    writer.StoreString(18U, "000001");
    writer.StoreString(24U, "102");
    return std::move(writer).Take();
}

[[nodiscard]] sdk::VendorHeadBytes VendorHead(
    std::uint8_t service_id,
    std::uint16_t message_id,
    std::size_t body_bytes,
    std::uint64_t sequence) {
    WireWriter writer(sdk::kVendorHeadBytes);
    writer.StoreU16(7U, 101U);
    writer.StoreU16(9U, message_id);
    writer.StoreU32(11U, 93'000'000U);
    writer.StoreU64(15U, 100'000U + sequence);
    std::vector<std::byte> encoded = std::move(writer).Take();
    encoded[0U] = static_cast<std::byte>(sdk::kVendorHeadBytes);
    encoded[5U] = std::byte{1U};
    encoded[6U] = static_cast<std::byte>(service_id);

    const std::uint32_t message_bytes = static_cast<std::uint32_t>(
        sdk::kVendorHeadBytes + body_bytes);
    for (std::size_t index = 0U; index < sizeof(message_bytes); ++index) {
        encoded[1U + index] = static_cast<std::byte>(
            (message_bytes >> (index * 8U)) & 0xffU);
    }

    sdk::VendorHeadBytes result{};
    for (std::size_t index = 0U; index < result.size(); ++index) {
        result[index] = encoded[index];
    }
    return result;
}

[[nodiscard]] ingress::CaptureMetaV1 CaptureMeta(
    std::uint32_t source_stream_id,
    std::uint64_t sequence) {
    ingress::CaptureMetaV1 metadata{};
    metadata.source_stream_id = source_stream_id;
    metadata.ingress_sequence = sequence;
    metadata.recv_realtime_ns = 1'000'000U + sequence;
    metadata.recv_monotonic_ns = 2'000'000U + sequence;
    metadata.capture_date = kTradeDate;
    return metadata;
}

[[nodiscard]] ingress::FastCapturePublishResultV1 PublishShanghai(
    const ingress::FastCaptureSinkRefV1& sink,
    std::uint64_t sequence,
    std::string_view security_id = "600000") {
    const std::vector<std::byte> body =
        ShanghaiTickBody(sequence, security_id);
    const sdk::VendorHeadBytes head =
        VendorHead(4U, 24U, body.size(), sequence);
    return sink.PublishCopy(
        CaptureMeta(kShanghaiTickSource, sequence),
        std::span<const std::byte, sdk::kVendorHeadBytes>(head),
        body);
}

[[nodiscard]] ingress::FastCapturePublishResultV1 PublishShenzhen(
    const ingress::FastCaptureSinkRefV1& sink,
    std::uint64_t sequence) {
    const std::vector<std::byte> body = ShenzhenOrderBody(sequence);
    const sdk::VendorHeadBytes head =
        VendorHead(6U, 33U, body.size(), sequence);
    return sink.PublishCopy(
        CaptureMeta(kShenzhenTickSource, sequence),
        std::span<const std::byte, sdk::kVendorHeadBytes>(head),
        body);
}

template <typename Predicate>
[[nodiscard]] bool WaitUntil(
    Predicate predicate,
    std::chrono::milliseconds timeout = 2s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::yield();
    }
    return true;
}

void ExpectStrictSourceOrder(
    TestContext* test,
    const std::vector<market::InstrumentHistoryRecordHandleV1>& records,
    std::uint64_t expected_count,
    std::string_view description) {
    bool ordered = records.size() == expected_count;
    for (std::size_t index = 0U; ordered && index < records.size(); ++index) {
        ordered = records[index] &&
                  records[index]->source_sequence() == index + 1U;
    }
    test->Expect(ordered, description);
}

void TestParallelDecodeIntoPrivateHistory(TestContext* test) {
    auto registry = MakeRegistry(test);
    if (registry == nullptr) {
        return;
    }
    auto fast_plane = MakeRuntime(test, registry.get());
    if (fast_plane == nullptr) {
        return;
    }

    constexpr std::uint64_t kRecordsPerSource = 32U;
    const ingress::FastCaptureSinkRefV1 sink = fast_plane->capture_sink();
    std::atomic<bool> producers_ok{true};
    std::thread shanghai([&]() {
        for (std::uint64_t sequence = 1U;
             sequence <= kRecordsPerSource; ++sequence) {
            if (PublishShanghai(sink, sequence) !=
                ingress::FastCapturePublishResultV1::kPublished) {
                producers_ok.store(false, std::memory_order_relaxed);
                return;
            }
        }
    });
    std::thread shenzhen([&]() {
        for (std::uint64_t sequence = 1U;
             sequence <= kRecordsPerSource; ++sequence) {
            if (PublishShenzhen(sink, sequence) !=
                ingress::FastCapturePublishResultV1::kPublished) {
                producers_ok.store(false, std::memory_order_relaxed);
                return;
            }
        }
    });
    shanghai.join();
    shenzhen.join();
    test->Expect(
        producers_ok.load(std::memory_order_relaxed),
        "two source callbacks publish concurrently");

    const bool history_ready = WaitUntil([&fast_plane]() {
        const auto snapshot = fast_plane->Snapshot();
        return snapshot.sources[1U].history_frontier.acknowledged_ticket ==
                   kRecordsPerSource &&
               snapshot.sources[3U].history_frontier.acknowledged_ticket ==
                   kRecordsPerSource;
    });
    test->Expect(history_ready, "both Fast histories become query-visible");
    const auto live_snapshot = fast_plane->Snapshot();
    test->Expect(
        live_snapshot.sources[1U].captured_records == kRecordsPerSource &&
            live_snapshot.sources[1U].decoded_records == kRecordsPerSource &&
            live_snapshot.sources[1U].history_submissions ==
                kRecordsPerSource &&
            live_snapshot.sources[1U].last_processed_sequence ==
                kRecordsPerSource &&
            !live_snapshot.sources[1U].worker_exited,
        "Shanghai source reaches its exact decoded prefix");
    test->Expect(
        live_snapshot.sources[3U].captured_records == kRecordsPerSource &&
            live_snapshot.sources[3U].decoded_records == kRecordsPerSource &&
            live_snapshot.sources[3U].history_submissions ==
                kRecordsPerSource &&
            live_snapshot.sources[3U].last_processed_sequence ==
                kRecordsPerSource &&
            !live_snapshot.sources[3U].worker_exited,
        "Shenzhen source reaches its exact decoded prefix");

    std::vector<market::InstrumentHistoryRecordHandleV1> shanghai_records;
    test->Expect(
        fast_plane->Tail(
            kShanghaiInstrument,
            1U,
            market::InstrumentHistoryLaneV1::kTick,
            static_cast<std::size_t>(kRecordsPerSource),
            &shanghai_records) ==
            market::InstrumentHistoryQueryErrorV1::kNone,
        "private history returns decoded Shanghai ticks");
    ExpectStrictSourceOrder(
        test,
        shanghai_records,
        kRecordsPerSource,
        "Shanghai instrument history preserves source order");
    if (!shanghai_records.empty()) {
        const auto* event =
            market::RetainedMarketEventGetV1<market::ShanghaiTickV1>(
                shanghai_records.back()->event());
        test->Expect(
            event != nullptr &&
                event->common.security_id == "600000" &&
                event->business_index ==
                    static_cast<std::int64_t>(kRecordsPerSource),
            "Shanghai history payload came from the real decoder");
    }

    std::vector<market::InstrumentHistoryRecordHandleV1> shenzhen_records;
    test->Expect(
        fast_plane->Tail(
            kShenzhenInstrument,
            3U,
            market::InstrumentHistoryLaneV1::kTick,
            static_cast<std::size_t>(kRecordsPerSource),
            &shenzhen_records) ==
            market::InstrumentHistoryQueryErrorV1::kNone,
        "private history returns decoded Shenzhen orders");
    ExpectStrictSourceOrder(
        test,
        shenzhen_records,
        kRecordsPerSource,
        "Shenzhen instrument history preserves source order");
    if (!shenzhen_records.empty()) {
        const auto* event =
            market::RetainedMarketEventGetV1<market::ShenzhenOrderV1>(
                shenzhen_records.back()->event());
        test->Expect(
            event != nullptr &&
                event->common.security_id == "000001" &&
                event->application_sequence ==
                    static_cast<std::int64_t>(
                        20'000U + kRecordsPerSource),
            "Shenzhen history payload came from the real decoder");
    }

    fast_plane->StopAndDrain();
    fast_plane->StopAndDrain();
    const auto stopped = fast_plane->Snapshot();
    test->Expect(
        stopped.stopped && stopped.clean_drain &&
            !stopped.accepting && !stopped.fatal,
        "clean StopAndDrain is idempotent and reconciles every prefix");
    market::InstrumentHistoryRecordHandleV1 stopped_query;
    test->Expect(
        fast_plane->Latest(
            kShanghaiInstrument,
            1U,
            market::InstrumentHistoryLaneV1::kTick,
            &stopped_query) ==
                market::InstrumentHistoryQueryErrorV1::kSourceFatal &&
            !stopped_query,
        "stopped Fast generation cannot serve stale history");
    test->Expect(
        sink.PublishCopy(
            CaptureMeta(kShanghaiTickSource, kRecordsPerSource + 1U),
            std::span<const std::byte, sdk::kVendorHeadBytes>(
                VendorHead(
                    4U,
                    24U,
                    ShanghaiTickBody(kRecordsPerSource + 1U).size(),
                    kRecordsPerSource + 1U)),
            ShanghaiTickBody(kRecordsPerSource + 1U)) ==
            ingress::FastCapturePublishResultV1::kStopped,
        "capture sink rejects admission after StopAndDrain");
}

void TestSequenceExhaustionFailsBeforeCapture(TestContext* test) {
    auto registry = MakeRegistry(test);
    if (registry == nullptr) {
        return;
    }
    auto config = FastConfig();
    config.first_ingress_sequence =
        std::numeric_limits<std::uint64_t>::max();
    auto fast_plane = MakeRuntime(test, registry.get(), config);
    if (fast_plane == nullptr) {
        return;
    }

    const std::vector<std::byte> body = ShanghaiTickBody(1U);
    const sdk::VendorHeadBytes head =
        VendorHead(4U, 24U, body.size(), 1U);
    ingress::CaptureMetaV1 metadata =
        CaptureMeta(kShanghaiTickSource, 1U);
    metadata.ingress_sequence =
        std::numeric_limits<std::uint64_t>::max();
    test->Expect(
        fast_plane->capture_sink().PublishCopy(
            metadata,
            std::span<const std::byte, sdk::kVendorHeadBytes>(head),
            body) == ingress::FastCapturePublishResultV1::kFatal,
        "sequence exhaustion fails the Fast generation closed");

    const auto snapshot = fast_plane->Snapshot();
    const auto& source = snapshot.sources[1U];
    test->Expect(
        snapshot.fatal && source.captured_records == 0U &&
            source.last_captured_sequence == 0U &&
            source.ring_used_bytes == 0U &&
            source.failure ==
                runtime::RealtimeFastPlaneFailureV1::kCaptureInvalid &&
            source.failure_sequence ==
                std::numeric_limits<std::uint64_t>::max(),
        "exhausted sequence is rejected before the capture ring commit");
    fast_plane->StopAndDrain();
}

struct AppendGate final {
    std::atomic<bool> entered{false};
    std::atomic<bool> release{false};
};

void BlockFirstShanghaiAppend(
    void* context,
    std::uint32_t,
    const market::OwnedInstrumentEventEnvelopeV1& envelope) noexcept {
    auto* const gate = static_cast<AppendGate*>(context);
    if (envelope.source_slot() == 1U &&
        envelope.dispatch_ticket() == 1U) {
        gate->entered.store(true, std::memory_order_release);
        while (!gate->release.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }
}

void TestAppendLocalVisibilityAndGenerationRevocation(TestContext* test) {
    auto registry = MakeRegistry(test);
    if (registry == nullptr) {
        return;
    }
    AppendGate gate;
    auto config = FastConfig();
    config.history.physical_worker_count = 2U;
    config.history.before_append_hook = &BlockFirstShanghaiAppend;
    config.history.before_append_hook_context = &gate;
    auto fast_plane = MakeRuntime(test, registry.get(), config);
    if (fast_plane == nullptr) {
        return;
    }
    const ingress::FastCaptureSinkRefV1 sink = fast_plane->capture_sink();

    test->Expect(
        PublishShanghai(sink, 1U, "600000") ==
            ingress::FastCapturePublishResultV1::kPublished,
        "first Shanghai instrument enters its history shard");
    const bool first_append_blocked = WaitUntil([&gate]() {
        return gate.entered.load(std::memory_order_acquire);
    });
    test->Expect(
        first_append_blocked,
        "first instrument append is blocked on its physical worker");
    if (!first_append_blocked) {
        gate.release.store(true, std::memory_order_release);
        fast_plane->StopAndDrain();
        return;
    }

    test->Expect(
        PublishShanghai(sink, 2U, "600001") ==
            ingress::FastCapturePublishResultV1::kPublished,
        "second Shanghai instrument enters an independent shard");
    const bool second_append_visible = WaitUntil([&fast_plane]() {
        const auto snapshot = fast_plane->Snapshot();
        return snapshot.sources[1U]
                   .history_frontier.completed_out_of_order == 1U;
    });
    test->Expect(
        second_append_visible,
        "second shard appends before the source-global prefix closes");

    market::InstrumentHistoryRecordHandleV1 provisional;
    test->Expect(
        fast_plane->Latest(
            kSecondShanghaiInstrument,
            1U,
            market::InstrumentHistoryLaneV1::kTick,
            &provisional) ==
                market::InstrumentHistoryQueryErrorV1::kNone &&
            provisional &&
            provisional->source_sequence() == 2U,
        "Fast query avoids cross-instrument source-prefix HOL");

    const std::vector<std::byte> body = ShanghaiTickBody(4U);
    const sdk::VendorHeadBytes head =
        VendorHead(4U, 24U, body.size(), 4U);
    ingress::CaptureMetaV1 invalid_metadata =
        CaptureMeta(kShanghaiTickSource, 4U);
    test->Expect(
        sink.PublishCopy(
            invalid_metadata,
            std::span<const std::byte, sdk::kVendorHeadBytes>(head),
            body) == ingress::FastCapturePublishResultV1::kFatal,
        "capture sequence gap atomically closes the Fast generation");
    const bool generation_revoked = WaitUntil([&fast_plane]() {
        const auto snapshot = fast_plane->Snapshot();
        return snapshot.sources[0U].history_frontier.fatal &&
               snapshot.sources[1U].history_frontier.fatal &&
               snapshot.sources[2U].history_frontier.fatal &&
               snapshot.sources[3U].history_frontier.fatal;
    });
    test->Expect(
        generation_revoked,
        "worker control path revokes all source histories");

    market::InstrumentHistoryRecordHandleV1 rejected;
    test->Expect(
        fast_plane->Latest(
            kSecondShanghaiInstrument,
            1U,
            market::InstrumentHistoryLaneV1::kTick,
            &rejected) ==
                market::InstrumentHistoryQueryErrorV1::kSourceFatal &&
            !rejected,
        "revoked Fast generation rejects every later query");
    test->Expect(
        provisional && provisional->source_sequence() == 2U,
        "previous handle stays memory-safe but is no longer authoritative");

    gate.release.store(true, std::memory_order_release);
    fast_plane->StopAndDrain();
}

void TestQueueFullFailsAtCapturedPrefix(TestContext* test) {
    auto registry = MakeRegistry(test);
    if (registry == nullptr) {
        return;
    }
    AppendGate gate;
    auto config = FastConfig(4608U, 4096U);
    config.history.physical_worker_count = 1U;
    config.history.queue_capacity = 1U;
    config.history.maximum_inflight_per_source = 8U;
    config.history.before_append_hook = &BlockFirstShanghaiAppend;
    config.history.before_append_hook_context = &gate;
    auto fast_plane = MakeRuntime(test, registry.get(), config);
    if (fast_plane == nullptr) {
        return;
    }
    const ingress::FastCaptureSinkRefV1 sink = fast_plane->capture_sink();

    test->Expect(
        PublishShanghai(sink, 1U) ==
            ingress::FastCapturePublishResultV1::kPublished,
        "first record enters the bounded fast plane");
    const bool append_blocked = WaitUntil([&gate]() {
        return gate.entered.load(std::memory_order_acquire);
    });
    test->Expect(
        append_blocked,
        "history worker blocks deterministically on the first append");
    if (!append_blocked) {
        gate.release.store(true, std::memory_order_release);
        fast_plane->StopAndDrain();
        return;
    }

    test->Expect(
        PublishShanghai(sink, 2U) ==
                ingress::FastCapturePublishResultV1::kPublished &&
            PublishShanghai(sink, 3U) ==
                ingress::FastCapturePublishResultV1::kPublished,
        "source worker reaches history queue backpressure");
    const bool history_queue_full = WaitUntil([&fast_plane]() {
        return fast_plane->Snapshot()
                   .sources[1U]
                   .history_backpressure_retries != 0U;
    });
    test->Expect(
        history_queue_full,
        "history queue full is observed before capture overflow");
    if (!history_queue_full) {
        gate.release.store(true, std::memory_order_release);
        fast_plane->StopAndDrain();
        return;
    }

    std::uint64_t successful_prefix = 3U;
    ingress::FastCapturePublishResultV1 result =
        ingress::FastCapturePublishResultV1::kPublished;
    while (result == ingress::FastCapturePublishResultV1::kPublished &&
           successful_prefix < 64U) {
        const std::uint64_t candidate = successful_prefix + 1U;
        result = PublishShanghai(sink, candidate);
        if (result == ingress::FastCapturePublishResultV1::kPublished) {
            successful_prefix = candidate;
        }
    }
    test->Expect(
        result == ingress::FastCapturePublishResultV1::kFull,
        "bounded capture queue reports full");

    const bool history_revoked = WaitUntil([&fast_plane]() {
        const auto snapshot = fast_plane->Snapshot();
        for (const auto& source : snapshot.sources) {
            if (!source.history_frontier.fatal) {
                return false;
            }
        }
        return true;
    });
    test->Expect(
        history_revoked,
        "decoder control path asynchronously revokes the full Fast generation");
    const auto fatal = fast_plane->Snapshot();
    const auto& source = fatal.sources[1U];
    test->Expect(
        fatal.fatal && !fatal.accepting &&
            source.failure ==
                runtime::RealtimeFastPlaneFailureV1::kCaptureFull,
        "capture queue full fails the fast plane closed");
    test->Expect(
        source.captured_records == successful_prefix &&
            source.last_captured_sequence == successful_prefix &&
            source.failure_sequence == successful_prefix + 1U,
        "fatal sequence is exactly the first record beyond captured prefix");
    test->Expect(
        source.last_submitted_sequence <= source.last_processed_sequence &&
            source.last_processed_sequence <=
                source.last_captured_sequence &&
            source.history_frontier.submitted_source_sequence <=
                source.last_captured_sequence &&
            source.history_frontier.fatal,
        "processed, submitted, and history frontiers stay inside capture prefix");
    market::InstrumentHistoryRecordHandleV1 cross_source_query;
    test->Expect(
        fast_plane->Latest(
            kShenzhenInstrument,
            3U,
            market::InstrumentHistoryLaneV1::kTick,
            &cross_source_query) ==
                market::InstrumentHistoryQueryErrorV1::kSourceFatal &&
            !cross_source_query,
        "one Fast failure revokes queries for the complete generation");
    test->Expect(
        PublishShanghai(sink, successful_prefix + 2U) ==
            ingress::FastCapturePublishResultV1::kFatal,
        "fatal fast plane rejects every later callback");

    gate.release.store(true, std::memory_order_release);
    fast_plane->StopAndDrain();
    const auto stopped = fast_plane->Snapshot();
    bool all_workers_exited = true;
    for (const auto& stopped_source : stopped.sources) {
        all_workers_exited = all_workers_exited &&
                             stopped_source.worker_exited;
    }
    test->Expect(
        stopped.stopped && stopped.fatal && all_workers_exited,
        "fatal StopAndDrain joins every worker without widening the prefix");
}

}  // namespace

int main() {
    TestContext test;
    TestParallelDecodeIntoPrivateHistory(&test);
    TestSequenceExhaustionFailsBeforeCapture(&test);
    TestAppendLocalVisibilityAndGenerationRevocation(&test);
    TestQueueFullFailsAtCapturedPrefix(&test);
    if (test.failures() != 0) {
        std::cerr << test.failures()
                  << " realtime fast-plane checks failed\n";
        return 1;
    }
    std::cout << "realtime fast-plane checks passed\n";
    return 0;
}
