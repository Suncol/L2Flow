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
constexpr std::size_t kShenzhenTransactionFixedBytes = 70U;

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
        config.stream_day_ids[slot][0U] =
            static_cast<std::byte>(slot + 1U);
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

struct ShanghaiTickSpec final {
    std::uint64_t business_sequence = 1U;
    std::uint32_t channel = 1U;
    std::string_view security_id = "600000";
    std::string_view type = "A";
    std::string_view tick_flag = "B";
    std::uint32_t exchange_time = 93'000'000U;
    std::uint64_t buy_order_id = 10'001U;
    std::uint64_t sell_order_id = 0U;
    std::uint32_t price_p3 = 12'345U;
    std::uint64_t quantity = 101U;
    std::uint64_t trade_amount_p3 = 0U;
};

[[nodiscard]] std::vector<std::byte> ShanghaiTickBody(
    const ShanghaiTickSpec& spec) {
    WireWriter writer(kShanghaiTickFixedBytes);
    writer.StoreU64(0U, spec.business_sequence);
    writer.StoreU32(8U, spec.channel);
    writer.StoreU32(18U, spec.exchange_time);
    writer.StoreU64(28U, spec.buy_order_id);
    writer.StoreU64(36U, spec.sell_order_id);
    writer.StoreU32(44U, spec.price_p3);
    writer.StoreU64(48U, spec.quantity);
    writer.StoreU64(56U, spec.trade_amount_p3);
    writer.StoreString(12U, spec.security_id);
    writer.StoreString(22U, spec.type);
    writer.StoreString(64U, spec.tick_flag);
    return std::move(writer).Take();
}

[[nodiscard]] std::vector<std::byte> ShanghaiTickBody(
    std::uint64_t sequence,
    std::string_view security_id = "600000") {
    ShanghaiTickSpec spec{};
    spec.business_sequence = sequence;
    spec.security_id = security_id;
    spec.buy_order_id = 10'000U + sequence;
    spec.quantity = 100U + sequence;
    return ShanghaiTickBody(spec);
}

struct ShenzhenOrderSpec final {
    std::uint32_t channel = 12U;
    std::uint64_t business_sequence = 1U;
    std::uint64_t quantity = 201U;
};

[[nodiscard]] std::vector<std::byte> ShenzhenOrderBody(
    const ShenzhenOrderSpec& spec) {
    WireWriter writer(kShenzhenOrderFixedBytes);
    writer.StoreU32(0U, spec.channel);
    writer.StoreU64(4U, spec.business_sequence);
    writer.StoreU64(30U, 123'456U);
    writer.StoreU64(38U, spec.quantity);
    writer.StoreU32(46U, 49U);
    writer.StoreU32(50U, 93'000'123U);
    writer.StoreU32(54U, 50U);
    writer.StoreString(12U, "010");
    writer.StoreString(18U, "000001");
    writer.StoreString(24U, "102");
    return std::move(writer).Take();
}

[[nodiscard]] std::vector<std::byte> ShenzhenOrderBody(
    std::uint64_t sequence) {
    ShenzhenOrderSpec spec{};
    spec.business_sequence = 20'000U + sequence;
    spec.quantity = 200U + sequence;
    return ShenzhenOrderBody(spec);
}

struct ShenzhenTransactionSpec final {
    std::uint32_t channel = 12U;
    std::uint64_t business_sequence = 2U;
};

[[nodiscard]] std::vector<std::byte> ShenzhenTransactionBody(
    const ShenzhenTransactionSpec& spec) {
    WireWriter writer(kShenzhenTransactionFixedBytes);
    writer.StoreU32(0U, spec.channel);
    writer.StoreU64(4U, spec.business_sequence);
    writer.StoreU64(18U, 81'001U);
    writer.StoreU64(26U, 82'002U);
    writer.StoreU64(46U, 123'456U);
    writer.StoreU64(54U, 50U);
    writer.StoreU32(62U, 70U);
    writer.StoreU32(66U, 93'000'123U);
    writer.StoreString(12U, "010");
    writer.StoreString(34U, "000001");
    writer.StoreString(40U, "102");
    return std::move(writer).Take();
}

[[nodiscard]] sdk::VendorHeadBytes VendorHead(
    std::uint8_t service_id,
    std::uint16_t message_id,
    std::size_t body_bytes,
    std::uint64_t vendor_sequence,
    std::uint32_t vendor_local_time) {
    WireWriter writer(sdk::kVendorHeadBytes);
    writer.StoreU16(7U, 101U);
    writer.StoreU16(9U, message_id);
    writer.StoreU32(11U, vendor_local_time);
    writer.StoreU64(15U, vendor_sequence);
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

[[nodiscard]] sdk::VendorHeadBytes VendorHead(
    std::uint8_t service_id,
    std::uint16_t message_id,
    std::size_t body_bytes,
    std::uint64_t sequence) {
    return VendorHead(
        service_id,
        message_id,
        body_bytes,
        100'000U + sequence,
        93'000'000U);
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

[[nodiscard]] ingress::FastCapturePublishResultV1 PublishShanghai(
    const ingress::FastCaptureSinkRefV1& sink,
    std::uint64_t ingress_sequence,
    std::uint64_t vendor_sequence,
    const ShanghaiTickSpec& spec,
    std::uint32_t vendor_local_time = 93'000'000U) {
    const std::vector<std::byte> body = ShanghaiTickBody(spec);
    const sdk::VendorHeadBytes head = VendorHead(
        4U,
        24U,
        body.size(),
        vendor_sequence,
        vendor_local_time);
    return sink.PublishCopy(
        CaptureMeta(kShanghaiTickSource, ingress_sequence),
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

[[nodiscard]] ingress::FastCapturePublishResultV1 PublishShenzhenOrder(
    const ingress::FastCaptureSinkRefV1& sink,
    std::uint64_t ingress_sequence,
    std::uint64_t vendor_sequence,
    const ShenzhenOrderSpec& spec) {
    const std::vector<std::byte> body = ShenzhenOrderBody(spec);
    const sdk::VendorHeadBytes head =
        VendorHead(6U, 33U, body.size(), vendor_sequence);
    return sink.PublishCopy(
        CaptureMeta(kShenzhenTickSource, ingress_sequence),
        std::span<const std::byte, sdk::kVendorHeadBytes>(head),
        body);
}

[[nodiscard]] ingress::FastCapturePublishResultV1
PublishShenzhenTransaction(
    const ingress::FastCaptureSinkRefV1& sink,
    std::uint64_t ingress_sequence,
    std::uint64_t vendor_sequence,
    const ShenzhenTransactionSpec& spec) {
    const std::vector<std::byte> body =
        ShenzhenTransactionBody(spec);
    const sdk::VendorHeadBytes head =
        VendorHead(6U, 36U, body.size(), vendor_sequence);
    return sink.PublishCopy(
        CaptureMeta(kShenzhenTickSource, ingress_sequence),
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

void TestShanghaiStatusPhaseAttribution(TestContext* test) {
    auto registry = MakeRegistry(test);
    if (registry == nullptr) {
        return;
    }
    auto fast_plane = MakeRuntime(test, registry.get());
    if (fast_plane == nullptr) {
        return;
    }
    const ingress::FastCaptureSinkRefV1 sink =
        fast_plane->capture_sink();

    ShanghaiTickSpec tick{};
    tick.business_sequence = 1U;
    tick.type = "S";
    tick.tick_flag = "TRADE";
    const bool published_trade_status =
        PublishShanghai(sink, 1U, 200'001U, tick) ==
        ingress::FastCapturePublishResultV1::kPublished;

    tick.business_sequence = 2U;
    tick.type = "A";
    tick.tick_flag = "B";
    const bool published_first_instrument_tick =
        PublishShanghai(sink, 2U, 200'002U, tick) ==
        ingress::FastCapturePublishResultV1::kPublished;

    tick.business_sequence = 3U;
    tick.security_id = "600001";
    const bool published_unknown_phase_tick =
        PublishShanghai(sink, 3U, 200'003U, tick) ==
        ingress::FastCapturePublishResultV1::kPublished;

    tick.business_sequence = 4U;
    tick.type = "S";
    tick.tick_flag = "OCALL";
    const bool published_opening_status =
        PublishShanghai(sink, 4U, 200'004U, tick) ==
        ingress::FastCapturePublishResultV1::kPublished;

    tick.business_sequence = 5U;
    tick.type = "A";
    tick.tick_flag = "B";
    const bool published_second_instrument_tick =
        PublishShanghai(sink, 5U, 200'005U, tick) ==
        ingress::FastCapturePublishResultV1::kPublished;

    tick.business_sequence = 6U;
    tick.security_id = "600000";
    tick.type = "S";
    tick.tick_flag = "UNKN";
    const bool published_unknown_status =
        PublishShanghai(sink, 6U, 200'006U, tick) ==
        ingress::FastCapturePublishResultV1::kPublished;

    tick.business_sequence = 7U;
    tick.type = "A";
    tick.tick_flag = "B";
    const bool published_after_unknown_status =
        PublishShanghai(sink, 7U, 200'007U, tick) ==
        ingress::FastCapturePublishResultV1::kPublished;

    tick.business_sequence = 8U;
    tick.type = "S";
    tick.tick_flag = "OCALL";
    const bool published_phase_change =
        PublishShanghai(sink, 8U, 200'008U, tick) ==
        ingress::FastCapturePublishResultV1::kPublished;

    ShanghaiTickSpec repeated_old_tick{};
    repeated_old_tick.business_sequence = 2U;
    repeated_old_tick.type = "A";
    repeated_old_tick.tick_flag = "B";
    const bool published_old_tick_duplicate =
        PublishShanghai(
            sink, 9U, 200'009U, repeated_old_tick) ==
        ingress::FastCapturePublishResultV1::kPublished;

    ShanghaiTickSpec repeated_old_status{};
    repeated_old_status.business_sequence = 1U;
    repeated_old_status.type = "S";
    repeated_old_status.tick_flag = "TRADE";
    const bool published_old_status_duplicate =
        PublishShanghai(
            sink, 10U, 200'010U, repeated_old_status) ==
        ingress::FastCapturePublishResultV1::kPublished;

    tick.business_sequence = 9U;
    tick.type = "A";
    tick.tick_flag = "B";
    const bool published_after_old_duplicate =
        PublishShanghai(sink, 11U, 200'011U, tick) ==
        ingress::FastCapturePublishResultV1::kPublished;

    test->Expect(
        published_trade_status &&
            published_first_instrument_tick &&
            published_unknown_phase_tick &&
            published_opening_status &&
            published_second_instrument_tick &&
            published_unknown_status &&
            published_after_unknown_status &&
            published_phase_change &&
            published_old_tick_duplicate &&
            published_old_status_duplicate &&
            published_after_old_duplicate,
        "real Shanghai Type=S status records and ticks enter Fast");

    const bool visible = WaitUntil([&fast_plane]() {
        const auto snapshot = fast_plane->Snapshot();
        const auto& source = snapshot.sources[1U];
        return source.exchange_duplicate_records == 2U &&
               source.history_frontier.acknowledged_ticket == 9U;
    });
    test->Expect(
        visible,
        "Shanghai status and attributed ticks become query-visible");

    std::vector<market::InstrumentHistoryRecordHandleV1> first;
    std::vector<market::InstrumentHistoryRecordHandleV1> second;
    test->Expect(
        fast_plane->Tail(
            kShanghaiInstrument,
            1U,
            market::InstrumentHistoryLaneV1::kTick,
            6U,
            &first) ==
                market::InstrumentHistoryQueryErrorV1::kNone &&
            first.size() == 6U,
        "first Shanghai instrument retains its status timeline");
    test->Expect(
        fast_plane->Tail(
            kSecondShanghaiInstrument,
            1U,
            market::InstrumentHistoryLaneV1::kTick,
            3U,
            &second) ==
                market::InstrumentHistoryQueryErrorV1::kNone &&
            second.size() == 3U,
        "second Shanghai instrument retains an independent timeline");

    const auto event_at = [](
                              const std::vector<
                                  market::InstrumentHistoryRecordHandleV1>&
                                  records,
                              std::size_t index) {
        return index < records.size()
            ? market::RetainedMarketEventGetV1<
                  market::ShanghaiTickV1>(
                  records[index]->event())
            : nullptr;
    };
    const market::ShanghaiTickV1* const first_status =
        event_at(first, 0U);
    const market::ShanghaiTickV1* const first_tick =
        event_at(first, 1U);
    const market::ShanghaiTickV1* const unknown_status =
        event_at(first, 2U);
    const market::ShanghaiTickV1* const after_unknown =
        event_at(first, 3U);
    const market::ShanghaiTickV1* const changed_status =
        event_at(first, 4U);
    const market::ShanghaiTickV1* const after_old_duplicate =
        event_at(first, 5U);
    const market::ShanghaiTickV1* const before_second_status =
        event_at(second, 0U);
    const market::ShanghaiTickV1* const second_status =
        event_at(second, 1U);
    const market::ShanghaiTickV1* const second_tick =
        event_at(second, 2U);

    test->Expect(
        first_status != nullptr &&
            first_status->fields.action ==
                market::TickActionV1::kStatus &&
            first_status->fields.phase ==
                market::TradingPhaseV1::kContinuous &&
            (first_status->fields.validity_bitmap &
             market::kTickPhaseValidV1) != 0U &&
            !first_status->common.sh_phase_attribution_deferred,
        "Type=S and TickBSFlag=TRADE publish Continuous for 600000");
    test->Expect(
        first_tick != nullptr &&
            first_tick->fields.phase ==
                market::TradingPhaseV1::kContinuous &&
            (first_tick->fields.validity_bitmap &
             market::kTickPhaseValidV1) != 0U,
        "the next 600000 tick inherits its accepted status phase");
    test->Expect(
        before_second_status != nullptr &&
            before_second_status->fields.phase ==
                market::TradingPhaseV1::kUnknown &&
            (before_second_status->fields.validity_bitmap &
             market::kTickPhaseValidV1) == 0U,
        "600001 remains Unknown before its own status");
    test->Expect(
        second_status != nullptr &&
            second_status->fields.phase ==
                market::TradingPhaseV1::kOpeningCall &&
            second_tick != nullptr &&
            second_tick->fields.phase ==
                market::TradingPhaseV1::kOpeningCall &&
            (second_tick->fields.validity_bitmap &
             market::kTickPhaseValidV1) != 0U,
        "TickBSFlag=OCALL affects only the matching instrument");
    test->Expect(
        unknown_status != nullptr &&
            unknown_status->fields.action ==
                market::TickActionV1::kStatus &&
            unknown_status->fields.phase ==
                market::TradingPhaseV1::kUnknown &&
            (unknown_status->fields.validity_bitmap &
             market::kTickPhaseValidV1) == 0U &&
            after_unknown != nullptr &&
            after_unknown->fields.phase ==
                market::TradingPhaseV1::kContinuous &&
            (after_unknown->fields.validity_bitmap &
             market::kTickPhaseValidV1) != 0U,
        "an unknown status is retained but never overwrites known phase");
    test->Expect(
        changed_status != nullptr &&
            changed_status->fields.phase ==
                market::TradingPhaseV1::kOpeningCall &&
            after_old_duplicate != nullptr &&
            after_old_duplicate->fields.phase ==
                market::TradingPhaseV1::kOpeningCall &&
            (after_old_duplicate->fields.validity_bitmap &
             market::kTickPhaseValidV1) != 0U &&
            first[5U]->source_sequence() == 11U,
        "old tick/status duplicates are suppressed across a phase change");

    const auto snapshot = fast_plane->Snapshot();
    const auto& source = snapshot.sources[1U];
    test->Expect(
        !snapshot.fatal &&
            source.phase_status_commits == 3U &&
            source.phase_attributed_records == 4U &&
            source.phase_unknown_records == 2U &&
            source.phase_product_count == 2U &&
            source.exchange_duplicate_records == 2U &&
            source.history_submissions == 9U,
        "phase counters reconcile accepted, inherited and unknown records");
    fast_plane->StopAndDrain();
    test->Expect(
        fast_plane->Snapshot().clean_drain,
        "phase-attributed Fast generation drains cleanly");
}

void TestExactDuplicatesAndBusinessIdentity(TestContext* test) {
    {
        auto registry = MakeRegistry(test);
        if (registry == nullptr) {
            return;
        }
        auto fast_plane = MakeRuntime(test, registry.get());
        if (fast_plane == nullptr) {
            return;
        }
        const ingress::FastCaptureSinkRefV1 sink =
            fast_plane->capture_sink();
        ShanghaiTickSpec tick{};
        tick.business_sequence = 1U;
        tick.price_p3 = 10'000U;
        tick.quantity = 1'000U;

        test->Expect(
            PublishShanghai(sink, 1U, 300'001U, tick) ==
                    ingress::FastCapturePublishResultV1::kPublished &&
                PublishShanghai(sink, 2U, 300'001U, tick) ==
                    ingress::FastCapturePublishResultV1::kPublished,
            "byte-exact vendor retransmission enters the capture prefix");
        const bool duplicate_seen = WaitUntil([&fast_plane]() {
            const auto snapshot = fast_plane->Snapshot();
            const auto& source = snapshot.sources[1U];
            return source.vendor_duplicate_records == 1U &&
                   source.last_processed_sequence == 2U &&
                   source.history_frontier.acknowledged_ticket == 1U;
        });
        test->Expect(
            duplicate_seen,
            "same vendor scope and sequence with exact evidence is suppressed");

        std::vector<market::InstrumentHistoryRecordHandleV1> records;
        test->Expect(
            fast_plane->Tail(
                kShanghaiInstrument,
                1U,
                market::InstrumentHistoryLaneV1::kTick,
                2U,
                &records) ==
                    market::InstrumentHistoryQueryErrorV1::kNone &&
                records.size() == 1U,
            "vendor duplicate does not double append history");
        fast_plane->StopAndDrain();
        const auto stopped = fast_plane->Snapshot();
        test->Expect(
            stopped.clean_drain && !stopped.fatal &&
                stopped.sources[1U].captured_records == 2U &&
                stopped.sources[1U].decoded_records == 1U &&
                stopped.sources[1U].history_submissions == 1U &&
                stopped.sources[1U].vendor_duplicate_records == 1U,
            "a duplicate as the final capture record reconciles cleanly");
    }

    {
        auto registry = MakeRegistry(test);
        if (registry == nullptr) {
            return;
        }
        auto fast_plane = MakeRuntime(test, registry.get());
        if (fast_plane == nullptr) {
            return;
        }
        const ingress::FastCaptureSinkRefV1 sink =
            fast_plane->capture_sink();
        ShanghaiTickSpec tick{};
        tick.business_sequence = 1U;
        tick.price_p3 = 10'000U;
        tick.quantity = 1'000U;
        tick.buy_order_id = 77U;
        tick.type = "T";
        tick.trade_amount_p3 = 10'000'000U;

        const bool first =
            PublishShanghai(sink, 1U, 400'001U, tick) ==
            ingress::FastCapturePublishResultV1::kPublished;
        const bool exchange_duplicate =
            PublishShanghai(sink, 2U, 400'002U, tick) ==
            ingress::FastCapturePublishResultV1::kPublished;
        tick.business_sequence = 2U;
        const bool second_business_event =
            PublishShanghai(sink, 3U, 400'003U, tick) ==
            ingress::FastCapturePublishResultV1::kPublished;
        test->Expect(
            first && exchange_duplicate && second_business_event,
            "new vendor deliveries reach exchange identity handling");

        const bool visible = WaitUntil([&fast_plane]() {
            const auto snapshot = fast_plane->Snapshot();
            const auto& source = snapshot.sources[1U];
            return source.exchange_duplicate_records == 1U &&
                   source.history_frontier.acknowledged_ticket == 2U;
        });
        test->Expect(
            visible,
            "exact repeated BizIndex is suppressed and next BizIndex appends");

        std::vector<market::InstrumentHistoryRecordHandleV1> records;
        test->Expect(
            fast_plane->Tail(
                kShanghaiInstrument,
                1U,
                market::InstrumentHistoryLaneV1::kTick,
                3U,
                &records) ==
                    market::InstrumentHistoryQueryErrorV1::kNone &&
                records.size() == 2U,
            "exchange duplicate contributes no history row");
        const market::ShanghaiTickV1* const first_event =
            records.size() > 0U
            ? market::RetainedMarketEventGetV1<
                  market::ShanghaiTickV1>(records[0U]->event())
            : nullptr;
        const market::ShanghaiTickV1* const second_event =
            records.size() > 1U
            ? market::RetainedMarketEventGetV1<
                  market::ShanghaiTickV1>(records[1U]->event())
            : nullptr;
        test->Expect(
            first_event != nullptr && second_event != nullptr &&
                records[0U]->source_sequence() == 1U &&
                records[1U]->source_sequence() == 3U &&
                first_event->business_index == 1 &&
                second_event->business_index == 2 &&
                first_event->fields.price.raw ==
                    second_event->fields.price.raw &&
                first_event->fields.quantity.raw ==
                    second_event->fields.quantity.raw,
            "equal price and quantity with distinct BizIndex remain two trades");
        const auto snapshot = fast_plane->Snapshot();
        test->Expect(
            !snapshot.fatal &&
                snapshot.sources[1U].captured_records == 3U &&
                snapshot.sources[1U].decoded_records == 3U &&
                snapshot.sources[1U].exchange_duplicate_records == 1U &&
                snapshot.sources[1U].history_submissions == 2U,
            "exchange duplicate accounting matches decoded and stored totals");
        fast_plane->StopAndDrain();
        test->Expect(
            fast_plane->Snapshot().clean_drain,
            "exchange duplicate generation drains cleanly");
    }
}

void TestShenzhenUnifiedSequenceScope(TestContext* test) {
    {
        auto registry = MakeRegistry(test);
        if (registry == nullptr) {
            return;
        }
        auto fast_plane = MakeRuntime(test, registry.get());
        if (fast_plane == nullptr) {
            return;
        }
        const ingress::FastCaptureSinkRefV1 sink =
            fast_plane->capture_sink();
        ShenzhenOrderSpec order{};
        order.channel = 12U;
        order.business_sequence = 1U;
        ShenzhenTransactionSpec transaction{};
        transaction.channel = 12U;
        transaction.business_sequence = 2U;
        ShenzhenTransactionSpec other_channel = transaction;
        other_channel.channel = 13U;
        other_channel.business_sequence = 1U;

        test->Expect(
            PublishShenzhenOrder(
                sink, 1U, 900'001U, order) ==
                    ingress::FastCapturePublishResultV1::kPublished &&
                PublishShenzhenTransaction(
                    sink, 2U, 910'001U, transaction) ==
                    ingress::FastCapturePublishResultV1::kPublished &&
                PublishShenzhenTransaction(
                    sink, 3U, 910'002U, other_channel) ==
                    ingress::FastCapturePublishResultV1::kPublished,
            "SZ order/transaction and independent channel enter Fast");
        const bool visible = WaitUntil([&fast_plane]() {
            const auto snapshot = fast_plane->Snapshot();
            return snapshot.sources[3U]
                       .history_frontier.acknowledged_ticket == 3U;
        });
        test->Expect(
            visible && !fast_plane->Snapshot().fatal,
            "SZ 6.33 then 6.36 is contiguous in one channel while "
            "another channel starts independently");

        std::vector<market::InstrumentHistoryRecordHandleV1> records;
        test->Expect(
            fast_plane->Tail(
                kShenzhenInstrument,
                3U,
                market::InstrumentHistoryLaneV1::kTick,
                3U,
                &records) ==
                    market::InstrumentHistoryQueryErrorV1::kNone &&
                records.size() == 3U,
            "accepted SZ unified-scope events all append once");
        fast_plane->StopAndDrain();
        test->Expect(
            fast_plane->Snapshot().clean_drain,
            "SZ unified-scope generation drains cleanly");
    }

    {
        auto registry = MakeRegistry(test);
        if (registry == nullptr) {
            return;
        }
        auto fast_plane = MakeRuntime(test, registry.get());
        if (fast_plane == nullptr) {
            return;
        }
        const ingress::FastCaptureSinkRefV1 sink =
            fast_plane->capture_sink();
        ShenzhenOrderSpec order{};
        order.channel = 12U;
        order.business_sequence = 1U;
        ShenzhenTransactionSpec transaction{};
        transaction.channel = 12U;
        transaction.business_sequence = 1U;

        test->Expect(
            PublishShenzhenOrder(
                sink, 1U, 920'001U, order) ==
                    ingress::FastCapturePublishResultV1::kPublished &&
                PublishShenzhenTransaction(
                    sink, 2U, 930'001U, transaction) ==
                    ingress::FastCapturePublishResultV1::kPublished,
            "same-channel SZ cross-family sequence collision enters capture");
        const bool failed = WaitUntil([&fast_plane]() {
            return fast_plane->Snapshot().fatal;
        });
        const auto snapshot = fast_plane->Snapshot();
        const auto& source = snapshot.sources[3U];
        test->Expect(
            failed &&
                source.exchange_sequence_conflicts == 1U &&
                source.failure_business_sequence == 1U &&
                source.failure_channel == 12U &&
                source.history_submissions == 1U,
            "SZ 6.33 and 6.36 share one ChannelNo sequence guard");
        fast_plane->StopAndDrain();
    }
}

void TestSequenceConflictsFailClosed(TestContext* test) {
    {
        auto registry = MakeRegistry(test);
        if (registry == nullptr) {
            return;
        }
        auto fast_plane = MakeRuntime(test, registry.get());
        if (fast_plane == nullptr) {
            return;
        }
        const ingress::FastCaptureSinkRefV1 sink =
            fast_plane->capture_sink();
        ShanghaiTickSpec tick{};
        tick.business_sequence = 1U;
        test->Expect(
            PublishShanghai(sink, 1U, 500'001U, tick) ==
                ingress::FastCapturePublishResultV1::kPublished,
            "vendor-conflict baseline enters Fast");
        tick.quantity += 1U;
        test->Expect(
            PublishShanghai(sink, 2U, 500'001U, tick) ==
                ingress::FastCapturePublishResultV1::kPublished,
            "same vendor sequence with changed body enters capture");

        const bool failed = WaitUntil([&fast_plane]() {
            return fast_plane->Snapshot().fatal;
        });
        test->Expect(
            failed,
            "vendor sequence conflict fails the complete Fast generation");
        const auto snapshot = fast_plane->Snapshot();
        const auto& source = snapshot.sources[1U];
        test->Expect(
            source.failure ==
                    runtime::RealtimeFastPlaneFailureV1::kSequenceFailed &&
                source.failure_sequence == 2U &&
                source.failure_vendor_sequence == 500'001U &&
                source.failure_business_sequence == 0U &&
                source.vendor_sequence_conflicts == 1U &&
                source.decoded_records == 1U &&
                source.history_submissions == 1U,
            "vendor conflict reports the exact failing delivery before decode");
        fast_plane->StopAndDrain();
    }

    {
        auto registry = MakeRegistry(test);
        if (registry == nullptr) {
            return;
        }
        auto fast_plane = MakeRuntime(test, registry.get());
        if (fast_plane == nullptr) {
            return;
        }
        const ingress::FastCaptureSinkRefV1 sink =
            fast_plane->capture_sink();
        ShanghaiTickSpec tick{};
        tick.business_sequence = 1U;
        tick.type = "S";
        tick.tick_flag = "TRADE";
        test->Expect(
            PublishShanghai(sink, 1U, 600'001U, tick) ==
                ingress::FastCapturePublishResultV1::kPublished,
            "accepted Shanghai status starts exchange and phase state");
        tick.tick_flag = "SUSP";
        test->Expect(
            PublishShanghai(sink, 2U, 600'002U, tick) ==
                ingress::FastCapturePublishResultV1::kPublished,
            "same BizIndex with a different status enters capture");

        const bool failed = WaitUntil([&fast_plane]() {
            return fast_plane->Snapshot().fatal;
        });
        test->Expect(
            failed,
            "exchange sequence conflict fails the complete Fast generation");
        const auto snapshot = fast_plane->Snapshot();
        const auto& source = snapshot.sources[1U];
        test->Expect(
            source.failure ==
                    runtime::RealtimeFastPlaneFailureV1::kSequenceFailed &&
                source.failure_sequence == 2U &&
                source.failure_vendor_sequence == 600'002U &&
                source.failure_business_sequence == 1U &&
                source.failure_channel == 1U &&
                source.exchange_sequence_conflicts == 1U &&
                source.decoded_records == 2U &&
                source.history_submissions == 1U &&
                source.phase_status_commits == 1U &&
                source.phase_product_count == 1U,
            "conflicting status reports its scope and cannot mutate phase");
        fast_plane->StopAndDrain();
    }
}

void TestSequenceGapsFailClosed(TestContext* test) {
    {
        auto registry = MakeRegistry(test);
        if (registry == nullptr) {
            return;
        }
        auto fast_plane = MakeRuntime(test, registry.get());
        if (fast_plane == nullptr) {
            return;
        }
        const ingress::FastCaptureSinkRefV1 sink =
            fast_plane->capture_sink();
        ShanghaiTickSpec tick{};
        tick.business_sequence = 1U;
        test->Expect(
            PublishShanghai(sink, 1U, 700'001U, tick) ==
                    ingress::FastCapturePublishResultV1::kPublished &&
                PublishShanghai(sink, 2U, 700'003U, tick) ==
                    ingress::FastCapturePublishResultV1::kPublished,
            "vendor sequence gap enters the contiguous capture prefix");
        const bool failed = WaitUntil([&fast_plane]() {
            return fast_plane->Snapshot().fatal;
        });
        test->Expect(
            failed,
            "vendor sequence gap fails Fast closed");
        const auto snapshot = fast_plane->Snapshot();
        const auto& source = snapshot.sources[1U];
        test->Expect(
            source.vendor_sequence_gaps == 1U &&
                source.failure ==
                    runtime::RealtimeFastPlaneFailureV1::kSequenceFailed &&
                source.failure_vendor_sequence == 700'003U &&
                source.decoded_records == 1U,
            "vendor gap is diagnosed before decoding the missing prefix");
        fast_plane->StopAndDrain();
    }

    {
        auto registry = MakeRegistry(test);
        if (registry == nullptr) {
            return;
        }
        auto fast_plane = MakeRuntime(test, registry.get());
        if (fast_plane == nullptr) {
            return;
        }
        const ingress::FastCaptureSinkRefV1 sink =
            fast_plane->capture_sink();
        ShanghaiTickSpec tick{};
        tick.business_sequence = 1U;
        const bool first =
            PublishShanghai(sink, 1U, 800'001U, tick) ==
            ingress::FastCapturePublishResultV1::kPublished;
        tick.business_sequence = 3U;
        const bool gap =
            PublishShanghai(sink, 2U, 800'002U, tick) ==
            ingress::FastCapturePublishResultV1::kPublished;
        test->Expect(
            first && gap,
            "exchange BizIndex gap enters the contiguous capture prefix");
        const bool failed = WaitUntil([&fast_plane]() {
            return fast_plane->Snapshot().fatal;
        });
        test->Expect(
            failed,
            "exchange BizIndex gap fails Fast closed");
        const auto snapshot = fast_plane->Snapshot();
        const auto& source = snapshot.sources[1U];
        test->Expect(
            source.exchange_sequence_gaps == 1U &&
                source.failure ==
                    runtime::RealtimeFastPlaneFailureV1::kSequenceFailed &&
                source.failure_vendor_sequence == 800'002U &&
                source.failure_business_sequence == 3U &&
                source.failure_channel == 1U &&
                source.decoded_records == 2U &&
                source.history_submissions == 1U,
            "exchange gap reports the exact channel and withholds its row");
        fast_plane->StopAndDrain();
    }
}

void TestSequenceAndPhaseLimitsFailClosed(TestContext* test) {
    {
        auto registry = MakeRegistry(test);
        if (registry == nullptr) {
            return;
        }
        auto fast_plane = MakeRuntime(test, registry.get());
        if (fast_plane == nullptr) {
            return;
        }
        const ingress::FastCaptureSinkRefV1 sink =
            fast_plane->capture_sink();
        ShanghaiTickSpec tick{};
        tick.business_sequence = 2U;
        const bool first =
            PublishShanghai(sink, 1U, 940'001U, tick) ==
            ingress::FastCapturePublishResultV1::kPublished;
        tick.business_sequence = 1U;
        const bool backward =
            PublishShanghai(sink, 2U, 940'002U, tick) ==
            ingress::FastCapturePublishResultV1::kPublished;
        test->Expect(
            first && backward,
            "unseen lower BizIndex enters the contiguous capture prefix");
        const bool failed = WaitUntil([&fast_plane]() {
            return fast_plane->Snapshot().fatal;
        });
        const auto snapshot = fast_plane->Snapshot();
        const auto& source = snapshot.sources[1U];
        test->Expect(
            failed &&
                source.failure ==
                    runtime::RealtimeFastPlaneFailureV1::kSequenceFailed &&
                source.failure_business_sequence == 1U &&
                source.exchange_sequence_gaps == 0U &&
                source.exchange_sequence_conflicts == 0U &&
                source.history_submissions == 1U,
            "unseen backward business sequence fails Fast closed");
        fast_plane->StopAndDrain();
    }

    {
        auto registry = MakeRegistry(test);
        if (registry == nullptr) {
            return;
        }
        auto config = FastConfig();
        config.maximum_seen_entries_per_scope = 1U;
        auto fast_plane = MakeRuntime(test, registry.get(), config);
        if (fast_plane == nullptr) {
            return;
        }
        const ingress::FastCaptureSinkRefV1 sink =
            fast_plane->capture_sink();
        ShanghaiTickSpec tick{};
        tick.business_sequence = 1U;
        const bool first =
            PublishShanghai(sink, 1U, 950'001U, tick) ==
            ingress::FastCapturePublishResultV1::kPublished;
        tick.business_sequence = 2U;
        const bool over_capacity =
            PublishShanghai(sink, 2U, 950'002U, tick) ==
            ingress::FastCapturePublishResultV1::kPublished;
        test->Expect(
            first && over_capacity,
            "entry-capacity boundary enters the capture prefix");
        const bool failed = WaitUntil([&fast_plane]() {
            return fast_plane->Snapshot().fatal;
        });
        const auto snapshot = fast_plane->Snapshot();
        const auto& source = snapshot.sources[1U];
        test->Expect(
            failed &&
                source.failure ==
                    runtime::RealtimeFastPlaneFailureV1::kSequenceFailed &&
                source.failure_vendor_sequence == 950'002U &&
                source.vendor_guard_entries == 1U &&
                source.vendor_sequence_conflicts == 0U &&
                source.decoded_records == 1U &&
                source.history_submissions == 1U,
            "next unique Vendor entry beyond the hard bound fails closed");
        fast_plane->StopAndDrain();
    }

    {
        auto registry = MakeRegistry(test);
        if (registry == nullptr) {
            return;
        }
        auto config = FastConfig();
        config.maximum_phase_products = 1U;
        std::unique_ptr<runtime::RealtimeFastPlaneRuntimeV1> fast_plane;
        const auto error = runtime::RealtimeFastPlaneRuntimeV1::Create(
            config, registry.get(), &fast_plane);
        test->Expect(
            error ==
                    runtime::RealtimeFastPlaneCreateErrorV1::
                        kInvalidConfiguration &&
                fast_plane == nullptr,
            "Shanghai registry beyond phase-slot bound fails at creation");
    }
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
    TestShanghaiStatusPhaseAttribution(&test);
    TestExactDuplicatesAndBusinessIdentity(&test);
    TestShenzhenUnifiedSequenceScope(&test);
    TestSequenceConflictsFailClosed(&test);
    TestSequenceGapsFailClosed(&test);
    TestSequenceAndPhaseLimitsFailClosed(&test);
    TestQueueFullFailsAtCapturedPrefix(&test);
    if (test.failures() != 0) {
        std::cerr << test.failures()
                  << " realtime fast-plane checks failed\n";
        return 1;
    }
    std::cout << "realtime fast-plane checks passed\n";
    return 0;
}
