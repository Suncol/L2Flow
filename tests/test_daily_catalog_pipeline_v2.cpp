#include "l2flow/runtime/realtime_pipeline_v1.h"
#include "l2flow/sdk/market_message_catalog_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

// End-to-end tests for frozen daily-catalog admission and direct source lanes.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace factor = l2flow::factor;
namespace ipc = l2flow::ipc;
namespace market = l2flow::market;
namespace mdl = datayes::mdl;
namespace realtime = l2flow::realtime;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;

namespace {

using namespace std::chrono_literals;

class TestContext final {
public:
    void Expect(bool condition, std::string_view description) {
        if (condition) {
            return;
        }
        ++failures_;
        std::cerr << "FAIL: " << description << '\n';
    }

    [[nodiscard]] int failures() const noexcept { return failures_; }

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
        StoreU16(descriptor, static_cast<std::uint16_t>(value.size()));
        StoreU32(
            descriptor + sizeof(std::uint16_t),
            value.empty()
                ? 0U
                : static_cast<std::uint32_t>(start - descriptor));
        const auto characters =
            std::span<const char>(value.data(), value.size());
        const auto encoded = std::as_bytes(characters);
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
            bytes_.at(offset + index) = static_cast<std::byte>(
                (value >> static_cast<unsigned int>(index * 8U)) &
                static_cast<Unsigned>(0xffU));
        }
    }

    std::vector<std::byte> bytes_;
};

[[nodiscard]] std::vector<std::byte> ShenzhenSnapshotBody(
    std::int64_t normalized_last_price_p6,
    std::string_view security_id = "000001") {
    WireWriter writer(224U);
    writer.StoreU32(0U, 93'000'123U);
    writer.StoreU32(4U, 12U);
    writer.StoreU64(32U, 12'000'000U);
    writer.StoreU64(40U, 1U);
    writer.StoreU64(48U, 100U);
    writer.StoreU64(56U, 1'234'560U);
    writer.StoreU64(
        64U, static_cast<std::uint64_t>(normalized_last_price_p6));
    writer.StoreString(8U, "010");
    writer.StoreString(14U, security_id);
    writer.StoreString(20U, "102 ");
    writer.StoreString(26U, "T");
    return std::move(writer).Take();
}

[[nodiscard]] std::vector<std::byte> ShenzhenOrderBody(
    std::uint64_t sequence,
    std::string_view security_id = "000001") {
    WireWriter writer(58U);
    writer.StoreU32(0U, 12U);
    writer.StoreU64(4U, sequence);
    writer.StoreU64(30U, 123'456U);
    writer.StoreU64(38U, 201U);
    writer.StoreU32(46U, 49U);
    writer.StoreU32(50U, 93'000'124U);
    writer.StoreU32(54U, 50U);
    writer.StoreString(12U, "010");
    writer.StoreString(18U, security_id);
    writer.StoreString(24U, "102 ");
    return std::move(writer).Take();
}

[[nodiscard]] std::vector<std::byte> ShanghaiTradeBody(
    std::uint64_t business_index,
    std::string_view security_id = "600007") {
    WireWriter writer(70U);
    writer.StoreU64(0U, business_index);
    writer.StoreU32(8U, 7U);
    writer.StoreU32(18U, 93'000'125U);
    writer.StoreU64(28U, 11'001U);
    writer.StoreU64(36U, 22'002U);
    writer.StoreU32(44U, 12'345U);
    writer.StoreU64(48U, 41U);
    writer.StoreU64(56U, 506'145U);
    writer.StoreString(12U, security_id);
    writer.StoreString(22U, "T");
    writer.StoreString(64U, "B");
    return std::move(writer).Take();
}

[[nodiscard]] std::vector<std::byte> ShanghaiSnapshotBody(
    std::string_view security_id) {
    WireWriter writer(248U);
    writer.StoreU32(0U, 93'000'123U);
    writer.StoreU32(30U, 12'345U);
    writer.StoreString(4U, security_id);
    writer.StoreString(38U, "TRADE");
    return std::move(writer).Take();
}

[[nodiscard]] std::vector<std::byte> ShenzhenTransactionBody(
    std::uint64_t sequence,
    std::string_view security_id) {
    WireWriter writer(70U);
    writer.StoreU32(0U, 12U);
    writer.StoreU64(4U, sequence);
    writer.StoreU64(18U, sequence - 1U);
    writer.StoreU64(26U, 0U);
    writer.StoreU64(46U, 123'456U);
    writer.StoreU64(54U, 33U);
    writer.StoreU32(62U, 70U);
    writer.StoreU32(66U, 93'000'124U);
    writer.StoreString(12U, "010");
    writer.StoreString(34U, security_id);
    writer.StoreString(40U, "102 ");
    return std::move(writer).Take();
}

[[nodiscard]] std::vector<std::byte> ProductionBody(
    std::size_t catalog_index,
    std::string_view security_id,
    std::uint64_t sequence) {
    switch (catalog_index) {
        case 0U:
            return ShanghaiSnapshotBody(security_id);
        case 1U:
            return ShanghaiTradeBody(sequence, security_id);
        case 2U:
            return ShenzhenSnapshotBody(12'345'600, security_id);
        case 3U:
            return ShenzhenOrderBody(sequence, security_id);
        case 4U:
            return ShenzhenTransactionBody(sequence, security_id);
        default:
            return {};
    }
}

class FakeMessage final : public mdl::MDLMessage {
public:
    FakeMessage(sdk::MessageKey key, std::vector<std::byte> body)
        : body_(std::move(body)) {
        std::memset(&head_, 0, sizeof(head_));
        head_.HeadSize =
            static_cast<std::uint8_t>(sdk::kVendorHeadBytes);
        head_.MessageSize = static_cast<std::uint32_t>(
            sdk::kVendorHeadBytes + body_.size());
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID = key.service_id;
        head_.ServiceVersion = key.service_version;
        head_.MessageID = key.message_id;
        head_.LocalTime.m_Value = 93'000'000U;
        head_.SequenceID = 9988U;
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }
    mdl::MDLMessageHead* GetHead() const override {
        return const_cast<mdl::MDLMessageHead*>(&head_);
    }
    char* GetBody() const override {
        return body_.empty()
                   ? nullptr
                   : reinterpret_cast<char*>(
                         const_cast<std::byte*>(body_.data()));
    }
    mdl::MDLMessage* _Copy() const override { return nullptr; }

    void DestroyCallbackBytes() noexcept {
        std::fill(body_.begin(), body_.end(), std::byte{0xffU});
    }

private:
    mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
};

class ProjectionProbe final
    : public market::RealtimeAppliedRecordSinkV1,
      public realtime::ProcessingProgressSinkV2 {
public:
    explicit ProjectionProbe(bool block_first_applied = false)
        : block_first_applied_(block_first_applied) {}

    [[nodiscard]] bool PublishApplied(
        std::size_t ordinal,
        const market::RealtimeHistoryRecordV1& record) noexcept override {
        if (record.instrument_id() == 0U ||
            ordinal !=
                static_cast<std::size_t>(
                    record.instrument_id() - 1U) ||
            record.ingress_sequence() == 0U ||
            record.ingress_sequence() > 63U) {
            return false;
        }
        if (block_first_applied_ &&
            record.ingress_sequence() == 1U) {
            try {
                std::unique_lock<std::mutex> lock(gate_mutex_);
                first_applied_blocked_ = true;
                gate_cv_.notify_all();
                gate_cv_.wait(lock, [this] {
                    return release_first_applied_ ||
                           coverage_lost_.load(
                               std::memory_order_acquire);
                });
            } catch (...) {
                return false;
            }
        }
        const std::uint64_t mask =
            std::uint64_t{1U}
            << static_cast<unsigned int>(
                   record.ingress_sequence() - 1U);
        applied_mask_.fetch_or(mask, std::memory_order_release);
        return true;
    }

    void MarkCoverageLost() noexcept override {
        coverage_lost_.store(true, std::memory_order_release);
        gate_cv_.notify_all();
    }

    [[nodiscard]] bool PublishProcessingProgress(
        realtime::ProcessingProgressV2 progress) noexcept override {
        if (!progress.valid()) {
            return false;
        }
        UpdateMaximum(&accepted_, progress.accepted_sequence);
        UpdateMaximum(&applied_, progress.applied_sequence);
        return true;
    }

    [[nodiscard]] std::uint64_t applied_mask() const noexcept {
        return applied_mask_.load(std::memory_order_acquire);
    }
    [[nodiscard]] std::uint64_t accepted() const noexcept {
        return accepted_.load(std::memory_order_acquire);
    }
    [[nodiscard]] std::uint64_t applied() const noexcept {
        return applied_.load(std::memory_order_acquire);
    }
    [[nodiscard]] bool coverage_lost() const noexcept {
        return coverage_lost_.load(std::memory_order_acquire);
    }
    [[nodiscard]] bool WaitUntilFirstAppliedBlocked(
        std::chrono::nanoseconds timeout) {
        std::unique_lock<std::mutex> lock(gate_mutex_);
        return gate_cv_.wait_for(lock, timeout, [this] {
            return first_applied_blocked_;
        });
    }
    void ReleaseFirstApplied() noexcept {
        {
            std::lock_guard<std::mutex> lock(gate_mutex_);
            release_first_applied_ = true;
        }
        gate_cv_.notify_all();
    }

private:
    static void UpdateMaximum(
        std::atomic<std::uint64_t>* target,
        std::uint64_t candidate) noexcept {
        std::uint64_t current = target->load(std::memory_order_relaxed);
        while (current < candidate &&
               !target->compare_exchange_weak(
                   current,
                   candidate,
                   std::memory_order_release,
                   std::memory_order_relaxed)) {
        }
    }

    std::atomic<std::uint64_t> applied_mask_{0U};
    std::atomic<std::uint64_t> accepted_{0U};
    std::atomic<std::uint64_t> applied_{0U};
    std::atomic<bool> coverage_lost_{false};
    bool block_first_applied_ = false;
    mutable std::mutex gate_mutex_;
    std::condition_variable gate_cv_;
    bool first_applied_blocked_ = false;
    bool release_first_applied_ = false;
};

class OpeningBurstAppliedBlocker final
    : public market::RealtimeAppliedRecordSinkV1 {
public:
    [[nodiscard]] bool PublishApplied(
        std::size_t ordinal,
        const market::RealtimeHistoryRecordV1& record) noexcept override {
        if (record.instrument_id() == 0U ||
            ordinal !=
                static_cast<std::size_t>(
                    record.instrument_id() - 1U) ||
            record.ingress_sequence() == 0U) {
            return false;
        }
        if (record.ingress_sequence() == 1U) {
            try {
                std::unique_lock<std::mutex> lock(gate_mutex_);
                first_applied_blocked_ = true;
                gate_cv_.notify_all();
                gate_cv_.wait(lock, [this] {
                    return release_first_applied_ ||
                           coverage_lost_.load(
                               std::memory_order_acquire);
                });
            } catch (...) {
                return false;
            }
        }
        applied_calls_.fetch_add(1U, std::memory_order_release);
        return true;
    }

    void MarkCoverageLost() noexcept override {
        coverage_lost_.store(true, std::memory_order_release);
        gate_cv_.notify_all();
    }

    [[nodiscard]] bool WaitUntilFirstAppliedBlocked(
        std::chrono::nanoseconds timeout) {
        std::unique_lock<std::mutex> lock(gate_mutex_);
        return gate_cv_.wait_for(lock, timeout, [this] {
            return first_applied_blocked_;
        });
    }

    void ReleaseFirstApplied() noexcept {
        {
            std::lock_guard<std::mutex> lock(gate_mutex_);
            release_first_applied_ = true;
        }
        gate_cv_.notify_all();
    }

    [[nodiscard]] std::uint64_t applied_calls() const noexcept {
        return applied_calls_.load(std::memory_order_acquire);
    }

private:
    std::atomic<std::uint64_t> applied_calls_{0U};
    std::atomic<bool> coverage_lost_{false};
    mutable std::mutex gate_mutex_;
    std::condition_variable gate_cv_;
    bool first_applied_blocked_ = false;
    bool release_first_applied_ = false;
};

class NativeSequenceObservationProbe final
    : public realtime::NativeSequenceObservationSinkV1 {
public:
    void ObserveNativeSequence(
        const realtime::NativeSequenceObservationV1& observation)
        noexcept override {
        observations_.push_back(observation);
    }

    void MarkNativeSequenceObservationFailure(
        realtime::NativeSequenceObservationFailureV1,
        const sdk::MessageKey&) noexcept override {
        ++failures_;
    }

    [[nodiscard]] const std::vector<
        realtime::NativeSequenceObservationV1>&
    observations() const noexcept {
        return observations_;
    }

    [[nodiscard]] std::uint64_t failures() const noexcept {
        return failures_;
    }

private:
    std::vector<realtime::NativeSequenceObservationV1>
        observations_;
    std::uint64_t failures_ = 0U;
};

class GenerationPublicationOrder final {
public:
    [[nodiscard]] bool ObserveSink(
        const market::IntradayInstrumentStoreGenerationV1& generation)
        noexcept {
        const std::uint64_t expected =
            sink_calls_.load(std::memory_order_acquire) + 1U;
        if (generation.watermark().generation != expected ||
            generation.watermark().catalog_snapshot == nullptr ||
            generation.watermark().catalog_snapshot->catalog_scope() !=
                market::InstrumentCatalogScopeV2::
                    kDeclaredDailyAShare ||
            !generation.watermark().catalog_snapshot
                 ->coverage_complete() ||
            !generation.watermark().processing_progress.valid() ||
            generation.watermark().processing_progress.accepted_sequence !=
                generation.watermark().processing_progress.applied_sequence ||
            factor_calls_.load(std::memory_order_acquire) + 1U != expected) {
            invalid_order_.store(true, std::memory_order_release);
            return false;
        }
        sink_calls_.store(expected, std::memory_order_release);
        return !fail_sink_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool ObserveFactor(
        const market::IntradayInstrumentStoreGenerationV1& generation)
        noexcept {
        const std::uint64_t expected =
            factor_calls_.load(std::memory_order_acquire) + 1U;
        if (generation.watermark().generation != expected ||
            sink_calls_.load(std::memory_order_acquire) != expected) {
            invalid_order_.store(true, std::memory_order_release);
            return false;
        }
        factor_calls_.store(expected, std::memory_order_release);
        return true;
    }

    void FailSink() noexcept {
        fail_sink_.store(true, std::memory_order_release);
    }

    [[nodiscard]] std::uint64_t sink_calls() const noexcept {
        return sink_calls_.load(std::memory_order_acquire);
    }
    [[nodiscard]] std::uint64_t factor_calls() const noexcept {
        return factor_calls_.load(std::memory_order_acquire);
    }
    [[nodiscard]] bool invalid_order() const noexcept {
        return invalid_order_.load(std::memory_order_acquire);
    }

private:
    std::atomic<std::uint64_t> sink_calls_{0U};
    std::atomic<std::uint64_t> factor_calls_{0U};
    std::atomic<bool> fail_sink_{false};
    std::atomic<bool> invalid_order_{false};
};

class StoreGenerationSinkProbe final
    : public ipc::RealtimeStoreGenerationSinkV2 {
public:
    explicit StoreGenerationSinkProbe(
        std::shared_ptr<GenerationPublicationOrder> order)
        : order_(std::move(order)) {}

    [[nodiscard]] bool PublishStoreGeneration(
        const std::shared_ptr<
            const market::IntradayInstrumentStoreGenerationV1>&
            generation) noexcept override {
        if (generation == nullptr || order_ == nullptr) {
            return false;
        }
        last_generation_ = generation;
        return order_->ObserveSink(*generation);
    }

    [[nodiscard]] const std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>&
    last_generation() const noexcept {
        return last_generation_;
    }

private:
    std::shared_ptr<GenerationPublicationOrder> order_;
    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        last_generation_;
};

class OrderedFactorCalculator final
    : public factor::RealtimeFactorCalculatorV1 {
public:
    explicit OrderedFactorCalculator(
        std::shared_ptr<GenerationPublicationOrder> order)
        : order_(std::move(order)) {}

    [[nodiscard]] std::span<const factor::RealtimeFactorDefinitionV1>
    definitions() const noexcept override {
        return delegate_.definitions();
    }

    [[nodiscard]] factor::RealtimeFactorCalculatorErrorV1 Calculate(
        const market::IntradayInstrumentStoreGenerationV1& store,
        std::vector<factor::RealtimeFactorPointV1>* output)
        const noexcept override {
        if (order_ == nullptr || !order_->ObserveFactor(store)) {
            return factor::RealtimeFactorCalculatorErrorV1::
                kCalculationFailed;
        }
        return delegate_.Calculate(store, output);
    }

private:
    std::shared_ptr<GenerationPublicationOrder> order_;
    factor::SnapshotLastPriceProjectionV1 delegate_;
};

struct PipelineCatalogFixture final {
    std::shared_ptr<const market::DailyInstrumentCatalogV2> catalog;
    std::unique_ptr<market::InstrumentRuntimeStateV2> runtime_state;
};

[[nodiscard]] std::vector<std::byte> OpaqueBytes(
    std::string_view value) {
    const auto characters =
        std::span<const char>(value.data(), value.size());
    const auto bytes = std::as_bytes(characters);
    return std::vector<std::byte>(bytes.begin(), bytes.end());
}

[[nodiscard]] bool MakeCatalogFixture(
    std::uint64_t session_epoch,
    PipelineCatalogFixture* output) {
    if (output == nullptr || session_epoch == 0U) {
        return false;
    }
    *output = {};
    const market::InstrumentMetadataV2 metadata{
        market::QuantityUnitV1::kShare,
        market::SecurityTypeV1::kEquity,
        market::AssetScopeV1::kDocumentedCore};
    std::array<market::DailyInstrumentSourceEntryV2, 2U> entries{};
    entries[0U].key.market = market::MarketV1::kShanghai;
    entries[0U].key.security_id = OpaqueBytes("600007");
    entries[0U].metadata = metadata;
    entries[1U].key.market = market::MarketV1::kShenzhen;
    entries[1U].key.security_id_source = OpaqueBytes("102 ");
    entries[1U].key.security_id = OpaqueBytes("000001");
    entries[1U].metadata = metadata;

    market::DailyInstrumentCatalogConfigV2 config{};
    config.trade_date = 20260729U;
    config.catalog_version = 7U;
    config.session_epoch = session_epoch;
    config.market_scope = market::kDailyCatalogMainlandScopeV2;
    config.coverage_complete = true;
    std::unique_ptr<market::DailyInstrumentCatalogV2> catalog;
    if (market::DailyInstrumentCatalogV2::Create(
            config, entries, &catalog) !=
            market::DailyInstrumentCatalogCreateErrorV2::kNone ||
        catalog == nullptr) {
        return false;
    }
    output->catalog =
        std::shared_ptr<const market::DailyInstrumentCatalogV2>(
            std::move(catalog));
    return market::InstrumentRuntimeStateV2::Create(
               *output->catalog, &output->runtime_state) ==
               market::InstrumentRuntimeStateErrorV2::kNone &&
           output->runtime_state != nullptr;
}

[[nodiscard]] runtime::RealtimePipelineConfigV1 MakeConfig(
    const PipelineCatalogFixture& fixture,
    const std::shared_ptr<ProjectionProbe>& projection) {
    runtime::RealtimePipelineConfigV1 config{};
    config.run_id[0U] = std::byte{0x31U};
    config.run_id[15U] = std::byte{0x73U};
    config.trade_date = 20260729U;
    config.daily_catalog = fixture.catalog;
    config.runtime_state = fixture.runtime_state.get();
    config.source_stream_ids = {1001U, 1002U, 2001U, 2002U};
    config.maximum_sdk_message_bytes = 4096U;
    config.decoder_queue_capacity_per_source = 32U;
    config.store_worker_count = 2U;
    config.store_queue_capacity_per_source_worker = 32U;
    config.intraday_store.segment_target_bytes = 4U * 1024U;
    config.intraday_store.maximum_session_records = 1024U;
    config.intraday_store.maximum_session_accounted_bytes =
        64U * 1024U * 1024U;
    config.intraday_store.maximum_records_per_batch = 64U;
    config.intraday_store.coverage_from_open = true;
    config.enforce_receive_trade_date = false;
    config.applied_record_sink = projection;
    config.processing_progress_sink = projection;
    config.sdk.enabled = false;
    return config;
}

void CheckStoreGenerationSinkOrderingAndFailure(
    TestContext* test) {
    PipelineCatalogFixture fixture;
    test->Expect(
        MakeCatalogFixture(20U, &fixture),
        "create store-generation-sink daily catalog");
    if (fixture.runtime_state == nullptr) {
        return;
    }

    const auto projection = std::make_shared<ProjectionProbe>();
    const auto order = std::make_shared<GenerationPublicationOrder>();
    const auto sink =
        std::make_shared<StoreGenerationSinkProbe>(order);
    const auto calculator =
        std::make_shared<OrderedFactorCalculator>(order);
    runtime::RealtimePipelineConfigV1 config =
        MakeConfig(fixture, projection);
    config.store_generation_sink = sink;
    config.factor_calculator = calculator;

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            config, &pipeline, &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create store-generation-sink pipeline: " + detail);
    if (pipeline == nullptr) {
        return;
    }

    const runtime::RealtimePipelineCutResultV1 first =
        pipeline->CutAndPublishGeneration(3s);
    test->Expect(
        first.published() && first.store_generation != nullptr &&
            sink->last_generation().get() ==
                first.store_generation.get() &&
            !sink->last_generation().owner_before(
                first.store_generation) &&
            !first.store_generation.owner_before(
                sink->last_generation()) &&
            order->sink_calls() == 1U &&
            order->factor_calls() == 1U &&
            !order->invalid_order(),
        "exact Store generation is sink-published before Factor");

    order->FailSink();
    const runtime::RealtimePipelineCutResultV1 second =
        pipeline->CutAndPublishGeneration(3s);
    test->Expect(
        second.error ==
                runtime::RealtimePipelineCutErrorV1::
                    kStoreGenerationPublishFailed &&
            second.store_generation != nullptr &&
            second.factor_generation == nullptr &&
            order->sink_calls() == 2U &&
            order->factor_calls() == 1U &&
            pipeline->fatal() &&
            runtime::RealtimePipelineCutErrorNameV1(second.error) ==
                "store_generation_publish_failed",
        "Store-generation sink failure prevents Factor and fails closed");
    pipeline->StopAndDrain();
}

template <typename Predicate>
[[nodiscard]] bool WaitUntil(Predicate predicate) {
    const auto deadline =
        std::chrono::steady_clock::now() + 3s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::yield();
    }
    return predicate();
}

[[nodiscard]] runtime::RealtimePipelineIngressResultV1
InjectSourceMessage(
    runtime::RealtimePipelineV1* pipeline,
    std::uint8_t source,
    std::uint64_t source_value) {
    runtime::RealtimePipelineIngressResultV1 invalid{};
    invalid.error =
        runtime::RealtimePipelineIngressErrorV1::kNullMessage;
    if (pipeline == nullptr ||
        source >= market::kRealtimeHistorySourceCountV1) {
        return invalid;
    }

    const std::string_view security_id =
        source < 2U ? "600007" : "000001";
    FakeMessage message(
        sdk::kProductionMessageKeysV1[source],
        ProductionBody(source, security_id, source_value));
    const runtime::RealtimePipelineIngressResultV1 result =
        pipeline->InjectSdkMessageForTest(&message);
    message.DestroyCallbackBytes();
    return result;
}

[[nodiscard]] bool PrepareEverySourceQueueFullAtAppliedGap(
    TestContext* test,
    runtime::RealtimePipelineV1* pipeline,
    const std::shared_ptr<ProjectionProbe>& projection,
    std::size_t applied_window) {
    if (test == nullptr || pipeline == nullptr ||
        projection == nullptr || applied_window == 0U) {
        return false;
    }

    const runtime::RealtimePipelineIngressResultV1 first =
        InjectSourceMessage(pipeline, 2U, 1U);
    const bool first_blocked =
        first.accepted() &&
        projection->WaitUntilFirstAppliedBlocked(3s);
    test->Expect(
        first_blocked,
        "full-lane fixture parks sequence one in the applied sink");
    if (!first_blocked) {
        return false;
    }

    bool prefix_admitted = true;
    for (std::uint64_t sequence = 2U;
         sequence <= static_cast<std::uint64_t>(applied_window);
         ++sequence) {
        const runtime::RealtimePipelineIngressResultV1 admitted =
            InjectSourceMessage(
                pipeline, 1U, 10'000U + sequence);
        prefix_admitted =
            prefix_admitted && admitted.accepted() &&
            WaitUntil([&] {
                const runtime::RealtimePipelineSnapshotV1 snapshot =
                    pipeline->Snapshot();
                return snapshot.fatal ||
                       snapshot.decoded_messages >= sequence;
            }) &&
            !pipeline->fatal();
    }
    test->Expect(
        prefix_admitted,
        "records through D enter History while sequence one is blocked");
    if (!prefix_admitted) {
        return false;
    }

    std::uint64_t next_sequence =
        static_cast<std::uint64_t>(applied_window);
    bool owners_parked_at_gate = true;
    for (std::uint8_t source = 0U;
         source < market::kRealtimeHistorySourceCountV1;
         ++source) {
        const runtime::RealtimePipelineIngressResultV1 admitted =
            InjectSourceMessage(
                pipeline, source, 20'000U + ++next_sequence);
        owners_parked_at_gate =
            owners_parked_at_gate && admitted.accepted() &&
            WaitUntil([&, source] {
                const runtime::RealtimePipelineSnapshotV1 snapshot =
                    pipeline->Snapshot();
                return snapshot.fatal ||
                       (snapshot.decoded_messages == applied_window &&
                        snapshot.decoder_queues[source]
                                .message_depth == 0U);
            }) &&
            !pipeline->fatal();
    }
    test->Expect(
        owners_parked_at_gate,
        "one popped command per lane waits beyond the applied window");
    if (!owners_parked_at_gate) {
        return false;
    }

    bool queued_behind_owner = true;
    for (std::uint8_t source = 0U;
         source < market::kRealtimeHistorySourceCountV1;
         ++source) {
        const runtime::RealtimePipelineIngressResultV1 admitted =
            InjectSourceMessage(
                pipeline, source, 30'000U + ++next_sequence);
        queued_behind_owner =
            queued_behind_owner && admitted.accepted();
    }
    const bool every_message_capacity_full =
        queued_behind_owner &&
        WaitUntil([&] {
            const runtime::RealtimePipelineSnapshotV1 snapshot =
                pipeline->Snapshot();
            return snapshot.fatal ||
                   (snapshot.accepted_messages == next_sequence &&
                    snapshot.decoded_messages == applied_window &&
                    snapshot.processing_progress.applied_sequence ==
                        0U &&
                    std::all_of(
                        snapshot.decoder_queues.begin(),
                        snapshot.decoder_queues.end(),
                        [](const auto& queue) {
                            return queue.message_capacity == 1U &&
                                   queue.message_depth == 1U &&
                                   queue.total_depth == 1U;
                        }));
        }) &&
        !pipeline->fatal();
    test->Expect(
        every_message_capacity_full,
        "all four source queues reach their logical message capacity");
    return every_message_capacity_full;
}

void CheckEmptyGeneration(
    TestContext* test,
    runtime::RealtimePipelineV1* pipeline) {
    const runtime::RealtimePipelineCutResultV1 cut =
        pipeline->CutAndPublishGeneration(5s);
    test->Expect(
        cut.published(),
        "a no-data daily catalog publishes through parked fences");
    if (!cut.published()) {
        return;
    }
    const auto& catalog =
        cut.store_generation->catalog_snapshot();
    test->Expect(
        catalog != nullptr &&
            catalog->catalog_scope() ==
                market::InstrumentCatalogScopeV2::
                    kDeclaredDailyAShare &&
            catalog->coverage_complete() &&
            catalog->catalog_generation() == 1U &&
            catalog->bound_count() == 2U &&
            catalog->available_count() == 0U &&
            cut.store_generation->instrument_count() == 2U &&
            cut.factor_generation != nullptr &&
            cut.factor_generation->points().empty() &&
            cut.factor_generation->processing_lag_records() == 0U,
        "no-data generation retains the complete frozen catalog");
}

void CheckPopulatedGeneration(
    TestContext* test,
    runtime::RealtimePipelineV1* pipeline,
    market::InstrumentRuntimeStateV2* directory,
    const std::shared_ptr<ProjectionProbe>& projection) {
    FakeMessage snapshot(
        sdk::kProductionMessageKeysV1[2U],
        ShenzhenSnapshotBody(12'345'600));
    FakeMessage order(
        sdk::kProductionMessageKeysV1[3U],
        ShenzhenOrderBody(101U));
    FakeMessage trade(
        sdk::kProductionMessageKeysV1[1U],
        ShanghaiTradeBody(202U));

    const runtime::RealtimePipelineIngressResultV1 snapshot_result =
        pipeline->InjectSdkMessageForTest(&snapshot);
    snapshot.DestroyCallbackBytes();
    const runtime::RealtimePipelineIngressResultV1 order_result =
        pipeline->InjectSdkMessageForTest(&order);
    order.DestroyCallbackBytes();
    const runtime::RealtimePipelineIngressResultV1 trade_result =
        pipeline->InjectSdkMessageForTest(&trade);
    trade.DestroyCallbackBytes();
    test->Expect(
        snapshot_result.accepted() && order_result.accepted() &&
            trade_result.accepted() &&
            snapshot_result.global_ingress_sequence == 1U &&
            order_result.global_ingress_sequence == 2U &&
            trade_result.global_ingress_sequence == 3U,
        "three callback messages enter one dense asynchronous capture prefix");

    const runtime::RealtimePipelineCutResultV1 cut =
        pipeline->CutAndPublishGeneration(5s);
    test->Expect(
        cut.published(),
        "applied prefix publishes as one immutable generation");
    if (!cut.published()) {
        const runtime::RealtimePipelineSnapshotV1 failed =
            pipeline->Snapshot();
        std::cerr
            << "populated cut diagnostic error="
            << runtime::RealtimePipelineCutErrorNameV1(cut.error)
            << " generation_error="
            << market::RealtimeHistoryGenerationErrorNameV1(
                   cut.generation_error)
            << " accepted=" << failed.accepted_messages
            << " decoded=" << failed.decoded_messages
            << " applied="
            << failed.processing_progress.applied_sequence
            << " started=" << failed.last_started_generation
            << " published=" << failed.last_published_generation
            << " fatal=" << failed.fatal << '\n';
        return;
    }

    std::shared_ptr<const market::DailyInstrumentCatalogSnapshotV2>
        catalog;
    test->Expect(
        directory->AcquireSnapshot(&catalog) ==
                market::InstrumentRuntimeStateErrorV2::kNone &&
            catalog != nullptr,
        "live directory snapshot is available");
    if (catalog == nullptr) {
        return;
    }
    test->Expect(
        catalog->capacity() == 2U &&
            catalog->catalog_generation() == 1U &&
            catalog->bound_count() == 2U &&
            catalog->available_count() == 2U &&
            catalog->snapshot_available_count() == 1U &&
            catalog->tick_available_count() == 2U &&
            catalog->factor_eligible_count() == 1U &&
            catalog->coverage_complete(),
        "daily runtime counts satisfy exact snapshot/tick/eligible semantics");

    const realtime::ProcessingProgressV2 progress =
        cut.store_generation->watermark().processing_progress;
    test->Expect(
        progress.accepted_sequence == 3U &&
            progress.applied_sequence == 3U &&
            progress.processing_lag_records() == 0U &&
            cut.store_generation->instrument_count() == 2U,
        "generation carries the exact accepted and applied watermarks");

    market::RealtimeLatestRecordViewV1 latest_snapshot{};
    market::RealtimeLatestRecordViewV1 latest_tick_one{};
    market::RealtimeLatestRecordViewV1 latest_tick_two{};
    test->Expect(
        pipeline->GetLatestSnapshot(2U, &latest_snapshot) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            latest_snapshot.record != nullptr &&
            latest_snapshot.record->instrument_id() == 2U &&
            pipeline->GetLatestTick(2U, &latest_tick_one) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            latest_tick_one.record != nullptr &&
            latest_tick_one.record->instrument_id() == 2U &&
            pipeline->GetLatestTick(1U, &latest_tick_two) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            latest_tick_two.record != nullptr &&
            latest_tick_two.record->instrument_id() == 1U,
        "known session IDs use direct ordinal latest reads");

    const std::shared_ptr<const factor::RealtimeFactorGenerationV1>&
        factor_generation = cut.factor_generation;
    const factor::RealtimeFactorPointV1* const factor_point =
        factor_generation == nullptr
            ? nullptr
            : factor_generation->Find(2U);
    test->Expect(
        factor_generation != nullptr &&
            factor_generation->catalog_scope() ==
                market::InstrumentCatalogScopeV2::
                    kDeclaredDailyAShare &&
            factor_generation->coverage_complete() &&
            factor_generation->bound_count() == 2U &&
            factor_generation->available_count() == 2U &&
            factor_generation->factor_eligible_count() == 1U &&
            factor_generation->points().size() == 1U &&
            factor_point != nullptr &&
            factor_point->values.size() == 1U &&
            factor_point->values[0U].valid &&
            factor_point->values[0U].value == 12.3456,
        "Factor publishes only the exact eligible available subset");

    test->Expect(
        WaitUntil([&] {
            return projection->applied_mask() == 0b111U &&
                   projection->accepted() == 3U &&
                   projection->applied() == 3U &&
                   !projection->coverage_lost();
        }),
        "external data/progress projections expose continuous progress");
}

void CheckMainlandAShareIngressFilter(TestContext* test) {
    constexpr std::array<std::string_view,
                         sdk::kProductionMessageCountV1>
        kNonAShareSecurityIds{
            "900901", "900901", "200001", "200001", "200001"};
    constexpr std::array<std::uint64_t,
                         market::kRealtimeHistorySourceCountV1>
        kOneFilteredPerProductionTuple{1U, 1U, 1U, 2U};

    PipelineCatalogFixture filtered_fixture;
    test->Expect(
        MakeCatalogFixture(21U, &filtered_fixture),
        "create default A-share-filter catalog");
    if (filtered_fixture.runtime_state == nullptr) {
        return;
    }

    const auto filtered_projection =
        std::make_shared<ProjectionProbe>();
    const auto native_observations =
        std::make_shared<NativeSequenceObservationProbe>();
    runtime::RealtimePipelineConfigV1 filtered_config =
        MakeConfig(filtered_fixture, filtered_projection);
    filtered_config.native_sequence_observation_sink =
        native_observations;
    std::unique_ptr<runtime::RealtimePipelineV1> filtered_pipeline;
    std::string detail;
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            filtered_config, &filtered_pipeline, &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            filtered_pipeline != nullptr,
        "create default A-share-filter pipeline: " + detail);
    if (filtered_pipeline == nullptr) {
        return;
    }

    const realtime::OwnedIngressMessagePoolSnapshotV1
        pool_before_filter = filtered_pipeline->Snapshot().ingress_pool;
    bool all_production_tuples_filtered = true;
    for (std::size_t index = 0U;
         index < sdk::kProductionMessageCountV1;
         ++index) {
        FakeMessage message(
            sdk::kProductionMessageKeysV1[index],
            ProductionBody(
                index,
                kNonAShareSecurityIds[index],
                100U + index));
        const runtime::RealtimePipelineIngressResultV1 result =
            filtered_pipeline->InjectSdkMessageForTest(&message);
        message.DestroyCallbackBytes();
        all_production_tuples_filtered =
            all_production_tuples_filtered &&
            result.error ==
                runtime::RealtimePipelineIngressErrorV1::
                    kFilteredNonAShare &&
            runtime::RealtimePipelineIngressErrorNameV1(
                result.error) == "filtered_non_a_share" &&
            !result.accepted() &&
            result.global_ingress_sequence == 0U &&
            result.source_sequence == 0U &&
            result.tick_stream_sequence == 0U;
    }
    const runtime::RealtimePipelineSnapshotV1 after_tuple_filter =
        filtered_pipeline->Snapshot();
    test->Expect(
        all_production_tuples_filtered &&
            after_tuple_filter.accepted_messages == 0U &&
            after_tuple_filter.filtered_messages == 5U &&
            after_tuple_filter.filtered_messages_by_source ==
                kOneFilteredPerProductionTuple &&
            after_tuple_filter.global_ingress_sequence == 0U &&
            after_tuple_filter.tick_stream_sequence == 0U &&
            after_tuple_filter.source_sequences ==
                std::array<std::uint64_t,
                           market::kRealtimeHistorySourceCountV1>{} &&
            after_tuple_filter.ingress_pool.active_messages == 0U &&
            after_tuple_filter.ingress_pool.allocated_blocks ==
                pool_before_filter.allocated_blocks &&
            after_tuple_filter.ingress_pool.cached_blocks ==
                pool_before_filter.cached_blocks &&
            after_tuple_filter.ingress_pool.allocated_bytes ==
                pool_before_filter.allocated_bytes &&
            !after_tuple_filter.fatal,
        "all five production tuples filter non-A shares before ownership "
        "or sequence allocation");

    FakeMessage first_allowed(
        sdk::kProductionMessageKeysV1[1U],
        ShanghaiTradeBody(901U, "600007"));
    FakeMessage between_filtered(
        sdk::kProductionMessageKeysV1[1U],
        ShanghaiTradeBody(902U, "900901"));
    FakeMessage second_allowed(
        sdk::kProductionMessageKeysV1[1U],
        ShanghaiTradeBody(903U, "600007"));
    const runtime::RealtimePipelineIngressResultV1 first_result =
        filtered_pipeline->InjectSdkMessageForTest(&first_allowed);
    first_allowed.DestroyCallbackBytes();
    const runtime::RealtimePipelineIngressResultV1 middle_result =
        filtered_pipeline->InjectSdkMessageForTest(&between_filtered);
    between_filtered.DestroyCallbackBytes();
    const runtime::RealtimePipelineIngressResultV1 second_result =
        filtered_pipeline->InjectSdkMessageForTest(&second_allowed);
    second_allowed.DestroyCallbackBytes();
    test->Expect(
        first_result.accepted() &&
            first_result.global_ingress_sequence == 1U &&
            first_result.source_sequence == 1U &&
            first_result.tick_stream_sequence == 1U &&
            middle_result.error ==
                runtime::RealtimePipelineIngressErrorV1::
                    kFilteredNonAShare &&
            middle_result.global_ingress_sequence == 0U &&
            middle_result.source_sequence == 0U &&
            middle_result.tick_stream_sequence == 0U &&
            second_result.accepted() &&
            second_result.global_ingress_sequence == 2U &&
            second_result.source_sequence == 2U &&
            second_result.tick_stream_sequence == 2U,
        "allowed-filtered-allowed callbacks retain dense global, source, "
        "and mixed-tick sequences");
    const auto& observations =
        native_observations->observations();
    const std::size_t observation_count = observations.size();
    test->Expect(
        native_observations->failures() == 0U &&
            observation_count >= 3U &&
            observations[observation_count - 3U]
                    .descriptor.sequence == 901U &&
            observations[observation_count - 3U].record_class ==
                realtime::NativeSequenceRecoveryRecordClassV1::
                    kTarget &&
            observations[observation_count - 3U]
                    .ingress_sequence == 1U &&
            observations[observation_count - 2U]
                    .descriptor.sequence == 902U &&
            observations[observation_count - 2U].record_class ==
                realtime::NativeSequenceRecoveryRecordClassV1::
                    kFiltered &&
            observations[observation_count - 2U]
                    .ingress_sequence == 0U &&
            observations[observation_count - 1U]
                    .descriptor.sequence == 903U &&
            observations[observation_count - 1U].record_class ==
                realtime::NativeSequenceRecoveryRecordClassV1::
                    kTarget &&
            observations[observation_count - 1U]
                    .ingress_sequence == 2U,
        "native observer sees target-filtered-target in callback order "
        "without assigning a FAST ingress identity to the skip marker");

    const bool filtered_applied_before_cut = WaitUntil([&] {
        const runtime::RealtimePipelineSnapshotV1 snapshot =
            filtered_pipeline->Snapshot();
        return snapshot.fatal ||
               (snapshot.accepted_messages == 2U &&
                snapshot.decoded_messages == 2U &&
                snapshot.processing_progress.accepted_sequence == 2U &&
                snapshot.processing_progress.applied_sequence == 2U &&
                snapshot.store.appended_records == 2U);
    });
    test->Expect(
        filtered_applied_before_cut &&
            !filtered_pipeline->Snapshot().fatal,
        "accepted records surrounding a filter decision fully apply");

    const runtime::RealtimePipelineCutResultV1 filtered_cut =
        filtered_pipeline->CutAndPublishGeneration(5s);
    const runtime::RealtimePipelineSnapshotV1 filtered_snapshot =
        filtered_pipeline->Snapshot();
    std::shared_ptr<
        const market::DailyInstrumentCatalogSnapshotV2>
        filtered_catalog;
    const bool filtered_catalog_ready =
        filtered_fixture.runtime_state->AcquireSnapshot(
            &filtered_catalog) ==
            market::InstrumentRuntimeStateErrorV2::kNone &&
        filtered_catalog != nullptr;
    test->Expect(
        filtered_applied_before_cut && filtered_cut.published() &&
            filtered_cut.store_generation != nullptr &&
            filtered_cut.store_generation->watermark()
                    .processing_progress.accepted_sequence == 2U &&
            filtered_cut.store_generation->watermark()
                    .processing_progress.applied_sequence == 2U &&
            filtered_snapshot.accepted_messages == 2U &&
            filtered_snapshot.filtered_messages == 6U &&
            filtered_snapshot.filtered_messages_by_source ==
                std::array<std::uint64_t,
                           market::kRealtimeHistorySourceCountV1>{
                    1U, 2U, 1U, 2U} &&
            filtered_snapshot.decoded_messages == 2U &&
            filtered_snapshot.global_ingress_sequence == 2U &&
            filtered_snapshot.tick_stream_sequence == 2U &&
            filtered_snapshot.source_sequences ==
                std::array<std::uint64_t,
                           market::kRealtimeHistorySourceCountV1>{
                    0U, 2U, 0U, 0U} &&
            filtered_snapshot.ignored_messages == 0U &&
            filtered_snapshot.rejected_messages == 0U &&
            filtered_snapshot.store.appended_records == 2U &&
            filtered_catalog_ready &&
            filtered_catalog->bound_count() == 2U &&
            WaitUntil([&] {
                return filtered_projection->applied_mask() == 0b11U &&
                       filtered_projection->accepted() == 2U &&
                       filtered_projection->applied() == 2U;
            }) &&
            !filtered_snapshot.fatal,
        "filtered callbacks stay outside the published prefix, Store, and "
        "daily runtime state");
    filtered_pipeline->StopAndDrain();

    PipelineCatalogFixture malformed_fixture;
    test->Expect(
        MakeCatalogFixture(23U, &malformed_fixture),
        "create malformed-key catalog");
    if (malformed_fixture.runtime_state == nullptr) {
        return;
    }
    const auto malformed_projection =
        std::make_shared<ProjectionProbe>();
    std::unique_ptr<runtime::RealtimePipelineV1> malformed_pipeline;
    detail.clear();
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            MakeConfig(malformed_fixture, malformed_projection),
            &malformed_pipeline,
            &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            malformed_pipeline != nullptr,
        "create malformed-key pipeline: " + detail);
    if (malformed_pipeline == nullptr) {
        return;
    }

    FakeMessage malformed(
        sdk::kProductionMessageKeysV1[0U],
        std::vector<std::byte>(10U, std::byte{0U}));
    const runtime::RealtimePipelineIngressResultV1 malformed_result =
        malformed_pipeline->InjectSdkMessageForTest(&malformed);
    const runtime::RealtimePipelineSnapshotV1 malformed_snapshot =
        malformed_pipeline->Snapshot();
    test->Expect(
        malformed_result.error ==
                runtime::RealtimePipelineIngressErrorV1::
                    kInstrumentKeyRejected &&
            !malformed_result.accepted() &&
            malformed_result.global_ingress_sequence == 0U &&
            malformed_result.source_sequence == 0U &&
            malformed_result.tick_stream_sequence == 0U &&
            malformed_snapshot.accepted_messages == 0U &&
            malformed_snapshot.filtered_messages == 0U &&
            malformed_snapshot.rejected_messages == 1U &&
            malformed_snapshot.global_ingress_sequence == 0U &&
            malformed_snapshot.last_decode_error ==
                market::MarketDecodeErrorV1::kTruncated &&
            malformed_snapshot.fatal,
        "malformed required key data fails closed and is not counted as "
        "a normal filter decision");
    malformed_pipeline->StopAndDrain();
}

void CheckCatalogMissFailsBeforeCommit(TestContext* test) {
    PipelineCatalogFixture fixture;
    test->Expect(
        MakeCatalogFixture(24U, &fixture),
        "create catalog-miss validation fixture");
    if (fixture.runtime_state == nullptr) {
        return;
    }
    const auto projection = std::make_shared<ProjectionProbe>();
    runtime::RealtimePipelineConfigV1 config =
        MakeConfig(fixture, projection);
    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            config, &pipeline, &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create catalog-miss validation pipeline: " + detail);
    if (pipeline == nullptr) {
        return;
    }

    const realtime::OwnedIngressMessagePoolSnapshotV1 before =
        pipeline->Snapshot().ingress_pool;
    FakeMessage missing(
        sdk::kProductionMessageKeysV1[1U],
        ShanghaiTradeBody(77U, "600008"));
    const runtime::RealtimePipelineIngressResultV1 result =
        pipeline->InjectSdkMessageForTest(&missing);
    missing.DestroyCallbackBytes();
    const runtime::RealtimePipelineSnapshotV1 after =
        pipeline->Snapshot();
    test->Expect(
        result.error ==
                runtime::RealtimePipelineIngressErrorV1::kCatalogMiss &&
            !result.accepted() &&
            result.global_ingress_sequence == 0U &&
            result.source_sequence == 0U &&
            result.tick_stream_sequence == 0U &&
            after.accepted_messages == 0U &&
            after.global_ingress_sequence == 0U &&
            after.tick_stream_sequence == 0U &&
            after.source_sequences ==
                std::array<std::uint64_t,
                           market::kRealtimeHistorySourceCountV1>{} &&
            after.ingress_pool.active_messages == 0U &&
            after.ingress_pool.allocated_blocks ==
                before.allocated_blocks &&
            after.ingress_pool.allocated_bytes ==
                before.allocated_bytes &&
            after.rejected_messages == 1U && after.fatal,
        "valid A-share catalog miss fails closed before pool or sequence "
        "commit");
    pipeline->StopAndDrain();
}

void CheckExplicitAppliedDispatchWindow(
    TestContext* test) {
    PipelineCatalogFixture fixture;
    test->Expect(
        MakeCatalogFixture(18U, &fixture),
        "create dispatch-window catalog");
    if (fixture.runtime_state == nullptr) {
        return;
    }
    const auto projection =
        std::make_shared<ProjectionProbe>(true);
    runtime::RealtimePipelineConfigV1 config =
        MakeConfig(fixture, projection);
    config.decoder_queue_capacity_per_source = 1U;
    config.store_worker_count = 2U;
    config.store_queue_capacity_per_source_worker = 64U;

    std::size_t window = 0U;
    test->Expect(
        runtime::RealtimePipelineAppliedWindowCapacityV1(
            config, &window) &&
            window == 8U,
        "derive the explicit eight-record applied dispatch window");

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            config, &pipeline, &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create dispatch-window pipeline: " + detail);
    if (pipeline == nullptr) {
        return;
    }

    FakeMessage first(
        sdk::kProductionMessageKeysV1[2U],
        ShenzhenSnapshotBody(12'345'600));
    const runtime::RealtimePipelineIngressResultV1 first_result =
        pipeline->InjectSdkMessageForTest(&first);
    first.DestroyCallbackBytes();
    test->Expect(
        first_result.accepted() &&
            projection->WaitUntilFirstAppliedBlocked(3s),
        "sequence one blocks only its Store worker");

    bool all_callbacks_accepted = first_result.accepted();
    for (std::uint64_t sequence = 2U;
         sequence <= static_cast<std::uint64_t>(window) + 1U;
         ++sequence) {
        FakeMessage later(
            sdk::kProductionMessageKeysV1[1U],
            ShanghaiTradeBody(200U + sequence));
        const runtime::RealtimePipelineIngressResultV1 admitted =
            pipeline->InjectSdkMessageForTest(&later);
        later.DestroyCallbackBytes();
        all_callbacks_accepted =
            all_callbacks_accepted && admitted.accepted();
        if (sequence <= static_cast<std::uint64_t>(window)) {
            test->Expect(
                WaitUntil([&] {
                    return pipeline->Snapshot()
                               .decoded_messages >= sequence;
                }),
                "sequence inside the applied window dispatches");
        }
    }
    test->Expect(
        all_callbacks_accepted,
        "callback admission remains nonblocking at the completion window");
    test->Expect(
        WaitUntil([&] {
            const runtime::RealtimePipelineSnapshotV1 snapshot =
                pipeline->Snapshot();
            return snapshot.decoded_messages == window &&
                   snapshot.processing_progress.accepted_sequence ==
                       window + 1U &&
                   snapshot.processing_progress.applied_sequence == 0U &&
                   !snapshot.fatal;
        }),
        "W+1 is accepted but cannot overrun the W-sized completion tracker");

    projection->ReleaseFirstApplied();
    const runtime::RealtimePipelineCutResultV1 cut =
        pipeline->CutAndPublishGeneration(5s);
    test->Expect(
        cut.published() &&
            cut.store_generation->watermark()
                    .processing_progress.applied_sequence ==
                window + 1U &&
            pipeline->Snapshot().decoded_messages == window + 1U &&
            !pipeline->fatal(),
        "releasing the gap advances the exact prefix through W+1");
    pipeline->StopAndDrain();
}

void CheckReservedFenceWithEverySourceQueueFull(
    TestContext* test) {
    PipelineCatalogFixture fixture;
    test->Expect(
        MakeCatalogFixture(25U, &fixture),
        "create full-lane generation-fence catalog");
    if (fixture.runtime_state == nullptr) {
        return;
    }

    const auto projection =
        std::make_shared<ProjectionProbe>(true);
    runtime::RealtimePipelineConfigV1 config =
        MakeConfig(fixture, projection);
    config.decoder_queue_capacity_per_source = 1U;
    config.store_worker_count = 2U;
    config.store_queue_capacity_per_source_worker = 64U;

    std::size_t applied_window = 0U;
    test->Expect(
        runtime::RealtimePipelineAppliedWindowCapacityV1(
            config, &applied_window) &&
            applied_window == 8U,
        "derive full-lane fence applied window");

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            config, &pipeline, &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create full-lane generation-fence pipeline: " + detail);
    if (pipeline == nullptr) {
        return;
    }

    const bool prepared =
        PrepareEverySourceQueueFullAtAppliedGap(
            test, pipeline.get(), projection, applied_window);
    if (!prepared) {
        projection->ReleaseFirstApplied();
        pipeline->StopAndDrain();
        return;
    }

    runtime::RealtimePipelineCutResultV1 first_cut{};
    std::atomic<bool> first_cut_finished{false};
    std::thread cut_thread([&] {
        first_cut = pipeline->CutAndPublishGeneration(10s);
        first_cut_finished.store(true, std::memory_order_release);
    });

    const bool reserved_fences_visible =
        WaitUntil([&] {
            const runtime::RealtimePipelineSnapshotV1 snapshot =
                pipeline->Snapshot();
            return snapshot.fatal ||
                   std::all_of(
                       snapshot.decoder_queues.begin(),
                       snapshot.decoder_queues.end(),
                       [](const auto& queue) {
                           return queue.message_capacity == 1U &&
                                  queue.message_depth == 1U &&
                                  queue.total_depth == 2U;
                       });
        }) &&
        !pipeline->fatal();
    test->Expect(
        reserved_fences_visible &&
            !first_cut_finished.load(std::memory_order_acquire),
        "each full source queue admits one reserved parked fence");

    projection->ReleaseFirstApplied();
    const bool first_cut_completed =
        WaitUntil([&] {
            return first_cut_finished.load(std::memory_order_acquire);
        });
    cut_thread.join();

    const runtime::RealtimePipelineSnapshotV1 after_first =
        pipeline->Snapshot();
    const bool first_cut_valid =
        reserved_fences_visible && first_cut_completed &&
            first_cut.published() &&
            first_cut.store_generation != nullptr &&
            first_cut.store_generation->watermark()
                    .processing_progress.accepted_sequence == 16U &&
            first_cut.store_generation->watermark()
                    .processing_progress.applied_sequence == 16U &&
            after_first.last_started_generation == 1U &&
            after_first.last_published_generation == 1U &&
            after_first.accepted_messages == 16U &&
            after_first.decoded_messages == 16U &&
            after_first.processing_progress.applied_sequence == 16U &&
            std::all_of(
                after_first.decoder_queues.begin(),
                after_first.decoder_queues.end(),
                [](const auto& queue) {
                    return queue.message_depth == 0U &&
                           queue.total_depth == 0U &&
                           queue.message_high_water == 1U &&
                           queue.full_count == 0U;
                }) &&
            !after_first.fatal;
    test->Expect(
        first_cut_valid,
        "full-lane cut drains the exact prefix without deadlock");
    if (!first_cut_valid) {
        std::cerr
            << "full-lane cut diagnostic error="
            << runtime::RealtimePipelineCutErrorNameV1(first_cut.error)
            << " generation_error="
            << market::RealtimeHistoryGenerationErrorNameV1(
                   first_cut.generation_error)
            << " reserved=" << reserved_fences_visible
            << " finished=" << first_cut_completed
            << " started=" << after_first.last_started_generation
            << " published=" << after_first.last_published_generation
            << " accepted=" << after_first.accepted_messages
            << " decoded=" << after_first.decoded_messages
            << " applied="
            << after_first.processing_progress.applied_sequence
            << " fatal=" << after_first.fatal << '\n';
    }

    const runtime::RealtimePipelineCutResultV1 second_cut =
        pipeline->CutAndPublishGeneration(5s);
    const runtime::RealtimePipelineSnapshotV1 after_second =
        pipeline->Snapshot();
    const bool second_cut_valid =
        second_cut.published() &&
            second_cut.store_generation != nullptr &&
            second_cut.store_generation->watermark().generation == 2U &&
            second_cut.store_generation->watermark()
                    .processing_progress.accepted_sequence == 16U &&
            after_second.last_started_generation == 2U &&
            after_second.last_published_generation == 2U &&
            std::all_of(
                after_second.decoder_queues.begin(),
                after_second.decoder_queues.end(),
                [](const auto& queue) {
                    return queue.message_depth == 0U &&
                           queue.total_depth == 0U;
                }) &&
            !after_second.fatal;
    test->Expect(
        second_cut_valid,
        "a consecutive generation reuses every reserved control slot");
    if (!second_cut_valid) {
        std::cerr
            << "second cut diagnostic error="
            << runtime::RealtimePipelineCutErrorNameV1(second_cut.error)
            << " generation_error="
            << market::RealtimeHistoryGenerationErrorNameV1(
                   second_cut.generation_error)
            << " started=" << after_second.last_started_generation
            << " published=" << after_second.last_published_generation
            << " fatal=" << after_second.fatal << '\n';
    }
    pipeline->StopAndDrain();
}

void CheckGenerationAndStopConcurrentExit(
    TestContext* test) {
    PipelineCatalogFixture fixture;
    test->Expect(
        MakeCatalogFixture(26U, &fixture),
        "create generation-stop concurrency catalog");
    if (fixture.runtime_state == nullptr) {
        return;
    }

    const auto projection =
        std::make_shared<ProjectionProbe>(true);
    runtime::RealtimePipelineConfigV1 config =
        MakeConfig(fixture, projection);
    config.decoder_queue_capacity_per_source = 1U;
    config.store_worker_count = 2U;
    config.store_queue_capacity_per_source_worker = 64U;
    std::size_t applied_window = 0U;
    test->Expect(
        runtime::RealtimePipelineAppliedWindowCapacityV1(
            config, &applied_window) &&
            applied_window == 8U,
        "derive generation-stop applied window");

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            config, &pipeline, &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create generation-stop concurrency pipeline: " + detail);
    if (pipeline == nullptr) {
        return;
    }

    const bool prepared =
        PrepareEverySourceQueueFullAtAppliedGap(
            test, pipeline.get(), projection, applied_window);
    if (!prepared) {
        projection->ReleaseFirstApplied();
        pipeline->StopAndDrain();
        return;
    }

    runtime::RealtimePipelineCutResultV1 cut{};
    std::atomic<bool> cut_finished{false};
    std::thread cut_thread([&] {
        cut = pipeline->CutAndPublishGeneration(10s);
        cut_finished.store(true, std::memory_order_release);
    });
    const bool fences_inserted =
        WaitUntil([&] {
            const runtime::RealtimePipelineSnapshotV1 snapshot =
                pipeline->Snapshot();
            return snapshot.fatal ||
                   std::all_of(
                       snapshot.decoder_queues.begin(),
                       snapshot.decoder_queues.end(),
                       [](const auto& queue) {
                           return queue.message_depth == 1U &&
                                  queue.total_depth == 2U;
                       });
        }) &&
        !pipeline->fatal();
    test->Expect(
        fences_inserted &&
            !cut_finished.load(std::memory_order_acquire),
        "generation owns the cut while all full lanes await fences");
    if (!fences_inserted) {
        projection->ReleaseFirstApplied();
        cut_thread.join();
        pipeline->StopAndDrain();
        return;
    }

    std::atomic<bool> stop_started{false};
    std::atomic<bool> stop_finished{false};
    std::thread stop_thread([&] {
        stop_started.store(true, std::memory_order_release);
        pipeline->StopAndDrain();
        stop_finished.store(true, std::memory_order_release);
    });
    test->Expect(
        WaitUntil([&] {
            return stop_started.load(std::memory_order_acquire);
        }) &&
            !cut_finished.load(std::memory_order_acquire) &&
            !stop_finished.load(std::memory_order_acquire),
        "stop serializes behind the active parked generation cut");

    projection->ReleaseFirstApplied();
    const bool both_finished =
        WaitUntil([&] {
            return cut_finished.load(std::memory_order_acquire) &&
                   stop_finished.load(std::memory_order_acquire);
        });
    cut_thread.join();
    stop_thread.join();

    const runtime::RealtimePipelineSnapshotV1 stopped =
        pipeline->Snapshot();
    const bool generation_stop_valid =
        both_finished && cut.published() &&
            cut.store_generation != nullptr &&
            cut.store_generation->watermark().generation == 1U &&
            stopped.stopped && !stopped.accepting &&
            stopped.last_published_generation == 1U &&
            stopped.processing_progress.accepted_sequence == 16U &&
            stopped.processing_progress.applied_sequence == 16U &&
            std::all_of(
                stopped.decoder_queues.begin(),
                stopped.decoder_queues.end(),
                [](const auto& queue) {
                    return queue.message_depth == 0U &&
                           queue.total_depth == 0U;
                }) &&
            !stopped.fatal;
    test->Expect(
        generation_stop_valid,
        "concurrent generation and stop release and join every lane");
    if (!generation_stop_valid) {
        std::cerr
            << "generation-stop diagnostic both_finished="
            << both_finished
            << " cut_error="
            << runtime::RealtimePipelineCutErrorNameV1(cut.error)
            << " generation_error="
            << market::RealtimeHistoryGenerationErrorNameV1(
                   cut.generation_error)
            << " cut_finished="
            << cut_finished.load(std::memory_order_acquire)
            << " stop_finished="
            << stop_finished.load(std::memory_order_acquire)
            << " accepted="
            << stopped.processing_progress.accepted_sequence
            << " applied="
            << stopped.processing_progress.applied_sequence
            << " started=" << stopped.last_started_generation
            << " published=" << stopped.last_published_generation
            << " stopped=" << stopped.stopped
            << " fatal=" << stopped.fatal << '\n';
    }
}

void CheckSourceDecoderQueueFullKeepsAcceptedPrefix(
    TestContext* test) {
    PipelineCatalogFixture fixture;
    test->Expect(
        MakeCatalogFixture(22U, &fixture),
        "create source-queue-full catalog");
    if (fixture.runtime_state == nullptr) {
        return;
    }

    const auto projection =
        std::make_shared<ProjectionProbe>(true);
    runtime::RealtimePipelineConfigV1 config =
        MakeConfig(fixture, projection);
    config.decoder_queue_capacity_per_source = 1U;
    config.store_worker_count = 2U;
    config.store_queue_capacity_per_source_worker = 64U;

    std::size_t window = 0U;
    test->Expect(
        runtime::RealtimePipelineAppliedWindowCapacityV1(
            config, &window) &&
            window == 8U,
        "derive completion window for queue-full test");

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            config, &pipeline, &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create queue-full pipeline: " + detail);
    if (pipeline == nullptr) {
        return;
    }

    FakeMessage first(
        sdk::kProductionMessageKeysV1[2U],
        ShenzhenSnapshotBody(12'345'600));
    const runtime::RealtimePipelineIngressResultV1 first_result =
        pipeline->InjectSdkMessageForTest(&first);
    first.DestroyCallbackBytes();
    test->Expect(
        first_result.accepted() &&
            projection->WaitUntilFirstAppliedBlocked(3s),
        "queue-full test blocks the first applied sequence");

    bool prefix_accepted = first_result.accepted();
    runtime::RealtimePipelineIngressResultV1 last_accepted_result =
        first_result;
    for (std::uint64_t sequence = 2U;
         sequence <= static_cast<std::uint64_t>(window) + 1U;
         ++sequence) {
        FakeMessage message(
            sdk::kProductionMessageKeysV1[1U],
            ShanghaiTradeBody(400U + sequence));
        const runtime::RealtimePipelineIngressResultV1 result =
            pipeline->InjectSdkMessageForTest(&message);
        message.DestroyCallbackBytes();
        prefix_accepted = prefix_accepted && result.accepted();
        if (result.accepted()) {
            last_accepted_result = result;
        }
        if (sequence <= static_cast<std::uint64_t>(window)) {
            test->Expect(
                WaitUntil([&] {
                    return pipeline->Snapshot().decoded_messages >=
                           sequence;
                }),
                "queue-full prefix dispatches within the completion window");
        }
    }
    test->Expect(
        prefix_accepted &&
            WaitUntil([&] {
                const auto snapshot = pipeline->Snapshot();
                return snapshot.decoded_messages == window &&
                       snapshot.processing_progress
                               .accepted_sequence ==
                           window + 1U &&
                       snapshot.processing_progress.applied_sequence ==
                           0U &&
                       !snapshot.fatal;
            }),
        "source decoder is held at the applied gate with one queue slot free");

    runtime::RealtimePipelineIngressResultV1 overflow_result{};
    bool saw_overflow = false;
    for (std::uint64_t attempt = 0U;
         attempt < 3U && !saw_overflow;
         ++attempt) {
        FakeMessage candidate(
            sdk::kProductionMessageKeysV1[1U],
            ShanghaiTradeBody(500U + attempt));
        const runtime::RealtimePipelineIngressResultV1 result =
            pipeline->InjectSdkMessageForTest(&candidate);
        candidate.DestroyCallbackBytes();
        if (result.accepted()) {
            last_accepted_result = result;
        } else {
            overflow_result = result;
            saw_overflow = true;
        }
    }
    const runtime::RealtimePipelineSnapshotV1 failed =
        pipeline->Snapshot();
    const bool prefix_preserved =
        saw_overflow &&
            overflow_result.error ==
                runtime::RealtimePipelineIngressErrorV1::
                    kDecoderAdmissionFailed &&
            !overflow_result.accepted() &&
            overflow_result.global_ingress_sequence == 0U &&
            overflow_result.source_sequence == 0U &&
            overflow_result.tick_stream_sequence == 0U &&
            failed.accepted_messages ==
                last_accepted_result.global_ingress_sequence &&
            failed.global_ingress_sequence ==
                last_accepted_result.global_ingress_sequence &&
            failed.processing_progress.accepted_sequence ==
                last_accepted_result.global_ingress_sequence &&
            failed.processing_progress.applied_sequence <=
                failed.processing_progress.accepted_sequence &&
            failed.processing_progress.processing_lag_records() ==
                failed.processing_progress.accepted_sequence -
                    failed.processing_progress.applied_sequence &&
            failed.tick_stream_sequence ==
                last_accepted_result.tick_stream_sequence &&
            failed.source_sequences[
                last_accepted_result.source_slot] ==
                last_accepted_result.source_sequence &&
            failed.rejected_messages == 1U && failed.fatal;
    test->Expect(
        prefix_preserved,
        "queue-full failure does not advance any accepted capture counter");
    if (!prefix_preserved) {
        std::cerr
            << "queue-full diagnostic saw_overflow=" << saw_overflow
            << " error="
            << runtime::RealtimePipelineIngressErrorNameV1(
                   overflow_result.error)
            << " overflow_global="
            << overflow_result.global_ingress_sequence
            << " overflow_source=" << overflow_result.source_sequence
            << " overflow_tick="
            << overflow_result.tick_stream_sequence
            << " expected=" << last_accepted_result.global_ingress_sequence
            << " accepted_messages=" << failed.accepted_messages
            << " global=" << failed.global_ingress_sequence
            << " accepted="
            << failed.processing_progress.accepted_sequence
            << " applied="
            << failed.processing_progress.applied_sequence
            << " expected_tick="
            << last_accepted_result.tick_stream_sequence
            << " tick=" << failed.tick_stream_sequence
            << " expected_source="
            << last_accepted_result.source_sequence
            << " source="
            << failed.source_sequences[
                   last_accepted_result.source_slot]
            << " rejected=" << failed.rejected_messages
            << " fatal=" << failed.fatal << '\n';
    }

    projection->ReleaseFirstApplied();
    pipeline->StopAndDrain();
}

void CheckOpeningBurstCapacityHeadroom(TestContext* test) {
    constexpr std::size_t legacy_decoder_capacity = 4'096U;
    constexpr std::size_t production_decoder_capacity = 65'536U;
    constexpr std::size_t production_store_capacity = 32'768U;
    constexpr std::size_t completion_capacity = 4'098U;
    constexpr std::uint64_t burst_records = 9'000U;

    const auto run_burst = [test](
                               std::uint64_t session_epoch,
                               std::size_t decoder_capacity,
                               bool expect_admission_failure) {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(session_epoch, &fixture),
            "create controlled opening-burst catalog");
        if (fixture.runtime_state == nullptr) {
            return;
        }

        const auto blocker =
            std::make_shared<OpeningBurstAppliedBlocker>();
        const std::shared_ptr<ProjectionProbe> no_projection;
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture, no_projection);
        config.decoder_queue_capacity_per_source =
            decoder_capacity;
        config.completion_tracker_capacity = completion_capacity;
        config.tick_ring_capacity = completion_capacity;
        config.store_worker_count = 2U;
        config.store_queue_capacity_per_source_worker =
            production_store_capacity;
        config.intraday_store.maximum_session_records =
            burst_records + 64U;
        config.intraday_store.maximum_session_accounted_bytes =
            256U * 1024U * 1024U;
        config.applied_record_sink = blocker;

        std::size_t applied_window = 0U;
        test->Expect(
            runtime::RealtimePipelineAppliedWindowCapacityV1(
                config, &applied_window) &&
                applied_window == completion_capacity - 1U,
            "controlled opening burst has a fixed decoder drain window");

        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        test->Expect(
            runtime::RealtimePipelineV1::Create(
                config, &pipeline, &detail) ==
                    runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr,
            "create controlled opening-burst pipeline: " + detail);
        if (pipeline == nullptr) {
            return;
        }

        FakeMessage first(
            sdk::kProductionMessageKeysV1[2U],
            ShenzhenSnapshotBody(12'345'600));
        const runtime::RealtimePipelineIngressResultV1 first_result =
            pipeline->InjectSdkMessageForTest(&first);
        first.DestroyCallbackBytes();
        const bool first_blocked =
            first_result.accepted() &&
            blocker->WaitUntilFirstAppliedBlocked(3s);
        test->Expect(
            first_blocked,
            "controlled opening burst blocks the first applied record");
        if (!first_blocked) {
            blocker->ReleaseFirstApplied();
            pipeline->StopAndDrain();
            return;
        }

        bool all_accepted = true;
        runtime::RealtimePipelineIngressResultV1 failure{};
        for (std::uint64_t sequence = 2U;
             sequence <= burst_records;
             ++sequence) {
            FakeMessage message(
                sdk::kProductionMessageKeysV1[1U],
                ShanghaiTradeBody(20'000U + sequence));
            const runtime::RealtimePipelineIngressResultV1 result =
                pipeline->InjectSdkMessageForTest(&message);
            message.DestroyCallbackBytes();
            if (!result.accepted()) {
                all_accepted = false;
                failure = result;
                break;
            }
        }

        const runtime::RealtimePipelineSnapshotV1 held =
            pipeline->Snapshot();
        constexpr std::size_t shanghai_tick_source = 1U;
        if (expect_admission_failure) {
            test->Expect(
                !all_accepted &&
                    failure.error ==
                        runtime::RealtimePipelineIngressErrorV1::
                            kDecoderAdmissionFailed &&
                    held.accepted_messages < burst_records &&
                    held.rejected_messages == 1U && held.fatal &&
                    held.decoder_queues[shanghai_tick_source]
                            .message_capacity ==
                        legacy_decoder_capacity &&
                    held.decoder_queues[shanghai_tick_source]
                            .full_count == 1U,
                "the legacy 4096-record decoder queue reproducibly "
                "fails closed under the controlled 9000-record burst");
        } else {
            const auto& source_queue =
                held.decoder_queues[shanghai_tick_source];
            test->Expect(
                all_accepted &&
                    held.accepted_messages == burst_records &&
                    held.processing_progress.accepted_sequence ==
                        burst_records &&
                    held.processing_progress.applied_sequence == 0U &&
                    source_queue.message_capacity ==
                        production_decoder_capacity &&
                    source_queue.message_high_water >
                        legacy_decoder_capacity &&
                    source_queue.full_count == 0U && !held.fatal,
                "the 65536-record production decoder queue admits the "
                "same controlled burst with explicit headroom");
        }

        blocker->ReleaseFirstApplied();
        if (!expect_admission_failure && all_accepted) {
            test->Expect(
                WaitUntil([&] {
                    const runtime::RealtimePipelineSnapshotV1 snapshot =
                        pipeline->Snapshot();
                    return snapshot.fatal ||
                           (snapshot.decoded_messages == burst_records &&
                            snapshot.processing_progress
                                    .applied_sequence ==
                                burst_records &&
                            snapshot.store.appended_records ==
                                burst_records);
                }) &&
                    !pipeline->fatal() &&
                    blocker->applied_calls() == burst_records,
                "the enlarged decoder and 32768-record Store queues drain "
                "the exact accepted burst after the consumer resumes");
        }
        pipeline->StopAndDrain();
    };

    run_burst(26U, legacy_decoder_capacity, true);
    run_burst(27U, production_decoder_capacity, false);
}

void CheckFastDecoderAcceptedPublicationAndIdleBoundary(
    TestContext* test) {
    PipelineCatalogFixture fixture;
    test->Expect(
        MakeCatalogFixture(21U, &fixture),
        "create decoder-queue idle-boundary catalog");
    if (fixture.runtime_state == nullptr) {
        return;
    }

    constexpr std::uint64_t rounds = 4096U;
    constexpr std::uint64_t records_per_round = 2U;
    constexpr std::uint64_t total_records =
        rounds * records_per_round;
    const std::shared_ptr<ProjectionProbe> no_projection;
    runtime::RealtimePipelineConfigV1 config =
        MakeConfig(fixture, no_projection);
    config.decoder_queue_capacity_per_source = 2U;
    config.store_queue_capacity_per_source_worker = 8U;
    config.intraday_store.maximum_session_records =
        total_records + 64U;
    config.intraday_store.maximum_session_accounted_bytes =
        128U * 1024U * 1024U;

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            config, &pipeline, &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create fast direct-decoder idle-boundary pipeline: " + detail);
    if (pipeline == nullptr) {
        return;
    }

    std::uint64_t expected = 0U;
    bool completed = true;
    bool accepted_frontier_violation = false;
    for (std::uint64_t round = 0U;
         round < rounds && completed;
         ++round) {
        for (std::uint64_t index = 0U;
             index < records_per_round;
             ++index) {
            ++expected;
            FakeMessage message(
                sdk::kProductionMessageKeysV1[1U],
                ShanghaiTradeBody(10'000U + expected));
            const runtime::RealtimePipelineIngressResultV1 admitted =
                pipeline->InjectSdkMessageForTest(&message);
            message.DestroyCallbackBytes();
            if (!admitted.accepted()) {
                completed = false;
                break;
            }
            const runtime::RealtimePipelineSnapshotV1 after_admission =
                pipeline->Snapshot();
            if (admitted.global_ingress_sequence != expected ||
                admitted.source_sequence != expected ||
                admitted.tick_stream_sequence != expected ||
                after_admission.accepted_messages != expected ||
                after_admission.global_ingress_sequence != expected ||
                after_admission.processing_progress
                        .accepted_sequence != expected ||
                after_admission.processing_progress.applied_sequence >
                    after_admission.processing_progress
                        .accepted_sequence ||
                after_admission.decoded_messages >
                    after_admission.accepted_messages) {
                accepted_frontier_violation = true;
                completed = false;
                break;
            }

            if (index + 1U < records_per_round) {
                // There is no public seam inside TryPushWithCommit that can
                // pause specifically between its accepted-counter commit and
                // release tail publication. Repeatedly letting the decoder
                // race one short burst is the strongest end-to-end check:
                // CompleteAppliedSequence fail-closes if any fast consumer
                // finishes beyond the accepted frontier, and the terminal
                // record also exposes a lost wakeup without a later push.
                std::this_thread::yield();
            }
        }

        if (completed) {
            completed = WaitUntil([&] {
                const runtime::RealtimePipelineSnapshotV1 snapshot =
                    pipeline->Snapshot();
                if (snapshot.processing_progress.applied_sequence >
                        snapshot.processing_progress
                            .accepted_sequence ||
                    snapshot.decoded_messages >
                        snapshot.accepted_messages) {
                    accepted_frontier_violation = true;
                    return true;
                }
                return snapshot.fatal ||
                       (snapshot.accepted_messages == expected &&
                        snapshot.decoded_messages == expected &&
                        snapshot.processing_progress.accepted_sequence ==
                            expected &&
                        snapshot.processing_progress.applied_sequence ==
                            expected &&
                        snapshot.store.appended_records == expected);
            });
            if (completed &&
                (pipeline->Snapshot().fatal ||
                 accepted_frontier_violation)) {
                completed = false;
            }
        }
    }

    const runtime::RealtimePipelineSnapshotV1 final =
        pipeline->Snapshot();
    test->Expect(
        completed && !accepted_frontier_violation &&
            expected == total_records &&
            final.accepted_messages == total_records &&
            final.decoded_messages == total_records &&
            final.processing_progress.accepted_sequence ==
                total_records &&
            final.processing_progress.applied_sequence ==
                total_records &&
            final.store.appended_records == total_records &&
            !final.fatal,
        "short bursts preserve the exact applied prefix for the terminal "
        "message across repeated direct-decoder idle boundaries");
    pipeline->StopAndDrain();
}

}  // namespace

int main() {
    TestContext test;
    PipelineCatalogFixture fixture;
    test.Expect(
        MakeCatalogFixture(17U, &fixture),
        "frozen daily catalog and runtime state create");
    if (fixture.runtime_state == nullptr) {
        return 1;
    }

    const auto projection = std::make_shared<ProjectionProbe>();
    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test.Expect(
        runtime::RealtimePipelineV1::Create(
            MakeConfig(fixture, projection),
            &pipeline,
            &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "daily-catalog direct-decoder pipeline creates: " + detail);
    if (pipeline == nullptr) {
        return 1;
    }

    CheckEmptyGeneration(&test, pipeline.get());
    CheckPopulatedGeneration(
        &test, pipeline.get(), fixture.runtime_state.get(), projection);
    CheckMainlandAShareIngressFilter(&test);
    CheckCatalogMissFailsBeforeCommit(&test);

    const runtime::RealtimePipelineSnapshotV1 before_stop =
        pipeline->Snapshot();
    test.Expect(
        before_stop.accepted_messages == 3U &&
            before_stop.decoded_messages == 3U &&
            before_stop.processing_progress.accepted_sequence == 3U &&
            before_stop.processing_progress.applied_sequence == 3U &&
            !before_stop.fatal,
        "pipeline snapshot exposes a healthy processing prefix");

    pipeline->StopAndDrain();
    const runtime::RealtimePipelineSnapshotV1 stopped =
        pipeline->Snapshot();
    test.Expect(
        stopped.stopped && !stopped.accepting && !stopped.fatal &&
            stopped.processing_progress.accepted_sequence == 3U &&
            stopped.processing_progress.applied_sequence == 3U,
        "all processing workers stop after draining");

    CheckExplicitAppliedDispatchWindow(&test);
    CheckReservedFenceWithEverySourceQueueFull(&test);
    CheckGenerationAndStopConcurrentExit(&test);
    CheckSourceDecoderQueueFullKeepsAcceptedPrefix(&test);
    CheckOpeningBurstCapacityHeadroom(&test);
    CheckStoreGenerationSinkOrderingAndFailure(&test);
    CheckFastDecoderAcceptedPublicationAndIdleBoundary(&test);

    return test.failures() == 0 ? 0 : 1;
}
