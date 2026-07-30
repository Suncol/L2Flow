#include "l2flow/runtime/realtime_pipeline_v1.h"
#include "l2flow/sdk/market_message_catalog_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

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
      public market::ObservedInstrumentBindingSinkV2,
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

    [[nodiscard]] bool PublishObservedInstrumentBinding(
        const market::ObservedInstrumentBindResultV2& binding)
        noexcept override {
        if (!binding.newly_bound || !binding.entry.bound() ||
            binding.entry.instrument_id !=
                binding.entry.ordinal + 1U ||
            binding.catalog_generation != binding.bound_count ||
            binding.bound_count == 0U ||
            binding.bound_count > 63U) {
            return false;
        }
        const std::uint64_t mask =
            std::uint64_t{1U}
            << static_cast<unsigned int>(binding.bound_count - 1U);
        binding_mask_.fetch_or(mask, std::memory_order_release);
        return true;
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
    [[nodiscard]] std::uint64_t binding_mask() const noexcept {
        return binding_mask_.load(std::memory_order_acquire);
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
    std::atomic<std::uint64_t> binding_mask_{0U};
    std::atomic<std::uint64_t> accepted_{0U};
    std::atomic<std::uint64_t> applied_{0U};
    std::atomic<bool> coverage_lost_{false};
    bool block_first_applied_ = false;
    mutable std::mutex gate_mutex_;
    std::condition_variable gate_cv_;
    bool first_applied_blocked_ = false;
    bool release_first_applied_ = false;
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
                market::ObservedInstrumentCatalogScopeV2::kObservedOnly ||
            generation.watermark().catalog_snapshot->coverage_complete() ||
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

[[nodiscard]] runtime::RealtimePipelineConfigV1 MakeConfig(
    market::ObservedInstrumentDirectoryV2* directory,
    const std::shared_ptr<ProjectionProbe>& projection) {
    runtime::RealtimePipelineConfigV1 config{};
    config.run_id[0U] = std::byte{0x31U};
    config.run_id[15U] = std::byte{0x73U};
    config.trade_date = 20260729U;
    config.directory = directory;
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
    config.instrument_binding_sink = projection;
    config.processing_progress_sink = projection;
    config.sdk.enabled = false;
    return config;
}

void CheckStoreGenerationSinkOrderingAndFailure(
    TestContext* test) {
    std::unique_ptr<market::ObservedInstrumentDirectoryV2> directory;
    test->Expect(
        market::ObservedInstrumentDirectoryV2::Create(
            market::ObservedInstrumentDirectoryConfigV2{4U, 20U},
            &directory) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            directory != nullptr,
        "create store-generation-sink directory");
    if (directory == nullptr) {
        return;
    }

    const auto projection = std::make_shared<ProjectionProbe>();
    const auto order = std::make_shared<GenerationPublicationOrder>();
    const auto sink =
        std::make_shared<StoreGenerationSinkProbe>(order);
    const auto calculator =
        std::make_shared<OrderedFactorCalculator>(order);
    runtime::RealtimePipelineConfigV1 config =
        MakeConfig(directory.get(), projection);
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

void CheckEmptyGeneration(
    TestContext* test,
    runtime::RealtimePipelineV1* pipeline) {
    const runtime::RealtimePipelineCutResultV1 cut =
        pipeline->CutAndPublishGeneration(5s);
    test->Expect(
        cut.published(),
        "an empty OBSERVED_ONLY catalog publishes without a T3 gate");
    if (!cut.published()) {
        return;
    }
    const auto& catalog =
        cut.store_generation->catalog_snapshot();
    test->Expect(
        catalog != nullptr &&
            catalog->catalog_scope() ==
                market::ObservedInstrumentCatalogScopeV2::kObservedOnly &&
            !catalog->coverage_complete() &&
            catalog->bound_count() == 0U &&
            catalog->available_count() == 0U &&
            cut.store_generation->instrument_count() == 0U &&
            cut.factor_generation != nullptr &&
            cut.factor_generation->points().empty() &&
            cut.factor_generation->processing_lag_records() == 0U,
        "empty generation retains explicit observed-universe semantics");
}

void CheckPopulatedGeneration(
    TestContext* test,
    runtime::RealtimePipelineV1* pipeline,
    market::ObservedInstrumentDirectoryV2* directory,
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
        return;
    }

    std::shared_ptr<const market::ObservedInstrumentCatalogSnapshotV2>
        catalog;
    test->Expect(
        directory->AcquireSnapshot(&catalog) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            catalog != nullptr,
        "live directory snapshot is available");
    if (catalog == nullptr) {
        return;
    }
    test->Expect(
        catalog->capacity() == 8U &&
            catalog->catalog_generation() == 2U &&
            catalog->bound_count() == 2U &&
            catalog->available_count() == 2U &&
            catalog->snapshot_available_count() == 1U &&
            catalog->tick_available_count() == 2U &&
            catalog->factor_eligible_count() == 1U &&
            !catalog->coverage_complete(),
        "observed counts satisfy exact snapshot/tick/eligible semantics");

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
        pipeline->GetLatestSnapshot(1U, &latest_snapshot) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            latest_snapshot.record != nullptr &&
            latest_snapshot.record->instrument_id() == 1U &&
            pipeline->GetLatestTick(1U, &latest_tick_one) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            latest_tick_one.record != nullptr &&
            latest_tick_one.record->instrument_id() == 1U &&
            pipeline->GetLatestTick(2U, &latest_tick_two) ==
                market::RealtimeLatestQueryErrorV1::kNone &&
            latest_tick_two.record != nullptr &&
            latest_tick_two.record->instrument_id() == 2U,
        "known session IDs use direct ordinal latest reads");

    const std::shared_ptr<const factor::RealtimeFactorGenerationV1>&
        factor_generation = cut.factor_generation;
    const factor::RealtimeFactorPointV1* const factor_point =
        factor_generation == nullptr
            ? nullptr
            : factor_generation->Find(1U);
    test->Expect(
        factor_generation != nullptr &&
            factor_generation->catalog_scope() ==
                market::ObservedInstrumentCatalogScopeV2::kObservedOnly &&
            !factor_generation->coverage_complete() &&
            factor_generation->bound_count() == 2U &&
            factor_generation->available_count() == 2U &&
            factor_generation->factor_eligible_count() == 1U &&
            factor_generation->points().size() == 1U &&
            factor_point != nullptr &&
            factor_point->values.size() == 1U &&
            factor_point->values[0U].valid &&
            factor_point->values[0U].value == 12.3456,
        "Factor publishes only the exact eligible observed subset");

    test->Expect(
        WaitUntil([&] {
            return projection->binding_mask() == 0b11U &&
                   projection->applied_mask() == 0b111U &&
                   projection->accepted() == 3U &&
                   projection->applied() == 3U &&
                   !projection->coverage_lost();
        }),
        "external binding/data/progress projections expose processing progress");
}

void CheckMainlandAShareIngressFilter(TestContext* test) {
    constexpr std::array<std::string_view,
                         sdk::kProductionMessageCountV1>
        kNonAShareSecurityIds{
            "900901", "900901", "200001", "200001", "200001"};
    constexpr std::array<std::uint64_t,
                         market::kRealtimeHistorySourceCountV1>
        kOneFilteredPerProductionTuple{1U, 1U, 1U, 2U};

    std::unique_ptr<market::ObservedInstrumentDirectoryV2>
        filtered_directory;
    test->Expect(
        market::ObservedInstrumentDirectoryV2::Create(
            market::ObservedInstrumentDirectoryConfigV2{8U, 21U},
            &filtered_directory) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            filtered_directory != nullptr,
        "create default A-share-filter directory");
    if (filtered_directory == nullptr) {
        return;
    }

    const auto filtered_projection =
        std::make_shared<ProjectionProbe>();
    runtime::RealtimePipelineConfigV1 filtered_config =
        MakeConfig(
            filtered_directory.get(), filtered_projection);
    test->Expect(
        filtered_config.enable_mainland_a_share_filter,
        "Mainland A-share admission filter defaults to enabled");

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
            after_tuple_filter.mainland_a_share_filter_enabled &&
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
        const market::ObservedInstrumentCatalogSnapshotV2>
        filtered_catalog;
    const bool filtered_catalog_ready =
        filtered_directory->AcquireSnapshot(&filtered_catalog) ==
            market::ObservedInstrumentDirectoryErrorV2::kNone &&
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
            filtered_catalog->bound_count() == 1U &&
            WaitUntil([&] {
                return filtered_projection->binding_mask() == 0b1U &&
                       filtered_projection->applied_mask() == 0b11U &&
                       filtered_projection->accepted() == 2U &&
                       filtered_projection->applied() == 2U;
            }) &&
            !filtered_snapshot.fatal,
        "filtered callbacks stay outside the published prefix, Store, and "
        "observed directory");
    filtered_pipeline->StopAndDrain();

    std::unique_ptr<market::ObservedInstrumentDirectoryV2>
        unfiltered_directory;
    test->Expect(
        market::ObservedInstrumentDirectoryV2::Create(
            market::ObservedInstrumentDirectoryConfigV2{8U, 22U},
            &unfiltered_directory) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            unfiltered_directory != nullptr,
        "create explicitly unfiltered directory");
    if (unfiltered_directory == nullptr) {
        return;
    }

    const auto unfiltered_projection =
        std::make_shared<ProjectionProbe>();
    runtime::RealtimePipelineConfigV1 unfiltered_config =
        MakeConfig(
            unfiltered_directory.get(), unfiltered_projection);
    unfiltered_config.enable_mainland_a_share_filter = false;
    std::unique_ptr<runtime::RealtimePipelineV1> unfiltered_pipeline;
    detail.clear();
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            unfiltered_config, &unfiltered_pipeline, &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            unfiltered_pipeline != nullptr,
        "create explicitly unfiltered pipeline: " + detail);
    if (unfiltered_pipeline == nullptr) {
        return;
    }

    constexpr std::array<std::uint64_t,
                         sdk::kProductionMessageCountV1>
        kExpectedSourceSequences{1U, 1U, 1U, 1U, 2U};
    constexpr std::array<std::uint64_t,
                         sdk::kProductionMessageCountV1>
        kExpectedTickSequences{0U, 1U, 0U, 2U, 3U};
    bool all_unfiltered = true;
    for (std::size_t index = 0U;
         index < sdk::kProductionMessageCountV1;
         ++index) {
        FakeMessage message(
            sdk::kProductionMessageKeysV1[index],
            ProductionBody(
                index,
                kNonAShareSecurityIds[index],
                200U + index));
        const runtime::RealtimePipelineIngressResultV1 result =
            unfiltered_pipeline->InjectSdkMessageForTest(&message);
        message.DestroyCallbackBytes();
        all_unfiltered =
            all_unfiltered && result.accepted() &&
            result.global_ingress_sequence == index + 1U &&
            result.source_sequence ==
                kExpectedSourceSequences[index] &&
            result.tick_stream_sequence ==
                kExpectedTickSequences[index];
    }

    const bool unfiltered_applied_before_cut = WaitUntil([&] {
        const runtime::RealtimePipelineSnapshotV1 snapshot =
            unfiltered_pipeline->Snapshot();
        return snapshot.fatal ||
               (snapshot.accepted_messages == 5U &&
                snapshot.decoded_messages == 5U &&
                snapshot.processing_progress.accepted_sequence == 5U &&
                snapshot.processing_progress.applied_sequence == 5U &&
                snapshot.store.appended_records == 5U);
    });
    test->Expect(
        unfiltered_applied_before_cut &&
            !unfiltered_pipeline->Snapshot().fatal,
        "all explicitly unfiltered production tuples fully apply");

    const runtime::RealtimePipelineCutResultV1 unfiltered_cut =
        unfiltered_pipeline->CutAndPublishGeneration(5s);
    const runtime::RealtimePipelineSnapshotV1 unfiltered_snapshot =
        unfiltered_pipeline->Snapshot();
    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2>
        unfiltered_catalog;
    const bool unfiltered_catalog_ready =
        unfiltered_directory->AcquireSnapshot(&unfiltered_catalog) ==
            market::ObservedInstrumentDirectoryErrorV2::kNone &&
        unfiltered_catalog != nullptr;
    test->Expect(
        all_unfiltered && unfiltered_applied_before_cut &&
            unfiltered_cut.published() &&
            unfiltered_cut.store_generation != nullptr &&
            unfiltered_cut.store_generation->watermark()
                    .processing_progress.accepted_sequence == 5U &&
            unfiltered_cut.store_generation->watermark()
                    .processing_progress.applied_sequence == 5U &&
            unfiltered_snapshot.accepted_messages == 5U &&
            unfiltered_snapshot.filtered_messages == 0U &&
            unfiltered_snapshot.filtered_messages_by_source ==
                std::array<std::uint64_t,
                           market::kRealtimeHistorySourceCountV1>{} &&
            unfiltered_snapshot.decoded_messages == 5U &&
            unfiltered_snapshot.global_ingress_sequence == 5U &&
            unfiltered_snapshot.tick_stream_sequence == 3U &&
            unfiltered_snapshot.source_sequences ==
                std::array<std::uint64_t,
                           market::kRealtimeHistorySourceCountV1>{
                    1U, 1U, 1U, 2U} &&
            unfiltered_snapshot.ignored_messages == 0U &&
            unfiltered_snapshot.rejected_messages == 0U &&
            unfiltered_snapshot.store.appended_records == 5U &&
            !unfiltered_snapshot.mainland_a_share_filter_enabled &&
            unfiltered_catalog_ready &&
            unfiltered_catalog->bound_count() == 2U &&
            WaitUntil([&] {
                return unfiltered_projection->binding_mask() == 0b11U &&
                       unfiltered_projection->applied_mask() == 0b11111U &&
                       unfiltered_projection->accepted() == 5U &&
                       unfiltered_projection->applied() == 5U;
            }) &&
            !unfiltered_snapshot.fatal,
        "explicitly disabling the filter restores admission for every "
        "production tuple");
    unfiltered_pipeline->StopAndDrain();

    std::unique_ptr<market::ObservedInstrumentDirectoryV2>
        malformed_directory;
    test->Expect(
        market::ObservedInstrumentDirectoryV2::Create(
            market::ObservedInstrumentDirectoryConfigV2{2U, 23U},
            &malformed_directory) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            malformed_directory != nullptr,
        "create malformed-key directory");
    if (malformed_directory == nullptr) {
        return;
    }
    const auto malformed_projection =
        std::make_shared<ProjectionProbe>();
    std::unique_ptr<runtime::RealtimePipelineV1> malformed_pipeline;
    detail.clear();
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            MakeConfig(
                malformed_directory.get(), malformed_projection),
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

void CheckExplicitAppliedDispatchWindow(
    TestContext* test) {
    std::unique_ptr<market::ObservedInstrumentDirectoryV2> directory;
    test->Expect(
        market::ObservedInstrumentDirectoryV2::Create(
            market::ObservedInstrumentDirectoryConfigV2{4U, 18U},
            &directory) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            directory != nullptr,
        "create dispatch-window directory");
    if (directory == nullptr) {
        return;
    }
    const auto projection =
        std::make_shared<ProjectionProbe>(true);
    runtime::RealtimePipelineConfigV1 config =
        MakeConfig(directory.get(), projection);
    config.decoder_queue_capacity_per_source = 1U;
    config.store_worker_count = 2U;
    config.store_queue_capacity_per_source_worker = 64U;

    std::size_t window = 0U;
    test->Expect(
        runtime::RealtimePipelineAppliedWindowCapacityV1(
            config, &window) &&
            window == 9U,
        "derive one explicit nine-record applied dispatch window");

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

void CheckProcessingQueueFullKeepsAcceptedPrefix(
    TestContext* test) {
    std::unique_ptr<market::ObservedInstrumentDirectoryV2> directory;
    test->Expect(
        market::ObservedInstrumentDirectoryV2::Create(
            market::ObservedInstrumentDirectoryConfigV2{4U, 22U},
            &directory) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            directory != nullptr,
        "create queue-full directory");
    if (directory == nullptr) {
        return;
    }

    const auto projection =
        std::make_shared<ProjectionProbe>(true);
    runtime::RealtimePipelineConfigV1 config =
        MakeConfig(directory.get(), projection);
    config.processing_queue_capacity = 1U;
    config.decoder_queue_capacity_per_source = 1U;
    config.store_worker_count = 2U;
    config.store_queue_capacity_per_source_worker = 64U;

    std::size_t window = 0U;
    test->Expect(
        runtime::RealtimePipelineAppliedWindowCapacityV1(
            config, &window) &&
            window == 9U,
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
        "dispatcher is blocked with one free processing slot");

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
                    kProcessingAdmissionFailed &&
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

void CheckProcessingQueueIdleBoundaryLastMessage(
    TestContext* test) {
    std::unique_ptr<market::ObservedInstrumentDirectoryV2> directory;
    test->Expect(
        market::ObservedInstrumentDirectoryV2::Create(
            market::ObservedInstrumentDirectoryConfigV2{4U, 21U},
            &directory) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            directory != nullptr,
        "create processing-queue idle-boundary directory");
    if (directory == nullptr) {
        return;
    }

    constexpr std::uint64_t rounds = 4096U;
    constexpr std::uint64_t records_per_round = 2U;
    constexpr std::uint64_t total_records =
        rounds * records_per_round;
    const std::shared_ptr<ProjectionProbe> no_projection;
    runtime::RealtimePipelineConfigV1 config =
        MakeConfig(directory.get(), no_projection);
    config.processing_queue_capacity = 2U;
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
        "create processing-queue idle-boundary pipeline: " + detail);
    if (pipeline == nullptr) {
        return;
    }

    std::uint64_t expected = 0U;
    bool completed = true;
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

            if (index + 1U < records_per_round) {
                // Let the dispatcher race the producer toward its next empty
                // Pop. The following record is the terminal record for this
                // burst, so a lost wakeup cannot be hidden by a later push.
                std::this_thread::yield();
            }
        }

        if (completed) {
            completed = WaitUntil([&] {
                const runtime::RealtimePipelineSnapshotV1 snapshot =
                    pipeline->Snapshot();
                return snapshot.fatal ||
                       (snapshot.accepted_messages == expected &&
                        snapshot.decoded_messages == expected &&
                        snapshot.processing_progress.accepted_sequence ==
                            expected &&
                        snapshot.processing_progress.applied_sequence ==
                            expected &&
                        snapshot.store.appended_records == expected);
            });
            if (completed && pipeline->Snapshot().fatal) {
                completed = false;
            }
        }
    }

    const runtime::RealtimePipelineSnapshotV1 final =
        pipeline->Snapshot();
    test->Expect(
        completed && expected == total_records &&
            final.accepted_messages == total_records &&
            final.decoded_messages == total_records &&
            final.processing_progress.accepted_sequence ==
                total_records &&
            final.processing_progress.applied_sequence ==
                total_records &&
            final.store.appended_records == total_records &&
            !final.fatal,
        "short bursts preserve the exact applied prefix for the terminal "
        "message across repeated processing-queue idle boundaries");
    pipeline->StopAndDrain();
}

}  // namespace

int main() {
    TestContext test;
    std::unique_ptr<market::ObservedInstrumentDirectoryV2> directory;
    test.Expect(
        market::ObservedInstrumentDirectoryV2::Create(
            market::ObservedInstrumentDirectoryConfigV2{8U, 17U},
            &directory) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            directory != nullptr,
        "fixed-capacity observed directory creates");
    if (directory == nullptr) {
        return 1;
    }

    const auto projection = std::make_shared<ProjectionProbe>();
    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test.Expect(
        runtime::RealtimePipelineV1::Create(
            MakeConfig(directory.get(), projection),
            &pipeline,
            &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "observed-universe pipeline creates: " + detail);
    if (pipeline == nullptr) {
        return 1;
    }

    CheckEmptyGeneration(&test, pipeline.get());
    CheckPopulatedGeneration(
        &test, pipeline.get(), directory.get(), projection);
    CheckMainlandAShareIngressFilter(&test);

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
    CheckProcessingQueueFullKeepsAcceptedPrefix(&test);
    CheckStoreGenerationSinkOrderingAndFailure(&test);
    CheckProcessingQueueIdleBoundaryLastMessage(&test);

    return test.failures() == 0 ? 0 : 1;
}
