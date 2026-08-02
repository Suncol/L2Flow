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
#include <limits>
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

    std::size_t BeginList(
        std::size_t descriptor,
        std::uint32_t count,
        std::size_t item_bytes) {
        const std::size_t start = bytes_.size();
        bytes_.resize(
            start + static_cast<std::size_t>(count) * item_bytes,
            std::byte{0U});
        StoreU32(descriptor, count);
        StoreU32(
            descriptor + sizeof(std::uint32_t),
            static_cast<std::uint32_t>(start - descriptor));
        return start;
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

[[nodiscard]] std::vector<std::byte> ShanghaiStatusBody(
    std::uint64_t business_index,
    std::string_view phase,
    std::string_view security_id = "600007") {
    WireWriter writer(70U);
    writer.StoreU64(0U, business_index);
    writer.StoreU32(8U, 7U);
    writer.StoreU32(18U, 93'000'125U);
    writer.StoreString(12U, security_id);
    writer.StoreString(22U, "S");
    writer.StoreString(64U, phase);
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

[[nodiscard]] std::vector<std::byte> ShanghaiNestedQueueSnapshotBody(
    bool malformed_at_last_level) {
    // A queue target is registered for every depth level. CheckedBodyView's
    // non-overlap validation therefore performs substantial deterministic work
    // while the wire stays small enough for callback admission to outrun it.
    constexpr std::uint32_t bid_depth = 4096U;
    constexpr std::size_t bid_levels_descriptor = 228U;
    constexpr std::size_t level_bytes = 28U;
    constexpr std::size_t level_price = 4U;
    constexpr std::size_t level_order_count = 16U;
    constexpr std::size_t level_queue = 20U;
    constexpr std::size_t queue_item_bytes = 16U;

    WireWriter writer(248U);
    writer.StoreU32(0U, 93'000'123U);
    writer.StoreU32(30U, 12'345U);
    writer.StoreString(4U, "600007");
    writer.StoreString(38U, "TRADE");

    const std::size_t bids = writer.BeginList(
        bid_levels_descriptor, bid_depth, level_bytes);
    for (std::uint32_t index = 0U; index < bid_depth; ++index) {
        const std::size_t level =
            bids + static_cast<std::size_t>(index) * level_bytes;
        writer.StoreU32(level + level_price, 12'345U - index);
        writer.StoreU32(level + level_order_count, 1U);
        if (malformed_at_last_level && index + 1U == bid_depth) {
            writer.StoreU32(level + level_queue, 1U);
            writer.StoreU32(
                level + level_queue + sizeof(std::uint32_t),
                std::numeric_limits<std::uint32_t>::max());
        } else {
            static_cast<void>(writer.BeginList(
                level + level_queue, 1U, queue_item_bytes));
        }
    }
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

void CheckStoreOnlyGenerationSkipsFactor(TestContext* test) {
    PipelineCatalogFixture fixture;
    test->Expect(
        MakeCatalogFixture(21U, &fixture),
        "create store-only generation daily catalog");
    if (fixture.runtime_state == nullptr) {
        return;
    }

    const auto projection = std::make_shared<ProjectionProbe>();
    const auto order = std::make_shared<GenerationPublicationOrder>();
    const auto calculator =
        std::make_shared<OrderedFactorCalculator>(order);
    runtime::RealtimePipelineConfigV1 config =
        MakeConfig(fixture, projection);
    config.factor_calculator = calculator;
    config.factor_generation_enabled = false;

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            config, &pipeline, &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create store-only generation pipeline: " + detail);
    if (pipeline == nullptr) {
        return;
    }

    const runtime::RealtimePipelineCutResultV1 periodic =
        pipeline->CutAndPublishGeneration(3s);
    test->Expect(
        periodic.published() &&
            !periodic.factor_generation_enabled &&
            periodic.store_generation != nullptr &&
            periodic.factor_generation == nullptr &&
            periodic.factor_result.generation == nullptr &&
            pipeline->AcquireLatestFactorGeneration() == nullptr &&
            order->factor_calls() == 0U,
        "store-only periodic generation publishes without Factor work");

    const runtime::RealtimePipelineCutResultV1 terminal =
        pipeline->StopAndPublishFinalGeneration(3s);
    test->Expect(
        terminal.published() &&
            !terminal.factor_generation_enabled &&
            terminal.store_generation != nullptr &&
            terminal.factor_generation == nullptr &&
            terminal.factor_result.generation == nullptr &&
            pipeline->AcquireLatestFactorGeneration() == nullptr &&
            order->factor_calls() == 0U && !pipeline->fatal(),
        "store-only terminal generation publishes without Factor work");
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

void CheckExternalIngressCapacityRetryOwnershipAndStop(
    TestContext* test) {
    const auto run_case = [test](
                              std::uint64_t session_epoch,
                              bool stop_while_waiting) {
        PipelineCatalogFixture fixture;
        test->Expect(
            MakeCatalogFixture(session_epoch, &fixture),
            "create external-ingress retry catalog");
        if (fixture.runtime_state == nullptr) {
            return;
        }

        const auto blocker =
            std::make_shared<OpeningBurstAppliedBlocker>();
        const std::shared_ptr<ProjectionProbe> no_projection;
        runtime::RealtimePipelineConfigV1 config =
            MakeConfig(fixture, no_projection);
        config.external_ingress_enabled = true;
        config.decoder_queue_capacity_per_source = 4U;
        config.completion_tracker_capacity = 2U;
        config.tick_ring_capacity = 2U;
        config.store_queue_capacity_per_source_worker = 16U;
        config.intraday_store.maximum_session_records = 32U;
        config.applied_record_sink = blocker;

        std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
        std::string detail;
        test->Expect(
            runtime::RealtimePipelineV1::Create(
                config, &pipeline, &detail) ==
                    runtime::RealtimePipelineCreateErrorV1::kNone &&
                pipeline != nullptr,
            "create external-ingress retry pipeline: " + detail);
        if (pipeline == nullptr) {
            return;
        }

        const auto ingest = [&](
                                FakeMessage* message,
                                std::uint64_t clock_value) {
            runtime::RealtimePipelineExternalIngressV1 input{};
            input.message = message;
            input.recv_realtime_ns = 1'000U + clock_value;
            input.recv_monotonic_ns = 2'000U + clock_value;
            input.admission_timeout = 5s;
            return pipeline->IngestExternalMessage(input);
        };

        FakeMessage first(
            sdk::kProductionMessageKeysV1[2U],
            ShenzhenSnapshotBody(12'345'600));
        const runtime::RealtimePipelineIngressResultV1 first_result =
            ingest(&first, 1U);
        first.DestroyCallbackBytes();
        const bool first_blocked =
            first_result.accepted() &&
            blocker->WaitUntilFirstAppliedBlocked(3s);
        test->Expect(
            first_blocked,
            "external-ingress retry fixture blocks its first applied record");
        if (!first_blocked) {
            blocker->ReleaseFirstApplied();
            pipeline->StopAndDrain();
            return;
        }

        bool prefix_accepted = true;
        for (std::uint64_t sequence = 2U;
             sequence <= 6U;
             ++sequence) {
            FakeMessage message(
                sdk::kProductionMessageKeysV1[2U],
                ShenzhenSnapshotBody(12'345'600 + sequence));
            const runtime::RealtimePipelineIngressResultV1 result =
                ingest(&message, sequence);
            message.DestroyCallbackBytes();
            prefix_accepted = prefix_accepted && result.accepted() &&
                              result.global_ingress_sequence == sequence &&
                              result.source_sequence == sequence;
        }
        constexpr std::size_t kShenzhenSnapshotSource = 2U;
        const bool queue_full = prefix_accepted && WaitUntil([&] {
            const runtime::RealtimePipelineSnapshotV1 snapshot =
                pipeline->Snapshot();
            return snapshot.processing_progress.applied_sequence == 0U &&
                   snapshot.decoder_queues[kShenzhenSnapshotSource]
                           .message_depth == 4U;
        });
        test->Expect(
            queue_full,
            "external-ingress retry fixture fills the source queue while "
            "the applied window is blocked");
        if (!queue_full) {
            blocker->ReleaseFirstApplied();
            pipeline->StopAndDrain();
            return;
        }
        const std::uint64_t full_count_before_seventh =
            pipeline->Snapshot()
                .decoder_queues[kShenzhenSnapshotSource]
                .full_count;

        FakeMessage seventh(
            sdk::kProductionMessageKeysV1[2U],
            ShenzhenSnapshotBody(12'345'607));
        runtime::RealtimePipelineIngressResultV1 seventh_result{};
        std::atomic<bool> seventh_finished{false};
        std::thread seventh_thread([&] {
            seventh_result = ingest(&seventh, 7U);
            seventh_finished.store(true, std::memory_order_release);
        });
        const bool seventh_waiting = WaitUntil([&] {
            const runtime::RealtimePipelineSnapshotV1 snapshot =
                pipeline->Snapshot();
            return !seventh_finished.load(std::memory_order_acquire) &&
                   snapshot.decoder_queues[kShenzhenSnapshotSource]
                           .full_count > full_count_before_seventh;
        });
        test->Expect(
            seventh_waiting,
            "external ingress waits outside admission on a full decoder "
            "queue");

        if (stop_while_waiting) {
            std::thread stop_thread([&] { pipeline->StopAndDrain(); });
            const bool stop_linearized = WaitUntil([&] {
                return !pipeline->Snapshot().accepting;
            });
            const bool waiter_rejected_before_release =
                stop_linearized && WaitUntil([&] {
                    return seventh_finished.load(
                        std::memory_order_acquire);
                });
            blocker->ReleaseFirstApplied();
            seventh_thread.join();
            stop_thread.join();
            seventh.DestroyCallbackBytes();
            const runtime::RealtimePipelineSnapshotV1 stopped =
                pipeline->Snapshot();
            test->Expect(
                seventh_waiting && waiter_rejected_before_release &&
                    seventh_result.error ==
                        runtime::RealtimePipelineIngressErrorV1::kStopped &&
                    !seventh_result.accepted() && stopped.stopped &&
                    !stopped.fatal && stopped.accepted_messages == 6U &&
                    stopped.decoded_messages == 6U &&
                    stopped.processing_progress.applied_sequence == 6U &&
                    stopped.store.appended_records == 6U &&
                    stopped.post_cut_messages == 1U &&
                    stopped.decoder_queues[kShenzhenSnapshotSource]
                            .message_depth == 0U &&
                    stopped.ingress_pool.active_messages == 0U,
                "StopAndDrain rejects a capacity waiter before decoder "
                "close and drains the exact accepted prefix");
            return;
        }

        FakeMessage eighth(
            sdk::kProductionMessageKeysV1[2U],
            ShenzhenSnapshotBody(12'345'608));
        runtime::RealtimePipelineIngressResultV1 eighth_result{};
        std::thread eighth_thread([&] {
            eighth_result = ingest(&eighth, 8U);
        });
        blocker->ReleaseFirstApplied();
        seventh_thread.join();
        eighth_thread.join();
        seventh.DestroyCallbackBytes();
        eighth.DestroyCallbackBytes();
        // CompleteAppliedSequence() runs from History before the source owner
        // increments decoded_messages. Waiting only for applied_sequence can
        // therefore observe the valid, short-lived applied=8/decoded=7
        // boundary. Wait for the whole externally asserted snapshot instead
        // of treating that publication order as an ownership failure.
        const bool exact_suffix_drained = WaitUntil([&] {
            const runtime::RealtimePipelineSnapshotV1 snapshot =
                pipeline->Snapshot();
            return snapshot.fatal ||
                   (snapshot.accepted_messages == 8U &&
                    snapshot.decoded_messages == 8U &&
                    snapshot.processing_progress.applied_sequence == 8U &&
                    snapshot.store.appended_records == 8U);
        });
        const runtime::RealtimePipelineSnapshotV1 drained =
            pipeline->Snapshot();
        const bool exact_retry_result =
            seventh_waiting && seventh_result.accepted() &&
                eighth_result.accepted() &&
                seventh_result.global_ingress_sequence == 7U &&
                eighth_result.global_ingress_sequence == 8U &&
                exact_suffix_drained && !drained.fatal &&
                drained.accepted_messages == 8U &&
                drained.decoded_messages == 8U &&
                drained.processing_progress.applied_sequence == 8U &&
                drained.store.appended_records == 8U;
        if (!exact_retry_result) {
            std::cerr
                << "external retry diagnostic: seventh_waiting="
                << seventh_waiting
                << " seventh_error="
                << static_cast<unsigned>(seventh_result.error)
                << " seventh_sequence="
                << seventh_result.global_ingress_sequence
                << " eighth_error="
                << static_cast<unsigned>(eighth_result.error)
                << " eighth_sequence="
                << eighth_result.global_ingress_sequence
                << " suffix_drained=" << exact_suffix_drained
                << " fatal=" << drained.fatal
                << " accepted=" << drained.accepted_messages
                << " decoded=" << drained.decoded_messages
                << " applied="
                << drained.processing_progress.applied_sequence
                << " store=" << drained.store.appended_records
                << '\n';
        }
        test->Expect(
            exact_retry_result,
            "concurrent external callers retain one sequence owner across "
            "a bounded capacity retry");
        pipeline->StopAndDrain();
    };

    run_case(31U, false);
    run_case(32U, true);
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

void CheckParallelDecodeFarmOrderedFenceAndDrain(TestContext* test) {
    PipelineCatalogFixture fixture;
    test->Expect(
        MakeCatalogFixture(28U, &fixture),
        "create parallel-decode-farm catalog");
    if (fixture.runtime_state == nullptr) {
        return;
    }

    const std::shared_ptr<ProjectionProbe> no_projection;
    runtime::RealtimePipelineConfigV1 base =
        MakeConfig(fixture, no_projection);
    test->Expect(
        runtime::RealtimePipelineConfigV1{}
                .parallel_decoder_worker_count == 0U,
        "public pipeline config keeps the legacy decoder topology by default");
    base.decoder_queue_capacity_per_source = 512U;
    base.store_queue_capacity_per_source_worker = 1024U;
    base.intraday_store.maximum_session_records = 2048U;
    base.intraday_store.maximum_session_accounted_bytes =
        128U * 1024U * 1024U;

    auto expect_invalid = [&](runtime::RealtimePipelineConfigV1 config,
                              std::string_view description) {
        std::unique_ptr<runtime::RealtimePipelineV1> invalid;
        std::string detail;
        test->Expect(
            runtime::RealtimePipelineV1::Create(
                std::move(config), &invalid, &detail) ==
                    runtime::RealtimePipelineCreateErrorV1::
                        kInvalidConfiguration &&
                invalid == nullptr,
            description);
    };

    runtime::RealtimePipelineConfigV1 too_many_workers = base;
    too_many_workers.parallel_decoder_worker_count =
        static_cast<std::uint32_t>(
            runtime::kRealtimeParallelDecoderMaximumWorkersV1 + 1U);
    expect_invalid(
        std::move(too_many_workers),
        "parallel decoder rejects worker count above its fixed snapshot bound");

    runtime::RealtimePipelineConfigV1 zero_slots = base;
    zero_slots.parallel_decoder_worker_count = 2U;
    zero_slots.parallel_decoder_slots_per_source_worker = 0U;
    expect_invalid(
        std::move(zero_slots),
        "parallel decoder rejects a zero lease count");

    runtime::RealtimePipelineConfigV1 too_many_slots = base;
    too_many_slots.parallel_decoder_worker_count = 2U;
    too_many_slots.parallel_decoder_slots_per_source_worker = 1025U;
    expect_invalid(
        std::move(too_many_slots),
        "parallel decoder rejects an unbounded lease count");

    runtime::RealtimePipelineConfigV1 config = base;
    config.parallel_decoder_worker_count = 4U;
    config.parallel_decoder_slots_per_source_worker = 4U;
    // The production default keeps the idle-inline latency path. Disable it
    // here so every accepted message deterministically exercises issue,
    // concurrent stateless decode, completion reorder, and ordered commit.
    config.parallel_decoder_idle_inline_enabled = false;

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            config, &pipeline, &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create deterministic parallel decode farm: " + detail);
    if (pipeline == nullptr) {
        return;
    }

    constexpr std::uint64_t pre_cut_records = 1024U;
    constexpr std::uint64_t chunk_records = 128U;
    bool all_admitted = true;
    for (std::uint64_t sequence = 1U;
         sequence <= pre_cut_records;
         ++sequence) {
        const runtime::RealtimePipelineIngressResultV1 admitted =
            InjectSourceMessage(pipeline.get(), 0U, sequence);
        all_admitted = all_admitted && admitted.accepted() &&
                       admitted.global_ingress_sequence == sequence &&
                       admitted.source_sequence == sequence;
        if (!all_admitted) {
            break;
        }
        // Leave the final chunk in flight so Cut must cover decode leases and
        // the ordered completion prefix, rather than observing an idle farm.
        if (sequence % chunk_records == 0U &&
            sequence != pre_cut_records) {
            all_admitted = WaitUntil([&] {
                const runtime::RealtimePipelineSnapshotV1 snapshot =
                    pipeline->Snapshot();
                return snapshot.fatal ||
                       snapshot.decoded_messages == sequence;
            }) &&
                           !pipeline->fatal();
        }
    }
    test->Expect(
        all_admitted,
        "parallel farm accepts the complete pre-cut source prefix");

    const runtime::RealtimePipelineCutResultV1 cut =
        pipeline->CutAndPublishGeneration(5s);
    const runtime::RealtimePipelineSnapshotV1 after_cut =
        pipeline->Snapshot();
    std::uint64_t parsed_by_workers = 0U;
    for (std::uint32_t worker = 0U;
         worker < config.parallel_decoder_worker_count;
         ++worker) {
        parsed_by_workers +=
            after_cut.parallel_decoder.workers[worker].parsed_messages;
    }
    const runtime::RealtimeParallelDecoderSourceSnapshotV1& source_zero =
        after_cut.parallel_decoder.sources[0U];
    test->Expect(
        cut.published() &&
            cut.store_generation->watermark()
                    .processing_progress.accepted_sequence ==
                pre_cut_records &&
            cut.store_generation->watermark()
                    .processing_progress.applied_sequence ==
                pre_cut_records &&
            after_cut.accepted_messages == pre_cut_records &&
            after_cut.decoded_messages == pre_cut_records &&
            after_cut.processing_progress.applied_sequence ==
                pre_cut_records &&
            after_cut.store.appended_records == pre_cut_records &&
            after_cut.parallel_decoder.enabled &&
            !after_cut.parallel_decoder.idle_inline_enabled &&
            source_zero.dispatched_messages == pre_cut_records &&
            source_zero.inline_messages == 0U &&
            source_zero.farm_messages == pre_cut_records &&
            source_zero.completed_messages == pre_cut_records &&
            source_zero.committed_messages == pre_cut_records &&
            source_zero.committed_source_sequence == pre_cut_records &&
            source_zero.farm_outstanding == 0U &&
            source_zero.completion_depth == 0U &&
            source_zero.completion_capacity != 0U &&
            source_zero.completion_high_water <=
                source_zero.completion_capacity &&
            source_zero.completion_publish_failures == 0U &&
            parsed_by_workers == pre_cut_records &&
            !after_cut.fatal,
        "generation fence waits for every parallel lease and publishes the "
        "exact ordered pre-cut prefix");

    constexpr std::uint64_t post_cut_records = 64U;
    bool suffix_admitted = cut.published();
    if (suffix_admitted) {
        FakeMessage status(
            sdk::kProductionMessageKeysV1[1U],
            ShanghaiStatusBody(50'001U, "TRADE"));
        const runtime::RealtimePipelineIngressResultV1 admitted =
            pipeline->InjectSdkMessageForTest(&status);
        status.DestroyCallbackBytes();
        suffix_admitted = admitted.accepted() &&
                          admitted.global_ingress_sequence ==
                              pre_cut_records + 1U &&
                          admitted.source_sequence == 1U;
    }
    for (std::uint64_t sequence = 2U;
         sequence <= post_cut_records && suffix_admitted;
         ++sequence) {
        const runtime::RealtimePipelineIngressResultV1 admitted =
            InjectSourceMessage(
                pipeline.get(), 1U, 50'000U + sequence);
        suffix_admitted = admitted.accepted() &&
                          admitted.global_ingress_sequence ==
                              pre_cut_records + sequence &&
                          admitted.source_sequence == sequence;
    }
    const runtime::RealtimePipelineCutResultV1 terminal =
        pipeline->StopAndPublishFinalGeneration(5s);
    const runtime::RealtimePipelineSnapshotV1 stopped =
        pipeline->Snapshot();
    market::RealtimeLatestRecordViewV1 latest_tick{};
    const bool latest_phase_ordered =
        pipeline->GetLatestTick(1U, &latest_tick) ==
            market::RealtimeLatestQueryErrorV1::kNone &&
        latest_tick.record != nullptr &&
        [&] {
            const market::StoredMarketEventViewV1 event =
                latest_tick.record->event();
            const market::ShanghaiTickV1* const tick =
                market::StoredMarketEventGetV1<market::ShanghaiTickV1>(
                    event);
            return tick != nullptr &&
                   tick->fields.action == market::TickActionV1::kTrade &&
                   tick->fields.phase ==
                       market::TradingPhaseV1::kContinuous &&
                   (tick->fields.validity_bitmap &
                    market::kTickPhaseValidV1) != 0U;
        }();
    const std::uint64_t total_records =
        pre_cut_records + post_cut_records;
    test->Expect(
        suffix_admitted && terminal.published() && stopped.stopped &&
            !stopped.fatal && latest_phase_ordered &&
            stopped.accepted_messages == total_records &&
            stopped.decoded_messages == total_records &&
            stopped.processing_progress.accepted_sequence ==
                total_records &&
            stopped.processing_progress.applied_sequence ==
                total_records &&
            stopped.store.appended_records == total_records &&
            stopped.parallel_decoder.sources[1U]
                    .committed_source_sequence == post_cut_records &&
            stopped.parallel_decoder.sources[1U]
                    .completion_depth == 0U,
        "terminal stop drains the post-cut parallel suffix and preserves "
        "ordered Shanghai phase attribution");
}

void CheckParallelDecodeFarmFourSourceWorkerOffsetMapping(
    TestContext* test) {
    static_assert(market::kRealtimeHistorySourceCountV1 == 4U);

    PipelineCatalogFixture fixture;
    test->Expect(
        MakeCatalogFixture(31U, &fixture),
        "create four-source parallel worker-mapping catalog");
    if (fixture.runtime_state == nullptr) {
        return;
    }

    const std::shared_ptr<ProjectionProbe> no_projection;
    runtime::RealtimePipelineConfigV1 config =
        MakeConfig(fixture, no_projection);
    constexpr std::uint32_t worker_count = 4U;
    constexpr std::uint64_t records_per_source = 5U;
    constexpr std::uint64_t total_records =
        records_per_source *
        market::kRealtimeHistorySourceCountV1;
    config.parallel_decoder_worker_count = worker_count;
    config.parallel_decoder_slots_per_source_worker = 1U;
    // Force every record through the farm. A fence after each source makes
    // the worker-counter delta an exact observation of that source's mapping.
    config.parallel_decoder_idle_inline_enabled = false;

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            config, &pipeline, &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create four-source parallel worker-mapping pipeline: " + detail);
    if (pipeline == nullptr) {
        return;
    }

    std::array<std::uint64_t, worker_count> previous_parsed{};
    std::uint64_t admitted_records = 0U;
    bool all_admitted = true;
    bool every_source_offset_exact = true;
    for (std::size_t source_index = 0U;
         source_index < market::kRealtimeHistorySourceCountV1 &&
         all_admitted;
         ++source_index) {
        const std::uint8_t source =
            static_cast<std::uint8_t>(source_index);
        for (std::uint64_t source_sequence = 1U;
             source_sequence <= records_per_source;
             ++source_sequence) {
            const std::uint64_t expected_global_sequence =
                admitted_records + 1U;
            const runtime::RealtimePipelineIngressResultV1 admitted =
                InjectSourceMessage(
                    pipeline.get(), source, source_sequence);
            all_admitted = admitted.accepted() &&
                           admitted.global_ingress_sequence ==
                               expected_global_sequence &&
                           admitted.source_sequence == source_sequence &&
                           admitted.source_slot == source;
            if (!all_admitted) {
                break;
            }
            admitted_records = expected_global_sequence;
        }

        if (!all_admitted) {
            break;
        }

        const runtime::RealtimePipelineCutResultV1 cut =
            pipeline->CutAndPublishGeneration(5s);
        const runtime::RealtimePipelineSnapshotV1 snapshot =
            pipeline->Snapshot();
        bool source_offset_exact =
            cut.published() && !snapshot.fatal &&
            snapshot.accepted_messages == admitted_records &&
            snapshot.decoded_messages == admitted_records &&
            snapshot.processing_progress.applied_sequence ==
                admitted_records &&
            snapshot.store.appended_records == admitted_records;
        for (std::size_t worker = 0U;
             worker < worker_count;
             ++worker) {
            const std::uint64_t parsed =
                snapshot.parallel_decoder.workers[worker]
                    .parsed_messages;
            const std::uint64_t expected_delta =
                worker == source_index ? 2U : 1U;
            source_offset_exact =
                source_offset_exact &&
                parsed >= previous_parsed[worker] &&
                parsed - previous_parsed[worker] == expected_delta;
            previous_parsed[worker] = parsed;
        }
        const runtime::RealtimeParallelDecoderSourceSnapshotV1&
            source_snapshot =
                snapshot.parallel_decoder.sources[source_index];
        source_offset_exact =
            source_offset_exact &&
            source_snapshot.dispatched_messages == records_per_source &&
            source_snapshot.inline_messages == 0U &&
            source_snapshot.farm_messages == records_per_source &&
            source_snapshot.completed_messages == records_per_source &&
            source_snapshot.committed_messages == records_per_source &&
            source_snapshot.committed_source_sequence ==
                records_per_source &&
            source_snapshot.farm_outstanding == 0U &&
            source_snapshot.completion_depth == 0U &&
            source_snapshot.completion_publish_failures == 0U;
        every_source_offset_exact =
            every_source_offset_exact && source_offset_exact;
    }

    const runtime::RealtimePipelineSnapshotV1 final =
        pipeline->Snapshot();
    bool workers_uniform = true;
    for (std::size_t worker = 0U;
         worker < worker_count;
         ++worker) {
        const runtime::RealtimeParallelDecoderWorkerSnapshotV1&
            worker_snapshot =
                final.parallel_decoder.workers[worker];
        workers_uniform =
            workers_uniform &&
            worker_snapshot.parsed_messages == records_per_source &&
            worker_snapshot.parse_failures == 0U &&
            worker_snapshot.issue_depth == 0U;
    }
    test->Expect(
        all_admitted && every_source_offset_exact && workers_uniform &&
            admitted_records == total_records &&
            final.parallel_decoder.enabled &&
            !final.parallel_decoder.idle_inline_enabled &&
            final.parallel_decoder.worker_count == worker_count &&
            final.accepted_messages == total_records &&
            final.decoded_messages == total_records &&
            final.processing_progress.applied_sequence == total_records &&
            final.store.appended_records == total_records &&
            !final.fatal,
        "four-source offset mapping assigns each source's extra task to its "
        "matching worker and distributes 20 tasks evenly across W4");
    pipeline->StopAndDrain();
}

void CheckAdaptiveParallelDecodeOwnershipHandoff(TestContext* test) {
    PipelineCatalogFixture fixture;
    test->Expect(
        MakeCatalogFixture(30U, &fixture),
        "create adaptive parallel-decode catalog");
    if (fixture.runtime_state == nullptr) {
        return;
    }

    const std::shared_ptr<ProjectionProbe> no_projection;
    runtime::RealtimePipelineConfigV1 config =
        MakeConfig(fixture, no_projection);
    config.decoder_queue_capacity_per_source = 512U;
    config.store_queue_capacity_per_source_worker = 1024U;
    config.parallel_decoder_worker_count = 4U;
    config.parallel_decoder_slots_per_source_worker = 8U;
    config.parallel_decoder_idle_inline_enabled = true;
    config.parallel_decoder_farm_activation_queue_depth = 1U;
    config.maximum_sdk_message_bytes = 256U * 1024U;
    config.intraday_store.maximum_session_records = 2048U;
    config.intraday_store.maximum_session_accounted_bytes =
        2ULL * 1024ULL * 1024ULL * 1024ULL;

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            config, &pipeline, &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create adaptive parallel-decode pipeline: " + detail);
    if (pipeline == nullptr) {
        return;
    }

    // First prove the idle path inline before creating pressure. Parsing one
    // maximum nested-queue descriptor set is then deliberately much more
    // expensive than the following compact callback copies, allowing the
    // suffix to cross the source-local threshold. Only one large decoded
    // event per attempt reaches Store, keeping retained memory bounded under
    // sanitizers.
    const runtime::RealtimePipelineIngressResultV1 idle_prefix =
        InjectSourceMessage(pipeline.get(), 0U, 89'999U);
    const bool idle_prefix_applied = idle_prefix.accepted() && WaitUntil([&] {
        return pipeline->Snapshot().processing_progress.applied_sequence ==
               1U;
    });
    FakeMessage heavy(
        sdk::kProductionMessageKeysV1[0U],
        ShanghaiNestedQueueSnapshotBody(false));
    constexpr std::uint64_t kPressureBurstPerAttempt = 320U;
    constexpr std::size_t kMaximumPressureAttempts = 3U;
    bool admitted_all = idle_prefix_applied;
    bool burst_drained = false;
    bool crossed_to_farm = false;
    std::uint64_t pressure_records = idle_prefix_applied ? 1U : 0U;
    runtime::RealtimePipelineSnapshotV1 after_burst{};
    // Thread scheduling can let the owner finish a deliberately heavy record
    // before this producer gets another timeslice. Retry a bounded burst in
    // the same ordered session so the handoff assertion remains deterministic.
    for (std::size_t attempt = 0U;
         attempt < kMaximumPressureAttempts && admitted_all &&
         !crossed_to_farm;
         ++attempt) {
        for (std::uint64_t offset = 0U;
             offset < kPressureBurstPerAttempt && admitted_all;
             ++offset) {
            const std::uint64_t sequence = pressure_records + 1U;
            const runtime::RealtimePipelineIngressResultV1 admitted =
                offset == 0U
                    ? pipeline->InjectSdkMessageForTest(&heavy)
                    : InjectSourceMessage(
                          pipeline.get(), 0U, 90'000U + sequence);
            admitted_all = admitted.accepted() &&
                           admitted.global_ingress_sequence == sequence &&
                           admitted.source_sequence == sequence;
            if (admitted_all) {
                pressure_records = sequence;
            }
        }
        burst_drained = admitted_all && WaitUntil([&] {
            const runtime::RealtimePipelineSnapshotV1 snapshot =
                pipeline->Snapshot();
            return snapshot.fatal ||
                   snapshot.processing_progress.applied_sequence ==
                       pressure_records;
        });
        after_burst = pipeline->Snapshot();
        const auto& candidate_source =
            after_burst.parallel_decoder.sources[0U];
        std::size_t active_workers = 0U;
        for (std::uint32_t worker = 0U;
             worker < config.parallel_decoder_worker_count;
             ++worker) {
            active_workers +=
                after_burst.parallel_decoder.workers[worker]
                            .parsed_messages != 0U
                    ? 1U
                    : 0U;
        }
        crossed_to_farm =
            burst_drained && !after_burst.fatal &&
            candidate_source.inline_messages != 0U &&
            candidate_source.farm_messages != 0U &&
            active_workers >= 2U &&
            candidate_source.farm_outstanding == 0U;
    }
    const auto& burst_source =
        after_burst.parallel_decoder.sources[0U];

    const std::uint64_t inline_before_suffix =
        burst_source.inline_messages;
    const std::uint64_t farm_before_suffix = burst_source.farm_messages;
    const runtime::RealtimePipelineIngressResultV1 suffix =
        InjectSourceMessage(pipeline.get(), 0U, 90'000U);
    const bool suffix_applied = suffix.accepted() && WaitUntil([&] {
        const runtime::RealtimePipelineSnapshotV1 snapshot =
            pipeline->Snapshot();
        return snapshot.fatal ||
               snapshot.processing_progress.applied_sequence ==
                   pressure_records + 1U;
    });
    const runtime::RealtimePipelineCutResultV1 cut =
        pipeline->CutAndPublishGeneration(5s);
    const runtime::RealtimePipelineSnapshotV1 after_cut =
        pipeline->Snapshot();
    const auto& final_source =
        after_cut.parallel_decoder.sources[0U];
    test->Expect(
        crossed_to_farm && suffix_applied && cut.published() &&
            !after_cut.fatal &&
            after_cut.accepted_messages == pressure_records + 1U &&
            after_cut.decoded_messages == pressure_records + 1U &&
            after_cut.processing_progress.applied_sequence ==
                pressure_records + 1U &&
            after_cut.store.appended_records == pressure_records + 1U &&
            final_source.inline_messages ==
                inline_before_suffix + 1U &&
            final_source.farm_messages == farm_before_suffix &&
            final_source.dispatched_messages == pressure_records + 1U &&
            final_source.completed_messages == pressure_records + 1U &&
            final_source.committed_messages == pressure_records + 1U &&
            final_source.farm_outstanding == 0U &&
            final_source.completion_depth == 0U &&
            final_source.completion_publish_failures == 0U,
        "adaptive owner moves inline-to-farm-to-inline and fences an exact "
        "non-overlapping source prefix");
    pipeline->StopAndDrain();
}

void CheckCapacityClampedAdaptiveActivation(TestContext* test) {
    PipelineCatalogFixture fixture;
    test->Expect(
        MakeCatalogFixture(33U, &fixture),
        "create capacity-clamped adaptive catalog");
    if (fixture.runtime_state == nullptr) {
        return;
    }

    const auto blocker =
        std::make_shared<OpeningBurstAppliedBlocker>();
    const std::shared_ptr<ProjectionProbe> no_projection;
    runtime::RealtimePipelineConfigV1 config =
        MakeConfig(fixture, no_projection);
    constexpr std::size_t kQueueCapacity = 512U;
    constexpr std::uint64_t kQueuedSuffix = kQueueCapacity;
    constexpr std::uint64_t kTotalRecords = kQueuedSuffix + 2U;
    config.decoder_queue_capacity_per_source = kQueueCapacity;
    config.store_queue_capacity_per_source_worker = 1024U;
    config.parallel_decoder_worker_count = 4U;
    config.parallel_decoder_slots_per_source_worker = 8U;
    config.parallel_decoder_idle_inline_enabled = true;
    // Keep the configured default 8,192. Capacity clamping makes the
    // effective threshold 512 - 128 = 384.
    config.completion_tracker_capacity = 2U;
    config.tick_ring_capacity = 2U;
    config.intraday_store.maximum_session_records = 1024U;
    config.applied_record_sink = blocker;

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            config, &pipeline, &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create capacity-clamped adaptive pipeline: " + detail);
    if (pipeline == nullptr) {
        return;
    }

    const runtime::RealtimePipelineIngressResultV1 first =
        InjectSourceMessage(pipeline.get(), 0U, 1U);
    const bool first_blocked =
        first.accepted() && blocker->WaitUntilFirstAppliedBlocked(3s);
    const runtime::RealtimePipelineIngressResultV1 second =
        first_blocked
            ? InjectSourceMessage(pipeline.get(), 0U, 2U)
            : runtime::RealtimePipelineIngressResultV1{};
    const bool second_parked =
        first_blocked && second.accepted() && WaitUntil([&] {
            const runtime::RealtimePipelineSnapshotV1 snapshot =
                pipeline->Snapshot();
            return snapshot.fatal ||
                   (snapshot.decoded_messages == 1U &&
                    snapshot.decoder_queues[0U].message_depth == 0U);
        });

    bool suffix_admitted = second_parked;
    for (std::uint64_t sequence = 3U;
         sequence <= kTotalRecords && suffix_admitted;
         ++sequence) {
        const runtime::RealtimePipelineIngressResultV1 admitted =
            InjectSourceMessage(pipeline.get(), 0U, sequence);
        suffix_admitted =
            admitted.accepted() &&
            admitted.global_ingress_sequence == sequence &&
            admitted.source_sequence == sequence;
    }
    const runtime::RealtimePipelineSnapshotV1 full =
        pipeline->Snapshot();
    const bool exact_full_prefix =
        suffix_admitted && !full.fatal &&
        full.decoder_queues[0U].message_depth == kQueueCapacity &&
        full.decoder_queues[0U].full_count == 0U;

    blocker->ReleaseFirstApplied();
    const bool drained = exact_full_prefix && WaitUntil([&] {
        const runtime::RealtimePipelineSnapshotV1 snapshot =
            pipeline->Snapshot();
        const auto& source = snapshot.parallel_decoder.sources[0U];
        return snapshot.fatal ||
               (snapshot.decoded_messages == kTotalRecords &&
                snapshot.processing_progress.applied_sequence ==
                    kTotalRecords &&
                snapshot.store.appended_records == kTotalRecords &&
                source.dispatched_messages == kTotalRecords &&
                source.completed_messages == kTotalRecords &&
                source.committed_messages == kTotalRecords &&
                source.farm_outstanding == 0U);
    });
    const runtime::RealtimePipelineSnapshotV1 final =
        pipeline->Snapshot();
    const auto& source = final.parallel_decoder.sources[0U];
    test->Expect(
        first_blocked && second_parked && exact_full_prefix && drained &&
            !final.fatal && final.accepted_messages == kTotalRecords &&
            source.farm_messages != 0U &&
            source.inline_messages != 0U &&
            source.dispatched_messages == kTotalRecords &&
            source.completed_messages == kTotalRecords &&
            source.committed_messages == kTotalRecords &&
            source.farm_outstanding == 0U &&
            final.decoder_queues[0U].full_count == 0U,
        "capacity-clamped adaptive routing observes the already-loaded Pop "
        "depth, activates before the 512-record suffix drains, and preserves "
        "the exact prefix");
    pipeline->StopAndDrain();
}

void CheckParallelDecodeFarmFatalDrain(TestContext* test) {
    PipelineCatalogFixture fixture;
    test->Expect(
        MakeCatalogFixture(29U, &fixture),
        "create parallel-decode fatal-drain catalog");
    if (fixture.runtime_state == nullptr) {
        return;
    }

    const std::shared_ptr<ProjectionProbe> no_projection;
    runtime::RealtimePipelineConfigV1 config =
        MakeConfig(fixture, no_projection);
    config.parallel_decoder_worker_count = 2U;
    config.parallel_decoder_slots_per_source_worker = 4U;
    config.parallel_decoder_idle_inline_enabled = false;
    config.maximum_sdk_message_bytes = 256U * 1024U;

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            config, &pipeline, &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create parallel-decode fatal-drain pipeline: " + detail);
    if (pipeline == nullptr) {
        return;
    }

    // Construct both large callback bodies before publishing source sequence
    // one. Its SecurityID is valid, so admission succeeds; stateless decode
    // then validates 4,095 nested queue ranges before reaching the deliberately
    // invalid final descriptor. Sequence three repeats the expensive valid
    // path, keeping worker zero occupied while the ordered owner trips fatal
    // and the remaining already-issued leases take the cancellation path.
    FakeMessage malformed(
        sdk::kProductionMessageKeysV1[0U],
        ShanghaiNestedQueueSnapshotBody(true));
    FakeMessage long_suffix(
        sdk::kProductionMessageKeysV1[0U],
        ShanghaiNestedQueueSnapshotBody(false));

    constexpr std::uint64_t burst_messages = 8U;
    const runtime::RealtimePipelineIngressResultV1 malformed_admitted =
        pipeline->InjectSdkMessageForTest(&malformed);
    bool burst_admitted = malformed_admitted.accepted() &&
                          malformed_admitted.global_ingress_sequence == 1U &&
                          malformed_admitted.source_sequence == 1U;
    if (burst_admitted) {
        FakeMessage second(
            sdk::kProductionMessageKeysV1[0U],
            ShanghaiSnapshotBody("600007"));
        const runtime::RealtimePipelineIngressResultV1 admitted =
            pipeline->InjectSdkMessageForTest(&second);
        second.DestroyCallbackBytes();
        burst_admitted = admitted.accepted() &&
                          admitted.global_ingress_sequence == 2U &&
                          admitted.source_sequence == 2U;
    }
    if (burst_admitted) {
        const runtime::RealtimePipelineIngressResultV1 admitted =
            pipeline->InjectSdkMessageForTest(&long_suffix);
        burst_admitted = admitted.accepted() &&
                          admitted.global_ingress_sequence == 3U &&
                          admitted.source_sequence == 3U;
    }
    for (std::uint64_t sequence = 4U;
         sequence <= burst_messages && burst_admitted;
         ++sequence) {
        FakeMessage suffix(
            sdk::kProductionMessageKeysV1[0U],
            ShanghaiSnapshotBody("600007"));
        const runtime::RealtimePipelineIngressResultV1 admitted =
            pipeline->InjectSdkMessageForTest(&suffix);
        suffix.DestroyCallbackBytes();
        burst_admitted = admitted.accepted() &&
                          admitted.global_ingress_sequence == sequence &&
                          admitted.source_sequence == sequence;
    }
    malformed.DestroyCallbackBytes();
    long_suffix.DestroyCallbackBytes();

    const bool dispatch_settled = burst_admitted && WaitUntil([&] {
        const runtime::RealtimePipelineSnapshotV1 snapshot =
            pipeline->Snapshot();
        return snapshot.fatal ||
               snapshot.parallel_decoder.sources[0U].farm_messages ==
                   burst_messages;
    });
    const runtime::RealtimePipelineSnapshotV1 dispatched =
        pipeline->Snapshot();
    const bool full_farm_burst = dispatch_settled &&
        dispatched.parallel_decoder.sources[0U].farm_messages ==
            burst_messages;
    const bool fatal_observed = full_farm_burst && WaitUntil([&] {
        return pipeline->Snapshot().fatal;
    });
    const runtime::RealtimePipelineSnapshotV1 failed =
        pipeline->Snapshot();

    pipeline->StopAndDrain();
    const runtime::RealtimePipelineSnapshotV1 stopped =
        pipeline->Snapshot();
    std::uint64_t parsed = 0U;
    std::uint64_t parse_failures = 0U;
    std::size_t issue_depth = 0U;
    std::size_t issue_high_water = 0U;
    for (std::uint32_t worker = 0U;
         worker < config.parallel_decoder_worker_count;
         ++worker) {
        parsed += stopped.parallel_decoder.workers[worker]
                      .parsed_messages;
        parse_failures += stopped.parallel_decoder.workers[worker]
                              .parse_failures;
        issue_depth += stopped.parallel_decoder.workers[worker]
                           .issue_depth;
        issue_high_water = std::max(
            issue_high_water,
            stopped.parallel_decoder.workers[worker]
                .issue_high_water);
    }
    const runtime::RealtimeParallelDecoderSourceSnapshotV1& source =
        stopped.parallel_decoder.sources[0U];
    test->Expect(
        burst_admitted && full_farm_burst && fatal_observed &&
            malformed_admitted.error ==
                runtime::RealtimePipelineIngressErrorV1::kNone &&
            failed.accepted_messages == burst_messages &&
            failed.decoded_messages == 0U &&
            failed.processing_progress.accepted_sequence ==
                burst_messages &&
            failed.processing_progress.applied_sequence == 0U &&
            failed.store.appended_records == 0U &&
            failed.last_decode_error !=
                market::MarketDecodeErrorV1::kNone &&
            stopped.stopped && stopped.fatal &&
            stopped.accepted_messages == burst_messages &&
            stopped.decoded_messages == 0U &&
            stopped.rejected_messages == 0U &&
            stopped.decoder_queues[0U].message_depth == 0U &&
            stopped.ingress_pool.active_messages == 0U &&
            source.dispatched_messages == burst_messages &&
            source.inline_messages == 0U &&
            source.farm_messages == burst_messages &&
            source.completed_messages == parsed &&
            source.committed_messages == 0U &&
            source.committed_source_sequence == 0U &&
            source.farm_outstanding == 0U &&
            source.discarded_messages == burst_messages &&
            source.completion_depth == 0U &&
            source.completion_high_water > 1U &&
            source.completion_publish_failures == 0U &&
            parsed >= 1U &&
            parse_failures == 1U &&
            source.discarded_messages > parse_failures &&
            issue_depth == 0U && issue_high_water > 1U,
        "fatal burst drains every issued lease and queued completion without "
        "leaving ingress ownership behind");
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
    CheckExternalIngressCapacityRetryOwnershipAndStop(&test);
    CheckStoreGenerationSinkOrderingAndFailure(&test);
    CheckStoreOnlyGenerationSkipsFactor(&test);
    CheckFastDecoderAcceptedPublicationAndIdleBoundary(&test);
    CheckParallelDecodeFarmOrderedFenceAndDrain(&test);
    CheckParallelDecodeFarmFourSourceWorkerOffsetMapping(&test);
    CheckAdaptiveParallelDecodeOwnershipHandoff(&test);
    CheckCapacityClampedAdaptiveActivation(&test);
    CheckParallelDecodeFarmFatalDrain(&test);

    return test.failures() == 0 ? 0 : 1;
}
