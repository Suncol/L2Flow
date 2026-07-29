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
#include <filesystem>
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

#include <stdlib.h>

namespace factor = l2flow::factor;
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

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> pattern{};
        constexpr char literal[] =
            "/tmp/l2flow-observed-pipeline-v2-XXXXXX";
        static_assert(sizeof(literal) <= pattern.size());
        std::memcpy(pattern.data(), literal, sizeof(literal));
        char* const created = ::mkdtemp(pattern.data());
        if (created != nullptr) {
            path_ = created;
        }
    }

    ~TemporaryDirectory() {
        if (!path_.empty()) {
            std::error_code ignored;
            std::filesystem::remove_all(path_, ignored);
        }
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

class JournalSyncBlocker final {
public:
    static void BeforeSync(void* context) noexcept {
        auto* const blocker = static_cast<JournalSyncBlocker*>(context);
        if (blocker == nullptr) {
            return;
        }
        try {
            std::unique_lock<std::mutex> lock(blocker->mutex_);
            blocker->entered_ = true;
            blocker->condition_.notify_all();
            blocker->condition_.wait(lock, [blocker] {
                return blocker->released_;
            });
        } catch (...) {
            // The production path never installs this deterministic test seam.
        }
    }

    [[nodiscard]] bool WaitUntilEntered(
        std::chrono::nanoseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return condition_.wait_for(lock, timeout, [this] {
            return entered_;
        });
    }

    void Release() noexcept {
        try {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                released_ = true;
            }
            condition_.notify_all();
        } catch (...) {
            condition_.notify_all();
        }
    }

private:
    std::mutex mutex_;
    std::condition_variable condition_;
    bool entered_ = false;
    bool released_ = false;
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
    std::int64_t normalized_last_price_p6) {
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
    writer.StoreString(14U, "000001");
    writer.StoreString(20U, "102 ");
    writer.StoreString(26U, "T");
    return std::move(writer).Take();
}

[[nodiscard]] std::vector<std::byte> ShenzhenOrderBody(
    std::uint64_t sequence) {
    WireWriter writer(58U);
    writer.StoreU32(0U, 12U);
    writer.StoreU64(4U, sequence);
    writer.StoreU64(30U, 123'456U);
    writer.StoreU64(38U, 201U);
    writer.StoreU32(46U, 49U);
    writer.StoreU32(50U, 93'000'124U);
    writer.StoreU32(54U, 50U);
    writer.StoreString(12U, "010");
    writer.StoreString(18U, "000001");
    writer.StoreString(24U, "102 ");
    return std::move(writer).Take();
}

[[nodiscard]] std::vector<std::byte> ShanghaiTradeBody(
    std::uint64_t business_index) {
    WireWriter writer(70U);
    writer.StoreU64(0U, business_index);
    writer.StoreU32(8U, 7U);
    writer.StoreU32(18U, 93'000'125U);
    writer.StoreU64(28U, 11'001U);
    writer.StoreU64(36U, 22'002U);
    writer.StoreU32(44U, 12'345U);
    writer.StoreU64(48U, 41U);
    writer.StoreU64(56U, 506'145U);
    writer.StoreString(12U, "600007");
    writer.StoreString(22U, "T");
    writer.StoreString(64U, "B");
    return std::move(writer).Take();
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
        UpdateMaximum(&durable_, progress.durable_sequence);
        UpdateMaximum(&applied_, progress.applied_sequence);
        return true;
    }

    [[nodiscard]] std::uint64_t applied_mask() const noexcept {
        return applied_mask_.load(std::memory_order_acquire);
    }
    [[nodiscard]] std::uint64_t binding_mask() const noexcept {
        return binding_mask_.load(std::memory_order_acquire);
    }
    [[nodiscard]] std::uint64_t durable() const noexcept {
        return durable_.load(std::memory_order_acquire);
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
    std::atomic<std::uint64_t> durable_{0U};
    std::atomic<std::uint64_t> applied_{0U};
    std::atomic<bool> coverage_lost_{false};
    bool block_first_applied_ = false;
    mutable std::mutex gate_mutex_;
    std::condition_variable gate_cv_;
    bool first_applied_blocked_ = false;
    bool release_first_applied_ = false;
};

[[nodiscard]] runtime::RealtimePipelineConfigV1 MakeConfig(
    market::ObservedInstrumentDirectoryV2* directory,
    const std::filesystem::path& journal_path,
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
    config.journal.path = journal_path.string();
    config.journal.queue_capacity = 32U;
    config.journal.max_batch_records = 8U;
    config.journal.max_batch_delay = 50us;
    config.applied_record_sink = projection;
    config.instrument_binding_sink = projection;
    config.processing_progress_sink = projection;
    config.sdk.enabled = false;
    return config;
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
            cut.factor_generation->processing_lag_records() == 0U &&
            cut.factor_generation->durability_lag_records() == 0U,
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
        "applied prefix publishes without waiting for Journal durability");
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
            progress.durable_sequence <= 3U &&
            progress.applied_sequence == 3U &&
            progress.processing_lag_records() == 0U &&
            progress.durability_lag_records() ==
                3U - progress.durable_sequence &&
            cut.store_generation->instrument_count() == 2U,
        "generation carries independent accepted, durable, and applied watermarks");

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
                   projection->durable() <= 3U &&
                   projection->applied() == 3U &&
                   !projection->coverage_lost();
        }),
        "external binding/data/progress projections expose independent progress");
}

void CheckExplicitAppliedDispatchWindow(
    TestContext* test,
    const std::filesystem::path& journal_path) {
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
    runtime::RealtimePipelineConfigV1 config = MakeConfig(
        directory.get(), journal_path, projection);
    config.decoder_queue_capacity_per_source = 1U;
    config.store_worker_count = 2U;
    config.store_queue_capacity_per_source_worker = 64U;
    config.journal.queue_capacity = 32U;
    config.journal.max_batch_records = 1U;

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
            return snapshot.journal.durable_sequence >= window + 1U &&
                   snapshot.decoded_messages == window &&
                   snapshot.processing_progress.accepted_sequence ==
                       window + 1U &&
                   snapshot.processing_progress.applied_sequence == 0U &&
                   !snapshot.fatal;
        }),
        "W+1 is durable but cannot overrun the W-sized completion tracker");

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

void CheckBlockedJournalDoesNotBlockLatest(
    TestContext* test,
    const std::filesystem::path& journal_path) {
    std::unique_ptr<market::ObservedInstrumentDirectoryV2> directory;
    test->Expect(
        market::ObservedInstrumentDirectoryV2::Create(
            market::ObservedInstrumentDirectoryConfigV2{4U, 19U},
            &directory) ==
                market::ObservedInstrumentDirectoryErrorV2::kNone &&
            directory != nullptr,
        "create blocked-Journal directory");
    if (directory == nullptr) {
        return;
    }

    JournalSyncBlocker sync_blocker;
    const auto projection = std::make_shared<ProjectionProbe>();
    runtime::RealtimePipelineConfigV1 config = MakeConfig(
        directory.get(), journal_path, projection);
    config.journal.max_batch_records = 1U;
    config.journal.before_sync_for_test =
        &JournalSyncBlocker::BeforeSync;
    config.journal.before_sync_context_for_test = &sync_blocker;

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            config, &pipeline, &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create pipeline with deterministically blocked Journal sync: " +
            detail);
    if (pipeline == nullptr) {
        return;
    }

    FakeMessage snapshot(
        sdk::kProductionMessageKeysV1[2U],
        ShenzhenSnapshotBody(12'345'600));
    const runtime::RealtimePipelineIngressResultV1 ingress =
        pipeline->InjectSdkMessageForTest(&snapshot);
    snapshot.DestroyCallbackBytes();
    test->Expect(
        ingress.accepted() &&
            sync_blocker.WaitUntilEntered(3s),
        "writer reaches the blocked fdatasync after callback admission");

    market::RealtimeLatestRecordViewV1 latest{};
    test->Expect(
        WaitUntil([&] {
            const runtime::RealtimePipelineSnapshotV1 state =
                pipeline->Snapshot();
            const bool latest_visible =
                pipeline->GetLatestSnapshot(1U, &latest) ==
                    market::RealtimeLatestQueryErrorV1::kNone &&
                latest.record != nullptr &&
                latest.record->ingress_sequence() == 1U;
            return state.processing_progress.accepted_sequence == 1U &&
                   state.processing_progress.applied_sequence == 1U &&
                   state.processing_progress.durable_sequence == 0U &&
                   state.processing_progress.processing_lag_records() ==
                       0U &&
                   state.processing_progress.durability_lag_records() ==
                       1U &&
                   state.journal.written_sequence == 1U &&
                   state.journal.durable_sequence == 0U &&
                   projection->accepted() == 1U &&
                   projection->applied() == 1U &&
                   projection->durable() == 0U &&
                   latest_visible;
        }),
        "applied progress and latest data advance while fdatasync is blocked");

    const runtime::RealtimePipelineCutResultV1 cut =
        pipeline->CutAndPublishGeneration(3s);
    test->Expect(
        cut.published() && cut.store_generation != nullptr &&
            cut.store_generation->watermark()
                    .processing_progress.accepted_sequence == 1U &&
            cut.store_generation->watermark()
                    .processing_progress.applied_sequence == 1U &&
            cut.store_generation->watermark()
                    .processing_progress.durable_sequence == 0U,
        "generation publication waits for applied data but never durability");

    sync_blocker.Release();
    test->Expect(
        WaitUntil([&] {
            const runtime::RealtimePipelineSnapshotV1 state =
                pipeline->Snapshot();
            return state.processing_progress.durable_sequence == 1U &&
                   state.processing_progress.durability_lag_records() ==
                       0U &&
                   projection->durable() == 1U;
        }),
        "durable progress advances independently after sync is released");
    pipeline->StopAndDrain();
}

}  // namespace

int main() {
    TestContext test;
    TemporaryDirectory temporary;
    test.Expect(
        !temporary.path().empty(),
        "temporary Journal directory is available");
    if (temporary.path().empty()) {
        return 1;
    }

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
            MakeConfig(
                directory.get(),
                temporary.path() / "capture.journal",
                projection),
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

    const runtime::RealtimePipelineSnapshotV1 before_stop =
        pipeline->Snapshot();
    test.Expect(
        before_stop.accepted_messages == 3U &&
            before_stop.decoded_messages == 3U &&
            before_stop.processing_progress.accepted_sequence == 3U &&
            before_stop.processing_progress.durable_sequence <= 3U &&
            before_stop.processing_progress.applied_sequence == 3U &&
            !before_stop.fatal,
        "pipeline snapshot exposes independent healthy progress prefixes");

    pipeline->StopAndDrain();
    const runtime::RealtimePipelineSnapshotV1 stopped =
        pipeline->Snapshot();
    test.Expect(
        stopped.stopped && !stopped.accepting && !stopped.fatal &&
            stopped.journal.finished &&
            stopped.journal.durable_sequence == 3U &&
            stopped.processing_progress.accepted_sequence == 3U &&
            stopped.processing_progress.durable_sequence == 3U &&
            stopped.processing_progress.applied_sequence == 3U,
        "mandatory Journal and all workers stop after draining");

    CheckExplicitAppliedDispatchWindow(
        &test, temporary.path() / "window.journal");
    CheckBlockedJournalDoesNotBlockLatest(
        &test, temporary.path() / "blocked-sync.journal");

    return test.failures() == 0 ? 0 : 1;
}
