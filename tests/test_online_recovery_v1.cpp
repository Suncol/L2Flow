#include "l2flow/recovery/online_recovery_v1.h"

#include "l2flow/market/daily_instrument_catalog_v2.h"
#include "l2flow/market/instrument_runtime_state_v2.h"
#include "l2flow/realtime/owned_ingress_message_v1.h"
#include "l2flow/runtime/realtime_pipeline_v1.h"
#include "l2flow/sdk/market_message_catalog_v1.h"

#include "mdl_api.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <unistd.h>

namespace {

namespace common = l2flow::common;
namespace market = l2flow::market;
namespace mdl = datayes::mdl;
namespace realtime = l2flow::realtime;
namespace recovery = l2flow::recovery;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;

using namespace std::chrono_literals;

constexpr std::uint32_t kTradeDate = 20260731U;
constexpr std::array<std::uint32_t, 4U> kSourceStreamIds{
    1001U, 1002U, 2001U, 2002U};

class TestContext final {
public:
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    [[nodiscard]] int failures() const noexcept { return failures_; }

private:
    int failures_ = 0;
};

class WireWriter final {
public:
    explicit WireWriter(std::size_t fixed_bytes)
        : bytes_(fixed_bytes, std::byte{0U}) {}

    void StoreU32(std::size_t offset, std::uint32_t value) {
        StoreUnsigned(offset, value);
    }

    void StoreU64(std::size_t offset, std::uint64_t value) {
        StoreUnsigned(offset, value);
    }

    void StoreString(std::size_t descriptor, std::string_view value) {
        const std::size_t start = bytes_.size();
        StoreUnsigned(
            descriptor, static_cast<std::uint16_t>(value.size()));
        StoreU32(
            descriptor + sizeof(std::uint16_t),
            value.empty()
                ? 0U
                : static_cast<std::uint32_t>(start - descriptor));
        const auto encoded = std::as_bytes(std::span(value));
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

[[nodiscard]] std::vector<std::byte> ShenzhenTransactionBody(
    std::uint64_t application_sequence,
    std::uint64_t price_raw) {
    WireWriter writer(70U);
    writer.StoreU32(0U, 12U);
    writer.StoreU64(4U, application_sequence);
    writer.StoreU64(18U, application_sequence - 1U);
    writer.StoreU64(26U, 0U);
    writer.StoreU64(46U, price_raw);
    writer.StoreU64(54U, 33U);
    writer.StoreU32(62U, 70U);
    writer.StoreU32(66U, 93'000'124U);
    writer.StoreString(12U, "010");
    writer.StoreString(34U, "000001");
    writer.StoreString(40U, "102 ");
    return std::move(writer).Take();
}

class TestMessage final : public mdl::MDLMessage {
public:
    TestMessage(
        std::uint64_t vendor_sequence,
        std::uint64_t application_sequence,
        std::uint64_t price_raw)
        : body_(ShenzhenTransactionBody(
              application_sequence, price_raw)) {
        std::memset(&head_, 0, sizeof(head_));
        head_.HeadSize =
            static_cast<std::uint8_t>(sdk::kVendorHeadBytes);
        head_.MessageSize = static_cast<std::uint32_t>(
            sdk::kVendorHeadBytes + body_.size());
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID = 6U;
        head_.ServiceVersion = 101U;
        head_.MessageID = 36U;
        head_.LocalTime.m_Value = 93'000'000U;
        head_.SequenceID = vendor_sequence;
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

private:
    mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
};

class TestReplaySource final : public recovery::StartupReplaySourceV1 {
public:
    TestReplaySource(
        std::vector<std::shared_ptr<TestMessage>> messages,
        std::vector<sdk::MessageKey> fences,
        std::function<bool()> after_fences = {},
        std::size_t checkpoints_before_messages = 0U)
        : messages_(std::move(messages)),
          fences_(std::move(fences)),
          after_fences_(std::move(after_fences)),
          checkpoints_before_messages_(
              checkpoints_before_messages) {}

    recovery::StartupReplayResultV1 Replay(
        recovery::StartupReplaySinkV1& sink) noexcept override {
        replay_calls.fetch_add(1U, std::memory_order_relaxed);
        recovery::StartupReplayResultV1 result{};
        for (const sdk::MessageKey& key : fences_) {
            std::string detail;
            if (!sink.CaptureTupleFence(key, &detail)) {
                result.error = recovery::StartupReplayErrorV1::kSinkRejected;
                result.detail = std::move(detail);
                return result;
            }
        }
        if (after_fences_ && !after_fences_()) {
            result.error = recovery::StartupReplayErrorV1::kUnexpectedFailure;
            result.detail = "test journal capture failed";
            return result;
        }
        for (std::size_t index = 0U;
             index < checkpoints_before_messages_;
             ++index) {
            cooperative_checkpoint_calls.fetch_add(
                1U, std::memory_order_relaxed);
            std::string detail;
            if (!sink.CooperativeCheckpoint(&detail)) {
                result.error =
                    recovery::StartupReplayErrorV1::kSinkRejected;
                result.detail = std::move(detail);
                return result;
            }
        }
        for (std::size_t index = 0U; index < messages_.size(); ++index) {
            recovery::StartupReplayPublicationV1 publication{};
            publication.message = messages_[index].get();
            publication.key = sdk::MessageKey{6U, 101U, 36U};
            publication.source_line = index + 2U;
            publication.csv_sequence =
                messages_[index]->GetHead()->SequenceID;
            std::string detail;
            publish_calls.fetch_add(1U, std::memory_order_relaxed);
            if (!sink.Publish(publication, &detail)) {
                result.error = recovery::StartupReplayErrorV1::kSinkRejected;
                result.error_line = publication.source_line;
                result.detail = std::move(detail);
                return result;
            }
            ++result.counts.shenzhen_transactions;
        }
        return result;
    }

    std::atomic<std::size_t> cooperative_checkpoint_calls{0U};
    std::atomic<std::size_t> publish_calls{0U};
    std::atomic<std::size_t> replay_calls{0U};

private:
    std::vector<std::shared_ptr<TestMessage>> messages_;
    std::vector<sdk::MessageKey> fences_;
    std::function<bool()> after_fences_;
    std::size_t checkpoints_before_messages_ = 0U;
};

class BlockingAppliedSink final
    : public market::RealtimeAppliedRecordSinkV1 {
public:
    [[nodiscard]] bool PublishApplied(
        std::size_t,
        const market::RealtimeHistoryRecordV1&) noexcept override {
        blocked_.store(true, std::memory_order_release);
        while (!released_.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        return true;
    }

    void MarkCoverageLost() noexcept override {
        Release();
    }

    [[nodiscard]] bool WaitUntilBlocked() const {
        const auto deadline =
            std::chrono::steady_clock::now() + 1s;
        while (!blocked_.load(std::memory_order_acquire)) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            std::this_thread::yield();
        }
        return true;
    }

    void Release() noexcept {
        released_.store(true, std::memory_order_release);
    }

private:
    std::atomic<bool> blocked_{false};
    std::atomic<bool> released_{false};
};

[[nodiscard]] common::Identity128 RunId(std::uint8_t first) {
    common::Identity128 result{};
    result[0U] = static_cast<std::byte>(first);
    result[15U] = std::byte{0xa5U};
    return result;
}

[[nodiscard]] runtime::RealtimePipelineLiveStatusV1 HealthyLiveStatus(
    std::uint64_t accepted,
    std::uint64_t applied) {
    runtime::RealtimePipelineLiveStatusV1 result{};
    result.processing_progress.accepted_sequence = accepted;
    result.processing_progress.applied_sequence = applied;
    result.accepting = true;
    return result;
}

template <typename Predicate>
[[nodiscard]] bool WaitUntil(
    Predicate predicate,
    std::chrono::milliseconds timeout = 1s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::yield();
    }
    return true;
}

[[nodiscard]] std::vector<sdk::MessageKey> AllFences() {
    return {
        sdk::kProductionMessageKeysV1.begin(),
        sdk::kProductionMessageKeysV1.end()};
}

struct OnlineFixture final {
    std::filesystem::path journal_path;
    std::shared_ptr<const market::DailyInstrumentCatalogV2> catalog;
    std::unique_ptr<market::InstrumentRuntimeStateV2> runtime_state;
    std::unique_ptr<runtime::RealtimePipelineV1> shadow;
    std::shared_ptr<recovery::MdlLiveJournalV1> journal;

    OnlineFixture() = default;
    OnlineFixture(const OnlineFixture&) = delete;
    OnlineFixture& operator=(const OnlineFixture&) = delete;

    ~OnlineFixture() {
        if (journal != nullptr) {
            static_cast<void>(journal->StopAndFlush());
        }
        if (shadow != nullptr) {
            shadow->StopAndDrain();
        }
        std::error_code error;
        static_cast<void>(
            std::filesystem::remove_all(journal_path, error));
    }

    [[nodiscard]] bool Create(
        std::uint8_t identity,
        std::shared_ptr<market::RealtimeAppliedRecordSinkV1>
            applied_sink = nullptr,
        std::size_t decoder_queue_capacity = 16U) {
        market::DailyInstrumentSourceEntryV2 source{};
        source.key.market = market::MarketV1::kShenzhen;
        source.key.security_id_source = {
            std::byte{'1'}, std::byte{'0'},
            std::byte{'2'}, std::byte{' '}};
        source.key.security_id = {
            std::byte{'0'}, std::byte{'0'}, std::byte{'0'},
            std::byte{'0'}, std::byte{'0'}, std::byte{'1'}};
        source.metadata = market::InstrumentMetadataV2{
            market::QuantityUnitV1::kShare,
            market::SecurityTypeV1::kEquity,
            market::AssetScopeV1::kDocumentedCore};
        market::DailyInstrumentCatalogConfigV2 catalog_config{};
        catalog_config.trade_date = kTradeDate;
        catalog_config.catalog_version = identity;
        catalog_config.session_epoch = identity;
        catalog_config.market_scope = market::kDailyCatalogMainlandScopeV2;
        catalog_config.coverage_complete = true;
        std::unique_ptr<market::DailyInstrumentCatalogV2> created_catalog;
        if (market::DailyInstrumentCatalogV2::Create(
                catalog_config,
                std::span(&source, 1U),
                &created_catalog) !=
                market::DailyInstrumentCatalogCreateErrorV2::kNone ||
            created_catalog == nullptr) {
            return false;
        }
        catalog = std::shared_ptr<const market::DailyInstrumentCatalogV2>(
            std::move(created_catalog));
        if (market::InstrumentRuntimeStateV2::Create(
                *catalog, &runtime_state) !=
                market::InstrumentRuntimeStateErrorV2::kNone ||
            runtime_state == nullptr) {
            return false;
        }

        runtime::RealtimePipelineConfigV1 pipeline_config{};
        pipeline_config.run_id = RunId(identity);
        pipeline_config.trade_date = kTradeDate;
        pipeline_config.daily_catalog = catalog;
        pipeline_config.runtime_state = runtime_state.get();
        pipeline_config.source_stream_ids = kSourceStreamIds;
        pipeline_config.maximum_sdk_message_bytes = 4096U;
        pipeline_config.decoder_queue_capacity_per_source =
            decoder_queue_capacity;
        pipeline_config.store_worker_count = 1U;
        pipeline_config.store_queue_capacity_per_source_worker = 16U;
        pipeline_config.intraday_store.segment_target_bytes =
            market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
        pipeline_config.intraday_store.maximum_session_records = 128U;
        pipeline_config.intraday_store.maximum_session_accounted_bytes =
            16U * 1024U * 1024U;
        pipeline_config.intraday_store.maximum_records_per_batch = 16U;
        pipeline_config.intraday_store.coverage_from_open = true;
        pipeline_config.sdk.enabled = false;
        pipeline_config.external_ingress_enabled = true;
        pipeline_config.applied_record_sink =
            std::move(applied_sink);
        std::string detail;
        if (runtime::RealtimePipelineV1::Create(
                pipeline_config, &shadow, &detail) !=
                runtime::RealtimePipelineCreateErrorV1::kNone ||
            shadow == nullptr) {
            return false;
        }

        journal_path = std::filesystem::temp_directory_path() /
                       ("l2flow-online-recovery-" +
                        std::to_string(
                            static_cast<unsigned long long>(::getpid())) +
                        "-" + std::to_string(identity));
        std::error_code error;
        static_cast<void>(
            std::filesystem::remove_all(journal_path, error));
        recovery::LiveJournalConfigV1 journal_config{};
        journal_config.directory = journal_path;
        journal_config.run_id = RunId(
            static_cast<std::uint8_t>(identity + 64U));
        journal_config.trade_date = kTradeDate;
        journal_config.maximum_message_bytes = 4096U;
        journal_config.segment_maximum_bytes =
            recovery::kLiveJournalMinimumSegmentBytesV1;
        journal_config.maximum_total_bytes = 4ULL * 1024ULL * 1024ULL;
        journal_config.queue_capacity_records = 32U;
        journal_config.sync_batch_records = 8U;
        journal_config.sync_interval = 1ms;
        return recovery::MdlLiveJournalV1::Create(
                   journal_config, &journal) ==
                   recovery::LiveJournalErrorV1::kNone &&
               journal != nullptr;
    }

    [[nodiscard]] bool Capture(
        const TestMessage& message,
        std::uint64_t clock) const {
        if (journal == nullptr) {
            return false;
        }
        realtime::OwnedIngressMessageInspectionV1 inspection{};
        if (realtime::InspectOwnedIngressMessageV1(
                &message, 4096U, &inspection) !=
                realtime::OwnedIngressMessageErrorV1::kNone ||
            !inspection) {
            return false;
        }
        realtime::RealtimeIngressCaptureInputV1 input{};
        input.inspection = &inspection;
        input.recv_realtime_ns = clock;
        input.recv_monotonic_ns = clock + 1U;
        return journal->Capture(input);
    }

    [[nodiscard]] recovery::OnlineRecoveryConfigV1 Config(
        std::shared_ptr<recovery::StartupReplaySourceV1> source) const {
        recovery::OnlineRecoveryConfigV1 config{};
        config.live_journal = journal;
        config.csv_replay_source = std::move(source);
        config.shadow_pipeline = shadow.get();
        config.trade_date = kTradeDate;
        config.source_stream_ids = kSourceStreamIds;
        config.maximum_message_bytes = 4096U;
        config.overlap_retention_per_tuple = 8U;
        config.warmup_timeout = 5s;
        config.per_record_admission_timeout = 2s;
        return config;
    }
};

[[nodiscard]] std::unique_ptr<recovery::OnlineRecoveryHandoffV1>
CreateHandoff(
    TestContext* test,
    recovery::OnlineRecoveryConfigV1 config,
    std::string_view description) {
    std::unique_ptr<recovery::OnlineRecoveryHandoffV1> handoff;
    std::string detail;
    const auto error = recovery::OnlineRecoveryHandoffV1::Create(
        std::move(config), &handoff, &detail);
    test->Expect(
        error == recovery::OnlineRecoveryErrorV1::kNone &&
            handoff != nullptr,
        description);
    if (handoff == nullptr && !detail.empty()) {
        std::cerr << "online create detail: " << detail << '\n';
    }
    return handoff;
}

void TestPromotionBoundaryAndTail(TestContext* test) {
    OnlineFixture fixture;
    test->Expect(fixture.Create(11U), "create online boundary fixture");
    if (fixture.shadow == nullptr || fixture.journal == nullptr) {
        return;
    }
    auto csv = std::make_shared<TestMessage>(100U, 100U, 100'000U);
    auto duplicate =
        std::make_shared<TestMessage>(100U, 100U, 100'000U);
    auto suffix =
        std::make_shared<TestMessage>(101U, 101U, 101'000U);
    auto source = std::make_shared<TestReplaySource>(
        std::vector<std::shared_ptr<TestMessage>>{csv},
        AllFences(),
        [&fixture, duplicate, suffix] {
            return fixture.Capture(*duplicate, 1'000U) &&
                   fixture.Capture(*suffix, 2'000U);
        });
    auto config = fixture.Config(source);
    config.governor_quantum_records = 1U;
    config.certified_queue_utilization_percent = [] { return 80U; };
    config.certified_handoff_healthy = [] { return true; };
    std::atomic<std::uint64_t> preview_samples{0U};
    config.preview_live_status = [&preview_samples] {
        preview_samples.fetch_add(1U, std::memory_order_relaxed);
        return HealthyLiveStatus(0U, 0U);
    };
    auto handoff = CreateHandoff(
        test, std::move(config), "create online boundary handoff");
    if (handoff == nullptr) {
        return;
    }

    const recovery::OnlineRecoveryBoundaryV1 boundary =
        handoff->RecoverToPromotionBoundary();
    const recovery::OnlineRecoverySnapshotV1 recovered =
        handoff->Snapshot();
    test->Expect(
        boundary.ready() && boundary.journal_frontier == 2U &&
            boundary.shadow_ingress_frontier == 2U &&
            recovered.csv_complete &&
            recovered.promotion_boundary_ready &&
            !recovered.promoted &&
            recovered.csv_publications == 1U &&
            recovered.journal_records_read == 2U &&
            recovered.journal_duplicates_suppressed == 1U &&
            recovered.journal_overlap_digests == 1U &&
            recovered.journal_live_suffix_digests_skipped == 1U &&
            recovered.journal_suffix_publications == 1U &&
            recovered.last_journal_serial == 2U &&
            recovered.promotion_journal_frontier == 2U &&
            recovered.promotion_shadow_ingress_frontier == 2U &&
            recovered.replay_throttle_events == 3U,
        "CSV duplicate is suppressed, suffix is admitted, and B is fixed under throttling");

    const runtime::RealtimePipelineCutResultV1 first_generation =
        fixture.shadow->CutAndPublishGeneration(3s);
    market::IntradayInstrumentSummaryV1 first_row{};
    test->Expect(
        first_generation.published() &&
            first_generation.store_generation->Find(1U, &first_row) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            first_row.record_count == 2U,
        "first authoritative generation contains CSV plus non-overlap live suffix");

    test->Expect(
        handoff->MarkPromoted(9'999U) &&
            !handoff->MarkPromoted(10'000U),
        "promotion completion timestamp is nonzero and write-once");

    preview_samples.store(0U, std::memory_order_relaxed);
    auto tail = std::make_shared<TestMessage>(102U, 102U, 102'000U);
    test->Expect(
        fixture.Capture(*tail, 3'000U),
        "capture B+1 after promotion boundary");
    const auto before_tail = handoff->Snapshot();
    test->Expect(
        before_tail.last_journal_serial == 2U &&
            before_tail.promotion_journal_frontier == 2U,
        "later journal acceptance does not move immutable promotion frontier B");
    const auto pumped = handoff->PumpNext(
        std::chrono::steady_clock::now() + 3s);
    test->Expect(
        pumped.disposition ==
                recovery::OnlineRecoveryPumpDispositionV1::kRecord &&
            pumped.error == recovery::OnlineRecoveryErrorV1::kNone &&
            pumped.journal_serial == 3U &&
            preview_samples.load(std::memory_order_relaxed) == 1U,
        "permanent tail consumes B+1 after one owner-health sample");
    test->Expect(
        fixture.journal->StopAndFlush(),
        "flush online tail journal");
    preview_samples.store(0U, std::memory_order_relaxed);
    const auto end = handoff->PumpNext(
        std::chrono::steady_clock::now() + 3s);
    const auto final_snapshot = handoff->Snapshot();
    test->Expect(
        end.disposition == recovery::OnlineRecoveryPumpDispositionV1::kEnd &&
            final_snapshot.promoted &&
            final_snapshot.promotion_realtime_ns == 9'999U &&
            final_snapshot.last_journal_serial == 3U &&
            final_snapshot.journal_overlap_digests == 1U &&
            final_snapshot.journal_live_suffix_digests_skipped == 2U &&
            final_snapshot.replay_throttle_events == 3U &&
            final_snapshot.journal_suffix_publications == 2U &&
            preview_samples.load(std::memory_order_relaxed) == 1U,
        "tail skips redundant digests/cooldown and samples owner health once before End");

    const runtime::RealtimePipelineCutResultV1 final_generation =
        fixture.shadow->CutAndPublishGeneration(3s);
    market::IntradayInstrumentSummaryV1 final_row{};
    test->Expect(
        final_generation.published() &&
            final_generation.store_generation->Find(1U, &final_row) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            final_row.record_count == 3U,
        "post-promotion authoritative Store continues on the journal suffix");
}

void TestPrePromotionCandidateCatchUpAndPhases(TestContext* test) {
    OnlineFixture fixture;
    test->Expect(
        fixture.Create(32U),
        "create pre-promotion candidate fixture");
    if (fixture.shadow == nullptr || fixture.journal == nullptr) {
        return;
    }
    auto source = std::make_shared<TestReplaySource>(
        std::vector<std::shared_ptr<TestMessage>>{},
        AllFences());
    auto handoff = CreateHandoff(
        test,
        fixture.Config(source),
        "create pre-promotion candidate handoff");
    if (handoff == nullptr) {
        return;
    }

    const auto requested_deadline =
        std::chrono::steady_clock::now() + 3s;
    const auto initial =
        handoff->PrepareInitialCandidate(requested_deadline);
    const auto initial_snapshot = handoff->Snapshot();
    test->Expect(
        initial.ready() && initial.journal_frontier == 0U &&
            initial.shadow_ingress_frontier == 0U &&
            initial.warmup_deadline == requested_deadline &&
            source->replay_calls.load(std::memory_order_relaxed) == 1U &&
            initial_snapshot.csv_complete &&
            initial_snapshot.candidate_boundary_ready &&
            initial_snapshot.phase ==
                recovery::OnlineRecoveryPhaseV1::kCandidateReady &&
            !initial_snapshot.promotion_boundary_ready &&
            initial_snapshot.promotion_journal_frontier == 0U &&
            initial_snapshot.promotion_shadow_ingress_frontier == 0U,
        "initial preparation publishes only candidate B=0 under one fixed deadline");

    const auto duplicate_prepare =
        handoff->PrepareInitialCandidate(requested_deadline + 1s);
    const auto pre_promotion_pump = handoff->PumpNext(
        std::chrono::steady_clock::now() + 10ms);
    const auto after_invalid_calls = handoff->Snapshot();
    test->Expect(
        duplicate_prepare.error ==
                recovery::OnlineRecoveryErrorV1::kInvalidConfiguration &&
            !duplicate_prepare.ready() &&
            source->replay_calls.load(std::memory_order_relaxed) == 1U &&
            pre_promotion_pump.disposition ==
                recovery::OnlineRecoveryPumpDispositionV1::kFailed &&
            pre_promotion_pump.error ==
                recovery::OnlineRecoveryErrorV1::kInvalidConfiguration &&
            after_invalid_calls.error ==
                recovery::OnlineRecoveryErrorV1::kNone &&
            after_invalid_calls.phase ==
                recovery::OnlineRecoveryPhaseV1::kCandidateReady &&
            after_invalid_calls.candidate_boundary_ready &&
            !after_invalid_calls.promotion_boundary_ready,
        "CSV initialization is once-only and tail pumping is gated before promotion");

    const auto idle = handoff->CatchUpOneBeforePromotion(
        std::chrono::steady_clock::now());
    test->Expect(
        idle.disposition ==
                recovery::OnlineRecoveryCandidateAdvanceDispositionV1::
                    kIdle &&
            idle.error == recovery::OnlineRecoveryErrorV1::kNone &&
            idle.candidate.ready() &&
            idle.candidate.journal_frontier == 0U &&
            idle.candidate.warmup_deadline == initial.warmup_deadline,
        "an expired short poll leaves the ready candidate unchanged");

    std::array<std::shared_ptr<TestMessage>, 3U> suffixes{
        std::make_shared<TestMessage>(1U, 1U, 100'001U),
        std::make_shared<TestMessage>(2U, 2U, 100'002U),
        std::make_shared<TestMessage>(3U, 3U, 100'003U)};
    bool captured = true;
    for (std::size_t index = 0U; index < suffixes.size(); ++index) {
        captured = captured && fixture.Capture(
            *suffixes[index],
            1'000U + static_cast<std::uint64_t>(index));
    }
    const bool committed = WaitUntil([&fixture] {
        const auto journal = fixture.journal->Snapshot();
        return journal.healthy() && journal.accepted_serial == 3U &&
               journal.committed_serial == 3U;
    });
    test->Expect(
        captured && committed,
        "commit three durable suffix serials before candidate polling");
    if (!captured || !committed) {
        return;
    }

    for (std::uint64_t expected = 1U; expected <= 3U; ++expected) {
        const auto advanced = handoff->CatchUpOneBeforePromotion(
            std::chrono::steady_clock::now() + 1s);
        const auto snapshot = handoff->Snapshot();
        test->Expect(
            advanced.disposition ==
                    recovery::OnlineRecoveryCandidateAdvanceDispositionV1::
                        kAdvanced &&
                advanced.error == recovery::OnlineRecoveryErrorV1::kNone &&
                advanced.candidate.ready() &&
                advanced.candidate.journal_frontier == expected &&
                advanced.candidate.shadow_ingress_frontier == expected &&
                advanced.candidate.warmup_deadline ==
                    initial.warmup_deadline &&
                snapshot.last_journal_serial == expected &&
                snapshot.candidate_journal_frontier == expected &&
                snapshot.candidate_shadow_ingress_frontier == expected &&
                snapshot.candidate_boundary_ready &&
                snapshot.phase ==
                    recovery::OnlineRecoveryPhaseV1::kCandidateReady &&
                !snapshot.promotion_boundary_ready &&
                snapshot.promotion_journal_frontier == 0U &&
                snapshot.promotion_shadow_ingress_frontier == 0U,
            "each pre-promotion catch-up consumes exactly one durable serial without moving final B");
    }

    const auto boundary = handoff->FreezePromotionBoundary();
    const auto frozen = handoff->Snapshot();
    const auto catch_up_after_freeze =
        handoff->CatchUpOneBeforePromotion(
            std::chrono::steady_clock::now() + 10ms);
    const auto pump_before_mark = handoff->PumpNext(
        std::chrono::steady_clock::now() + 10ms);
    const auto frozen_after_invalid_calls = handoff->Snapshot();
    test->Expect(
        boundary.ready() && boundary.journal_frontier == 3U &&
            boundary.shadow_ingress_frontier == 3U &&
            frozen.promotion_boundary_ready && !frozen.promoted &&
            frozen.phase ==
                recovery::OnlineRecoveryPhaseV1::kPromotionFrozen &&
            frozen.promotion_journal_frontier == 3U &&
            frozen.promotion_shadow_ingress_frontier == 3U &&
            catch_up_after_freeze.disposition ==
                recovery::OnlineRecoveryCandidateAdvanceDispositionV1::
                    kFailed &&
            catch_up_after_freeze.error ==
                recovery::OnlineRecoveryErrorV1::kInvalidConfiguration &&
            pump_before_mark.disposition ==
                recovery::OnlineRecoveryPumpDispositionV1::kFailed &&
            pump_before_mark.error ==
                recovery::OnlineRecoveryErrorV1::kInvalidConfiguration &&
            frozen_after_invalid_calls.error ==
                recovery::OnlineRecoveryErrorV1::kNone &&
            frozen_after_invalid_calls.phase ==
                recovery::OnlineRecoveryPhaseV1::kPromotionFrozen &&
            frozen_after_invalid_calls.promotion_journal_frontier == 3U,
        "only FreezePromotionBoundary fixes B and all pre-promotion APIs remain phase gated");

    test->Expect(
        handoff->MarkPromoted(9'001U) &&
            handoff->Snapshot().phase ==
                recovery::OnlineRecoveryPhaseV1::kPromoted,
        "promotion explicitly unlocks the permanent journal tail");
}

void TestEarlyFreezeLeavesDurableSuffixForPromotedTail(
    TestContext* test) {
    OnlineFixture fixture;
    test->Expect(
        fixture.Create(35U),
        "create early-freeze candidate fixture");
    if (fixture.shadow == nullptr || fixture.journal == nullptr) {
        return;
    }
    auto source = std::make_shared<TestReplaySource>(
        std::vector<std::shared_ptr<TestMessage>>{},
        AllFences());
    auto handoff = CreateHandoff(
        test,
        fixture.Config(source),
        "create early-freeze candidate handoff");
    if (handoff == nullptr) {
        return;
    }

    const auto initial = handoff->PrepareInitialCandidate(
        std::chrono::steady_clock::now() + 3s);
    test->Expect(
        initial.ready() && initial.journal_frontier == 0U &&
            initial.shadow_ingress_frontier == 0U,
        "early-freeze fixture starts from candidate B0=0");
    if (!initial.ready()) {
        return;
    }

    std::array<std::shared_ptr<TestMessage>, 3U> suffixes{
        std::make_shared<TestMessage>(1U, 1U, 400'001U),
        std::make_shared<TestMessage>(2U, 2U, 400'002U),
        std::make_shared<TestMessage>(3U, 3U, 400'003U)};
    bool captured = true;
    for (std::size_t index = 0U; index < suffixes.size(); ++index) {
        captured = captured && fixture.Capture(
            *suffixes[index],
            40'000U + static_cast<std::uint64_t>(index));
    }
    const bool committed = WaitUntil([&fixture] {
        const auto journal = fixture.journal->Snapshot();
        return journal.healthy() && journal.accepted_serial == 3U &&
               journal.committed_serial == 3U;
    });
    test->Expect(
        captured && committed,
        "make B+1 through B+3 durable before the single catch-up");
    if (!captured || !committed) {
        return;
    }

    const auto advanced = handoff->CatchUpOneBeforePromotion(
        std::chrono::steady_clock::now() + 1s);
    const auto after_one = handoff->Snapshot();
    const auto durable_before_freeze = fixture.journal->Snapshot();
    test->Expect(
        advanced.disposition ==
                recovery::OnlineRecoveryCandidateAdvanceDispositionV1::
                    kAdvanced &&
            advanced.error == recovery::OnlineRecoveryErrorV1::kNone &&
            advanced.candidate.ready() &&
            advanced.candidate.journal_frontier == 1U &&
            advanced.candidate.shadow_ingress_frontier == 1U &&
            after_one.last_journal_serial == 1U &&
            after_one.candidate_journal_frontier == 1U &&
            after_one.journal_records_read == 1U &&
            !after_one.promotion_boundary_ready &&
            durable_before_freeze.accepted_serial == 3U &&
            durable_before_freeze.committed_serial == 3U,
        "one catch-up consumes only B+1 while durable B+2 and B+3 remain unread");

    const auto boundary = handoff->FreezePromotionBoundary();
    const auto frozen = handoff->Snapshot();
    test->Expect(
        boundary.ready() && boundary.journal_frontier == 1U &&
            boundary.shadow_ingress_frontier == 1U &&
            frozen.phase ==
                recovery::OnlineRecoveryPhaseV1::kPromotionFrozen &&
            frozen.last_journal_serial == 1U &&
            frozen.journal_records_read == 1U &&
            frozen.promotion_journal_frontier == 1U &&
            frozen.promotion_shadow_ingress_frontier == 1U,
        "early Freeze fixes P=B+1 without consuming already-durable B+2/B+3");

    const bool promoted = handoff->MarkPromoted(9'002U);
    const auto second = handoff->PumpNext(
        std::chrono::steady_clock::now() + 1s);
    const auto after_second = handoff->Snapshot();
    const auto third = handoff->PumpNext(
        std::chrono::steady_clock::now() + 1s);
    const auto after_third = handoff->Snapshot();
    test->Expect(
        promoted &&
            second.disposition ==
                recovery::OnlineRecoveryPumpDispositionV1::kRecord &&
            second.error == recovery::OnlineRecoveryErrorV1::kNone &&
            second.journal_serial == 2U &&
            after_second.last_journal_serial == 2U &&
            after_second.promotion_journal_frontier == 1U &&
            third.disposition ==
                recovery::OnlineRecoveryPumpDispositionV1::kRecord &&
            third.error == recovery::OnlineRecoveryErrorV1::kNone &&
            third.journal_serial == 3U &&
            after_third.last_journal_serial == 3U &&
            after_third.journal_records_read == 3U &&
            after_third.promotion_journal_frontier == 1U &&
            after_third.promotion_shadow_ingress_frontier == 1U,
        "only the promoted tail consumes the retained durable suffix in serial order");
}

void TestPrePromotionCancellationWinsJournalEnd(TestContext* test) {
    OnlineFixture fixture;
    test->Expect(
        fixture.Create(34U),
        "create pre-promotion cancellation fixture");
    if (fixture.shadow == nullptr || fixture.journal == nullptr) {
        return;
    }
    auto source = std::make_shared<TestReplaySource>(
        std::vector<std::shared_ptr<TestMessage>>{},
        AllFences());
    std::atomic<bool> cancel_requested{false};
    std::atomic<std::uint64_t> cancel_samples{0U};
    auto config = fixture.Config(source);
    config.cancel_requested = [&] {
        const bool requested =
            cancel_requested.load(std::memory_order_acquire);
        cancel_samples.fetch_add(1U, std::memory_order_release);
        return requested;
    };
    auto handoff = CreateHandoff(
        test,
        std::move(config),
        "create pre-promotion cancellation handoff");
    if (handoff == nullptr) {
        return;
    }

    const auto initial = handoff->PrepareInitialCandidate(
        std::chrono::steady_clock::now() + 3s);
    test->Expect(
        initial.ready(),
        "prepare the cancellation-race candidate");
    if (!initial.ready()) {
        return;
    }
    cancel_samples.store(0U, std::memory_order_release);

    recovery::OnlineRecoveryCandidateAdvanceV1 advance{};
    std::atomic<bool> finished{false};
    std::thread worker([&] {
        advance = handoff->CatchUpOneBeforePromotion(
            std::chrono::steady_clock::now() + 2s);
        finished.store(true, std::memory_order_release);
    });
    const bool waiting_for_next = WaitUntil([&] {
        return handoff->Snapshot().phase ==
                   recovery::OnlineRecoveryPhaseV1::kCandidateCatchUp &&
               cancel_samples.load(std::memory_order_acquire) >= 3U &&
               !finished.load(std::memory_order_acquire);
    });
    // The third cancellation sample is the final check immediately before
    // ReadNext.  Give the worker a scheduling window to enter the empty
    // writing journal's wait for the next durable serial.
    std::this_thread::sleep_for(10ms);
    const bool blocked_before_shutdown =
        waiting_for_next && !finished.load(std::memory_order_acquire);
    cancel_requested.store(true, std::memory_order_release);
    const bool stopped = fixture.journal->StopAndFlush();
    worker.join();

    const auto failed = handoff->Snapshot();
    test->Expect(
        blocked_before_shutdown && stopped &&
            advance.disposition ==
                recovery::OnlineRecoveryCandidateAdvanceDispositionV1::
                    kFailed &&
            advance.error ==
                recovery::OnlineRecoveryErrorV1::kCancelled &&
            advance.candidate.error ==
                recovery::OnlineRecoveryErrorV1::kCancelled &&
            failed.error ==
                recovery::OnlineRecoveryErrorV1::kCancelled &&
            failed.phase == recovery::OnlineRecoveryPhaseV1::kFailed &&
            failed.error !=
                recovery::OnlineRecoveryErrorV1::kJournalFailed,
        "cancellation wins when StopAndFlush wakes a blocked candidate reader with End");
}

void TestCandidateAdmissionUsesFixedWarmupDeadline(TestContext* test) {
    OnlineFixture fixture;
    const auto blocker = std::make_shared<BlockingAppliedSink>();
    test->Expect(
        fixture.Create(33U, blocker, 1U),
        "create pre-promotion admission-budget fixture");
    if (fixture.shadow == nullptr || fixture.journal == nullptr) {
        blocker->Release();
        return;
    }
    auto source = std::make_shared<TestReplaySource>(
        std::vector<std::shared_ptr<TestMessage>>{},
        AllFences());
    auto config = fixture.Config(source);
    config.warmup_timeout = 2s;
    config.per_record_admission_timeout = 1s;
    auto handoff = CreateHandoff(
        test,
        std::move(config),
        "create pre-promotion admission-budget handoff");
    if (handoff == nullptr) {
        blocker->Release();
        return;
    }

    const auto initial = handoff->PrepareInitialCandidate(
        std::chrono::steady_clock::now() + 2s);
    bool shadow_filled = initial.ready();
    for (std::uint64_t sequence = 1U; sequence <= 10U; ++sequence) {
        TestMessage message(
            sequence, sequence, 200'000U + sequence);
        runtime::RealtimePipelineExternalIngressV1 input{};
        input.message = &message;
        input.recv_realtime_ns = 10'000U + sequence;
        input.recv_monotonic_ns = 20'000U + sequence;
        input.admission_timeout = 1s;
        const auto ingress =
            fixture.shadow->IngestExternalMessage(input);
        shadow_filled = shadow_filled && ingress.accepted();
        if (sequence == 1U) {
            shadow_filled =
                shadow_filled && blocker->WaitUntilBlocked();
        }
    }
    const auto filled_status = fixture.shadow->LiveStatus();
    shadow_filled =
        shadow_filled &&
        filled_status.processing_progress.accepted_sequence == 10U &&
        filled_status.processing_progress.applied_sequence == 0U;

    auto suffix =
        std::make_shared<TestMessage>(100U, 100U, 300'000U);
    const bool captured = fixture.Capture(*suffix, 30'000U);
    const bool committed = WaitUntil([&fixture] {
        const auto journal = fixture.journal->Snapshot();
        return journal.healthy() && journal.accepted_serial == 1U &&
               journal.committed_serial == 1U;
    });
    test->Expect(
        shadow_filled && captured && committed,
        "prepare a durable record behind a full shadow admission queue");
    if (!shadow_filled || !captured || !committed) {
        blocker->Release();
        return;
    }

    std::thread releaser([blocker] {
        std::this_thread::sleep_for(80ms);
        blocker->Release();
    });
    const auto started = std::chrono::steady_clock::now();
    const auto advanced = handoff->CatchUpOneBeforePromotion(
        started + 20ms);
    const auto elapsed =
        std::chrono::steady_clock::now() - started;
    releaser.join();
    const auto recovered = handoff->Snapshot();
    test->Expect(
        advanced.disposition ==
                recovery::OnlineRecoveryCandidateAdvanceDispositionV1::
                    kAdvanced &&
            advanced.error == recovery::OnlineRecoveryErrorV1::kNone &&
            advanced.candidate.ready() &&
            advanced.candidate.journal_frontier == 1U &&
            advanced.candidate.shadow_ingress_frontier == 11U &&
            advanced.candidate.warmup_deadline ==
                initial.warmup_deadline &&
            elapsed >= 50ms && elapsed < 500ms &&
            recovered.error == recovery::OnlineRecoveryErrorV1::kNone &&
            recovered.last_journal_serial == 1U &&
            !recovered.promotion_boundary_ready,
        "after dequeue, admission may outlive the short poll but remains inside the fixed warmup deadline");
}

void TestWorkConservingGovernor(TestContext* test) {
    {
        OnlineFixture fixture;
        test->Expect(
            fixture.Create(18U),
            "create preview high-water fixture");
        if (fixture.shadow != nullptr && fixture.journal != nullptr) {
            auto first =
                std::make_shared<TestMessage>(100U, 100U, 100'000U);
            auto second =
                std::make_shared<TestMessage>(101U, 101U, 101'000U);
            auto source = std::make_shared<TestReplaySource>(
                std::vector<std::shared_ptr<TestMessage>>{
                    first, second},
                AllFences(),
                std::function<bool()>{},
                recovery::
                    kStartupReplayCooperativeCheckpointRecordsV1 *
                    2U);
            std::atomic<std::uint64_t> preview_accepted{64U};
            std::atomic<std::uint64_t> preview_applied{0U};
            auto config = fixture.Config(source);
            config.preview_outstanding_low_water_records = 0U;
            config.preview_outstanding_high_water_records = 64U;
            config.pressure_poll_interval = 100us;
            config.preview_live_status = [&] {
                return HealthyLiveStatus(
                    preview_accepted.load(std::memory_order_acquire),
                    preview_applied.load(std::memory_order_acquire));
            };
            config.certified_pressure_sample = [] {
                return recovery::OnlineRecoveryCertifiedPressureV1{};
            };
            auto handoff = CreateHandoff(
                test,
                std::move(config),
                "create preview high-water handoff");
            if (handoff != nullptr) {
                recovery::OnlineRecoveryBoundaryV1 boundary{};
                std::atomic<bool> finished{false};
                std::thread worker([&] {
                    boundary = handoff->RecoverToPromotionBoundary();
                    finished.store(true, std::memory_order_release);
                });
                const bool paused = WaitUntil([&] {
                    return handoff->Snapshot()
                               .preview_pause_events != 0U;
                });
                const auto while_paused = handoff->Snapshot();
                test->Expect(
                    paused && !finished.load(std::memory_order_acquire) &&
                        while_paused.csv_publications == 0U &&
                        source->cooperative_checkpoint_calls.load(
                            std::memory_order_relaxed) == 1U &&
                        source->publish_calls.load(
                            std::memory_order_relaxed) == 0U,
                    "preview high water gates parser work before any publication");
                preview_applied.store(64U, std::memory_order_release);
                worker.join();
                const auto recovered = handoff->Snapshot();
                const auto generation =
                    fixture.shadow->CutAndPublishGeneration(3s);
                market::IntradayInstrumentSummaryV1 row{};
                test->Expect(
                    boundary.ready() &&
                        recovered.preview_pause_events == 1U &&
                        recovered.csv_parser_checkpoint_events ==
                            recovery::
                                kStartupReplayCooperativeCheckpointRecordsV1 *
                                2U &&
                        recovered.csv_publications == 2U &&
                        source->cooperative_checkpoint_calls.load(
                            std::memory_order_relaxed) ==
                            recovery::
                                kStartupReplayCooperativeCheckpointRecordsV1 *
                                2U &&
                        source->publish_calls.load(
                            std::memory_order_relaxed) == 2U &&
                        generation.published() &&
                        generation.store_generation->Find(1U, &row) ==
                            market::IntradayInstrumentStoreQueryErrorV1::
                                kNone &&
                        row.record_count == 2U,
                    "preview drain resumes complete History without loss");
            }
        }
    }

    {
        OnlineFixture fixture;
        test->Expect(
            fixture.Create(19U),
            "create sustained-small-lag fixture");
        if (fixture.shadow != nullptr && fixture.journal != nullptr) {
            auto first =
                std::make_shared<TestMessage>(100U, 100U, 100'000U);
            auto second =
                std::make_shared<TestMessage>(101U, 101U, 101'000U);
            auto source = std::make_shared<TestReplaySource>(
                std::vector<std::shared_ptr<TestMessage>>{
                    first, second},
                AllFences());
            auto config = fixture.Config(source);
            config.preview_live_status = [] {
                return HealthyLiveStatus(1U, 0U);
            };
            auto handoff = CreateHandoff(
                test,
                std::move(config),
                "create sustained-small-lag handoff");
            if (handoff != nullptr) {
                const auto boundary =
                    handoff->RecoverToPromotionBoundary();
                const auto recovered = handoff->Snapshot();
                test->Expect(
                    boundary.ready() &&
                        recovered.csv_publications == 2U &&
                        recovered.preview_pause_events == 0U,
                    "preview lag below high water cannot starve recovery");
            }
        }
    }

    {
        OnlineFixture fixture;
        test->Expect(
            fixture.Create(20U),
            "create quiet-governor fixture");
        if (fixture.shadow != nullptr && fixture.journal != nullptr) {
            auto first =
                std::make_shared<TestMessage>(100U, 100U, 100'000U);
            auto second =
                std::make_shared<TestMessage>(101U, 101U, 101'000U);
            auto source = std::make_shared<TestReplaySource>(
                std::vector<std::shared_ptr<TestMessage>>{
                    first, second},
                AllFences());
            std::atomic<std::uint64_t> certified_samples{0U};
            auto config = fixture.Config(source);
            config.governor_quantum_records = 1U;
            config.preview_live_status = [] {
                return HealthyLiveStatus(0U, 0U);
            };
            config.certified_pressure_sample = [&] {
                certified_samples.fetch_add(
                    1U, std::memory_order_relaxed);
                return recovery::OnlineRecoveryCertifiedPressureV1{};
            };
            auto handoff = CreateHandoff(
                test,
                std::move(config),
                "create quiet-governor handoff");
            if (handoff != nullptr) {
                const auto boundary =
                    handoff->RecoverToPromotionBoundary();
                const auto recovered = handoff->Snapshot();
                test->Expect(
                    boundary.ready() &&
                        recovered.csv_publications == 2U &&
                        certified_samples.load(
                            std::memory_order_relaxed) == 2U &&
                        recovered.replay_throttle_events == 0U &&
                        recovered.replay_pause_events == 0U &&
                        recovered.preview_pause_events == 0U &&
                        recovered.shadow_pause_events == 0U &&
                        recovered.cooperative_yield_events == 0U,
                    "quiet combined-pressure path executes full throughput with no sleep or yield path");
            }
        }
    }

    {
        OnlineFixture fixture;
        test->Expect(
            fixture.Create(21U),
            "create preview pressure deadline fixture");
        if (fixture.shadow != nullptr && fixture.journal != nullptr) {
            auto message =
                std::make_shared<TestMessage>(100U, 100U, 100'000U);
            auto source = std::make_shared<TestReplaySource>(
                std::vector<std::shared_ptr<TestMessage>>{message},
                AllFences(),
                std::function<bool()>{},
                1U);
            auto config = fixture.Config(source);
            config.warmup_timeout = 20ms;
            config.pressure_poll_interval = 100us;
            config.preview_outstanding_low_water_records = 0U;
            config.preview_outstanding_high_water_records = 1U;
            config.preview_live_status = [] {
                return HealthyLiveStatus(1U, 0U);
            };
            auto handoff = CreateHandoff(
                test,
                std::move(config),
                "create preview pressure deadline handoff");
            if (handoff != nullptr) {
                const auto boundary =
                    handoff->RecoverToPromotionBoundary();
                const auto recovered = handoff->Snapshot();
                test->Expect(
                    boundary.error ==
                            recovery::OnlineRecoveryErrorV1::
                                kWarmupTimeout &&
                        !boundary.ready() &&
                        recovered.csv_publications == 0U &&
                        recovered.csv_parser_checkpoint_events == 1U &&
                        recovered.preview_pause_events == 1U &&
                        source->cooperative_checkpoint_calls.load(
                            std::memory_order_relaxed) == 1U &&
                        source->publish_calls.load(
                            std::memory_order_relaxed) == 0U,
                    "persistent preview high water cancels no-Publish parsing at the warmup deadline");
            }
        }
    }

    {
        OnlineFixture fixture;
        test->Expect(
            fixture.Create(22U),
            "create retention overflow fixture");
        if (fixture.shadow != nullptr && fixture.journal != nullptr) {
            auto source = std::make_shared<TestReplaySource>(
                std::vector<std::shared_ptr<TestMessage>>{},
                AllFences());
            auto config = fixture.Config(source);
            config.overlap_retention_per_tuple =
                std::numeric_limits<std::size_t>::max();
            std::unique_ptr<recovery::OnlineRecoveryHandoffV1>
                handoff;
            std::string detail;
            const auto error =
                recovery::OnlineRecoveryHandoffV1::Create(
                    std::move(config), &handoff, &detail);
            test->Expect(
                error ==
                        recovery::OnlineRecoveryErrorV1::
                            kInvalidConfiguration &&
                    handoff == nullptr,
                "retention capacity rejects K+1 overflow before allocation");
        }
    }
}

void TestTerminalHealthPreemptsPressure(TestContext* test) {
    {
        OnlineFixture fixture;
        test->Expect(
            fixture.Create(23U),
            "create pressure/CERTIFIED health fixture");
        if (fixture.shadow != nullptr && fixture.journal != nullptr) {
            auto message =
                std::make_shared<TestMessage>(100U, 100U, 100'000U);
            auto source = std::make_shared<TestReplaySource>(
                std::vector<std::shared_ptr<TestMessage>>{message},
                AllFences(),
                std::function<bool()>{},
                1U);
            std::atomic<bool> certified_healthy{true};
            auto config = fixture.Config(source);
            config.pressure_poll_interval = 1ms;
            config.preview_outstanding_low_water_records = 0U;
            config.preview_outstanding_high_water_records = 1U;
            config.preview_live_status = [] {
                return HealthyLiveStatus(1U, 0U);
            };
            config.certified_pressure_sample = [&] {
                recovery::OnlineRecoveryCertifiedPressureV1 result{};
                result.healthy = certified_healthy.load(
                    std::memory_order_acquire);
                return result;
            };
            auto handoff = CreateHandoff(
                test,
                std::move(config),
                "create pressure/CERTIFIED health handoff");
            if (handoff != nullptr) {
                recovery::OnlineRecoveryBoundaryV1 boundary{};
                std::thread worker([&] {
                    boundary = handoff->RecoverToPromotionBoundary();
                });
                const bool paused = WaitUntil([&] {
                    return handoff->Snapshot()
                               .preview_pause_events == 1U;
                });
                certified_healthy.store(false, std::memory_order_release);
                worker.join();
                const auto snapshot = handoff->Snapshot();
                test->Expect(
                    paused &&
                        boundary.error ==
                            recovery::OnlineRecoveryErrorV1::
                                kCertifiedFailed &&
                        snapshot.preview_pause_events == 1U &&
                        snapshot.csv_publications == 0U &&
                        source->publish_calls.load(
                            std::memory_order_relaxed) == 0U,
                    "preview pressure cannot hide a terminal CERTIFIED failure");
            }
        }
    }

    {
        OnlineFixture fixture;
        test->Expect(
            fixture.Create(24U),
            "create pressure/shadow health fixture");
        if (fixture.shadow != nullptr && fixture.journal != nullptr) {
            auto message =
                std::make_shared<TestMessage>(100U, 100U, 100'000U);
            auto source = std::make_shared<TestReplaySource>(
                std::vector<std::shared_ptr<TestMessage>>{message},
                AllFences(),
                std::function<bool()>{},
                1U);
            auto config = fixture.Config(source);
            config.pressure_poll_interval = 1ms;
            config.preview_outstanding_low_water_records = 0U;
            config.preview_outstanding_high_water_records = 1U;
            config.preview_live_status = [] {
                return HealthyLiveStatus(1U, 0U);
            };
            auto handoff = CreateHandoff(
                test,
                std::move(config),
                "create pressure/shadow health handoff");
            if (handoff != nullptr) {
                recovery::OnlineRecoveryBoundaryV1 boundary{};
                std::thread worker([&] {
                    boundary = handoff->RecoverToPromotionBoundary();
                });
                const bool paused = WaitUntil([&] {
                    return handoff->Snapshot()
                               .preview_pause_events == 1U;
                });
                fixture.shadow->StopAndDrain();
                worker.join();
                const auto snapshot = handoff->Snapshot();
                test->Expect(
                    paused &&
                        boundary.error ==
                            recovery::OnlineRecoveryErrorV1::
                                kShadowAdmissionFailed &&
                        snapshot.preview_pause_events == 1U &&
                        snapshot.csv_publications == 0U &&
                        source->publish_calls.load(
                            std::memory_order_relaxed) == 0U,
                    "preview pressure cannot hide a terminal shadow failure");
            }
        }
    }

    {
        OnlineFixture fixture;
        test->Expect(
            fixture.Create(28U),
            "create pressure/control-plane health fixture");
        if (fixture.shadow != nullptr && fixture.journal != nullptr) {
            auto message =
                std::make_shared<TestMessage>(100U, 100U, 100'000U);
            auto source = std::make_shared<TestReplaySource>(
                std::vector<std::shared_ptr<TestMessage>>{message},
                AllFences(),
                std::function<bool()>{},
                1U);
            std::atomic<bool> control_planes_healthy{true};
            auto config = fixture.Config(source);
            config.pressure_poll_interval = 1ms;
            config.preview_outstanding_low_water_records = 0U;
            config.preview_outstanding_high_water_records = 1U;
            config.preview_live_status = [] {
                return HealthyLiveStatus(1U, 0U);
            };
            config.control_planes_healthy = [&] {
                return control_planes_healthy.load(
                    std::memory_order_acquire);
            };
            auto handoff = CreateHandoff(
                test,
                std::move(config),
                "create pressure/control-plane health handoff");
            if (handoff != nullptr) {
                recovery::OnlineRecoveryBoundaryV1 boundary{};
                std::thread worker([&] {
                    boundary = handoff->RecoverToPromotionBoundary();
                });
                const bool paused = WaitUntil([&] {
                    return handoff->Snapshot()
                               .preview_pause_events == 1U;
                });
                control_planes_healthy.store(
                    false, std::memory_order_release);
                worker.join();
                const auto snapshot = handoff->Snapshot();
                test->Expect(
                    paused &&
                        boundary.error ==
                            recovery::OnlineRecoveryErrorV1::
                                kUnexpectedFailure &&
                        snapshot.preview_pause_events == 1U &&
                        snapshot.csv_publications == 0U &&
                        source->publish_calls.load(
                            std::memory_order_relaxed) == 0U,
                    "preview pressure cannot hide a terminal FAST control-plane failure");
            }
        }
    }

    {
        OnlineFixture fixture;
        test->Expect(
            fixture.Create(29U),
            "create pressure/journal health fixture");
        if (fixture.shadow != nullptr && fixture.journal != nullptr) {
            auto message =
                std::make_shared<TestMessage>(100U, 100U, 100'000U);
            auto source = std::make_shared<TestReplaySource>(
                std::vector<std::shared_ptr<TestMessage>>{message},
                AllFences(),
                std::function<bool()>{},
                1U);
            auto config = fixture.Config(source);
            config.pressure_poll_interval = 1ms;
            config.preview_outstanding_low_water_records = 0U;
            config.preview_outstanding_high_water_records = 1U;
            config.preview_live_status = [] {
                return HealthyLiveStatus(1U, 0U);
            };
            auto handoff = CreateHandoff(
                test,
                std::move(config),
                "create pressure/journal health handoff");
            if (handoff != nullptr) {
                recovery::OnlineRecoveryBoundaryV1 boundary{};
                std::thread worker([&] {
                    boundary = handoff->RecoverToPromotionBoundary();
                });
                const bool paused = WaitUntil([&] {
                    return handoff->Snapshot()
                               .preview_pause_events == 1U;
                });
                const bool rejected_invalid_capture =
                    !fixture.journal->Capture({});
                worker.join();
                const auto snapshot = handoff->Snapshot();
                test->Expect(
                    paused && rejected_invalid_capture &&
                        fixture.journal->failed() &&
                        boundary.error ==
                            recovery::OnlineRecoveryErrorV1::
                                kJournalFailed &&
                        snapshot.preview_pause_events == 1U &&
                        snapshot.csv_publications == 0U &&
                        source->publish_calls.load(
                            std::memory_order_relaxed) == 0U,
                    "preview pressure cannot hide an asynchronous live-journal failure");
            }
        }
    }
}

void TestTailHysteresisAcrossDeadlines(TestContext* test) {
    OnlineFixture fixture;
    test->Expect(
        fixture.Create(25U),
        "create cross-deadline hysteresis fixture");
    if (fixture.shadow == nullptr || fixture.journal == nullptr) {
        return;
    }
    auto source = std::make_shared<TestReplaySource>(
        std::vector<std::shared_ptr<TestMessage>>{},
        AllFences());
    std::atomic<std::uint64_t> accepted{0U};
    std::atomic<std::uint64_t> applied{0U};
    auto config = fixture.Config(source);
    config.pressure_poll_interval = 500us;
    config.preview_outstanding_low_water_records = 0U;
    config.preview_outstanding_high_water_records = 4U;
    config.preview_live_status = [&] {
        return HealthyLiveStatus(
            accepted.load(std::memory_order_acquire),
            applied.load(std::memory_order_acquire));
    };
    auto handoff = CreateHandoff(
        test,
        std::move(config),
        "create cross-deadline hysteresis handoff");
    if (handoff == nullptr) {
        return;
    }
    const auto boundary = handoff->RecoverToPromotionBoundary();
    auto suffix =
        std::make_shared<TestMessage>(100U, 100U, 100'000U);
    test->Expect(
        boundary.ready() && handoff->MarkPromoted(1U) &&
            fixture.Capture(*suffix, 1'000U),
        "promote and append suffix for hysteresis test");

    accepted.store(4U, std::memory_order_release);
    const auto high = handoff->PumpNext(
        std::chrono::steady_clock::now() + 3ms);
    const auto after_high = handoff->Snapshot();
    applied.store(2U, std::memory_order_release);
    const auto between = handoff->PumpNext(
        std::chrono::steady_clock::now() + 3ms);
    const auto after_between = handoff->Snapshot();
    applied.store(4U, std::memory_order_release);
    const auto drained = handoff->PumpNext(
        std::chrono::steady_clock::now() + 2s);
    test->Expect(
        high.disposition ==
                recovery::OnlineRecoveryPumpDispositionV1::kIdle &&
            between.disposition ==
                recovery::OnlineRecoveryPumpDispositionV1::kIdle &&
            drained.disposition ==
                recovery::OnlineRecoveryPumpDispositionV1::kRecord &&
            drained.journal_serial == 1U &&
            after_high.preview_pause_events == 1U &&
            after_high.last_journal_serial == 0U &&
            after_between.preview_pause_events == 1U &&
            after_between.last_journal_serial == 0U,
        "a high-water episode remains draining between low/high across PumpNext deadlines");
}

void TestCleanShutdownTailDrain(TestContext* test) {
    OnlineFixture fixture;
    test->Expect(
        fixture.Create(26U),
        "create clean shutdown tail fixture");
    if (fixture.shadow == nullptr || fixture.journal == nullptr) {
        return;
    }
    auto source = std::make_shared<TestReplaySource>(
        std::vector<std::shared_ptr<TestMessage>>{},
        AllFences());
    std::atomic<bool> preview_quiesced{false};
    auto config = fixture.Config(source);
    config.preview_live_status = [&] {
        runtime::RealtimePipelineLiveStatusV1 status =
            HealthyLiveStatus(1U, 1U);
        if (preview_quiesced.load(std::memory_order_acquire)) {
            status.accepting = false;
            status.stopped = true;
        }
        return status;
    };
    auto handoff = CreateHandoff(
        test,
        std::move(config),
        "create clean shutdown tail handoff");
    if (handoff == nullptr) {
        return;
    }
    const auto boundary = handoff->RecoverToPromotionBoundary();
    auto suffix =
        std::make_shared<TestMessage>(100U, 100U, 100'000U);
    const bool rejected_before_promotion =
        !handoff->BeginCleanShutdownTailDrain(
            std::chrono::steady_clock::now() + 2s);
    const bool promoted = handoff->MarkPromoted(1U);
    const bool suffix_captured = fixture.Capture(*suffix, 1'000U);
    const bool drain_started =
        handoff->BeginCleanShutdownTailDrain(
            std::chrono::steady_clock::now() + 2s);
    preview_quiesced.store(true, std::memory_order_release);
    const bool journal_flushed = fixture.journal->StopAndFlush();
    const auto record = handoff->PumpNext(
        std::chrono::steady_clock::now() + 2s);
    const auto end = handoff->PumpNext(
        std::chrono::steady_clock::now() + 2s);
    const auto generation =
        fixture.shadow->StopAndPublishFinalGeneration(2s);
    market::IntradayInstrumentSummaryV1 row{};
    const auto snapshot = handoff->Snapshot();
    const auto journal = fixture.journal->Snapshot();
    test->Expect(
        boundary.ready() && rejected_before_promotion && promoted &&
            suffix_captured && drain_started && journal_flushed &&
            record.disposition ==
                recovery::OnlineRecoveryPumpDispositionV1::kRecord &&
            record.journal_serial == 1U &&
            end.disposition ==
                recovery::OnlineRecoveryPumpDispositionV1::kEnd &&
            snapshot.error ==
                recovery::OnlineRecoveryErrorV1::kNone &&
            snapshot.last_journal_serial == journal.committed_serial &&
            journal.committed_serial == journal.accepted_serial &&
            generation.published() &&
            generation.store_generation->Find(1U, &row) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            row.record_count == 1U,
        "expected preview quiesce still drains the flushed journal through End into final History");
}

void TestCleanShutdownTailDrainFailsClosed(TestContext* test) {
    OnlineFixture fixture;
    test->Expect(
        fixture.Create(27U),
        "create fail-closed shutdown tail fixture");
    if (fixture.shadow == nullptr || fixture.journal == nullptr) {
        return;
    }
    auto source = std::make_shared<TestReplaySource>(
        std::vector<std::shared_ptr<TestMessage>>{},
        AllFences());
    std::atomic<bool> preview_failed{false};
    auto config = fixture.Config(source);
    config.preview_live_status = [&] {
        runtime::RealtimePipelineLiveStatusV1 status =
            HealthyLiveStatus(0U, 0U);
        if (preview_failed.load(std::memory_order_acquire)) {
            status.accepting = false;
            status.stopped = true;
            status.fatal = true;
        }
        return status;
    };
    auto handoff = CreateHandoff(
        test,
        std::move(config),
        "create fail-closed shutdown tail handoff");
    if (handoff == nullptr) {
        return;
    }
    const auto boundary = handoff->RecoverToPromotionBoundary();
    const bool promoted = handoff->MarkPromoted(1U);
    const bool drain_started =
        handoff->BeginCleanShutdownTailDrain(
            std::chrono::steady_clock::now() + 2s);
    preview_failed.store(true, std::memory_order_release);
    const bool journal_flushed = fixture.journal->StopAndFlush();
    const auto pumped = handoff->PumpNext(
        std::chrono::steady_clock::now() + 2s);
    test->Expect(
        boundary.ready() && promoted && drain_started &&
            journal_flushed &&
            pumped.disposition ==
                recovery::OnlineRecoveryPumpDispositionV1::kFailed &&
            pumped.error ==
                recovery::OnlineRecoveryErrorV1::kUnexpectedFailure &&
            handoff->Snapshot().error ==
                recovery::OnlineRecoveryErrorV1::kUnexpectedFailure,
        "shutdown relaxation never hides a fatal preview owner");
}

void TestCleanShutdownTailDrainDeadline(TestContext* test) {
    OnlineFixture fixture;
    test->Expect(
        fixture.Create(30U),
        "create bounded shutdown tail fixture");
    if (fixture.shadow == nullptr || fixture.journal == nullptr) {
        return;
    }
    auto source = std::make_shared<TestReplaySource>(
        std::vector<std::shared_ptr<TestMessage>>{},
        AllFences());
    std::atomic<bool> permanently_saturated{false};
    auto config = fixture.Config(source);
    config.pressure_poll_interval = 100us;
    config.preview_outstanding_low_water_records = 0U;
    config.preview_outstanding_high_water_records = 1U;
    config.preview_live_status = [&] {
        return permanently_saturated.load(std::memory_order_acquire)
                   ? HealthyLiveStatus(2U, 0U)
                   : HealthyLiveStatus(0U, 0U);
    };
    auto handoff = CreateHandoff(
        test,
        std::move(config),
        "create bounded shutdown tail handoff");
    if (handoff == nullptr) {
        return;
    }
    const auto boundary = handoff->RecoverToPromotionBoundary();
    const bool promoted = handoff->MarkPromoted(1U);
    const bool rejected_expired_deadline =
        !handoff->BeginCleanShutdownTailDrain(
            std::chrono::steady_clock::now());
    const bool drain_started =
        handoff->BeginCleanShutdownTailDrain(
            std::chrono::steady_clock::now() + 10ms);
    const bool rejected_deadline_extension =
        !handoff->BeginCleanShutdownTailDrain(
            std::chrono::steady_clock::now() + 1s);
    permanently_saturated.store(true, std::memory_order_release);
    const bool journal_flushed = fixture.journal->StopAndFlush();
    const auto started = std::chrono::steady_clock::now();
    const auto pumped = handoff->PumpNext(
        started + 1s);
    const auto elapsed =
        std::chrono::steady_clock::now() - started;
    test->Expect(
        boundary.ready() && promoted && rejected_expired_deadline &&
            drain_started && rejected_deadline_extension &&
            journal_flushed &&
            pumped.disposition ==
                recovery::OnlineRecoveryPumpDispositionV1::kFailed &&
            pumped.error ==
                recovery::OnlineRecoveryErrorV1::kBackpressureTimeout &&
            handoff->Snapshot().error ==
                recovery::OnlineRecoveryErrorV1::kBackpressureTimeout &&
            elapsed < 250ms,
        "permanent tail pressure fails closed at the clean shutdown deadline");
}

void TestCleanShutdownAdmissionUsesRemainingDeadline(
    TestContext* test) {
    OnlineFixture fixture;
    const auto blocker = std::make_shared<BlockingAppliedSink>();
    test->Expect(
        fixture.Create(31U, blocker, 1U),
        "create bounded shadow-admission fixture");
    if (fixture.shadow == nullptr || fixture.journal == nullptr) {
        blocker->Release();
        return;
    }
    auto source = std::make_shared<TestReplaySource>(
        std::vector<std::shared_ptr<TestMessage>>{},
        AllFences());
    auto config = fixture.Config(source);
    config.per_record_admission_timeout = 2s;
    auto handoff = CreateHandoff(
        test,
        std::move(config),
        "create bounded shadow-admission handoff");
    if (handoff == nullptr) {
        blocker->Release();
        return;
    }

    bool shadow_filled = true;
    for (std::uint64_t sequence = 1U; sequence <= 10U; ++sequence) {
        TestMessage message(
            sequence, sequence, 100'000U + sequence);
        runtime::RealtimePipelineExternalIngressV1 input{};
        input.message = &message;
        input.recv_realtime_ns = 1'000U + sequence;
        input.recv_monotonic_ns = 2'000U + sequence;
        input.admission_timeout = 1s;
        const auto ingress =
            fixture.shadow->IngestExternalMessage(input);
        shadow_filled = shadow_filled && ingress.accepted();
        if (sequence == 1U) {
            shadow_filled =
                shadow_filled && blocker->WaitUntilBlocked();
        }
    }
    const runtime::RealtimePipelineLiveStatusV1 filled_status =
        fixture.shadow->LiveStatus();
    shadow_filled =
        shadow_filled &&
        filled_status.processing_progress.accepted_sequence == 10U &&
        filled_status.processing_progress.applied_sequence == 0U;

    const auto boundary = handoff->RecoverToPromotionBoundary();
    const bool promoted = handoff->MarkPromoted(1U);
    auto suffix =
        std::make_shared<TestMessage>(100U, 100U, 200'000U);
    const bool suffix_captured = fixture.Capture(*suffix, 3'000U);
    const bool journal_flushed = fixture.journal->StopAndFlush();
    const auto drain_deadline =
        std::chrono::steady_clock::now() + 20ms;
    const bool drain_started =
        handoff->BeginCleanShutdownTailDrain(drain_deadline);
    const auto started = std::chrono::steady_clock::now();
    const auto pumped = handoff->PumpNext(started + 1s);
    const auto elapsed =
        std::chrono::steady_clock::now() - started;
    blocker->Release();

    test->Expect(
        shadow_filled && boundary.ready() && promoted &&
            suffix_captured && journal_flushed && drain_started &&
            pumped.disposition ==
                recovery::OnlineRecoveryPumpDispositionV1::kFailed &&
            pumped.error ==
                recovery::OnlineRecoveryErrorV1::kBackpressureTimeout &&
            elapsed < 250ms,
        "a full shadow source queue cannot extend clean shutdown to the ordinary admission timeout");
}

void TestOverlapFailures(TestContext* test) {
    {
        OnlineFixture fixture;
        test->Expect(fixture.Create(12U), "create payload conflict fixture");
        if (fixture.shadow != nullptr && fixture.journal != nullptr) {
            auto csv =
                std::make_shared<TestMessage>(100U, 100U, 100'000U);
            auto conflict =
                std::make_shared<TestMessage>(100U, 100U, 999'000U);
            auto source = std::make_shared<TestReplaySource>(
                std::vector<std::shared_ptr<TestMessage>>{csv},
                AllFences(),
                [&fixture, conflict] {
                    return fixture.Capture(*conflict, 1'000U);
                });
            auto handoff = CreateHandoff(
                test,
                fixture.Config(source),
                "create payload conflict handoff");
            if (handoff != nullptr) {
                const auto boundary =
                    handoff->RecoverToPromotionBoundary();
                const auto recovered = handoff->Snapshot();
                test->Expect(
                    boundary.error ==
                            recovery::OnlineRecoveryErrorV1::kOverlapConflict &&
                        !boundary.ready() &&
                        recovered.error ==
                            recovery::OnlineRecoveryErrorV1::kOverlapConflict &&
                        recovered.journal_overlap_digests == 1U,
                    "same tuple/SequenceID with different semantic payload fails closed");
            }
        }
    }

    {
        OnlineFixture fixture;
        test->Expect(fixture.Create(13U), "create retention eviction fixture");
        if (fixture.shadow != nullptr && fixture.journal != nullptr) {
            auto first =
                std::make_shared<TestMessage>(100U, 100U, 100'000U);
            auto second =
                std::make_shared<TestMessage>(101U, 101U, 101'000U);
            auto delayed =
                std::make_shared<TestMessage>(100U, 100U, 100'000U);
            auto source = std::make_shared<TestReplaySource>(
                std::vector<std::shared_ptr<TestMessage>>{first, second},
                AllFences(),
                [&fixture, delayed] {
                    return fixture.Capture(*delayed, 1'000U);
                });
            auto config = fixture.Config(source);
            config.overlap_retention_per_tuple = 1U;
            auto handoff = CreateHandoff(
                test,
                std::move(config),
                "create retention eviction handoff");
            if (handoff != nullptr) {
                const auto boundary =
                    handoff->RecoverToPromotionBoundary();
                test->Expect(
                    boundary.error ==
                            recovery::OnlineRecoveryErrorV1::kOverlapMissing &&
                        !boundary.ready(),
                    "evicted identity below immutable CSV cutoff is not misclassified as suffix");
            }
        }
    }

    {
        OnlineFixture fixture;
        test->Expect(fixture.Create(14U), "create suffix return fixture");
        if (fixture.shadow != nullptr && fixture.journal != nullptr) {
            auto csv =
                std::make_shared<TestMessage>(100U, 100U, 100'000U);
            auto suffix =
                std::make_shared<TestMessage>(101U, 101U, 101'000U);
            auto delayed_prefix =
                std::make_shared<TestMessage>(100U, 100U, 100'000U);
            auto source = std::make_shared<TestReplaySource>(
                std::vector<std::shared_ptr<TestMessage>>{csv},
                AllFences(),
                [&fixture, suffix, delayed_prefix] {
                    return fixture.Capture(*suffix, 1'000U) &&
                           fixture.Capture(*delayed_prefix, 2'000U);
                });
            auto handoff = CreateHandoff(
                test,
                fixture.Config(source),
                "create suffix return handoff");
            if (handoff != nullptr) {
                const auto boundary =
                    handoff->RecoverToPromotionBoundary();
                const auto recovered = handoff->Snapshot();
                test->Expect(
                    boundary.error ==
                            recovery::OnlineRecoveryErrorV1::kOverlapConflict &&
                        recovered.journal_suffix_publications == 1U &&
                        recovered.journal_overlap_digests == 0U &&
                        recovered.journal_live_suffix_digests_skipped == 1U,
                    "tuple return to retained prefix fails before another semantic decode");
            }
        }
    }
}

void TestFenceHealthAndQuietSession(TestContext* test) {
    {
        OnlineFixture fixture;
        test->Expect(fixture.Create(15U), "create missing fence fixture");
        if (fixture.shadow != nullptr && fixture.journal != nullptr) {
            auto fences = AllFences();
            fences.pop_back();
            auto source = std::make_shared<TestReplaySource>(
                std::vector<std::shared_ptr<TestMessage>>{},
                std::move(fences));
            auto handoff = CreateHandoff(
                test,
                fixture.Config(source),
                "create missing fence handoff");
            if (handoff != nullptr) {
                const auto boundary =
                    handoff->RecoverToPromotionBoundary();
                test->Expect(
                    boundary.error == recovery::OnlineRecoveryErrorV1::
                                          kReplayPublicationInvalid &&
                        !boundary.ready(),
                    "promotion requires a linearized fence for all five production tuples");
            }
        }
    }

    {
        OnlineFixture fixture;
        test->Expect(fixture.Create(16U), "create certified health fixture");
        if (fixture.shadow != nullptr && fixture.journal != nullptr) {
            auto csv =
                std::make_shared<TestMessage>(100U, 100U, 100'000U);
            auto source = std::make_shared<TestReplaySource>(
                std::vector<std::shared_ptr<TestMessage>>{csv},
                AllFences());
            auto config = fixture.Config(source);
            config.certified_queue_utilization_percent = [] { return 100U; };
            config.certified_handoff_healthy = [] { return false; };
            auto handoff = CreateHandoff(
                test,
                std::move(config),
                "create certified health handoff");
            if (handoff != nullptr) {
                const auto boundary =
                    handoff->RecoverToPromotionBoundary();
                test->Expect(
                    boundary.error ==
                            recovery::OnlineRecoveryErrorV1::kCertifiedFailed &&
                        !boundary.ready() &&
                        recovery::OnlineRecoveryErrorNameV1(boundary.error) ==
                            "certified_failed",
                    "terminal CERTIFIED health aborts immediately instead of waiting at 100 percent");
            }
        }
    }

    {
        OnlineFixture fixture;
        test->Expect(fixture.Create(17U), "create quiet session fixture");
        if (fixture.shadow != nullptr && fixture.journal != nullptr) {
            auto source = std::make_shared<TestReplaySource>(
                std::vector<std::shared_ptr<TestMessage>>{},
                AllFences());
            auto handoff = CreateHandoff(
                test,
                fixture.Config(source),
                "create quiet session handoff");
            if (handoff != nullptr) {
                const auto boundary =
                    handoff->RecoverToPromotionBoundary();
                test->Expect(
                    boundary.ready() && boundary.journal_frontier == 0U &&
                        boundary.shadow_ingress_frontier == 0U &&
                        handoff->Snapshot().csv_complete,
                    "quiet five-tuple session reaches a valid zero-record boundary");
                test->Expect(
                    handoff->MarkPromoted(1U) &&
                        fixture.journal->StopAndFlush(),
                    "quiet session can promote and close its empty WAL");
                const auto end = handoff->PumpNext(
                    std::chrono::steady_clock::now() + 2s);
                test->Expect(
                    end.disposition ==
                        recovery::OnlineRecoveryPumpDispositionV1::kEnd,
                    "quiet stopped WAL exposes End without requiring traffic");
            }
        }
    }
}

void TestShanghaiDigestExcludesDecoderStatePhase(TestContext* test) {
    market::ShanghaiTickV1 first{};
    first.common.kind = market::MarketEventKindV1::kShanghaiTick;
    first.common.market = market::MarketV1::kShanghai;
    first.common.security_id = "600001";
    first.common.security_id_valid = true;
    first.business_index = 1;
    first.channel = 12;
    first.raw_type = "A";
    first.raw_tick_flag = "B";
    first.raw_type_valid = true;
    first.raw_tick_flag_valid = true;
    first.fields.action = market::TickActionV1::kAdd;
    first.fields.phase = market::TradingPhaseV1::kStart;
    first.fields.validity_bitmap = 0U;

    market::ShanghaiTickV1 state_advanced = first;
    state_advanced.fields.phase = market::TradingPhaseV1::kContinuous;
    state_advanced.fields.validity_bitmap = market::kTickPhaseValidV1;

    common::Sha256Digest first_digest{};
    common::Sha256Digest state_advanced_digest{};
    const bool first_ok =
        runtime::RealtimePipelineStartupSemanticDigestV1(
            market::DecodedMarketEventV1{first},
            false,
            &first_digest);
    const bool state_advanced_ok =
        runtime::RealtimePipelineStartupSemanticDigestV1(
            market::DecodedMarketEventV1{state_advanced},
            false,
            &state_advanced_digest);
    test->Expect(
        first_ok && state_advanced_ok &&
            first_digest == state_advanced_digest,
        "online Shanghai overlap digest excludes decoder-state-derived phase");

    state_advanced.raw_tick_flag = "S";
    common::Sha256Digest changed_payload_digest{};
    const bool changed_payload_ok =
        runtime::RealtimePipelineStartupSemanticDigestV1(
            market::DecodedMarketEventV1{state_advanced},
            false,
            &changed_payload_digest);
    test->Expect(
        changed_payload_ok && changed_payload_digest != first_digest,
        "online Shanghai overlap digest still binds raw payload semantics");
}

}  // namespace

int main() {
    TestContext test;
    TestPromotionBoundaryAndTail(&test);
    TestPrePromotionCandidateCatchUpAndPhases(&test);
    TestEarlyFreezeLeavesDurableSuffixForPromotedTail(&test);
    TestPrePromotionCancellationWinsJournalEnd(&test);
    TestCandidateAdmissionUsesFixedWarmupDeadline(&test);
    TestWorkConservingGovernor(&test);
    TestTerminalHealthPreemptsPressure(&test);
    TestTailHysteresisAcrossDeadlines(&test);
    TestCleanShutdownTailDrain(&test);
    TestCleanShutdownTailDrainFailsClosed(&test);
    TestCleanShutdownTailDrainDeadline(&test);
    TestCleanShutdownAdmissionUsesRemainingDeadline(&test);
    TestOverlapFailures(&test);
    TestFenceHealthAndQuietSession(&test);
    TestShanghaiDigestExcludesDecoderStatePhase(&test);
    if (test.failures() == 0) {
        std::cout << "online recovery V1 tests passed\n";
        return 0;
    }
    return 1;
}
