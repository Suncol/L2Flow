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
#include <memory>
#include <span>
#include <string>
#include <string_view>
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
        std::function<bool()> after_fences = {})
        : messages_(std::move(messages)),
          fences_(std::move(fences)),
          after_fences_(std::move(after_fences)) {}

    recovery::StartupReplayResultV1 Replay(
        recovery::StartupReplaySinkV1& sink) noexcept override {
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
        for (std::size_t index = 0U; index < messages_.size(); ++index) {
            recovery::StartupReplayPublicationV1 publication{};
            publication.message = messages_[index].get();
            publication.key = sdk::MessageKey{6U, 101U, 36U};
            publication.source_line = index + 2U;
            publication.csv_sequence =
                messages_[index]->GetHead()->SequenceID;
            std::string detail;
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

private:
    std::vector<std::shared_ptr<TestMessage>> messages_;
    std::vector<sdk::MessageKey> fences_;
    std::function<bool()> after_fences_;
};

[[nodiscard]] common::Identity128 RunId(std::uint8_t first) {
    common::Identity128 result{};
    result[0U] = static_cast<std::byte>(first);
    result[15U] = std::byte{0xa5U};
    return result;
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

    [[nodiscard]] bool Create(std::uint8_t identity) {
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
        pipeline_config.decoder_queue_capacity_per_source = 16U;
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
    config.certified_queue_utilization_percent = [] { return 80U; };
    config.certified_handoff_healthy = [] { return true; };
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
            recovered.journal_suffix_publications == 1U &&
            recovered.last_journal_serial == 2U &&
            recovered.promotion_journal_frontier == 2U &&
            recovered.promotion_shadow_ingress_frontier == 2U &&
            recovered.replay_throttle_events == 1U,
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
            pumped.journal_serial == 3U,
        "permanent tail consumes B+1 only after boundary handling");
    test->Expect(
        fixture.journal->StopAndFlush(),
        "flush online tail journal");
    const auto end = handoff->PumpNext(
        std::chrono::steady_clock::now() + 3s);
    const auto final_snapshot = handoff->Snapshot();
    test->Expect(
        end.disposition == recovery::OnlineRecoveryPumpDispositionV1::kEnd &&
            final_snapshot.promoted &&
            final_snapshot.promotion_realtime_ns == 9'999U &&
            final_snapshot.last_journal_serial == 3U &&
            final_snapshot.journal_suffix_publications == 2U,
        "tail reaches exact durable End without changing promotion identity");

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
                test->Expect(
                    boundary.error ==
                            recovery::OnlineRecoveryErrorV1::kOverlapConflict &&
                        !boundary.ready() &&
                        handoff->Snapshot().error ==
                            recovery::OnlineRecoveryErrorV1::kOverlapConflict,
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
                test->Expect(
                    boundary.error ==
                            recovery::OnlineRecoveryErrorV1::kOverlapConflict &&
                        handoff->Snapshot().journal_suffix_publications == 1U,
                    "tuple cannot return to retained CSV prefix after entering live suffix");
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
    TestOverlapFailures(&test);
    TestFenceHealthAndQuietSession(&test);
    TestShanghaiDigestExcludesDecoderStatePhase(&test);
    if (test.failures() == 0) {
        std::cout << "online recovery V1 tests passed\n";
        return 0;
    }
    return 1;
}
