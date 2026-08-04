#include "../apps/partial_event_stable_broker_v2.h"

#include "l2flow/ipc/partial_order_event_reader_c_v2.h"
#include "l2flow/ipc/partial_order_event_reader_v2.h"
#include "l2flow/ipc/partial_order_event_reader_v3.h"
#include "l2flow/ipc/realtime_partial_order_event_service_v2.h"
#include "l2flow/market/daily_instrument_catalog_v2.h"
#include "l2flow/market/instrument_runtime_state_v2.h"
#include "l2flow/runtime/realtime_pipeline_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <filesystem>
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

namespace ipc = l2flow::ipc;
namespace app = l2flow::apps;
namespace common = l2flow::common;
namespace market = l2flow::market;
namespace realtime = l2flow::realtime;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;
namespace mdl = datayes::mdl;

namespace {

using namespace std::chrono_literals;
constexpr std::uint32_t kTradeDate = 20260729U;

[[nodiscard]] common::Identity128 TestRunId() noexcept {
    common::Identity128 result{};
    result[0U] = std::byte{0x51U};
    result[15U] = std::byte{0xa7U};
    return result;
}

class TestContext final {
public:
    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << message << '\n';
        }
    }
    [[nodiscard]] int failures() const noexcept { return failures_; }

private:
    int failures_ = 0;
};

class TempDirectory final {
public:
    TempDirectory() {
        std::array<char, 64U> pattern{};
        constexpr std::string_view value =
            "/tmp/l2flow-partial-event-integration-XXXXXX";
        std::copy(value.begin(), value.end(), pattern.begin());
        if (::mkdtemp(pattern.data()) != nullptr) {
            path_ = pattern.data();
        }
    }
    TempDirectory(const TempDirectory&) = delete;
    TempDirectory& operator=(const TempDirectory&) = delete;
    ~TempDirectory() {
        std::error_code error;
        if (!path_.empty()) {
            static_cast<void>(std::filesystem::remove_all(path_, error));
        }
    }
    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

class CReader final {
public:
    CReader() noexcept = default;
    CReader(const CReader&) = delete;
    CReader& operator=(const CReader&) = delete;
    ~CReader() {
        l2flow_partial_order_event_reader_close_v2(value_);
    }
    [[nodiscard]] l2flow_partial_order_event_reader_v2* get()
        const noexcept {
        return value_;
    }
    [[nodiscard]] l2flow_partial_order_event_reader_v2** output() noexcept {
        l2flow_partial_order_event_reader_close_v2(value_);
        value_ = nullptr;
        return &value_;
    }

private:
    l2flow_partial_order_event_reader_v2* value_ = nullptr;
};

template <typename Predicate>
[[nodiscard]] bool WaitUntil(Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::yield();
    }
    return predicate();
}

class WireWriter final {
public:
    explicit WireWriter(std::size_t bytes)
        : bytes_(bytes, std::byte{0}) {}

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
            static_cast<std::uint32_t>(start - descriptor));
        bytes_.resize(start + value.size());
        if (!value.empty()) {
            std::memcpy(bytes_.data() + start, value.data(), value.size());
        }
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
                (value >>
                 static_cast<unsigned int>(index * 8U)) &
                static_cast<Unsigned>(0xffU));
        }
    }
    std::vector<std::byte> bytes_;
};

[[nodiscard]] std::vector<std::byte> ShenzhenOrderBody(
    std::uint32_t channel,
    std::uint64_t sequence,
    std::string_view security_id = "000001") {
    WireWriter writer(58U);
    writer.StoreU32(0U, channel);
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

[[nodiscard]] std::vector<std::byte> ShenzhenTransactionBody(
    std::uint32_t channel,
    std::uint64_t sequence,
    std::uint64_t bid_sequence,
    std::string_view security_id = "000001") {
    WireWriter writer(70U);
    writer.StoreU32(0U, channel);
    writer.StoreU64(4U, sequence);
    writer.StoreU64(18U, bid_sequence);
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

[[nodiscard]] std::vector<std::byte> ShanghaiTradeBody(
    std::uint32_t channel,
    std::uint64_t business_index,
    std::string_view security_id = "600007") {
    WireWriter writer(70U);
    writer.StoreU64(0U, business_index);
    writer.StoreU32(8U, channel);
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
        head_.SequenceID = 99U;
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

class FastSink final : public market::RealtimeAppliedRecordSinkV1 {
public:
    [[nodiscard]] bool PublishApplied(
        std::size_t,
        const market::RealtimeHistoryRecordV1&) noexcept override {
        count_.fetch_add(1U, std::memory_order_release);
        return true;
    }
    void MarkCoverageLost() noexcept override {
        coverage_lost_.store(true, std::memory_order_release);
    }
    [[nodiscard]] std::uint64_t count() const noexcept {
        return count_.load(std::memory_order_acquire);
    }
    [[nodiscard]] bool coverage_lost() const noexcept {
        return coverage_lost_.load(std::memory_order_acquire);
    }

private:
    std::atomic<std::uint64_t> count_{0U};
    std::atomic<bool> coverage_lost_{false};
};

void CountEventFailure(void* context) noexcept {
    auto* const count =
        static_cast<std::atomic<std::uint64_t>*>(context);
    if (count != nullptr) {
        count->fetch_add(1U, std::memory_order_release);
    }
}

[[nodiscard]] std::vector<std::byte> OpaqueBytes(
    std::string_view value) {
    const auto bytes = std::as_bytes(
        std::span(value.data(), value.size()));
    return {bytes.begin(), bytes.end()};
}

struct Fixture final {
    std::shared_ptr<const market::DailyInstrumentCatalogV2> catalog;
    std::unique_ptr<market::InstrumentRuntimeStateV2> runtime_state;
    std::shared_ptr<FastSink> fast;
    std::atomic<std::uint64_t> failure_notifications{0U};
    std::uint32_t shanghai_instrument_id = 0U;
    std::uint32_t shenzhen_instrument_id = 0U;
    std::shared_ptr<ipc::RealtimePartialOrderEventServiceV2> service;
    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::unique_ptr<ipc::PartialOrderEventReaderV2> reader;
    std::unique_ptr<ipc::PartialOrderEventReaderV3> compact_reader;

    ~Fixture() {
        if (pipeline != nullptr) {
            pipeline->StopAndDrain();
        }
        if (service != nullptr) {
            service->Stop();
        }
    }
};

struct BrokerFailureContext final {
    app::PartialEventStableBrokerV2* broker = nullptr;
    pid_t worker = -1;
    std::uint64_t lease_epoch = 0U;
    std::atomic<std::uint64_t> notifications{0U};
};

void SignalBrokerFailure(void* opaque) noexcept {
    auto* const context = static_cast<BrokerFailureContext*>(opaque);
    if (context == nullptr || context->broker == nullptr) {
        return;
    }
    context->notifications.fetch_add(1U, std::memory_order_release);
    context->broker->SignalWorkerFailed(
        context->worker, context->lease_epoch);
}

[[nodiscard]] bool BuildFixture(
    TestContext* test,
    std::chrono::nanoseconds horizon,
    std::uint64_t queue_capacity,
    std::size_t maximum_events,
    std::size_t maximum_order_states,
    Fixture* output,
    ipc::RealtimePartialOrderEventFailureNotifierV2 failure_notifier =
        &CountEventFailure,
    void* failure_notifier_context = nullptr,
    ipc::RealtimePartialOrderEventJournalLayoutV2 journal_layout =
        ipc::RealtimePartialOrderEventJournalLayoutV2::
            kMaterializedStateV2) {
    if (test == nullptr || output == nullptr) {
        return false;
    }
    std::array<market::DailyInstrumentSourceEntryV2, 2U> entries{};
    entries[0U].key.market = market::MarketV1::kShenzhen;
    entries[0U].key.security_id_source = OpaqueBytes("102 ");
    entries[0U].key.security_id = OpaqueBytes("000001");
    entries[1U].key.market = market::MarketV1::kShanghai;
    entries[1U].key.security_id = OpaqueBytes("600007");
    for (auto& entry : entries) {
        entry.metadata.quantity_unit = market::QuantityUnitV1::kShare;
        entry.metadata.security_type = market::SecurityTypeV1::kEquity;
        entry.metadata.asset_scope =
            market::AssetScopeV1::kDocumentedCore;
    }
    market::DailyInstrumentCatalogConfigV2 catalog_config{};
    catalog_config.trade_date = kTradeDate;
    catalog_config.catalog_version = 1U;
    catalog_config.session_epoch = 1U;
    catalog_config.market_scope = market::kDailyCatalogMainlandScopeV2;
    catalog_config.coverage_complete = true;
    std::unique_ptr<market::DailyInstrumentCatalogV2> catalog;
    if (market::DailyInstrumentCatalogV2::Create(
            catalog_config, entries, &catalog) !=
            market::DailyInstrumentCatalogCreateErrorV2::kNone ||
        catalog == nullptr) {
        return false;
    }
    output->catalog =
        std::shared_ptr<const market::DailyInstrumentCatalogV2>(
            std::move(catalog));
    const auto shenzhen = output->catalog->Lookup(entries[0U].key);
    const auto shanghai = output->catalog->Lookup(entries[1U].key);
    if (!shenzhen.known() || !shanghai.known()) {
        return false;
    }
    output->shenzhen_instrument_id = shenzhen.entry->instrument_id;
    output->shanghai_instrument_id = shanghai.entry->instrument_id;
    if (market::InstrumentRuntimeStateV2::Create(
            *output->catalog, &output->runtime_state) !=
            market::InstrumentRuntimeStateErrorV2::kNone ||
        output->runtime_state == nullptr) {
        return false;
    }

    output->fast = std::make_shared<FastSink>();
    ipc::RealtimePartialOrderEventServiceConfigV2 service_config{};
    service_config.run_id = TestRunId();
    service_config.session_epoch = 1U;
    service_config.trade_date = kTradeDate;
    service_config.coverage_start_unix_ns = 1U;
    service_config.fast_sink = output->fast;
    service_config.failure_notifier = failure_notifier;
    service_config.failure_notifier_context =
        failure_notifier_context == nullptr
            ? static_cast<void*>(&output->failure_notifications)
            : failure_notifier_context;
    service_config.channel_capacity = 8U;
    service_config.handoff_queue_capacity = queue_capacity;
    service_config.maximum_pending_entries =
        std::max<std::uint64_t>(64U, queue_capacity * 4U);
    service_config.maximum_pending_entries_per_channel =
        service_config.maximum_pending_entries;
    service_config.duplicate_retention_entries = 64U;
    service_config.maximum_reorder_span =
        std::max<std::uint64_t>(1024U, queue_capacity * 4U);
    service_config.discovery_horizon = horizon;
    service_config.maximum_shanghai_order_states = maximum_order_states;
    service_config.maximum_shenzhen_order_states = maximum_order_states;
    service_config.maximum_derived_events = maximum_events;
    service_config.event_journal_capacity = maximum_events;
    service_config.order_state_capacity = 128U;
    service_config.journal_layout = journal_layout;
    service_config.maximum_mapping_bytes = 4U * 1024U * 1024U;
    service_config.lazy_commit_chunk_bytes = 4096U;
    int system_error = 0;
    const auto create_error =
        ipc::RealtimePartialOrderEventServiceV2::Create(
            service_config, &output->service, &system_error);
    test->Expect(
        create_error ==
                ipc::RealtimePartialOrderEventServiceCreateErrorV2::kNone &&
            output->service != nullptr,
        "create partial Event service");
    if (output->service == nullptr ||
        !output->service->StartWorker(&system_error)) {
        return false;
    }

    int descriptor = -1;
    if (!output->service->DuplicateReadOnlyDescriptor(
            &descriptor, &system_error)) {
        return false;
    }
    const auto session = output->service->session();
    ipc::PartialOrderEventExpectedSessionV2 expected{};
    expected.run_id = session.run_id;
    expected.session_epoch = session.session_epoch;
    expected.trade_date = session.trade_date;
    expected.publication_generation = session.publication_generation;
    expected.correction_epoch = session.correction_epoch;
    const auto reader_error =
        journal_layout ==
                ipc::RealtimePartialOrderEventJournalLayoutV2::
                    kCompactStateReferenceV3
            ? ipc::PartialOrderEventReaderV3::OpenDescriptor(
                  descriptor,
                  expected,
                  &output->compact_reader,
                  &system_error)
            : ipc::PartialOrderEventReaderV2::OpenDescriptor(
                  descriptor,
                  expected,
                  &output->reader,
                  &system_error);
    static_cast<void>(::close(descriptor));
    if (reader_error != ipc::PartialOrderEventReaderOpenErrorV2::kNone ||
        (journal_layout ==
                 ipc::RealtimePartialOrderEventJournalLayoutV2::
                     kCompactStateReferenceV3
             ? output->compact_reader == nullptr
             : output->reader == nullptr)) {
        return false;
    }

    runtime::RealtimePipelineConfigV1 pipeline_config{};
    pipeline_config.run_id = service_config.run_id;
    pipeline_config.trade_date = kTradeDate;
    pipeline_config.daily_catalog = output->catalog;
    pipeline_config.runtime_state = output->runtime_state.get();
    pipeline_config.source_stream_ids = {1001U, 1002U, 2001U, 2002U};
    pipeline_config.maximum_sdk_message_bytes = 4096U;
    pipeline_config.decoder_queue_capacity_per_source = 64U;
    pipeline_config.completion_tracker_capacity = 256U;
    pipeline_config.tick_ring_capacity = 256U;
    pipeline_config.store_worker_count = 1U;
    pipeline_config.store_queue_capacity_per_source_worker = 64U;
    pipeline_config.intraday_store.segment_target_bytes = 4096U;
    pipeline_config.intraday_store.maximum_session_records = 256U;
    pipeline_config.intraday_store.maximum_session_accounted_bytes =
        8U * 1024U * 1024U;
    pipeline_config.intraday_store.maximum_records_per_batch = 64U;
    pipeline_config.intraday_store.coverage_from_open = false;
    pipeline_config.applied_record_sink = output->service;
    pipeline_config.native_sequence_observation_sink = output->service;
    pipeline_config.sdk.enabled = false;
    std::string detail;
    const auto pipeline_error = runtime::RealtimePipelineV1::Create(
        pipeline_config, &output->pipeline, &detail);
    test->Expect(
        pipeline_error == runtime::RealtimePipelineCreateErrorV1::kNone &&
            output->pipeline != nullptr,
        "create partial Event pipeline");
    return output->pipeline != nullptr;
}

[[nodiscard]] bool InjectOrder(
    Fixture* fixture,
    std::uint32_t channel,
    std::uint64_t sequence) {
    FakeMessage message(
        sdk::MessageKey{6U, 101U, 33U},
        ShenzhenOrderBody(channel, sequence));
    return fixture->pipeline->InjectSdkMessageForTest(&message).accepted();
}

[[nodiscard]] bool InjectTransaction(
    Fixture* fixture,
    std::uint32_t channel,
    std::uint64_t sequence,
    std::uint64_t bid_sequence) {
    FakeMessage message(
        sdk::MessageKey{6U, 101U, 36U},
        ShenzhenTransactionBody(channel, sequence, bid_sequence));
    return fixture->pipeline->InjectSdkMessageForTest(&message).accepted();
}

[[nodiscard]] bool InjectShanghaiTrade(
    Fixture* fixture,
    std::uint32_t channel,
    std::uint64_t business_index) {
    FakeMessage message(
        sdk::MessageKey{4U, 101U, 24U},
        ShanghaiTradeBody(channel, business_index));
    return fixture->pipeline->InjectSdkMessageForTest(&message).accepted();
}

[[nodiscard]] bool SnapshotStateErrorCoherent(
    const ipc::RealtimePartialOrderEventServiceSnapshotV2& snapshot) {
    if (snapshot.globally_frozen) {
        return !snapshot.accepting &&
               snapshot.state ==
                   ipc::PartialOrderEventServiceStateV2::kFrozenResource &&
               snapshot.last_error !=
                   ipc::PartialOrderEventLastErrorV2::kNone;
    }
    switch (snapshot.state) {
        case ipc::PartialOrderEventServiceStateV2::kInitializing:
        case ipc::PartialOrderEventServiceStateV2::kContiguous:
        case ipc::PartialOrderEventServiceStateV2::kReordering:
        case ipc::PartialOrderEventServiceStateV2::kCatchingUp:
            return snapshot.last_error ==
                   ipc::PartialOrderEventLastErrorV2::kNone;
        case ipc::PartialOrderEventServiceStateV2::kFrozenConflict:
            return snapshot.last_error ==
                   ipc::PartialOrderEventLastErrorV2::
                       kConflictingDuplicate;
        case ipc::PartialOrderEventServiceStateV2::kFrozenResource:
            return snapshot.last_error ==
                   ipc::PartialOrderEventLastErrorV2::kResourceExhausted;
        case ipc::PartialOrderEventServiceStateV2::kRestarting:
            return snapshot.last_error ==
                   ipc::PartialOrderEventLastErrorV2::kWorkerExited;
        case ipc::PartialOrderEventServiceStateV2::kCorrectionPending:
            return snapshot.last_error ==
                   ipc::PartialOrderEventLastErrorV2::kOutOfOrderInput;
        case ipc::PartialOrderEventServiceStateV2::kStoppedClean:
            return snapshot.last_error ==
                       ipc::PartialOrderEventLastErrorV2::kNone ||
                   snapshot.last_error ==
                       ipc::PartialOrderEventLastErrorV2::kPermanentGap;
    }
    return false;
}

void RunCrossFamilyReorderScenario(TestContext* test) {
    Fixture fixture{};
    if (!BuildFixture(test, 25ms, 64U, 64U, 32U, &fixture)) {
        return;
    }
    test->Expect(
        InjectTransaction(&fixture, 12U, 102U, 100U) &&
            InjectOrder(&fixture, 12U, 100U) &&
            InjectOrder(&fixture, 12U, 101U),
        "inject startup late-lower 6.36:N+2 before 6.33:N/N+1");
    test->Expect(
        WaitUntil([&] {
            return fixture.fast->count() == 3U &&
                   fixture.service->Snapshot().canonical_apply_frontier ==
                       3U;
        }),
        "bounded origin seals and canonical projection catches up");

    ipc::PartialOrderEventStatusSnapshotV2 status{};
    test->Expect(
        fixture.reader->ReadStatus(&status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            status.cut.canonical_apply_frontier == 3U &&
            status.cut.state ==
                ipc::PartialOrderEventServiceStateV2::kContiguous &&
            status.ordering_quality ==
                ipc::PartialOrderEventOrderingQualityV2::
                    kBoundedReorderedPartial,
        "V2 status exposes bounded partial contiguous cut");
    std::vector<ipc::PartialOrderEventEnvelopeV2> rows(
        static_cast<std::size_t>(status.cut.event_published_frontier));
    ipc::PartialOrderEventReadBatchResultV2 batch{};
    const auto read = fixture.reader->ReadEvents(1U, rows, &batch);
    bool monotonic = read == ipc::PartialOrderEventReadResultV2::kOk &&
                     batch.rows_read == rows.size() && !rows.empty();
    bool saw_reordered_ingress = false;
    std::int64_t prior = 0;
    for (const auto& row : rows) {
        monotonic = monotonic &&
                    row.event.native_event_sequence >= prior;
        prior = row.event.native_event_sequence;
        if (row.event.native_event_sequence == 102 &&
            row.event.ingress_sequence == 1U) {
            saw_reordered_ingress = true;
        }
    }
    test->Expect(
        monotonic && saw_reordered_ingress,
        "Event History follows native order while preserving arrival anchors");

    ipc::PartialOrderEventOrderStateV2 order_state{};
    ipc::PartialOrderEventOrderKeyV2 key{};
    key.market = L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1;
    key.instrument_id = fixture.shenzhen_instrument_id;
    key.channel = 12;
    key.order_id = 100;
    test->Expect(
        fixture.reader->FindOrderState(key, &order_state) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            order_state.canonical_apply_sequence == 3U &&
            order_state.order_revision.native_event_sequence == 102,
        "latest order-state reflects the canonical transaction revision");
}

void RunCompactStateJournalScenario(TestContext* test) {
    Fixture fixture{};
    if (!BuildFixture(
            test,
            1ms,
            64U,
            64U,
            32U,
            &fixture,
            &CountEventFailure,
            nullptr,
            ipc::RealtimePartialOrderEventJournalLayoutV2::
                kCompactStateReferenceV3)) {
        return;
    }
    const auto preallocated =
        fixture.service->JournalResourceSnapshot();
    test->Expect(
        fixture.service->journal_wire_major() ==
                ipc::kPartialOrderEventWireMajorV3 &&
            fixture.service->Snapshot().journal_wire_major ==
                ipc::kPartialOrderEventWireMajorV3 &&
            preallocated.fully_preallocated,
        "compact service advertises V3 and preallocates its complete backing");

    test->Expect(
        InjectTransaction(&fixture, 12U, 102U, 100U) &&
            InjectOrder(&fixture, 12U, 100U) &&
            InjectOrder(&fixture, 12U, 101U),
        "inject reordered target traffic into compact Event service");
    test->Expect(
        WaitUntil([&] {
            const auto snapshot = fixture.service->Snapshot();
            return snapshot.canonical_apply_frontier == 3U &&
                   snapshot.published_event_frontier != 0U &&
                   snapshot.observed_native_messages == 3U &&
                   snapshot.applied_records == 3U;
        }),
        "compact Event worker publishes the complete canonical prefix");

    ipc::PartialOrderEventStatusSnapshotV3 status{};
    ipc::PartialOrderEventOrderStateV3 state{};
    const ipc::PartialOrderEventOrderKeyV3 key{
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1,
        fixture.shenzhen_instrument_id,
        12,
        100};
    test->Expect(
        fixture.compact_reader->ReadStatus(&status) ==
                ipc::PartialOrderEventReadResultV3::kOk &&
            status.cut.canonical_apply_frontier == 3U &&
            status.cut.event_published_frontier != 0U &&
            fixture.compact_reader->FindOrderState(key, &state) ==
                ipc::PartialOrderEventReadResultV3::kOk &&
            state.canonical_apply_sequence == 3U &&
            state.order_revision.native_event_sequence == 102,
        "V3 reader reconstructs the final service state from the published Event row");

    const auto after = fixture.service->JournalResourceSnapshot();
    test->Expect(
        after.fully_preallocated &&
            after.event_backing_allocation_calls ==
                preallocated.event_backing_allocation_calls &&
            after.order_state_backing_allocation_calls ==
                preallocated.order_state_backing_allocation_calls &&
            after.event_prefault_attempts ==
                preallocated.event_prefault_attempts &&
            after.order_state_prefault_attempts ==
                preallocated.order_state_prefault_attempts,
        "compact Event worker performs no backing growth or prefault call after startup");
}

void RunShanghaiStartupReorderScenario(TestContext* test) {
    Fixture fixture{};
    if (!BuildFixture(test, 25ms, 64U, 64U, 32U, &fixture)) {
        return;
    }
    test->Expect(
        InjectShanghaiTrade(&fixture, 7U, 102U) &&
            InjectShanghaiTrade(&fixture, 7U, 100U) &&
            InjectShanghaiTrade(&fixture, 7U, 101U),
        "inject SDK-disabled Shanghai (Channel, BizIndex) 102,100,101");
    test->Expect(
        WaitUntil([&] {
            const auto snapshot = fixture.service->Snapshot();
            return fixture.fast->count() == 3U &&
                   snapshot.canonical_apply_frontier == 3U &&
                   snapshot.unsealed_channel_count == 0U &&
                   snapshot.gap_channel_count == 0U;
        }),
        "bounded Shanghai origin seals and reorders the captured suffix");

    ipc::PartialOrderEventStatusSnapshotV2 status{};
    test->Expect(
        fixture.reader->ReadStatus(&status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            status.temporal_coverage ==
                ipc::PartialOrderEventTemporalCoverageV2::kProcessStart &&
            status.ordering_quality ==
                ipc::PartialOrderEventOrderingQualityV2::
                    kBoundedReorderedPartial &&
            status.cut.canonical_apply_frontier == 3U &&
            status.cut.captured_source_frontier == 3U &&
            status.cut.state ==
                ipc::PartialOrderEventServiceStateV2::kContiguous,
        "Shanghai cut remains process-start bounded partial, not native completeness");
    std::vector<ipc::PartialOrderEventEnvelopeV2> rows(
        static_cast<std::size_t>(status.cut.event_published_frontier));
    ipc::PartialOrderEventReadBatchResultV2 batch{};
    const auto read = fixture.reader->ReadEvents(1U, rows, &batch);
    bool ordered =
        read == ipc::PartialOrderEventReadResultV2::kOk &&
        batch.rows_read == rows.size() && !rows.empty();
    std::array<bool, 3U> saw_tick{};
    std::uint64_t prior_canonical = 0U;
    for (std::size_t index = 0U; ordered && index < rows.size(); ++index) {
        const auto& row = rows[index];
        std::int64_t expected_native = 0;
        std::uint64_t expected_arrival = 0U;
        if (row.canonical_apply_sequence == 1U) {
            expected_native = 100;
            expected_arrival = 2U;
            saw_tick[0U] = true;
        } else if (row.canonical_apply_sequence == 2U) {
            expected_native = 101;
            expected_arrival = 3U;
            saw_tick[1U] = true;
        } else if (row.canonical_apply_sequence == 3U) {
            expected_native = 102;
            expected_arrival = 1U;
            saw_tick[2U] = true;
        } else {
            ordered = false;
            continue;
        }
        ordered =
            row.canonical_apply_sequence >= prior_canonical &&
            row.event.derived_event_sequence == index + 1U &&
            row.event.market ==
                L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHANGHAI_V1 &&
            row.event.instrument_id == fixture.shanghai_instrument_id &&
            row.event.native_event_sequence == expected_native &&
            row.event.source_sequence == expected_arrival &&
            row.event.ingress_sequence == expected_arrival;
        prior_canonical = row.canonical_apply_sequence;
    }
    test->Expect(
        ordered && std::all_of(
                       saw_tick.begin(),
                       saw_tick.end(),
                       [](bool value) { return value; }),
        "Shanghai Event History is native ordered and retains source/arrival anchors");
}

void RunShenzhenChannelZeroScenario(TestContext* test) {
    Fixture fixture{};
    if (!BuildFixture(test, 1ms, 64U, 16U, 8U, &fixture)) {
        return;
    }
    test->Expect(
        InjectOrder(&fixture, 0U, 1U) &&
            WaitUntil([&] {
                const auto snapshot = fixture.service->Snapshot();
                return fixture.fast->count() == 1U &&
                       snapshot.canonical_apply_frontier == 1U;
            }) &&
            !fixture.service->Snapshot().globally_frozen,
        "valid Shenzhen channel-zero target reaches FAST and partial Event");
    ipc::PartialOrderEventOrderKeyV2 key{};
    key.market = L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1;
    key.instrument_id = fixture.shenzhen_instrument_id;
    key.channel = 0;
    key.order_id = 1;
    ipc::PartialOrderEventOrderStateV2 state{};
    test->Expect(
        fixture.reader->FindOrderState(key, &state) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            state.canonical_apply_sequence == 1U &&
            state.order_revision.channel == 0 &&
            state.order_revision.native_event_sequence == 1,
        "channel-zero order state is published without a false resource failure");
}

void RunCorrectionIsolationScenario(TestContext* test) {
    Fixture fixture{};
    if (!BuildFixture(test, 2ms, 64U, 64U, 32U, &fixture)) {
        return;
    }
    test->Expect(
        InjectOrder(&fixture, 12U, 200U) &&
            WaitUntil([&] {
                return fixture.service->Snapshot()
                           .canonical_apply_frontier == 1U;
            }),
        "seal first channel origin");
    test->Expect(
        InjectOrder(&fixture, 12U, 199U) &&
            WaitUntil([&] {
                const auto snapshot = fixture.service->Snapshot();
                return fixture.fast->count() == 2U &&
                       snapshot.correction_pending_channel_count == 1U &&
                       snapshot.frozen_channel_count == 0U;
            }),
        "post-seal N-1 order requests a correction generation");
    test->Expect(
        InjectOrder(&fixture, 13U, 300U) &&
            WaitUntil([&] {
                const auto snapshot = fixture.service->Snapshot();
                return fixture.fast->count() == 3U &&
                       snapshot.canonical_apply_frontier == 2U &&
                       snapshot.correction_pending_channel_count == 1U &&
                       !snapshot.globally_frozen;
            }),
        "an independent channel advances while correction remains pending");
    test->Expect(
        InjectTransaction(&fixture, 12U, 199U, 200U) &&
            WaitUntil([&] {
                const auto snapshot = fixture.service->Snapshot();
                return fixture.fast->count() == 4U &&
                       snapshot.frozen_channel_count == 1U &&
                       snapshot.correction_pending_channel_count == 0U &&
                       snapshot.state ==
                           ipc::PartialOrderEventServiceStateV2::
                               kFrozenConflict;
            }),
        "same N-1 identity with a different message freezes conflict above correction");
    test->Expect(
        InjectOrder(&fixture, 13U, 301U),
        "inject an independent channel after the conflicting channel freezes");
    test->Expect(
        WaitUntil([&] {
            const auto snapshot = fixture.service->Snapshot();
            return fixture.fast->count() == 5U &&
                   snapshot.canonical_apply_frontier == 3U &&
                   snapshot.frozen_channel_count == 1U &&
                   snapshot.correction_pending_channel_count == 0U &&
                   !snapshot.globally_frozen;
        }),
        "frozen conflict remains channel-local while another channel advances");
    ipc::PartialOrderEventStatusSnapshotV2 status{};
    test->Expect(
        fixture.reader->ReadStatus(&status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            status.cut.canonical_apply_frontier == 3U &&
            status.cut.state ==
                ipc::PartialOrderEventServiceStateV2::kFrozenConflict &&
            status.cut.last_error ==
                ipc::PartialOrderEventLastErrorV2::
                    kConflictingDuplicate &&
            status.cut.affected_channel_count == 1U &&
            fixture.failure_notifications.load(
                std::memory_order_acquire) == 0U,
        "public cut reports conflict rather than the superseded correction");
    std::array<ipc::PartialOrderEventChannelHealthV2, 8U> channels{};
    std::size_t rows_read = 0U;
    test->Expect(
        fixture.reader->ReadAffectedChannels(
            channels, &rows_read, &status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            rows_read == 1U && channels[0U].channel == 12 &&
            channels[0U].state ==
                ipc::PartialOrderEventServiceStateV2::kFrozenConflict &&
            channels[0U].last_error ==
                ipc::PartialOrderEventLastErrorV2::
                    kConflictingDuplicate,
        "affected-channel row gives frozen-conflict diagnostics priority");

    ipc::PartialOrderEventOrderKeyV2 retained_key{};
    retained_key.market =
        L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1;
    retained_key.instrument_id = fixture.shenzhen_instrument_id;
    retained_key.channel = 12;
    retained_key.order_id = 200;
    ipc::PartialOrderEventOrderStateV2 retained_state{};
    test->Expect(
        fixture.reader->FindOrderState(
            retained_key, &retained_state) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            retained_state.canonical_apply_sequence == 1U &&
            retained_state.order_revision.native_event_sequence == 200,
        "correction and conflict leave the channel's last-good order state readable");
    std::vector<ipc::PartialOrderEventEnvelopeV2> events(
        static_cast<std::size_t>(status.cut.event_published_frontier));
    ipc::PartialOrderEventReadBatchResultV2 batch{};
    const auto event_read =
        fixture.reader->ReadEvents(1U, events, &batch);
    bool saw_retained = false;
    bool saw_independent_first = false;
    bool saw_independent_second = false;
    bool saw_rejected_correction = false;
    for (const auto& event : events) {
        saw_retained =
            saw_retained ||
            (event.canonical_apply_sequence == 1U &&
             event.event.channel == 12 &&
             event.event.native_event_sequence == 200);
        saw_independent_first =
            saw_independent_first ||
            (event.canonical_apply_sequence == 2U &&
             event.event.channel == 13 &&
             event.event.native_event_sequence == 300);
        saw_independent_second =
            saw_independent_second ||
            (event.canonical_apply_sequence == 3U &&
             event.event.channel == 13 &&
             event.event.native_event_sequence == 301);
        saw_rejected_correction =
            saw_rejected_correction ||
            event.event.native_event_sequence == 199;
    }
    test->Expect(
        event_read == ipc::PartialOrderEventReadResultV2::kOk &&
            batch.rows_read == events.size() && saw_retained &&
            saw_independent_first && saw_independent_second &&
            !saw_rejected_correction,
        "last-good Event remains readable while the independent channel publishes");
}

void RunEventFailureIsolationScenario(TestContext* test) {
    Fixture fixture{};
    if (!BuildFixture(test, 1ms, 64U, 16U, 1U, &fixture)) {
        return;
    }
    test->Expect(
        InjectOrder(&fixture, 12U, 1U) &&
            WaitUntil([&] {
                return fixture.service->Snapshot()
                           .canonical_apply_frontier == 1U;
            }),
        "publish first order before projector capacity failure");
    test->Expect(
        InjectOrder(&fixture, 12U, 2U) &&
            WaitUntil([&] {
                return fixture.service->Snapshot().globally_frozen;
            }) &&
            InjectOrder(&fixture, 12U, 3U) &&
            WaitUntil([&] { return fixture.fast->count() == 3U; }),
        "projector failure freezes Event but not later FAST publication");
    ipc::PartialOrderEventStatusSnapshotV2 status{};
    test->Expect(
        !fixture.pipeline->fatal() && !fixture.fast->coverage_lost() &&
            fixture.reader->ReadStatus(&status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            status.cut.canonical_apply_frontier == 1U,
        "reader retains last-good canonical cut after projector failure");

    fixture.service->MarkDraining();
    fixture.pipeline->StopAndDrain();
    fixture.service->MarkStoppedClean();
    const auto terminal = fixture.service->Snapshot();
    test->Expect(
        terminal.globally_frozen &&
            terminal.last_error ==
                ipc::PartialOrderEventLastErrorV2::kResourceExhausted &&
            terminal.state !=
                ipc::PartialOrderEventServiceStateV2::kStoppedClean &&
            fixture.reader->ReadStatus(&status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            status.cut.state !=
                ipc::PartialOrderEventServiceStateV2::kStoppedClean &&
            status.cut.last_error ==
                ipc::PartialOrderEventLastErrorV2::kResourceExhausted &&
            status.cut.canonical_apply_frontier == 1U,
        "capacity failure stays resource-exhausted through clean shutdown");
}

void RunBrokeredFailureIntegrationScenario(TestContext* test) {
    TempDirectory directory;
    if (directory.path().empty()) {
        test->Expect(false, "create temporary Event socket directory");
        return;
    }
    const auto socket_path = directory.path() / "partial.events";
    app::PartialEventStableBrokerConfigV2 broker_config{};
    broker_config.public_socket_path = socket_path;
    broker_config.expected_run_id = TestRunId();
    broker_config.expected_session_epoch = 1U;
    broker_config.expected_trade_date = kTradeDate;
    broker_config.maximum_mapping_bytes = 4U * 1024U * 1024U;
    broker_config.allowed_uid = ::geteuid();
    std::unique_ptr<app::PartialEventStableBrokerV2> broker;
    int system_error = 0;
    test->Expect(
        app::PartialEventStableBrokerV2::Create(
            broker_config, &broker, &system_error) ==
                app::PartialEventStableBrokerCreateErrorV2::kNone &&
            broker != nullptr && system_error == 0,
        "create stable broker for service failure integration");
    if (broker == nullptr) {
        return;
    }

    BrokerFailureContext failure_context{};
    failure_context.broker = broker.get();
    failure_context.worker = ::getpid();
    test->Expect(
        broker->MarkWorkerRestarting(
            failure_context.worker, &failure_context.lease_epoch) &&
            failure_context.lease_epoch != 0U,
        "register exact Event worker lease");

    Fixture fixture{};
    if (!BuildFixture(
            test,
            1ms,
            64U,
            16U,
            1U,
            &fixture,
            &SignalBrokerFailure,
            &failure_context)) {
        return;
    }
    int journal_descriptor = -1;
    const bool duplicated = fixture.service->DuplicateReadOnlyDescriptor(
        &journal_descriptor, &system_error);
    const auto adopt_result = duplicated
                                  ? broker->AdoptGeneration(
                                        journal_descriptor,
                                        fixture.service->session())
                                  : ipc::PartialEventBrokerResultV2::
                                        kInternalError;
    if (journal_descriptor >= 0) {
        static_cast<void>(::close(journal_descriptor));
    }
    test->Expect(
        duplicated &&
            adopt_result == ipc::PartialEventBrokerResultV2::kOk &&
            broker->state() == ipc::PartialEventBrokerStateV2::kReady,
        "adopt the live service journal before test input");
    if (!duplicated || adopt_result != ipc::PartialEventBrokerResultV2::kOk) {
        return;
    }

    l2flow_partial_order_event_expected_session_v2 expected{};
    const common::Identity128 run_id = TestRunId();
    std::memcpy(expected.run_id, run_id.data(), run_id.size());
    expected.session_epoch = 1U;
    expected.trade_date = kTradeDate;
    CReader attached;
    test->Expect(
        l2flow_partial_order_event_reader_open_v2(
            socket_path.c_str(),
            &expected,
            nullptr,
            1000U,
            attached.output(),
            &system_error) == L2FLOW_PARTIAL_ORDER_EVENT_OPEN_OK_V2 &&
            attached.get() != nullptr,
        "attach C reader to the live Event socket");
    if (attached.get() == nullptr) {
        return;
    }

    test->Expect(
        InjectOrder(&fixture, 12U, 1U) &&
            WaitUntil([&] {
                return fixture.service->Snapshot()
                           .canonical_apply_frontier == 1U;
            }),
        "publish one last-good Event before terminal failure");
    l2flow_partial_order_event_checkpoint_v2 checkpoint{};
    l2flow_partial_order_event_envelope_v2 initial_event[1U]{};
    l2flow_partial_order_event_read_batch_result_v2 initial_read{};
    test->Expect(
        l2flow_partial_order_event_reader_checkpoint_v2(
            attached.get(), 1U, 0U, &checkpoint) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            l2flow_partial_order_event_reader_read_v2(
                attached.get(),
                &checkpoint,
                initial_event,
                1U,
                &initial_read) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            initial_read.records_written == 1U &&
            initial_event[0U].canonical_apply_sequence == 1U,
        "attached reader sees the initial Event row");

    test->Expect(
        InjectOrder(&fixture, 12U, 2U) &&
            WaitUntil([&] {
                return fixture.service->Snapshot().globally_frozen &&
                       broker->state() ==
                           ipc::PartialEventBrokerStateV2::kStale;
            }),
        "terminal Event failure marks the broker lease stale");

    l2flow_partial_order_event_envelope_v2 retained_event[1U]{};
    l2flow_partial_order_event_read_batch_result_v2 retained_read{};
    test->Expect(
        l2flow_partial_order_event_reader_read_v2(
            attached.get(),
            &checkpoint,
            retained_event,
            1U,
            &retained_read) == L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            retained_read.records_written == 1U &&
            retained_event[0U].canonical_apply_sequence == 1U &&
            retained_read.status.broker_state ==
                L2FLOW_PARTIAL_ORDER_EVENT_BROKER_STALE_V2 &&
            retained_read.status.broker_stale == 1U,
        "attached reader keeps last-good Event History after failure");

    l2flow_partial_order_event_order_key_v2 key{};
    key.market = L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1;
    key.instrument_id = fixture.shenzhen_instrument_id;
    key.channel = 12;
    key.order_id = 1;
    l2flow_partial_order_event_order_state_v2 attached_state{};
    l2flow_partial_order_event_status_v2 attached_state_status{};
    test->Expect(
        l2flow_partial_order_event_reader_find_order_state_v2(
            attached.get(),
            &key,
            &attached_state,
            &attached_state_status) ==
                L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
            attached_state.canonical_apply_sequence == 1U &&
            attached_state_status.broker_state ==
                L2FLOW_PARTIAL_ORDER_EVENT_BROKER_STALE_V2,
        "attached reader keeps last-good derived order state");

    CReader newly_attached;
    test->Expect(
        l2flow_partial_order_event_reader_open_v2(
            socket_path.c_str(),
            &expected,
            nullptr,
            1000U,
            newly_attached.output(),
            &system_error) == L2FLOW_PARTIAL_ORDER_EVENT_OPEN_OK_V2 &&
            newly_attached.get() != nullptr,
        "the same Event socket accepts a new reader while stale");
    if (newly_attached.get() != nullptr) {
        l2flow_partial_order_event_envelope_v2 new_event[1U]{};
        l2flow_partial_order_event_read_batch_result_v2 new_read{};
        l2flow_partial_order_event_order_state_v2 new_state{};
        l2flow_partial_order_event_status_v2 new_state_status{};
        test->Expect(
            l2flow_partial_order_event_reader_read_v2(
                newly_attached.get(),
                &checkpoint,
                new_event,
                1U,
                &new_read) == L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
                new_read.records_written == 1U &&
                new_read.status.broker_stale == 1U &&
                l2flow_partial_order_event_reader_find_order_state_v2(
                    newly_attached.get(),
                    &key,
                    &new_state,
                    &new_state_status) ==
                    L2FLOW_PARTIAL_ORDER_EVENT_READ_OK_V2 &&
                new_state.canonical_apply_sequence == 1U &&
                new_state_status.broker_stale == 1U,
            "new reader receives the same last-good Event and order state");
    }

    broker->MarkWorkerFailed(
        failure_context.worker, failure_context.lease_epoch);
    test->Expect(
        InjectOrder(&fixture, 12U, 3U) &&
            WaitUntil([&] { return fixture.fast->count() == 3U; }) &&
            fixture.service->Snapshot().canonical_apply_frontier == 1U &&
            failure_context.notifications.load(
                std::memory_order_acquire) == 1U &&
            !fixture.pipeline->fatal(),
        "FAST continues after brokered Event failure without Event mutation");
}

void RunCoverageLossDiagnosticScenario(TestContext* test) {
    Fixture fixture{};
    if (!BuildFixture(test, 1ms, 64U, 16U, 8U, &fixture)) {
        return;
    }
    fixture.service->MarkCoverageLost();
    test->Expect(
        WaitUntil([&] {
            const auto snapshot = fixture.service->Snapshot();
            return fixture.fast->coverage_lost() &&
                   snapshot.globally_frozen &&
                   snapshot.last_error ==
                       ipc::PartialOrderEventLastErrorV2::kPermanentGap &&
                   fixture.failure_notifications.load(
                       std::memory_order_acquire) == 1U;
        }),
        "upstream coverage loss reports permanent gap, not worker exit");
    ipc::PartialOrderEventStatusSnapshotV2 status{};
    test->Expect(
        WaitUntil([&] {
            return fixture.reader->ReadStatus(&status) ==
                       ipc::PartialOrderEventReadResultV2::kOk &&
                   status.cut.last_error ==
                       ipc::PartialOrderEventLastErrorV2::kPermanentGap;
        }),
        "public partial Event cut preserves the coverage-loss diagnosis");
}

void RunQueueFailureIsolationScenario(TestContext* test) {
    Fixture fixture{};
    if (!BuildFixture(test, 20ms, 2U, 16U, 8U, &fixture)) {
        return;
    }
    fixture.service->SetWorkerPausedForTest(true);
    test->Expect(
        WaitUntil([&] {
            return fixture.service->WorkerPauseReachedForTest();
        }),
        "pause Event worker before queue pressure");
    realtime::NativeSequenceObservationV1 observation{};
    observation.descriptor.domain.market =
        realtime::NativeSequenceMarketV1::kShenzhen;
    observation.descriptor.domain.channel = 12U;
    observation.descriptor.sequence = 1U;
    observation.message_key = sdk::MessageKey{6U, 101U, 33U};
    observation.record_class =
        realtime::NativeSequenceRecoveryRecordClassV1::kFiltered;
    fixture.service->ObserveNativeSequence(observation);
    observation.descriptor.sequence = 2U;
    fixture.service->ObserveNativeSequence(observation);
    observation.descriptor.sequence = 3U;
    fixture.service->ObserveNativeSequence(observation);
    const auto pressured = fixture.service->Snapshot();
    test->Expect(
        WaitUntil([&] {
            return fixture.service->Snapshot().globally_frozen &&
                   fixture.failure_notifications.load(
                       std::memory_order_acquire) == 1U;
        }) &&
            pressured.handoff_queue_depth == 2U &&
            pressured.handoff_queue_high_water == 2U,
        "bounded observer queue overflow freezes Event and signals once");
    fixture.service->MarkNativeSequenceObservationFailure(
        realtime::NativeSequenceObservationFailureV1::kExtraction,
        sdk::MessageKey{6U, 101U, 33U});
    const auto repeated_failure = fixture.service->Snapshot();
    fixture.service->SetWorkerPausedForTest(false);
    ipc::PartialOrderEventStatusSnapshotV2 status{};
    test->Expect(
        InjectOrder(&fixture, 12U, 4U) &&
            WaitUntil([&] { return fixture.fast->count() == 1U; }) &&
            WaitUntil([&] {
                return fixture.service->Snapshot().handoff_queue_depth == 0U;
            }) &&
            fixture.service->Snapshot().handoff_queue_high_water == 2U &&
            repeated_failure.state ==
                ipc::PartialOrderEventServiceStateV2::kFrozenResource &&
            repeated_failure.last_error ==
                ipc::PartialOrderEventLastErrorV2::kResourceExhausted &&
            !repeated_failure.accepting &&
            fixture.failure_notifications.load(
                std::memory_order_acquire) == 1U &&
            WaitUntil([&] {
                return fixture.reader->ReadStatus(&status) ==
                           ipc::PartialOrderEventReadResultV2::kOk &&
                       status.cut.last_error ==
                           ipc::PartialOrderEventLastErrorV2::
                               kResourceExhausted;
            }) &&
            !fixture.pipeline->fatal(),
        "FAST remains available and the first terminal diagnosis is retained");
}

void RunTargetJoinHeadGraceScenario(TestContext* test) {
    Fixture fixture{};
    constexpr std::uint64_t kObservationCount = 1024U;
    if (!BuildFixture(
            test,
            1min,
            kObservationCount,
            16U,
            8U,
            &fixture)) {
        return;
    }

    realtime::NativeSequenceObservationV1 observation{};
    observation.descriptor.domain.market =
        realtime::NativeSequenceMarketV1::kShenzhen;
    observation.descriptor.domain.channel = 12U;
    observation.message_key = sdk::MessageKey{6U, 101U, 33U};
    observation.record_class =
        realtime::NativeSequenceRecoveryRecordClassV1::kTarget;
    for (std::uint64_t sequence = 1U;
         sequence <= kObservationCount;
         ++sequence) {
        observation.descriptor.sequence = sequence;
        observation.ingress_sequence = sequence;
        fixture.service->ObserveNativeSequence(observation);
    }

    const bool first_missing_applied_expired = WaitUntil([&] {
        return fixture.service->Snapshot().processed_handoffs != 0U;
    });
    const auto after_first_expiry = fixture.service->Snapshot();
    test->Expect(
        first_missing_applied_expired &&
            after_first_expiry.observed_native_messages ==
                kObservationCount &&
            after_first_expiry.processed_handoffs < kObservationCount &&
            after_first_expiry.handoff_queue_depth != 0U &&
            after_first_expiry.dropped_handoffs == 0U &&
            !after_first_expiry.globally_frozen,
        "target join grace starts at the FIFO head instead of expiring the queued tail in bulk");

    fixture.service->Stop();
    const auto stopped = fixture.service->Snapshot();
    test->Expect(
        stopped.processed_handoffs == stopped.enqueued_handoffs &&
            stopped.handoff_queue_depth == 0U &&
            stopped.dropped_handoffs == 0U && !stopped.globally_frozen,
        "forced shutdown drains a target-only join backlog without loss");
}

void RunSnapshotCoherenceScenario(TestContext* test) {
    Fixture fixture{};
    if (!BuildFixture(test, 1ms, 64U, 32U, 16U, &fixture)) {
        return;
    }
    std::atomic<bool> monitor_stop{false};
    std::atomic<bool> incoherent{false};
    std::thread monitor([&] {
        while (!monitor_stop.load(std::memory_order_acquire)) {
            if (!SnapshotStateErrorCoherent(fixture.service->Snapshot())) {
                incoherent.store(true, std::memory_order_release);
                return;
            }
        }
    });

    const bool reached_transient_states =
        InjectOrder(&fixture, 12U, 200U) &&
        WaitUntil([&] {
            return fixture.service->Snapshot().canonical_apply_frontier ==
                   1U;
        }) &&
        InjectOrder(&fixture, 12U, 199U) &&
        WaitUntil([&] {
            return fixture.service->Snapshot()
                       .correction_pending_channel_count == 1U;
        }) &&
        InjectTransaction(&fixture, 12U, 199U, 200U) &&
        WaitUntil([&] {
            return fixture.service->Snapshot().state ==
                   ipc::PartialOrderEventServiceStateV2::kFrozenConflict;
        });
    fixture.service->MarkCoverageLost();
    fixture.service->MarkNativeSequenceObservationFailure(
        realtime::NativeSequenceObservationFailureV1::kExtraction,
        sdk::MessageKey{6U, 101U, 33U});
    const auto terminal = fixture.service->Snapshot();
    monitor_stop.store(true, std::memory_order_release);
    monitor.join();

    ipc::PartialOrderEventStatusSnapshotV2 status{};
    test->Expect(
        reached_transient_states,
        "exercise contiguous, correction, and conflict states");
    test->Expect(
        !incoherent.load(std::memory_order_acquire),
        "Snapshot never exposes a torn state/error pair");
    test->Expect(
        SnapshotStateErrorCoherent(terminal) &&
            terminal.last_error ==
                ipc::PartialOrderEventLastErrorV2::kPermanentGap,
        "Snapshot retains a coherent first terminal diagnosis");
    test->Expect(
        fixture.failure_notifications.load(std::memory_order_acquire) == 1U,
        "terminal failure notifier fires exactly once");
    test->Expect(
        WaitUntil([&] {
            return fixture.reader->ReadStatus(&status) ==
                       ipc::PartialOrderEventReadResultV2::kOk &&
                   status.cut.state ==
                       ipc::PartialOrderEventServiceStateV2::
                           kFrozenResource &&
                   status.cut.last_error ==
                       ipc::PartialOrderEventLastErrorV2::kPermanentGap;
        }),
        "journal retains the same first terminal diagnosis");
}

void RunJournalPublicationIdleBarrierScenario(TestContext* test) {
    Fixture fixture{};
    if (!BuildFixture(test, 1min, 64U, 16U, 8U, &fixture)) {
        return;
    }
    fixture.service->SetWorkerPausedForTest(true);
    if (!WaitUntil([&] {
            return fixture.service->WorkerPauseReachedForTest();
        })) {
        test->Expect(false, "pause worker before journal idle-barrier test");
        fixture.service->SetWorkerPausedForTest(false);
        return;
    }
    fixture.service->SetJournalPublicationPausedForTest(true);
    realtime::NativeSequenceObservationV1 observation{};
    observation.descriptor.domain.market =
        realtime::NativeSequenceMarketV1::kShenzhen;
    observation.descriptor.domain.channel = 12U;
    observation.descriptor.sequence = 1U;
    observation.message_key = sdk::MessageKey{6U, 101U, 33U};
    observation.record_class =
        realtime::NativeSequenceRecoveryRecordClassV1::kFiltered;
    fixture.service->ObserveNativeSequence(observation);
    fixture.service->SetWorkerPausedForTest(false);
    const bool publication_paused = WaitUntil([&] {
        return fixture.service->JournalPublicationPauseReachedForTest();
    });
    const auto while_paused = fixture.service->Snapshot();
    const bool idle_while_paused =
        fixture.service->WaitUntilIdleForTest(2ms);
    fixture.service->MarkCoverageLost();
    const auto failed_while_paused = fixture.service->Snapshot();
    fixture.service->SetJournalPublicationPausedForTest(false);

    ipc::PartialOrderEventStatusSnapshotV2 status{};
    test->Expect(
        publication_paused && while_paused.handoff_queue_depth == 0U &&
            !idle_while_paused && failed_while_paused.globally_frozen &&
            failed_while_paused.last_error ==
                ipc::PartialOrderEventLastErrorV2::kPermanentGap &&
            fixture.service->WaitUntilIdleForTest(5s) &&
            fixture.reader->ReadStatus(&status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            status.cut.commit_sequence != 0U &&
            status.cut.state ==
                ipc::PartialOrderEventServiceStateV2::kFrozenResource &&
            status.cut.last_error ==
                ipc::PartialOrderEventLastErrorV2::kPermanentGap,
        "dirty terminal edge survives an in-progress journal publication");
}

void RunProgressAccuracyScenario(TestContext* test) {
    Fixture fixture{};
    if (!BuildFixture(test, 20ms, 64U, 32U, 16U, &fixture)) {
        return;
    }
    test->Expect(
        InjectOrder(&fixture, 12U, 100U) &&
            WaitUntil([&] {
                const auto snapshot = fixture.service->Snapshot();
                return snapshot.canonical_apply_frontier == 1U &&
                       snapshot.unsealed_channel_count == 0U &&
                       snapshot.gap_channel_count == 0U;
            }),
        "establish a healthy channel before opening a later gap");

    const auto affected_start = std::chrono::steady_clock::now();
    realtime::NativeSequenceObservationV1 observation{};
    observation.descriptor.domain.market =
        realtime::NativeSequenceMarketV1::kShenzhen;
    observation.descriptor.domain.channel = 12U;
    observation.descriptor.sequence = 101U;
    observation.message_key = sdk::MessageKey{6U, 101U, 33U};
    observation.record_class =
        realtime::NativeSequenceRecoveryRecordClassV1::kTarget;
    fixture.service->ObserveNativeSequence(observation);
    observation.descriptor.sequence = 103U;
    fixture.service->ObserveNativeSequence(observation);

    ipc::PartialOrderEventChannelHealthV2 health{};
    const bool published = WaitUntil([&] {
        ipc::PartialOrderEventStatusSnapshotV2 status{};
        std::array<ipc::PartialOrderEventChannelHealthV2, 8U> channels{};
        std::size_t rows_read = 0U;
        if (fixture.reader->ReadAffectedChannels(
                channels, &rows_read, &status) !=
                ipc::PartialOrderEventReadResultV2::kOk) {
            return false;
        }
        for (std::size_t index = 0U; index < rows_read; ++index) {
            if (channels[index].market ==
                    L2FLOW_INSTRUMENT_DERIVED_EVENT_MARKET_SHENZHEN_V1 &&
                channels[index].channel == 12 &&
                channels[index].oldest_missing_native_sequence == 102) {
                health = channels[index];
                return true;
            }
        }
        return false;
    });
    const auto affected_elapsed =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - affected_start);
    const std::uint64_t affected_elapsed_ns =
        affected_elapsed.count() > 0
            ? static_cast<std::uint64_t>(affected_elapsed.count())
            : 0U;
    constexpr std::uint64_t kClockMarginNs = 5'000'000U;
    const std::uint64_t maximum_reported_age =
        affected_elapsed_ns <=
                std::numeric_limits<std::uint64_t>::max() -
                    kClockMarginNs
            ? affected_elapsed_ns + kClockMarginNs
            : std::numeric_limits<std::uint64_t>::max();
    test->Expect(
        published && health.contiguous_native_sequence == 100 &&
            health.expected_native_sequence == 101 &&
            health.highest_observed_native_sequence == 103 &&
            health.oldest_missing_native_sequence == 102 &&
            health.oldest_gap_age_ns <= maximum_reported_age,
        "progress separates certification wait at 101 from the observed gap at 102 and ages the new incident");
}

void RunBackingPreallocationScenario(TestContext* test) {
    Fixture fixture{};
    if (!BuildFixture(test, 1ms, 64U, 32U, 16U, &fixture)) {
        return;
    }
    const auto before = fixture.service->JournalResourceSnapshot();
    test->Expect(
        before.fully_preallocated &&
            before.event_prefaulted_bytes +
                    ipc::kPartialOrderEventHeaderBytesV2 ==
                before.committed_event_region_bytes &&
            before.order_state_prefaulted_bytes ==
                before.order_state_backed_bytes &&
            InjectOrder(&fixture, 9U, 100U) &&
            WaitUntil([&] {
                return fixture.service->Snapshot()
                           .canonical_apply_frontier == 1U;
            }),
        "Event journal is fully backed before worker admission");
    fixture.service->MarkDraining();
    fixture.pipeline->StopAndDrain();
    fixture.service->MarkStoppedClean();
    const auto after = fixture.service->JournalResourceSnapshot();
    test->Expect(
        after == before,
        "Event worker performs no backing allocation while processing");
}

void RunKnownGapNoTimeoutScenario(TestContext* test) {
    Fixture fixture{};
    if (!BuildFixture(test, 1ms, 64U, 32U, 16U, &fixture)) {
        return;
    }
    test->Expect(
        InjectOrder(&fixture, 12U, 100U) &&
            WaitUntil([&] {
                return fixture.service->Snapshot()
                           .canonical_apply_frontier == 1U;
            }) &&
            InjectOrder(&fixture, 12U, 102U) &&
            WaitUntil([&] {
                return fixture.service->Snapshot().gap_channel_count ==
                       1U;
            }),
        "open a known native gap after the bounded origin is sealed");
    std::this_thread::sleep_for(10ms);
    ipc::PartialOrderEventStatusSnapshotV2 gap_status{};
    std::array<ipc::PartialOrderEventChannelHealthV2, 8U> gap_channels{};
    std::size_t gap_rows = 0U;
    test->Expect(
        fixture.reader->ReadAffectedChannels(
            gap_channels, &gap_rows, &gap_status) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            gap_status.cut.canonical_apply_frontier == 1U &&
            gap_rows == 1U &&
            gap_channels[0U].oldest_missing_native_sequence == 101,
        "discovery horizon never skips a known gap after origin sealing");

    test->Expect(
        InjectOrder(&fixture, 12U, 101U) &&
            WaitUntil([&] {
                const auto snapshot = fixture.service->Snapshot();
                return fixture.fast->count() == 3U &&
                       snapshot.canonical_apply_frontier == 3U &&
                       snapshot.gap_channel_count == 0U;
            }),
        "backfill 101 releases 101 then the retained 102");
    ipc::PartialOrderEventStatusSnapshotV2 recovered{};
    test->Expect(
        fixture.reader->ReadStatus(&recovered) ==
                ipc::PartialOrderEventReadResultV2::kOk &&
            recovered.cut.canonical_apply_frontier == 3U &&
            recovered.cut.state ==
                ipc::PartialOrderEventServiceStateV2::kContiguous,
        "known gap recovery returns the channel to contiguous");
    std::vector<ipc::PartialOrderEventEnvelopeV2> events(
        static_cast<std::size_t>(
            recovered.cut.event_published_frontier));
    ipc::PartialOrderEventReadBatchResultV2 batch{};
    const auto read = fixture.reader->ReadEvents(1U, events, &batch);
    bool saw_101 = false;
    bool saw_102 = false;
    bool ordered = true;
    for (const auto& event : events) {
        if (event.canonical_apply_sequence == 2U) {
            saw_101 = true;
            ordered = ordered &&
                      event.event.native_event_sequence == 101;
        } else if (event.canonical_apply_sequence == 3U) {
            saw_102 = true;
            ordered = ordered &&
                      event.event.native_event_sequence == 102;
        }
    }
    test->Expect(
        read == ipc::PartialOrderEventReadResultV2::kOk &&
            batch.rows_read == events.size() && saw_101 && saw_102 &&
            ordered,
        "Event History publishes the repaired 101,102 suffix in native order");
}

void RunSignedSequenceBoundaryScenario(TestContext* test) {
    Fixture fixture{};
    if (!BuildFixture(test, 1ms, 64U, 16U, 8U, &fixture)) {
        return;
    }
    constexpr std::uint64_t maximum_native =
        static_cast<std::uint64_t>(
            std::numeric_limits<std::int64_t>::max());
    realtime::NativeSequenceObservationV1 observation{};
    observation.descriptor.domain.market =
        realtime::NativeSequenceMarketV1::kShenzhen;
    observation.descriptor.domain.channel = 31U;
    observation.descriptor.sequence = maximum_native - 1U;
    observation.message_key = sdk::MessageKey{6U, 101U, 33U};
    observation.record_class =
        realtime::NativeSequenceRecoveryRecordClassV1::kFiltered;
    fixture.service->ObserveNativeSequence(observation);
    test->Expect(
        WaitUntil([&] {
            const auto snapshot = fixture.service->Snapshot();
            return snapshot.channel_count == 1U &&
                   snapshot.unsealed_channel_count == 0U &&
                   snapshot.pending_entries == 0U &&
                   !snapshot.globally_frozen;
        }),
        "INT64_MAX-1 is the largest certifiable native sequence");

    observation.descriptor.sequence = maximum_native - 2U;
    fixture.service->ObserveNativeSequence(observation);
    test->Expect(
        WaitUntil([&] {
            return fixture.service->Snapshot()
                       .correction_pending_channel_count == 1U;
        }),
        "late input below the sealed maximum boundary exposes channel health");
    ipc::PartialOrderEventStatusSnapshotV2 status{};
    std::array<ipc::PartialOrderEventChannelHealthV2, 8U> channels{};
    std::size_t rows = 0U;
    test->Expect(
        WaitUntil([&] {
            return fixture.reader->ReadAffectedChannels(
                       channels, &rows, &status) ==
                       ipc::PartialOrderEventReadResultV2::kOk &&
                   rows == 1U &&
                   channels[0U].contiguous_native_sequence ==
                       std::numeric_limits<std::int64_t>::max() - 1 &&
                   channels[0U].expected_native_sequence ==
                       std::numeric_limits<std::int64_t>::max();
        }),
        "native sequence progress reports a representable INT64_MAX next value");

    observation.descriptor.sequence = maximum_native;
    fixture.service->ObserveNativeSequence(observation);
    test->Expect(
        WaitUntil([&] {
            const auto snapshot = fixture.service->Snapshot();
            return snapshot.globally_frozen &&
                   snapshot.last_error ==
                       ipc::PartialOrderEventLastErrorV2::
                           kProjectionFailure &&
                   fixture.failure_notifications.load(
                       std::memory_order_acquire) == 1U;
        }),
        "INT64_MAX input fails closed because no signed next value exists");
    ipc::PartialOrderEventStatusSnapshotV2 failed_status{};
    test->Expect(
        WaitUntil([&] {
            return fixture.reader->ReadStatus(&failed_status) ==
                       ipc::PartialOrderEventReadResultV2::kOk &&
                   failed_status.cut.state ==
                       ipc::PartialOrderEventServiceStateV2::
                           kFrozenResource &&
                   failed_status.cut.last_error ==
                       ipc::PartialOrderEventLastErrorV2::
                           kProjectionFailure;
        }),
        "terminal failure replaces transient correction diagnostics in the journal cut");
}

void RunBusyQueueSealBarrierScenario(TestContext* test) {
    Fixture fixture{};
    constexpr std::uint64_t kQueueCapacity = 4096U;
    if (!BuildFixture(
            test, 1ms, kQueueCapacity, 16U, 8U, &fixture)) {
        return;
    }
    fixture.service->SetWorkerPausedForTest(true);
    if (!WaitUntil([&] {
            return fixture.service->WorkerPauseReachedForTest();
        })) {
        test->Expect(false, "pause worker for sustained backlog setup");
        return;
    }
    test->Expect(
        InjectOrder(&fixture, 7U, 1U),
        "queue one target before sustained filtered traffic");
    realtime::NativeSequenceObservationV1 observation{};
    observation.message_key = sdk::MessageKey{6U, 101U, 33U};
    observation.record_class =
        realtime::NativeSequenceRecoveryRecordClassV1::kFiltered;
    std::array<std::uint64_t, 2U> next_sequence{1U, 1U};
    constexpr std::array<std::uint32_t, 2U> channels{0U, 13U};
    for (std::size_t index = 0U; index < 3500U; ++index) {
        const std::size_t lane = index % channels.size();
        observation.descriptor.domain.market =
            realtime::NativeSequenceMarketV1::kShenzhen;
        observation.descriptor.domain.channel = channels[lane];
        observation.descriptor.sequence = next_sequence[lane]++;
        fixture.service->ObserveNativeSequence(observation);
    }

    std::atomic<bool> producer_stop{false};
    fixture.service->SetWorkerPausedForTest(false);
    std::thread producer([&] {
        std::size_t lane = 0U;
        while (!producer_stop.load(std::memory_order_acquire)) {
            const auto snapshot = fixture.service->Snapshot();
            if (snapshot.handoff_queue_depth >= 3500U) {
                std::this_thread::yield();
                continue;
            }
            observation.descriptor.domain.market =
                realtime::NativeSequenceMarketV1::kShenzhen;
            observation.descriptor.domain.channel = channels[lane];
            observation.descriptor.sequence = next_sequence[lane]++;
            fixture.service->ObserveNativeSequence(observation);
            lane = (lane + 1U) % channels.size();
        }
    });
    const bool sealed_while_busy = WaitUntil([&] {
        const auto snapshot = fixture.service->Snapshot();
        return snapshot.channel_count == 3U &&
               snapshot.unsealed_channel_count == 0U &&
               snapshot.handoff_queue_depth > 100U &&
               snapshot.canonical_apply_frontier >= 1U &&
               snapshot.published_event_frontier >= 1U;
    });
    producer_stop.store(true, std::memory_order_release);
    producer.join();
    test->Expect(
        sealed_while_busy &&
            !fixture.service->Snapshot().globally_frozen,
        "queue-position seal and aged target publication advance while filtered traffic remains queued");
    test->Expect(
        WaitUntil([&] {
            return fixture.service->WaitUntilIdleForTest(1ms) &&
                   fixture.service->Snapshot().pending_entries == 0U;
        }),
        "busy filtered continuity backlog drains without a known-gap skip");
}

}  // namespace

int main() {
    TestContext test;
    RunCrossFamilyReorderScenario(&test);
    RunCompactStateJournalScenario(&test);
    RunShanghaiStartupReorderScenario(&test);
    RunShenzhenChannelZeroScenario(&test);
    RunCorrectionIsolationScenario(&test);
    RunEventFailureIsolationScenario(&test);
    RunBrokeredFailureIntegrationScenario(&test);
    RunCoverageLossDiagnosticScenario(&test);
    RunQueueFailureIsolationScenario(&test);
    RunTargetJoinHeadGraceScenario(&test);
    RunSnapshotCoherenceScenario(&test);
    RunJournalPublicationIdleBarrierScenario(&test);
    RunProgressAccuracyScenario(&test);
    RunBackingPreallocationScenario(&test);
    RunKnownGapNoTimeoutScenario(&test);
    RunSignedSequenceBoundaryScenario(&test);
    RunBusyQueueSealBarrierScenario(&test);
    if (test.failures() != 0) {
        std::cerr << test.failures()
                  << " partial Event service checks failed\n";
        return 1;
    }
    std::cout << "realtime partial Event service checks passed\n";
    return 0;
}
