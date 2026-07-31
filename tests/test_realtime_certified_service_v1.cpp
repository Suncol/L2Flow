#include "l2flow/ipc/certified_order_event_reader_v1.h"
#include "l2flow/ipc/realtime_certified_reader_v1.h"
#include "l2flow/ipc/realtime_certified_service_v1.h"
#include "l2flow/ipc/realtime_wire_projection_v2.h"
#include "l2flow/market/daily_instrument_catalog_v2.h"
#include "l2flow/market/instrument_runtime_state_v2.h"
#include "l2flow/runtime/realtime_pipeline_v1.h"
#include "l2flow/sdk/market_message_catalog_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace ipc = l2flow::ipc;
namespace market = l2flow::market;
namespace mdl = datayes::mdl;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;

namespace {

using namespace std::chrono_literals;

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
        const auto encoded = std::as_bytes(
            std::span(value.data(), value.size()));
        bytes_.insert(bytes_.end(), encoded.begin(), encoded.end());
    }
    [[nodiscard]] std::vector<std::byte> Take() && {
        return std::move(bytes_);
    }

private:
    template <typename Unsigned>
    void StoreUnsigned(std::size_t offset, Unsigned value) {
        static_assert(std::is_unsigned_v<Unsigned>);
        for (std::size_t index = 0U; index < sizeof(Unsigned);
             ++index) {
            bytes_.at(offset + index) = static_cast<std::byte>(
                (value >>
                 static_cast<unsigned int>(index * 8U)) &
                static_cast<Unsigned>(0xffU));
        }
    }

    std::vector<std::byte> bytes_;
};

[[nodiscard]] std::vector<std::byte> ShanghaiTradeBody(
    std::uint64_t biz_index,
    std::uint64_t quantity,
    std::string_view security_id = "600007",
    std::string_view raw_type = "T") {
    WireWriter writer(70U);
    writer.StoreU64(0U, biz_index);
    writer.StoreU32(8U, 7U);
    writer.StoreU32(18U, 93'000'125U);
    writer.StoreU64(28U, 11'001U);
    writer.StoreU64(36U, 22'002U);
    writer.StoreU32(44U, 12'345U);
    writer.StoreU64(48U, quantity);
    writer.StoreU64(56U, 506'145U);
    writer.StoreString(12U, security_id);
    writer.StoreString(22U, raw_type);
    writer.StoreString(64U, "B");
    return std::move(writer).Take();
}

[[nodiscard]] std::vector<std::byte> ShanghaiSnapshotBody(
    std::string_view security_id = "600007") {
    WireWriter writer(248U);
    writer.StoreU32(0U, 93'000'123U);
    writer.StoreU32(30U, 12'345U);
    writer.StoreString(4U, security_id);
    writer.StoreString(38U, "TRADE");
    return std::move(writer).Take();
}

class FakeMessage final : public mdl::MDLMessage {
public:
    explicit FakeMessage(
        std::vector<std::byte> body,
        sdk::MessageKey key = sdk::MessageKey{4U, 101U, 24U})
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
        std::size_t ordinal,
        const market::RealtimeHistoryRecordV1& record)
        noexcept override {
        if (!market::IsTickEventKindV1(record.kind())) {
            snapshot_count_.fetch_add(
                1U, std::memory_order_release);
            return true;
        }
        ipc::RealtimeWireTickPayloadV2 payload{};
        if (!ipc::ProjectRealtimeWireTickPayloadV2(
                record, ordinal, &payload)) {
            return false;
        }
        const std::size_t index =
            reserved_count_.fetch_add(
                1U, std::memory_order_relaxed);
        if (index >= native_sequences_.size()) {
            return false;
        }
        native_sequences_[index].store(
            payload.native_event_sequence,
            std::memory_order_relaxed);
        // Count is the test sink's public visibility frontier, not merely a
        // reservation cursor. Publishing it after the payload prevents a
        // waiter from observing count=N while slot N-1 is still zero.
        count_.fetch_add(1U, std::memory_order_acq_rel);
        return true;
    }

    void MarkCoverageLost() noexcept override {
        coverage_lost_.store(true, std::memory_order_release);
    }

    [[nodiscard]] std::size_t count() const noexcept {
        return count_.load(std::memory_order_acquire);
    }
    [[nodiscard]] std::int64_t native(std::size_t index) const noexcept {
        return native_sequences_[index].load(
            std::memory_order_acquire);
    }
    [[nodiscard]] bool coverage_lost() const noexcept {
        return coverage_lost_.load(std::memory_order_acquire);
    }
    [[nodiscard]] std::size_t snapshot_count() const noexcept {
        return snapshot_count_.load(std::memory_order_acquire);
    }

private:
    std::array<std::atomic<std::int64_t>, 16U>
        native_sequences_{};
    std::atomic<std::size_t> reserved_count_{0U};
    std::atomic<std::size_t> count_{0U};
    std::atomic<std::size_t> snapshot_count_{0U};
    std::atomic<bool> coverage_lost_{false};
};

struct Fixture final {
    std::shared_ptr<const market::DailyInstrumentCatalogV2> catalog;
    std::unique_ptr<market::InstrumentRuntimeStateV2> runtime_state;
};

[[nodiscard]] bool BuildFixture(Fixture* output) {
    if (output == nullptr) {
        return false;
    }
    market::DailyInstrumentSourceEntryV2 source{};
    source.key.market = market::MarketV1::kShanghai;
    const std::string_view security_id = "600007";
    const auto bytes = std::as_bytes(
        std::span(security_id.data(), security_id.size()));
    source.key.security_id.assign(bytes.begin(), bytes.end());
    source.metadata.quantity_unit =
        market::QuantityUnitV1::kShare;
    source.metadata.security_type =
        market::SecurityTypeV1::kEquity;
    source.metadata.asset_scope =
        market::AssetScopeV1::kDocumentedCore;
    market::DailyInstrumentCatalogConfigV2 config{};
    config.trade_date = 20260730U;
    config.catalog_version = 1U;
    config.session_epoch = 1U;
    config.market_scope = market::kDailyCatalogMainlandScopeV2;
    config.coverage_complete = true;
    std::unique_ptr<market::DailyInstrumentCatalogV2> catalog;
    if (market::DailyInstrumentCatalogV2::Create(
            config, std::span(&source, 1U), &catalog) !=
            market::DailyInstrumentCatalogCreateErrorV2::kNone ||
        catalog == nullptr) {
        return false;
    }
    output->catalog =
        std::shared_ptr<const market::DailyInstrumentCatalogV2>(
            std::move(catalog));
    return market::InstrumentRuntimeStateV2::Create(
               *output->catalog, &output->runtime_state) ==
               market::InstrumentRuntimeStateErrorV2::kNone;
}

template <typename Predicate>
[[nodiscard]] bool WaitUntil(Predicate predicate) {
    const auto deadline =
        std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::yield();
    }
    return predicate();
}

[[nodiscard]] bool WorkerOnlyControlHasNoResponse(
    const std::filesystem::path& socket_path) {
    const std::string native = socket_path.string();
    if (native.empty() ||
        native.size() >= sizeof(sockaddr_un::sun_path)) {
        return false;
    }
    const int client =
        ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (client < 0) {
        return false;
    }
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::memcpy(
        address.sun_path, native.c_str(), native.size() + 1U);
    const socklen_t address_bytes = static_cast<socklen_t>(
        offsetof(sockaddr_un, sun_path) + native.size() + 1U);
    ipc::RealtimeCertifiedControlRequestV1 request{};
    request.magic = ipc::kRealtimeCertifiedControlRequestMagicV1;
    request.abi_major = ipc::kRealtimeCertifiedWireMajorV1;
    request.abi_minor = ipc::kRealtimeCertifiedWireMinorV1;
    request.request_bytes = sizeof(request);
    request.opcode = static_cast<std::uint16_t>(
        ipc::RealtimeCertifiedControlOpcodeV1::kGetSession);
    request.nonce = 0x1234U;
    const bool sent =
        ::connect(
            client,
            reinterpret_cast<const sockaddr*>(&address),
            address_bytes) == 0 &&
        ::send(
            client, &request, sizeof(request), MSG_NOSIGNAL) ==
            static_cast<ssize_t>(sizeof(request));
    pollfd descriptor{};
    descriptor.fd = client;
    descriptor.events = POLLIN;
    const int poll_result = sent ? ::poll(&descriptor, 1U, 50) : -1;
    static_cast<void>(::close(client));
    return sent && poll_result == 0;
}

[[nodiscard]] runtime::RealtimePipelineIngressResultV1 Inject(
    runtime::RealtimePipelineV1* pipeline,
    std::uint64_t sequence,
    std::uint64_t quantity,
    std::string_view security_id = "600007") {
    FakeMessage message(
        ShanghaiTradeBody(sequence, quantity, security_id));
    return pipeline->InjectSdkMessageForTest(&message);
}

[[nodiscard]] runtime::RealtimePipelineIngressResultV1 InjectWithType(
    runtime::RealtimePipelineV1* pipeline,
    std::uint64_t sequence,
    std::uint64_t quantity,
    std::string_view raw_type) {
    FakeMessage message(
        ShanghaiTradeBody(
            sequence, quantity, "600007", raw_type));
    return pipeline->InjectSdkMessageForTest(&message);
}

[[nodiscard]] runtime::RealtimePipelineIngressResultV1
InjectSnapshot(runtime::RealtimePipelineV1* pipeline) {
    FakeMessage message(
        ShanghaiSnapshotBody(),
        sdk::MessageKey{4U, 101U, 4U});
    return pipeline->InjectSdkMessageForTest(&message);
}

struct WorkerBarrierFixture final {
    Fixture instrument{};
    std::shared_ptr<FastSink> fast;
    ipc::RealtimeCertifiedServiceConfigV1 service_config{};
    std::shared_ptr<ipc::RealtimeCertifiedMarketServiceV1> service;
    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;

    ~WorkerBarrierFixture() {
        if (pipeline != nullptr) {
            pipeline->StopAndDrain();
        }
        if (service != nullptr) {
            service->StopControl();
        }
    }
};

[[nodiscard]] bool BuildWorkerBarrierFixture(
    TestContext* test,
    std::string_view socket_tag,
    std::byte run_marker,
    WorkerBarrierFixture* output) {
    if (test == nullptr || output == nullptr) {
        return false;
    }
    const std::string label(socket_tag);
    const bool instrument_ready =
        BuildFixture(&output->instrument) &&
        output->instrument.runtime_state != nullptr;
    test->Expect(
        instrument_ready,
        "build " + label + " worker-barrier fixture");
    if (!instrument_ready) {
        return false;
    }

    output->fast = std::make_shared<FastSink>();
    auto& service_config = output->service_config;
    service_config.run_id[0] = run_marker;
    service_config.run_id[15] = std::byte{0x7e};
    service_config.session_epoch = 1U;
    service_config.trade_date = 20260730U;
    service_config.daily_catalog = output->instrument.catalog;
    service_config.fast_sink = output->fast;
    service_config.certified_tick_ring_capacity = 64U;
    service_config.channel_capacity = 8U;
    service_config.handoff_queue_capacity = 128U;
    service_config.maximum_pending_entries = 64U;
    service_config.maximum_pending_entries_per_channel = 64U;
    service_config.certified_duplicate_retention_entries = 64U;
    service_config.maximum_reorder_span = 1024U;
    service_config.maximum_mapping_bytes = 8U * 1024U * 1024U;
    service_config.maximum_order_states = 128U;
    service_config.maximum_derived_events = 1024U;
    service_config.control_socket_path =
        std::filesystem::path("/tmp") /
        ("l2flow-certified-barrier-" + label + "-" +
         std::to_string(static_cast<long long>(::getpid())) +
         ".sock");

    int system_error = 0;
    const auto service_error =
        ipc::RealtimeCertifiedMarketServiceV1::Create(
            service_config, &output->service, &system_error);
    const bool service_ready =
        service_error ==
            ipc::RealtimeCertifiedServiceCreateErrorV1::kNone &&
        output->service != nullptr;
    test->Expect(
        service_ready,
        "create " + label + " worker-barrier service");
    if (!service_ready) {
        return false;
    }
    const bool worker_started =
        output->service->StartWorker(&system_error);
    test->Expect(
        worker_started,
        "start " + label + " worker-barrier worker");
    if (!worker_started) {
        return false;
    }

    runtime::RealtimePipelineConfigV1 pipeline_config{};
    pipeline_config.run_id = service_config.run_id;
    pipeline_config.trade_date = service_config.trade_date;
    pipeline_config.daily_catalog = output->instrument.catalog;
    pipeline_config.runtime_state =
        output->instrument.runtime_state.get();
    pipeline_config.source_stream_ids =
        {1001U, 1002U, 2001U, 2002U};
    pipeline_config.maximum_sdk_message_bytes = 4096U;
    pipeline_config.decoder_queue_capacity_per_source = 32U;
    pipeline_config.completion_tracker_capacity = 256U;
    pipeline_config.tick_ring_capacity = 256U;
    pipeline_config.store_worker_count = 1U;
    pipeline_config.store_queue_capacity_per_source_worker = 32U;
    pipeline_config.intraday_store.segment_target_bytes = 4096U;
    pipeline_config.intraday_store.maximum_session_records = 64U;
    pipeline_config.intraday_store.maximum_session_accounted_bytes =
        8U * 1024U * 1024U;
    pipeline_config.intraday_store.maximum_records_per_batch = 16U;
    pipeline_config.intraday_store.coverage_from_open = true;
    pipeline_config.applied_record_sink = output->service;
    pipeline_config.native_sequence_observation_sink =
        output->service;
    pipeline_config.sdk.enabled = false;

    std::string detail;
    const auto pipeline_error = runtime::RealtimePipelineV1::Create(
        pipeline_config, &output->pipeline, &detail);
    const bool pipeline_ready =
        pipeline_error ==
            runtime::RealtimePipelineCreateErrorV1::kNone &&
        output->pipeline != nullptr;
    test->Expect(
        pipeline_ready,
        "create " + label + " worker-barrier pipeline: " + detail);
    return pipeline_ready;
}

void RunGapBeforeBarrierRejectsActivationScenario(
    TestContext* test) {
    WorkerBarrierFixture fixture;
    if (!BuildWorkerBarrierFixture(
            test, "gap-reject", std::byte{0x51}, &fixture)) {
        return;
    }
    test->Expect(
        WorkerOnlyControlHasNoResponse(
            fixture.service_config.control_socket_path),
        "GAP_OPEN worker-only phase exposes no control response");
    test->Expect(
        Inject(fixture.pipeline.get(), 3U, 41U).accepted(),
        "inject leading gap before activation barrier");
    test->Expect(
        WaitUntil([&] {
            const auto snapshot = fixture.service->Snapshot();
            return snapshot.wire_snapshot_consistent &&
                   snapshot.state ==
                       ipc::RealtimeCertifiedStateV1::kGapOpen &&
                   snapshot.canonical_apply_frontier == 0U;
        }) &&
            fixture.service->WaitUntilIdleForTest(5s),
        "GAP_OPEN is fully committed before activation barrier");

    int system_error = 0;
    test->Expect(
        !fixture.service->ActivateControlAfterPrefix(
            5s, &system_error) &&
            system_error == EIO,
        "pre-barrier GAP_OPEN rejects control activation with EIO");
    test->Expect(
        WorkerOnlyControlHasNoResponse(
            fixture.service_config.control_socket_path),
        "rejected GAP_OPEN activation leaves control unavailable");
}

void RunFrozenBeforeBarrierRejectsActivationScenario(
    TestContext* test) {
    WorkerBarrierFixture fixture;
    if (!BuildWorkerBarrierFixture(
            test, "frozen-reject", std::byte{0x52}, &fixture)) {
        return;
    }
    test->Expect(
        WorkerOnlyControlHasNoResponse(
            fixture.service_config.control_socket_path),
        "FROZEN worker-only phase exposes no control response");
    fixture.service->MarkNativeSequenceObservationFailure(
        l2flow::realtime::NativeSequenceObservationFailureV1::
            kHandoff,
        sdk::MessageKey{4U, 101U, 24U});
    test->Expect(
        WaitUntil([&] {
            const auto snapshot = fixture.service->Snapshot();
            return snapshot.wire_snapshot_consistent &&
                   snapshot.globally_frozen_resource &&
                   snapshot.state ==
                       ipc::RealtimeCertifiedStateV1::
                           kFrozenResource;
        }),
        "FROZEN_RESOURCE is committed before activation barrier");

    int system_error = 0;
    test->Expect(
        !fixture.service->ActivateControlAfterPrefix(
            5s, &system_error) &&
            system_error == EIO,
        "pre-barrier FROZEN_RESOURCE rejects activation with EIO");
    test->Expect(
        WorkerOnlyControlHasNoResponse(
            fixture.service_config.control_socket_path),
        "rejected FROZEN activation leaves control unavailable");
}

void RunPostBarrierGapCannotRewriteActivationScenario(
    TestContext* test) {
    WorkerBarrierFixture fixture;
    if (!BuildWorkerBarrierFixture(
            test, "post-gap", std::byte{0x53}, &fixture)) {
        return;
    }
    test->Expect(
        Inject(fixture.pipeline.get(), 1U, 41U).accepted(),
        "inject healthy prefix before exact activation barrier");
    test->Expect(
        WaitUntil([&] {
            const auto snapshot = fixture.service->Snapshot();
            return snapshot.wire_snapshot_consistent &&
                   snapshot.state ==
                       ipc::RealtimeCertifiedStateV1::kContiguous &&
                   snapshot.canonical_apply_frontier == 1U;
        }) &&
            fixture.service->WaitUntilIdleForTest(5s),
        "healthy prefix is fully committed before exact barrier");

    int system_error = 0;
    const bool activated =
        fixture.service->ActivateControlAfterPrefix(
            5s, &system_error);
    test->Expect(
        activated && system_error == 0,
        "healthy exact barrier activates control");

    // This handoff is deliberately submitted immediately after the exact
    // barrier result. It changes the live mutable state, but cannot rewrite
    // the already-captured activation decision for that FIFO prefix.
    test->Expect(
        Inject(fixture.pipeline.get(), 3U, 43U).accepted(),
        "inject state-changing live gap immediately after barrier");
    test->Expect(
        WaitUntil([&] {
            const auto snapshot = fixture.service->Snapshot();
            return snapshot.wire_snapshot_consistent &&
                   snapshot.state ==
                       ipc::RealtimeCertifiedStateV1::kGapOpen &&
                   snapshot.canonical_apply_frontier == 1U;
        }),
        "post-barrier live handoff advances mutable state to GAP_OPEN");

    ipc::RealtimeCertifiedReaderOpenOptionsV1 open{};
    open.control_socket_path =
        fixture.service_config.control_socket_path;
    std::memcpy(
        open.expected_session.run_id.data(),
        fixture.service_config.run_id.data(),
        fixture.service_config.run_id.size());
    open.expected_session.session_epoch =
        fixture.service_config.session_epoch;
    open.expected_session.trade_date =
        fixture.service_config.trade_date;
    std::unique_ptr<ipc::RealtimeCertifiedReaderV1> reader;
    const auto open_error = ipc::RealtimeCertifiedReaderV1::Open(
        open, &reader, &system_error);
    test->Expect(
        open_error ==
            ipc::RealtimeCertifiedReaderOpenErrorV1::kNone &&
            reader != nullptr,
        "post-barrier GAP cannot revoke exact control activation");
    ipc::RealtimeCertifiedStatusSnapshotV1 reader_status{};
    test->Expect(
        reader != nullptr &&
            reader->ReadStatus(&reader_status) ==
                ipc::RealtimeCertifiedReadResultV1::kOk &&
            reader_status.state ==
                ipc::RealtimeCertifiedStateV1::kGapOpen &&
            reader_status.canonical_apply_frontier == 1U,
        "activated control reports later GAP without polluting barrier result");
}

void RunRecoveryScenario(TestContext* test) {
    Fixture fixture{};
    test->Expect(BuildFixture(&fixture), "build catalog fixture");
    if (fixture.runtime_state == nullptr) {
        return;
    }
    const auto fast = std::make_shared<FastSink>();
    ipc::RealtimeCertifiedServiceConfigV1 service_config{};
    service_config.run_id[0] = std::byte{0x42};
    service_config.run_id[15] = std::byte{0x24};
    service_config.session_epoch = 1U;
    service_config.trade_date = 20260730U;
    service_config.daily_catalog = fixture.catalog;
    service_config.fast_sink = fast;
    service_config.certified_tick_ring_capacity = 64U;
    service_config.channel_capacity = 8U;
    service_config.handoff_queue_capacity = 128U;
    service_config.maximum_pending_entries = 64U;
    service_config.maximum_pending_entries_per_channel = 64U;
    service_config.certified_duplicate_retention_entries = 64U;
    service_config.maximum_reorder_span = 1024U;
    service_config.maximum_mapping_bytes = 8U * 1024U * 1024U;
    service_config.maximum_order_states = 128U;
    service_config.maximum_derived_events = 1024U;
    service_config.control_socket_path =
        std::filesystem::path("/tmp") /
        ("l2flow-certified-service-" +
         std::to_string(static_cast<long long>(::getpid())) +
         ".sock");

    std::shared_ptr<ipc::RealtimeCertifiedMarketServiceV1> service;
    int system_error = 0;
    test->Expect(
        ipc::RealtimeCertifiedMarketServiceV1::Create(
            service_config, &service, &system_error) ==
                ipc::RealtimeCertifiedServiceCreateErrorV1::kNone &&
            service != nullptr,
        "create certified service");
    if (service == nullptr) {
        return;
    }
    test->Expect(
        service->StartWorker(&system_error),
        "start recovery certified worker");
    test->Expect(
        WorkerOnlyControlHasNoResponse(
            service_config.control_socket_path),
        "worker-only recovery phase exposes no control response");

    runtime::RealtimePipelineConfigV1 pipeline_config{};
    pipeline_config.run_id = service_config.run_id;
    pipeline_config.trade_date = service_config.trade_date;
    pipeline_config.daily_catalog = fixture.catalog;
    pipeline_config.runtime_state = fixture.runtime_state.get();
    pipeline_config.source_stream_ids = {1001U, 1002U, 2001U, 2002U};
    pipeline_config.maximum_sdk_message_bytes = 4096U;
    pipeline_config.decoder_queue_capacity_per_source = 32U;
    pipeline_config.completion_tracker_capacity = 256U;
    pipeline_config.tick_ring_capacity = 256U;
    pipeline_config.store_worker_count = 1U;
    pipeline_config.store_queue_capacity_per_source_worker = 32U;
    pipeline_config.intraday_store.segment_target_bytes = 4096U;
    pipeline_config.intraday_store.maximum_session_records = 64U;
    pipeline_config.intraday_store.maximum_session_accounted_bytes =
        8U * 1024U * 1024U;
    pipeline_config.intraday_store.maximum_records_per_batch = 16U;
    pipeline_config.intraday_store.coverage_from_open = true;
    pipeline_config.applied_record_sink = service;
    pipeline_config.native_sequence_observation_sink = service;
    pipeline_config.sdk.enabled = false;

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            pipeline_config, &pipeline, &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create pipeline: " + detail);
    if (pipeline == nullptr) {
        service->StopControl();
        return;
    }

    test->Expect(
        InjectSnapshot(pipeline.get()).accepted(),
        "inject normal snapshot before tracked ticks");
    test->Expect(
        WaitUntil([&] {
            const auto snapshot = service->Snapshot();
            return fast->snapshot_count() == 1U &&
                   snapshot.wire_snapshot_consistent &&
                   snapshot.state ==
                       ipc::RealtimeCertifiedStateV1::kNoData &&
                   snapshot.canonical_apply_frontier == 0U &&
                   !snapshot.globally_frozen_resource;
        }),
        "snapshot remains FAST-only and cannot freeze tick CERTIFIED");

    test->Expect(Inject(pipeline.get(), 3U, 41U).accepted(),
                 "inject native 3 as the first observed packet");
    test->Expect(
        WaitUntil([&] {
            const auto snapshot = service->Snapshot();
            return fast->count() == 1U &&
                   fast->native(0U) == 3 &&
                   snapshot.wire_snapshot_consistent &&
                   snapshot.state ==
                       ipc::RealtimeCertifiedStateV1::kGapOpen &&
                   snapshot.canonical_apply_frontier == 0U;
        }),
        "first native 3 stays FAST-visible but opens leading CERTIFIED gap");
    ipc::RealtimeCertifiedTickEnvelopeV1 latest{};
    test->Expect(
        !service->ReadLatestForTest(0U, &latest),
        "leading gap publishes no false CERTIFIED latest");

    test->Expect(Inject(pipeline.get(), 1U, 42U).accepted(),
                 "inject leading backfill native 1");
    test->Expect(
        WaitUntil([&] {
            const auto snapshot = service->Snapshot();
            return fast->count() == 2U &&
                   fast->native(1U) == 1 &&
                   snapshot.state ==
                       ipc::RealtimeCertifiedStateV1::kGapOpen &&
                   snapshot.canonical_apply_frontier == 1U;
        }),
        "partial leading backfill advances only dense CERTIFIED prefix");
    test->Expect(
        service->ReadLatestForTest(0U, &latest) &&
            latest.payload.native_event_sequence == 1,
        "gap keeps last-good CERTIFIED latest readable");

    test->Expect(Inject(pipeline.get(), 2U, 43U).accepted(),
                 "inject final leading backfill native 2");
    test->Expect(
        WaitUntil([&] {
            const auto snapshot = service->Snapshot();
            return fast->count() == 3U &&
                   snapshot.state ==
                       ipc::RealtimeCertifiedStateV1::kContiguous &&
                   snapshot.canonical_apply_frontier == 3U &&
                   snapshot.gap_opened_count == 1U &&
                   snapshot.gap_recovered_count == 1U;
        }),
        "backfill catches CERTIFIED up without blocking FAST");
    const std::array<std::int64_t, 3U> expected{1, 2, 3};
    for (std::uint64_t sequence = 1U; sequence <= 3U; ++sequence) {
        ipc::RealtimeCertifiedTickEnvelopeV1 envelope{};
        test->Expect(
            service->ReadCanonicalForTest(sequence, &envelope) &&
                envelope.payload.native_event_sequence ==
                    expected[static_cast<std::size_t>(
                        sequence - 1U)],
            "certified ring is native ordered after repair");
    }

    test->Expect(
        service->ActivateControlAfterPrefix(5s, &system_error),
        "activate certified control after committed prefix barrier");
    test->Expect(
        !service->StartControl(&system_error),
        "certified control activation is one-shot");

    const auto filtered =
        Inject(pipeline.get(), 4U, 44U, "900901");
    test->Expect(
        filtered.error ==
            runtime::RealtimePipelineIngressErrorV1::
                kFilteredNonAShare,
        "native 4 is an output-free filter marker");
    test->Expect(
        WaitUntil([&] {
            const auto snapshot = service->Snapshot();
            return snapshot.wire_snapshot_consistent &&
                   snapshot.state ==
                       ipc::RealtimeCertifiedStateV1::kContiguous &&
                   snapshot.canonical_apply_frontier == 3U &&
                   snapshot.pending_token_count == 0U;
        }),
        "filtered tail explicitly commits and leaves canonical status valid");
    test->Expect(Inject(pipeline.get(), 5U, 45U).accepted(),
                 "inject native 5 after filtered marker");
    test->Expect(
        WaitUntil([&] {
            return fast->count() == 4U &&
                   service->Snapshot().canonical_apply_frontier == 4U;
        }),
        "filtered native position advances continuity without a tick");

    test->Expect(Inject(pipeline.get(), 5U, 45U).accepted(),
                 "inject exact duplicate 5");
    test->Expect(
        WaitUntil([&] {
            const auto snapshot = service->Snapshot();
            return fast->count() == 5U &&
                   snapshot.canonical_apply_frontier == 4U &&
                   snapshot.exact_duplicate_message_count >= 1U;
        }),
        "exact duplicate is idempotent only in CERTIFIED");

    test->Expect(Inject(pipeline.get(), 5U, 99U).accepted(),
                 "inject conflicting duplicate 5");
    test->Expect(
        WaitUntil([&] {
            return fast->count() == 6U &&
                   service->Snapshot().state ==
                       ipc::RealtimeCertifiedStateV1::
                           kFrozenConflict;
        }),
        "conflict freezes CERTIFIED while FAST remains live");
    test->Expect(
        WaitUntil([&] {
            const auto frozen = service->Snapshot();
            ipc::RealtimeCertifiedTickEnvelopeV1 retained{};
            return frozen.wire_snapshot_consistent &&
                   frozen.canonical_apply_frontier == 4U &&
                   fast->native(5U) == 5 &&
                   !fast->coverage_lost() &&
                   !pipeline->fatal() &&
                   service->ReadLatestForTest(0U, &retained) &&
                   retained.payload.native_event_sequence == 5;
        }),
        "frozen CERTIFIED preserves last-good latest and FAST health");

    ipc::RealtimeCertifiedReaderOpenOptionsV1 open{};
    open.control_socket_path = service_config.control_socket_path;
    std::memcpy(
        open.expected_session.run_id.data(),
        service_config.run_id.data(),
        service_config.run_id.size());
    open.expected_session.session_epoch = 1U;
    open.expected_session.trade_date = 20260730U;
    std::unique_ptr<ipc::RealtimeCertifiedReaderV1> reader;
    test->Expect(
        ipc::RealtimeCertifiedReaderV1::Open(
            open, &reader, &system_error) ==
                ipc::RealtimeCertifiedReaderOpenErrorV1::kNone &&
            reader != nullptr,
        "open production UDS certified reader");
    ipc::RealtimeCertifiedStatusSnapshotV1 reader_status{};
    test->Expect(
        reader != nullptr &&
            reader->ReadStatus(&reader_status) ==
                ipc::RealtimeCertifiedReadResultV1::kOk &&
            reader_status.state ==
                ipc::RealtimeCertifiedStateV1::kFrozenConflict &&
            reader->ReadLatest(0U, &latest) ==
                ipc::RealtimeCertifiedReadResultV1::kOk,
        "reader keeps last-good data available while frozen");
    ipc::RealtimeCertifiedChannelStateV1 channel{};
    test->Expect(
        reader != nullptr &&
            reader->ReadChannelState(0U, &channel) ==
                ipc::RealtimeCertifiedReadResultV1::kOk &&
            channel.origin_sequence == 1 &&
            channel.observed_contiguous_frontier == 5 &&
            channel.certified_published_frontier == 5,
        "wire channel preserves documented from-open origin and coverage");

    std::unique_ptr<ipc::CertifiedOrderEventReaderV1> event_reader;
    test->Expect(
        ipc::CertifiedOrderEventReaderV1::Open(
            open, &event_reader, &system_error) ==
                ipc::CertifiedOrderEventReaderOpenErrorV1::kNone &&
            event_reader != nullptr,
        "open production UDS Tick/Event reader");
    ipc::CertifiedOrderEventStatusSnapshotV1 event_status{};
    test->Expect(
        event_reader != nullptr &&
            event_reader->ReadStatus(&event_status) ==
                ipc::CertifiedOrderEventReadResultV1::kOk &&
            event_status.tick.state ==
                ipc::RealtimeCertifiedStateV1::kFrozenConflict &&
            event_status.tick.canonical_apply_frontier == 4U &&
            event_status.event_canonical_apply_frontier == 4U &&
            event_status.coherent_canonical_apply_frontier == 4U &&
            event_status.event_published_sequence > 0U,
        "external Event journal preserves repaired coherent prefix");
    std::array<ipc::CertifiedOrderEventEnvelopeV1, 32U>
        event_rows{};
    ipc::CertifiedOrderEventReadBatchResultV1 event_batch{};
    test->Expect(
        event_reader != nullptr &&
            event_reader->Read(1U, event_rows, &event_batch) ==
                ipc::CertifiedOrderEventReadResultV1::kOk &&
            event_batch.written ==
                event_status.event_published_sequence &&
            event_batch.next_event_sequence ==
                event_status.event_published_sequence + 1U,
        "external Event cursor reads complete retained prefix");
    bool event_order_valid =
        event_batch.written != 0U &&
        event_batch.written <= event_rows.size();
    std::uint64_t previous_canonical = 0U;
    for (std::size_t index = 0U;
         event_order_valid && index < event_batch.written;
         ++index) {
        const auto& row = event_rows[index];
        const std::uint64_t canonical =
            row.canonical_apply_sequence;
        const std::int64_t expected_native =
            canonical == 4U
                ? 5
                : static_cast<std::int64_t>(canonical);
        event_order_valid =
            row.event.derived_event_sequence == index + 1U &&
            canonical >= 1U && canonical <= 4U &&
            canonical >= previous_canonical &&
            row.event.native_event_sequence == expected_native;
        previous_canonical = canonical;
    }
    test->Expect(
        event_order_valid,
        "external Event rows retain dense sequence and repaired native order");

    ipc::CertifiedOrderEventHistorySnapshotV1 events{};
    test->Expect(
        service->AcquireEventGeneration(&events) ==
                ipc::CertifiedOrderEventHistoryErrorV1::kNone &&
            events.valid() &&
            events.generation().input_frontier
                    .canonical_apply_sequence == 4U &&
            !events.events().empty(),
        "CERTIFIED Event/history advances through repaired prefix only");

    pipeline->StopAndDrain();
    service->MarkStoppedClean();
    test->Expect(
        service->Snapshot().state ==
            ipc::RealtimeCertifiedStateV1::kFrozenConflict,
        "clean-stop request does not erase terminal conflict state");
    service->StopControl();
}

void RunEventCapacityFailOpenScenario(TestContext* test) {
    Fixture fixture{};
    test->Expect(
        BuildFixture(&fixture),
        "build Event-capacity fixture");
    if (fixture.runtime_state == nullptr) {
        return;
    }
    const auto fast = std::make_shared<FastSink>();
    ipc::RealtimeCertifiedServiceConfigV1 service_config{};
    service_config.run_id[0] = std::byte{0x43};
    service_config.run_id[15] = std::byte{0x25};
    service_config.session_epoch = 1U;
    service_config.trade_date = 20260730U;
    service_config.daily_catalog = fixture.catalog;
    service_config.fast_sink = fast;
    service_config.certified_tick_ring_capacity = 8U;
    service_config.channel_capacity = 2U;
    service_config.handoff_queue_capacity = 16U;
    service_config.maximum_pending_entries = 8U;
    service_config.maximum_pending_entries_per_channel = 8U;
    service_config.certified_duplicate_retention_entries = 8U;
    service_config.maximum_reorder_span = 16U;
    service_config.maximum_mapping_bytes = 2U * 1024U * 1024U;
    service_config.maximum_order_states = 8U;
    // One Shanghai add produces exactly one order-revision Event. The next
    // add therefore exercises a deterministic Event-capacity failure after a
    // valid last-good Tick/Event transaction exists.
    service_config.maximum_derived_events = 1U;
    service_config.control_socket_path =
        std::filesystem::path("/tmp") /
        ("l2flow-certified-capacity-" +
         std::to_string(static_cast<long long>(::getpid())) +
         ".sock");

    std::shared_ptr<ipc::RealtimeCertifiedMarketServiceV1> service;
    int system_error = 0;
    test->Expect(
        ipc::RealtimeCertifiedMarketServiceV1::Create(
            service_config, &service, &system_error) ==
                ipc::RealtimeCertifiedServiceCreateErrorV1::kNone &&
            service != nullptr,
        "create Event-capacity certified service");
    if (service == nullptr) {
        return;
    }
    test->Expect(
        service->Start(&system_error),
        "start Event-capacity certified service");

    runtime::RealtimePipelineConfigV1 pipeline_config{};
    pipeline_config.run_id = service_config.run_id;
    pipeline_config.trade_date = service_config.trade_date;
    pipeline_config.daily_catalog = fixture.catalog;
    pipeline_config.runtime_state = fixture.runtime_state.get();
    pipeline_config.source_stream_ids =
        {1001U, 1002U, 2001U, 2002U};
    pipeline_config.maximum_sdk_message_bytes = 4096U;
    pipeline_config.decoder_queue_capacity_per_source = 16U;
    pipeline_config.completion_tracker_capacity = 64U;
    pipeline_config.tick_ring_capacity = 64U;
    pipeline_config.store_worker_count = 1U;
    pipeline_config.store_queue_capacity_per_source_worker = 16U;
    pipeline_config.intraday_store.segment_target_bytes = 4096U;
    pipeline_config.intraday_store.maximum_session_records = 16U;
    pipeline_config.intraday_store.maximum_session_accounted_bytes =
        2U * 1024U * 1024U;
    pipeline_config.intraday_store.maximum_records_per_batch = 8U;
    pipeline_config.intraday_store.coverage_from_open = true;
    pipeline_config.applied_record_sink = service;
    pipeline_config.native_sequence_observation_sink = service;
    pipeline_config.sdk.enabled = false;

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    test->Expect(
        runtime::RealtimePipelineV1::Create(
            pipeline_config, &pipeline, &detail) ==
                runtime::RealtimePipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create Event-capacity pipeline: " + detail);
    if (pipeline == nullptr) {
        service->StopControl();
        return;
    }

    test->Expect(
        InjectWithType(pipeline.get(), 1U, 41U, "A").accepted(),
        "inject first one-Event add");
    test->Expect(
        WaitUntil([&] {
            const auto snapshot = service->Snapshot();
            return fast->count() == 1U &&
                   snapshot.wire_snapshot_consistent &&
                   snapshot.canonical_apply_frontier == 1U &&
                   snapshot.state ==
                       ipc::RealtimeCertifiedStateV1::kContiguous;
        }),
        "first Tick/Event transaction becomes last-good");

    ipc::RealtimeCertifiedTickEnvelopeV1 latest{};
    ipc::CertifiedOrderEventHistorySnapshotV1 events{};
    test->Expect(
        service->ReadLatestForTest(0U, &latest) &&
            latest.payload.native_event_sequence == 1 &&
            service->AcquireEventGeneration(&events) ==
                ipc::CertifiedOrderEventHistoryErrorV1::kNone &&
            events.valid() &&
            events.generation().input_frontier
                    .canonical_apply_sequence == 1U &&
            events.events().size() == 1U,
        "capture matching last-good Tick and Event");

    test->Expect(
        InjectWithType(pipeline.get(), 2U, 42U, "A").accepted(),
        "Event-capacity failure still accepts FAST tick");
    test->Expect(
        WaitUntil([&] {
            const auto snapshot = service->Snapshot();
            return fast->count() == 2U &&
                   snapshot.wire_snapshot_consistent &&
                   snapshot.globally_frozen_resource &&
                   snapshot.state ==
                       ipc::RealtimeCertifiedStateV1::
                           kFrozenResource;
        }),
        "Event capacity freezes only CERTIFIED");
    test->Expect(
        WaitUntil([&] {
            const auto frozen = service->Snapshot();
            ipc::RealtimeCertifiedTickEnvelopeV1 retained{};
            ipc::CertifiedOrderEventHistorySnapshotV1 retained_events{};
            return frozen.wire_snapshot_consistent &&
                   frozen.canonical_apply_frontier == 1U &&
                   service->ReadLatestForTest(0U, &retained) &&
                   retained.payload.native_event_sequence == 1 &&
                   service->AcquireEventGeneration(
                       &retained_events) ==
                       ipc::CertifiedOrderEventHistoryErrorV1::kNone &&
                   retained_events.valid() &&
                   retained_events.generation().input_frontier
                           .canonical_apply_sequence == 1U &&
                   retained_events.events().size() == 1U &&
                   fast->native(1U) == 2 &&
                   !fast->coverage_lost() &&
                   !pipeline->fatal();
        }),
        "resource failure preserves matching last-good CERTIFIED views");

    ipc::RealtimeCertifiedReaderOpenOptionsV1 open{};
    open.control_socket_path = service_config.control_socket_path;
    std::memcpy(
        open.expected_session.run_id.data(),
        service_config.run_id.data(),
        service_config.run_id.size());
    open.expected_session.session_epoch =
        service_config.session_epoch;
    open.expected_session.trade_date =
        service_config.trade_date;
    std::unique_ptr<ipc::CertifiedOrderEventReaderV1>
        event_reader;
    test->Expect(
        ipc::CertifiedOrderEventReaderV1::Open(
            open, &event_reader, &system_error) ==
                ipc::CertifiedOrderEventReaderOpenErrorV1::kNone &&
            event_reader != nullptr,
        "open Event reader after CERTIFIED resource freeze");
    ipc::CertifiedOrderEventStatusSnapshotV1 event_status{};
    ipc::CertifiedOrderEventEnvelopeV1 retained_event{};
    test->Expect(
        event_reader != nullptr &&
            event_reader->ReadStatus(&event_status) ==
                ipc::CertifiedOrderEventReadResultV1::kOk &&
            event_status.tick.canonical_apply_frontier == 1U &&
            event_status.event_canonical_apply_frontier == 1U &&
            event_status.coherent_canonical_apply_frontier == 1U &&
            event_status.event_published_sequence == 1U &&
            event_reader->ReadEvent(1U, &retained_event) ==
                ipc::CertifiedOrderEventReadResultV1::kOk &&
            retained_event.canonical_apply_sequence == 1U,
        "external reader retains matching last-good Tick/Event after freeze");

    test->Expect(
        InjectWithType(pipeline.get(), 3U, 43U, "A").accepted(),
        "FAST remains writable after CERTIFIED freezes");
    test->Expect(
        WaitUntil([&] {
            return fast->count() == 3U &&
                   fast->native(2U) == 3;
        }) &&
            !fast->coverage_lost() &&
            !pipeline->fatal() &&
            service->Snapshot().canonical_apply_frontier == 1U,
        "post-freeze FAST progress does not advance false CERTIFIED data");

    pipeline->StopAndDrain();
    service->MarkStoppedClean();
    test->Expect(
        service->Snapshot().state ==
            ipc::RealtimeCertifiedStateV1::kFrozenResource,
        "clean-stop request does not erase terminal resource state");
    service->StopControl();
}

}  // namespace

int main() {
    TestContext test;
    RunGapBeforeBarrierRejectsActivationScenario(&test);
    RunFrozenBeforeBarrierRejectsActivationScenario(&test);
    RunPostBarrierGapCannotRewriteActivationScenario(&test);
    RunRecoveryScenario(&test);
    RunEventCapacityFailOpenScenario(&test);
    if (test.failures() != 0) {
        return 1;
    }
    std::cout << "PASS: realtime certified service v1\n";
    return 0;
}
