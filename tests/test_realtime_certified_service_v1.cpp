#include "l2flow/common/linux_thread_affinity_v1.h"
#include "l2flow/ipc/certified_order_event_reader_c_v1.h"
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
namespace common = l2flow::common;
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
    WorkerBarrierFixture* output,
    bool start_worker = true,
    std::string_view worker_cpu_set = {},
    std::string_view control_cpu_set = {},
    std::shared_ptr<const std::atomic<bool>> exposure_gate = nullptr,
    bool control_requires_prefix_commit = false) {
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
    service_config.worker_cpu_set = std::string(worker_cpu_set);
    service_config.control_cpu_set = std::string(control_cpu_set);
    service_config.control_exposure_gate =
        std::move(exposure_gate);
    service_config.control_requires_prefix_commit =
        control_requires_prefix_commit;
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
        "create " + label + " worker-barrier service: " +
            std::string(
                ipc::RealtimeCertifiedServiceCreateErrorNameV1(
                    service_error)) +
            ", errno=" + std::to_string(system_error));
    if (!service_ready) {
        return false;
    }
    if (!start_worker) {
        return true;
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

void RunLinuxCpuSetParserScenario(TestContext* test) {
    common::LinuxCpuSetV1 parsed{};
    const auto parsed_error = common::ParseLinuxCpuSetV1(
        "0,2-4,7", &parsed);
    test->Expect(
        parsed_error == common::LinuxCpuSetParseErrorV1::kNone &&
            parsed.count() == 5U && parsed.contains(0U) &&
            parsed.contains(2U) && parsed.contains(3U) &&
            parsed.contains(4U) && parsed.contains(7U) &&
            !parsed.contains(1U) && !parsed.contains(5U),
        "strict Linux cpuset parser expands disjoint CPUs and ranges");

    common::LinuxCpuSetV1 repeated{};
    test->Expect(
        common::ParseLinuxCpuSetV1(
            "0,2-4,7", &repeated) ==
                common::LinuxCpuSetParseErrorV1::kNone &&
            repeated == parsed,
        "Linux cpuset parsing is deterministic");

    common::LinuxCpuSetV1 subset{};
    static_cast<void>(subset.Add(2U));
    static_cast<void>(subset.Add(4U));
    common::LinuxCpuSetV1 outside = subset;
    static_cast<void>(outside.Add(8U));
    test->Expect(
        common::LinuxCpuSetIsSubsetV1(subset, parsed) &&
            !common::LinuxCpuSetIsSubsetV1(outside, parsed),
        "Linux cpuset subset validation checks every requested CPU");

    common::LinuxCpuSetV1 dirty{};
    static_cast<void>(dirty.Add(9U));
    test->Expect(
        common::ParseLinuxCpuSetV1("", &dirty) ==
                common::LinuxCpuSetParseErrorV1::kEmpty &&
            dirty.empty(),
        "empty Linux cpuset is rejected and clears prior output");
    test->Expect(
        common::ParseLinuxCpuSetV1(" 0", &dirty) ==
            common::LinuxCpuSetParseErrorV1::kInvalidSyntax,
        "Linux cpuset rejects leading whitespace");
    test->Expect(
        common::ParseLinuxCpuSetV1("0 ", &dirty) ==
            common::LinuxCpuSetParseErrorV1::kInvalidSyntax,
        "Linux cpuset rejects trailing whitespace");
    test->Expect(
        common::ParseLinuxCpuSetV1("0,", &dirty) ==
            common::LinuxCpuSetParseErrorV1::kInvalidSyntax,
        "Linux cpuset rejects trailing separators");
    test->Expect(
        common::ParseLinuxCpuSetV1("2-2", &dirty) ==
            common::LinuxCpuSetParseErrorV1::kDescendingRange,
        "Linux cpuset rejects singleton range spelling");
    test->Expect(
        common::ParseLinuxCpuSetV1("3-2", &dirty) ==
            common::LinuxCpuSetParseErrorV1::kDescendingRange,
        "Linux cpuset rejects descending ranges");
    test->Expect(
        common::ParseLinuxCpuSetV1("1,1", &dirty) ==
            common::LinuxCpuSetParseErrorV1::kDuplicateCpu,
        "Linux cpuset rejects duplicate CPUs");
    test->Expect(
        common::ParseLinuxCpuSetV1("1-3,3", &dirty) ==
            common::LinuxCpuSetParseErrorV1::kDuplicateCpu,
        "Linux cpuset rejects overlapping ranges");
    test->Expect(
        common::ParseLinuxCpuSetV1("1024", &dirty) ==
            common::LinuxCpuSetParseErrorV1::kCpuOutOfRange,
        "Linux cpuset rejects CPUs beyond its fixed ABI bound");
    test->Expect(
        common::ParseLinuxCpuSetV1("x", &dirty) ==
            common::LinuxCpuSetParseErrorV1::kInvalidSyntax,
        "Linux cpuset rejects nonnumeric items");
    test->Expect(
        common::ParseLinuxCpuSetV1("0", nullptr) ==
            common::LinuxCpuSetParseErrorV1::kNullOutput,
        "Linux cpuset rejects a null output");
}

void RunCpuSetConfigurationValidationScenario(TestContext* test) {
    WorkerBarrierFixture fixture;
    if (!BuildWorkerBarrierFixture(
            test,
            "affinity-validation",
            std::byte{0x56},
            &fixture,
            false)) {
        return;
    }

    auto invalid_worker = fixture.service_config;
    invalid_worker.worker_cpu_set = "0 ";
    std::shared_ptr<ipc::RealtimeCertifiedMarketServiceV1> service;
    int system_error = 0;
    const auto worker_error =
        ipc::RealtimeCertifiedMarketServiceV1::Create(
            invalid_worker, &service, &system_error);
    test->Expect(
        worker_error ==
                ipc::RealtimeCertifiedServiceCreateErrorV1::
                    kInvalidConfiguration &&
            service == nullptr && system_error == EINVAL,
        "service rejects an invalid worker cpuset during Create");

    auto invalid_control = fixture.service_config;
    invalid_control.control_cpu_set = "1,,2";
    system_error = 0;
    const auto control_error =
        ipc::RealtimeCertifiedMarketServiceV1::Create(
            invalid_control, &service, &system_error);
    test->Expect(
        control_error ==
                ipc::RealtimeCertifiedServiceCreateErrorV1::
                    kInvalidConfiguration &&
            service == nullptr && system_error == EINVAL,
        "service rejects an invalid control cpuset during Create");
}

void RunConfiguredThreadAffinityScenario(TestContext* test) {
    common::LinuxCpuSetV1 allowed{};
    int system_error = 0;
    const auto allowed_error =
        common::ReadCurrentLinuxThreadAffinityV1(
            &allowed, &system_error);
    test->Expect(
        allowed_error == common::LinuxThreadAffinityErrorV1::kNone &&
            system_error == 0 && !allowed.empty(),
        "read a nonempty allowed cpuset for the affinity fixture");
    if (allowed_error != common::LinuxThreadAffinityErrorV1::kNone ||
        allowed.empty()) {
        return;
    }

    std::size_t event_cpu =
        common::kLinuxCpuSetMaximumCpuCountV1;
    std::size_t fast_cpu =
        common::kLinuxCpuSetMaximumCpuCountV1;
    for (std::size_t cpu = 0U;
         cpu < common::kLinuxCpuSetMaximumCpuCountV1;
         ++cpu) {
        if (allowed.contains(cpu)) {
            if (event_cpu ==
                common::kLinuxCpuSetMaximumCpuCountV1) {
                event_cpu = cpu;
            } else {
                fast_cpu = cpu;
                break;
            }
        }
    }
    test->Expect(
        event_cpu < common::kLinuxCpuSetMaximumCpuCountV1,
        "select an allowed CPU for the affinity fixture");
    if (event_cpu >= common::kLinuxCpuSetMaximumCpuCountV1) {
        return;
    }

    const std::string cpu_text = std::to_string(event_cpu);
    common::LinuxCpuSetV1 requested{};
    static_cast<void>(requested.Add(event_cpu));

    common::LinuxCpuSetV1 creator_expected = allowed;
    bool creator_narrowed = false;
    if (fast_cpu < common::kLinuxCpuSetMaximumCpuCountV1) {
        creator_expected.Clear();
        static_cast<void>(creator_expected.Add(fast_cpu));
        common::LinuxCpuSetV1 narrowed_actual{};
        const auto narrow_error =
            common::ApplyCurrentLinuxThreadAffinityExactV1(
                creator_expected, &narrowed_actual, &system_error);
        test->Expect(
            narrow_error ==
                    common::LinuxThreadAffinityErrorV1::kNone &&
                system_error == 0 &&
                narrowed_actual == creator_expected,
            "narrow the creator to a disjoint FAST CPU before Create");
        if (narrow_error !=
            common::LinuxThreadAffinityErrorV1::kNone) {
            return;
        }
        creator_narrowed = true;
    }
    const auto restore_creator = [&]() noexcept {
        if (!creator_narrowed) {
            return true;
        }
        common::LinuxCpuSetV1 restored{};
        int restore_error = 0;
        const auto result =
            common::ApplyCurrentLinuxThreadAffinityExactV1(
                allowed, &restored, &restore_error);
        if (result == common::LinuxThreadAffinityErrorV1::kNone &&
            restore_error == 0 && restored == allowed) {
            creator_narrowed = false;
            return true;
        }
        return false;
    };

    WorkerBarrierFixture fixture;
    if (!BuildWorkerBarrierFixture(
            test,
            "thread-affinity",
            std::byte{0x57},
            &fixture,
            true,
            cpu_text,
            cpu_text)) {
        test->Expect(
            restore_creator(),
            "restore creator affinity after fixture creation failure");
        return;
    }

    common::LinuxCpuSetV1 worker_actual{};
    system_error = 0;
    test->Expect(
        fixture.service->ReadWorkerCpuSetForTest(
            &worker_actual, &system_error) &&
            system_error == 0 && worker_actual == requested,
        "StartWorker expands from FAST and returns after exact Event readback");

    system_error = 0;
    const bool control_started =
        fixture.service->StartControl(&system_error);
    common::LinuxCpuSetV1 control_actual{};
    const bool control_read =
        control_started &&
        fixture.service->ReadControlCpuSetForTest(
            &control_actual, &system_error);
    test->Expect(
        control_read && system_error == 0 &&
            control_actual == requested,
        "StartControl expands from FAST and returns after exact Event readback");
    int late_barrier_error = 0;
    test->Expect(
        control_started &&
            !fixture.service->WaitForPrefixBarrier(
                5s, &late_barrier_error) &&
            late_barrier_error == EALREADY,
        "explicit StartControl makes the recovery barrier permanently late");

    common::LinuxCpuSetV1 creator_actual{};
    system_error = 0;
    test->Expect(
        common::ReadCurrentLinuxThreadAffinityV1(
            &creator_actual, &system_error) ==
                common::LinuxThreadAffinityErrorV1::kNone &&
            system_error == 0 &&
            creator_actual == creator_expected,
        "Event thread affinity does not widen the creator FAST mask");
    test->Expect(
        restore_creator(),
        "restore the test creator's original affinity exactly");
}

void RunControlPreStartStateScenario(TestContext* test) {
    WorkerBarrierFixture fixture;
    if (!BuildWorkerBarrierFixture(
            test,
            "control-prestart",
            std::byte{0x55},
            &fixture,
            false)) {
        return;
    }
    int system_error = 0;
    const bool rejected_without_worker =
        !fixture.service->StartControl(&system_error);
    const auto after_rejection = fixture.service->Snapshot();
    fixture.service->StopControl();
    const auto after_stop = fixture.service->Snapshot();
    test->Expect(
        rejected_without_worker && system_error == EINVAL &&
            after_rejection.control_state ==
                ipc::RealtimeCertifiedServiceSnapshotV1::
                    ControlState::kNotStarted &&
            after_stop.control_state ==
                ipc::RealtimeCertifiedServiceSnapshotV1::
                    ControlState::kStopped,
        "CERTIFIED control distinguishes intentional pre-start from stopped");
}

void RunRepeatablePrefixProbeScenario(TestContext* test) {
    WorkerBarrierFixture fixture;
    if (!BuildWorkerBarrierFixture(
            test,
            "repeatable-prefix-probe",
            std::byte{0x58},
            &fixture,
            true,
            {},
            {},
            nullptr,
            true)) {
        return;
    }

    int system_error = 0;
    test->Expect(
        !fixture.service->StartControl(&system_error) &&
            system_error == EBUSY,
        "recovery-mode control requires final prefix commit");

    ipc::RealtimeCertifiedPrefixFenceResultV1 empty{};
    const auto empty_error = fixture.service->ProbePrefixFence(
        5s, &empty, &system_error);
    test->Expect(
        empty_error ==
                ipc::RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kNone &&
            system_error == 0 && empty.ready() &&
            empty.state == ipc::RealtimeCertifiedStateV1::kNoData &&
            empty.canonical_apply_frontier == 0U &&
            empty.event_history.valid() &&
            empty.event_history.generation()
                    .input_frontier.canonical_apply_sequence == 0U &&
            empty.event_journal_frontier == 0U &&
            empty.event_published_sequence == 0U &&
            !fixture.service->StartupPrefixRecoveredForTest(),
        "empty exact probe is ready without publishing recovery coverage");

    test->Expect(
        Inject(fixture.pipeline.get(), 3U, 41U).accepted() &&
            WaitUntil([&] {
                const auto snapshot = fixture.service->Snapshot();
                return snapshot.wire_snapshot_consistent &&
                       snapshot.state ==
                           ipc::RealtimeCertifiedStateV1::kGapOpen;
            }),
        "create a retryable native gap before a probe");
    ipc::RealtimeCertifiedPrefixFenceResultV1 gap{};
    const auto gap_error = fixture.service->ProbePrefixFence(
        5s, &gap, &system_error);
    test->Expect(
        gap_error ==
                ipc::RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kNone &&
            gap.retryable() &&
            gap.state == ipc::RealtimeCertifiedStateV1::kGapOpen &&
            gap.fence_id > empty.fence_id &&
            gap.canonical_apply_frontier == 0U &&
            !fixture.service->StartupPrefixRecoveredForTest(),
        "GapOpen is a completed retryable probe without coverage mutation");

    test->Expect(
        Inject(fixture.pipeline.get(), 1U, 42U).accepted() &&
            Inject(fixture.pipeline.get(), 2U, 43U).accepted() &&
            WaitUntil([&] {
                const auto snapshot = fixture.service->Snapshot();
                return snapshot.wire_snapshot_consistent &&
                       snapshot.state ==
                           ipc::RealtimeCertifiedStateV1::kContiguous &&
                       snapshot.canonical_apply_frontier == 3U;
            }),
        "repair the probed gap to one contiguous prefix");

    ipc::RealtimeCertifiedPrefixFenceResultV1 ready{};
    const auto ready_error = fixture.service->ProbePrefixFence(
        5s, &ready, &system_error);
    const auto ready_generation = ready.event_history.generation();
    test->Expect(
        ready_error ==
                ipc::RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kNone &&
            ready.ready() &&
            ready.state ==
                ipc::RealtimeCertifiedStateV1::kContiguous &&
            ready.fence_id > gap.fence_id &&
            ready.canonical_apply_frontier == 3U &&
            ready.event_journal_frontier == 3U &&
            ready_generation.input_frontier
                    .canonical_apply_sequence == 3U &&
            ready.event_published_sequence ==
                static_cast<std::uint64_t>(
                    ready_generation.event_count) &&
            !fixture.service->StartupPrefixRecoveredForTest(),
        "repeated probe returns one exact coherent Tick/Event prefix");

    int probe_control_error = 0;
    test->Expect(
        !fixture.service->StartControl(&probe_control_error) &&
            probe_control_error == EBUSY &&
            !fixture.service->StartupPrefixRecoveredForTest(),
        "ready probe cannot authorize recovery-mode control exposure");

    ipc::RealtimeCertifiedPrefixFenceResultV1 committed{};
    system_error = 0;
    test->Expect(
        fixture.service->WaitForPrefixBarrier(
            5s, &committed, &system_error) &&
            system_error == 0 && committed.ready() &&
            committed.fence_id > ready.fence_id &&
            committed.canonical_apply_frontier == 3U &&
            committed.event_history.generation()
                    .input_frontier.canonical_apply_sequence == 3U &&
            fixture.service->StartupPrefixRecoveredForTest() &&
            fixture.service->StartControl(&system_error),
        "final one-shot commit publishes coverage and authorizes control");
}

void RunSerialProbeHealthTransitionScenario(TestContext* test) {
    WorkerBarrierFixture fixture;
    if (!BuildWorkerBarrierFixture(
            test,
            "serial-probe-health",
            std::byte{0x5d},
            &fixture,
            true,
            {},
            {},
            nullptr,
            true)) {
        return;
    }

    test->Expect(
        Inject(fixture.pipeline.get(), 1U, 71U).accepted() &&
            Inject(fixture.pipeline.get(), 3U, 73U).accepted() &&
            WaitUntil([&] {
                const auto snapshot = fixture.service->Snapshot();
                return snapshot.wire_snapshot_consistent &&
                       snapshot.state ==
                           ipc::RealtimeCertifiedStateV1::kGapOpen &&
                       snapshot.canonical_apply_frontier == 1U;
            }),
        "serial probe fixture opens the first gap at native sequence 2");
    ipc::RealtimeCertifiedPrefixFenceResultV1 first_gap{};
    int system_error = 0;
    const auto first_gap_error = fixture.service->ProbePrefixFence(
        5s, &first_gap, &system_error);
    test->Expect(
        first_gap_error ==
                ipc::RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kNone &&
            system_error == 0 && first_gap.retryable() &&
            first_gap.state ==
                ipc::RealtimeCertifiedStateV1::kGapOpen &&
            first_gap.canonical_apply_frontier == 1U &&
            first_gap.event_journal_frontier == 1U &&
            first_gap.event_history.generation()
                    .input_frontier.canonical_apply_sequence == 1U,
        "seq1+seq3 probe reports GapOpen at committed frontier 1");

    test->Expect(
        Inject(fixture.pipeline.get(), 2U, 72U).accepted() &&
            WaitUntil([&] {
                const auto snapshot = fixture.service->Snapshot();
                return snapshot.wire_snapshot_consistent &&
                       snapshot.state ==
                           ipc::RealtimeCertifiedStateV1::kContiguous &&
                       snapshot.canonical_apply_frontier == 3U;
            }),
        "injecting only seq2 repairs and drains the pending seq3 prefix");
    ipc::RealtimeCertifiedPrefixFenceResultV1 ready{};
    const auto ready_error = fixture.service->ProbePrefixFence(
        5s, &ready, &system_error);
    test->Expect(
        ready_error ==
                ipc::RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kNone &&
            system_error == 0 && ready.ready() &&
            ready.state ==
                ipc::RealtimeCertifiedStateV1::kContiguous &&
            ready.fence_id > first_gap.fence_id &&
            ready.canonical_apply_frontier == 3U &&
            ready.event_journal_frontier == 3U &&
            ready.event_history.generation()
                    .input_frontier.canonical_apply_sequence == 3U,
        "seq2 repair makes the next serial probe ready at frontier 3");

    test->Expect(
        Inject(fixture.pipeline.get(), 5U, 75U).accepted() &&
            WaitUntil([&] {
                const auto snapshot = fixture.service->Snapshot();
                return snapshot.wire_snapshot_consistent &&
                       snapshot.state ==
                           ipc::RealtimeCertifiedStateV1::kGapOpen &&
                       snapshot.canonical_apply_frontier == 3U;
            }),
        "later seq5 opens a new gap without rewriting frontier 3");
    ipc::RealtimeCertifiedPrefixFenceResultV1 second_gap{};
    const auto second_gap_error = fixture.service->ProbePrefixFence(
        5s, &second_gap, &system_error);
    test->Expect(
        second_gap_error ==
                ipc::RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kNone &&
            system_error == 0 && second_gap.retryable() &&
            second_gap.state ==
                ipc::RealtimeCertifiedStateV1::kGapOpen &&
            second_gap.fence_id > ready.fence_id &&
            second_gap.canonical_apply_frontier == 3U &&
            second_gap.event_journal_frontier == 3U &&
            second_gap.event_history.generation()
                    .input_frontier.canonical_apply_sequence == 3U &&
            !fixture.service->StartupPrefixRecoveredForTest(),
        "a healthy adjacent prefix can be followed by a new serial GapOpen cut");
}

void RunPrefixProbeTimeoutLateAckScenario(TestContext* test) {
    WorkerBarrierFixture fixture;
    if (!BuildWorkerBarrierFixture(
            test,
            "prefix-probe-late-ack",
            std::byte{0x5c},
            &fixture,
            true,
            {},
            {},
            nullptr,
            true)) {
        return;
    }

    fixture.service->SetPrefixProbePausedForTest(true);
    ipc::RealtimeCertifiedPrefixFenceResultV1 first{};
    int first_system_error = 0;
    const auto first_error = fixture.service->ProbePrefixFence(
        20ms, &first, &first_system_error);
    const bool old_marker_reached = WaitUntil([&] {
        return fixture.service->PrefixProbeReachedForTest();
    });
    test->Expect(
        first_error ==
                ipc::RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kTimedOut &&
            first_system_error == ETIMEDOUT && first.fence_id != 0U &&
            old_marker_reached &&
            !fixture.service->StartupPrefixRecoveredForTest(),
        "timed-out probe leaves one unacknowledged side-effect-free marker");

    test->Expect(
        Inject(fixture.pipeline.get(), 1U, 61U).accepted() &&
            WaitUntil([&] {
                const auto snapshot = fixture.service->Snapshot();
                return snapshot.enqueued_observations >= 1U &&
                       snapshot.enqueued_applied_records >= 1U;
            }),
        "enqueue later live data behind the timed-out probe marker");
    ipc::RealtimeCertifiedPrefixFenceResultV1 second{};
    int second_system_error = 0;
    auto second_error =
        ipc::RealtimeCertifiedPrefixFenceOperationErrorV1::
            kInternalFailure;
    std::thread second_waiter([&] {
        second_error = fixture.service->ProbePrefixFence(
            5s, &second, &second_system_error);
    });
    const bool waited_for_old_ack = WaitUntil([&] {
        return fixture.service->
            PrefixProbeWaitingForPriorAckForTest();
    });
    fixture.service->SetPrefixProbePausedForTest(false);
    second_waiter.join();

    test->Expect(
        waited_for_old_ack &&
            !fixture.service->
                PrefixProbeWaitingForPriorAckForTest() &&
            second_error ==
                ipc::RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kNone &&
            second_system_error == 0 && second.ready() &&
            second.state ==
                ipc::RealtimeCertifiedStateV1::kContiguous &&
            second.fence_id > first.fence_id &&
            second.canonical_apply_frontier == 1U &&
            second.event_journal_frontier == 1U &&
            second.event_history.generation()
                    .input_frontier.canonical_apply_sequence == 1U &&
            !fixture.service->StartupPrefixRecoveredForTest(),
        "next probe waits for late ack then captures a distinct newer FIFO cut");
}

void RunPrefixCommitTimeoutCancellationScenario(TestContext* test) {
    WorkerBarrierFixture fixture;
    if (!BuildWorkerBarrierFixture(
            test,
            "prefix-timeout-cancel",
            std::byte{0x59},
            &fixture,
            true,
            {},
            {},
            nullptr,
            true)) {
        return;
    }
    fixture.service->SetPrefixCommitPausedForTest(true);
    ipc::RealtimeCertifiedPrefixFenceResultV1 result{};
    int system_error = 0;
    bool committed = true;
    std::thread waiter([&] {
        committed = fixture.service->WaitForPrefixBarrier(
            20ms, &result, &system_error);
    });
    const bool worker_reached = WaitUntil([&] {
        return fixture.service->PrefixCommitReachedForTest();
    });
    waiter.join();
    test->Expect(
        worker_reached && !committed &&
            system_error == ETIMEDOUT &&
            result.operation_error ==
                ipc::RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kTimedOut &&
            !fixture.service->StartupPrefixRecoveredForTest(),
        "caller cancellation wins timeout before final metadata commit");

    fixture.service->SetPrefixCommitPausedForTest(false);
    const bool worker_finished = WaitUntil([&] {
        return fixture.service->PrefixCommitFinishedForTest();
    });
    int retry_error = 0;
    ipc::RealtimeCertifiedPrefixFenceResultV1 retry{};
    test->Expect(
        worker_finished &&
            !fixture.service->StartupPrefixRecoveredForTest() &&
            !fixture.service->WaitForPrefixBarrier(
                5s, &retry, &retry_error) &&
            retry_error == EALREADY,
        "late worker cannot mark recovered and final barrier remains one-shot");
    int control_error = 0;
    test->Expect(
        !fixture.service->StartControl(&control_error) &&
            control_error == EBUSY,
        "cancelled final commit permanently blocks recovery control");
}

void RunPrefixCommitGlobalFreezeDominanceScenario(
    TestContext* test) {
    WorkerBarrierFixture fixture;
    if (!BuildWorkerBarrierFixture(
            test,
            "prefix-global-freeze",
            std::byte{0x5a},
            &fixture,
            true,
            {},
            {},
            nullptr,
            true)) {
        return;
    }

    fixture.service->SetPrefixCommitPausedForTest(true);
    ipc::RealtimeCertifiedPrefixFenceResultV1 result{};
    int system_error = 0;
    bool committed = true;
    std::thread waiter([&] {
        committed = fixture.service->WaitForPrefixBarrier(
            5s, &result, &system_error);
    });
    const bool worker_reached = WaitUntil([&] {
        return fixture.service->PrefixCommitReachedForTest();
    });
    // This release-store occurs after the fence header publication and before
    // its immutable result capture, exercising the otherwise narrow race.
    fixture.service->MarkNativeSequenceObservationFailure(
        l2flow::realtime::NativeSequenceObservationFailureV1::kHandoff,
        sdk::MessageKey{4U, 102U, 25U});
    fixture.service->SetPrefixCommitPausedForTest(false);
    waiter.join();
    const bool worker_finished = WaitUntil([&] {
        return fixture.service->PrefixCommitFinishedForTest();
    });

    test->Expect(
        worker_reached && worker_finished && !committed &&
            system_error == EIO && result.terminal() &&
            result.state ==
                ipc::RealtimeCertifiedStateV1::kFrozenResource &&
            !fixture.service->StartupPrefixRecoveredForTest(),
        "global resource freeze dominates final promotion after header publication");
    int control_error = 0;
    test->Expect(
        !fixture.service->StartControl(&control_error) &&
            control_error == EBUSY,
        "resource-frozen final commit cannot authorize recovery control");
}

void RunPrefixCommitStopCancellationScenario(TestContext* test) {
    WorkerBarrierFixture fixture;
    if (!BuildWorkerBarrierFixture(
            test,
            "prefix-stop-cancel",
            std::byte{0x5b},
            &fixture,
            true,
            {},
            {},
            nullptr,
            true)) {
        return;
    }

    fixture.service->SetPrefixCommitPausedForTest(true);
    ipc::RealtimeCertifiedPrefixFenceResultV1 result{};
    int system_error = 0;
    bool committed = true;
    std::thread waiter([&] {
        committed = fixture.service->WaitForPrefixBarrier(
            10s, &result, &system_error);
    });
    const bool worker_reached = WaitUntil([&] {
        return fixture.service->PrefixCommitReachedForTest();
    });
    std::atomic<bool> stop_returned{false};
    std::thread stopper([&] {
        fixture.service->StopControl();
        stop_returned.store(true, std::memory_order_release);
    });
    const bool stop_completed_without_fence_timeout = WaitUntil([&] {
        return stop_returned.load(std::memory_order_acquire);
    });
    if (!stop_completed_without_fence_timeout) {
        // Keep a failing test recoverable rather than waiting for the 10s
        // operation timeout if lifecycle locking regresses.
        fixture.service->SetPrefixCommitPausedForTest(false);
    }
    stopper.join();
    waiter.join();
    fixture.service->SetPrefixCommitPausedForTest(false);

    test->Expect(
        worker_reached && stop_completed_without_fence_timeout &&
            fixture.service->PrefixCommitFinishedForTest() &&
            !committed && system_error == EPIPE &&
            result.operation_error ==
                ipc::RealtimeCertifiedPrefixFenceOperationErrorV1::
                    kWorkerFailed &&
            !fixture.service->StartupPrefixRecoveredForTest(),
        "StopControl cancels pending promotion without waiting for the fence timeout");
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
        !fixture.service->WaitForPrefixBarrier(
            5s, &system_error) &&
            system_error == EIO,
        "pre-barrier GAP_OPEN rejects the prefix barrier with EIO");
    int activation_error = 0;
    test->Expect(
        !fixture.service->StartControl(&activation_error) &&
            activation_error == EBUSY,
        "failed recovery barrier permanently blocks control exposure");
    test->Expect(
        WorkerOnlyControlHasNoResponse(
            fixture.service_config.control_socket_path),
        "rejected GAP_OPEN barrier leaves control unavailable");
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
        !fixture.service->WaitForPrefixBarrier(
            5s, &system_error) &&
            system_error == EIO,
        "pre-barrier FROZEN_RESOURCE rejects the prefix barrier with EIO");
    test->Expect(
        WorkerOnlyControlHasNoResponse(
            fixture.service_config.control_socket_path),
        "rejected FROZEN barrier leaves control unavailable");
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
    const bool barrier_ready =
        fixture.service->WaitForPrefixBarrier(5s, &system_error);
    const bool activated =
        barrier_ready && fixture.service->StartControl(&system_error);
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
    fixture.pipeline->StopAndDrain();
    fixture.service->StopControl();
    const auto stopped = fixture.service->Snapshot();
    test->Expect(
        stopped.control_state ==
            ipc::RealtimeCertifiedServiceSnapshotV1::
                ControlState::kStopped,
        "intentional CERTIFIED control stop is not reported as an asynchronous failure");
}

void RunControlFailureHealthScenario(TestContext* test) {
    WorkerBarrierFixture fixture;
    if (!BuildWorkerBarrierFixture(
            test,
            "control-health",
            std::byte{0x54},
            &fixture)) {
        return;
    }
    const auto worker_only = fixture.service->Snapshot();
    test->Expect(
        worker_only.worker_running &&
            worker_only.control_state ==
                ipc::RealtimeCertifiedServiceSnapshotV1::
                    ControlState::kNotStarted,
        "worker-only recovery phase does not require control exposure");

    int system_error = 0;
    const bool activated =
        fixture.service->WaitForPrefixBarrier(5s, &system_error) &&
        fixture.service->StartControl(&system_error);
    const bool active_health = WaitUntil([&] {
        const auto snapshot = fixture.service->Snapshot();
        return snapshot.control_state ==
               ipc::RealtimeCertifiedServiceSnapshotV1::
                   ControlState::kRunning;
    });
    const bool failure_injected =
        fixture.service->FailControlForTest();
    const bool failure_published = WaitUntil([&] {
        const auto snapshot = fixture.service->Snapshot();
        return snapshot.control_state ==
               ipc::RealtimeCertifiedServiceSnapshotV1::
                   ControlState::kFailed;
    });
    test->Expect(
        activated && system_error == 0 && active_health &&
            failure_injected && failure_published &&
            fixture.service->Snapshot().worker_running &&
            !fixture.pipeline->fatal(),
        "asynchronous CERTIFIED accept-loop failure is visible without corrupting FAST data workers");
}

void RunControlExposureGateScenario(TestContext* test) {
    const auto gate = std::make_shared<std::atomic<bool>>(false);
    WorkerBarrierFixture fixture;
    if (!BuildWorkerBarrierFixture(
            test,
            "control-exposure-gate",
            std::byte{0x55},
            &fixture,
            true,
            {},
            {},
            gate)) {
        return;
    }
    int system_error = 0;
    test->Expect(
        fixture.service->WaitForPrefixBarrier(5s, &system_error) &&
            fixture.service->StartControl(&system_error),
        "start CERTIFIED Tick/Event control behind false exposure gate");

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
    open.timeout = 100ms;
    std::unique_ptr<ipc::RealtimeCertifiedReaderV1> hidden_tick;
    std::unique_ptr<ipc::CertifiedOrderEventReaderV1> hidden_event;
    const auto hidden_tick_error =
        ipc::RealtimeCertifiedReaderV1::Open(
            open, &hidden_tick, &system_error);
    const auto hidden_event_error =
        ipc::CertifiedOrderEventReaderV1::Open(
            open, &hidden_event, &system_error);
    test->Expect(
        hidden_tick_error !=
                ipc::RealtimeCertifiedReaderOpenErrorV1::kNone &&
            hidden_tick == nullptr &&
            hidden_event_error !=
                ipc::CertifiedOrderEventReaderOpenErrorV1::kNone &&
            hidden_event == nullptr,
        "false shared gate transfers neither CERTIFIED descriptor");

    gate->store(true, std::memory_order_release);
    std::unique_ptr<ipc::RealtimeCertifiedReaderV1> visible_tick;
    std::unique_ptr<ipc::CertifiedOrderEventReaderV1> visible_event;
    const auto visible_tick_error =
        ipc::RealtimeCertifiedReaderV1::Open(
            open, &visible_tick, &system_error);
    const auto visible_event_error =
        ipc::CertifiedOrderEventReaderV1::Open(
            open, &visible_event, &system_error);
    ipc::CertifiedOrderEventStatusSnapshotV1 status{};
    test->Expect(
        visible_tick_error ==
                ipc::RealtimeCertifiedReaderOpenErrorV1::kNone &&
            visible_tick != nullptr &&
            visible_event_error ==
                ipc::CertifiedOrderEventReaderOpenErrorV1::kNone &&
            visible_event != nullptr &&
            visible_event->ReadStatus(&status) ==
                ipc::CertifiedOrderEventReadResultV1::kOk &&
            status.coverage_from_open() &&
            status.startup_prefix_recovered(),
        "one release store makes both CERTIFIED views obtainable");
    fixture.pipeline->StopAndDrain();
    fixture.service->StopControl();
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
        service->handoff_queue_capacity() ==
            service_config.handoff_queue_capacity,
        "certified service applies the configured handoff queue capacity");
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
        service->WaitForPrefixBarrier(5s, &system_error),
        "commit certified prefix barrier before control activation");
    test->Expect(
        service->StartControl(&system_error),
        "activate certified control after committed prefix barrier");
    test->Expect(
        !service->StartControl(&system_error),
        "certified control activation is one-shot");

    l2flow_certified_order_event_expected_session_v1
        promotion_expected{};
    std::memcpy(
        promotion_expected.run_id,
        service_config.run_id.data(),
        service_config.run_id.size());
    promotion_expected.session_epoch =
        service_config.session_epoch;
    promotion_expected.trade_date = service_config.trade_date;
    l2flow_certified_order_event_reader_v1*
        promotion_reader = nullptr;
    int promotion_open_system_error = 0;
    test->Expect(
        l2flow_certified_order_event_reader_open_v1(
            service_config.control_socket_path.c_str(),
            &promotion_expected,
            5'000U,
            &promotion_reader,
            &promotion_open_system_error) ==
                L2FLOW_CERTIFIED_ORDER_EVENT_OPEN_OK_V1 &&
            promotion_reader != nullptr &&
            promotion_open_system_error == 0,
        "C reader attaches after repaired prefix promotion");
    l2flow_certified_order_event_session_v1
        promotion_session{};
    l2flow_certified_order_event_status_v1
        promotion_status{};
    test->Expect(
        promotion_reader != nullptr &&
            l2flow_certified_order_event_reader_session_v1(
                promotion_reader, &promotion_session) ==
                L2FLOW_CERTIFIED_ORDER_EVENT_READ_OK_V1 &&
            l2flow_certified_order_event_reader_status_v1(
                promotion_reader, &promotion_status) ==
                L2FLOW_CERTIFIED_ORDER_EVENT_READ_OK_V1 &&
            promotion_session.coverage_flags ==
                (L2FLOW_CERTIFIED_ORDER_EVENT_COVERAGE_FROM_OPEN_V1 |
                 L2FLOW_CERTIFIED_ORDER_EVENT_STARTUP_PREFIX_RECOVERED_V1) &&
            promotion_status.coverage_flags ==
                promotion_session.coverage_flags &&
            promotion_status.certified_state ==
                L2FLOW_CERTIFIED_ORDER_EVENT_STATE_CONTIGUOUS_V1 &&
            promotion_status.tick_canonical_apply_frontier == 3U &&
            promotion_status.event_canonical_apply_frontier == 3U &&
            promotion_status.coherent_canonical_apply_frontier == 3U &&
            promotion_status.gap_opened_count == 1U &&
            promotion_status.gap_recovered_count == 1U,
        "promoted C status exposes recovered coverage, quality, and coherent frontier");
    std::vector<l2flow_certified_order_event_envelope_v1>
        promotion_history(
            static_cast<std::size_t>(
                service_config.maximum_derived_events));
    l2flow_certified_order_event_read_batch_result_v1
        promotion_history_result{};
    test->Expect(
        promotion_reader != nullptr &&
            l2flow_certified_order_event_reader_read_v1(
                promotion_reader,
                1U,
                promotion_history.data(),
                promotion_history.size(),
                &promotion_history_result) ==
                L2FLOW_CERTIFIED_ORDER_EVENT_READ_OK_V1 &&
            promotion_history_result.records_written > 0U &&
            promotion_history_result.records_written ==
                promotion_history_result.status
                    .event_published_sequence &&
            promotion_history_result.next_event_sequence ==
                promotion_history_result.records_written + 1U,
        "C cursor drains the complete repaired 3,1,2 Event history");
    bool promotion_history_valid =
        promotion_history_result.records_written > 0U &&
        promotion_history_result.records_written <=
            promotion_history.size();
    for (std::size_t index = 0U;
         promotion_history_valid &&
         index < promotion_history_result.records_written;
         ++index) {
        const auto& row = promotion_history[index];
        promotion_history_valid =
            row.event.derived_event_sequence == index + 1U &&
            row.canonical_apply_sequence >= 1U &&
            row.canonical_apply_sequence <= 3U &&
            row.event.native_event_sequence ==
                static_cast<std::int64_t>(
                    row.canonical_apply_sequence);
    }
    test->Expect(
        promotion_history_valid,
        "repaired Event history is dense and native ordered");
    const std::uint64_t promotion_tail_sequence =
        promotion_history_result.next_event_sequence;
    l2flow_certified_order_event_read_batch_result_v1
        promotion_idle_result{};
    test->Expect(
        promotion_reader != nullptr &&
            l2flow_certified_order_event_reader_read_v1(
                promotion_reader,
                promotion_tail_sequence,
                promotion_history.data(),
                promotion_history.size(),
                &promotion_idle_result) ==
                L2FLOW_CERTIFIED_ORDER_EVENT_READ_OK_V1 &&
            promotion_idle_result.records_written == 0U &&
            promotion_idle_result.next_event_sequence ==
                promotion_tail_sequence &&
            promotion_idle_result.status
                    .coherent_canonical_apply_frontier == 3U,
        "drained History cursor becomes tail-idle without EOF or cursor movement");

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

    l2flow_certified_order_event_read_batch_result_v1
        promotion_tail_result{};
    int promotion_tail_read =
        L2FLOW_CERTIFIED_ORDER_EVENT_READ_INCONSISTENT_V1;
    const bool promotion_tail_ready = WaitUntil([&] {
        promotion_tail_result = {};
        promotion_tail_read =
            l2flow_certified_order_event_reader_read_v1(
                promotion_reader,
                promotion_tail_sequence,
                promotion_history.data(),
                promotion_history.size(),
                &promotion_tail_result);
        return promotion_tail_read ==
                   L2FLOW_CERTIFIED_ORDER_EVENT_READ_OK_V1 &&
               promotion_tail_result.records_written > 0U;
    });
    test->Expect(
        promotion_reader != nullptr && promotion_tail_ready &&
            promotion_tail_result.status
                    .tick_canonical_apply_frontier == 4U &&
            promotion_tail_result.status
                    .event_canonical_apply_frontier == 4U &&
            promotion_tail_result.status
                    .coherent_canonical_apply_frontier == 4U &&
            promotion_tail_result.next_event_sequence ==
                promotion_tail_sequence +
                    promotion_tail_result.records_written,
        "same C History cursor consumes the first post-promotion live tail");
    bool promotion_tail_valid =
        promotion_tail_result.records_written > 0U &&
        promotion_tail_result.records_written <=
            promotion_history.size();
    for (std::size_t index = 0U;
         promotion_tail_valid &&
         index < promotion_tail_result.records_written;
         ++index) {
        const auto& row = promotion_history[index];
        promotion_tail_valid =
            row.event.derived_event_sequence ==
                promotion_tail_sequence + index &&
            row.canonical_apply_sequence == 4U &&
            row.event.native_event_sequence == 5;
    }
    test->Expect(
        promotion_tail_valid,
        "post-promotion Event tail remains dense at the coherent frontier");
    l2flow_certified_order_event_reader_close_v1(
        promotion_reader);
    promotion_reader = nullptr;

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
            event_status.coverage_from_open() &&
            event_status.startup_prefix_recovered() &&
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
        "open ordinary from-open Event reader before first data");
    ipc::CertifiedOrderEventStatusSnapshotV1 initial_event_status{};
    std::array<ipc::CertifiedOrderEventEnvelopeV1, 4U>
        live_tail_rows{};
    ipc::CertifiedOrderEventReadBatchResultV1 idle_batch{};
    test->Expect(
        event_reader != nullptr &&
            event_reader->ReadStatus(&initial_event_status) ==
                ipc::CertifiedOrderEventReadResultV1::kOk &&
            initial_event_status.coverage_from_open() &&
            !initial_event_status.startup_prefix_recovered() &&
            initial_event_status.tick.canonical_apply_frontier == 0U &&
            initial_event_status.event_canonical_apply_frontier == 0U &&
            initial_event_status.coherent_canonical_apply_frontier == 0U &&
            event_reader->Read(1U, live_tail_rows, &idle_batch) ==
                ipc::CertifiedOrderEventReadResultV1::kOk &&
            idle_batch.written == 0U &&
            idle_batch.next_event_sequence == 1U,
        "ordinary from-open Event cursor starts tail-idle without recovery metadata");
    int late_start_barrier_error = 0;
    ipc::CertifiedOrderEventStatusSnapshotV1
        after_late_barrier_status{};
    test->Expect(
        !service->WaitForPrefixBarrier(
            5s, &late_start_barrier_error) &&
            late_start_barrier_error == EALREADY &&
            event_reader->ReadStatus(&after_late_barrier_status) ==
                ipc::CertifiedOrderEventReadResultV1::kOk &&
            after_late_barrier_status.coverage_flags ==
                initial_event_status.coverage_flags &&
            after_late_barrier_status.coverage_from_open() &&
            !after_late_barrier_status.startup_prefix_recovered() &&
            after_late_barrier_status.coherent_canonical_apply_frontier ==
                initial_event_status.coherent_canonical_apply_frontier,
        "Start rejects late recovery promotion without mutating exposed coverage");

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

    ipc::CertifiedOrderEventReadBatchResultV1 live_tail_batch{};
    ipc::CertifiedOrderEventReadResultV1 live_tail_read =
        ipc::CertifiedOrderEventReadResultV1::kInconsistent;
    const bool live_tail_ready = WaitUntil([&] {
        live_tail_batch = {};
        live_tail_read = event_reader == nullptr
                             ? ipc::CertifiedOrderEventReadResultV1::
                                   kCorrupt
                             : event_reader->Read(
                                   idle_batch.next_event_sequence,
                                   live_tail_rows,
                                   &live_tail_batch);
        return live_tail_read ==
                   ipc::CertifiedOrderEventReadResultV1::kOk &&
               live_tail_batch.written != 0U;
    });
    test->Expect(
        event_reader != nullptr && live_tail_ready &&
            live_tail_batch.written == 1U &&
            live_tail_batch.next_event_sequence == 2U &&
            live_tail_batch.status.coverage_from_open() &&
            !live_tail_batch.status.startup_prefix_recovered() &&
            live_tail_batch.status.coherent_canonical_apply_frontier == 1U &&
            live_tail_rows[0U].canonical_apply_sequence == 1U &&
            live_tail_rows[0U].event.derived_event_sequence == 1U,
        "same ordinary cursor reads the first live Event append");

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
    RunLinuxCpuSetParserScenario(&test);
    RunCpuSetConfigurationValidationScenario(&test);
    RunConfiguredThreadAffinityScenario(&test);
    RunRepeatablePrefixProbeScenario(&test);
    RunSerialProbeHealthTransitionScenario(&test);
    RunPrefixProbeTimeoutLateAckScenario(&test);
    RunPrefixCommitTimeoutCancellationScenario(&test);
    RunPrefixCommitGlobalFreezeDominanceScenario(&test);
    RunPrefixCommitStopCancellationScenario(&test);
    RunGapBeforeBarrierRejectsActivationScenario(&test);
    RunFrozenBeforeBarrierRejectsActivationScenario(&test);
    RunPostBarrierGapCannotRewriteActivationScenario(&test);
    RunControlPreStartStateScenario(&test);
    RunControlFailureHealthScenario(&test);
    RunControlExposureGateScenario(&test);
    RunRecoveryScenario(&test);
    RunEventCapacityFailOpenScenario(&test);
    if (test.failures() != 0) {
        return 1;
    }
    std::cout << "PASS: realtime certified service v1\n";
    return 0;
}
