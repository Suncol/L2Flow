#include "l2flow/ingress/raw_ingress_app.h"
#include "l2flow/ingress/finalization_report_v1.h"
#include "l2flow/ingress/raw_finalization_continuation_posix.h"
#include "l2flow/ingress/raw_manifest_store.h"
#include "l2flow/ingress/raw_manifest_transition.h"
#include "l2flow/ingress/raw_namespace.h"
#include "l2flow/ingress/raw_reader.h"
#include "l2flow/ingress/raw_reserve_state_posix.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/raw_segment_artifacts.h"
#include "l2flow/ingress/raw_v1.h"
#include "l2flow/ingress/reserve_emergency_transition_v1.h"
#include "l2flow/common/sha256.h"
#include "raw_finalization_continuation_test_peer.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ingress = l2flow::ingress;
namespace sdk = l2flow::sdk;
namespace mdl = datayes::mdl;

namespace {

struct TestContext final {
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }
    int failures = 0;
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t seed) {
    std::array<std::byte, Size> result{};
    for (std::size_t index = 0U;
         index < Size;
         ++index) {
        result[index] =
            static_cast<std::byte>(
                static_cast<std::uint8_t>(seed + index));
    }
    return result;
}

std::uint64_t DigestLabel(
    const l2flow::common::Sha256Digest& digest) {
    std::uint64_t result = 0U;
    for (std::size_t index = 0U;
         index < 8U;
         ++index) {
        result =
            (result << 8U) |
            std::to_integer<std::uint64_t>(
                digest[index]);
    }
    return result;
}

class FixedClock final : public ingress::CaptureClock {
public:
    std::uint64_t MonotonicRawNanoseconds() override {
        return monotonic_++;
    }
    std::uint64_t RealtimeNanoseconds() override {
        return realtime_++;
    }

private:
    std::uint64_t monotonic_ = 100U;
    std::uint64_t realtime_ = 200U;
};

std::uint64_t AtomicWorkerClock(
    void* context) noexcept {
    return static_cast<
               std::atomic<std::uint64_t>*>(context)
        ->load(std::memory_order_acquire);
}

class EmptyPreparedSink final : public ingress::RawWalSink {
public:
    EmptyPreparedSink(
        const ingress::RawIngressRuntimeState& runtime,
        bool cursor_matches,
        bool advance_on_seal = false)
        : advance_on_seal_(advance_on_seal) {
        identity_.writer_instance =
            runtime.writer_instance;
        identity_.stream_day_id =
            runtime.stream_day_id;
        identity_.source_stream_id =
            runtime.source_stream_id;
        identity_.capture_date =
            runtime.capture_date;
        identity_.segment_sequence =
            runtime.current_segment_sequence;
        identity_.segment_base_wal_pos =
            runtime.append.global_wal_pos -
            runtime.append.segment_offset;
        identity_.first_ingress_sequence = 1U;
        snapshot_.initialized = true;
        snapshot_.append = {
            runtime.append.global_wal_pos,
            runtime.append.ingress_sequence,
            runtime.append.segment_offset};
        snapshot_.durable = {
            runtime.durable.global_wal_pos,
            runtime.durable.ingress_sequence,
            runtime.durable.segment_offset};
        if (!cursor_matches) {
            ++snapshot_.append.global_wal_pos;
            ++snapshot_.append.segment_offset;
            snapshot_.durable = snapshot_.append;
        }
    }

    bool AppendRecord(
        const ingress::RawWalRecordInputV1& input)
        noexcept override {
        ingress::RawRecordLayoutV1 layout{};
        if (!snapshot_.initialized ||
            snapshot_.sealed ||
            snapshot_.closed ||
            snapshot_.fatal ||
            ingress::ComputeRawRecordLayoutV1(
                input.vendor_body.size(),
                &layout) !=
                ingress::RawV1Error::kNone ||
            input.meta.ingress_sequence !=
                snapshot_.append
                        .ingress_sequence +
                    1U) {
            return false;
        }
        snapshot_.append.global_wal_pos +=
            layout.record_size;
        snapshot_.append.segment_offset +=
            layout.record_size;
        snapshot_.append.ingress_sequence =
            input.meta.ingress_sequence;
        return true;
    }

    bool FlushDurable() noexcept override {
        if (!snapshot_.initialized ||
            snapshot_.sealed ||
            snapshot_.fatal) {
            return false;
        }
        snapshot_.durable = snapshot_.append;
        return true;
    }

    bool SealAndClose() noexcept override {
        if (!FlushDurable()) {
            return false;
        }
        snapshot_.sealed = true;
        snapshot_.closed = true;
        if (advance_on_seal_) {
            snapshot_.append.global_wal_pos += 8U;
            snapshot_.append.segment_offset += 8U;
            snapshot_.append.ingress_sequence = 1U;
            snapshot_.durable = snapshot_.append;
        }
        return true;
    }

    ingress::RawWalWriterSnapshot Snapshot()
        const noexcept override {
        return snapshot_;
    }

    ingress::RawWalFailure failure()
        const noexcept override {
        return {};
    }
    ingress::RawWalSinkIdentityV1 identity()
        const noexcept override {
        ingress::RawWalSinkIdentityV1 result =
            identity_;
        if (foreign_identity_.load(
                std::memory_order_acquire)) {
            result.writer_instance[0U] ^=
                std::byte{0x7fU};
        }
        return result;
    }

    void MakeIdentityForeignForTest() noexcept {
        foreign_identity_.store(
            true, std::memory_order_release);
    }

private:
    bool advance_on_seal_ = false;
    std::atomic<bool> foreign_identity_{false};
    ingress::RawWalSinkIdentityV1 identity_{};
    ingress::RawWalWriterSnapshot snapshot_{};
};

class SlowRecordingPreparedSink final
    : public ingress::RawWalSink {
public:
    explicit SlowRecordingPreparedSink(
        const ingress::RawIngressRuntimeState& runtime) {
        identity_.writer_instance =
            runtime.writer_instance;
        identity_.stream_day_id =
            runtime.stream_day_id;
        identity_.source_stream_id =
            runtime.source_stream_id;
        identity_.capture_date =
            runtime.capture_date;
        identity_.segment_sequence =
            runtime.current_segment_sequence;
        identity_.segment_base_wal_pos =
            runtime.append.global_wal_pos -
            runtime.append.segment_offset;
        identity_.first_ingress_sequence =
            runtime.recovered_next_ingress_sequence;
        snapshot_.initialized = true;
        snapshot_.append = {
            runtime.append.global_wal_pos,
            runtime.append.ingress_sequence,
            runtime.append.segment_offset};
        snapshot_.durable = {
            runtime.durable.global_wal_pos,
            runtime.durable.ingress_sequence,
            runtime.durable.segment_offset};
        snapshot_.journal_logical_size =
            ingress::kRawV1JournalHeaderBytes +
            ingress::kRawV1DurableMarkerBytes;
    }

    bool AppendRecord(
        const ingress::RawWalRecordInputV1& input)
        noexcept override {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(25));
        try {
            if (input.vendor_head.size() !=
                ingress::kVendorMessageHeadBytes) {
                return false;
            }
            ingress::RawRecordInputV1 raw_input{};
            raw_input.meta = input.meta;
            std::copy(
                input.vendor_head.begin(),
                input.vendor_head.end(),
                raw_input.vendor_head.begin());
            raw_input.vendor_body =
                input.vendor_body;
            std::vector<std::byte> wire;
            if (ingress::EncodeRawRecordV1(
                    raw_input, &wire) !=
                ingress::RawV1Error::kNone) {
                return false;
            }
            std::lock_guard<std::mutex> lock(mutex_);
            if (!snapshot_.initialized ||
                snapshot_.sealed ||
                snapshot_.closed ||
                snapshot_.fatal ||
                input.meta.ingress_sequence !=
                    snapshot_.append
                            .ingress_sequence +
                        1U) {
                return false;
            }
            snapshot_.append.global_wal_pos +=
                wire.size();
            snapshot_.append.segment_offset +=
                wire.size();
            snapshot_.append.ingress_sequence =
                input.meta.ingress_sequence;
            appended_.push_back(std::move(wire));
            return true;
        } catch (...) {
            return false;
        }
    }

    bool FlushDurable() noexcept override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!snapshot_.initialized ||
            snapshot_.sealed ||
            snapshot_.fatal) {
            return false;
        }
        snapshot_.durable = snapshot_.append;
        return true;
    }

    bool SealAndClose() noexcept override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!snapshot_.initialized ||
            snapshot_.fatal) {
            return false;
        }
        snapshot_.durable = snapshot_.append;
        snapshot_.sealed = true;
        snapshot_.closed = true;
        return true;
    }

    ingress::RawWalWriterSnapshot Snapshot()
        const noexcept override {
        std::lock_guard<std::mutex> lock(mutex_);
        return snapshot_;
    }

    ingress::RawWalFailure failure()
        const noexcept override {
        return {};
    }

    ingress::RawWalSinkIdentityV1 identity()
        const noexcept override {
        return identity_;
    }

    std::vector<std::vector<std::byte>>
    appended_records() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return appended_;
    }

private:
    mutable std::mutex mutex_;
    ingress::RawWalSinkIdentityV1 identity_{};
    ingress::RawWalWriterSnapshot snapshot_{};
    std::vector<std::vector<std::byte>> appended_;
};

class EmptyLiveSource final
    : public ingress::RawLiveTailSource {
public:
    int ReadControl(
        ingress::RawControlSnapshot* output,
        std::uint64_t* generation) noexcept override {
        if (output == nullptr || generation == nullptr) {
            return EINVAL;
        }
        *output = control;
        *generation = 2U;
        return 0;
    }

    int InspectSegment(
        std::uint32_t sequence,
        ingress::RawLiveSegmentInfo* output)
        noexcept override {
        if (output == nullptr ||
            sequence != segment.header.segment_sequence) {
            return ENOENT;
        }
        *output = segment;
        return 0;
    }

    ingress::RawLiveReadResult ReadSegmentSome(
        std::uint32_t,
        std::uint64_t,
        std::span<std::byte>) noexcept override {
        return {0U, EIO};
    }

    ingress::RawControlSnapshot control{};
    ingress::RawLiveSegmentInfo segment{};
};

struct SdkEvents final {
    std::vector<std::string> values;
    mdl::MessageHandlerBase* handler = nullptr;
    const mdl::MDLMessage* shutdown_message = nullptr;
    std::size_t shutdown_message_count = 1U;
    std::string server_address;
    mdl::MDLMessageEncoding message_encoding =
        mdl::MDLEID_UNDEFINED;
};

class ShutdownMessage final : public mdl::MDLMessage {
public:
    explicit ShutdownMessage(
        const sdk::MessageKey& key) {
        std::memset(&head_, 0, sizeof(head_));
        head_.HeadSize =
            static_cast<std::uint8_t>(
                ingress::kVendorMessageHeadBytes);
        head_.MessageSize =
            static_cast<std::uint32_t>(
                ingress::kVendorMessageHeadBytes);
        head_.MessageEncoding =
            static_cast<std::uint8_t>(
                mdl::MDLEID_BINARY);
        head_.ServiceID = key.service_id;
        head_.ServiceVersion =
            key.service_version;
        head_.MessageID = key.message_id;
        head_.SequenceID = 77U;
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }
    mdl::MDLMessageHead* GetHead() const override {
        return const_cast<mdl::MDLMessageHead*>(
            &head_);
    }
    char* GetBody() const override {
        return nullptr;
    }
    mdl::MDLMessage* _Copy() const override {
        return nullptr;
    }

private:
    mdl::MDLMessageHead head_{};
};

class FakeSubscriber final : public sdk::SdkSubscriber {
public:
    explicit FakeSubscriber(
        std::shared_ptr<SdkEvents> events)
        : events_(std::move(events)) {}

    void SetServerAddress(std::string_view value) override {
        events_->values.emplace_back("set-address");
        events_->server_address = value;
    }
    void SetUserName(std::string_view) override {
        events_->values.emplace_back("set-user");
    }
    void SetHeartbeatInterval(
        std::uint32_t) override {}
    void SetHeartbeatTimeout(
        std::uint32_t) override {}
    void SetMessageEncoding(
        mdl::MDLMessageEncoding value) override {
        events_->message_encoding = value;
    }
    void EnableMergeMessage(bool) override {}
    void SetSendMacAuth(bool) override {}
    void EnableServerSelect(bool) override {}
    void AddSubscription(
        const sdk::MessageKey&) override {
        events_->values.emplace_back("subscribe");
    }
    std::string Connect() override {
        events_->values.emplace_back("connect");
        return {};
    }
    bool Release(std::string*) noexcept override {
        events_->values.emplace_back(
            "subscriber-release");
        return true;
    }

private:
    std::shared_ptr<SdkEvents> events_;
};

class FakeManager final : public sdk::SdkManager {
public:
    explicit FakeManager(
        std::shared_ptr<SdkEvents> events)
        : events_(std::move(events)) {}

    void EnableLog(
        std::string_view, bool) override {
        events_->values.emplace_back("enable-log");
    }
    std::unique_ptr<sdk::SdkSubscriber>
    CreateSubscriber(
        mdl::MessageHandlerBase* handler,
        bool multithread) override {
        events_->values.emplace_back(
            "create-subscriber");
        events_->handler = handler;
        if (multithread) {
            return nullptr;
        }
        return std::make_unique<FakeSubscriber>(
            events_);
    }
    void Shutdown() override {
        events_->values.emplace_back("shutdown");
        if (events_->handler != nullptr &&
            events_->shutdown_message != nullptr) {
            for (std::size_t index = 0U;
                 index <
                     events_->shutdown_message_count;
                 ++index) {
                events_->handler->OnMessage(
                    nullptr,
                    events_->shutdown_message);
            }
        }
    }
    bool Release(std::string*) noexcept override {
        events_->values.emplace_back(
            "manager-release");
        return true;
    }

private:
    std::shared_ptr<SdkEvents> events_;
};

class FakeFactory final : public sdk::SdkFactory {
public:
    explicit FakeFactory(
        std::shared_ptr<SdkEvents> events)
        : events_(std::move(events)) {}

    std::unique_ptr<sdk::SdkManager> Create(
        int, int) override {
        ++create_calls;
        events_->values.emplace_back("create-manager");
        return std::make_unique<FakeManager>(
            events_);
    }

    std::uint64_t create_calls = 0U;

private:
    std::shared_ptr<SdkEvents> events_;
};

class RecordingCleanStopGate final
    : public ingress::RawIngressCleanStopGateV1 {
public:
    bool Complete(
        const ingress::RawIngressCleanStopEvidenceV1&
            evidence) noexcept override {
        ++calls;
        last_evidence = evidence;
        accepted =
            evidence.exact() &&
            evidence.final_wal.sealed &&
            evidence.final_wal.closed &&
            evidence.final_wal.append ==
                evidence.final_wal.durable &&
            evidence.reconciliation.exact() &&
            evidence.observer
                    .observer_processed_wal_pos ==
                evidence.final_wal.append
                    .global_wal_pos;
        return accepted;
    }

    std::uint64_t calls = 0U;
    bool accepted = false;
    ingress::RawIngressCleanStopEvidenceV1
        last_evidence{};
};

class LifecycleRecorder final
    : public ingress::RawIngressLifecycleObserver {
public:
    void Observe(
        ingress::RawIngressLifecycleEvent event)
        noexcept override {
        events.push_back(event);
    }

    std::vector<ingress::RawIngressLifecycleEvent>
        events;
};

ingress::RawIngressAppConfigV1 MakeConfig() {
    ingress::RawIngressAppConfigV1 result;
    result.stable =
        ingress::DefaultRawIngressConfig(
            sdk::IngressKind::ShTick);
    const std::string endpoint_bytes =
        "{\"schema_version\":1,"
        "\"ingress_kind\":\"sh-tick\","
        "\"name\":\"sh-tick-test\","
        "\"resolved_server_address\":\"127.0.0.1:12345\","
        "\"message_encoding\":1,"
        "\"merge_message\":false,"
        "\"send_mac_auth\":false,"
        "\"server_select\":false}";
    result.stable.endpoint_contract_sha256 =
        l2flow::common::Sha256Hex(
            l2flow::common::ComputeSha256(
                endpoint_bytes));
    result.stable.credential_name = "mdl-token";
    result.stable.sdk_log_prefix =
        "/var/log/l2flow/sh-tick";
    result.stable.metrics_textfile_path =
        "/run/l2flow/sh-tick.prom";
    result.stable.max_message_bytes =
        ingress::kVendorMessageHeadBytes;
    result.stable.ring_capacity_bytes = 4096U;
    result.stable.raw_root = "/data/l2flow/raw";
    result.stable.segment_target_bytes = 8192U;
    result.stable.sync_bytes = 1024U;
    result.stable.sparse_index_every_bytes = 1024U;
    result.stable.reserve_domain_id = "raw-test";
    result.stable.reserve_coordinator_socket =
        "/run/l2flow/reserve.sock";
    result.stable.emergency_reserve_bytes = 4096U;
    result.stable.canonical_clock_source_config =
        "CLOCK_MONOTONIC_RAW+CLOCK_REALTIME";

    std::string endpoint_error;
    result.endpoint =
        sdk::VerifyEndpointContractBytes(
            endpoint_bytes,
            result.stable.endpoint_contract_sha256,
            result.stable.kind,
            &endpoint_error);
    if (result.endpoint == nullptr) {
        throw std::runtime_error(
            "cannot verify endpoint fixture: " +
            endpoint_error);
    }
    result.credential_token = "secret";

    const sdk::IngressSpec& spec =
        sdk::GetIngressSpec(result.stable.kind);
    result.recovered.source_stream_id =
        spec.source_stream_id;
    result.recovered.capture_date = 20260718U;
    result.recovered.stream_day_id =
        Pattern<16U>(0x10U);
    result.recovered.recovered_next_ingress_sequence =
        1U;
    result.recovered.append = {
        .segment_sequence = 1U,
        .global_wal_pos =
            ingress::kRawV1SegmentHeaderBytes,
        .ingress_sequence = 0U,
        .segment_offset =
            ingress::kRawV1SegmentHeaderBytes,
    };
    result.recovered.durable =
        result.recovered.append;
    result.recovered.writer_instance =
        Pattern<16U>(0x30U);
    result.recovered.current_segment_sequence = 1U;
    result.recovered.clock_epoch_algorithm_version =
        result.stable.clock_epoch_algorithm_version;
    result.recovered.clock_epoch_digest =
        Pattern<32U>(0x50U);
    result.recovered.clock_epoch_label =
        DigestLabel(
            result.recovered.clock_epoch_digest);
    result.connect_generation = 1U;
    return result;
}

std::unique_ptr<ingress::RawLiveTail> MakeTail(
    const ingress::RawIngressAppConfigV1& config,
    EmptyLiveSource* source) {
    source->control.writer_instance =
        config.recovered.writer_instance;
    source->control.stream_day_id =
        config.recovered.stream_day_id;
    source->control.source_stream_id =
        config.recovered.source_stream_id;
    source->control.capture_date =
        config.recovered.capture_date;
    source->control.segment_sequence =
        config.recovered.current_segment_sequence;
    source->control.append_global_wal_pos =
        config.recovered.append.global_wal_pos;
    source->control.append_ingress_sequence =
        config.recovered.append.ingress_sequence;
    source->control.append_segment_offset =
        config.recovered.append.segment_offset;
    source->control.durable_global_wal_pos =
        config.recovered.durable.global_wal_pos;
    source->control.durable_ingress_sequence =
        config.recovered.durable.ingress_sequence;
    source->control.durable_segment_offset =
        config.recovered.durable.segment_offset;
    source->control.clock_epoch_label =
        config.recovered.clock_epoch_label;
    source->control.heartbeat_monotonic_ns = 1U;

    source->segment.header.source_stream_id =
        config.recovered.source_stream_id;
    source->segment.header.capture_date =
        config.recovered.capture_date;
    source->segment.header.stream_day_id =
        config.recovered.stream_day_id;
    source->segment.header.segment_sequence =
        config.recovered.current_segment_sequence;
    source->segment.header.segment_base_wal_pos =
        config.recovered.append.global_wal_pos -
        config.recovered.append.segment_offset;
    source->segment.header.first_ingress_sequence = 1U;
    source->segment.visible_end_offset =
        config.recovered.append.segment_offset;

    ingress::RawLiveTailAttachV1 attach;
    attach.writer_instance =
        config.recovered.writer_instance;
    attach.stream_day_id =
        config.recovered.stream_day_id;
    attach.source_stream_id =
        config.recovered.source_stream_id;
    attach.capture_date =
        config.recovered.capture_date;
    attach.segment_sequence =
        config.recovered.current_segment_sequence;
    attach.global_wal_pos =
        config.recovered.append.global_wal_pos;
    attach.segment_offset =
        config.recovered.append.segment_offset;
    attach.next_ingress_sequence =
        config.recovered.recovered_next_ingress_sequence;
    std::unique_ptr<ingress::RawLiveTail> tail;
    if (ingress::RawLiveTail::Attach(
            source, attach, &tail) !=
        ingress::RawLiveTailError::kNone) {
        return nullptr;
    }
    return tail;
}

void TestOrderedEmptyRuntime(TestContext* test) {
    ingress::RawIngressAppConfigV1 config =
        MakeConfig();
    test->Expect(
        ingress::ValidateRawIngressConfig(
            config.stable).empty() &&
            ingress::ValidateRawIngressRuntimeState(
                config.stable,
                config.recovered).empty(),
        "test runtime inputs satisfy typed Phase-2 validation");

    EmptyLiveSource live_source;
    auto tail = MakeTail(config, &live_source);
    auto sdk_events = std::make_shared<SdkEvents>();
    auto factory =
        std::make_shared<FakeFactory>(sdk_events);
    auto clean_gate =
        std::make_unique<RecordingCleanStopGate>();
    RecordingCleanStopGate* const clean_gate_view =
        clean_gate.get();
    LifecycleRecorder lifecycle;

    ingress::RawIngressApp app(
        config,
        factory,
        std::make_unique<FixedClock>(),
        std::make_unique<EmptyPreparedSink>(
            config.recovered, true),
        std::move(tail),
        std::move(clean_gate),
        {},
        &lifecycle);
    std::string error;
    test->Expect(
        app.Initialize(&error) &&
            app.state() ==
                ingress::RawIngressAppState::kRunning &&
            !app.fatal(),
        "prepared Raw runtime reaches SDK Running");
    test->Expect(
        app.Stop(&error) &&
            app.state() ==
                ingress::RawIngressAppState::kStopped &&
            !app.fatal(),
        "empty Raw runtime stops with exact sealed evidence");
    test->Expect(
        clean_gate_view->calls == 1U &&
            clean_gate_view->accepted &&
            app.reconciliation().exact(),
        "certificate/coordinator gate receives exact final evidence once");
    const std::string metrics =
        app.prometheus_metrics();
    test->Expect(
        metrics.find(
            "l2flow_raw_callback_records_total 0\n") !=
                std::string::npos &&
            metrics.find(
                "l2flow_raw_append_global_wal_pos 4096\n") !=
                std::string::npos &&
            metrics.find(
                "l2flow_raw_durable_global_wal_pos 4096\n") !=
                std::string::npos &&
            metrics.find(
                "l2flow_raw_reconciliation_exact 1\n") !=
                std::string::npos &&
            metrics.find(config.credential_token) ==
                std::string::npos &&
            metrics.find(
                config.endpoint
                    ->resolved_server_address()) ==
                std::string::npos &&
            metrics.find(config.stable.raw_root) ==
                std::string::npos,
        "Raw metrics expose exact progress without credentials, endpoint address, or paths");

    const std::vector<
        ingress::RawIngressLifecycleEvent> expected{
        ingress::RawIngressLifecycleEvent::
            kCaptureWorkerStarted,
        ingress::RawIngressLifecycleEvent::
            kReadinessWorkerStarted,
        ingress::RawIngressLifecycleEvent::
            kSdkConnectStarting,
        ingress::RawIngressLifecycleEvent::
            kHandlerBeginStopping,
        ingress::RawIngressLifecycleEvent::
            kHandlerQuiesced,
        ingress::RawIngressLifecycleEvent::
            kCaptureWorkerJoined,
        ingress::RawIngressLifecycleEvent::
            kReadinessWorkerJoined,
        ingress::RawIngressLifecycleEvent::
            kCleanStopBarrierComplete,
    };
    test->Expect(
        lifecycle.events == expected,
        "workers precede Connect and both catch-up barriers precede clean-stop gate");
    test->Expect(
        !sdk_events->values.empty() &&
            sdk_events->values.back() ==
                "manager-release" &&
            sdk_events->handler != nullptr &&
            sdk_events->server_address ==
                config.endpoint
                    ->resolved_server_address() &&
            sdk_events->message_encoding ==
                config.endpoint
                    ->message_encoding(),
        "SDK consumes immutable verified endpoint fields and releases only after ordered Raw shutdown");
}

void TestRecoveredEmptyRuntimePreservesIngressCursor(
    TestContext* test) {
    ingress::RawIngressAppConfigV1 config =
        MakeConfig();
    constexpr std::uint64_t kRecoveredRecords = 7U;
    ingress::RawRecordLayoutV1 recovered_record{};
    const bool recovered_layout_valid =
        ingress::ComputeRawRecordLayoutV1(
            0U, &recovered_record) ==
        ingress::RawV1Error::kNone;
    const std::uint64_t recovered_record_bytes =
        recovered_record.record_size * kRecoveredRecords;
    config.recovered.append.global_wal_pos +=
        recovered_record_bytes;
    config.recovered.append.segment_offset +=
        recovered_record_bytes;
    config.recovered.append.ingress_sequence =
        kRecoveredRecords;
    config.recovered.durable =
        config.recovered.append;
    config.recovered.recovered_next_ingress_sequence =
        kRecoveredRecords + 1U;
    test->Expect(
        recovered_layout_valid &&
            ingress::ValidateRawIngressRuntimeState(
            config.stable,
            config.recovered).empty(),
        "recovered empty-stop fixture has a valid nonzero Raw cursor");

    EmptyLiveSource live_source;
    auto tail = MakeTail(config, &live_source);
    auto clean_gate =
        std::make_unique<RecordingCleanStopGate>();
    RecordingCleanStopGate* const clean_gate_view =
        clean_gate.get();
    ingress::RawIngressApp app(
        config,
        std::make_shared<FakeFactory>(
            std::make_shared<SdkEvents>()),
        std::make_unique<FixedClock>(),
        std::make_unique<EmptyPreparedSink>(
            config.recovered, true),
        std::move(tail),
        std::move(clean_gate));
    std::string error;
    test->Expect(
        app.Initialize(&error) && app.Stop(&error) &&
            !app.fatal(),
        "recovered runtime with no new callbacks completes a clean stop");
    test->Expect(
        clean_gate_view->calls == 1U &&
            clean_gate_view->accepted &&
            clean_gate_view->last_evidence
                    .callback.captured_records == 0U &&
            clean_gate_view->last_evidence
                    .callback.captured_ingress_sequence ==
                kRecoveredRecords &&
            clean_gate_view->last_evidence
                    .capture.append.records == 0U &&
            clean_gate_view->last_evidence
                    .capture.durable.records == 0U &&
            clean_gate_view->last_evidence
                    .capture.append.last_ingress_sequence ==
                kRecoveredRecords &&
            clean_gate_view->last_evidence
                    .capture.durable.last_ingress_sequence ==
                kRecoveredRecords &&
            clean_gate_view->last_evidence
                    .final_wal.append.ingress_sequence ==
                kRecoveredRecords,
        "empty-generation evidence keeps the recovered absolute cursor while worker record counts remain incremental");
}

void TestStableSyncPolicyReachesCaptureWorker(
    TestContext* test) {
    const auto RunCase =
        [test](
            std::uint32_t sync_interval_milliseconds,
            std::uint64_t sync_bytes,
            bool advance_clock,
            std::string_view description) {
            ingress::RawIngressAppConfigV1 config =
                MakeConfig();
            config.stable.sync_interval_milliseconds =
                sync_interval_milliseconds;
            config.stable.sync_bytes = sync_bytes;
            EmptyLiveSource live_source;
            auto tail = MakeTail(config, &live_source);
            auto sdk_events =
                std::make_shared<SdkEvents>();
            auto factory =
                std::make_shared<FakeFactory>(
                    sdk_events);
            std::atomic<std::uint64_t>
                worker_clock{100U};
            ingress::RawIngressAppOptionsV1 options;
            options.worker_monotonic_now =
                &AtomicWorkerClock;
            options.worker_monotonic_clock_context =
                &worker_clock;
            ingress::RawIngressApp app(
                config,
                factory,
                std::make_unique<FixedClock>(),
                std::make_unique<
                    EmptyPreparedSink>(
                    config.recovered, true),
                std::move(tail),
                std::make_unique<
                    RecordingCleanStopGate>(),
                options);
            std::string error;
            bool observed_append = false;
            bool observed_durable = false;
            if (app.Initialize(&error)) {
                const sdk::IngressSpec& spec =
                    sdk::GetIngressSpec(
                        config.stable.kind);
                ShutdownMessage message(
                    spec.required.front());
                sdk_events->handler->OnMessage(
                    nullptr, &message);
                for (std::size_t attempt = 0U;
                     attempt < 1000U;
                     ++attempt) {
                    const auto snapshot =
                        app.capture_snapshot();
                    if (snapshot.append.records ==
                        1U) {
                        observed_append = true;
                        break;
                    }
                    std::this_thread::sleep_for(
                        std::chrono::
                            milliseconds(1));
                }
                if (advance_clock) {
                    worker_clock.store(
                        UINT64_C(1'000'100),
                        std::memory_order_release);
                }
                for (std::size_t attempt = 0U;
                     attempt < 1000U;
                     ++attempt) {
                    if (app.capture_snapshot()
                            .durable.records == 1U) {
                        observed_durable = true;
                        break;
                    }
                    std::this_thread::sleep_for(
                        std::chrono::
                            milliseconds(1));
                }
            }
            test->Expect(
                observed_append &&
                    observed_durable,
                description);
            auto ack =
                app.BeginEmergencyStop(nullptr);
            test->Expect(
                ack != nullptr && ack->valid(),
                "sync-policy fixture remains internally exact before emergency teardown");
            static_cast<void>(
                app.Stop(nullptr));
        };

    // The injected clock is fixed, so only the one-byte batch threshold can
    // make the record durable. The worker's 4 MiB default would leave it
    // append-only.
    RunCase(
        60'000U,
        1U,
        false,
        "stable sync_bytes is applied to the live Raw capture worker");

    // The batch threshold is larger than this record. Advancing the injected
    // clock by exactly 1 ms proves the configured interval is used; the
    // worker's 10 ms default would not flush.
    RunCase(
        1U,
        4096U,
        true,
        "stable sync_interval_milliseconds is applied to the live Raw capture worker");
}

void TestCursorMismatchBeforeSdk(TestContext* test) {
    ingress::RawIngressAppConfigV1 config =
        MakeConfig();
    EmptyLiveSource live_source;
    auto tail = MakeTail(config, &live_source);
    auto sdk_events = std::make_shared<SdkEvents>();
    auto factory =
        std::make_shared<FakeFactory>(sdk_events);
    auto clean_gate =
        std::make_unique<RecordingCleanStopGate>();
    RecordingCleanStopGate* const clean_gate_view =
        clean_gate.get();

    ingress::RawIngressApp app(
        config,
        factory,
        std::make_unique<FixedClock>(),
        std::make_unique<EmptyPreparedSink>(
            config.recovered, false),
        std::move(tail),
        std::move(clean_gate));
    std::string error;
    test->Expect(
        !app.Initialize(&error) &&
            app.fatal() &&
            factory->create_calls == 0U &&
            clean_gate_view->calls == 0U,
        "recovery/sink cursor mismatch is rejected before SDK construction");
}

void TestTailIdentityMismatchRejected(
    TestContext* test) {
    ingress::RawIngressAppConfigV1 config =
        MakeConfig();
    ingress::RawIngressAppConfigV1 foreign =
        config;
    foreign.recovered.writer_instance[0U] ^=
        std::byte{0x7fU};
    EmptyLiveSource live_source;
    auto tail = MakeTail(foreign, &live_source);
    auto sdk_events = std::make_shared<SdkEvents>();
    auto factory =
        std::make_shared<FakeFactory>(sdk_events);
    bool rejected = false;
    try {
        ingress::RawIngressApp app(
            config,
            factory,
            std::make_unique<FixedClock>(),
            std::make_unique<EmptyPreparedSink>(
                config.recovered, true),
            std::move(tail),
            std::make_unique<
                RecordingCleanStopGate>());
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    test->Expect(
        rejected &&
            factory->create_calls == 0U,
        "foreign writer/stream live tail is rejected before workers or SDK construction");
}

void TestSinkIdentityMismatchBeforeSdk(
    TestContext* test) {
    ingress::RawIngressAppConfigV1 config =
        MakeConfig();
    ingress::RawIngressRuntimeState foreign =
        config.recovered;
    foreign.stream_day_id[0U] ^=
        std::byte{0x55U};
    EmptyLiveSource live_source;
    auto tail = MakeTail(config, &live_source);
    auto sdk_events = std::make_shared<SdkEvents>();
    auto factory =
        std::make_shared<FakeFactory>(sdk_events);
    ingress::RawIngressApp app(
        config,
        factory,
        std::make_unique<FixedClock>(),
        std::make_unique<EmptyPreparedSink>(
            foreign, true),
        std::move(tail),
        std::make_unique<RecordingCleanStopGate>());
    std::string error;
    test->Expect(
        !app.Initialize(&error) &&
            app.fatal() &&
            factory->create_calls == 0U,
        "foreign prepared sink with the same numeric cursor is rejected before SDK construction");
}

void TestUnreachableFinalCursorTimesOut(
    TestContext* test) {
    ingress::RawIngressAppConfigV1 config =
        MakeConfig();
    EmptyLiveSource live_source;
    auto tail = MakeTail(config, &live_source);
    auto sdk_events = std::make_shared<SdkEvents>();
    auto factory =
        std::make_shared<FakeFactory>(sdk_events);
    auto clean_gate =
        std::make_unique<RecordingCleanStopGate>();
    RecordingCleanStopGate* const clean_gate_view =
        clean_gate.get();
    ingress::RawIngressAppOptionsV1 options;
    options.observer_final_catch_up_timeout_ns = 1U;

    ingress::RawIngressApp app(
        config,
        factory,
        std::make_unique<FixedClock>(),
        std::make_unique<EmptyPreparedSink>(
            config.recovered, true, true),
        std::move(tail),
        std::move(clean_gate),
        options);
    std::string error;
    test->Expect(
        app.Initialize(&error),
        "unreachable final cursor fixture reaches Running before its sink advances");
    test->Expect(
        !app.Stop(&error) &&
            app.state() ==
                ingress::RawIngressAppState::kStopped &&
            app.fatal() &&
            clean_gate_view->calls == 0U,
        "RawIngressApp final readiness join terminates on configured catch-up timeout");
}

void TestEndpointCapabilityCannotBeSubstituted(
    TestContext* test) {
    static_assert(
        !std::is_copy_constructible_v<
            sdk::VerifiedEndpointContract>);
    ingress::RawIngressAppConfigV1 config =
        MakeConfig();
    sdk::EndpointContract mutable_copy =
        config.endpoint->CopyValue();
    mutable_copy.resolved_server_address =
        "foreign.invalid:1";
    mutable_copy.message_encoding =
        mdl::MDLEID_MKTPRO;
    test->Expect(
        config.endpoint->resolved_server_address() ==
                "127.0.0.1:12345" &&
            config.endpoint->message_encoding() ==
                mdl::MDLEID_BINARY,
        "mutating a compatibility copy cannot substitute fields behind the verified hash");

    const std::string foreign_bytes =
        "{\"schema_version\":1,"
        "\"ingress_kind\":\"sh-tick\","
        "\"name\":\"foreign\","
        "\"resolved_server_address\":\"foreign.invalid:1\","
        "\"message_encoding\":7,"
        "\"merge_message\":true,"
        "\"send_mac_auth\":true,"
        "\"server_select\":true}";
    const std::string foreign_sha =
        l2flow::common::Sha256Hex(
            l2flow::common::ComputeSha256(
                foreign_bytes));
    std::string verify_error;
    auto foreign =
        sdk::VerifyEndpointContractBytes(
            foreign_bytes,
            foreign_sha,
            sdk::IngressKind::ShTick,
            &verify_error);
    ingress::RawIngressAppConfigV1 mismatched =
        config;
    mismatched.endpoint = std::move(foreign);
    EmptyLiveSource live_source;
    auto tail = MakeTail(mismatched, &live_source);
    bool rejected = false;
    try {
        ingress::RawIngressApp app(
            mismatched,
            std::make_shared<FakeFactory>(
                std::make_shared<SdkEvents>()),
            std::make_unique<FixedClock>(),
            std::make_unique<EmptyPreparedSink>(
                mismatched.recovered, true),
            std::move(tail),
            std::make_unique<
                RecordingCleanStopGate>());
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    test->Expect(
        rejected,
        "foreign verified bytes cannot be paired with the stable endpoint hash");

    const std::string wrong_kind_bytes =
        "{\"schema_version\":1,"
        "\"ingress_kind\":\"sh-snapshot\","
        "\"name\":\"wrong-kind\","
        "\"resolved_server_address\":\"127.0.0.1:12345\","
        "\"message_encoding\":1,"
        "\"merge_message\":false,"
        "\"send_mac_auth\":false,"
        "\"server_select\":false}";
    const std::string wrong_kind_sha =
        l2flow::common::Sha256Hex(
            l2flow::common::ComputeSha256(
                wrong_kind_bytes));
    auto wrong_kind =
        sdk::VerifyEndpointContractBytes(
            wrong_kind_bytes,
            wrong_kind_sha,
            sdk::IngressKind::ShSnapshot,
            &verify_error);
    ingress::RawIngressAppConfigV1 wrong_kind_config =
        config;
    wrong_kind_config.stable
        .endpoint_contract_sha256 = wrong_kind_sha;
    wrong_kind_config.endpoint =
        std::move(wrong_kind);
    EmptyLiveSource wrong_kind_source;
    auto wrong_kind_tail =
        MakeTail(wrong_kind_config, &wrong_kind_source);
    rejected = false;
    try {
        ingress::RawIngressApp app(
            wrong_kind_config,
            std::make_shared<FakeFactory>(
                std::make_shared<SdkEvents>()),
            std::make_unique<FixedClock>(),
            std::make_unique<EmptyPreparedSink>(
                wrong_kind_config.recovered, true),
            std::move(wrong_kind_tail),
            std::make_unique<
                RecordingCleanStopGate>());
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    test->Expect(
        rejected,
        "verified endpoint ingress kind must match the Raw service kind even when its own hash matches");
}

void TestNeverInitializedStopIsNotClean(
    TestContext* test) {
    ingress::RawIngressAppConfigV1 config =
        MakeConfig();
    EmptyLiveSource live_source;
    auto tail = MakeTail(config, &live_source);
    auto sdk_events = std::make_shared<SdkEvents>();
    auto factory =
        std::make_shared<FakeFactory>(sdk_events);
    auto clean_gate =
        std::make_unique<RecordingCleanStopGate>();
    RecordingCleanStopGate* const clean_gate_view =
        clean_gate.get();
    ingress::RawIngressApp app(
        config,
        factory,
        std::make_unique<FixedClock>(),
        std::make_unique<EmptyPreparedSink>(
            config.recovered, true),
        std::move(tail),
        std::move(clean_gate));
    std::string error;
    test->Expect(
        !app.Stop(&error) &&
            app.state() ==
                ingress::RawIngressAppState::kStopped &&
            app.fatal() &&
            error.find("before initialization") !=
                std::string::npos &&
            clean_gate_view->calls == 0U &&
            factory->create_calls == 0U,
        "explicit Stop before Initialize is disposed-not-started, never a clean stop");
}

void TestEmergencyWriterAckLifecycle(
    TestContext* test) {
    static_assert(
        !std::is_default_constructible_v<
            ingress::RawEmergencyWriterAckV1>);
    static_assert(
        !std::is_copy_constructible_v<
            ingress::RawEmergencyWriterAckV1>);
    static_assert(
        !std::is_move_constructible_v<
            ingress::RawEmergencyWriterAckV1>);

    ingress::RawIngressAppConfigV1 config =
        MakeConfig();
    EmptyLiveSource live_source;
    auto tail = MakeTail(config, &live_source);
    auto sdk_events = std::make_shared<SdkEvents>();
    auto factory =
        std::make_shared<FakeFactory>(sdk_events);
    auto clean_gate =
        std::make_unique<RecordingCleanStopGate>();
    RecordingCleanStopGate* const clean_gate_view =
        clean_gate.get();
    LifecycleRecorder lifecycle;
    ingress::RawIngressApp app(
        config,
        factory,
        std::make_unique<FixedClock>(),
        std::make_unique<EmptyPreparedSink>(
            config.recovered, true),
        std::move(tail),
        std::move(clean_gate),
        {},
        &lifecycle);
    std::string error;
    test->Expect(
        app.Initialize(&error),
        "emergency ACK fixture reaches Running");
    const sdk::IngressSpec& spec =
        sdk::GetIngressSpec(config.stable.kind);
    ShutdownMessage shutdown_message(
        spec.required.front());
    sdk_events->shutdown_message =
        &shutdown_message;

    std::unique_ptr<ingress::RawEmergencyWriterAckV1>
        ack = app.BeginEmergencyStop(&error);
    test->Expect(
        ack != nullptr && ack->valid() &&
            ack->facts().exact() &&
            app.state() ==
                ingress::RawIngressAppState::kStopping &&
            !app.fatal(),
        "Running app freezes one exact live emergency ACK");
    if (ack != nullptr) {
        const auto& facts = ack->facts();
        ingress::RawRecordLayoutV1 layout{};
        const bool layout_valid =
            ingress::ComputeRawRecordLayoutV1(
                0U, &layout) ==
            ingress::RawV1Error::kNone;
        test->Expect(
            facts.writer.writer_instance ==
                    config.recovered.writer_instance &&
                facts.writer.stream_day_id ==
                    config.recovered.stream_day_id &&
                facts.writer.source_stream_id ==
                    config.recovered.source_stream_id &&
                facts.writer.capture_date ==
                    config.recovered.capture_date &&
                facts.sdk_shutdown_returned &&
                facts.callback_quiesced &&
                !facts.callback.callback_inflight &&
                facts.regular_writer_mutation_stopped &&
                facts.capture.emergency_paused &&
                facts.same_process_ring_suffix_retained &&
                layout_valid &&
                facts.callback.captured_records == 1U &&
                facts.callback.captured_vendor_bytes ==
                    ingress::kVendorMessageHeadBytes &&
                facts.callback
                        .captured_framed_wal_bytes ==
                    layout.record_size &&
                facts.callback.callbacks_after_stop ==
                    0U &&
                facts.capture.append.records +
                        facts.queued_record_count ==
                    1U &&
                facts.capture.append.framed_wal_bytes +
                        facts.queued_framed_wal_bytes ==
                    layout.record_size &&
                (facts.queued_record_count == 0U) ==
                    (facts.ring_used_bytes == 0U) &&
                facts.wal.append.ingress_sequence ==
                    facts.capture.append
                        .last_ingress_sequence &&
                facts.wal.durable.ingress_sequence ==
                    facts.capture.durable
                        .last_ingress_sequence,
            "ACK snapshot freezes exact identity, callback, WAL cursor, queue, and live ring ownership facts");
    }

    std::unique_ptr<ingress::RawEmergencyWriterAckV1>
        duplicate = app.BeginEmergencyStop(&error);
    test->Expect(
        duplicate == nullptr && ack != nullptr &&
            ack->valid() && !app.fatal(),
        "a second emergency ACK request is rejected without replacing the first capability");

    test->Expect(
        !app.Stop(&error) &&
            app.state() ==
                ingress::RawIngressAppState::kStopped &&
            app.fatal() &&
            ack != nullptr && !ack->valid() &&
            clean_gate_view->calls == 0U &&
            std::count(
                sdk_events->values.begin(),
                sdk_events->values.end(),
                "shutdown") == 1,
        "Stop after emergency ACK neither double-Shutdowns nor misreports the emergency path as a normal clean stop");

    const std::vector<
        ingress::RawIngressLifecycleEvent>
        prefix{
            ingress::RawIngressLifecycleEvent::
                kCaptureWorkerStarted,
            ingress::RawIngressLifecycleEvent::
                kReadinessWorkerStarted,
            ingress::RawIngressLifecycleEvent::
                kSdkConnectStarting,
            ingress::RawIngressLifecycleEvent::
                kHandlerBeginStopping,
            ingress::RawIngressLifecycleEvent::
                kHandlerQuiesced,
            ingress::RawIngressLifecycleEvent::
                kEmergencyWriterPaused,
            ingress::RawIngressLifecycleEvent::
                kEmergencyWriterAckFrozen,
        };
    test->Expect(
        lifecycle.events.size() >= prefix.size() &&
            std::equal(
                prefix.begin(),
                prefix.end(),
                lifecycle.events.begin()),
        "STOPPING and SDK/callback barriers precede writer pause and ACK publication");
}

void TestEmergencyWriterAckRejectsNeverInitialized(
    TestContext* test) {
    ingress::RawIngressAppConfigV1 config =
        MakeConfig();
    EmptyLiveSource live_source;
    auto tail = MakeTail(config, &live_source);
    auto factory = std::make_shared<FakeFactory>(
        std::make_shared<SdkEvents>());
    ingress::RawIngressApp app(
        config,
        factory,
        std::make_unique<FixedClock>(),
        std::make_unique<EmptyPreparedSink>(
            config.recovered, true),
        std::move(tail),
        std::make_unique<RecordingCleanStopGate>());
    std::string error;
    const auto ack = app.BeginEmergencyStop(&error);
    test->Expect(
        ack == nullptr && app.fatal() &&
            app.state() ==
                ingress::RawIngressAppState::kConstructed &&
            factory->create_calls == 0U &&
            error.find("requires a Running app") !=
                std::string::npos,
        "never-initialized app cannot forge an emergency writer ACK");
}

void TestEmergencyWriterAckNegativeStates(
    TestContext* test) {
    {
        ingress::RawIngressAppConfigV1 config =
            MakeConfig();
        EmptyLiveSource live_source;
        auto tail = MakeTail(config, &live_source);
        auto events = std::make_shared<SdkEvents>();
        ingress::RawIngressApp app(
            config,
            std::make_shared<FakeFactory>(events),
            std::make_unique<FixedClock>(),
            std::make_unique<EmptyPreparedSink>(
                config.recovered, true),
            std::move(tail),
            std::make_unique<
                RecordingCleanStopGate>());
        std::string error;
        test->Expect(
            app.Initialize(&error),
            "fatal emergency negative fixture reaches Running");
        events->handler->OnMessage(nullptr, nullptr);
        const auto ack =
            app.BeginEmergencyStop(&error);
        test->Expect(
            ack == nullptr && app.fatal() &&
                app.state() ==
                    ingress::RawIngressAppState::kRunning,
            "fatal Running app cannot manufacture an ACK");
        static_cast<void>(app.Stop(nullptr));
    }

    {
        ingress::RawIngressAppConfigV1 config =
            MakeConfig();
        EmptyLiveSource live_source;
        auto tail = MakeTail(config, &live_source);
        auto sink =
            std::make_unique<EmptyPreparedSink>(
                config.recovered, true);
        EmptyPreparedSink* const sink_view =
            sink.get();
        ingress::RawIngressApp app(
            config,
            std::make_shared<FakeFactory>(
                std::make_shared<SdkEvents>()),
            std::make_unique<FixedClock>(),
            std::move(sink),
            std::move(tail),
            std::make_unique<
                RecordingCleanStopGate>());
        std::string error;
        test->Expect(
            app.Initialize(&error),
            "identity emergency negative fixture reaches Running");
        sink_view->MakeIdentityForeignForTest();
        const auto ack =
            app.BeginEmergencyStop(&error);
        test->Expect(
            ack == nullptr && app.fatal() &&
                error.find("identity/cursor mismatch") !=
                    std::string::npos,
            "foreign live writer identity is rejected before SDK Shutdown or ACK");
        static_cast<void>(app.Stop(nullptr));
    }

    {
        ingress::RawIngressAppConfigV1 config =
            MakeConfig();
        EmptyLiveSource live_source;
        auto tail = MakeTail(config, &live_source);
        ingress::RawIngressApp app(
            config,
            std::make_shared<FakeFactory>(
                std::make_shared<SdkEvents>()),
            std::make_unique<FixedClock>(),
            std::make_unique<EmptyPreparedSink>(
                config.recovered, true),
            std::move(tail),
            std::make_unique<
                RecordingCleanStopGate>());
        std::string error;
        test->Expect(
            app.Initialize(&error) &&
                app.Stop(&error),
            "post-Stop emergency negative fixture completes ordinary Stop");
        const auto ack =
            app.BeginEmergencyStop(&error);
        test->Expect(
            ack == nullptr && app.fatal() &&
                app.state() ==
                    ingress::RawIngressAppState::kStopped,
            "Stopped app cannot issue a later emergency ACK");
    }
}

#include "raw_finalization_continuation_e2e.inc"

}  // namespace

int main() {
    TestContext test;
    TestOrderedEmptyRuntime(&test);
    TestRecoveredEmptyRuntimePreservesIngressCursor(
        &test);
    TestStableSyncPolicyReachesCaptureWorker(
        &test);
    TestCursorMismatchBeforeSdk(&test);
    TestTailIdentityMismatchRejected(&test);
    TestSinkIdentityMismatchBeforeSdk(&test);
    TestUnreachableFinalCursorTimesOut(&test);
    TestEndpointCapabilityCannotBeSubstituted(&test);
    TestNeverInitializedStopIsNotClean(&test);
    TestEmergencyWriterAckLifecycle(&test);
    TestEmergencyWriterAckRejectsNeverInitialized(
        &test);
    TestEmergencyWriterAckNegativeStates(&test);
    TestFinalizationContinuationPosixE2e(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Raw ingress app test(s) failed\n";
        return 1;
    }
    std::cout << "Phase 2 Raw ingress app tests passed\n";
    return 0;
}
