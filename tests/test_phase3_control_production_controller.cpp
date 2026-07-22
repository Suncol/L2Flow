#include "l2flow/control/control_production_controller.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/raw_v1.h"

#include "mdl_shl2_msg.h"
#include "mdl_sys_msg.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
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
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace control = l2flow::control;
namespace ingress = l2flow::ingress;
namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sys = datayes::mdl::mdl_sys_msg;

namespace {

constexpr std::uint32_t kSourceStreamId = 1002U;
constexpr std::uint32_t kCaptureDate = 20260722U;
constexpr l2flow::sdk::MessageKey kRequired{4U, 101U, 24U};

struct TestContext final {
    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }
    int failures = 0;
};

template <std::size_t Size>
void Fill(std::array<std::byte, Size>* output, std::uint8_t seed) {
    for (std::size_t index = 0U; index < output->size(); ++index) {
        (*output)[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(seed + index));
    }
}

void StoreU16(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint16_t value) {
    bytes[offset] = static_cast<std::byte>(value & 0xffU);
    bytes[offset + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint32_t value) {
    for (std::size_t index = 0U; index < 4U; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
}

void StoreU64(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint64_t value) {
    for (std::size_t index = 0U; index < 8U; ++index) {
        bytes[offset + index] = static_cast<std::byte>(
            (value >> (index * 8U)) & 0xffU);
    }
}

ingress::RawV1VendorHead VendorHead(
    std::uint32_t body_size,
    std::uint8_t service_id,
    std::uint16_t service_version,
    std::uint16_t message_id,
    std::uint64_t sequence) {
    ingress::RawV1VendorHead head{};
    std::span<std::byte> bytes(head);
    head[0] = static_cast<std::byte>(ingress::kVendorMessageHeadBytes);
    StoreU32(
        bytes,
        1U,
        static_cast<std::uint32_t>(ingress::kVendorMessageHeadBytes) +
            body_size);
    head[5] = std::byte{1U};
    head[6] = static_cast<std::byte>(service_id);
    StoreU16(bytes, 7U, service_version);
    StoreU16(bytes, 9U, message_id);
    StoreU32(bytes, 11U, 123U);
    StoreU64(bytes, 15U, sequence);
    return head;
}

std::vector<std::byte> SuccessfulLogonBody() {
    std::vector<std::byte> body(48U, std::byte{0U});
    std::span<std::byte> bytes(body);
    StoreU32(bytes, 12U, 1U);
    StoreU32(bytes, 16U, 12U);
    StoreU32(bytes, 20U, 0U);
    StoreU32(bytes, 24U, kRequired.service_id);
    StoreU32(bytes, 28U, kRequired.service_version);
    StoreU32(bytes, 32U, 1U);
    StoreU32(bytes, 36U, 8U);
    StoreU32(bytes, 40U, kRequired.message_id);
    StoreU32(bytes, 44U, 0U);
    return body;
}

struct RecordSpec final {
    std::uint8_t service_id = 0U;
    std::uint16_t service_version = 0U;
    std::uint16_t message_id = 0U;
    std::vector<std::byte> body;
};

RecordSpec LogonRecord() {
    return {
        2U,
        101U,
        static_cast<std::uint16_t>(sys::LogonResponse::MessageID),
        SuccessfulLogonBody()};
}

RecordSpec MarketRecord() {
    return {
        4U,
        101U,
        24U,
        std::vector<std::byte>(sizeof(sh::NGTSTick), std::byte{0U})};
}

RecordSpec IrrelevantRecord() {
    return {4U, 101U, 999U, {}};
}

RecordSpec MalformedLogonRecord() {
    return {
        2U,
        101U,
        static_cast<std::uint16_t>(sys::LogonResponse::MessageID),
        {std::byte{0xffU}}};
}

class TestSource final : public ingress::RawLiveTailSource {
public:
    int ReadControl(
        ingress::RawControlSnapshot* output,
        std::uint64_t* generation) noexcept override {
        if (output == nullptr || generation == nullptr) {
            return EINVAL;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        *output = control;
        *generation = generation_;
        return 0;
    }

    int InspectSegment(
        std::uint32_t sequence,
        ingress::RawLiveSegmentInfo* output) noexcept override {
        if (output == nullptr || sequence != segment.segment_sequence) {
            return EINVAL;
        }
        output->header = segment;
        output->visible_end_offset = wire.size();
        output->sealed = false;
        return 0;
    }

    ingress::RawLiveReadResult ReadSegmentSome(
        std::uint32_t sequence,
        std::uint64_t offset,
        std::span<std::byte> output) noexcept override {
        if (sequence != segment.segment_sequence || offset > wire.size()) {
            return {0U, EINVAL};
        }
        const std::size_t begin = static_cast<std::size_t>(offset);
        const std::size_t amount =
            std::min(output.size(), wire.size() - begin);
        std::copy_n(wire.data() + begin, amount, output.data());
        return {amount, 0};
    }

    void PublishAll() noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        control.append_global_wal_pos = wire.size();
        control.append_segment_offset = wire.size();
        control.append_ingress_sequence = total_records;
        control.durable_global_wal_pos = control.append_global_wal_pos;
        control.durable_segment_offset = control.append_segment_offset;
        control.durable_ingress_sequence = control.append_ingress_sequence;
        control.heartbeat_monotonic_ns = 1000U;
        generation_ += 2U;
    }

    ingress::SegmentHeaderV1 segment{};
    ingress::RawControlSnapshot control{};
    std::vector<std::byte> wire;
    std::vector<std::uint64_t> record_end_offsets;
    std::uint64_t total_records = 0U;

private:
    mutable std::mutex mutex_;
    std::uint64_t generation_ = 2U;
};

struct BuiltRuntime final {
    std::unique_ptr<TestSource> source;
    ingress::RawProductionReplaySnapshotV1 replay;
    l2flow::common::Identity128 writer{};
};

BuiltRuntime BuildRuntime(
    std::span<const RecordSpec> records,
    std::size_t history_count) {
    if (history_count > records.size()) {
        throw std::runtime_error("history count exceeds records");
    }
    BuiltRuntime built;
    built.source = std::make_unique<TestSource>();
    ingress::SegmentHeaderV1& segment = built.source->segment;
    segment.source_stream_id = kSourceStreamId;
    segment.capture_date = kCaptureDate;
    Fill(&segment.stream_day_id, 0x10U);
    segment.segment_sequence = 1U;
    segment.segment_base_wal_pos = 0U;
    segment.first_ingress_sequence = 1U;
    segment.created_realtime_ns = 1U;
    segment.created_monotonic_ns = 1U;
    Fill(&segment.host_uuid, 0x20U);
    Fill(&segment.linux_boot_id, 0x30U);
    segment.clock_epoch_algorithm = 1U;
    Fill(&segment.clock_epoch_digest, 0x40U);
    segment.clock_epoch_label = 7U;
    Fill(&segment.sdk_archive_sha256, 0x50U);
    Fill(&segment.libmdl_api_sha256, 0x60U);
    Fill(&segment.endpoint_contract_sha256, 0x70U);
    Fill(&segment.config_sha256, 0x80U);
    segment.raw_schema_sha256 = ingress::RawSchemaSha256Digest();
    Fill(&segment.build_manifest_sha256, 0x90U);
    ingress::RawV1SegmentHeaderWire header{};
    if (ingress::EncodeSegmentHeaderV1(segment, &header) !=
        ingress::RawV1Error::kNone) {
        throw std::runtime_error("segment header encode failed");
    }
    built.source->wire.assign(header.begin(), header.end());
    std::uint64_t sequence = 1U;
    for (const RecordSpec& spec : records) {
        ingress::RawRecordInputV1 input;
        input.meta.source_stream_id = kSourceStreamId;
        input.meta.ingress_sequence = sequence;
        input.meta.recv_realtime_ns = 10'000U + sequence;
        input.meta.recv_monotonic_ns = 20'000U + sequence;
        input.meta.capture_date = kCaptureDate;
        input.vendor_head = VendorHead(
            static_cast<std::uint32_t>(spec.body.size()),
            spec.service_id,
            spec.service_version,
            spec.message_id,
            sequence);
        input.vendor_body = spec.body;
        std::vector<std::byte> encoded;
        if (ingress::EncodeRawRecordV1(input, &encoded) !=
            ingress::RawV1Error::kNone) {
            throw std::runtime_error("Raw record encode failed");
        }
        built.source->wire.insert(
            built.source->wire.end(), encoded.begin(), encoded.end());
        built.source->record_end_offsets.push_back(
            built.source->wire.size());
        ++sequence;
    }
    built.source->total_records = records.size();
    Fill(&built.writer, 0xa0U);
    ingress::RawControlSnapshot& control = built.source->control;
    control.writer_instance = built.writer;
    control.stream_day_id = segment.stream_day_id;
    control.source_stream_id = kSourceStreamId;
    control.capture_date = kCaptureDate;
    control.segment_sequence = 1U;
    const std::uint64_t history_end = history_count == 0U
        ? ingress::kRawV1SegmentHeaderBytes
        : built.source->record_end_offsets[history_count - 1U];
    control.append_global_wal_pos = history_end;
    control.append_ingress_sequence = history_count;
    control.append_segment_offset = history_end;
    control.durable_global_wal_pos = history_end;
    control.durable_ingress_sequence = history_count;
    control.durable_segment_offset = history_end;
    control.heartbeat_monotonic_ns = 1000U;

    auto history_bytes = std::make_shared<const std::vector<std::byte>>(
        built.source->wire.begin(),
        built.source->wire.begin() + static_cast<std::ptrdiff_t>(history_end));
    ingress::RawSegmentScanResult scan = ingress::ScanRawSegmentV1(
        history_bytes, history_end);
    if (!scan.ok()) {
        throw std::runtime_error("history Raw scan failed");
    }
    built.replay.control = control;
    built.replay.control_generation = 2U;
    built.replay.live_attach.writer_instance = built.writer;
    built.replay.live_attach.stream_day_id = segment.stream_day_id;
    built.replay.live_attach.source_stream_id = kSourceStreamId;
    built.replay.live_attach.capture_date = kCaptureDate;
    built.replay.live_attach.segment_sequence = 1U;
    built.replay.live_attach.global_wal_pos = history_end;
    built.replay.live_attach.segment_offset = history_end;
    built.replay.live_attach.next_ingress_sequence = history_count + 1U;
    built.replay.scans.push_back(std::move(scan));
    return built;
}

struct RuntimeState final {
    std::vector<std::string> events;
    std::uint64_t connect_calls = 0U;
    ingress::RawIngressTailConsumerEvidenceV1 evidence{};
    std::atomic<std::uint64_t> stop_completed{0U};
};

class FakeRuntime final
    : public ingress::RawProductionAuthoritativeRuntimeV1 {
public:
    FakeRuntime(
        BuiltRuntime built,
        std::shared_ptr<RuntimeState> state,
        bool connect_succeeds = true,
        bool fresh_control_sample_succeeds = true) noexcept
        : source_(std::move(built.source)),
          replay_(std::move(built.replay)),
          writer_(built.writer),
          state_(std::move(state)),
          connect_succeeds_(connect_succeeds),
          fresh_control_sample_succeeds_(
              fresh_control_sample_succeeds) {}

    ~FakeRuntime() override {
        if (app_state_ == ingress::RawIngressAppState::kRunning &&
            consumer_ != nullptr) {
            consumer_->AbortAndJoin();
        }
    }

    ingress::RawProductionReplaySnapshotResultV1
    PrepareAuthoritativeReplay(
        ingress::RawProductionReplaySnapshotLimitsV1) noexcept override {
        ingress::RawProductionReplaySnapshotResultV1 result;
        if (prepared_ || app_state_ !=
                ingress::RawIngressAppState::kConstructed) {
            result.error = ingress::RawProductionReplaySnapshotErrorV1::
                kInvalidState;
            return result;
        }
        prepared_ = true;
        result.snapshot = replay_;
        return result;
    }

    ingress::RawLiveTailError AttachAuthoritativeTail(
        const ingress::RawLiveTailAttachV1& attach,
        std::unique_ptr<ingress::RawLiveTail>* output) noexcept override {
        if (!prepared_ || attached_ || output == nullptr ||
            attach.global_wal_pos != replay_.live_attach.global_wal_pos ||
            attach.next_ingress_sequence !=
                replay_.live_attach.next_ingress_sequence) {
            return ingress::RawLiveTailError::kInvalidAttach;
        }
        const ingress::RawLiveTailError result =
            ingress::RawLiveTail::Attach(source_.get(), attach, output);
        attached_ = result == ingress::RawLiveTailError::kNone;
        return result;
    }

    bool InstallExternalTailConsumer(
        ingress::RawIngressExternalTailConsumerV1* consumer,
        std::string*) noexcept override {
        if (!attached_ || consumer == nullptr || consumer_ != nullptr) {
            return false;
        }
        consumer_ = consumer;
        return true;
    }

    bool Initialize(std::string* error) noexcept override {
        state_->events.emplace_back("capture-start");
        if (consumer_ == nullptr ||
            !consumer_->StartBeforeConnect(error)) {
            if (consumer_ != nullptr) {
                consumer_->AbortAndJoin();
            }
            app_state_ = ingress::RawIngressAppState::kStopped;
            fatal_ = true;
            return false;
        }
        state_->events.emplace_back("external-start");
        if (!connect_succeeds_) {
            consumer_->AbortAndJoin();
            app_state_ = ingress::RawIngressAppState::kStopped;
            fatal_ = true;
            return false;
        }
        state_->events.emplace_back("connect");
        ++state_->connect_calls;
        app_state_ = ingress::RawIngressAppState::kRunning;
        return true;
    }

    bool Stop(std::string* error) noexcept override {
        if (app_state_ != ingress::RawIngressAppState::kRunning ||
            consumer_ == nullptr) {
            return false;
        }
        app_state_ = ingress::RawIngressAppState::kStopping;
        state_->events.emplace_back("sdk-shutdown");
        state_->events.emplace_back("callback-quiesce");
        state_->events.emplace_back("raw-seal");
        const ingress::RawControlSnapshot control = source_->control;
        ingress::RawReadinessStopCursorV1 cursor;
        cursor.writer_instance = control.writer_instance;
        cursor.stream_day_id = control.stream_day_id;
        cursor.source_stream_id = control.source_stream_id;
        cursor.capture_date = control.capture_date;
        cursor.segment_sequence = control.segment_sequence;
        cursor.global_wal_pos = control.append_global_wal_pos;
        cursor.ingress_sequence = control.append_ingress_sequence;
        cursor.segment_offset = control.append_segment_offset;
        const bool stopped = consumer_->StopAtAndJoin(
            cursor, &state_->evidence, error);
        state_->events.emplace_back("external-stop");
        app_state_ = ingress::RawIngressAppState::kStopped;
        fatal_ = !stopped;
        if (stopped) {
            state_->events.emplace_back("clean-stop");
        }
        state_->stop_completed.store(1U, std::memory_order_release);
        return stopped;
    }

    ingress::RawProductionControlSampleV1
    SampleControlFresh() const noexcept override {
        ingress::RawProductionControlSampleV1 result;
        if (!fresh_control_sample_succeeds_) {
            result.error_number = EIO;
            return result;
        }
        result.error_number = source_->ReadControl(
            &result.snapshot, &result.generation);
        return result;
    }

    ingress::RawIngressAppState app_state() const noexcept override {
        return app_state_;
    }
    bool app_fatal() const noexcept override { return fatal_; }
    std::uint64_t connect_generation() const noexcept override {
        return 7U;
    }

    TestSource* source() noexcept { return source_.get(); }

private:
    std::unique_ptr<TestSource> source_;
    ingress::RawProductionReplaySnapshotV1 replay_;
    l2flow::common::Identity128 writer_{};
    std::shared_ptr<RuntimeState> state_;
    ingress::RawIngressExternalTailConsumerV1* consumer_ = nullptr;
    ingress::RawIngressAppState app_state_ =
        ingress::RawIngressAppState::kConstructed;
    bool connect_succeeds_ = true;
    bool fresh_control_sample_succeeds_ = true;
    bool prepared_ = false;
    bool attached_ = false;
    bool fatal_ = false;
};

struct SinkState final {
    std::vector<control::ControlRecordV1> records;
    std::vector<control::ControlRecordWireV1> wires;
    std::uint64_t identical = 0U;
};

class MemorySink final : public control::ControlRecordSinkV1 {
public:
    explicit MemorySink(
        std::shared_ptr<SinkState> state,
        bool always_conflict = false)
        : state_(std::move(state)),
          always_conflict_(always_conflict) {}

    control::ControlRecordPublishResultV1 Publish(
        const control::ControlRecordV1& record,
        const control::ControlRecordWireV1& wire,
        std::uint64_t) noexcept override {
        if (always_conflict_) {
            return control::ControlRecordPublishResultV1::kConflict;
        }
        try {
            for (std::size_t index = 0U;
                 index < state_->records.size();
                 ++index) {
                const control::ControlRecordV1& existing =
                    state_->records[index];
                if (existing.source_stream_id == record.source_stream_id &&
                    existing.capture_date == record.capture_date &&
                    existing.stream_day_id == record.stream_day_id &&
                    existing.origin_ingress_sequence ==
                        record.origin_ingress_sequence &&
                    existing.origin_record_end_wal_pos ==
                        record.origin_record_end_wal_pos) {
                    if (state_->wires[index] == wire) {
                        ++state_->identical;
                        return control::ControlRecordPublishResultV1::
                            kAcceptedIdentical;
                    }
                    return control::ControlRecordPublishResultV1::kConflict;
                }
            }
            state_->records.push_back(record);
            state_->wires.push_back(wire);
            return control::ControlRecordPublishResultV1::kPublishedNew;
        } catch (...) {
            return control::ControlRecordPublishResultV1::kFailure;
        }
    }

private:
    std::shared_ptr<SinkState> state_;
    bool always_conflict_ = false;
};

std::uint64_t ConstantClock(void*) noexcept { return 1000U; }
std::uint64_t ZeroClock(void*) noexcept { return 0U; }

control::ControlProductionControllerConfigV1 ControllerConfig(
    const BuiltRuntime& built,
    int checkpoint_directory_fd = -1,
    bool invalid_clock = false) {
    control::ControlProductionControllerConfigV1 config;
    config.decoder.source_stream_id = kSourceStreamId;
    config.decoder.capture_date = kCaptureDate;
    config.decoder.stream_day_id =
        built.replay.control.stream_day_id;
    config.decoder.stable_config_sha256 =
        built.replay.scans.front().segment.config_sha256;
    config.decoder.required = {kRequired};
    config.worker.writer_instance = built.writer;
    config.worker.connect_generation = 7U;
    config.worker.record_publish_timeout_ns = 1'000'000U;
    config.worker.final_catch_up_timeout_ns = 1'000'000U;
    config.worker.monotonic_now = invalid_clock
        ? &ZeroClock
        : &ConstantClock;
    config.startup_timeout_ns = 1'000'000U;
    config.checkpoint_directory_fd = checkpoint_directory_fd;
    return config;
}

bool WaitDecoder(
    const control::ControlProductionControllerV1& controller,
    std::uint64_t expected) {
    for (std::size_t attempt = 0U; attempt < 1'000'000U; ++attempt) {
        if (controller.DecoderSnapshot().processed_ingress_sequence ==
            expected) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

class TemporaryDirectory final {
public:
    TemporaryDirectory() {
        std::array<char, 64U> pattern{};
        constexpr std::string_view value =
            "/tmp/l2flow-controller-checkpoints-XXXXXX";
        std::copy(value.begin(), value.end(), pattern.begin());
        char* const created = ::mkdtemp(pattern.data());
        if (created == nullptr) {
            throw std::runtime_error("mkdtemp failed");
        }
        path_ = created;
        if (::chmod(path_.c_str(), 0700U) != 0) {
            throw std::runtime_error("chmod failed");
        }
        descriptor_ = ::open(
            path_.c_str(),
            O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        if (descriptor_ < 0) {
            throw std::runtime_error("checkpoint directory open failed");
        }
    }
    ~TemporaryDirectory() {
        if (descriptor_ >= 0) {
            static_cast<void>(::close(descriptor_));
        }
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }
    int descriptor() const noexcept { return descriptor_; }

private:
    std::string path_;
    int descriptor_ = -1;
};

void TestLiveLifecycleReadyAndExactStop(TestContext* test) {
    const std::vector<RecordSpec> records{
        IrrelevantRecord(), LogonRecord(), MarketRecord()};
    BuiltRuntime built = BuildRuntime(records, 1U);
    const auto config = ControllerConfig(built);
    auto runtime_state = std::make_shared<RuntimeState>();
    auto runtime = std::make_unique<FakeRuntime>(
        std::move(built), runtime_state);
    TestSource* const source = runtime->source();
    auto sink_state = std::make_shared<SinkState>();
    std::unique_ptr<control::ControlProductionControllerV1> controller;
    std::string error;
    const auto created = control::ControlProductionControllerV1::Create(
        config,
        std::move(runtime),
        std::make_unique<MemorySink>(sink_state),
        &controller,
        &error);
    const bool initialized =
        controller != nullptr && controller->Initialize(&error);
    if (initialized) {
        source->PublishAll();
    }
    const bool caught_up = initialized && WaitDecoder(*controller, 3U);
    control::ControlReadinessGateConfigV1 gate;
    gate.maximum_decoder_lag_bytes = 1'000'000U;
    gate.maximum_durability_lag_bytes = 1'000'000U;
    gate.heartbeat_timeout_ns = 1'000'000U;
    const control::ControlProductionReadinessSampleV1 readiness =
        controller == nullptr
            ? control::ControlProductionReadinessSampleV1{}
            : controller->EvaluateReadiness(
                  gate, std::nullopt, 1000U, 1000U);
    const bool stopped =
        controller != nullptr && controller->Stop(&error);
    const std::vector<std::string> expected_events{
        "capture-start",
        "external-start",
        "connect",
        "sdk-shutdown",
        "callback-quiesce",
        "raw-seal",
        "external-stop",
        "clean-stop",
    };
    test->Expect(
        created ==
                control::ControlProductionControllerCreateErrorV1::kNone &&
            initialized && caught_up && readiness.gate.ready &&
            readiness.gate.reason ==
                control::ControlReadinessReasonV1::kReady &&
            stopped && runtime_state->events == expected_events &&
            runtime_state->connect_calls == 1U &&
            runtime_state->evidence.authoritative_control &&
            runtime_state->evidence.processed_ingress_sequence == 3U &&
            controller->Snapshot().state ==
                control::ControlProductionControllerStateV1::kStopped &&
            sink_state->records.size() == 1U,
        "controller orders replay, pre-Connect worker startup, fresh Phase-3 READY, Raw terminal catch-up, and clean stop");
}

void TestStartupFailurePreventsConnect(TestContext* test) {
    const std::vector<RecordSpec> no_records;
    BuiltRuntime built = BuildRuntime(no_records, 0U);
    const auto config = ControllerConfig(built, -1, true);
    auto runtime_state = std::make_shared<RuntimeState>();
    auto runtime = std::make_unique<FakeRuntime>(
        std::move(built), runtime_state);
    std::unique_ptr<control::ControlProductionControllerV1> controller;
    const auto created = control::ControlProductionControllerV1::Create(
        config,
        std::move(runtime),
        std::make_unique<MemorySink>(std::make_shared<SinkState>()),
        &controller,
        nullptr);
    const bool initialized =
        controller != nullptr && controller->Initialize(nullptr);
    test->Expect(
        created ==
                control::ControlProductionControllerCreateErrorV1::kNone &&
            !initialized && runtime_state->connect_calls == 0U &&
            controller->Snapshot().state ==
                control::ControlProductionControllerStateV1::kFailed &&
            !controller->Snapshot().checkpoint_publication_attempted,
        "invalid worker startup clock aborts the generation before SDK Connect and never publishes a checkpoint");
}

void TestConnectFailureAndLiveSinkConflictAbort(TestContext* test) {
    {
        const std::vector<RecordSpec> no_records;
        BuiltRuntime built = BuildRuntime(no_records, 0U);
        const auto config = ControllerConfig(built);
        auto runtime_state = std::make_shared<RuntimeState>();
        auto runtime = std::make_unique<FakeRuntime>(
            std::move(built), runtime_state, false);
        std::unique_ptr<control::ControlProductionControllerV1> controller;
        const auto created = control::ControlProductionControllerV1::Create(
            config,
            std::move(runtime),
            std::make_unique<MemorySink>(
                std::make_shared<SinkState>()),
            &controller,
            nullptr);
        const bool initialized =
            controller != nullptr && controller->Initialize(nullptr);
        test->Expect(
            created ==
                    control::ControlProductionControllerCreateErrorV1::kNone &&
                !initialized && runtime_state->connect_calls == 0U &&
                controller->Snapshot().state ==
                    control::ControlProductionControllerStateV1::kFailed &&
                !controller->Snapshot().checkpoint_publication_attempted,
            "SDK Connect failure aborts and joins the already-started Phase-3 worker without publishing a checkpoint");
    }

    {
        const std::vector<RecordSpec> records{
            IrrelevantRecord(), LogonRecord()};
        BuiltRuntime built = BuildRuntime(records, 1U);
        const auto config = ControllerConfig(built);
        auto runtime_state = std::make_shared<RuntimeState>();
        auto runtime = std::make_unique<FakeRuntime>(
            std::move(built), runtime_state);
        TestSource* const source = runtime->source();
        std::unique_ptr<control::ControlProductionControllerV1> controller;
        const auto created = control::ControlProductionControllerV1::Create(
            config,
            std::move(runtime),
            std::make_unique<MemorySink>(
                std::make_shared<SinkState>(), true),
            &controller,
            nullptr);
        const bool initialized =
            controller != nullptr && controller->Initialize(nullptr);
        if (initialized) {
            source->PublishAll();
        }
        bool failed = false;
        if (controller != nullptr) {
            for (std::size_t attempt = 0U;
                 attempt < 1'000'000U;
                 ++attempt) {
                if (controller->Snapshot().state ==
                    control::ControlProductionControllerStateV1::kFailed) {
                    failed = true;
                    break;
                }
                std::this_thread::yield();
            }
        }
        bool generation_stopped = false;
        for (std::size_t attempt = 0U;
             attempt < 1'000'000U;
             ++attempt) {
            if (runtime_state->stop_completed.load(
                    std::memory_order_acquire) == 1U) {
                generation_stopped = true;
                break;
            }
            std::this_thread::yield();
        }
        test->Expect(
            created ==
                    control::ControlProductionControllerCreateErrorV1::kNone &&
                initialized && failed && generation_stopped &&
                controller->Snapshot().worker_failure ==
                    control::ControlLiveWorkerFailureV1::
                        kControlRecordSinkRejected &&
                !controller->Snapshot().checkpoint_publication_attempted &&
                !runtime_state->evidence.authoritative_control &&
                std::find(
                    runtime_state->events.begin(),
                    runtime_state->events.end(),
                    "sdk-shutdown") != runtime_state->events.end(),
            "a live derived-sink conflict revokes health, automatically stops the SDK generation from a supervisor thread, and cannot forge normal terminal evidence");
    }
}

struct HistoricalRun final {
    bool ok = false;
    control::ControlProductionControllerSnapshotV1 controller{};
    control::ControlDecoderSnapshotV1 decoder{};
};

HistoricalRun RunHistorical(
    const std::vector<RecordSpec>& records,
    int checkpoint_directory_fd,
    const std::shared_ptr<SinkState>& sink_state) {
    BuiltRuntime built = BuildRuntime(records, records.size());
    const auto config = ControllerConfig(built, checkpoint_directory_fd);
    auto runtime_state = std::make_shared<RuntimeState>();
    auto runtime = std::make_unique<FakeRuntime>(
        std::move(built), runtime_state);
    std::unique_ptr<control::ControlProductionControllerV1> controller;
    HistoricalRun result;
    const auto created = control::ControlProductionControllerV1::Create(
        config,
        std::move(runtime),
        std::make_unique<MemorySink>(sink_state),
        &controller,
        nullptr);
    if (created !=
            control::ControlProductionControllerCreateErrorV1::kNone ||
        controller == nullptr || !controller->Initialize(nullptr) ||
        !controller->Stop(nullptr)) {
        return result;
    }
    result.ok = true;
    result.controller = controller->Snapshot();
    result.decoder = controller->DecoderSnapshot();
    return result;
}

void TestCheckpointSuffixReplayMatchesFullReplay(TestContext* test) {
    TemporaryDirectory checkpoints;
    auto sink_state = std::make_shared<SinkState>();
    const std::vector<RecordSpec> first_history{
        LogonRecord(), MarketRecord()};
    const HistoricalRun first = RunHistorical(
        first_history, checkpoints.descriptor(), sink_state);
    std::vector<RecordSpec> extended = first_history;
    extended.push_back(IrrelevantRecord());
    const HistoricalRun resumed = RunHistorical(
        extended, checkpoints.descriptor(), sink_state);
    const HistoricalRun full = RunHistorical(
        extended, -1, sink_state);
    test->Expect(
        first.ok && first.controller.checkpoint_publication_attempted &&
            first.controller.checkpoint_published && resumed.ok &&
            resumed.controller.checkpoint_loaded &&
            resumed.controller.checkpoint_restored &&
            resumed.controller.replayed_records == 1U && full.ok &&
            full.controller.replayed_records == 3U &&
            resumed.decoder.state_sha256 == full.decoder.state_sha256 &&
            resumed.decoder.processed_ingress_sequence == 3U &&
            sink_state->identical >= 1U,
        "restart restores a real Raw-bound checkpoint, replays only the suffix through the same idempotent sink, and reaches the full-replay state hash");
}

void TestCheckpointConfigMismatchFallsBackToRaw(TestContext* test) {
    TemporaryDirectory checkpoints;
    auto sink_state = std::make_shared<SinkState>();
    const std::vector<RecordSpec> prefix{
        LogonRecord(), MarketRecord()};
    const HistoricalRun original = RunHistorical(
        prefix, checkpoints.descriptor(), sink_state);
    std::vector<RecordSpec> extended = prefix;
    extended.push_back(IrrelevantRecord());
    BuiltRuntime discovery = BuildRuntime(extended, extended.size());
    control::ControlCheckpointPosixLoadResultV1 loaded =
        control::LoadLatestControlCheckpointV1At(
            checkpoints.descriptor(), discovery.replay.control, nullptr);

    bool replaced = false;
    if (loaded.ok()) {
        control::ControlDecoderCheckpointV1 incompatible =
            *loaded.checkpoint;
        incompatible.state.stable_config_sha256[0U] ^=
            std::byte{0x5aU};
        l2flow::common::Sha256Digest recomputed{};
        const bool hashed =
            control::ComputeControlDecoderStateSha256V1(
                incompatible.state, &recomputed);
        incompatible.state.state_sha256 = recomputed;
        const bool removed = ::unlinkat(
            checkpoints.descriptor(), loaded.filename.c_str(), 0) == 0;
        const bool directory_synced =
            removed && ::fsync(checkpoints.descriptor()) == 0;
        const control::ControlCheckpointPosixPublishResultV1 published =
            hashed && directory_synced
                ? control::PublishControlCheckpointV1At(
                      checkpoints.descriptor(),
                      incompatible,
                      discovery.replay.control,
                      nullptr)
                : control::ControlCheckpointPosixPublishResultV1{};
        replaced = published.ok();
    }

    BuiltRuntime current = BuildRuntime(extended, extended.size());
    const auto config = ControllerConfig(current, checkpoints.descriptor());
    auto runtime_state = std::make_shared<RuntimeState>();
    auto runtime = std::make_unique<FakeRuntime>(
        std::move(current), runtime_state);
    std::unique_ptr<control::ControlProductionControllerV1> controller;
    const auto created = control::ControlProductionControllerV1::Create(
        config,
        std::move(runtime),
        std::make_unique<MemorySink>(sink_state),
        &controller,
        nullptr);
    const bool initialized =
        controller != nullptr && controller->Initialize(nullptr);
    const bool stopped = initialized && controller->Stop(nullptr);
    const auto snapshot = controller == nullptr
        ? control::ControlProductionControllerSnapshotV1{}
        : controller->Snapshot();
    test->Expect(
        original.ok && loaded.ok() && replaced &&
            created ==
                control::ControlProductionControllerCreateErrorV1::kNone &&
            initialized && stopped && snapshot.checkpoint_loaded &&
            !snapshot.checkpoint_restored &&
            snapshot.checkpoint_rejected_to_full_replay &&
            snapshot.checkpoint_restore_error ==
                control::ControlDecoderCreateErrorV1::kInvalidCheckpoint &&
            snapshot.replayed_records == extended.size() &&
            controller->DecoderSnapshot().processed_ingress_sequence ==
                extended.size(),
        "a canonical checkpoint with an incompatible stable config is only an optimization candidate and falls back to complete authoritative Raw replay");
}

void TestUnsafeCheckpointCandidateFailsClosed(TestContext* test) {
    TemporaryDirectory checkpoints;
    const std::vector<RecordSpec> records{LogonRecord()};
    const HistoricalRun original = RunHistorical(
        records,
        checkpoints.descriptor(),
        std::make_shared<SinkState>());
    BuiltRuntime discovery = BuildRuntime(records, records.size());
    const control::ControlCheckpointPosixLoadResultV1 loaded =
        control::LoadLatestControlCheckpointV1At(
            checkpoints.descriptor(), discovery.replay.control, nullptr);
    const bool replaced_with_symlink = loaded.ok() &&
        ::unlinkat(
            checkpoints.descriptor(), loaded.filename.c_str(), 0) == 0 &&
        ::symlinkat(
            "/dev/null",
            checkpoints.descriptor(),
            loaded.filename.c_str()) == 0 &&
        ::fsync(checkpoints.descriptor()) == 0;

    BuiltRuntime current = BuildRuntime(records, records.size());
    const auto config = ControllerConfig(current, checkpoints.descriptor());
    auto runtime_state = std::make_shared<RuntimeState>();
    std::unique_ptr<control::ControlProductionControllerV1> controller;
    const auto created = control::ControlProductionControllerV1::Create(
        config,
        std::make_unique<FakeRuntime>(
            std::move(current), runtime_state),
        std::make_unique<MemorySink>(std::make_shared<SinkState>()),
        &controller,
        nullptr);
    test->Expect(
        original.ok && loaded.ok() && replaced_with_symlink &&
            created ==
                control::ControlProductionControllerCreateErrorV1::
                    kCheckpointDiscovery &&
            controller == nullptr && runtime_state->connect_calls == 0U &&
            runtime_state->events.empty(),
        "an unsafe checkpoint final in the active Raw namespace fails closed before worker startup or SDK construction");
}

void TestMalformedCommittedReplayRemainsPoisoned(TestContext* test) {
    const std::vector<RecordSpec> records{MalformedLogonRecord()};
    BuiltRuntime built = BuildRuntime(records, records.size());
    const auto config = ControllerConfig(built);
    auto runtime_state = std::make_shared<RuntimeState>();
    auto sink_state = std::make_shared<SinkState>();
    std::unique_ptr<control::ControlProductionControllerV1> controller;
    const auto created = control::ControlProductionControllerV1::Create(
        config,
        std::make_unique<FakeRuntime>(
            std::move(built), runtime_state),
        std::make_unique<MemorySink>(sink_state),
        &controller,
        nullptr);
    const bool initialized =
        controller != nullptr && controller->Initialize(nullptr);
    const bool stopped = initialized && controller->Stop(nullptr);
    const control::ControlDecoderSnapshotV1 decoder = controller == nullptr
        ? control::ControlDecoderSnapshotV1{}
        : controller->DecoderSnapshot();
    test->Expect(
        created ==
                control::ControlProductionControllerCreateErrorV1::kNone &&
            initialized && stopped && decoder.poisoned &&
            !decoder.control_ready &&
            decoder.counters.control_decode_errors == 1U &&
            sink_state->records.size() == 1U &&
            sink_state->records.front().control_type ==
                control::ControlTypeV1::kDecodeError &&
            controller->Snapshot().replayed_control_records == 1U,
        "a malformed committed control record advances the authoritative cursor, durably emits DecodeError, and permanently prevents READY without corrupting clean-stop accounting");
}

void TestCheckpointPublicationFailureDoesNotForgeRawFailure(
    TestContext* test) {
    TemporaryDirectory checkpoints;
    const std::vector<RecordSpec> records{IrrelevantRecord()};
    BuiltRuntime built = BuildRuntime(records, records.size());
    const auto config = ControllerConfig(built, checkpoints.descriptor());
    auto runtime_state = std::make_shared<RuntimeState>();
    std::unique_ptr<control::ControlProductionControllerV1> controller;
    const auto created = control::ControlProductionControllerV1::Create(
        config,
        std::make_unique<FakeRuntime>(
            std::move(built), runtime_state, true, false),
        std::make_unique<MemorySink>(std::make_shared<SinkState>()),
        &controller,
        nullptr);
    const bool initialized =
        controller != nullptr && controller->Initialize(nullptr);
    const bool stopped = initialized && controller->Stop(nullptr);
    const auto snapshot = controller == nullptr
        ? control::ControlProductionControllerSnapshotV1{}
        : controller->Snapshot();
    test->Expect(
        created ==
                control::ControlProductionControllerCreateErrorV1::kNone &&
            initialized && stopped &&
            snapshot.state ==
                control::ControlProductionControllerStateV1::kStopped &&
            snapshot.checkpoint_publication_attempted &&
            !snapshot.checkpoint_published &&
            snapshot.checkpoint_publish_error ==
                control::ControlCheckpointPosixStoreErrorV1::
                    kInvalidRawFrontier &&
            runtime_state->evidence.authoritative_control &&
            !runtime_state->events.empty() &&
            runtime_state->events.back() == "clean-stop",
        "checkpoint publication failure is reported as an optimization failure but cannot erase exact Raw/Phase-3 clean-stop evidence");
}

}  // namespace

int main() {
    TestContext test;
    TestLiveLifecycleReadyAndExactStop(&test);
    TestStartupFailurePreventsConnect(&test);
    TestConnectFailureAndLiveSinkConflictAbort(&test);
    TestCheckpointSuffixReplayMatchesFullReplay(&test);
    TestCheckpointConfigMismatchFallsBackToRaw(&test);
    TestUnsafeCheckpointCandidateFailsClosed(&test);
    TestMalformedCommittedReplayRemainsPoisoned(&test);
    TestCheckpointPublicationFailureDoesNotForgeRawFailure(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " production controller test(s) failed\n";
        return 1;
    }
    std::cout << "Phase3 production controller tests passed\n";
    return 0;
}
