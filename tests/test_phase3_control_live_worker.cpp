#include "l2flow/control/control_live_worker.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/raw_v1.h"

#include "mdl_shl2_msg.h"
#include "mdl_sys_msg.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <span>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace control = l2flow::control;
namespace ingress = l2flow::ingress;
namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sys = datayes::mdl::mdl_sys_msg;

namespace {

constexpr std::uint32_t kSourceStreamId = 1002U;
constexpr std::uint32_t kCaptureDate = 20260721U;
constexpr l2flow::sdk::MessageKey kRequired{4U, 101U, 24U};

struct TestContext final {
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << description << '\n';
        }
    }
    int failures = 0;
};

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

template <std::size_t Size>
void Fill(std::array<std::byte, Size>* output, std::uint8_t seed) {
    for (std::size_t index = 0U; index < output->size(); ++index) {
        (*output)[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(seed + index));
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
    head[0] = static_cast<std::byte>(
        ingress::kVendorMessageHeadBytes);
    StoreU32(
        bytes,
        1U,
        static_cast<std::uint32_t>(
            ingress::kVendorMessageHeadBytes) +
            body_size);
    head[5] = std::byte{1U};
    head[6] = static_cast<std::byte>(service_id);
    StoreU16(bytes, 7U, service_version);
    StoreU16(bytes, 9U, message_id);
    StoreU32(bytes, 11U, 123U);
    StoreU64(bytes, 15U, sequence);
    return head;
}

std::vector<std::byte> SuccessfulLogonBody(
    std::uint32_t required_message_result = 0U) {
    // LogonResponse fixed bytes, one ServicesItem and one MessagesItem.
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
    StoreU32(bytes, 44U, required_message_result);
    return body;
}

struct RecordSpec final {
    std::uint8_t service_id = 0U;
    std::uint16_t service_version = 0U;
    std::uint16_t message_id = 0U;
    std::vector<std::byte> body;
};

class StaticLiveSource final : public ingress::RawLiveTailSource {
public:
    int ReadControl(
        ingress::RawControlSnapshot* output,
        std::uint64_t* generation) noexcept override {
        if (output == nullptr || generation == nullptr) {
            return EINVAL;
        }
        std::lock_guard<std::mutex> lock(control_mutex_);
        if (control_error_ != 0) {
            return control_error_;
        }
        *output = control;
        *generation = control_generation_;
        return 0;
    }

    void PublishAll(bool advance_generation = true) noexcept {
        std::lock_guard<std::mutex> lock(control_mutex_);
        control.append_global_wal_pos =
            segment.segment_base_wal_pos + wire.size();
        control.append_ingress_sequence = record_count;
        control.append_segment_offset = wire.size();
        control.durable_global_wal_pos =
            control.append_global_wal_pos;
        control.durable_ingress_sequence = record_count;
        control.durable_segment_offset = wire.size();
        if (advance_generation) {
            control_generation_ += 2U;
        }
    }

    void SetControlError(int error_number) noexcept {
        std::lock_guard<std::mutex> lock(control_mutex_);
        control_error_ = error_number;
    }

    [[nodiscard]] ingress::RawControlSnapshot ControlSnapshot()
        const noexcept {
        std::lock_guard<std::mutex> lock(control_mutex_);
        return control;
    }

    int InspectSegment(
        std::uint32_t sequence,
        ingress::RawLiveSegmentInfo* output) noexcept override {
        if (output == nullptr) {
            return EINVAL;
        }
        if (sequence != segment.segment_sequence) {
            return ENOENT;
        }
        output->header = segment;
        output->visible_end_offset =
            static_cast<std::uint64_t>(wire.size());
        output->sealed = false;
        return 0;
    }

    ingress::RawLiveReadResult ReadSegmentSome(
        std::uint32_t sequence,
        std::uint64_t offset,
        std::span<std::byte> output) noexcept override {
        if (sequence != segment.segment_sequence ||
            offset > wire.size()) {
            return {0U, EINVAL};
        }
        const std::size_t start = static_cast<std::size_t>(offset);
        const std::size_t amount =
            std::min(output.size(), wire.size() - start);
        std::copy_n(wire.data() + start, amount, output.data());
        return {amount, 0};
    }

    ingress::SegmentHeaderV1 segment{};
    ingress::RawControlSnapshot control{};
    std::vector<std::byte> wire;
    std::uint64_t record_count = 0U;

private:
    mutable std::mutex control_mutex_;
    std::uint64_t control_generation_ = 2U;
    int control_error_ = 0;
};

bool BuildSource(
    std::span<const RecordSpec> records,
    StaticLiveSource* source,
    l2flow::common::Identity128* writer) {
    if (source == nullptr || writer == nullptr) {
        return false;
    }
    ingress::SegmentHeaderV1& segment = source->segment;
    segment.source_stream_id = kSourceStreamId;
    segment.capture_date = kCaptureDate;
    Fill(&segment.stream_day_id, 1U);
    segment.segment_sequence = 1U;
    segment.segment_base_wal_pos = 0U;
    segment.first_ingress_sequence = 1U;
    segment.created_realtime_ns = 1U;
    segment.created_monotonic_ns = 1U;
    Fill(&segment.host_uuid, 21U);
    Fill(&segment.linux_boot_id, 41U);
    segment.clock_epoch_algorithm = 1U;
    Fill(&segment.clock_epoch_digest, 51U);
    segment.clock_epoch_label = 1U;
    Fill(&segment.sdk_archive_sha256, 61U);
    Fill(&segment.libmdl_api_sha256, 81U);
    Fill(&segment.endpoint_contract_sha256, 101U);
    Fill(&segment.config_sha256, 121U);
    segment.raw_schema_sha256 = ingress::RawSchemaSha256Digest();
    Fill(&segment.build_manifest_sha256, 161U);

    ingress::RawV1SegmentHeaderWire header{};
    if (ingress::EncodeSegmentHeaderV1(segment, &header) !=
        ingress::RawV1Error::kNone) {
        return false;
    }
    source->wire.assign(header.begin(), header.end());
    std::uint64_t sequence = 1U;
    for (const RecordSpec& spec : records) {
        ingress::RawRecordInputV1 input;
        input.meta.source_stream_id = kSourceStreamId;
        input.meta.ingress_sequence = sequence;
        input.meta.recv_realtime_ns = 1'000U + sequence;
        input.meta.recv_monotonic_ns = 2'000U + sequence;
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
            return false;
        }
        source->wire.insert(
            source->wire.end(), encoded.begin(), encoded.end());
        ++sequence;
    }

    Fill(writer, 211U);
    source->control.writer_instance = *writer;
    source->control.stream_day_id = segment.stream_day_id;
    source->control.source_stream_id = kSourceStreamId;
    source->control.capture_date = kCaptureDate;
    source->control.segment_sequence = 1U;
    source->control.append_global_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    source->control.append_ingress_sequence = 0U;
    source->control.append_segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    source->control.durable_global_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    source->control.durable_ingress_sequence = 0U;
    source->control.durable_segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    source->control.heartbeat_monotonic_ns = 100U;
    source->record_count = records.size();
    return true;
}

std::unique_ptr<ingress::RawLiveTail> AttachTail(
    StaticLiveSource* source,
    const l2flow::common::Identity128& writer) {
    ingress::RawLiveTailAttachV1 attach;
    attach.writer_instance = writer;
    attach.stream_day_id = source->segment.stream_day_id;
    attach.source_stream_id = kSourceStreamId;
    attach.capture_date = kCaptureDate;
    attach.segment_sequence = source->segment.segment_sequence;
    attach.global_wal_pos =
        source->segment.segment_base_wal_pos +
        ingress::kRawV1SegmentHeaderBytes;
    attach.segment_offset = ingress::kRawV1SegmentHeaderBytes;
    attach.next_ingress_sequence = 1U;
    std::unique_ptr<ingress::RawLiveTail> tail;
    if (ingress::RawLiveTail::Attach(source, attach, &tail) !=
        ingress::RawLiveTailError::kNone) {
        return nullptr;
    }
    return tail;
}

std::unique_ptr<control::ControlDecoderV1> CreateDecoder(
    const StaticLiveSource& source) {
    control::ControlDecoderConfigV1 config;
    config.source_stream_id = kSourceStreamId;
    config.capture_date = kCaptureDate;
    config.stream_day_id = source.segment.stream_day_id;
    config.stable_config_sha256 = source.segment.config_sha256;
    config.required = {kRequired};
    std::unique_ptr<control::ControlDecoderV1> decoder;
    if (control::ControlDecoderV1::Create(config, &decoder) !=
        control::ControlDecoderCreateErrorV1::kNone) {
        return nullptr;
    }
    return decoder;
}

struct SinkState final {
    std::vector<control::ControlRecordV1> records;
    std::vector<control::ControlRecordWireV1> wires;
    std::vector<std::uint64_t> deadlines;
    std::uint64_t published_new = 0U;
    std::uint64_t accepted_identical = 0U;
    std::uint64_t conflicts = 0U;
};

bool SameSinkKey(
    const control::ControlRecordV1& left,
    const control::ControlRecordV1& right) noexcept {
    return left.source_stream_id == right.source_stream_id &&
           left.capture_date == right.capture_date &&
           left.stream_day_id == right.stream_day_id &&
           left.origin_ingress_sequence ==
               right.origin_ingress_sequence &&
           left.origin_record_end_wal_pos ==
               right.origin_record_end_wal_pos;
}

class RecordingSink final : public control::ControlRecordSinkV1 {
public:
    explicit RecordingSink(std::shared_ptr<SinkState> state)
        : state_(std::move(state)) {}

    control::ControlRecordPublishResultV1 Publish(
        const control::ControlRecordV1& record,
        const control::ControlRecordWireV1& canonical_wire,
        std::uint64_t deadline_monotonic_ns) noexcept override {
        const std::size_t record_count = state_->records.size();
        const std::size_t wire_count = state_->wires.size();
        const std::size_t deadline_count = state_->deadlines.size();
        try {
            for (std::size_t index = 0U;
                 index < state_->records.size();
                 ++index) {
                if (!SameSinkKey(state_->records[index], record)) {
                    continue;
                }
                if (state_->wires[index] == canonical_wire) {
                    ++state_->accepted_identical;
                    return control::ControlRecordPublishResultV1::
                        kAcceptedIdentical;
                }
                ++state_->conflicts;
                return control::ControlRecordPublishResultV1::kConflict;
            }
            state_->records.push_back(record);
            state_->wires.push_back(canonical_wire);
            state_->deadlines.push_back(deadline_monotonic_ns);
            ++state_->published_new;
            return control::ControlRecordPublishResultV1::kPublishedNew;
        } catch (...) {
            state_->records.resize(record_count);
            state_->wires.resize(wire_count);
            state_->deadlines.resize(deadline_count);
            return control::ControlRecordPublishResultV1::kFailure;
        }
    }

private:
    std::shared_ptr<SinkState> state_;
};

class FixedOutcomeSink final : public control::ControlRecordSinkV1 {
public:
    explicit FixedOutcomeSink(
        control::ControlRecordPublishResultV1 outcome) noexcept
        : outcome_(outcome) {}

    control::ControlRecordPublishResultV1 Publish(
        const control::ControlRecordV1&,
        const control::ControlRecordWireV1&,
        std::uint64_t) noexcept override {
        return outcome_;
    }

private:
    control::ControlRecordPublishResultV1 outcome_;
};

struct FailureState final {
    static void Notify(
        void* context,
        control::ControlLiveWorkerFailureV1 failure) noexcept {
        auto* state = static_cast<FailureState*>(context);
        ++state->calls;
        state->failure = failure;
    }
    std::uint64_t calls = 0U;
    control::ControlLiveWorkerFailureV1 failure =
        control::ControlLiveWorkerFailureV1::kNone;
};

std::uint64_t ConstantClock(void*) noexcept {
    return 100U;
}

struct StepClock final {
    static std::uint64_t Now(void* context) noexcept {
        auto* clock = static_cast<StepClock*>(context);
        const std::size_t index = std::min(
            clock->next,
            clock->values.size() - 1U);
        ++clock->next;
        return clock->values[index];
    }

    std::array<std::uint64_t, 4U> values{};
    std::size_t next = 0U;
};

control::ControlLiveWorkerConfigV1 WorkerConfig(
    const l2flow::common::Identity128& writer,
    FailureState* failure) {
    control::ControlLiveWorkerConfigV1 config;
    config.writer_instance = writer;
    config.connect_generation = 7U;
    config.record_publish_timeout_ns = 1'000U;
    config.final_catch_up_timeout_ns = 1'000'000U;
    config.monotonic_now = &ConstantClock;
    config.failure_callback = &FailureState::Notify;
    config.failure_context = failure;
    return config;
}

control::ControlLiveStopCursorV1 StopCursor(
    const StaticLiveSource& source,
    const l2flow::common::Identity128& writer) {
    control::ControlLiveStopCursorV1 cursor;
    cursor.writer_instance = writer;
    cursor.stream_day_id = source.segment.stream_day_id;
    cursor.source_stream_id = kSourceStreamId;
    cursor.capture_date = kCaptureDate;
    const ingress::RawControlSnapshot control =
        source.ControlSnapshot();
    cursor.segment_sequence = control.segment_sequence;
    cursor.global_wal_pos = control.append_global_wal_pos;
    cursor.ingress_sequence = control.append_ingress_sequence;
    cursor.segment_offset = control.append_segment_offset;
    return cursor;
}

bool WaitProcessed(
    const control::ControlLiveWorkerV1& worker,
    std::uint64_t expected) {
    for (std::size_t attempt = 0U; attempt < 1'000'000U; ++attempt) {
        if (worker.Snapshot().processed_ingress_sequence == expected) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

bool WaitEmitted(
    const control::ControlLiveWorkerV1& worker,
    std::uint64_t expected) {
    for (std::size_t attempt = 0U; attempt < 1'000'000U; ++attempt) {
        if (worker.Snapshot().emitted_control_records == expected) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

bool WaitStartup(const control::ControlLiveWorkerV1& worker) {
    for (std::size_t attempt = 0U; attempt < 1'000'000U; ++attempt) {
        if (worker.Snapshot().startup_complete) {
            return true;
        }
        std::this_thread::yield();
    }
    return false;
}

void TestLiveReadyAndExactStop(TestContext* test) {
    const std::vector<RecordSpec> records{
        {2U,
         101U,
         static_cast<std::uint16_t>(sys::LogonResponse::MessageID),
         SuccessfulLogonBody()},
        {4U,
         101U,
         24U,
         std::vector<std::byte>(sizeof(sh::NGTSTick), std::byte{0U})},
    };
    StaticLiveSource source;
    l2flow::common::Identity128 writer{};
    test->Expect(
        BuildSource(records, &source, &writer),
        "live worker Raw fixture encodes");
    FailureState failure;
    auto sink_state = std::make_shared<SinkState>();
    std::unique_ptr<control::ControlLiveWorkerV1> worker;
    test->Expect(
        control::ControlLiveWorkerV1::Create(
            WorkerConfig(writer, &failure),
            AttachTail(&source, writer),
            CreateDecoder(source),
            std::make_unique<RecordingSink>(sink_state),
            &worker) == control::ControlLiveWorkerCreateErrorV1::kNone &&
            worker != nullptr,
        "aligned decoder and live-tail cursor create worker");
    if (worker == nullptr) {
        return;
    }
    test->Expect(
        !worker->Checkpoint().has_value(),
        "worker cannot expose a checkpoint before healthy startup");

    source.PublishAll();

    bool run_result = false;
    std::thread thread([&worker, &run_result]() {
        run_result = worker->Run();
    });
    test->Expect(
        WaitProcessed(*worker, 2U),
        "worker consumes logon and required market from Raw live tail");
    test->Expect(
        WaitEmitted(*worker, 1U),
        "worker obtains a durable sink acknowledgement");
    const control::ControlDecoderSnapshotV1 decoder =
        worker->DecoderSnapshot();
    test->Expect(
        decoder.connection_epoch == 1U &&
            decoder.subscription_epoch == 1U &&
            decoder.decoder_evidence_ready,
        "worker drives authoritative decoder epochs and market evidence");

    control::ControlReadinessGateConfigV1 gate;
    gate.maximum_decoder_lag_bytes = 1U << 20U;
    gate.maximum_durability_lag_bytes = 1U << 20U;
    gate.heartbeat_timeout_ns = 1'000U;
    const control::ControlReadinessResultV1 ready =
        worker->EvaluateReadiness(
            gate,
            std::nullopt,
            100U,
            1'000U,
            true);
    test->Expect(
        ready.ready &&
            ready.reason == control::ControlReadinessReasonV1::kReady,
        "fresh Raw sample plus current-generation logon yields READY");
    test->Expect(
        worker->Checkpoint().has_value(),
        "healthy worker exposes a logical checkpoint for durable binding");

    test->Expect(
        worker->StopAt(StopCursor(source, writer)),
        "worker accepts exact terminal stop cursor");
    thread.join();
    test->Expect(run_result, "worker stops only after exact catch-up");
    const control::ControlLiveWorkerSnapshotV1 snapshot =
        worker->Snapshot();
    test->Expect(
        snapshot.finished && snapshot.decoder_healthy &&
            snapshot.emitted_control_records == 1U &&
            failure.calls == 0U && sink_state->records.size() == 1U &&
            sink_state->wires.size() == 1U &&
            sink_state->deadlines.size() == 1U &&
            sink_state->wires.front().size() ==
                control::kControlRecordV1Bytes &&
            sink_state->deadlines.front() == 1'100U &&
            sink_state->published_new == 1U &&
            sink_state->records.front().control_type ==
                control::ControlTypeV1::kLogonSuccess,
        "successful run publishes canonical fixed control output");
    control::ControlRecordV1 decoded_wire;
    if (sink_state->wires.size() == 1U) {
        test->Expect(
            control::DecodeControlRecordV1(
                sink_state->wires.front(), &decoded_wire) ==
                    control::ControlRecordV1Error::kNone &&
                decoded_wire.origin_ingress_sequence == 1U,
            "sink wire is a valid canonical 256-byte ControlRecordV1");
    } else {
        test->Expect(
            false,
            "sink wire is a valid canonical 256-byte ControlRecordV1");
    }
    const control::ControlReadinessResultV1 stopped =
        worker->EvaluateReadiness(
            gate,
            std::nullopt,
            100U,
            1'000U,
            true);
    test->Expect(
        !stopped.ready && stopped.reason ==
            control::ControlReadinessReasonV1::kDecoderUnhealthy,
        "stopping the live worker immediately revokes READY");
    source.SetControlError(EIO);
    const control::ControlReadinessResultV1 unreadable =
        worker->EvaluateReadiness(
            gate,
            std::nullopt,
            100U,
            1'000U,
            true);
    test->Expect(
        !unreadable.ready && unreadable.reason ==
            control::ControlReadinessReasonV1::
                kRawControlSampleUnavailable,
        "a fresh Raw read failure cannot reuse the prior READY sample");
    source.SetControlError(0);
}

void TestMalformedPoisonsButDoesNotStopRawConsumption(TestContext* test) {
    const std::vector<RecordSpec> records{
        {2U,
         101U,
         static_cast<std::uint16_t>(sys::LogonResponse::MessageID),
         SuccessfulLogonBody()},
        {2U,
         101U,
         static_cast<std::uint16_t>(sys::SubscribeResponse::MessageID),
         std::vector<std::byte>{std::byte{0x01}}},
        {4U,
         101U,
         24U,
         std::vector<std::byte>(sizeof(sh::NGTSTick), std::byte{0U})},
    };
    StaticLiveSource source;
    l2flow::common::Identity128 writer{};
    test->Expect(
        BuildSource(records, &source, &writer),
        "malformed-control Raw fixture encodes");
    FailureState failure;
    auto sink_state = std::make_shared<SinkState>();
    std::unique_ptr<control::ControlLiveWorkerV1> worker;
    test->Expect(
        control::ControlLiveWorkerV1::Create(
            WorkerConfig(writer, &failure),
            AttachTail(&source, writer),
            CreateDecoder(source),
            std::make_unique<RecordingSink>(sink_state),
            &worker) == control::ControlLiveWorkerCreateErrorV1::kNone &&
            worker != nullptr,
        "malformed-control worker creates");
    if (worker == nullptr) {
        return;
    }
    source.PublishAll();
    bool run_result = false;
    std::thread thread([&worker, &run_result]() {
        run_result = worker->Run();
    });
    test->Expect(
        WaitProcessed(*worker, 3U),
        "committed malformed control does not stop later Raw consumption");
    test->Expect(
        WaitEmitted(*worker, 2U),
        "malformed-control diagnostic reaches durable sink retention");
    const control::ControlDecoderSnapshotV1 decoder =
        worker->DecoderSnapshot();
    const control::ControlLiveWorkerSnapshotV1 snapshot =
        worker->Snapshot();
    test->Expect(
        decoder.poisoned && snapshot.decoder_healthy &&
            snapshot.committed_malformed_controls == 1U,
        "malformed control is sticky POISONED but worker remains operational");

    control::ControlReadinessGateConfigV1 gate;
    gate.maximum_decoder_lag_bytes = 1U << 20U;
    gate.maximum_durability_lag_bytes = 1U << 20U;
    gate.heartbeat_timeout_ns = 1'000U;
    const control::ControlReadinessResultV1 not_ready =
        worker->EvaluateReadiness(
            gate,
            std::nullopt,
            100U,
            1'000U,
            true);
    test->Expect(
        !not_ready.ready &&
            not_ready.reason ==
                control::ControlReadinessReasonV1::kControlPoisoned,
        "malformed control explicitly revokes READY as POISONED");
    test->Expect(
        worker->StopAt(StopCursor(source, writer)),
        "poisoned worker still accepts exact terminal cursor");
    thread.join();
    test->Expect(
        run_result && failure.calls == 0U &&
            sink_state->records.size() == 2U &&
            sink_state->records.back().control_type ==
                control::ControlTypeV1::kDecodeError,
        "poison diagnostic is published and clean Raw catch-up completes");
}

void TestSameControlGenerationCannotQualifyLogon(TestContext* test) {
    const std::vector<RecordSpec> records{
        {2U,
         101U,
         static_cast<std::uint16_t>(sys::LogonResponse::MessageID),
         SuccessfulLogonBody()},
        {4U,
         101U,
         24U,
         std::vector<std::byte>(sizeof(sh::NGTSTick), std::byte{0U})},
    };
    StaticLiveSource source;
    l2flow::common::Identity128 writer{};
    test->Expect(
        BuildSource(records, &source, &writer),
        "same-generation Raw fixture encodes");
    FailureState failure;
    std::unique_ptr<control::ControlLiveWorkerV1> worker;
    test->Expect(
        control::ControlLiveWorkerV1::Create(
            WorkerConfig(writer, &failure),
            AttachTail(&source, writer),
            CreateDecoder(source),
            std::make_unique<RecordingSink>(
                std::make_shared<SinkState>()),
            &worker) == control::ControlLiveWorkerCreateErrorV1::kNone &&
            worker != nullptr,
        "same-generation worker creates at an empty frontier");
    if (worker == nullptr) {
        return;
    }
    source.PublishAll(false);
    bool run_result = false;
    std::thread thread([&worker, &run_result]() {
        run_result = worker->Run();
    });
    test->Expect(
        WaitProcessed(*worker, 2U) && WaitEmitted(*worker, 1U),
        "same-generation records are decoded and durably published");

    control::ControlReadinessGateConfigV1 gate;
    gate.maximum_decoder_lag_bytes = 1U << 20U;
    gate.maximum_durability_lag_bytes = 1U << 20U;
    gate.heartbeat_timeout_ns = 1'000U;
    const control::ControlReadinessResultV1 not_ready =
        worker->EvaluateReadiness(
            gate,
            std::nullopt,
            100U,
            1'000U,
            true);
    test->Expect(
        !not_ready.ready && not_ready.reason ==
            control::ControlReadinessReasonV1::
                kCurrentGenerationLogonMissing,
        "LogonSuccess must come from a control generation after Create");
    test->Expect(
        worker->StopAt(StopCursor(source, writer)),
        "same-generation worker accepts exact terminal cursor");
    thread.join();
    test->Expect(
        run_result && failure.calls == 0U,
        "same-generation evidence is nonfatal but never READY");
}

void TestEmptyWorkerHasNoCheckpoint(TestContext* test) {
    StaticLiveSource source;
    l2flow::common::Identity128 writer{};
    test->Expect(
        BuildSource(
            std::span<const RecordSpec>{}, &source, &writer),
        "empty checkpoint fixture encodes");
    FailureState failure;
    std::unique_ptr<control::ControlLiveWorkerV1> worker;
    test->Expect(
        control::ControlLiveWorkerV1::Create(
            WorkerConfig(writer, &failure),
            AttachTail(&source, writer),
            CreateDecoder(source),
            std::make_unique<RecordingSink>(
                std::make_shared<SinkState>()),
            &worker) == control::ControlLiveWorkerCreateErrorV1::kNone &&
            worker != nullptr,
        "empty worker creates at the genesis append frontier");
    if (worker == nullptr) {
        return;
    }
    bool run_result = false;
    std::thread thread([&worker, &run_result]() {
        run_result = worker->Run();
    });
    test->Expect(
        WaitStartup(*worker) && worker->Snapshot().decoder_healthy,
        "empty worker completes healthy startup");
    test->Expect(
        !worker->Checkpoint().has_value(),
        "zero-record decoder model is not a publishable checkpoint");
    test->Expect(
        worker->StopAt(StopCursor(source, writer)),
        "empty worker accepts the exact header-boundary stop cursor");
    thread.join();
    test->Expect(
        run_result && failure.calls == 0U,
        "empty worker stops cleanly without manufacturing a checkpoint");
}

void TestStopCursorArithmeticFailsClosed(TestContext* test) {
    const std::vector<RecordSpec> no_records{};
    for (std::size_t scenario = 0U; scenario < 3U; ++scenario) {
        StaticLiveSource source;
        l2flow::common::Identity128 writer{};
        test->Expect(
            BuildSource(no_records, &source, &writer),
            "invalid-stop Raw fixture encodes");
        FailureState failure;
        auto sink_state = std::make_shared<SinkState>();
        std::unique_ptr<control::ControlLiveWorkerV1> worker;
        test->Expect(
            control::ControlLiveWorkerV1::Create(
                WorkerConfig(writer, &failure),
                AttachTail(&source, writer),
                CreateDecoder(source),
                std::make_unique<RecordingSink>(sink_state),
                &worker) ==
                    control::ControlLiveWorkerCreateErrorV1::kNone &&
                worker != nullptr,
            "invalid-stop worker creates at a valid genesis cursor");
        if (worker == nullptr) {
            continue;
        }

        control::ControlLiveStopCursorV1 cursor =
            StopCursor(source, writer);
        if (scenario == 0U) {
            ++cursor.global_wal_pos;
        } else if (scenario == 1U) {
            ++cursor.ingress_sequence;
        } else {
            // These bytes can describe segment 2's empty header frontier,
            // but cannot be relabelled as segment 3 because one intervening
            // 4096-byte header is missing.
            cursor.segment_sequence = 3U;
            cursor.global_wal_pos =
                2U * ingress::kRawV1SegmentHeaderBytes;
            cursor.segment_offset =
                ingress::kRawV1SegmentHeaderBytes;
        }
        test->Expect(
            !worker->StopAt(cursor) &&
                worker->Snapshot().failure ==
                    control::ControlLiveWorkerFailureV1::
                        kStopTargetInvalid &&
                failure.calls == 1U,
            scenario == 0U
                ? "unaligned stop WAL cursor is rejected immediately"
                : scenario == 1U
                    ? "stop sequence advance without Raw bytes is rejected immediately"
                    : "stop segment jump without every header is rejected immediately");
    }
}

void TestIdempotentSinkOutcomes(TestContext* test) {
    const std::vector<RecordSpec> identical_records{
        {2U,
         101U,
         static_cast<std::uint16_t>(sys::LogonResponse::MessageID),
         SuccessfulLogonBody()},
    };
    auto sink_state = std::make_shared<SinkState>();
    for (std::size_t replay = 0U; replay < 2U; ++replay) {
        StaticLiveSource source;
        l2flow::common::Identity128 writer{};
        test->Expect(
            BuildSource(identical_records, &source, &writer),
            "idempotent replay fixture encodes");
        FailureState failure;
        std::unique_ptr<control::ControlLiveWorkerV1> worker;
        test->Expect(
            control::ControlLiveWorkerV1::Create(
                WorkerConfig(writer, &failure),
                AttachTail(&source, writer),
                CreateDecoder(source),
                std::make_unique<RecordingSink>(sink_state),
                &worker) ==
                    control::ControlLiveWorkerCreateErrorV1::kNone &&
                worker != nullptr,
            "idempotent replay worker creates");
        if (worker == nullptr) {
            continue;
        }
        source.PublishAll();
        test->Expect(
            worker->StopAt(StopCursor(source, writer)),
            "idempotent replay worker accepts exact stop cursor");
        const bool run_result = worker->Run();
        test->Expect(
            run_result && failure.calls == 0U &&
                worker->Snapshot().emitted_control_records == 1U &&
                worker->Checkpoint().has_value(),
            replay == 0U
                ? "new durable sink publication is acknowledged"
                : "identical durable replay acknowledgement is accepted");
    }
    test->Expect(
        sink_state->published_new == 1U &&
            sink_state->accepted_identical == 1U &&
            sink_state->conflicts == 0U &&
            sink_state->records.size() == 1U &&
            sink_state->wires.size() == 1U,
        "identical replay retains exactly one physical canonical record");

    const std::vector<RecordSpec> conflicting_records{
        {2U,
         101U,
         static_cast<std::uint16_t>(sys::LogonResponse::MessageID),
         SuccessfulLogonBody(1U)},
    };
    StaticLiveSource conflict_source;
    l2flow::common::Identity128 conflict_writer{};
    test->Expect(
        BuildSource(
            conflicting_records, &conflict_source, &conflict_writer),
        "same-key conflicting fixture encodes");
    FailureState conflict_failure;
    std::unique_ptr<control::ControlLiveWorkerV1> conflict_worker;
    test->Expect(
        control::ControlLiveWorkerV1::Create(
            WorkerConfig(conflict_writer, &conflict_failure),
            AttachTail(&conflict_source, conflict_writer),
            CreateDecoder(conflict_source),
            std::make_unique<RecordingSink>(sink_state),
            &conflict_worker) ==
                control::ControlLiveWorkerCreateErrorV1::kNone &&
            conflict_worker != nullptr,
        "same-key conflicting worker creates");
    if (conflict_worker == nullptr) {
        return;
    }
    conflict_source.PublishAll();
    const bool conflict_run = conflict_worker->Run();
    test->Expect(
        !conflict_run && conflict_failure.calls == 1U &&
            conflict_worker->Snapshot().failure ==
                control::ControlLiveWorkerFailureV1::
                    kControlRecordSinkRejected &&
            sink_state->conflicts == 1U &&
            sink_state->records.size() == 1U &&
            !conflict_worker->Checkpoint().has_value(),
        "same idempotency key with different canonical bytes fail-stops");
}

void TestInitialCursorMismatchFailsClosed(TestContext* test) {
    const std::vector<RecordSpec> records{
        {2U,
         101U,
         static_cast<std::uint16_t>(sys::LogonResponse::MessageID),
         SuccessfulLogonBody()},
    };
    StaticLiveSource source;
    l2flow::common::Identity128 writer{};
    test->Expect(
        BuildSource(records, &source, &writer),
        "cursor-mismatch Raw fixture encodes");
    // These records were already append-visible before this worker's
    // generation-start sample. They must never be relabeled as evidence for
    // the next SDK Connect generation.
    source.PublishAll();
    FailureState failure;
    std::unique_ptr<control::ControlLiveWorkerV1> worker;
    test->Expect(
        control::ControlLiveWorkerV1::Create(
            WorkerConfig(writer, &failure),
            AttachTail(&source, writer),
            CreateDecoder(source),
            std::make_unique<RecordingSink>(
                std::make_shared<SinkState>()),
            &worker) ==
                control::ControlLiveWorkerCreateErrorV1::
                    kInitialAppendMismatch &&
            worker == nullptr,
        "old backlog cannot masquerade as a new live generation");

    // A zero-state decoder may only attach at the genesis segment/header.
    // Re-encode the otherwise valid fixture as segment 2 to prove that a
    // recovery append cursor cannot silently stand in for full replay.
    StaticLiveSource second_source;
    l2flow::common::Identity128 second_writer{};
    test->Expect(
        BuildSource(
            std::span<const RecordSpec>{},
            &second_source,
            &second_writer),
        "non-genesis cursor fixture encodes");
    second_source.segment.segment_sequence = 2U;
    second_source.segment.segment_base_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    second_source.control.segment_sequence = 2U;
    second_source.control.append_global_wal_pos =
        2U * ingress::kRawV1SegmentHeaderBytes;
    second_source.control.durable_global_wal_pos =
        second_source.control.append_global_wal_pos;
    ingress::RawV1SegmentHeaderWire second_header{};
    test->Expect(
        ingress::EncodeSegmentHeaderV1(
            second_source.segment, &second_header) ==
            ingress::RawV1Error::kNone,
        "second-segment mismatch fixture re-encodes");
    std::copy(
        second_header.begin(),
        second_header.end(),
        second_source.wire.begin());
    test->Expect(
        control::ControlLiveWorkerV1::Create(
            WorkerConfig(second_writer, &failure),
            AttachTail(&second_source, second_writer),
            CreateDecoder(second_source),
            std::make_unique<RecordingSink>(
                std::make_shared<SinkState>()),
            &worker) ==
                control::ControlLiveWorkerCreateErrorV1::
                    kInitialCursorMismatch &&
            worker == nullptr,
        "zero-state decoder rejects a non-genesis live-tail attach");

    StaticLiveSource fatal_source;
    l2flow::common::Identity128 fatal_writer{};
    test->Expect(
        BuildSource(
            std::span<const RecordSpec>{},
            &fatal_source,
            &fatal_writer),
        "fatal generation-start fixture encodes");
    fatal_source.control.fatal_state = 1U;
    test->Expect(
        control::ControlLiveWorkerV1::Create(
            WorkerConfig(fatal_writer, &failure),
            AttachTail(&fatal_source, fatal_writer),
            CreateDecoder(fatal_source),
            std::make_unique<RecordingSink>(
                std::make_shared<SinkState>()),
            &worker) ==
                control::ControlLiveWorkerCreateErrorV1::
                    kInitialControlInvalid &&
            worker == nullptr,
        "fatal Raw control cannot start an SDK generation");

    StaticLiveSource malformed_frontier_source;
    l2flow::common::Identity128 malformed_frontier_writer{};
    test->Expect(
        BuildSource(
            std::span<const RecordSpec>{},
            &malformed_frontier_source,
            &malformed_frontier_writer),
        "malformed generation-start frontier fixture encodes");
    std::unique_ptr<ingress::RawLiveTail> malformed_frontier_tail =
        AttachTail(
            &malformed_frontier_source,
            malformed_frontier_writer);
    malformed_frontier_source.control.append_global_wal_pos += 8U;
    malformed_frontier_source.control.append_segment_offset += 8U;
    test->Expect(
        control::ControlLiveWorkerV1::Create(
            WorkerConfig(malformed_frontier_writer, &failure),
            std::move(malformed_frontier_tail),
            CreateDecoder(malformed_frontier_source),
            std::make_unique<RecordingSink>(
                std::make_shared<SinkState>()),
            &worker) ==
                control::ControlLiveWorkerCreateErrorV1::
                    kInitialControlInvalid &&
            worker == nullptr,
        "sequence/byte-incoherent Raw cursor cannot start a generation");
}

void TestSinkFailureRevokesCheckpoint(TestContext* test) {
    const std::vector<RecordSpec> records{
        {2U,
         101U,
         static_cast<std::uint16_t>(sys::LogonResponse::MessageID),
         SuccessfulLogonBody()},
    };
    StaticLiveSource source;
    l2flow::common::Identity128 writer{};
    test->Expect(
        BuildSource(records, &source, &writer),
        "sink-failure Raw fixture encodes");
    FailureState failure;
    std::unique_ptr<control::ControlLiveWorkerV1> worker;
    test->Expect(
        control::ControlLiveWorkerV1::Create(
            WorkerConfig(writer, &failure),
            AttachTail(&source, writer),
            CreateDecoder(source),
            std::make_unique<FixedOutcomeSink>(
                control::ControlRecordPublishResultV1::kFailure),
            &worker) == control::ControlLiveWorkerCreateErrorV1::kNone &&
            worker != nullptr,
        "rejecting control-record sink worker creates");
    if (worker == nullptr) {
        return;
    }
    source.PublishAll();
    const bool run_result = worker->Run();
    const control::ControlLiveWorkerSnapshotV1 snapshot =
        worker->Snapshot();
    test->Expect(
        !run_result && snapshot.finished && !snapshot.decoder_healthy &&
            snapshot.failure ==
                control::ControlLiveWorkerFailureV1::
                    kControlRecordSinkRejected &&
            failure.calls == 1U,
        "derived-record sink failure fail-stops the worker");
    test->Expect(
        !worker->Checkpoint().has_value(),
        "failed sink generation cannot expose a publishable checkpoint");
}

void TestLateSinkAcknowledgementFailsClosed(TestContext* test) {
    const std::vector<RecordSpec> records{
        {2U,
         101U,
         static_cast<std::uint16_t>(sys::LogonResponse::MessageID),
         SuccessfulLogonBody()},
    };
    StaticLiveSource source;
    l2flow::common::Identity128 writer{};
    test->Expect(
        BuildSource(records, &source, &writer),
        "late-sink Raw fixture encodes");
    FailureState failure;
    StepClock clock{{100U, 100U, 100U, 1'101U}, 0U};
    control::ControlLiveWorkerConfigV1 config =
        WorkerConfig(writer, &failure);
    config.monotonic_now = &StepClock::Now;
    config.monotonic_clock_context = &clock;
    config.record_publish_timeout_ns = 1'000U;
    auto sink_state = std::make_shared<SinkState>();
    std::unique_ptr<control::ControlLiveWorkerV1> worker;
    test->Expect(
        control::ControlLiveWorkerV1::Create(
            config,
            AttachTail(&source, writer),
            CreateDecoder(source),
            std::make_unique<RecordingSink>(sink_state),
            &worker) == control::ControlLiveWorkerCreateErrorV1::kNone &&
            worker != nullptr,
        "late-sink worker creates");
    if (worker == nullptr) {
        return;
    }
    source.PublishAll();
    const bool run_result = worker->Run();
    const control::ControlLiveWorkerSnapshotV1 snapshot =
        worker->Snapshot();
    test->Expect(
        !run_result && failure.calls == 1U &&
            snapshot.failure ==
                control::ControlLiveWorkerFailureV1::
                    kControlRecordSinkTimedOut &&
            !snapshot.decoder_healthy &&
            snapshot.emitted_control_records == 0U &&
            sink_state->published_new == 1U &&
            !worker->Checkpoint().has_value(),
        "acknowledgement after the absolute deadline fail-stops worker");
}

}  // namespace

int main() {
    TestContext test;
    TestLiveReadyAndExactStop(&test);
    TestMalformedPoisonsButDoesNotStopRawConsumption(&test);
    TestSameControlGenerationCannotQualifyLogon(&test);
    TestEmptyWorkerHasNoCheckpoint(&test);
    TestStopCursorArithmeticFailsClosed(&test);
    TestIdempotentSinkOutcomes(&test);
    TestInitialCursorMismatchFailsClosed(&test);
    TestSinkFailureRevokesCheckpoint(&test);
    TestLateSinkAcknowledgementFailsClosed(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase3 live-worker assertion(s) failed\n";
        return 1;
    }
    std::cout << "Phase3 control live-worker tests passed\n";
    return 0;
}
