#include "l2flow/control/control_checkpoint_v1.h"
#include "l2flow/control/control_decoder.h"
#include "l2flow/control/control_readiness_gate.h"
#include "l2flow/control/control_record_v1.h"
#include "l2flow/control/quality_flags_v1.h"
#include "l2flow/ingress/raw_replay.h"
#include "l2flow/ingress/raw_schema.h"
#include "l2flow/ingress/raw_v1.h"

#include "mdl_api_msg.h"
#include "mdl_shl2_msg.h"
#include "mdl_sys_msg.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace control = l2flow::control;
namespace ingress = l2flow::ingress;
namespace mdl = datayes::mdl;
namespace api = datayes::mdl::mdl_api_msg;
namespace sh = datayes::mdl::mdl_shl2_msg;
namespace sys = datayes::mdl::mdl_sys_msg;

namespace {

constexpr std::uint32_t kSourceStreamId = 9001U;
constexpr std::uint32_t kCaptureDate = 20260721U;
constexpr l2flow::sdk::MessageKey kRequired{4U, 101U, 24U};
constexpr l2flow::sdk::MessageKey kOptional{4U, 101U, 4U};

struct TestContext final {
    void Expect(bool condition, const std::string& description) {
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
void Fill(
    std::array<std::byte, Size>* output,
    std::uint8_t seed) {
    for (std::size_t index = 0U; index < output->size(); ++index) {
        (*output)[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                seed + static_cast<std::uint8_t>(index)));
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

struct Status final {
    l2flow::sdk::MessageKey key{};
    std::uint32_t value = 0U;
};

std::vector<std::byte> StatusBody(
    bool logon,
    std::uint32_t return_code,
    std::span<const Status> statuses) {
    const std::size_t fixed_bytes = logon ? 24U : 8U;
    const std::size_t services_descriptor = logon ? 12U : 0U;
    const std::size_t service_offset = fixed_bytes;
    const std::size_t messages_descriptor = service_offset + 8U;
    const std::size_t messages_offset = service_offset + 16U;
    std::vector<std::byte> body(
        messages_offset + statuses.size() * 8U,
        std::byte{0U});
    std::span<std::byte> bytes(body);
    StoreU32(bytes, services_descriptor, 1U);
    StoreU32(
        bytes,
        services_descriptor + 4U,
        static_cast<std::uint32_t>(
            service_offset - services_descriptor));
    if (logon) {
        StoreU32(bytes, 20U, return_code);
    }
    StoreU32(bytes, service_offset, 4U);
    StoreU32(bytes, service_offset + 4U, 101U);
    StoreU32(
        bytes,
        messages_descriptor,
        static_cast<std::uint32_t>(statuses.size()));
    StoreU32(bytes, messages_descriptor + 4U, 8U);
    for (std::size_t index = 0U; index < statuses.size(); ++index) {
        StoreU32(
            bytes,
            messages_offset + index * 8U,
            statuses[index].key.message_id);
        StoreU32(
            bytes,
            messages_offset + index * 8U + 4U,
            statuses[index].value);
    }
    return body;
}

std::vector<std::byte> ApiBody(std::size_t fixed_bytes) {
    return std::vector<std::byte>(fixed_bytes, std::byte{0U});
}

std::vector<std::byte> MarketBody() {
    return std::vector<std::byte>(
        sizeof(sh::NGTSTick), std::byte{0U});
}

struct RecordSpec final {
    std::uint8_t service_id = 0U;
    std::uint16_t service_version = 0U;
    std::uint16_t message_id = 0U;
    std::vector<std::byte> body;
};

ingress::RawSegmentScanResult BuildScan(
    const std::vector<RecordSpec>& records,
    std::uint64_t segment_base_wal_pos = 0U) {
    ingress::SegmentHeaderV1 segment;
    segment.source_stream_id = kSourceStreamId;
    segment.capture_date = kCaptureDate;
    Fill(&segment.stream_day_id, 1U);
    segment.segment_sequence =
        segment_base_wal_pos == 0U ? 1U : 2U;
    segment.segment_base_wal_pos = segment_base_wal_pos;
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
    Fill(&segment.config_sha256, 201U);
    segment.raw_schema_sha256 = ingress::RawSchemaSha256Digest();
    Fill(&segment.build_manifest_sha256, 161U);

    ingress::RawV1SegmentHeaderWire segment_wire{};
    if (ingress::EncodeSegmentHeaderV1(segment, &segment_wire) !=
        ingress::RawV1Error::kNone) {
        return {};
    }
    auto wire = std::make_shared<std::vector<std::byte>>(
        segment_wire.begin(), segment_wire.end());
    std::uint64_t sequence = 1U;
    for (const RecordSpec& spec : records) {
        ingress::RawRecordInputV1 input;
        input.meta.source_stream_id = kSourceStreamId;
        input.meta.connection_epoch_hint = 99U;
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
        std::vector<std::byte> record_wire;
        if (ingress::EncodeRawRecordV1(input, &record_wire) !=
            ingress::RawV1Error::kNone) {
            return {};
        }
        wire->insert(
            wire->end(), record_wire.begin(), record_wire.end());
        ++sequence;
    }
    return ingress::ScanRawSegmentV1(
        wire, static_cast<std::uint64_t>(wire->size()));
}

control::ControlDecoderConfigV1 DecoderConfig(
    const ingress::RawSegmentScanResult& scan) {
    control::ControlDecoderConfigV1 config;
    config.source_stream_id = kSourceStreamId;
    config.capture_date = kCaptureDate;
    config.stream_day_id = scan.segment.stream_day_id;
    Fill(&config.stable_config_sha256, 201U);
    config.required = {kRequired};
    config.optional = {kOptional};
    return config;
}

std::unique_ptr<control::ControlDecoderV1> CreateDecoder(
    const ingress::RawSegmentScanResult& scan,
    TestContext* test) {
    std::unique_ptr<control::ControlDecoderV1> decoder;
    test->Expect(
        control::ControlDecoderV1::Create(
            DecoderConfig(scan), &decoder) ==
                control::ControlDecoderCreateErrorV1::kNone &&
            decoder != nullptr,
        "valid Phase3 decoder configuration creates");
    return decoder;
}

ingress::RawReplaySegmentContext SegmentContext(
    const ingress::RawSegmentScanResult& scan) {
    ingress::RawReplaySegmentContext context;
    context.source_stream_id = scan.segment.source_stream_id;
    context.capture_date = scan.segment.capture_date;
    context.stream_day_id = scan.segment.stream_day_id;
    context.segment_sequence = scan.segment.segment_sequence;
    context.segment_base_wal_pos =
        scan.segment.segment_base_wal_pos;
    context.config_sha256 = scan.segment.config_sha256;
    context.raw_schema_sha256 = scan.segment.raw_schema_sha256;
    context.clock_epoch.algorithm =
        scan.segment.clock_epoch_algorithm;
    context.clock_epoch.digest = scan.segment.clock_epoch_digest;
    context.clock_epoch.label = scan.segment.clock_epoch_label;
    return context;
}

ingress::RawLiveRecord LiveRecord(
    const ingress::RawSegmentScanResult& scan,
    std::size_t index) {
    l2flow::common::Identity128 writer{};
    Fill(&writer, 231U);
    return ingress::RawLiveRecord{
        scan.records[index],
        scan.segment,
        ingress::RawLiveRecordProvenance::kAppendVisible,
        writer,
        2U};
}

ingress::RawReplayRecord ReplayRecord(
    const ingress::RawSegmentScanResult& scan,
    std::size_t index) {
    return ingress::RawReplayRecord{
        scan.records[index],
        SegmentContext(scan),
        ingress::RawReplayProvenance::kDurable};
}

ingress::RawControlSnapshot RawSnapshot(
    const ingress::RawSegmentScanResult& scan,
    std::size_t last_record,
    const l2flow::common::Identity128& writer) {
    ingress::RawControlSnapshot snapshot;
    snapshot.writer_instance = writer;
    snapshot.stream_day_id = scan.segment.stream_day_id;
    snapshot.source_stream_id = scan.segment.source_stream_id;
    snapshot.capture_date = scan.segment.capture_date;
    snapshot.segment_sequence = scan.segment.segment_sequence;
    snapshot.append_global_wal_pos =
        scan.records[last_record].record_end_wal_pos();
    snapshot.append_ingress_sequence =
        scan.records[last_record].header().ingress_sequence;
    snapshot.append_segment_offset =
        scan.records[last_record].record_end_offset();
    snapshot.durable_global_wal_pos =
        snapshot.append_global_wal_pos;
    snapshot.durable_ingress_sequence =
        snapshot.append_ingress_sequence;
    snapshot.durable_segment_offset =
        snapshot.append_segment_offset;
    snapshot.heartbeat_monotonic_ns = 100U;
    return snapshot;
}

std::vector<RecordSpec> ControlSequence() {
    const std::array<Status, 2U> both_ok{{
        Status{kRequired, 0U}, Status{kOptional, 0U}}};
    const std::array<Status, 2U> required_only{{
        Status{kRequired, 0U}, Status{kOptional, 1U}}};
    const std::array<Status, 1U> optional_failed{{
        Status{kOptional, 1U}}};
    return {
        {4U, 101U, 24U, MarketBody()},
        {2U,
         101U,
         static_cast<std::uint16_t>(sys::LogonResponse::MessageID),
         StatusBody(true, 1U, both_ok)},
        {1U,
         101U,
         static_cast<std::uint16_t>(api::ConnectingEvent::MessageID),
         ApiBody(sizeof(api::ConnectingEvent))},
        {2U,
         101U,
         static_cast<std::uint16_t>(sys::LogonResponse::MessageID),
         StatusBody(true, 0U, both_ok)},
        {4U, 101U, 24U, MarketBody()},
        {2U,
         101U,
         static_cast<std::uint16_t>(
             sys::SubscribeResponse::MessageID),
         StatusBody(false, 0U, optional_failed)},
        {1U,
         101U,
         static_cast<std::uint16_t>(
             api::DisconnectedEvent::MessageID),
         ApiBody(sizeof(api::DisconnectedEvent))},
        {4U, 101U, 24U, MarketBody()},
        {1U,
         101U,
         static_cast<std::uint16_t>(api::ConnectingEvent::MessageID),
         ApiBody(sizeof(api::ConnectingEvent))},
        {2U,
         101U,
         static_cast<std::uint16_t>(sys::LogonResponse::MessageID),
         StatusBody(true, 0U, required_only)},
        {4U, 101U, 24U, MarketBody()},
    };
}

void TestEpochsCheckpointAndReplay(TestContext* test) {
    const std::vector<RecordSpec> control_sequence =
        ControlSequence();
    const ingress::RawSegmentScanResult scan =
        BuildScan(control_sequence);
    test->Expect(
        scan.ok() && scan.records.size() == 11U,
        "CONTROL-001 Raw fixture validates");
    if (!scan.ok() || scan.records.size() != 11U) {
        return;
    }
    std::unique_ptr<control::ControlDecoderV1> live =
        CreateDecoder(scan, test);
    if (live == nullptr) {
        return;
    }

    ingress::RawLiveRecord wrong_live = LiveRecord(scan, 0U);
    wrong_live.segment.stream_day_id[0U] ^= std::byte{0x01};
    test->Expect(
        live->Process(wrong_live).error ==
                control::ControlProcessErrorV1::kLiveSegmentMismatch &&
            live->Snapshot().next_ingress_sequence == 1U,
        "live decoder rejects a record rebound to another stream-day");
    ingress::RawReplayRecord wrong_replay = ReplayRecord(scan, 0U);
    wrong_replay.segment.config_sha256[0U] ^= std::byte{0x01};
    test->Expect(
        live->Process(wrong_replay).error ==
                control::ControlProcessErrorV1::kReplaySegmentMismatch &&
            live->Snapshot().next_ingress_sequence == 1U,
        "replay decoder rejects a record rebound to another config");
    wrong_live = LiveRecord(scan, 0U);
    wrong_live.segment.segment_base_wal_pos +=
        ingress::kRawV1RecordAlignment;
    test->Expect(
        live->Process(wrong_live).error ==
                control::ControlProcessErrorV1::kLiveSegmentMismatch &&
            live->Snapshot().next_ingress_sequence == 1U,
        "live decoder rejects a view rebound to different segment coordinates");
    wrong_replay = ReplayRecord(scan, 0U);
    wrong_replay.segment.segment_base_wal_pos +=
        ingress::kRawV1RecordAlignment;
    test->Expect(
        live->Process(wrong_replay).error ==
                control::ControlProcessErrorV1::kReplaySegmentMismatch &&
            live->Snapshot().next_ingress_sequence == 1U,
        "replay decoder rejects a view rebound to different segment coordinates");
    wrong_live = LiveRecord(scan, 0U);
    wrong_live.control_generation = 0U;
    test->Expect(
        live->Process(wrong_live).error ==
                control::ControlProcessErrorV1::kLiveSegmentMismatch &&
            live->Snapshot().next_ingress_sequence == 1U,
        "live decoder requires a published control-page generation");

    const control::ControlProcessResultV1 prelogin =
        live->Process(LiveRecord(scan, 0U));
    test->Expect(
        prelogin.ok() && prelogin.cursor_committed &&
            prelogin.attribution.connection_epoch == 0U &&
            (prelogin.attribution.quality_flags &
             control::QualityBit(
                 control::QualityFlagV1::kSessionUnknown)) != 0U,
        "market before logon stays epoch0 with SESSION_UNKNOWN");
    const control::ControlProcessResultV1 failed_logon =
        live->Process(LiveRecord(scan, 1U));
    test->Expect(
        failed_logon.ok() &&
            failed_logon.control_record.has_value() &&
            failed_logon.control_record->control_type ==
                control::ControlTypeV1::kLogonFailure &&
            live->Snapshot().connection_epoch == 0U,
        "failed logon does not increment connection epoch");
    static_cast<void>(live->Process(LiveRecord(scan, 2U)));
    const control::ControlProcessResultV1 first_logon =
        live->Process(LiveRecord(scan, 3U));
    test->Expect(
        first_logon.ok() &&
            first_logon.control_record.has_value() &&
            first_logon.control_record->connection_epoch == 1U &&
            first_logon.control_record->subscription_epoch == 1U,
        "first successful logon atomically establishes epoch1");
    static_cast<void>(live->Process(LiveRecord(scan, 4U)));
    test->Expect(
        live->Snapshot().decoder_evidence_ready,
        "valid required market record completes decoder evidence");
    const control::ControlProcessResultV1 optional_failure =
        live->Process(LiveRecord(scan, 5U));
    const control::ControlDecoderSnapshotV1 checkpoint_state =
        live->Snapshot();
    test->Expect(
        optional_failure.ok() && checkpoint_state.control_ready &&
            checkpoint_state.decoder_evidence_ready &&
            checkpoint_state.subscription_epoch == 2U &&
            (optional_failure.attribution.quality_flags &
             control::QualityBit(
                 control::QualityFlagV1::kSubscriptionChanged)) != 0U,
        "optional failure changes effective set once without blocking READY");

    const control::ControlDecoderCheckpointV1 logical =
        live->Checkpoint();
    std::vector<std::byte> checkpoint_wire;
    test->Expect(
        control::ValidateControlDecoderCheckpointV1(logical) ==
                control::ControlCheckpointV1Error::kNone &&
            logical.state.current_address_sha256 !=
                l2flow::common::Sha256Digest{},
        "real decoder checkpoint satisfies tightened API-event reachability");
    test->Expect(
        control::EncodeControlDecoderCheckpointV1(
            logical, &checkpoint_wire) ==
            control::ControlCheckpointV1Error::kNone,
        "valid decoder checkpoint encodes");
    control::ControlDecoderCheckpointV1 decoded_checkpoint;
    test->Expect(
        control::DecodeControlDecoderCheckpointV1(
            checkpoint_wire, &decoded_checkpoint) ==
            control::ControlCheckpointV1Error::kNone,
        "checkpoint wire round-trips and validates");
    control::ControlDecoderCheckpointV1 forged_state =
        decoded_checkpoint;
    forged_state.state.state_sha256[0U] ^= std::byte{0x01};
    test->Expect(
        control::ValidateControlDecoderCheckpointV1(forged_state) ==
            control::ControlCheckpointV1Error::kInvalidModel,
        "checkpoint validation recomputes and binds state hash");
    control::ControlDecoderCheckpointV1 unreachable_epoch =
        decoded_checkpoint;
    unreachable_epoch.state.subscription_epoch = 0U;
    test->Expect(
        control::ComputeControlDecoderStateSha256V1(
            unreachable_epoch.state,
            &unreachable_epoch.state.state_sha256) &&
            control::ValidateControlDecoderCheckpointV1(
                unreachable_epoch) ==
                control::ControlCheckpointV1Error::kInvalidModel,
        "checkpoint rejects a hash-consistent but unreachable subscription epoch");
    unreachable_epoch = decoded_checkpoint;
    unreachable_epoch.state.connection_epoch = 0U;
    test->Expect(
        control::ComputeControlDecoderStateSha256V1(
            unreachable_epoch.state,
            &unreachable_epoch.state.state_sha256) &&
            control::ValidateControlDecoderCheckpointV1(
                unreachable_epoch) ==
                control::ControlCheckpointV1Error::kInvalidModel,
        "checkpoint rejects a hash-consistent but unreachable connection epoch");

    l2flow::common::Identity128 writer{};
    Fill(&writer, 77U);
    const ingress::RawControlSnapshot durable =
        RawSnapshot(scan, 5U, writer);
    std::unique_ptr<control::ControlDecoderV1> restored;
    test->Expect(
        control::ControlDecoderV1::Restore(
            DecoderConfig(scan),
            decoded_checkpoint,
            ReplayRecord(scan, 5U),
            durable,
            &restored) ==
                control::ControlDecoderCreateErrorV1::kNone &&
            restored != nullptr,
        "restore binds checkpoint to validated durable Raw boundary");

    const control::ControlProcessResultV1 disconnected =
        live->Process(LiveRecord(scan, 6U));
    const control::ControlDecoderCheckpointV1 disconnected_checkpoint =
        live->Checkpoint();
    test->Expect(
        disconnected.ok() &&
            disconnected.attribution.connection_epoch == 1U,
        "disconnect belongs to the old epoch");
    test->Expect(
        disconnected_checkpoint.state.disconnected_window &&
            disconnected_checkpoint.state.current_address_sha256 !=
                l2flow::common::Sha256Digest{} &&
            control::ValidateControlDecoderCheckpointV1(
                disconnected_checkpoint) ==
                control::ControlCheckpointV1Error::kNone,
        "real decoder disconnect checkpoint retains reachable address evidence");
    const control::ControlProcessResultV1 window_market =
        live->Process(LiveRecord(scan, 7U));
    const std::uint64_t disconnected_quality =
        control::QualityBit(
            control::QualityFlagV1::kSessionUnknown) |
        control::QualityBit(
            control::QualityFlagV1::kSourceDisconnected);
    test->Expect(
        window_market.ok() &&
            window_market.attribution.connection_epoch == 1U &&
            (window_market.attribution.quality_flags &
             disconnected_quality) == disconnected_quality,
        "market in disconnect window keeps old epoch and both flags");
    const control::ControlProcessResultV1 reconnecting =
        live->Process(LiveRecord(scan, 8U));
    test->Expect(
        (reconnecting.attribution.quality_flags &
         disconnected_quality) == disconnected_quality,
        "Connecting does not prematurely end disconnect window");
    const control::ControlProcessResultV1 second_logon =
        live->Process(LiveRecord(scan, 9U));
    test->Expect(
        second_logon.ok() &&
            second_logon.attribution.connection_epoch == 2U &&
            second_logon.attribution.subscription_epoch == 2U &&
            !live->Snapshot().decoder_evidence_ready,
        "reconnect increments connection only and resets market evidence");
    static_cast<void>(live->Process(LiveRecord(scan, 10U)));
    const control::ControlDecoderSnapshotV1 live_final =
        live->Snapshot();
    test->Expect(
        live_final.connection_epoch == 2U &&
            live_final.subscription_epoch == 2U &&
            live_final.decoder_evidence_ready,
        "post-reconnect market completes current-connection evidence");

    if (restored != nullptr) {
        for (std::size_t index = 6U; index < scan.records.size(); ++index) {
            const control::ControlProcessResultV1 result =
                restored->Process(ReplayRecord(scan, index));
            test->Expect(
                result.ok(),
                "checkpoint suffix replay commits each record");
        }
        test->Expect(
            restored->Snapshot().state_sha256 ==
                live_final.state_sha256,
            "checkpoint suffix reaches exact full live state hash");
    }

    std::unique_ptr<control::ControlDecoderV1> replay =
        CreateDecoder(scan, test);
    if (replay != nullptr) {
        for (std::size_t index = 0U; index < scan.records.size(); ++index) {
            const control::ControlProcessResultV1 result =
                replay->Process(ReplayRecord(scan, index));
            test->Expect(result.ok(), "full Raw replay commits");
        }
        test->Expect(
            replay->Snapshot().state_sha256 == live_final.state_sha256,
            "live and full replay control state hashes are identical");
    }

    ingress::RawControlSnapshot not_durable = durable;
    not_durable.durable_global_wal_pos =
        scan.records[4U].record_end_wal_pos();
    not_durable.durable_ingress_sequence = 5U;
    std::unique_ptr<control::ControlDecoderV1> rejected;
    test->Expect(
        control::ControlDecoderV1::Restore(
            DecoderConfig(scan),
            decoded_checkpoint,
            ReplayRecord(scan, 5U),
            not_durable,
            &rejected) ==
            control::ControlDecoderCreateErrorV1::kInvalidCheckpoint,
        "restore rejects a checkpoint beyond durable Raw frontier");
    ingress::RawControlSnapshot impossible_frontier = durable;
    impossible_frontier.append_global_wal_pos +=
        ingress::kRawV1RecordAlignment;
    test->Expect(
        control::ControlDecoderV1::Restore(
            DecoderConfig(scan),
            decoded_checkpoint,
            ReplayRecord(scan, 5U),
            impossible_frontier,
            &rejected) ==
                control::ControlDecoderCreateErrorV1::kInvalidCheckpoint,
        "restore rejects an arithmetically incoherent Raw frontier");
    ingress::RawControlSnapshot crossed_frontier = durable;
    ++crossed_frontier.append_ingress_sequence;
    ++crossed_frontier.durable_ingress_sequence;
    test->Expect(
        control::ControlDecoderV1::Restore(
            DecoderConfig(scan),
            decoded_checkpoint,
            ReplayRecord(scan, 5U),
            crossed_frontier,
            &rejected) ==
                control::ControlDecoderCreateErrorV1::kInvalidCheckpoint,
        "restore rejects a newer durable sequence at the same WAL cursor");

    ingress::RawControlSnapshot same_segment_gap = durable;
    same_segment_gap.append_global_wal_pos +=
        ingress::kRawV1RecordAlignment;
    same_segment_gap.durable_global_wal_pos =
        same_segment_gap.append_global_wal_pos;
    same_segment_gap.append_segment_offset +=
        ingress::kRawV1RecordAlignment;
    same_segment_gap.durable_segment_offset =
        same_segment_gap.append_segment_offset;
    test->Expect(
        control::ControlDecoderV1::Restore(
            DecoderConfig(scan),
            decoded_checkpoint,
            ReplayRecord(scan, 5U),
            same_segment_gap,
            &rejected) ==
                control::ControlDecoderCreateErrorV1::kInvalidCheckpoint,
        "restore rejects same-sequence WAL movement inside one segment");

    ingress::RawControlSnapshot next_header = durable;
    next_header.segment_sequence += 1U;
    next_header.append_global_wal_pos =
        decoded_checkpoint.state.processed_record_end_wal_pos +
        ingress::kRawV1SegmentHeaderBytes;
    next_header.durable_global_wal_pos =
        next_header.append_global_wal_pos;
    next_header.append_segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    next_header.durable_segment_offset =
        next_header.append_segment_offset;
    std::unique_ptr<control::ControlDecoderV1> header_restored;
    test->Expect(
        control::ControlDecoderV1::Restore(
            DecoderConfig(scan),
            decoded_checkpoint,
            ReplayRecord(scan, 5U),
            next_header,
            &header_restored) ==
                control::ControlDecoderCreateErrorV1::kNone &&
            header_restored != nullptr,
        "restore accepts a checkpoint immediately before a durable next-segment header");

    ingress::RawControlSnapshot malformed_next_header = next_header;
    malformed_next_header.append_global_wal_pos +=
        ingress::kRawV1RecordAlignment;
    malformed_next_header.durable_global_wal_pos =
        malformed_next_header.append_global_wal_pos;
    test->Expect(
        control::ControlDecoderV1::Restore(
            DecoderConfig(scan),
            decoded_checkpoint,
            ReplayRecord(scan, 5U),
            malformed_next_header,
            &rejected) ==
                control::ControlDecoderCreateErrorV1::kInvalidCheckpoint,
        "restore rejects a non-header WAL gap at unchanged ingress sequence");

    ingress::RawControlSnapshot impossible_record_count = durable;
    constexpr std::uint64_t kMinimumRecordBytes =
        ingress::kRawV1RecordHeaderBytes +
        ingress::kRawV1RecordTrailerBytes;
    impossible_record_count.append_global_wal_pos +=
        kMinimumRecordBytes;
    impossible_record_count.durable_global_wal_pos =
        impossible_record_count.append_global_wal_pos;
    impossible_record_count.append_segment_offset +=
        kMinimumRecordBytes;
    impossible_record_count.durable_segment_offset =
        impossible_record_count.append_segment_offset;
    impossible_record_count.append_ingress_sequence += 2U;
    impossible_record_count.durable_ingress_sequence =
        impossible_record_count.append_ingress_sequence;
    test->Expect(
        control::ControlDecoderV1::Restore(
            DecoderConfig(scan),
            decoded_checkpoint,
            ReplayRecord(scan, 5U),
            impossible_record_count,
            &rejected) ==
                control::ControlDecoderCreateErrorV1::kInvalidCheckpoint,
        "restore rejects two durable sequence increments in one minimum record span");

    ingress::RawReplayRecord future_segment_boundary =
        ReplayRecord(scan, 5U);
    future_segment_boundary.segment.segment_sequence =
        durable.segment_sequence + 1U;
    test->Expect(
        control::ControlDecoderV1::Restore(
            DecoderConfig(scan),
            decoded_checkpoint,
            future_segment_boundary,
            durable,
            &rejected) ==
            control::ControlDecoderCreateErrorV1::kInvalidCheckpoint,
        "restore rejects a boundary attributed to a future Raw segment");

    const ingress::RawSegmentScanResult rebased_scan =
        BuildScan(
            control_sequence,
            ingress::kRawV1SegmentHeaderBytes);
    control::ControlDecoderCheckpointV1 rebased_checkpoint =
        decoded_checkpoint;
    rebased_checkpoint.state.processed_record_start_wal_pos +=
        ingress::kRawV1SegmentHeaderBytes;
    rebased_checkpoint.state.processed_record_end_wal_pos +=
        ingress::kRawV1SegmentHeaderBytes;
    rebased_checkpoint.state.latest_response_record_end_wal_pos +=
        ingress::kRawV1SegmentHeaderBytes;
    const bool rebased_hash_valid =
        control::ComputeControlDecoderStateSha256V1(
            rebased_checkpoint.state,
            &rebased_checkpoint.state.state_sha256);
    const ingress::RawControlSnapshot later_same_segment_frontier =
        RawSnapshot(scan, scan.records.size() - 1U, writer);
    ingress::RawControlSnapshot rebound_same_segment_frontier =
        later_same_segment_frontier;
    rebound_same_segment_frontier.segment_sequence =
        rebased_scan.segment.segment_sequence;
    const std::uint64_t mismatched_frontier_base =
        2U * ingress::kRawV1SegmentHeaderBytes;
    rebound_same_segment_frontier.append_global_wal_pos +=
        mismatched_frontier_base;
    rebound_same_segment_frontier.durable_global_wal_pos +=
        mismatched_frontier_base;
    test->Expect(
        rebased_scan.ok() && rebased_scan.records.size() > 5U &&
            rebased_hash_valid &&
            control::ControlDecoderV1::Restore(
                DecoderConfig(scan),
                rebased_checkpoint,
                ReplayRecord(rebased_scan, 5U),
                rebound_same_segment_frontier,
                &rejected) ==
                control::ControlDecoderCreateErrorV1::kInvalidCheckpoint,
        "restore rejects a same-sequence boundary rebound to another segment base");

    ingress::RawControlSnapshot overlapping_older_segment = durable;
    const std::uint64_t forged_current_base =
        decoded_checkpoint.state.processed_record_end_wal_pos -
        ingress::kRawV1RecordAlignment;
    overlapping_older_segment.segment_sequence = 2U;
    overlapping_older_segment.append_global_wal_pos =
        forged_current_base + ingress::kRawV1SegmentHeaderBytes;
    overlapping_older_segment.durable_global_wal_pos =
        overlapping_older_segment.append_global_wal_pos;
    overlapping_older_segment.append_segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    overlapping_older_segment.durable_segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    test->Expect(
        control::ControlDecoderV1::Restore(
            DecoderConfig(scan),
            decoded_checkpoint,
            ReplayRecord(scan, 5U),
            overlapping_older_segment,
            &rejected) ==
            control::ControlDecoderCreateErrorV1::kInvalidCheckpoint,
        "restore rejects an older-segment record overlapping the current segment base");
}

void TestMalformedPoison(TestContext* test) {
    const std::array<Status, 1U> ok{{Status{kRequired, 0U}}};
    const ingress::RawSegmentScanResult scan = BuildScan({
        {2U,
         0U,
         static_cast<std::uint16_t>(sys::LogonResponse::MessageID),
         StatusBody(true, 0U, ok)},
        {2U,
         101U,
         static_cast<std::uint16_t>(sys::LogonResponse::MessageID),
         StatusBody(true, 0U, ok)},
        {1U,
         101U,
         static_cast<std::uint16_t>(
             api::ConnectErrorEvent::MessageID),
         ApiBody(sizeof(api::ConnectErrorEvent))},
        {1U,
         101U,
         static_cast<std::uint16_t>(
             api::DisconnectedEvent::MessageID),
         ApiBody(sizeof(api::DisconnectedEvent))},
    });
    test->Expect(scan.ok(), "malformed-control fixture validates as Raw");
    if (!scan.ok()) {
        return;
    }
    std::unique_ptr<control::ControlDecoderV1> decoder =
        CreateDecoder(scan, test);
    if (decoder == nullptr) {
        return;
    }
    const control::ControlProcessResultV1 malformed =
        decoder->Process(LiveRecord(scan, 0U));
    test->Expect(
        malformed.error ==
                control::ControlProcessErrorV1::kMalformedSysControl &&
            malformed.cursor_committed && malformed.control_state_poisoned &&
            malformed.control_record.has_value() &&
            malformed.control_record->control_type ==
                control::ControlTypeV1::kDecodeError &&
            malformed.control_record->vendor_service_version == 0U,
        "unsupported recognized control version commits bounded poison");
    control::ControlRecordWireV1 encoded{};
    test->Expect(
        malformed.control_record.has_value() &&
            control::EncodeControlRecordV1(
                *malformed.control_record, &encoded) ==
                control::ControlRecordV1Error::kNone,
        "DecodeError record preserves and encodes zero vendor version");
    const control::ControlProcessResultV1 later =
        decoder->Process(LiveRecord(scan, 1U));
    const control::ControlDecoderSnapshotV1 state = decoder->Snapshot();
    test->Expect(
        later.ok() && later.cursor_committed && state.poisoned &&
            state.connection_epoch == 0U &&
            !state.control_ready && !state.decoder_evidence_ready,
        "later valid control advances audit cursor but poison is sticky");
    test->Expect(
        later.control_record.has_value() &&
            control::EncodeControlRecordV1(
                *later.control_record, &encoded) ==
                control::ControlRecordV1Error::kNone,
        "valid LogonSuccess emitted after poison remains codec-reachable");

    const control::ControlProcessResultV1 poisoned_connect_error =
        decoder->Process(LiveRecord(scan, 2U));
    const control::ControlProcessResultV1 poisoned_disconnect =
        decoder->Process(LiveRecord(scan, 3U));
    const std::uint64_t source_disconnected = control::QualityBit(
        control::QualityFlagV1::kSourceDisconnected);
    test->Expect(
        poisoned_connect_error.control_record.has_value() &&
            (poisoned_connect_error.control_record->quality_flags &
             source_disconnected) == 0U &&
            control::EncodeControlRecordV1(
                *poisoned_connect_error.control_record, &encoded) ==
                control::ControlRecordV1Error::kNone,
        "poison-frozen ConnectError without a disconnect window encodes");
    test->Expect(
        poisoned_disconnect.control_record.has_value() &&
            (poisoned_disconnect.control_record->quality_flags &
             source_disconnected) == 0U &&
            control::EncodeControlRecordV1(
                *poisoned_disconnect.control_record, &encoded) ==
                control::ControlRecordV1Error::kNone,
        "poison-frozen Disconnected without a disconnect window encodes");
}

void TestFiveReconnects(TestContext* test) {
    const std::array<Status, 1U> ok{{Status{kRequired, 0U}}};
    std::vector<RecordSpec> records;
    records.push_back(
        {2U,
         101U,
         static_cast<std::uint16_t>(sys::LogonResponse::MessageID),
         StatusBody(true, 0U, ok)});
    for (std::size_t index = 0U; index < 5U; ++index) {
        records.push_back(
            {1U,
             101U,
             static_cast<std::uint16_t>(
                 api::DisconnectedEvent::MessageID),
             ApiBody(sizeof(api::DisconnectedEvent))});
        records.push_back(
            {2U,
             101U,
             static_cast<std::uint16_t>(
                 sys::LogonResponse::MessageID),
             StatusBody(true, 0U, ok)});
    }
    const ingress::RawSegmentScanResult scan = BuildScan(records);
    std::unique_ptr<control::ControlDecoderV1> decoder =
        CreateDecoder(scan, test);
    if (!scan.ok() || decoder == nullptr) {
        return;
    }
    for (std::size_t index = 0U; index < scan.records.size(); ++index) {
        test->Expect(
            decoder->Process(LiveRecord(scan, index)).ok(),
            "reconnect step commits");
    }
    const control::ControlDecoderSnapshotV1 state = decoder->Snapshot();
    test->Expect(
        state.connection_epoch == 6U &&
            state.subscription_epoch == 1U &&
            state.counters.disconnect == 5U,
        "five disconnect/reconnect cycles produce exact epochs");
}

void TestRequiredSubscriptionFailureRevokesReady(TestContext* test) {
    const std::array<Status, 1U> required_ok{{
        Status{kRequired, 0U}}};
    const std::array<Status, 1U> required_failed{{
        Status{kRequired, 7U}}};
    const ingress::RawSegmentScanResult scan = BuildScan({
        {2U,
         101U,
         static_cast<std::uint16_t>(sys::LogonResponse::MessageID),
         StatusBody(true, 0U, required_ok)},
        {4U, 101U, 24U, MarketBody()},
        {2U,
         101U,
         static_cast<std::uint16_t>(
             sys::SubscribeResponse::MessageID),
         StatusBody(false, 0U, required_failed)},
        {2U,
         101U,
         static_cast<std::uint16_t>(
             sys::SubscribeResponse::MessageID),
         StatusBody(false, 0U, required_ok)},
        {4U, 101U, 24U, MarketBody()},
    });
    std::unique_ptr<control::ControlDecoderV1> decoder =
        CreateDecoder(scan, test);
    if (!scan.ok() || decoder == nullptr) {
        return;
    }

    static_cast<void>(decoder->Process(LiveRecord(scan, 0U)));
    static_cast<void>(decoder->Process(LiveRecord(scan, 1U)));
    const control::ControlDecoderSnapshotV1 initially_ready =
        decoder->Snapshot();
    test->Expect(
        initially_ready.control_ready &&
            initially_ready.decoder_evidence_ready &&
            initially_ready.subscription_epoch == 1U,
        "successful logon and required market evidence establish READY");

    const control::ControlProcessResultV1 failed =
        decoder->Process(LiveRecord(scan, 2U));
    const control::ControlDecoderSnapshotV1 revoked =
        decoder->Snapshot();
    test->Expect(
        failed.ok() && failed.control_record.has_value() &&
            failed.control_record->control_type ==
                control::ControlTypeV1::kSubscriptionRejected &&
            revoked.subscription_epoch == 2U &&
            revoked.required_first_seen_mask == 0U &&
            !revoked.control_ready &&
            !revoked.decoder_evidence_ready,
        "required subscription failure changes epoch and immediately revokes READY");

    l2flow::common::Identity128 writer{};
    Fill(&writer, 89U);
    const ingress::RawControlSnapshot raw =
        RawSnapshot(scan, 2U, writer);
    control::ControlReadinessRuntimeV1 runtime;
    runtime.writer_instance = writer;
    runtime.processed_wal_pos = revoked.processed_record_end_wal_pos;
    runtime.processed_ingress_sequence =
        revoked.processed_ingress_sequence;
    runtime.decoder_heartbeat_monotonic_ns = 100U;
    runtime.connect_generation = 1U;
    runtime.generation_first_ingress_sequence = 1U;
    runtime.successful_logon_connect_generation = 1U;
    runtime.successful_logon_ingress_sequence = 1U;
    runtime.decoder_healthy = true;
    runtime.capture_pipeline_healthy = true;
    const control::ControlReadinessGateConfigV1 gate_config{
        1024U, 1024U, 50U};
    const control::ControlReadinessResultV1 gate_revoked =
        control::EvaluateControlReadinessV1(
            gate_config,
            revoked,
            runtime,
            raw,
            std::nullopt,
            110U);
    test->Expect(
        !gate_revoked.ready &&
            gate_revoked.reason ==
                control::ControlReadinessReasonV1::
                    kControlEvidenceIncomplete,
        "final readiness gate rejects the required-failure state");

    const control::ControlProcessResultV1 recovered =
        decoder->Process(LiveRecord(scan, 3U));
    const control::ControlDecoderSnapshotV1 awaiting_market =
        decoder->Snapshot();
    test->Expect(
        recovered.ok() && recovered.control_record.has_value() &&
            recovered.control_record->control_type ==
                control::ControlTypeV1::kSubscriptionAccepted &&
            awaiting_market.subscription_epoch == 3U &&
            awaiting_market.control_ready &&
            !awaiting_market.decoder_evidence_ready &&
            awaiting_market.required_first_seen_mask == 0U,
        "required recovery changes epoch but cannot reuse old market evidence");

    static_cast<void>(decoder->Process(LiveRecord(scan, 4U)));
    test->Expect(
        decoder->Snapshot().decoder_evidence_ready,
        "new required market evidence restores decoder readiness");
}

void TestFinalReadinessGate(TestContext* test) {
    const std::array<Status, 1U> ok{{Status{kRequired, 0U}}};
    const ingress::RawSegmentScanResult scan = BuildScan({
        {2U,
         101U,
         static_cast<std::uint16_t>(sys::LogonResponse::MessageID),
         StatusBody(true, 0U, ok)},
        {4U, 101U, 24U, MarketBody()},
        {4U, 101U, 24U, MarketBody()},
    });
    std::unique_ptr<control::ControlDecoderV1> decoder =
        CreateDecoder(scan, test);
    if (!scan.ok() || decoder == nullptr) {
        return;
    }
    static_cast<void>(decoder->Process(LiveRecord(scan, 0U)));
    static_cast<void>(decoder->Process(LiveRecord(scan, 1U)));
    const control::ControlDecoderSnapshotV1 state = decoder->Snapshot();
    l2flow::common::Identity128 writer{};
    Fill(&writer, 88U);
    ingress::RawControlSnapshot raw = RawSnapshot(scan, 1U, writer);
    control::ControlReadinessRuntimeV1 runtime;
    runtime.writer_instance = writer;
    runtime.processed_wal_pos = state.processed_record_end_wal_pos;
    runtime.processed_ingress_sequence =
        state.processed_ingress_sequence;
    runtime.decoder_heartbeat_monotonic_ns = 100U;
    runtime.connect_generation = 1U;
    runtime.generation_first_ingress_sequence = 1U;
    runtime.successful_logon_connect_generation = 1U;
    runtime.successful_logon_ingress_sequence = 1U;
    runtime.decoder_healthy = true;
    runtime.capture_pipeline_healthy = true;
    control::ControlReadinessGateConfigV1 config{
        1024U, 1024U, 50U};
    const control::ControlReadinessResultV1 ready =
        control::EvaluateControlReadinessV1(
            config, state, runtime, raw, std::nullopt, 110U);
    test->Expect(
        ready.ready &&
            ready.reason == control::ControlReadinessReasonV1::kReady,
        "final READY uses coherent caught-up Raw sample and heartbeats");

    ingress::RawControlSnapshot incoherent_raw = raw;
    incoherent_raw.append_global_wal_pos +=
        ingress::kRawV1RecordAlignment;
    const control::ControlReadinessResultV1 incoherent =
        control::EvaluateControlReadinessV1(
            config,
            state,
            runtime,
            incoherent_raw,
            std::nullopt,
            110U);
    test->Expect(
        !incoherent.ready &&
            incoherent.reason ==
                control::ControlReadinessReasonV1::
                    kRawFrontierInvalid,
        "READY rejects an arithmetically incoherent Raw snapshot");

    ingress::RawControlSnapshot crossed_cursor = raw;
    ++crossed_cursor.append_ingress_sequence;
    ++crossed_cursor.durable_ingress_sequence;
    const control::ControlReadinessResultV1 crossed =
        control::EvaluateControlReadinessV1(
            config,
            state,
            runtime,
            crossed_cursor,
            std::nullopt,
            110U);
    test->Expect(
        !crossed.ready &&
            crossed.reason == control::ControlReadinessReasonV1::
                                  kDecoderCursorInvalid,
        "READY rejects equal-WAL but newer-sequence crossed cursors");

    constexpr std::uint64_t kMinimumRecordBytes =
        ingress::kRawV1RecordHeaderBytes +
        ingress::kRawV1RecordTrailerBytes;
    raw.append_global_wal_pos += kMinimumRecordBytes;
    raw.append_segment_offset += kMinimumRecordBytes;
    raw.append_ingress_sequence += 1U;
    const control::ControlReadinessResultV1 lagged =
        control::EvaluateControlReadinessV1(
            config, state, runtime, raw, std::nullopt, 110U);
    test->Expect(
        !lagged.ready && lagged.reason ==
            control::ControlReadinessReasonV1::kDecoderNotCaughtUp,
        "READY is never cached across a newer append sample");

    raw = RawSnapshot(scan, 1U, writer);
    const control::ControlProcessResultV1 advanced =
        decoder->Process(LiveRecord(scan, 2U));
    const control::ControlDecoderSnapshotV1 advanced_state =
        decoder->Snapshot();
    runtime.processed_wal_pos =
        advanced_state.processed_record_end_wal_pos;
    runtime.processed_ingress_sequence =
        advanced_state.processed_ingress_sequence;
    const control::ControlReadinessResultV1 advanced_after_sample =
        control::EvaluateControlReadinessV1(
            config,
            advanced_state,
            runtime,
            raw,
            std::nullopt,
            110U);
    test->Expect(
        advanced.ok() && advanced_after_sample.ready &&
            advanced_after_sample.reason ==
                control::ControlReadinessReasonV1::kReady,
        "decoder progress after Raw sampling still proves catch-up through that sample");

    raw = RawSnapshot(scan, 2U, writer);
    runtime.processed_wal_pos =
        advanced_state.processed_record_end_wal_pos +
        ingress::kRawV1SegmentHeaderBytes;
    const control::ControlReadinessResultV1 header_advanced =
        control::EvaluateControlReadinessV1(
            config,
            advanced_state,
            runtime,
            raw,
            std::nullopt,
            110U);
    test->Expect(
        header_advanced.ready,
        "a validated segment header may advance decoder WAL without advancing ingress sequence");
    runtime.processed_wal_pos =
        advanced_state.processed_record_end_wal_pos +
        ingress::kRawV1RecordAlignment;
    const control::ControlReadinessResultV1 impossible_header =
        control::EvaluateControlReadinessV1(
            config,
            advanced_state,
            runtime,
            raw,
            std::nullopt,
            110U);
    test->Expect(
        !impossible_header.ready &&
            impossible_header.reason ==
                control::ControlReadinessReasonV1::
                    kDecoderCursorInvalid,
        "WAL-only decoder progress must be an integral fixed segment header");

    raw = RawSnapshot(scan, 1U, writer);
    runtime.processed_wal_pos = state.processed_record_end_wal_pos;
    runtime.processed_ingress_sequence =
        state.processed_ingress_sequence;
    runtime.generation_first_ingress_sequence =
        state.next_ingress_sequence;
    const control::ControlReadinessResultV1 restarted =
        control::EvaluateControlReadinessV1(
            config, state, runtime, raw, std::nullopt, 110U);
    test->Expect(
        !restarted.ready && restarted.reason ==
            control::ControlReadinessReasonV1::
                kCurrentGenerationLogonMissing,
        "a new live generation cannot reuse checkpoint-era logon evidence");
    runtime.generation_first_ingress_sequence = 1U;

    std::unique_ptr<control::ControlDecoderV1> calendar_decoder =
        CreateDecoder(scan, test);
    if (calendar_decoder == nullptr) {
        return;
    }
    static_cast<void>(
        calendar_decoder->Process(LiveRecord(scan, 0U)));
    const control::ControlDecoderSnapshotV1 calendar_state =
        calendar_decoder->Snapshot();
    ingress::RawControlSnapshot calendar_raw =
        RawSnapshot(scan, 0U, writer);
    control::MarketSilenceProofV1 proof;
    proof.schema_version =
        control::kMarketSilenceProofSchemaVersionV1;
    proof.source_stream_id = kSourceStreamId;
    proof.capture_date = kCaptureDate;
    proof.stream_day_id = scan.segment.stream_day_id;
    Fill(&proof.calendar_sha256, 44U);
    config.approved_calendar_sha256 = proof.calendar_sha256;
    proof.valid_from_realtime_ns = 1'000U;
    proof.valid_until_realtime_ns = 2'000U;
    proof.evaluated_realtime_ns = 1'100U;
    proof.required_market_records_not_expected = true;
    runtime.processed_wal_pos =
        calendar_state.processed_record_end_wal_pos;
    runtime.processed_ingress_sequence =
        calendar_state.processed_ingress_sequence;
    runtime.sampled_realtime_ns = 1'500U;
    const control::ControlReadinessResultV1 calendar_ready =
        control::EvaluateControlReadinessV1(
            config,
            calendar_state,
            runtime,
            calendar_raw,
            proof,
            110U);
    test->Expect(
        calendar_ready.ready &&
            calendar_ready.calendar_exception_used,
        "namespace-bound calendar proof supplies the no-market exception");
    runtime.sampled_realtime_ns = 2'000U;
    const control::ControlReadinessResultV1 expired_calendar =
        control::EvaluateControlReadinessV1(
            config,
            calendar_state,
            runtime,
            calendar_raw,
            proof,
            110U);
    test->Expect(
        !expired_calendar.ready && expired_calendar.reason ==
            control::ControlReadinessReasonV1::kCalendarProofInvalid,
        "calendar exception expires at its explicit realtime boundary");
}

}  // namespace

int main() {
    TestContext test;
    TestEpochsCheckpointAndReplay(&test);
    TestMalformedPoison(&test);
    TestFiveReconnects(&test);
    TestRequiredSubscriptionFailureRevokesReady(&test);
    TestFinalReadinessGate(&test);
    if (test.failures != 0) {
        std::cerr << test.failures << " Phase3 decoder test(s) failed\n";
        return 1;
    }
    std::cout << "Phase3 control decoder tests passed\n";
    return 0;
}
