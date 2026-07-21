#include "l2flow/ingress/raw_readiness_observer.h"
#include "l2flow/ingress/raw_v1.h"

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

namespace ingress = l2flow::ingress;
namespace mdl = datayes::mdl;
namespace sh = datayes::mdl::mdl_shl2_msg;

namespace {

constexpr std::uint32_t kSourceStreamId = 9001U;
constexpr std::uint32_t kCaptureDate = 20260718U;
constexpr std::uint8_t kMarketServiceId =
    static_cast<std::uint8_t>(mdl::MDLSID_MDL_SHL2);
constexpr std::uint8_t kSystemServiceId =
    static_cast<std::uint8_t>(mdl::MDLSID_MDL_SYS);
constexpr std::uint16_t kSystemVersion =
    static_cast<std::uint16_t>(
        mdl::mdl_sys_msg::LogonResponse::ServiceVer);
constexpr std::uint16_t kLogonMessageId =
    static_cast<std::uint16_t>(
        mdl::mdl_sys_msg::LogonResponse::MessageID);
constexpr std::uint16_t kSubscribeMessageId =
    static_cast<std::uint16_t>(
        mdl::mdl_sys_msg::SubscribeResponse::MessageID);
constexpr l2flow::sdk::MessageKey kRequired{
    kMarketServiceId, 101U, 24U};

struct TestContext final {
    void Expect(
        bool condition,
        const std::string& description) {
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
    bytes[offset] =
        static_cast<std::byte>(value & 0xffU);
    bytes[offset + 1U] =
        static_cast<std::byte>((value >> 8U) & 0xffU);
}

void StoreU32(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint32_t value) {
    for (std::size_t index = 0U; index < 4U; ++index) {
        bytes[offset + index] =
            static_cast<std::byte>(
                (value >> (index * 8U)) & 0xffU);
    }
}

void StoreU64(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint64_t value) {
    for (std::size_t index = 0U; index < 8U; ++index) {
        bytes[offset + index] =
            static_cast<std::byte>(
                (value >> (index * 8U)) & 0xffU);
    }
}

template <std::size_t Size>
void Fill(
    std::array<std::byte, Size>* output,
    std::uint8_t seed) {
    for (std::size_t index = 0U;
         index < output->size();
         ++index) {
        (*output)[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                seed +
                static_cast<std::uint8_t>(index)));
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

std::vector<std::byte> StatusBody(
    bool logon,
    std::uint32_t return_code,
    std::uint32_t status) {
    const std::size_t service_offset =
        logon ? 24U : 8U;
    const std::size_t services_descriptor =
        logon ? 12U : 0U;
    const std::size_t messages_descriptor =
        service_offset + 8U;
    const std::size_t message_offset =
        service_offset + 16U;
    std::vector<std::byte> body(
        message_offset + 8U, std::byte{0U});
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
    StoreU32(bytes, service_offset, kMarketServiceId);
    StoreU32(bytes, service_offset + 4U, 101U);
    StoreU32(bytes, messages_descriptor, 1U);
    StoreU32(bytes, messages_descriptor + 4U, 8U);
    StoreU32(bytes, message_offset, kRequired.message_id);
    StoreU32(bytes, message_offset + 4U, status);
    return body;
}

std::vector<std::byte> ValidMarketBody() {
    return std::vector<std::byte>(
        sizeof(sh::NGTSTick), std::byte{0U});
}

std::vector<std::byte> InvalidMarketBody() {
    std::vector<std::byte> body = ValidMarketBody();
    std::span<std::byte> bytes(body);
    const std::size_t descriptor =
        offsetof(sh::NGTSTick, SecurityID);
    StoreU16(bytes, descriptor, 1U);
    StoreU32(bytes, descriptor + 2U, 1U);
    return body;
}

struct RecordSpec final {
    std::uint8_t service_id = 0U;
    std::uint16_t service_version = 0U;
    std::uint16_t message_id = 0U;
    std::vector<std::byte> body;
};

ingress::RawSegmentScanResult BuildScan(
    const std::vector<RecordSpec>& records,
    std::uint64_t first_ingress_sequence = 1U,
    std::uint32_t segment_sequence = 1U,
    std::uint64_t segment_base_wal_pos = 0U) {
    ingress::SegmentHeaderV1 segment;
    segment.source_stream_id = kSourceStreamId;
    segment.capture_date = kCaptureDate;
    Fill(&segment.stream_day_id, 1U);
    segment.segment_sequence = segment_sequence;
    segment.segment_base_wal_pos =
        segment_base_wal_pos;
    segment.first_ingress_sequence =
        first_ingress_sequence;
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
    Fill(&segment.raw_schema_sha256, 141U);
    Fill(&segment.build_manifest_sha256, 161U);

    ingress::RawV1SegmentHeaderWire segment_wire{};
    const ingress::RawV1Error header_error =
        ingress::EncodeSegmentHeaderV1(
            segment, &segment_wire);
    if (header_error != ingress::RawV1Error::kNone) {
        return {};
    }
    auto wire =
        std::make_shared<std::vector<std::byte>>(
            segment_wire.begin(), segment_wire.end());

    std::uint64_t ingress_sequence =
        first_ingress_sequence;
    for (const RecordSpec& spec : records) {
        ingress::RawRecordInputV1 input;
        input.meta.source_stream_id = kSourceStreamId;
        input.meta.connection_epoch_hint = 7U;
        input.meta.ingress_sequence = ingress_sequence;
        input.meta.recv_realtime_ns =
            1'000U + ingress_sequence;
        input.meta.recv_monotonic_ns =
            2'000U + ingress_sequence;
        input.meta.capture_date = kCaptureDate;
        input.vendor_head = VendorHead(
            static_cast<std::uint32_t>(spec.body.size()),
            spec.service_id,
            spec.service_version,
            spec.message_id,
            ingress_sequence);
        input.vendor_body = spec.body;
        std::vector<std::byte> record_wire;
        if (ingress::EncodeRawRecordV1(
                input, &record_wire) !=
            ingress::RawV1Error::kNone) {
            return {};
        }
        wire->insert(
            wire->end(),
            record_wire.begin(),
            record_wire.end());
        ++ingress_sequence;
    }
    return ingress::ScanRawSegmentV1(
        wire, static_cast<std::uint64_t>(wire->size()));
}

l2flow::common::Identity128 WriterIdentity(
    std::uint8_t seed) {
    l2flow::common::Identity128 result{};
    Fill(&result, seed);
    return result;
}

ingress::RawReadinessObserverConfig Config() {
    ingress::RawReadinessObserverConfig config;
    config.source_stream_id = kSourceStreamId;
    config.market_service_id = kMarketServiceId;
    config.required_market_messages = {kRequired};
    config.maximum_lag_bytes = 16U;
    config.heartbeat_timeout_ns = 100U;
    return config;
}

std::unique_ptr<ingress::RawReadinessObserver>
CreateObserver(TestContext* test) {
    std::unique_ptr<ingress::RawReadinessObserver> observer;
    test->Expect(
        ingress::RawReadinessObserver::Create(
            Config(), &observer) ==
                ingress::RawReadinessObserverCreateError::
                    kNone &&
            observer != nullptr,
        "valid observer configuration creates an observer");
    return observer;
}

ingress::RawReadinessObserverGeneration Generation(
    const ingress::RawSegmentScanResult& scan,
    const l2flow::common::Identity128& writer,
    std::uint64_t connect_generation,
    std::size_t first_record = 0U) {
    ingress::RawReadinessObserverGeneration result;
    result.writer_instance = writer;
    result.stream_day_id = scan.segment.stream_day_id;
    result.source_stream_id = kSourceStreamId;
    result.capture_date = kCaptureDate;
    result.connect_generation = connect_generation;
    result.recovery_wal_pos =
        first_record < scan.records.size()
            ? scan.records[first_record].record_start_wal_pos()
            : scan.validated_end_wal_pos;
    result.recovery_next_ingress_sequence =
        first_record < scan.records.size()
            ? scan.records[first_record].header().ingress_sequence
            : scan.segment.first_ingress_sequence +
                  static_cast<std::uint64_t>(
                      scan.records.size());
    result.recovery_segment_sequence =
        scan.segment.segment_sequence;
    result.recovery_segment_offset =
        first_record < scan.records.size()
            ? scan.records[first_record]
                  .record_start_offset()
            : scan.validated_end_offset;
    return result;
}

ingress::RawControlSnapshot AppendSnapshot(
    const ingress::RawReadinessObserverGeneration& generation,
    std::uint64_t append_wal_pos,
    std::uint64_t heartbeat_monotonic_ns) {
    ingress::RawControlSnapshot result;
    result.writer_instance = generation.writer_instance;
    result.stream_day_id = generation.stream_day_id;
    result.source_stream_id = generation.source_stream_id;
    result.capture_date = generation.capture_date;
    result.segment_sequence =
        generation.recovery_segment_sequence;
    result.append_global_wal_pos = append_wal_pos;
    result.append_segment_offset =
        append_wal_pos -
        (generation.recovery_wal_pos -
         generation.recovery_segment_offset);
    result.heartbeat_monotonic_ns =
        heartbeat_monotonic_ns;
    return result;
}

ingress::RawLiveSegmentTransitionV1 MakeTransition(
    const ingress::RawSegmentScanResult& previous,
    const ingress::RawSegmentScanResult& next,
    const l2flow::common::Identity128& writer,
    std::uint64_t next_ingress_sequence) {
    ingress::RawLiveSegmentTransitionV1 result;
    result.writer_instance = writer;
    result.previous_segment = previous.segment;
    result.previous_segment_end_offset =
        previous.validated_end_offset;
    result.next_segment = next.segment;
    result.next_data_begin_wal_pos =
        next.segment.segment_base_wal_pos +
        ingress::kRawV1SegmentHeaderBytes;
    result.next_ingress_sequence =
        next_ingress_sequence;
    result.control_generation = 2U;
    return result;
}

void TestConfigAndGenerationValidation(
    TestContext* test) {
    std::unique_ptr<ingress::RawReadinessObserver> observer;
    ingress::RawReadinessObserverConfig invalid = Config();
    invalid.heartbeat_timeout_ns = 0U;
    test->Expect(
        ingress::RawReadinessObserver::Create(
            std::move(invalid), &observer) ==
            ingress::RawReadinessObserverCreateError::
                kInvalidConfig,
        "zero heartbeat timeout is rejected");

    invalid = Config();
    invalid.required_market_messages.push_back(kRequired);
    test->Expect(
        ingress::RawReadinessObserver::Create(
            std::move(invalid), &observer) ==
            ingress::RawReadinessObserverCreateError::
                kInvalidConfig,
        "duplicate required market messages are rejected");
}

void TestEvidenceAndSampledCatchup(TestContext* test) {
    const std::uint32_t ok =
        static_cast<std::uint32_t>(mdl::MDLEC_OK);
    ingress::RawSegmentScanResult scan = BuildScan({
        {kMarketServiceId, 101U, 24U, ValidMarketBody()},
        {kSystemServiceId,
         kSystemVersion,
         kLogonMessageId,
         StatusBody(true, ok, ok)},
        {kMarketServiceId, 101U, 24U, InvalidMarketBody()},
        {kMarketServiceId, 101U, 24U, ValidMarketBody()},
        {kSystemServiceId,
         kSystemVersion,
         kLogonMessageId,
         StatusBody(true, ok, ok)},
        {kMarketServiceId, 101U, 24U, ValidMarketBody()},
    });
    test->Expect(
        scan.ok() && scan.records.size() == 6U,
        "readiness fixture is valid Raw V1");
    if (!scan.ok() || scan.records.size() != 6U) {
        return;
    }

    std::unique_ptr<ingress::RawReadinessObserver> observer =
        CreateObserver(test);
    const l2flow::common::Identity128 writer =
        WriterIdentity(1U);
    const ingress::RawReadinessObserverGeneration generation =
        Generation(scan, writer, 11U);
    test->Expect(
        observer->BeginGeneration(generation, 1'000U),
        "fresh writer/Connect generation starts");
    test->Expect(
        !observer->BeginGeneration(generation, 1'001U),
        "same writer/Connect generation cannot be reused");

    ingress::RawObservationalGateResult gate =
        observer->Evaluate(
            AppendSnapshot(
                generation,
                generation.recovery_wal_pos,
                1'009U),
            1'010U);
    test->Expect(
        !gate.ready &&
            gate.reason ==
                ingress::RawObservationalGateReason::
                    kEvidenceIncomplete &&
            !gate.authoritative_epoch,
        "new generation is observationally NOT_READY");

    test->Expect(
        observer->Observe(scan.records[0], writer, 1'020U) ==
            ingress::RawReadinessObserveResult::kProcessed,
        "validated pre-logon market record is processed");
    test->Expect(
        observer->Snapshot().required_first_seen_mask == 1U,
        "pre-logon market evidence is visible before reset");

    test->Expect(
        observer->Observe(scan.records[1], writer, 1'030U) ==
            ingress::RawReadinessObserveResult::kProcessed,
        "fixed LogonResponse and subscription status are parsed");
    ingress::RawReadinessObserverSnapshot snapshot =
        observer->Snapshot();
    test->Expect(
        snapshot.latest_logon_ok &&
            snapshot.required_subscription_ok_mask == 1U &&
            snapshot.required_first_seen_mask == 0U &&
            !snapshot.evidence_complete,
        "LogonResponse starts a fresh evidence generation");

    test->Expect(
        observer->Observe(scan.records[2], writer, 1'040U) ==
            ingress::RawReadinessObserveResult::kProcessed &&
            observer->Snapshot().required_first_seen_mask == 0U,
        "invalid dynamic descriptor does not satisfy first-seen");
    test->Expect(
        observer->Observe(scan.records[3], writer, 1'050U) ==
            ingress::RawReadinessObserveResult::kProcessed,
        "legal required market body completes evidence");
    snapshot = observer->Snapshot();
    test->Expect(
        snapshot.evidence_complete &&
            snapshot.observer_processed_wal_pos ==
                scan.records[3].record_end_wal_pos() &&
            snapshot.observer_heartbeat_monotonic_ns == 1'050U &&
            !snapshot.authoritative_epoch,
        "observer publishes cursor, heartbeat and compatibility evidence");

    gate = observer->Evaluate(
        AppendSnapshot(
            generation,
            scan.records[3].record_end_wal_pos(),
            1'059U),
        1'060U);
    test->Expect(
        gate.ready &&
            gate.reason ==
                ingress::RawObservationalGateReason::kReady &&
            gate.connect_generation == 11U &&
            gate.logon_generation == 1U,
        "fresh append snapshot caught up on same writer is READY");

    ingress::RawControlSnapshot unhealthy_writer =
        AppendSnapshot(
            generation,
            scan.records[3].record_end_wal_pos(),
            1'059U);
    unhealthy_writer.fatal_state = 1U;
    gate = observer->Evaluate(
        unhealthy_writer, 1'060U);
    test->Expect(
        !gate.ready &&
            gate.reason ==
                ingress::RawObservationalGateReason::
                    kWriterFatal,
        "Raw writer fatal state revokes observational readiness");
    unhealthy_writer.fatal_state = 0U;
    unhealthy_writer.heartbeat_monotonic_ns = 0U;
    gate = observer->Evaluate(
        unhealthy_writer, 1'060U);
    test->Expect(
        !gate.ready &&
            gate.reason ==
                ingress::RawObservationalGateReason::
                    kWriterHeartbeatMissing,
        "missing control-page writer heartbeat is unhealthy");
    unhealthy_writer.heartbeat_monotonic_ns = 900U;
    gate = observer->Evaluate(
        unhealthy_writer, 1'060U);
    test->Expect(
        !gate.ready &&
            gate.reason ==
                ingress::RawObservationalGateReason::
                    kWriterHeartbeatTimedOut,
        "stale control-page writer heartbeat is unhealthy");

    gate = observer->Evaluate(
        AppendSnapshot(
            generation,
            scan.records[2].record_end_wal_pos(),
            1'059U),
        1'060U);
    test->Expect(
        gate.ready &&
            gate.observer_processed_wal_pos >
                gate.sampled_append_wal_pos,
        "observer advancement after append sampling still satisfies catch-up");

    gate = observer->Evaluate(
        AppendSnapshot(
            generation,
            scan.records[3].record_end_wal_pos() + 8U,
            1'059U),
        1'060U);
    test->Expect(
        !gate.ready &&
            gate.reason ==
                ingress::RawObservationalGateReason::
                    kNotCaughtUp &&
            gate.lag_bytes == 8U,
        "small sampled lag still fails the exact catch-up gate");
    gate = observer->Evaluate(
        AppendSnapshot(
            generation,
            scan.records[3].record_end_wal_pos() + 32U,
            1'059U),
        1'060U);
    test->Expect(
        !gate.ready &&
            gate.reason ==
                ingress::RawObservationalGateReason::
                    kLagExceeded &&
            gate.lag_bytes == 32U,
        "configured lag threshold has an explicit failure reason");

    ingress::RawControlSnapshot wrong_writer =
        AppendSnapshot(
            generation,
            scan.records[3].record_end_wal_pos(),
            1'059U);
    wrong_writer.writer_instance = WriterIdentity(91U);
    gate = observer->Evaluate(wrong_writer, 1'060U);
    test->Expect(
        !gate.ready &&
            gate.reason ==
                ingress::RawObservationalGateReason::
                    kWriterInstanceMismatch,
        "writer replacement fences cached readiness");

    gate = observer->Evaluate(
        AppendSnapshot(
            generation,
            scan.records[3].record_end_wal_pos(),
            1'150U),
        1'151U);
    test->Expect(
        !gate.ready &&
            gate.reason ==
                ingress::RawObservationalGateReason::
                    kHeartbeatTimedOut,
        "heartbeat timeout revokes readiness on every evaluation");
    test->Expect(
        observer->PublishHeartbeat(writer, 1'160U),
        "idle live-tail poll can publish observer heartbeat");
    gate = observer->Evaluate(
        AppendSnapshot(
            generation,
            scan.records[3].record_end_wal_pos(),
            1'160U),
        1'161U);
    test->Expect(
        gate.ready,
        "fresh heartbeat restores the sampled compatibility gate");

    test->Expect(
        observer->Observe(scan.records[4], writer, 1'170U) ==
            ingress::RawReadinessObserveResult::kProcessed &&
            !observer->Snapshot().evidence_complete,
        "replacement LogonResponse immediately resets readiness");
    test->Expect(
        observer->Observe(scan.records[5], writer, 1'180U) ==
            ingress::RawReadinessObserveResult::kProcessed,
        "new-generation first-seen evidence is accepted");
    gate = observer->Evaluate(
        AppendSnapshot(
            generation,
            scan.records[5].record_end_wal_pos(),
            1'180U),
        1'181U);
    test->Expect(
        gate.ready && gate.logon_generation == 2U,
        "replacement logon obtains a distinct observational generation");

    ingress::RawReadinessObserverGeneration next_generation =
        Generation(scan, writer, 12U, 4U);
    test->Expect(
        observer->BeginGeneration(next_generation, 2'000U) &&
            !observer->Snapshot().evidence_complete,
        "new Connect generation starts from NOT_READY");
    test->Expect(
        observer->Observe(scan.records[0], writer, 2'001U) ==
            ingress::RawReadinessObserveResult::
                kWalCursorMismatch,
        "historical Raw record before recovery cursor is rejected");
}

void TestReplayInputAndFailureMemory(TestContext* test) {
    const std::uint32_t ok =
        static_cast<std::uint32_t>(mdl::MDLEC_OK);
    const std::uint32_t failed = ok + 1U;
    ingress::RawSegmentScanResult scan = BuildScan({
        {kSystemServiceId,
         kSystemVersion,
         kLogonMessageId,
         StatusBody(true, ok, failed)},
        {kSystemServiceId,
         kSystemVersion,
         kSubscribeMessageId,
         StatusBody(false, 0U, ok)},
        {kMarketServiceId, 101U, 24U, ValidMarketBody()},
        {kSystemServiceId,
         kSystemVersion,
         kLogonMessageId,
         StatusBody(true, ok, ok)},
        {kMarketServiceId, 101U, 24U, ValidMarketBody()},
    });
    test->Expect(
        scan.ok() && scan.records.size() == 5U,
        "failure-memory fixture is valid Raw V1");
    if (!scan.ok() || scan.records.size() != 5U) {
        return;
    }

    std::unique_ptr<ingress::RawReadinessObserver> observer =
        CreateObserver(test);
    const l2flow::common::Identity128 writer =
        WriterIdentity(10U);
    const ingress::RawReadinessObserverGeneration generation =
        Generation(scan, writer, 1U);
    test->Expect(
        observer->BeginGeneration(generation, 100U),
        "failure-memory generation starts");

    ingress::RawReplayRecord replay{
        scan.records[0],
        {},
        ingress::RawReplayProvenance::kDurable};
    test->Expect(
        observer->Observe(replay, writer, 101U) ==
            ingress::RawReadinessObserveResult::kProcessed,
        "RawReplayRecord overload consumes its validated Raw view");
    for (std::size_t index = 1U;
         index < scan.records.size();
         ++index) {
        test->Expect(
            observer->Observe(
                scan.records[index],
                writer,
                101U + static_cast<std::uint64_t>(index)) ==
                ingress::RawReadinessObserveResult::kProcessed,
            "subsequent validated failure-memory record is processed");
    }
    const ingress::RawReadinessObserverSnapshot snapshot =
        observer->Snapshot();
    test->Expect(
        snapshot.latest_logon_ok &&
            snapshot.required_subscription_ok_mask == 1U &&
            snapshot.required_first_seen_mask == 1U &&
            !snapshot.evidence_complete,
        "observed subscription failure remains fail-closed in generation");
}

void TestExplicitSegmentTransition(
    TestContext* test) {
    const RecordSpec record{
        kMarketServiceId,
        101U,
        24U,
        ValidMarketBody()};
    const ingress::RawSegmentScanResult first =
        BuildScan({record}, 1U, 1U, 0U);
    const ingress::RawSegmentScanResult second =
        BuildScan(
            {record},
            2U,
            2U,
            first.validated_end_wal_pos);
    test->Expect(
        first.ok() && second.ok() &&
            first.records.size() == 1U &&
            second.records.size() == 1U,
        "two-segment readiness fixture is valid");
    if (!first.ok() || !second.ok() ||
        first.records.size() != 1U ||
        second.records.size() != 1U) {
        return;
    }

    const l2flow::common::Identity128 writer =
        WriterIdentity(71U);
    auto start_at_first =
        [&]() {
            auto observer = CreateObserver(test);
            const auto generation =
                Generation(first, writer, 1U);
            test->Expect(
                observer->BeginGeneration(
                    generation, 100U) &&
                    observer->Observe(
                        first.records[0],
                        writer,
                        101U) ==
                        ingress::
                            RawReadinessObserveResult::
                                kProcessed,
                "observer reaches the sealed first-segment frontier");
            return observer;
        };
    const auto transition =
        MakeTransition(first, second, writer, 2U);

    auto observer = start_at_first();
    test->Expect(
        observer->ObserveSegmentTransition(
            transition, 102U) ==
            ingress::RawReadinessObserveResult::
                kProcessed,
        "validated transition advances across the next segment header");
    auto snapshot = observer->Snapshot();
    test->Expect(
        snapshot.observer_processed_wal_pos ==
                second.records[0]
                    .record_start_wal_pos() &&
            snapshot
                    .observer_processed_ingress_sequence ==
                1U &&
            snapshot
                    .observer_processed_segment_sequence ==
                2U &&
            snapshot
                    .observer_processed_segment_offset ==
                ingress::kRawV1SegmentHeaderBytes,
        "transition advances exactly to next data-begin without consuming ingress");
    test->Expect(
        observer->Observe(
            second.records[0], writer, 103U) ==
            ingress::RawReadinessObserveResult::
                kProcessed,
        "first record after rotation starts at the explicit header frontier");

    test->Expect(
        observer->ObserveSegmentTransition(
            transition, 104U) ==
            ingress::RawReadinessObserveResult::
                kSegmentTransitionMismatch,
        "replayed transition is rejected after the frontier advanced");

    auto gap_observer = start_at_first();
    auto gap = transition;
    ++gap.next_segment.segment_base_wal_pos;
    ++gap.next_data_begin_wal_pos;
    test->Expect(
        gap_observer->ObserveSegmentTransition(
            gap, 102U) ==
            ingress::RawReadinessObserveResult::
                kSegmentTransitionMismatch &&
            gap_observer->Snapshot()
                    .observer_processed_wal_pos ==
                first.validated_end_wal_pos,
        "one-byte next-header base gap fails closed without moving the frontier");

    auto foreign_observer = start_at_first();
    auto foreign = transition;
    foreign.next_segment.stream_day_id[0U] ^=
        std::byte{0x55U};
    test->Expect(
        foreign_observer->ObserveSegmentTransition(
            foreign, 102U) ==
            ingress::RawReadinessObserveResult::
                kNamespaceMismatch &&
            !foreign_observer->Snapshot()
                 .observer_healthy,
        "foreign stream-day transition permanently invalidates readiness");
}

void TestMalformedControlIsUnhealthy(TestContext* test) {
    ingress::RawSegmentScanResult scan = BuildScan({
        {kSystemServiceId,
         kSystemVersion,
         kLogonMessageId,
         std::vector<std::byte>(8U, std::byte{0U})},
    });
    test->Expect(
        scan.ok() && scan.records.size() == 1U,
        "malformed control body still has valid Raw framing");
    if (!scan.ok() || scan.records.size() != 1U) {
        return;
    }
    std::unique_ptr<ingress::RawReadinessObserver> observer =
        CreateObserver(test);
    const l2flow::common::Identity128 writer =
        WriterIdentity(31U);
    const ingress::RawReadinessObserverGeneration generation =
        Generation(scan, writer, 1U);
    test->Expect(
        observer->BeginGeneration(generation, 10U),
        "malformed-control generation starts");
    test->Expect(
        observer->Observe(scan.records[0], writer, 20U) ==
            ingress::RawReadinessObserveResult::
                kMalformedControl,
        "bounds-invalid fixed control body is surfaced");
    const ingress::RawReadinessObserverSnapshot snapshot =
        observer->Snapshot();
    test->Expect(
        !snapshot.observer_healthy &&
            snapshot.observer_processed_wal_pos ==
                scan.records[0].record_end_wal_pos(),
        "validated Raw cursor advances while malformed control fails closed");
    const ingress::RawObservationalGateResult gate =
        observer->Evaluate(
            AppendSnapshot(
                generation,
                scan.records[0].record_end_wal_pos(),
                20U),
            21U);
    test->Expect(
        !gate.ready &&
            gate.reason ==
                ingress::RawObservationalGateReason::
                    kObserverUnhealthy,
        "malformed control prevents observational READY");
}

}  // namespace

int main() {
    TestContext test;
    TestConfigAndGenerationValidation(&test);
    TestEvidenceAndSampledCatchup(&test);
    TestReplayInputAndFailureMemory(&test);
    TestExplicitSegmentTransition(&test);
    TestMalformedControlIsUnhealthy(&test);
    if (test.failures == 0) {
        std::cout
            << "Phase-2 Raw readiness observer tests passed\n";
    }
    return test.failures == 0 ? 0 : 1;
}
