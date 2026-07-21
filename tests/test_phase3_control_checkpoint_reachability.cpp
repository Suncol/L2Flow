#include "l2flow/control/control_checkpoint_v1.h"
#include "l2flow/control/quality_flags_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string_view>

namespace control = l2flow::control;

namespace {

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
    for (std::size_t index = 0U; index < Size; ++index) {
        (*output)[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(
                seed + static_cast<std::uint8_t>(index)));
    }
}

void Rehash(control::ControlDecoderCheckpointV1* checkpoint) {
    if (!control::ComputeControlDecoderStateSha256V1(
            checkpoint->state,
            &checkpoint->state.state_sha256)) {
        throw std::runtime_error("state hash failed");
    }
}

[[nodiscard]] bool Valid(
    const control::ControlDecoderCheckpointV1& checkpoint) {
    return control::ValidateControlDecoderCheckpointV1(checkpoint) ==
           control::ControlCheckpointV1Error::kNone;
}

control::ControlDecoderCheckpointV1 InitialCheckpoint() {
    control::ControlDecoderCheckpointV1 checkpoint;
    control::ControlDecoderSnapshotV1& state = checkpoint.state;
    state.source_stream_id = 9001U;
    state.capture_date = 20260721U;
    Fill(&state.stream_day_id, 0x10U);
    Fill(&state.stable_config_sha256, 0x30U);
    state.subscriptions.push_back(
        control::ControlSubscriptionStateV1{
            l2flow::sdk::MessageKey{4U, 101U, 24U},
            control::SubscriptionPolicyV1::kRequired,
            0U,
            false});
    state.requested_manifest_sha256 =
        control::ComputeRequestedSubscriptionManifestSha256V1(
            state.subscriptions);
    state.counters.processed_records = 1U;
    state.next_ingress_sequence = 2U;
    state.processed_ingress_sequence = 1U;
    state.processed_record_start_wal_pos = 4096U;
    state.processed_record_end_wal_pos = 4224U;
    Rehash(&checkpoint);
    return checkpoint;
}

control::ControlDecoderCheckpointV1 FirstLogonCheckpoint() {
    control::ControlDecoderCheckpointV1 checkpoint =
        InitialCheckpoint();
    control::ControlDecoderSnapshotV1& state = checkpoint.state;
    Fill(&state.response_manifest_sha256, 0x50U);
    state.latest_response_ingress_sequence = 1U;
    state.latest_response_record_end_wal_pos = 4224U;
    state.counters.emitted_control_records = 1U;
    state.counters.logon_success = 1U;
    state.connection_epoch = 1U;
    state.subscription_epoch = 1U;
    state.session_phase = control::ControlSessionPhaseV1::kLoggedIn;
    state.logged_in = true;
    state.subscriptions[0].status_known = true;
    state.effective_success_mask = 1U;
    state.control_ready = true;
    Rehash(&checkpoint);
    return checkpoint;
}

void AdvanceOrdinaryRecord(
    control::ControlDecoderCheckpointV1* checkpoint) {
    control::ControlDecoderSnapshotV1& state = checkpoint->state;
    state.counters.processed_records = 2U;
    state.next_ingress_sequence = 3U;
    state.processed_ingress_sequence = 2U;
    state.processed_record_start_wal_pos = 4224U;
    state.processed_record_end_wal_pos = 4352U;
    Rehash(checkpoint);
}

control::ControlDecoderCheckpointV1 ConnectingCheckpoint() {
    control::ControlDecoderCheckpointV1 checkpoint =
        InitialCheckpoint();
    control::ControlDecoderSnapshotV1& state = checkpoint.state;
    state.counters.emitted_control_records = 1U;
    state.session_phase =
        control::ControlSessionPhaseV1::kConnecting;
    Fill(&state.current_address_sha256, 0x70U);
    Rehash(&checkpoint);
    return checkpoint;
}

control::ControlDecoderCheckpointV1 PoisonedCheckpoint() {
    control::ControlDecoderCheckpointV1 checkpoint =
        InitialCheckpoint();
    control::ControlDecoderSnapshotV1& state = checkpoint.state;
    state.counters.emitted_control_records = 1U;
    state.counters.control_decode_errors = 1U;
    state.session_phase = control::ControlSessionPhaseV1::kPoisoned;
    state.poisoned = true;
    state.quality_flags = control::QualityBit(
        control::QualityFlagV1::kDecodeTruncated);
    Rehash(&checkpoint);
    return checkpoint;
}

control::ControlDecoderCheckpointV1 ChangedSubscriptionCheckpoint() {
    control::ControlDecoderCheckpointV1 checkpoint =
        FirstLogonCheckpoint();
    control::ControlDecoderSnapshotV1& state = checkpoint.state;
    state.counters.processed_records = 2U;
    state.counters.emitted_control_records = 2U;
    state.counters.subscription_responses = 1U;
    state.next_ingress_sequence = 3U;
    state.processed_ingress_sequence = 2U;
    state.processed_record_start_wal_pos = 4224U;
    state.processed_record_end_wal_pos = 4352U;
    state.latest_response_ingress_sequence = 2U;
    state.latest_response_record_end_wal_pos = 4352U;
    state.subscription_epoch = 2U;
    state.subscriptions[0].status = 7U;
    state.effective_success_mask = 0U;
    state.control_ready = false;
    state.quality_flags = control::QualityBit(
        control::QualityFlagV1::kSubscriptionChanged);
    Rehash(&checkpoint);
    return checkpoint;
}

control::ControlDecoderCheckpointV1 LogonFailedAfterSuccessCheckpoint() {
    control::ControlDecoderCheckpointV1 checkpoint =
        FirstLogonCheckpoint();
    control::ControlDecoderSnapshotV1& state = checkpoint.state;
    state.counters.processed_records = 2U;
    state.counters.emitted_control_records = 2U;
    state.counters.logon_failure = 1U;
    state.next_ingress_sequence = 3U;
    state.processed_ingress_sequence = 2U;
    state.processed_record_start_wal_pos = 4224U;
    state.processed_record_end_wal_pos = 4352U;
    state.latest_response_ingress_sequence = 2U;
    state.latest_response_record_end_wal_pos = 4352U;
    state.session_phase = control::ControlSessionPhaseV1::kLogonFailed;
    state.logged_in = false;
    state.control_ready = false;
    state.quality_flags = control::QualityBit(
        control::QualityFlagV1::kUnauthorized);
    Rehash(&checkpoint);
    return checkpoint;
}

void TestCursorReachability(TestContext* test) {
    const control::ControlDecoderCheckpointV1 initial =
        InitialCheckpoint();
    const control::ControlDecoderCheckpointV1 response_at_tail =
        FirstLogonCheckpoint();
    control::ControlDecoderCheckpointV1 response_before_tail =
        response_at_tail;
    AdvanceOrdinaryRecord(&response_before_tail);
    test->Expect(
        Valid(initial) && Valid(response_at_tail) &&
            Valid(response_before_tail),
        "aligned processed cursors and equal-or-strictly-earlier response pairs are reachable");

    control::ControlDecoderCheckpointV1 bad = initial;
    ++bad.state.processed_record_start_wal_pos;
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "unaligned processed record start is rejected");
    bad = initial;
    ++bad.state.processed_record_end_wal_pos;
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "unaligned processed record end is rejected");
    bad = initial;
    bad.state.processed_record_start_wal_pos = 4088U;
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "processed Raw record cannot begin inside the segment header");
    bad = initial;
    bad.state.processed_record_end_wal_pos =
        bad.state.processed_record_start_wal_pos + 104U;
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "processed Raw record span includes at least its header and trailer");
    bad = response_before_tail;
    bad.state.latest_response_ingress_sequence =
        bad.state.processed_ingress_sequence;
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "response sequence cannot equal the processed sequence while its WAL end is earlier");
    bad = response_before_tail;
    bad.state.latest_response_record_end_wal_pos =
        bad.state.processed_record_end_wal_pos;
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "response WAL end cannot equal the processed end while its sequence is earlier");
    bad = response_before_tail;
    bad.state.latest_response_record_end_wal_pos =
        bad.state.processed_record_start_wal_pos + 8U;
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "an earlier response cannot overlap the processed record");
    bad = response_before_tail;
    ++bad.state.latest_response_record_end_wal_pos;
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "unaligned latest-response WAL end is rejected");
}

void TestQualityReachability(TestContext* test) {
    const control::ControlDecoderCheckpointV1 connecting =
        ConnectingCheckpoint();
    const control::ControlDecoderCheckpointV1 poisoned =
        PoisonedCheckpoint();
    const control::ControlDecoderCheckpointV1 changed =
        ChangedSubscriptionCheckpoint();
    const control::ControlDecoderCheckpointV1 unauthorized =
        LogonFailedAfterSuccessCheckpoint();
    control::ControlDecoderCheckpointV1 noncanonical = connecting;
    noncanonical.state.quality_flags = control::QualityBit(
        control::QualityFlagV1::kNoncanonicalEmptyOffset);
    Rehash(&noncanonical);
    test->Expect(
        Valid(connecting) && Valid(poisoned) && Valid(changed) &&
            Valid(unauthorized) && Valid(noncanonical),
        "decoder-persisted quality bits remain valid in reachable associated states");

    control::ControlDecoderCheckpointV1 bad =
        InitialCheckpoint();
    bad.state.quality_flags = control::QualityBit(
        control::QualityFlagV1::kSessionUnknown);
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "attribution-only quality bits cannot enter checkpoint state");
    bad = InitialCheckpoint();
    bad.state.quality_flags = control::QualityBit(
        control::QualityFlagV1::kNoncanonicalEmptyOffset);
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "persisted quality evidence requires an emitted control record");
    bad = InitialCheckpoint();
    bad.state.quality_flags = control::QualityBit(
        control::QualityFlagV1::kDecodeOffsetInvalid);
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "decode-error quality evidence requires a decode-error counter");
    bad = poisoned;
    bad.state.quality_flags = 0U;
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "a persisted decode error retains at least one decoder error quality bit");
    bad = poisoned;
    bad.state.quality_flags |= control::QualityBit(
        control::QualityFlagV1::kNoncanonicalEmptyOffset);
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "a decode-error-only history cannot forge successful noncanonical decode evidence");
    bad = FirstLogonCheckpoint();
    bad.state.quality_flags = control::QualityBit(
        control::QualityFlagV1::kSubscriptionChanged);
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "the first logon change alone cannot forge SUBSCRIPTION_CHANGED persistence");
    bad = unauthorized;
    bad.state.session_phase =
        control::ControlSessionPhaseV1::kLoggedIn;
    bad.state.logged_in = true;
    bad.state.control_ready = true;
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "UNAUTHORIZED cannot coexist with the logged-in state that clears it");
}

void TestPhaseAndAddressReachability(TestContext* test) {
    const control::ControlDecoderCheckpointV1 connecting =
        ConnectingCheckpoint();
    const control::ControlDecoderCheckpointV1 logon =
        FirstLogonCheckpoint();
    test->Expect(
        Valid(connecting) && Valid(logon),
        "API address state and direct logon without an API address are both reachable");

    control::ControlDecoderCheckpointV1 bad =
        InitialCheckpoint();
    Fill(&bad.state.current_address_sha256, 0x70U);
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "INITIAL cannot contain an API current address");
    bad = connecting;
    bad.state.current_address_sha256 = {};
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "CONNECTING requires the address carried by its API event");
    bad = connecting;
    bad.state.counters.emitted_control_records = 0U;
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "CONNECTING cannot exist before any control record was emitted");
    bad = logon;
    Fill(&bad.state.current_address_sha256, 0x70U);
    bad.state.last_success_address_sha256 =
        bad.state.current_address_sha256;
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "an address hash requires evidence from an address-bearing API event");

    bad = connecting;
    bad.state.counters.disconnect = 1U;
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "CONNECTING requires an untyped API event rather than a relabeled disconnect");

    control::ControlDecoderCheckpointV1 post_logon_connecting = logon;
    AdvanceOrdinaryRecord(&post_logon_connecting);
    post_logon_connecting.state.counters.emitted_control_records = 2U;
    post_logon_connecting.state.session_phase =
        control::ControlSessionPhaseV1::kConnecting;
    post_logon_connecting.state.logged_in = false;
    post_logon_connecting.state.control_ready = false;
    Fill(
        &post_logon_connecting.state.current_address_sha256,
        0x70U);
    Rehash(&post_logon_connecting);
    test->Expect(
        Valid(post_logon_connecting) &&
            post_logon_connecting.state.connection_epoch == 1U &&
            post_logon_connecting.state.last_success_address_sha256 ==
                l2flow::common::Sha256Digest{},
        "a direct logon followed by Connecting may have current address present and last-success absent");

    control::ControlDecoderCheckpointV1 disconnected =
        InitialCheckpoint();
    disconnected.state.counters.emitted_control_records = 1U;
    disconnected.state.counters.disconnect = 1U;
    disconnected.state.session_phase =
        control::ControlSessionPhaseV1::kDisconnected;
    disconnected.state.disconnected_window = true;
    Fill(&disconnected.state.current_address_sha256, 0x70U);
    Rehash(&disconnected);
    test->Expect(
        Valid(disconnected),
        "DISCONNECTED with its API address and disconnect counter is reachable");
    disconnected.state.counters.disconnect = 0U;
    Rehash(&disconnected);
    test->Expect(
        !Valid(disconnected),
        "DISCONNECTED requires a committed disconnect event");

    bad = PoisonedCheckpoint();
    bad.state.disconnected_window = true;
    Rehash(&bad);
    test->Expect(
        !Valid(bad),
        "a retained disconnect window requires its address-bearing API evidence");

    control::ControlDecoderCheckpointV1 forged_switch =
        FirstLogonCheckpoint();
    AdvanceOrdinaryRecord(&forged_switch);
    forged_switch.state.counters.emitted_control_records = 2U;
    forged_switch.state.counters.logon_success = 2U;
    forged_switch.state.connection_epoch = 2U;
    forged_switch.state.latest_response_ingress_sequence =
        forged_switch.state.processed_ingress_sequence;
    forged_switch.state.latest_response_record_end_wal_pos =
        forged_switch.state.processed_record_end_wal_pos;
    forged_switch.state.connection_switched = true;
    Fill(&forged_switch.state.current_address_sha256, 0x70U);
    forged_switch.state.last_success_address_sha256 =
        forged_switch.state.current_address_sha256;
    Rehash(&forged_switch);
    test->Expect(
        !Valid(forged_switch),
        "CONNECTION_SWITCHED requires at least two address-bearing API events");

    control::ControlDecoderCheckpointV1 logon_failed =
        LogonFailedAfterSuccessCheckpoint();
    test->Expect(
        Valid(logon_failed),
        "LOGON_FAILED with a failure counter is reachable");
    logon_failed.state.counters.logon_failure = 0U;
    logon_failed.state.quality_flags = 0U;
    Rehash(&logon_failed);
    test->Expect(
        !Valid(logon_failed),
        "LOGON_FAILED requires a committed logon failure even when another response exists");
}

}  // namespace

int main() {
    TestContext test;
    try {
        TestCursorReachability(&test);
        TestQualityReachability(&test);
        TestPhaseAndAddressReachability(&test);
    } catch (const std::exception& exception) {
        std::cerr << "FAIL: unexpected exception: "
                  << exception.what() << '\n';
        ++test.failures;
    }
    if (test.failures == 0) {
        std::cout
            << "Phase3 checkpoint reachability tests passed\n";
    }
    return test.failures == 0 ? 0 : 1;
}
