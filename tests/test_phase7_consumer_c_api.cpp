#include "l2flow/consumer/consumer_c_api_v1.h"

#include "l2flow/canonical/canonical_schema_v1.h"
#include "l2flow/canonical/safe_mux_v1.h"
#include "l2flow/canonical/source_frontier_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string_view>

namespace canonical = l2flow::canonical;

namespace {

struct TestContext final {
    int failures = 0;

    void Expect(bool condition, std::string_view message) {
        if (!condition) {
            ++failures;
            std::cerr << "FAIL: " << message << '\n';
        }
    }
};

template <std::size_t Size>
std::array<std::byte, Size> Pattern(std::uint8_t seed) {
    std::array<std::byte, Size> value{};
    for (std::size_t index = 0U; index < Size; ++index) {
        value[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(seed + index));
    }
    return value;
}

void FillClock(
    const canonical::ClockEpochIdentityV1& input,
    l2flow_consumer_clock_epoch_v1* output) {
    output->algorithm = input.algorithm;
    std::memcpy(
        output->digest, input.digest.data(), input.digest.size());
    output->label = input.label;
}

void CheckAttach(TestContext* test) {
    l2flow_consumer_attach_identity_v1 expected{};
    expected.event_type = static_cast<std::uint16_t>(
        canonical::CanonicalEventTypeV1::kSnapshot);
    expected.record_size = static_cast<std::uint32_t>(
        canonical::kCanonicalSnapshotRecordBytesV1);
    const auto schema = canonical::CanonicalSchemaDescriptorSha256V1();
    const auto dtype = canonical::CanonicalDtypeDescriptorSha256V1();
    const auto registry = Pattern<32U>(0x60U);
    std::memcpy(expected.schema_sha256, schema.data(), schema.size());
    std::memcpy(expected.dtype_sha256, dtype.data(), dtype.size());
    expected.registry_version = 4U;
    std::memcpy(
        expected.registry_sha256, registry.data(), registry.size());
    l2flow_consumer_attach_identity_v1 actual = expected;
    test->Expect(
        l2flow_consumer_validate_attach_v1(&expected, &actual) ==
            L2FLOW_CONSUMER_C_OK_V1,
        "C attach helper accepts exact schema/dtype/size/registry identity");
    actual.dtype_sha256[0] ^= 0xffU;
    test->Expect(
        l2flow_consumer_validate_attach_v1(&expected, &actual) ==
            L2FLOW_CONSUMER_C_DTYPE_MISMATCH_V1,
        "C attach helper rejects dtype mismatch");
    actual = expected;
    actual.record_size -= 1U;
    test->Expect(
        l2flow_consumer_validate_attach_v1(&expected, &actual) ==
            L2FLOW_CONSUMER_C_RECORD_SIZE_MISMATCH_V1,
        "C attach helper rejects fixed record size mismatch");
}

void CheckMuxAndAsof(TestContext* test) {
    canonical::SourceFrontierConfigV1 config{};
    config.source_stream_id = 1001U;
    config.capture_date = 20260722U;
    config.stream_day_id = Pattern<16U>(0x10U);
    config.writer_instance = Pattern<16U>(0x20U);
    config.generation = 3U;
    config.clock_epoch.algorithm = 1U;
    config.clock_epoch.digest = Pattern<32U>(0x30U);
    config.clock_epoch.label = 9U;
    config.initial_state = canonical::SourceStateV1::kHealthy;
    canonical::SourceFrontierPageV1 page{};
    test->Expect(
        canonical::InitializeSourceFrontierPageV1(config, &page) ==
            canonical::SourceFrontierErrorV1::kNone,
        "C wrapper live frontier initializes");
    canonical::SourceFrontierCallbackGuardV1 guard(
        &page, config.writer_instance, config.generation);
    test->Expect(
        guard.entered() &&
            guard.CompleteCaptured(1U) ==
                canonical::SourceFrontierErrorV1::kNone &&
            canonical::PublishAppendProgressV1(
                &page, config.writer_instance, config.generation,
                1U, 8192U, 100) ==
                canonical::SourceFrontierErrorV1::kNone &&
            canonical::PublishProcessedProgressV1(
                &page, config.writer_instance, config.generation,
                1U, 8192U, 100) ==
                canonical::SourceFrontierErrorV1::kNone,
        "C wrapper candidate becomes committed");

    l2flow_consumer_mux_input_v1 input{};
    input.required = 1U;
    input.has_next = 1U;
    input.next_key.recv_monotonic_ns = 100;
    input.next_key.source_stream_id = config.source_stream_id;
    input.next_key.origin_ingress_sequence = 1U;
    input.next_origin_wal_end_pos = 8192U;
    FillClock(config.clock_epoch, &input.next_clock_epoch);
    input.next_capture_date = config.capture_date;
    std::memcpy(
        input.next_stream_day_id,
        config.stream_day_id.data(), config.stream_day_id.size());
    std::memcpy(
        input.next_writer_instance,
        config.writer_instance.data(), config.writer_instance.size());
    input.next_generation = config.generation;
    input.frontier_page = &page;
    l2flow_consumer_mux_selection_v1 selected{};
    test->Expect(
        l2flow_consumer_safe_mux_select_v1(
            &input, 1U, &selected) == L2FLOW_CONSUMER_C_OK_V1 &&
            selected.decision == static_cast<std::uint8_t>(
                canonical::SafeMuxDecisionV1::kReady) &&
            selected.input_index == 0U,
        "C safe-mux wrapper delegates a READY proof to Phase 5");

    l2flow_consumer_event_key_v1 tick{};
    tick.recv_monotonic_ns = 90;
    tick.source_stream_id = 1002U;
    tick.origin_ingress_sequence = 5U;
    l2flow_consumer_snapshot_asof_proof_v1 proof{};
    test->Expect(
        l2flow_consumer_snapshot_asof_prove_v1(
            &tick, &input.next_clock_epoch, &input, &proof) ==
                L2FLOW_CONSUMER_C_OK_V1 &&
            proof.proof == static_cast<std::uint8_t>(
                canonical::SnapshotAsofProofV1::kReady),
        "C snapshot-asof proof delegates strict later-next logic to Phase 5");

    std::array<l2flow_consumer_event_key_v1, 3U> consumed{};
    for (std::size_t index = 0U; index < consumed.size(); ++index) {
        consumed[index].recv_monotonic_ns =
            static_cast<std::int64_t>(70U + index * 10U);
        consumed[index].source_stream_id = config.source_stream_id;
        consumed[index].origin_ingress_sequence = index + 1U;
    }
    l2flow_consumer_snapshot_asof_selection_v1 asof{};
    test->Expect(
        l2flow_consumer_snapshot_asof_select_v1(
            consumed.data(), consumed.size(), &tick, &asof) ==
                L2FLOW_CONSUMER_C_OK_V1 &&
            asof.found == 1U && asof.index == 2U,
        "C snapshot-asof selection delegates latest <= tick to Phase 5");

    alignas(64) std::array<std::byte, 4097U> misaligned{};
    input.frontier_page = misaligned.data() + 1U;
    selected.decision = static_cast<std::uint8_t>(
        canonical::SafeMuxDecisionV1::kReady);
    selected.input_index = 42U;
    test->Expect(
        l2flow_consumer_safe_mux_select_v1(
            &input, 1U, &selected) ==
                L2FLOW_CONSUMER_C_INVALID_ARGUMENT_V1 &&
            selected.decision == static_cast<std::uint8_t>(
                canonical::SafeMuxDecisionV1::kInvalidInput) &&
            selected.input_index == 0U,
        "C mux wrapper fails closed for a misaligned opaque frontier");

    proof.proof = static_cast<std::uint8_t>(
        canonical::SnapshotAsofProofV1::kReady);
    test->Expect(
        l2flow_consumer_snapshot_asof_prove_v1(
            &tick, &input.next_clock_epoch, &input, &proof) ==
                L2FLOW_CONSUMER_C_INVALID_ARGUMENT_V1 &&
            proof.proof == static_cast<std::uint8_t>(
                canonical::SnapshotAsofProofV1::kInvalidInput),
        "C snapshot-asof proof fails closed for an invalid source proof");
}

}  // namespace

int main() {
    TestContext test;
    CheckAttach(&test);
    CheckMuxAndAsof(&test);
    if (test.failures != 0) {
        std::cerr << test.failures << " Phase 7 consumer C ABI checks failed\n";
        return 1;
    }
    std::cout << "Phase 7 consumer C ABI checks passed\n";
    return 0;
}
