#include "l2flow/ingress/raw_reader.h"
#include "l2flow/ingress/raw_replay.h"
#include "l2flow/ingress/raw_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace ingress = l2flow::ingress;

namespace {

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
    std::array<std::byte, Size>* bytes,
    std::uint8_t seed) {
    for (std::size_t index = 0U; index < Size; ++index) {
        (*bytes)[index] = static_cast<std::byte>(
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

struct RecordSpec final {
    std::uint64_t ingress_sequence = 0U;
    std::uint64_t realtime_ns = 0U;
    std::uint64_t monotonic_ns = 0U;
    std::uint8_t service_id = 0U;
    std::uint16_t service_version = 0U;
    std::uint16_t message_id = 0U;
    std::byte body_byte{};
};

ingress::RawSegmentScanResult BuildScan(
    std::uint32_t source_stream_id,
    std::uint32_t capture_date,
    std::uint32_t segment_sequence,
    std::uint64_t base_wal_pos,
    std::uint32_t clock_algorithm,
    std::uint8_t clock_digest_seed,
    const std::vector<RecordSpec>& records) {
    ingress::SegmentHeaderV1 segment;
    segment.source_stream_id = source_stream_id;
    segment.capture_date = capture_date;
    Fill(&segment.stream_day_id, 1U);
    segment.segment_sequence = segment_sequence;
    segment.segment_base_wal_pos = base_wal_pos;
    segment.first_ingress_sequence =
        records.empty() ? 1U : records.front().ingress_sequence;
    segment.created_realtime_ns = 1U;
    segment.created_monotonic_ns = 1U;
    Fill(&segment.host_uuid, 21U);
    Fill(&segment.linux_boot_id, 41U);
    segment.clock_epoch_algorithm = clock_algorithm;
    Fill(
        &segment.clock_epoch_digest,
        clock_digest_seed);
    segment.clock_epoch_label = clock_digest_seed;
    Fill(&segment.sdk_archive_sha256, 61U);
    Fill(&segment.libmdl_api_sha256, 81U);
    Fill(&segment.endpoint_contract_sha256, 101U);
    Fill(&segment.config_sha256, 121U);
    Fill(&segment.raw_schema_sha256, 141U);
    Fill(&segment.build_manifest_sha256, 161U);

    ingress::RawV1SegmentHeaderWire segment_wire{};
    static_cast<void>(ingress::EncodeSegmentHeaderV1(
        segment, &segment_wire));
    auto bytes =
        std::make_shared<std::vector<std::byte>>(
            segment_wire.begin(), segment_wire.end());

    for (const RecordSpec& spec : records) {
        const std::array<std::byte, 1U> body{
            spec.body_byte};
        ingress::RawRecordInputV1 input;
        input.meta.source_stream_id = source_stream_id;
        input.meta.connection_epoch_hint = 1U;
        input.meta.ingress_sequence =
            spec.ingress_sequence;
        input.meta.recv_realtime_ns = spec.realtime_ns;
        input.meta.recv_monotonic_ns = spec.monotonic_ns;
        input.meta.capture_date = capture_date;
        input.vendor_head = VendorHead(
            1U,
            spec.service_id,
            spec.service_version,
            spec.message_id,
            spec.ingress_sequence);
        input.vendor_body = body;

        std::vector<std::byte> record_wire;
        static_cast<void>(ingress::EncodeRawRecordV1(
            input, &record_wire));
        bytes->insert(
            bytes->end(),
            record_wire.begin(),
            record_wire.end());
    }

    return ingress::ScanRawSegmentV1(
        bytes, static_cast<std::uint64_t>(bytes->size()));
}

class FakeClock final : public ingress::RawReplayClock {
public:
    explicit FakeClock(std::uint64_t now) : now_(now) {}

    std::uint64_t NowMonotonicNs() noexcept override {
        return now_;
    }

    void Advance(std::uint64_t duration) {
        now_ += duration;
    }

private:
    std::uint64_t now_ = 0U;
};

class FakeSleeper final : public ingress::RawReplaySleeper {
public:
    explicit FakeSleeper(FakeClock* clock) : clock_(clock) {}

    bool SleepForNs(
        std::uint64_t duration_ns) noexcept override {
        sleeps.push_back(duration_ns);
        clock_->Advance(duration_ns);
        return succeed;
    }

    FakeClock* clock_;
    bool succeed = true;
    std::vector<std::uint64_t> sleeps;
};

std::unique_ptr<ingress::RawReplayEngine> MakeEngine(
    std::span<const ingress::RawReplaySegmentInput> inputs,
    const ingress::RawReplayFilter& filter,
    const ingress::RawReplayRunSettings& settings,
    ingress::RawReplayClock* clock,
    ingress::RawReplaySleeper* sleeper,
    TestContext* test) {
    std::unique_ptr<ingress::RawReplayEngine> engine;
    const ingress::RawReplayError error =
        ingress::RawReplayEngine::Create(
            inputs,
            filter,
            settings,
            clock,
            sleeper,
            &engine);
    test->Expect(
        error == ingress::RawReplayError::kNone &&
            engine != nullptr,
        std::string("engine creation: ") +
            std::string(ingress::RawReplayErrorName(error)));
    return engine;
}

std::vector<std::uint64_t> DrainSequences(
    ingress::RawReplayEngine* engine,
    TestContext* test) {
    std::vector<std::uint64_t> result;
    for (;;) {
        ingress::RawReplayStep step = engine->Next();
        if (step.kind == ingress::RawReplayStepKind::kEnd) {
            return result;
        }
        if (step.kind ==
                ingress::RawReplayStepKind::
                    kClockEpochBoundary ||
            step.kind ==
                ingress::RawReplayStepKind::
                    kMonotonicRegression) {
            continue;
        }
        test->Expect(
            step.kind ==
                    ingress::RawReplayStepKind::kRecord &&
                step.record.has_value(),
            "drain receives record or structural boundary");
        if (step.record.has_value()) {
            result.push_back(
                step.record->view.header().
                    ingress_sequence);
        }
    }
}

void TestFiltersAndProvenance(TestContext* test) {
    ingress::RawSegmentScanResult scan = BuildScan(
        1001U,
        20260718U,
        1U,
        0U,
        1U,
        20U,
        {
            {10U, 100U, 1000U, 2U, 7U, 41U, std::byte{1U}},
            {11U, 200U, 1100U, 3U, 8U, 42U, std::byte{2U}},
            {12U, 300U, 1200U, 2U, 7U, 41U, std::byte{3U}},
        });
    test->Expect(scan.ok(), "filter fixture scan is valid");
    if (!scan.ok() || scan.records.size() != 3U) {
        return;
    }
    const std::uint64_t durable_frontier =
        scan.records[1].record_end_wal_pos();
    const ingress::RawReplaySegmentInput input{
        &scan,
        ingress::RawReplayScanExtent::
            kIncludesRecoveredAppendOnly,
        durable_frontier};

    ingress::RawReplayFilter filter;
    filter.source_stream_ids = {1001U};
    filter.capture_dates = {20260718U};
    filter.vendor_service_ids = {2U};
    filter.vendor_service_versions = {7U};
    filter.vendor_message_ids = {41U};
    filter.wal.begin =
        scan.records[0].record_start_wal_pos();
    filter.wal.end =
        scan.records[2].record_start_wal_pos() + 1U;
    filter.ingress_sequence = {10U, 13U};
    filter.recv_realtime_ns = {100U, 301U};

    ingress::RawReplayRunSettings durable_settings;
    durable_settings.determinism_seed = 0xabcdefU;
    std::unique_ptr<ingress::RawReplayEngine> durable =
        MakeEngine(
            std::span<const ingress::RawReplaySegmentInput>(
                &input, 1U),
            filter,
            durable_settings,
            nullptr,
            nullptr,
            test);
    test->Expect(
        durable->settings().determinism_seed == 0xabcdefU &&
            durable->selected_record_count() == 1U,
        "seed is retained while append-only input stays excluded");
    ingress::RawReplayStep first = durable->Next();
    test->Expect(
        first.record.has_value() &&
            first.record->view.header().ingress_sequence == 10U &&
            first.record->provenance ==
                ingress::RawReplayProvenance::kDurable,
        "all Phase 2 selectors match the durable record");

    ingress::RawReplayRunSettings recovered_settings =
        durable_settings;
    recovered_settings.include_recovered_append_only = true;
    std::unique_ptr<ingress::RawReplayEngine> recovered =
        MakeEngine(
            std::span<const ingress::RawReplaySegmentInput>(
                &input, 1U),
            filter,
            recovered_settings,
            nullptr,
            nullptr,
            test);
    test->Expect(
        recovered->selected_record_count() == 2U,
        "append-only suffix requires explicit inclusion");
    static_cast<void>(recovered->Next());
    ingress::RawReplayStep second = recovered->Next();
    test->Expect(
        second.record.has_value() &&
            second.record->view.header().ingress_sequence == 12U &&
            second.record->provenance ==
                ingress::RawReplayProvenance::
                    kRecoveredAppendOnly,
        "included suffix retains recovered-append-only provenance");
}

void TestPacing(TestContext* test) {
    ingress::RawSegmentScanResult scan = BuildScan(
        1U,
        20260718U,
        1U,
        0U,
        1U,
        10U,
        {
            {1U, 1U, 100U, 1U, 1U, 1U, std::byte{1U}},
            {2U, 2U, 200U, 1U, 1U, 1U, std::byte{2U}},
            {3U, 3U, 350U, 1U, 1U, 1U, std::byte{3U}},
        });
    const ingress::RawReplaySegmentInput input{&scan};
    const std::span<const ingress::RawReplaySegmentInput>
        inputs(&input, 1U);
    const ingress::RawReplayFilter filter;

    ingress::RawReplayRunSettings fast_settings;
    std::unique_ptr<ingress::RawReplayEngine> fast =
        MakeEngine(
            inputs,
            filter,
            fast_settings,
            nullptr,
            nullptr,
            test);
    test->Expect(
        DrainSequences(fast.get(), test) ==
            std::vector<std::uint64_t>({1U, 2U, 3U}),
        "as-fast-as-possible preserves order without a clock");

    FakeClock original_clock(1000U);
    FakeSleeper original_sleeper(&original_clock);
    ingress::RawReplayRunSettings original_settings;
    original_settings.pace =
        ingress::RawReplayPace::kOriginalMonotonic;
    std::unique_ptr<ingress::RawReplayEngine> original =
        MakeEngine(
            inputs,
            filter,
            original_settings,
            &original_clock,
            &original_sleeper,
            test);
    static_cast<void>(DrainSequences(original.get(), test));
    test->Expect(
        original_sleeper.sleeps ==
            std::vector<std::uint64_t>({100U, 150U}),
        "original-monotonic pacing reproduces source intervals");

    FakeClock fixed_clock(500U);
    FakeSleeper fixed_sleeper(&fixed_clock);
    ingress::RawReplayRunSettings fixed_settings;
    fixed_settings.pace =
        ingress::RawReplayPace::kFixedMultiplier;
    fixed_settings.speed_numerator = 2U;
    fixed_settings.speed_denominator = 1U;
    std::unique_ptr<ingress::RawReplayEngine> fixed =
        MakeEngine(
            inputs,
            filter,
            fixed_settings,
            &fixed_clock,
            &fixed_sleeper,
            test);
    static_cast<void>(DrainSequences(fixed.get(), test));
    test->Expect(
        fixed_sleeper.sleeps ==
            std::vector<std::uint64_t>({50U, 75U}),
        "2x fixed multiplier halves source intervals");
}

void TestEpochAndRegression(TestContext* test) {
    ingress::RawSegmentScanResult first_scan = BuildScan(
        1U,
        20260718U,
        1U,
        0U,
        1U,
        10U,
        {
            {1U, 1U, 100U, 1U, 1U, 1U, std::byte{1U}},
            {2U, 2U, 200U, 1U, 1U, 1U, std::byte{2U}},
        });
    ingress::RawSegmentScanResult second_scan = BuildScan(
        1U,
        20260718U,
        2U,
        first_scan.validated_end_wal_pos,
        1U,
        90U,
        {
            {3U, 3U, 10U, 1U, 1U, 1U, std::byte{3U}},
            {4U, 4U, 5U, 1U, 1U, 1U, std::byte{4U}},
            {5U, 5U, 25U, 1U, 1U, 1U, std::byte{5U}},
        });
    const std::array<ingress::RawReplaySegmentInput, 2U>
        inputs{{{&first_scan}, {&second_scan}}};

    FakeClock clock(1000U);
    FakeSleeper sleeper(&clock);
    ingress::RawReplayRunSettings settings;
    settings.pace =
        ingress::RawReplayPace::kOriginalMonotonic;
    std::unique_ptr<ingress::RawReplayEngine> engine =
        MakeEngine(
            inputs,
            ingress::RawReplayFilter{},
            settings,
            &clock,
            &sleeper,
            test);

    test->Expect(
        engine->Next().kind ==
            ingress::RawReplayStepKind::kRecord,
        "first epoch starts immediately");
    test->Expect(
        engine->Next().sleep_ns == 100U,
        "same-epoch interval is paced");
    ingress::RawReplayStep boundary = engine->Next();
    test->Expect(
        boundary.kind ==
                ingress::RawReplayStepKind::
                    kClockEpochBoundary &&
            boundary.boundary.has_value() &&
            boundary.boundary->previous_clock_epoch.digest !=
                boundary.boundary->current_clock_epoch.digest,
        "full clock identity change emits an explicit boundary");
    test->Expect(
        engine->Next().sleep_ns == 0U,
        "first record after an epoch boundary never subtracts epochs");

    ingress::RawReplayStep regression = engine->Next();
    test->Expect(
        regression.kind ==
                ingress::RawReplayStepKind::
                    kMonotonicRegression &&
            regression.boundary.has_value() &&
            regression.boundary->
                    previous_recv_monotonic_ns == 10U &&
            regression.boundary->
                    current_recv_monotonic_ns == 5U,
        "monotonic regression is explicit and cannot underflow");
    test->Expect(
        engine->Next().sleep_ns == 0U,
        "regression resets pacing without a huge wait");
    test->Expect(
        engine->Next().sleep_ns == 20U,
        "pacing resumes from the post-regression origin");
    test->Expect(
        sleeper.sleeps ==
            std::vector<std::uint64_t>({100U, 20U}),
        "no sleep is issued across epoch or regression boundaries");
}

void TestPauseStepAndLifetime(TestContext* test) {
    std::unique_ptr<ingress::RawReplayEngine> engine;
    {
        ingress::RawSegmentScanResult scan = BuildScan(
            1U,
            20260718U,
            1U,
            0U,
            1U,
            10U,
            {
                {1U, 1U, 1U, 1U, 1U, 1U, std::byte{0x51U}},
                {2U, 2U, 2U, 1U, 1U, 1U, std::byte{0x52U}},
            });
        const ingress::RawReplaySegmentInput input{&scan};
        engine = MakeEngine(
            std::span<const ingress::RawReplaySegmentInput>(
                &input, 1U),
            ingress::RawReplayFilter{},
            ingress::RawReplayRunSettings{},
            nullptr,
            nullptr,
            test);
    }

    engine->Pause();
    test->Expect(
        engine->paused() &&
            engine->Next().kind ==
                ingress::RawReplayStepKind::kPaused,
        "pause is a nonblocking state-machine result");
    engine->RequestSingleStep();
    ingress::RawReplayStep first = engine->Next();
    test->Expect(
        first.record.has_value() &&
            first.record->view.vendor_body().front() ==
                std::byte{0x51U} &&
            engine->Next().kind ==
                ingress::RawReplayStepKind::kPaused,
        "single-step releases exactly one record while paused");

    engine->Resume();
    ingress::RawReplayStep second = engine->Next();
    engine.reset();
    test->Expect(
        second.record.has_value() &&
            second.record->view.vendor_body().front() ==
                std::byte{0x52U},
        "returned RawRecordView outlives scan and replay engine");
}

void TestValidation(TestContext* test) {
    ingress::RawSegmentScanResult scan = BuildScan(
        1U,
        20260718U,
        1U,
        0U,
        1U,
        10U,
        {{1U, 1U, 1U, 1U, 1U, 1U, std::byte{1U}}});
    ingress::RawReplaySegmentInput input{
        &scan,
        ingress::RawReplayScanExtent::
            kIncludesRecoveredAppendOnly,
        scan.records[0].record_start_wal_pos() + 1U};
    std::unique_ptr<ingress::RawReplayEngine> engine;
    test->Expect(
        ingress::RawReplayEngine::Create(
            std::span<const ingress::RawReplaySegmentInput>(
                &input, 1U),
            ingress::RawReplayFilter{},
            ingress::RawReplayRunSettings{},
            nullptr,
            nullptr,
            &engine) ==
            ingress::RawReplayError::
                kInvalidDurableFrontier,
        "durable frontier must be a validated record boundary");

    input.extent =
        ingress::RawReplayScanExtent::kDurableOnly;
    ingress::RawReplayFilter bad_filter;
    bad_filter.wal = {10U, 9U};
    test->Expect(
        ingress::RawReplayEngine::Create(
            std::span<const ingress::RawReplaySegmentInput>(
                &input, 1U),
            bad_filter,
            ingress::RawReplayRunSettings{},
            nullptr,
            nullptr,
            &engine) ==
            ingress::RawReplayError::kInvalidRange,
        "reversed half-open ranges fail closed");

    ingress::RawReplayRunSettings bad_multiplier;
    bad_multiplier.pace =
        ingress::RawReplayPace::kFixedMultiplier;
    bad_multiplier.speed_numerator = 0U;
    test->Expect(
        ingress::RawReplayEngine::Create(
            std::span<const ingress::RawReplaySegmentInput>(
                &input, 1U),
            ingress::RawReplayFilter{},
            bad_multiplier,
            nullptr,
            nullptr,
            &engine) ==
            ingress::RawReplayError::kInvalidMultiplier,
        "zero rational multiplier fails before clock validation");

    ingress::RawReplayRunSettings unknown_pace;
    unknown_pace.pace =
        static_cast<ingress::RawReplayPace>(255U);
    test->Expect(
        ingress::RawReplayEngine::Create(
            std::span<const ingress::RawReplaySegmentInput>(
                &input, 1U),
            ingress::RawReplayFilter{},
            unknown_pace,
            nullptr,
            nullptr,
            &engine) ==
            ingress::RawReplayError::kInvalidPace,
        "unknown pace enum fails closed");

    input.extent =
        static_cast<ingress::RawReplayScanExtent>(255U);
    test->Expect(
        ingress::RawReplayEngine::Create(
            std::span<const ingress::RawReplaySegmentInput>(
                &input, 1U),
            ingress::RawReplayFilter{},
            ingress::RawReplayRunSettings{},
            nullptr,
            nullptr,
            &engine) ==
            ingress::RawReplayError::kInvalidExtent,
        "unknown scan provenance extent fails closed");
}

void TestPacingFailuresAndResume(TestContext* test) {
    ingress::RawSegmentScanResult scan = BuildScan(
        1U,
        20260718U,
        1U,
        0U,
        1U,
        10U,
        {
            {1U, 1U, 1U, 1U, 1U, 1U, std::byte{1U}},
            {2U, 2U, 2U, 1U, 1U, 1U, std::byte{2U}},
        });
    const ingress::RawReplaySegmentInput input{&scan};
    const std::span<const ingress::RawReplaySegmentInput>
        inputs(&input, 1U);
    ingress::RawReplayRunSettings settings;
    settings.pace =
        ingress::RawReplayPace::kOriginalMonotonic;

    FakeClock failure_clock(0U);
    FakeSleeper failure_sleeper(&failure_clock);
    failure_sleeper.succeed = false;
    std::unique_ptr<ingress::RawReplayEngine> failure =
        MakeEngine(
            inputs,
            ingress::RawReplayFilter{},
            settings,
            &failure_clock,
            &failure_sleeper,
            test);
    static_cast<void>(failure->Next());
    ingress::RawReplayStep failed = failure->Next();
    test->Expect(
        failed.kind == ingress::RawReplayStepKind::kError &&
            failed.error ==
                ingress::RawReplayError::kSleepFailure,
        "injected sleeper failure is terminal and explicit");

    FakeClock resume_clock(100U);
    FakeSleeper resume_sleeper(&resume_clock);
    std::unique_ptr<ingress::RawReplayEngine> resumed =
        MakeEngine(
            inputs,
            ingress::RawReplayFilter{},
            settings,
            &resume_clock,
            &resume_sleeper,
            test);
    static_cast<void>(resumed->Next());
    resumed->Pause();
    resume_clock.Advance(10'000U);
    resumed->Resume();
    test->Expect(
        resumed->Next().sleep_ns == 0U &&
            resume_sleeper.sleeps.empty(),
        "resume resets the pacing origin instead of catching up");

    ingress::RawSegmentScanResult overflow_scan = BuildScan(
        1U,
        20260718U,
        1U,
        0U,
        1U,
        10U,
        {
            {1U, 1U, 1U, 1U, 1U, 1U, std::byte{1U}},
            {2U, 2U, 2U, 1U, 1U, 1U, std::byte{2U}},
        });
    const ingress::RawReplaySegmentInput overflow_input{
        &overflow_scan};
    FakeClock overflow_clock(
        std::numeric_limits<std::uint64_t>::max());
    FakeSleeper overflow_sleeper(&overflow_clock);
    std::unique_ptr<ingress::RawReplayEngine> overflow =
        MakeEngine(
            std::span<const ingress::RawReplaySegmentInput>(
                &overflow_input, 1U),
            ingress::RawReplayFilter{},
            settings,
            &overflow_clock,
            &overflow_sleeper,
            test);
    static_cast<void>(overflow->Next());
    const ingress::RawReplayStep overflowed =
        overflow->Next();
    test->Expect(
        overflowed.kind ==
                ingress::RawReplayStepKind::kError &&
            overflowed.error ==
                ingress::RawReplayError::
                    kDurationOverflow,
        "replay-clock target addition is overflow checked");
}

}  // namespace

int main() {
    TestContext test;
    TestFiltersAndProvenance(&test);
    TestPacing(&test);
    TestEpochAndRegression(&test);
    TestPauseStepAndLifetime(&test);
    TestValidation(&test);
    TestPacingFailuresAndResume(&test);

    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase 2 raw-replay tests failed\n";
        return 1;
    }
    std::cout << "Phase 2 raw-replay tests passed\n";
    return 0;
}
