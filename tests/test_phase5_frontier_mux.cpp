#include "l2flow/canonical/safe_mux_v1.h"
#include "l2flow/canonical/source_frontier_v1.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string_view>

namespace canonical = l2flow::canonical;
namespace common = l2flow::common;

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

canonical::ClockEpochIdentityV1 Clock(std::uint8_t seed) {
    canonical::ClockEpochIdentityV1 value;
    value.algorithm = 1U;
    value.digest = Pattern<32U>(seed);
    value.label = static_cast<std::uint64_t>(seed) + 100U;
    return value;
}

canonical::SourceFrontierConfigV1 Config() {
    canonical::SourceFrontierConfigV1 value;
    value.source_stream_id = 1001U;
    value.capture_date = 20260722U;
    value.stream_day_id = Pattern<16U>(1U);
    value.clock_epoch = Clock(2U);
    value.writer_instance = Pattern<16U>(3U);
    value.generation = 7U;
    value.initial_state = canonical::SourceStateV1::kHealthy;
    return value;
}

std::uint64_t LoadPageU64(
    const canonical::SourceFrontierPageV1& page,
    std::size_t offset) {
    const auto* pointer = reinterpret_cast<const std::uint64_t*>(
        page.bytes.data() + static_cast<std::ptrdiff_t>(offset));
    return __atomic_load_n(pointer, __ATOMIC_ACQUIRE);
}

void StorePageU64(
    canonical::SourceFrontierPageV1* page,
    std::size_t offset,
    std::uint64_t value) {
    auto* pointer = reinterpret_cast<std::uint64_t*>(
        page->bytes.data() + static_cast<std::ptrdiff_t>(offset));
    __atomic_store_n(pointer, value, __ATOMIC_RELEASE);
}

canonical::SourceFrontierV1 Read(
    TestContext* context,
    const canonical::SourceFrontierPageV1& page) {
    canonical::SourceFrontierV1 value;
    context->Expect(
        canonical::ReadSourceFrontierV1(page, &value) ==
            canonical::SourceFrontierErrorV1::kNone,
        "source frontier page reads coherently");
    return value;
}

void Capture(
    TestContext* context,
    canonical::SourceFrontierPageV1* page,
    std::uint64_t sequence) {
    const canonical::SourceFrontierConfigV1 config = Config();
    canonical::SourceFrontierCallbackGuardV1 guard(
        page, config.writer_instance, config.generation);
    context->Expect(guard.entered(), "callback enters the inflight gate");
    context->Expect(
        guard.CompleteCaptured(sequence) ==
            canonical::SourceFrontierErrorV1::kNone,
        "callback publishes the next exact capture sequence");
}

void CheckPageAndProgress(TestContext* context) {
    context->Expect(
        canonical::SourceFrontierAtomicsLockFreeV1(),
        "required 16/32/64-bit shared atomics are lock-free");
    canonical::SourceFrontierPageV1 page;
    const canonical::SourceFrontierConfigV1 config = Config();
    canonical::SourceFrontierConfigV1 invalid_initial = config;
    invalid_initial.initial_ingress_sequence = 7U;
    invalid_initial.initial_global_wal_pos = 0U;
    canonical::SourceFrontierPageV1 invalid_page;
    context->Expect(
        canonical::InitializeSourceFrontierPageV1(
            invalid_initial, &invalid_page) ==
            canonical::SourceFrontierErrorV1::kInvalidConfiguration,
        "nonzero initial ingress requires a nonzero WAL cursor");
    context->Expect(
        canonical::InitializeSourceFrontierPageV1(config, &page) ==
            canonical::SourceFrontierErrorV1::kNone,
        "valid source frontier initializes");
    canonical::SourceFrontierV1 value = Read(context, page);
    context->Expect(
        value.source_stream_id == config.source_stream_id &&
            value.stream_day_id == config.stream_day_id &&
            value.clock_epoch == config.clock_epoch &&
            value.clock_epoch.label == config.clock_epoch.label &&
            canonical::SourceFrontierCaughtUpV1(value),
        "initial page preserves full namespace and is caught up");
    context->Expect(
        canonical::InitializeSourceFrontierPageV1(config, &page) ==
                canonical::SourceFrontierErrorV1::kAlreadyInitialized &&
            Read(context, page).writer_instance == config.writer_instance,
        "one physical frontier page cannot be rebound to another generation");

    const common::Identity128 wrong_writer = Pattern<16U>(0xf0U);
    {
        canonical::SourceFrontierCallbackGuardV1 stale_guard(
            &page, wrong_writer, config.generation);
        context->Expect(
            !stale_guard.entered() &&
                stale_guard.error() ==
                    canonical::SourceFrontierErrorV1::kIdentityChanged,
            "stale callback owner cannot enter another writer generation");
    }
    context->Expect(
        canonical::PublishAppendProgressV1(
            &page, wrong_writer, config.generation,
            1U, 4097U, 10U) ==
                canonical::SourceFrontierErrorV1::kIdentityChanged &&
            canonical::PublishSourceStateV1(
                &page, wrong_writer, config.generation,
                canonical::SourceStateV1::kFatal, 0U) ==
                canonical::SourceFrontierErrorV1::kIdentityChanged &&
            Read(context, page).source_state ==
                canonical::SourceStateV1::kHealthy,
        "stale append/state publishers cannot advance or poison a foreign generation");

    context->Expect(
        canonical::PublishAppendProgressV1(
            &page, config.writer_instance, config.generation,
            1U, 4097U, 10U) ==
            canonical::SourceFrontierErrorV1::kNotCaptured,
        "Raw append cannot outrun callback capture");
    Capture(context, &page, 1U);
    context->Expect(
        canonical::PublishAppendProgressV1(
            &page, config.writer_instance, config.generation,
            1U, 4097U, 10U) ==
            canonical::SourceFrontierErrorV1::kNone,
        "captured record advances append cursor");
    context->Expect(
        canonical::PublishProcessedProgressV1(
            &page, config.writer_instance, config.generation,
            1U, 4098U, 10U) ==
            canonical::SourceFrontierErrorV1::kNotAppended,
        "normalizer cannot outrun append WAL cursor");
    context->Expect(
        canonical::PublishProcessedProgressV1(
            &page, config.writer_instance, config.generation,
            1U, 4097U, 10U) ==
            canonical::SourceFrontierErrorV1::kNone,
        "processed cursor advances only after publication");
    value = Read(context, page);
    context->Expect(
        canonical::SourceFrontierCaughtUpV1(value) &&
            value.safe_processed_frontier_ns == 10,
        "processed receive time is an exclusive safe boundary");

    const canonical::SourceFrontierV1 first = value;
    canonical::SourceFrontierV1 second = Read(context, page);
    context->Expect(
        canonical::TryPublishIdleFrontierV1(
            &page, first, second, 20) ==
            canonical::IdleFrontierResultV1::kPublished,
        "stable double read advances the idle exclusive frontier");
    value = Read(context, page);
    context->Expect(
        value.safe_processed_frontier_ns == 20,
        "idle frontier stores sampled monotonic time");

    second = value;
    second.clock_epoch.label ^= 0xffffU;
    context->Expect(
        canonical::TryPublishIdleFrontierV1(
            &page, value, second, 21) ==
            canonical::IdleFrontierResultV1::kPublished,
        "clock label is display metadata, not epoch identity");

    // A segment/header boundary may advance WAL without a market ingress
    // sequence or receive timestamp.  It must not regress an idle frontier.
    context->Expect(
        canonical::PublishAppendProgressV1(
            &page, config.writer_instance, config.generation,
            1U, 8191U, 11U) ==
            canonical::SourceFrontierErrorV1::kReceiveTimeRegression,
        "WAL-only progress cannot invent a new receive timestamp");
    context->Expect(
        canonical::PublishAppendProgressV1(
            &page, config.writer_instance, config.generation,
            1U, 8192U, 10U) ==
            canonical::SourceFrontierErrorV1::kNone &&
            canonical::PublishProcessedProgressV1(
                &page, config.writer_instance, config.generation,
                1U, 8192U, 10U) ==
                canonical::SourceFrontierErrorV1::kNone,
        "WAL-only boundary advances after idle proof without time regression");
    context->Expect(
        Read(context, page).safe_processed_frontier_ns == 21,
        "WAL-only progress does not lower safe frontier");

    Capture(context, &page, 2U);
    context->Expect(
        canonical::PublishAppendProgressV1(
            &page, config.writer_instance, config.generation,
            2U, 9000U, 30U) ==
            canonical::SourceFrontierErrorV1::kNone,
        "second record is append-covered before fatal freeze");
    context->Expect(
        canonical::PublishProcessedProgressV1(
            &page, config.writer_instance, config.generation,
            1U, 9000U, 10U) ==
            canonical::SourceFrontierErrorV1::kNotAppended,
        "an older ingress cannot borrow the append head's WAL boundary");
    context->Expect(
        canonical::PublishProcessedProgressV1(
            &page, wrong_writer, config.generation,
            2U, 9000U, 30U) ==
            canonical::SourceFrontierErrorV1::kIdentityChanged,
        "processed publication binds the exact Raw writer generation");
    context->Expect(
        canonical::PublishSourceStateV1(
            &page, config.writer_instance, config.generation,
            canonical::SourceStateV1::kFatal, 0U) ==
            canonical::SourceFrontierErrorV1::kNone &&
        canonical::PublishProcessedProgressV1(
            &page, config.writer_instance, config.generation,
            2U, 9000U, 30U) ==
            canonical::SourceFrontierErrorV1::kInvalidState &&
        Read(context, page).processed_ingress_sequence == 1U,
        "FATAL freezes global processed progress against later misuse");
    canonical::SourceFrontierCallbackGuardV1 after_fatal(
        &page, config.writer_instance, config.generation);
    context->Expect(
        !after_fatal.entered() &&
            after_fatal.error() ==
                canonical::SourceFrontierErrorV1::kInvalidState &&
            canonical::PublishAppendProgressV1(
                &page, config.writer_instance, config.generation,
                2U, 9000U, 30U) ==
                canonical::SourceFrontierErrorV1::kInvalidState,
        "FATAL freezes callback entry as well as append/processed progress");
}

void CheckIdleInterleavings(TestContext* context) {
    canonical::SourceFrontierPageV1 page;
    context->Expect(
        canonical::InitializeSourceFrontierPageV1(Config(), &page) ==
            canonical::SourceFrontierErrorV1::kNone,
        "interleaving page initializes");

    const canonical::SourceFrontierV1 first = Read(context, page);
    Capture(context, &page, 1U);
    const canonical::SourceFrontierV1 second = Read(context, page);
    context->Expect(
        canonical::TryPublishIdleFrontierV1(
            &page, first, second, 100) ==
            canonical::IdleFrontierResultV1::kNotCaughtUp,
        "callback completed between reads prevents idle advance");

    context->Expect(
        canonical::PublishAppendProgressV1(
            &page, Config().writer_instance, Config().generation,
            1U, 5000U, 100U) ==
                canonical::SourceFrontierErrorV1::kNone &&
            canonical::PublishProcessedProgressV1(
                &page, Config().writer_instance, Config().generation,
                1U, 5000U, 100U) ==
                canonical::SourceFrontierErrorV1::kNone,
        "interleaving record catches up");
    const canonical::SourceFrontierV1 stable_first = Read(context, page);
    {
        canonical::SourceFrontierCallbackGuardV1 guard(
            &page, Config().writer_instance, Config().generation);
        context->Expect(guard.entered(), "between-read callback enters");
        const canonical::SourceFrontierV1 inflight = Read(context, page);
        context->Expect(
            inflight.callback_generation ==
                    stable_first.callback_generation + 1U &&
                inflight.callback_inflight == 1U,
            "callback entry changes the ABA-resistant generation before timestamping");
        context->Expect(
            canonical::TryPublishIdleFrontierV1(
                &page, stable_first, inflight, 110) ==
                canonical::IdleFrontierResultV1::kCallbackInflight,
            "inflight callback between reads prevents idle advance");
        context->Expect(
            guard.CompleteCaptured(2U) ==
                canonical::SourceFrontierErrorV1::kNone,
            "between-read callback completes");
    }
    context->Expect(
        Read(context, page).callback_generation ==
            stable_first.callback_generation + 2U,
        "callback completion changes generation after publishing captured progress");

    context->Expect(
        canonical::PublishAppendProgressV1(
            &page, Config().writer_instance, Config().generation,
            2U, 5100U, 111U) ==
                canonical::SourceFrontierErrorV1::kNone &&
            canonical::PublishProcessedProgressV1(
                &page, Config().writer_instance, Config().generation,
                2U, 5100U, 111U) ==
                canonical::SourceFrontierErrorV1::kNone,
        "second callback catches up");
    const canonical::SourceFrontierV1 before = Read(context, page);
    const canonical::SourceFrontierV1 after = Read(context, page);
    {
        // A callback starting after the second observation obtains a receive
        // timestamp no smaller than the already sampled t.  Its inflight bit
        // need not invalidate that earlier proof.
        canonical::SourceFrontierCallbackGuardV1 guard(
            &page, Config().writer_instance, Config().generation);
        context->Expect(
            canonical::TryPublishIdleFrontierV1(
                &page, before, after, 120) ==
                canonical::IdleFrontierResultV1::kPublished,
            "callback after second read does not invalidate exclusive t");
        context->Expect(
            guard.CompleteCaptured(3U) ==
                canonical::SourceFrontierErrorV1::kNone,
            "post-proof callback captures normally");
    }
    context->Expect(
        canonical::PublishAppendProgressV1(
            &page, Config().writer_instance, Config().generation,
            3U, 5200U, 119U) ==
            canonical::SourceFrontierErrorV1::kReceiveTimeRegression,
        "post-proof callback cannot claim a receive time below idle t");
    context->Expect(
        canonical::PublishSourceStateV1(
            &page,
            Config().writer_instance,
            Config().generation,
            canonical::SourceStateV1::kDisconnected,
            1U) == canonical::SourceFrontierErrorV1::kNone,
        "source can publish disconnected state");
    const canonical::SourceFrontierV1 disconnected = Read(context, page);
    context->Expect(
        canonical::TryPublishIdleFrontierV1(
            &page, disconnected, disconnected, 130) ==
            canonical::IdleFrontierResultV1::kSourceUnhealthy,
        "disconnected source never publishes healthy idle frontier");
    context->Expect(
        canonical::PublishSourceStateV1(
            &page, Config().writer_instance, Config().generation,
            canonical::SourceStateV1::kFatal, 1U) ==
                canonical::SourceFrontierErrorV1::kNone &&
            canonical::PublishSourceStateV1(
                &page, Config().writer_instance, Config().generation,
                canonical::SourceStateV1::kDisconnected, 1U) ==
                canonical::SourceFrontierErrorV1::kInvalidState &&
            Read(context, page).source_state ==
                canonical::SourceStateV1::kFatal,
        "FATAL is sticky and cannot be overwritten by a stale transition");

    canonical::SourceFrontierPageV1 invalid_quality_page;
    canonical::SourceFrontierConfigV1 invalid_quality = Config();
    invalid_quality.initial_quality_flags = std::uint64_t{1U} << 63U;
    context->Expect(
        canonical::InitializeSourceFrontierPageV1(
            invalid_quality, &invalid_quality_page) ==
            canonical::SourceFrontierErrorV1::kInvalidConfiguration,
        "frontier rejects quality bits outside the frozen V1 mask");

    canonical::SourceFrontierPageV1 abandoned_page;
    context->Expect(
        canonical::InitializeSourceFrontierPageV1(
            Config(), &abandoned_page) ==
            canonical::SourceFrontierErrorV1::kNone,
        "abandoned callback page initializes");
    {
        canonical::SourceFrontierCallbackGuardV1 abandoned(
            &abandoned_page,
            Config().writer_instance,
            Config().generation);
        context->Expect(abandoned.entered(), "abandoned callback enters");
    }
    context->Expect(
        Read(context, abandoned_page).source_state ==
            canonical::SourceStateV1::kFatal,
        "callback leaving without capture fail-stops source");

    canonical::SourceFrontierPageV1 busy_page;
    context->Expect(
        canonical::InitializeSourceFrontierPageV1(Config(), &busy_page) ==
            canonical::SourceFrontierErrorV1::kNone,
        "busy frontier page initializes");
    constexpr std::size_t kProgressGenerationOffset = 192U;
    const std::uint64_t busy_even =
        LoadPageU64(busy_page, kProgressGenerationOffset);
    StorePageU64(
        &busy_page, kProgressGenerationOffset, busy_even + 1U);
    canonical::SourceFrontierV1 unavailable{};
    context->Expect(
        canonical::ReadSourceFrontierV1(busy_page, &unavailable) ==
                canonical::SourceFrontierErrorV1::kBusy &&
            canonical::PublishAppendProgressV1(
                &busy_page,
                Config().writer_instance,
                Config().generation,
                1U,
                4097U,
                10U) == canonical::SourceFrontierErrorV1::kBusy,
        "bounded progress contention is reported as retryable busy, not identity drift");
    StorePageU64(
        &busy_page, kProgressGenerationOffset, busy_even + 2U);
    context->Expect(
        canonical::ReadSourceFrontierV1(busy_page, &unavailable) ==
            canonical::SourceFrontierErrorV1::kNone,
        "frontier reads recover after progress contention releases");

    canonical::SourceFrontierPageV1 contended_fatal_page;
    context->Expect(
        canonical::InitializeSourceFrontierPageV1(
            Config(), &contended_fatal_page) ==
            canonical::SourceFrontierErrorV1::kNone,
        "fatal-latch contention page initializes");
    {
        canonical::SourceFrontierCallbackGuardV1 abandoned(
            &contended_fatal_page,
            Config().writer_instance,
            Config().generation);
        context->Expect(abandoned.entered(),
                        "contended abandoned callback enters");
        const std::uint64_t even = LoadPageU64(
            contended_fatal_page, kProgressGenerationOffset);
        context->Expect((even & 1U) == 0U,
                        "test observes an unlocked progress generation");
        StorePageU64(
            &contended_fatal_page,
            kProgressGenerationOffset,
            even + 1U);
    }
    const std::uint64_t stuck = LoadPageU64(
        contended_fatal_page, kProgressGenerationOffset);
    StorePageU64(
        &contended_fatal_page,
        kProgressGenerationOffset,
        stuck + 1U);
    const canonical::SourceFrontierV1 contended_fatal =
        Read(context, contended_fatal_page);
    context->Expect(
        contended_fatal.source_state ==
                canonical::SourceStateV1::kFatal &&
            contended_fatal.callback_inflight == 1U,
        "abandoned callback keeps an unmissable FATAL latch and fail-closed inflight count when the progress lock is unavailable");
}

canonical::SourceFrontierPageV1 MuxPage(
    TestContext* context,
    std::uint32_t source,
    std::int64_t safe,
    canonical::ClockEpochIdentityV1 clock,
    canonical::SourceStateV1 state =
        canonical::SourceStateV1::kHealthy,
    std::uint64_t processed_ingress = 100U,
    std::uint64_t processed_wal = 10'000U) {
    canonical::SourceFrontierConfigV1 config;
    config.source_stream_id = source;
    config.capture_date = 20260722U;
    config.stream_day_id =
        Pattern<16U>(static_cast<std::uint8_t>(source));
    config.clock_epoch = clock;
    config.writer_instance = Pattern<16U>(4U);
    config.generation = 1U;
    config.initial_ingress_sequence = processed_ingress;
    config.initial_global_wal_pos = processed_wal;
    config.initial_state = state;
    canonical::SourceFrontierPageV1 page;
    context->Expect(
        canonical::InitializeSourceFrontierPageV1(config, &page) ==
            canonical::SourceFrontierErrorV1::kNone,
        "mux live frontier page initializes");
    if (safe > 0 && state == canonical::SourceStateV1::kHealthy) {
        const canonical::SourceFrontierV1 first = Read(context, page);
        const canonical::SourceFrontierV1 second = Read(context, page);
        context->Expect(
            canonical::TryPublishIdleFrontierV1(
                &page, first, second, safe) ==
                canonical::IdleFrontierResultV1::kPublished,
            "mux live frontier publishes its exclusive idle bound");
    }
    return page;
}

void BindInput(
    TestContext* context,
    canonical::SafeMuxInputV1* input,
    const canonical::SourceFrontierPageV1* page) {
    input->frontier_page = page;
    const canonical::SourceFrontierV1 frontier = Read(context, *page);
    input->next_capture_date = frontier.capture_date;
    input->next_stream_day_id = frontier.stream_day_id;
    input->next_writer_instance = frontier.writer_instance;
    input->next_generation = frontier.generation;
}

canonical::CanonicalEventKeyV1 Key(
    std::int64_t time,
    std::uint32_t source,
    std::uint64_t ingress,
    std::uint8_t sub = 0U) {
    return {time, source, ingress, sub};
}

void CheckSafeMuxAndAsof(TestContext* context) {
    const canonical::ClockEpochIdentityV1 clock = Clock(10U);
    canonical::SourceFrontierPageV1 source0_page =
        MuxPage(context, 2002U, 0, clock);
    canonical::SourceFrontierPageV1 source1_page =
        MuxPage(context, 1002U, 0, clock);
    std::array<canonical::SafeMuxInputV1, 2U> inputs{};
    inputs[0].has_next = true;
    inputs[0].next_key = Key(100, 2002U, 5U);
    inputs[0].next_origin_wal_end_pos = 500U;
    inputs[0].next_clock_epoch = clock;
    BindInput(context, &inputs[0], &source0_page);
    inputs[1].has_next = true;
    inputs[1].next_key = Key(100, 1002U, 9U);
    inputs[1].next_origin_wal_end_pos = 900U;
    inputs[1].next_clock_epoch = clock;
    BindInput(context, &inputs[1], &source1_page);
    canonical::SafeMuxSelectionV1 selection =
        canonical::SelectSafeMuxCandidateV1(inputs);
    context->Expect(
        selection.ready() && selection.input_index == 1U,
        "same-time mux uses source/ingress/sub-index tie break");

    canonical::SafeMuxInputV1 missing_live_page = inputs[0];
    missing_live_page.frontier_page = nullptr;
    context->Expect(
        canonical::SelectSafeMuxCandidateV1(
            std::span(&missing_live_page, 1U)).decision ==
            canonical::SafeMuxDecisionV1::kInvalidInput,
        "every required mux input must provide a live frontier page");

    canonical::SourceFrontierPageV1 revoked_page =
        MuxPage(context, 2002U, 0, clock);
    inputs[0].frontier_page = &revoked_page;
    const canonical::SourceFrontierV1 revoked_identity =
        Read(context, revoked_page);
    context->Expect(
        canonical::SelectSafeMuxCandidateV1(inputs).ready(),
        "queued record is ready while its live generation is healthy");
    context->Expect(
        canonical::PublishSourceStateV1(
            &revoked_page,
            revoked_identity.writer_instance,
            revoked_identity.generation,
            canonical::SourceStateV1::kFatal,
            revoked_identity.quality_flags) ==
            canonical::SourceFrontierErrorV1::kNone,
        "test revokes the exact live mux generation");
    context->Expect(
        canonical::SelectSafeMuxCandidateV1(inputs).decision ==
            canonical::SafeMuxDecisionV1::kBlockedUnhealthy,
        "a saved queued proof cannot bypass a newly latched live FATAL");
    inputs[0].frontier_page = &source0_page;

    canonical::SafeMuxInputV1 impossible_time_frontier = inputs[0];
    canonical::SourceFrontierPageV1 invalid_page;
    impossible_time_frontier.frontier_page = &invalid_page;
    context->Expect(
        canonical::SelectSafeMuxCandidateV1(
            std::span(&impossible_time_frontier, 1U)).decision ==
            canonical::SafeMuxDecisionV1::kInvalidInput,
        "mux rejects an uninitialized live frontier page");

    ++inputs[0].next_generation;
    context->Expect(
        canonical::SelectSafeMuxCandidateV1(inputs).decision ==
            canonical::SafeMuxDecisionV1::kInvalidInput,
        "next record cannot be paired with a different frontier generation");
    --inputs[0].next_generation;

    canonical::SourceFrontierPageV1 uncommitted_page =
        MuxPage(context, 2002U, 0, clock,
                canonical::SourceStateV1::kHealthy, 4U, 400U);
    inputs[0].frontier_page = &uncommitted_page;
    context->Expect(
        canonical::SelectSafeMuxCandidateV1(inputs).decision ==
            canonical::SafeMuxDecisionV1::kInvalidInput,
        "physically published but globally uncommitted next record is rejected");
    inputs[0].frontier_page = &source0_page;

    const std::array<canonical::SafeMuxInputV1, 2U> duplicate_inputs{
        inputs[0], inputs[0]};
    context->Expect(
        canonical::SelectSafeMuxCandidateV1(duplicate_inputs).decision ==
            canonical::SafeMuxDecisionV1::kInvalidInput,
        "duplicate complete event keys are rejected instead of array-order tied");

    inputs[1].has_next = false;
    canonical::SourceFrontierPageV1 equal_page =
        MuxPage(context, 1002U, 100, clock);
    inputs[1].frontier_page = &equal_page;
    selection = canonical::SelectSafeMuxCandidateV1(inputs);
    context->Expect(
        selection.decision ==
            canonical::SafeMuxDecisionV1::kBlockedFrontier,
        "frontier equal to candidate time is not sufficient");
    canonical::SourceFrontierPageV1 beyond_page =
        MuxPage(context, 1002U, 101, clock);
    inputs[1].frontier_page = &beyond_page;
    selection = canonical::SelectSafeMuxCandidateV1(inputs);
    context->Expect(
        selection.ready() && selection.input_index == 0U,
        "exclusive frontier strictly beyond candidate releases it");

    canonical::SourceFrontierPageV1 other_clock_page =
        MuxPage(context, 1002U, 101, Clock(11U));
    inputs[1].frontier_page = &other_clock_page;
    context->Expect(
        canonical::SelectSafeMuxCandidateV1(inputs).decision ==
            canonical::SafeMuxDecisionV1::kClockEpochBarrier,
        "different full clock epochs insert a barrier");
    canonical::SourceFrontierPageV1 source1_fatal_page =
        MuxPage(context, 1002U, 0, clock,
                canonical::SourceStateV1::kFatal);
    inputs[1].frontier_page = &source1_fatal_page;
    context->Expect(
        canonical::SelectSafeMuxCandidateV1(inputs).decision ==
            canonical::SafeMuxDecisionV1::kBlockedUnhealthy,
        "fatal source without next event blocks strict mux");

    canonical::SourceFrontierPageV1 snapshot_page =
        MuxPage(context, 1001U, 0, clock);
    canonical::SafeMuxInputV1 snapshots;
    snapshots.has_next = true;
    snapshots.next_key = Key(100, 1001U, 7U);
    snapshots.next_origin_wal_end_pos = 700U;
    snapshots.next_clock_epoch = clock;
    BindInput(context, &snapshots, &snapshot_page);
    const canonical::CanonicalEventKeyV1 tick = Key(100, 1002U, 8U);
    context->Expect(
        canonical::ProveSnapshotAsofTickV1(
            tick, clock, snapshots) ==
            canonical::SnapshotAsofProofV1::kSnapshotPending,
        "same-time snapshot must be consumed before tick as-of");
    canonical::SourceFrontierPageV1 snapshot_fatal_page =
        MuxPage(context, 1001U, 0, clock,
                canonical::SourceStateV1::kFatal);
    snapshots.frontier_page = &snapshot_fatal_page;
    context->Expect(
        canonical::ProveSnapshotAsofTickV1(
            tick, clock, snapshots) ==
            canonical::SnapshotAsofProofV1::kBlockedUnhealthy,
        "queued snapshot from a FATAL generation cannot prove tick as-of");
    snapshots.frontier_page = &snapshot_page;
    snapshots.next_key.recv_monotonic_ns = 101;
    context->Expect(
        canonical::ProveSnapshotAsofTickV1(
            tick, clock, snapshots) ==
            canonical::SnapshotAsofProofV1::kReady,
        "strictly later next snapshot proves tick as-of complete");
    snapshots.has_next = false;
    canonical::SourceFrontierPageV1 snapshot_equal_page =
        MuxPage(context, 1001U, 100, clock);
    snapshots.frontier_page = &snapshot_equal_page;
    context->Expect(
        canonical::ProveSnapshotAsofTickV1(
            tick, clock, snapshots) ==
            canonical::SnapshotAsofProofV1::kBlockedFrontier,
        "equal snapshot frontier does not prove as-of completeness");
    canonical::SourceFrontierPageV1 snapshot_beyond_page =
        MuxPage(context, 1001U, 101, clock);
    snapshots.frontier_page = &snapshot_beyond_page;
    context->Expect(
        canonical::ProveSnapshotAsofTickV1(
            tick, clock, snapshots) ==
            canonical::SnapshotAsofProofV1::kReady,
        "snapshot frontier strictly beyond tick proves completeness");

    const std::array<canonical::CanonicalEventKeyV1, 4U> history{
        Key(90, 1001U, 1U),
        Key(100, 1001U, 2U),
        Key(100, 1001U, 3U),
        Key(101, 1001U, 4U)};
    const canonical::SnapshotAsofSelectionV1 asof =
        canonical::SelectLatestSnapshotAsofV1(history, tick);
    context->Expect(
        asof.found && asof.index == 2U,
        "as-of selection includes all consumed equal-time snapshots");
}

}  // namespace

int main() {
    TestContext context;
    CheckPageAndProgress(&context);
    CheckIdleInterleavings(&context);
    CheckSafeMuxAndAsof(&context);
    if (context.failures != 0) {
        std::cerr << "phase5 frontier/mux tests failed with "
                  << context.failures << " failure(s)\n";
        return 1;
    }
    std::cout << "phase5 frontier/mux tests passed\n";
    return 0;
}
