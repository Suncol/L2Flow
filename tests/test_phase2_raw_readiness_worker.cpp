#include "l2flow/ingress/raw_live_tail.h"
#include "l2flow/ingress/raw_readiness_observer.h"
#include "l2flow/ingress/raw_readiness_worker.h"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace ingress = l2flow::ingress;

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

class EmptyLiveSource final
    : public ingress::RawLiveTailSource {
public:
    int ReadControl(
        ingress::RawControlSnapshot* output,
        std::uint64_t* generation) noexcept override {
        if (stale) {
            return ESTALE;
        }
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
    bool stale = false;
};

class RotatingEmptyLiveSource final
    : public ingress::RawLiveTailSource {
public:
    int ReadControl(
        ingress::RawControlSnapshot* output,
        std::uint64_t* generation) noexcept override {
        if (output == nullptr || generation == nullptr) {
            return EINVAL;
        }
        *output = control;
        *generation = 3U;
        return 0;
    }

    int InspectSegment(
        std::uint32_t sequence,
        ingress::RawLiveSegmentInfo* output)
        noexcept override {
        if (output == nullptr) {
            return EINVAL;
        }
        if (sequence == first.header.segment_sequence) {
            *output = first;
            return 0;
        }
        if (sequence == second.header.segment_sequence) {
            *output = second;
            return 0;
        }
        return ENOENT;
    }

    ingress::RawLiveReadResult ReadSegmentSome(
        std::uint32_t,
        std::uint64_t,
        std::span<std::byte>) noexcept override {
        return {0U, EIO};
    }

    ingress::RawControlSnapshot control{};
    ingress::RawLiveSegmentInfo first{};
    ingress::RawLiveSegmentInfo second{};
};

struct Clock final {
    static std::uint64_t Now(void* context) noexcept {
        auto* clock = static_cast<Clock*>(context);
        return clock->now++;
    }
    std::uint64_t now = 100U;
};

struct FailureRecorder final {
    static void Notify(
        void* context,
        ingress::RawReadinessWorkerFailureKind kind)
        noexcept {
        auto* recorder =
            static_cast<FailureRecorder*>(context);
        ++recorder->calls;
        recorder->last = kind;
    }
    std::uint64_t calls = 0U;
    ingress::RawReadinessWorkerFailureKind last =
        ingress::RawReadinessWorkerFailureKind::kNone;
};

struct Fixture final {
    Fixture() {
        writer = Pattern<16U>(0x10U);
        stream_day = Pattern<16U>(0x30U);
        source.control.writer_instance = writer;
        source.control.stream_day_id = stream_day;
        source.control.source_stream_id = 2002U;
        source.control.capture_date = 20260718U;
        source.control.segment_sequence = 1U;
        source.control.append_global_wal_pos =
            ingress::kRawV1SegmentHeaderBytes;
        source.control.append_segment_offset =
            ingress::kRawV1SegmentHeaderBytes;
        source.control.durable_global_wal_pos =
            ingress::kRawV1SegmentHeaderBytes;
        source.control.durable_segment_offset =
            ingress::kRawV1SegmentHeaderBytes;
        source.control.heartbeat_monotonic_ns = 99U;

        source.segment.header.source_stream_id = 2002U;
        source.segment.header.capture_date = 20260718U;
        source.segment.header.stream_day_id = stream_day;
        source.segment.header.segment_sequence = 1U;
        source.segment.header.first_ingress_sequence = 1U;
        source.segment.visible_end_offset =
            ingress::kRawV1SegmentHeaderBytes;

        ingress::RawLiveTailAttachV1 attach{};
        attach.writer_instance = writer;
        attach.stream_day_id = stream_day;
        attach.source_stream_id = 2002U;
        attach.capture_date = 20260718U;
        attach.segment_sequence = 1U;
        attach.global_wal_pos =
            ingress::kRawV1SegmentHeaderBytes;
        attach.segment_offset =
            ingress::kRawV1SegmentHeaderBytes;
        attach.next_ingress_sequence = 1U;
        if (ingress::RawLiveTail::Attach(
                &source, attach, &tail) !=
            ingress::RawLiveTailError::kNone) {
            throw std::runtime_error(
                "cannot attach empty live tail fixture");
        }

        ingress::RawReadinessObserverConfig config{};
        config.source_stream_id = 2002U;
        config.market_service_id = 6U;
        config.required_market_messages = {
            {6U, 101U, 33U},
            {6U, 101U, 36U},
        };
        config.maximum_lag_bytes = 1U << 20U;
        config.heartbeat_timeout_ns = 1'000U;
        if (ingress::RawReadinessObserver::Create(
                std::move(config), &observer) !=
            ingress::RawReadinessObserverCreateError::
                kNone) {
            throw std::runtime_error(
                "cannot create readiness observer fixture");
        }

        generation.writer_instance = writer;
        generation.stream_day_id = stream_day;
        generation.source_stream_id = 2002U;
        generation.capture_date = 20260718U;
        generation.connect_generation = 1U;
        generation.recovery_wal_pos =
            ingress::kRawV1SegmentHeaderBytes;
        generation.recovery_next_ingress_sequence = 1U;
        generation.recovery_segment_sequence = 1U;
        generation.recovery_segment_offset =
            ingress::kRawV1SegmentHeaderBytes;
    }

    EmptyLiveSource source;
    l2flow::common::Identity128 writer{};
    l2flow::common::Identity128 stream_day{};
    std::unique_ptr<ingress::RawLiveTail> tail;
    std::unique_ptr<ingress::RawReadinessObserver> observer;
    ingress::RawReadinessObserverGeneration generation{};
    Clock clock;
    FailureRecorder failures;
};

ingress::RawReadinessWorkerConfig WorkerConfig(
    Fixture* fixture) {
    ingress::RawReadinessWorkerConfig config{};
    config.monotonic_now = &Clock::Now;
    config.monotonic_clock_context = &fixture->clock;
    config.failure_callback = &FailureRecorder::Notify;
    config.failure_context = &fixture->failures;
    return config;
}

ingress::RawReadinessStopCursorV1 StopCursor(
    const Fixture& fixture,
    std::uint64_t global_wal_pos =
        ingress::kRawV1SegmentHeaderBytes,
    std::uint64_t ingress_sequence = 0U,
    std::uint64_t segment_offset =
        ingress::kRawV1SegmentHeaderBytes) {
    ingress::RawReadinessStopCursorV1 result;
    result.writer_instance = fixture.writer;
    result.stream_day_id = fixture.stream_day;
    result.source_stream_id = 2002U;
    result.capture_date = 20260718U;
    result.segment_sequence = 1U;
    result.global_wal_pos = global_wal_pos;
    result.ingress_sequence = ingress_sequence;
    result.segment_offset = segment_offset;
    return result;
}

void TestExactEmptyCatchup(TestContext* test) {
    Fixture fixture;
    ingress::RawReadinessWorker worker(
        WorkerConfig(&fixture),
        *fixture.tail,
        *fixture.observer,
        fixture.generation);
    const auto stop = StopCursor(fixture);
    test->Expect(
        worker.StopAt(stop) &&
            worker.StopAt(stop) &&
            worker.Run(),
        "empty observer generation catches up exactly to an idempotent typed target");
    const auto snapshot = worker.Snapshot();
    test->Expect(
        snapshot.startup_succeeded &&
            snapshot.finished &&
            snapshot.requested_stop == stop &&
            snapshot.processed_wal_pos ==
                ingress::kRawV1SegmentHeaderBytes &&
            snapshot.processed_ingress_sequence ==
                0U &&
            snapshot.live_tail_segment_sequence ==
                stop.segment_sequence &&
            snapshot.live_tail_segment_offset ==
                stop.segment_offset &&
            snapshot
                    .live_tail_next_ingress_sequence ==
                stop.ingress_sequence + 1U &&
            snapshot.failure_kind ==
                ingress::RawReadinessWorkerFailureKind::
                    kNone &&
            fixture.failures.calls == 0U,
        "successful catch-up publishes coherent worker state");
}

void TestInvalidStopTargetIsRejected(TestContext* test) {
    Fixture fixture;
    ingress::RawReadinessWorker worker(
        WorkerConfig(&fixture),
        *fixture.tail,
        *fixture.observer,
        fixture.generation);
    test->Expect(
        !worker.StopAt(StopCursor(
            fixture,
            ingress::kRawV1SegmentHeaderBytes - 1U,
            0U,
            ingress::kRawV1SegmentHeaderBytes)) &&
            !worker.Run(),
        "stop target with inconsistent global and segment cursors is rejected");
    test->Expect(
        worker.Snapshot().failure_kind ==
                ingress::RawReadinessWorkerFailureKind::
                    kStopTargetInvalid &&
            fixture.failures.calls == 1U,
        "invalid typed target is first-writer-wins fatal evidence");
}

void TestForeignStopIdentityIsRejected(
    TestContext* test) {
    Fixture fixture;
    ingress::RawReadinessWorker worker(
        WorkerConfig(&fixture),
        *fixture.tail,
        *fixture.observer,
        fixture.generation);
    auto stop = StopCursor(fixture);
    stop.stream_day_id[0U] ^= std::byte{0x55U};
    test->Expect(
        !worker.StopAt(stop),
        "foreign stream-day target is rejected before catch-up");
    test->Expect(
        worker.Snapshot().failure_kind ==
                ingress::RawReadinessWorkerFailureKind::
                    kStopTargetInvalid &&
            fixture.failures.calls == 1U,
        "numeric cursor equality cannot bypass typed stop identity");
}

void TestFinalCatchupTimeout(TestContext* test) {
    Fixture fixture;
    auto config = WorkerConfig(&fixture);
    config.final_catch_up_timeout_ns = 2U;
    ingress::RawReadinessWorker worker(
        config,
        *fixture.tail,
        *fixture.observer,
        fixture.generation);
    test->Expect(
        worker.StopAt(StopCursor(
            fixture,
            ingress::kRawV1SegmentHeaderBytes + 8U,
            1U,
            ingress::kRawV1SegmentHeaderBytes + 8U)) &&
            !worker.Run(),
        "unreachable final frontier terminates at configured catch-up deadline");
    const auto snapshot = worker.Snapshot();
    test->Expect(
        snapshot.finished &&
            snapshot.failure_kind ==
                ingress::RawReadinessWorkerFailureKind::
                    kStopCatchUpTimedOut &&
            fixture.failures.calls == 1U,
        "timeout produces one stable fatal result instead of an unbounded join");
}

void TestEmptyRotatedSegmentCatchup(
    TestContext* test) {
    Fixture fixture;
    RotatingEmptyLiveSource source;
    source.first = fixture.source.segment;
    source.first.header.segment_base_wal_pos = 0U;
    source.first.visible_end_offset =
        ingress::kRawV1SegmentHeaderBytes;
    source.first.sealed = true;
    source.second = source.first;
    source.second.header.segment_sequence = 2U;
    source.second.header.segment_base_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    source.second.header.first_ingress_sequence = 1U;
    source.second.sealed = false;
    source.control = fixture.source.control;
    source.control.segment_sequence = 2U;
    source.control.append_global_wal_pos =
        2U * ingress::kRawV1SegmentHeaderBytes;
    source.control.append_segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    source.control.append_ingress_sequence = 0U;
    source.control.durable_global_wal_pos =
        source.control.append_global_wal_pos;
    source.control.durable_segment_offset =
        source.control.append_segment_offset;
    source.control.durable_ingress_sequence = 0U;

    ingress::RawLiveTailAttachV1 attach;
    attach.writer_instance = fixture.writer;
    attach.stream_day_id = fixture.stream_day;
    attach.source_stream_id = 2002U;
    attach.capture_date = 20260718U;
    attach.segment_sequence = 1U;
    attach.global_wal_pos =
        ingress::kRawV1SegmentHeaderBytes;
    attach.segment_offset =
        ingress::kRawV1SegmentHeaderBytes;
    attach.next_ingress_sequence = 1U;
    std::unique_ptr<ingress::RawLiveTail> tail;
    test->Expect(
        ingress::RawLiveTail::Attach(
            &source, attach, &tail) ==
                ingress::RawLiveTailError::kNone &&
            tail != nullptr,
        "empty rotation fixture attaches at the sealed old frontier");
    if (tail == nullptr) {
        return;
    }

    ingress::RawReadinessWorker worker(
        WorkerConfig(&fixture),
        *tail,
        *fixture.observer,
        fixture.generation);
    auto stop = StopCursor(
        fixture,
        2U * ingress::kRawV1SegmentHeaderBytes,
        0U,
        ingress::kRawV1SegmentHeaderBytes);
    stop.segment_sequence = 2U;
    test->Expect(
        worker.StopAt(stop) && worker.Run(),
        "explicit segment transition lets clean stop catch up to an empty new segment");
    const auto snapshot = worker.Snapshot();
    test->Expect(
        snapshot.processed_wal_pos ==
                stop.global_wal_pos &&
            snapshot.processed_ingress_sequence ==
                stop.ingress_sequence &&
            snapshot.processed_segment_sequence ==
                stop.segment_sequence &&
            snapshot.processed_segment_offset ==
                stop.segment_offset &&
            snapshot.failure_kind ==
                ingress::RawReadinessWorkerFailureKind::
                    kNone,
        "header frontier advances WAL and segment coordinates without advancing ingress");
}

void TestInstanceFenceIsFatal(TestContext* test) {
    Fixture fixture;
    fixture.source.stale = true;
    ingress::RawReadinessWorker worker(
        WorkerConfig(&fixture),
        *fixture.tail,
        *fixture.observer,
        fixture.generation);
    test->Expect(
        worker.StopAt(StopCursor(
            fixture,
            ingress::kRawV1SegmentHeaderBytes + 8U,
            1U,
            ingress::kRawV1SegmentHeaderBytes + 8U)) &&
            !worker.Run(),
        "live-tail instance replacement terminates observer worker");
    test->Expect(
        worker.Snapshot().failure_kind ==
                ingress::RawReadinessWorkerFailureKind::
                    kLiveTailInstanceChanged &&
            fixture.failures.last ==
                ingress::RawReadinessWorkerFailureKind::
                    kLiveTailInstanceChanged &&
            worker.Snapshot().live_tail_error ==
                ingress::RawLiveTailError::
                    kInstanceChanged,
        "instance fence and its first-failure detail are coherently published");
}

}  // namespace

int main() {
    TestContext test;
    TestExactEmptyCatchup(&test);
    TestInvalidStopTargetIsRejected(&test);
    TestForeignStopIdentityIsRejected(&test);
    TestFinalCatchupTimeout(&test);
    TestEmptyRotatedSegmentCatchup(&test);
    TestInstanceFenceIsFatal(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Raw readiness worker test(s) failed\n";
        return 1;
    }
    std::cout << "Phase 2 Raw readiness worker tests passed\n";
    return 0;
}
