#include "l2flow/market/instrument_history_v1.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace market = l2flow::market;

namespace {

using namespace std::chrono_literals;

class TestContext final {
public:
    void Expect(bool condition, std::string_view description) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << description << '\n';
        }
    }

    [[nodiscard]] int failures() const noexcept {
        return failures_;
    }

private:
    int failures_ = 0;
};

template <typename Event>
market::RetainedMarketEventV1 Retained(Event event) {
    using Owner = std::unique_ptr<const Event>;
    return market::RetainedMarketEventV1(
        std::in_place_type<Owner>,
        std::make_unique<const Event>(std::move(event)));
}

void PopulateCommon(
    market::DecodedMarketCommonV1* common,
    market::MarketEventKindV1 kind,
    std::uint32_t source_stream_id,
    std::uint64_t source_sequence,
    std::uint32_t instrument_id) {
    common->kind = kind;
    common->market =
        kind == market::MarketEventKindV1::kShanghaiSnapshot ||
                kind == market::MarketEventKindV1::kShanghaiTick
            ? market::MarketV1::kShanghai
            : market::MarketV1::kShenzhen;
    common->origin.source_stream_id = source_stream_id;
    common->origin.trade_date = 20260722U;
    common->origin.source_sequence = source_sequence;
    common->origin.recv_monotonic_ns =
        static_cast<std::int64_t>(source_sequence);
    common->origin.recv_realtime_ns =
        static_cast<std::int64_t>(source_sequence + 1000U);
    common->instrument_id = instrument_id;
}

std::optional<market::OwnedInstrumentEventEnvelopeV1> MakeEnvelope(
    std::uint8_t source_slot,
    std::uint32_t source_stream_id,
    std::uint64_t source_sequence,
    std::uint32_t instrument_id,
    market::MarketEventKindV1 kind,
    market::OwnedInstrumentEventCreateErrorV1* error = nullptr) {
    market::RetainedMarketEventV1 retained;
    switch (kind) {
        case market::MarketEventKindV1::kShanghaiSnapshot: {
            market::ShanghaiSnapshotV1 event{};
            PopulateCommon(
                &event.common, kind, source_stream_id,
                source_sequence, instrument_id);
            retained = Retained(std::move(event));
            break;
        }
        case market::MarketEventKindV1::kShanghaiTick: {
            market::ShanghaiTickV1 event{};
            PopulateCommon(
                &event.common, kind, source_stream_id,
                source_sequence, instrument_id);
            retained = Retained(std::move(event));
            break;
        }
        case market::MarketEventKindV1::kShenzhenSnapshot: {
            market::ShenzhenSnapshotV1 event{};
            PopulateCommon(
                &event.common, kind, source_stream_id,
                source_sequence, instrument_id);
            retained = Retained(std::move(event));
            break;
        }
        case market::MarketEventKindV1::kShenzhenOrder: {
            market::ShenzhenOrderV1 event{};
            PopulateCommon(
                &event.common, kind, source_stream_id,
                source_sequence, instrument_id);
            retained = Retained(std::move(event));
            break;
        }
        case market::MarketEventKindV1::kShenzhenTransaction: {
            market::ShenzhenTransactionV1 event{};
            PopulateCommon(
                &event.common, kind, source_stream_id,
                source_sequence, instrument_id);
            retained = Retained(std::move(event));
            break;
        }
    }
    std::optional<market::OwnedInstrumentEventEnvelopeV1> envelope;
    const auto create_error =
        market::OwnedInstrumentEventEnvelopeV1::Create(
            source_slot, std::move(retained), &envelope);
    if (error != nullptr) {
        *error = create_error;
    }
    return envelope;
}

market::InstrumentHistoryRuntimeConfigV1 Config(
    std::uint32_t workers = 4U) {
    market::InstrumentHistoryRuntimeConfigV1 config{};
    config.source_stream_ids = {101U, 102U, 103U, 104U};
    config.physical_worker_count = workers;
    config.queue_capacity = 8U;
    config.maximum_inflight_per_source = 128U;
    config.chunk_record_capacity = 3U;
    config.maximum_records_per_query = 1024U;
    config.maximum_records_per_logical_shard = 100'000U;
    config.maximum_instruments_per_logical_shard = 1'000U;
    config.maximum_owned_payload_bytes_per_logical_shard =
        64U * 1024U * 1024U;
    return config;
}

std::unique_ptr<market::InstrumentHistoryRuntimeV1> Runtime(
    TestContext* test,
    market::InstrumentHistoryRuntimeConfigV1 config = Config()) {
    std::unique_ptr<market::InstrumentHistoryRuntimeV1> runtime;
    const auto error = market::InstrumentHistoryRuntimeV1::Create(
        std::move(config), &runtime);
    test->Expect(
        error == market::InstrumentHistoryCreateErrorV1::kNone &&
            runtime != nullptr,
        "instrument history runtime creates");
    return runtime;
}

void SubmitRetry(
    TestContext* test,
    market::InstrumentHistoryRuntimeV1* runtime,
    std::optional<market::OwnedInstrumentEventEnvelopeV1>* envelope) {
    for (;;) {
        const std::uint32_t instrument_before =
            (*envelope)->instrument_id();
        const std::uint64_t sequence_before =
            (*envelope)->source_sequence();
        const auto* event_before = &(*envelope)->event();
        const auto error = runtime->TrySubmit(std::move(**envelope));
        if (error == market::InstrumentHistorySubmitErrorV1::kNone) {
            envelope->reset();
            return;
        }
        test->Expect(
            error == market::InstrumentHistorySubmitErrorV1::kQueueFull ||
                error ==
                    market::InstrumentHistorySubmitErrorV1::kInflightLimit,
            "retry helper sees only bounded backpressure");
        test->Expect(
            envelope->has_value() &&
                (*envelope)->instrument_id() == instrument_before &&
                (*envelope)->source_sequence() == sequence_before &&
                (*envelope)->dispatch_ticket() == 0U &&
                &(*envelope)->event() == event_before,
            "backpressure preserves the complete retry envelope");
        std::this_thread::yield();
    }
}

void TestOwnedEnvelopeAndTopology(TestContext* test) {
    static_assert(!std::is_copy_constructible_v<
                  market::OwnedInstrumentEventEnvelopeV1>);
    static_assert(std::is_nothrow_move_constructible_v<
                  market::OwnedInstrumentEventEnvelopeV1>);

    const std::array kinds{
        market::MarketEventKindV1::kShanghaiSnapshot,
        market::MarketEventKindV1::kShanghaiTick,
        market::MarketEventKindV1::kShenzhenSnapshot,
        market::MarketEventKindV1::kShenzhenOrder,
        market::MarketEventKindV1::kShenzhenTransaction};
    for (const auto kind : kinds) {
        auto envelope = MakeEnvelope(0U, 101U, 1U, 33U, kind);
        test->Expect(envelope.has_value(), "owned envelope creates");
        const bool snapshot =
            kind == market::MarketEventKindV1::kShanghaiSnapshot ||
            kind == market::MarketEventKindV1::kShenzhenSnapshot;
        test->Expect(
            envelope->lane() ==
                (snapshot
                     ? market::InstrumentHistoryLaneV1::kSnapshot
                     : market::InstrumentHistoryLaneV1::kTick),
            "event kind derives the exact snapshot/tick lane");
    }

    auto order = MakeEnvelope(
        3U, 104U, 10U, 49U,
        market::MarketEventKindV1::kShenzhenOrder);
    auto transaction = MakeEnvelope(
        3U, 104U, 11U, 49U,
        market::MarketEventKindV1::kShenzhenTransaction);
    test->Expect(
        order->lane() == market::InstrumentHistoryLaneV1::kTick &&
            transaction->lane() ==
                market::InstrumentHistoryLaneV1::kTick,
        "Shenzhen order and transaction share the tick lane");

    market::ShanghaiTickV1 borrowed{};
    PopulateCommon(
        &borrowed.common,
        market::MarketEventKindV1::kShanghaiTick,
        101U, 5U, 33U);
    const std::array<std::byte, 1U> borrowed_body{std::byte{1U}};
    borrowed.common.origin.body = borrowed_body;
    std::optional<market::OwnedInstrumentEventEnvelopeV1> rejected;
    const auto borrowed_error =
        market::OwnedInstrumentEventEnvelopeV1::Create(
            0U, Retained(std::move(borrowed)), &rejected);
    test->Expect(
        borrowed_error ==
                market::OwnedInstrumentEventCreateErrorV1::kBorrowedBody &&
            !rejected.has_value(),
        "envelope rejects retained borrowed body memory");

    auto runtime = Runtime(test, Config(4U));
    for (std::uint8_t shard = 0U; shard < 16U; ++shard) {
        test->Expect(
            runtime->PhysicalWorkerForLogicalShard(shard) == shard % 4U,
            "default topology maps logical shard modulo physical workers");
    }
    runtime->StopAndDrain();

    auto invalid = Config(2U);
    invalid.use_explicit_worker_mapping = true;
    invalid.physical_worker_by_logical_shard.fill(0U);
    std::unique_ptr<market::InstrumentHistoryRuntimeV1> rejected_runtime;
    test->Expect(
        market::InstrumentHistoryRuntimeV1::Create(
            invalid, &rejected_runtime) ==
                market::InstrumentHistoryCreateErrorV1::
                    kInvalidWorkerMapping &&
            rejected_runtime == nullptr,
        "explicit topology rejects a physical worker with no shard");

    invalid = Config(1U);
    invalid.maximum_records_per_query = 0U;
    test->Expect(
        market::InstrumentHistoryRuntimeV1::Create(
            invalid, &rejected_runtime) ==
                market::InstrumentHistoryCreateErrorV1::
                    kInvalidQueryLimit &&
            rejected_runtime == nullptr,
        "history creation requires a nonzero query limit");

    invalid = Config(1U);
    invalid.maximum_owned_payload_bytes_per_logical_shard = 0U;
    test->Expect(
        market::InstrumentHistoryRuntimeV1::Create(
            invalid, &rejected_runtime) ==
                market::InstrumentHistoryCreateErrorV1::
                    kInvalidStoreLimits &&
            rejected_runtime == nullptr,
        "history creation requires an explicit retained-byte limit");
}

void TestRetainedPayloadLimit(TestContext* test) {
    auto envelope = MakeEnvelope(
        0U, 101U, 1U, 16U,
        market::MarketEventKindV1::kShanghaiSnapshot);
    test->Expect(
        envelope.has_value() && envelope->owned_payload_bytes() > 0U,
        "payload-limit test creates a sized retained envelope");
    auto config = Config(1U);
    config.maximum_owned_payload_bytes_per_logical_shard =
        static_cast<std::uint64_t>(envelope->owned_payload_bytes() - 1U);
    auto runtime = Runtime(test, config);
    test->Expect(
        runtime->TrySubmit(std::move(*envelope)) ==
            market::InstrumentHistorySubmitErrorV1::kNone,
        "payload-limit record is admitted to its bounded worker queue");
    const auto barrier = runtime->CaptureBarrier(0U);
    test->Expect(
        runtime->WaitForBarrier(barrier, 2s) ==
                market::InstrumentHistoryBarrierWaitErrorV1::kSourceFatal &&
            runtime->Frontier(0U).fatal,
        "retained payload capacity fails the source closed without visibility");
}

void TestQueriesAndSourceLanes(TestContext* test) {
    auto runtime = Runtime(test);
    const std::array sequences{10U, 12U, 20U, 25U, 40U};
    for (const std::uint64_t sequence : sequences) {
        auto envelope = MakeEnvelope(
            0U, 101U, sequence, 33U,
            market::MarketEventKindV1::kShanghaiSnapshot);
        SubmitRetry(test, runtime.get(), &envelope);
    }
    auto order = MakeEnvelope(
        3U, 104U, 100U, 33U,
        market::MarketEventKindV1::kShenzhenOrder);
    auto transaction = MakeEnvelope(
        3U, 104U, 103U, 33U,
        market::MarketEventKindV1::kShenzhenTransaction);
    SubmitRetry(test, runtime.get(), &order);
    SubmitRetry(test, runtime.get(), &transaction);

    const auto source0_barrier = runtime->CaptureBarrier(0U);
    const auto source3_barrier = runtime->CaptureBarrier(3U);
    test->Expect(
        runtime->WaitForBarrier(source0_barrier, 2s) ==
                market::InstrumentHistoryBarrierWaitErrorV1::kNone &&
            runtime->WaitForBarrier(source3_barrier, 2s) ==
                market::InstrumentHistoryBarrierWaitErrorV1::kNone,
        "source-local barriers reach exact submitted prefixes");

    market::InstrumentHistoryRecordHandleV1 latest;
    test->Expect(
        runtime->Latest(
            33U, 0U, market::InstrumentHistoryLaneV1::kSnapshot,
            &latest) == market::InstrumentHistoryQueryErrorV1::kNone &&
            latest && latest->source_sequence() == 40U,
        "Latest returns the source-lane newest record");

    std::vector<market::InstrumentHistoryRecordHandleV1> tail;
    test->Expect(
        runtime->Tail(
            33U, 0U, market::InstrumentHistoryLaneV1::kSnapshot,
            3U, &tail) ==
                market::InstrumentHistoryQueryErrorV1::kNone &&
            tail.size() == 3U &&
            tail[0]->source_sequence() == 20U &&
            tail[1]->source_sequence() == 25U &&
            tail[2]->source_sequence() == 40U,
        "Tail crosses chunks and stays ascending");

    std::vector<market::InstrumentHistoryRecordHandleV1> range;
    auto range_result = runtime->RangeBySourceSequence(
        33U, 0U, market::InstrumentHistoryLaneV1::kSnapshot,
        11U, 41U, 10U, &range);
    test->Expect(
        range_result.error ==
                market::InstrumentHistoryQueryErrorV1::kNone &&
            !range_result.truncated && range.size() == 4U &&
            range.front()->source_sequence() == 12U &&
            range.back()->source_sequence() == 40U,
        "sequence range is [begin,end) across chunks");
    range_result = runtime->RangeBySourceSequence(
        33U, 0U, market::InstrumentHistoryLaneV1::kSnapshot,
        11U, 41U, 2U, &range);
    test->Expect(
        range_result.error ==
                market::InstrumentHistoryQueryErrorV1::kNone &&
            range_result.truncated && range.size() == 2U,
        "sequence range reports its hard output bound");

    test->Expect(
        runtime->Tail(
            33U, 3U, market::InstrumentHistoryLaneV1::kTick,
            10U, &tail) ==
                market::InstrumentHistoryQueryErrorV1::kNone &&
            tail.size() == 2U &&
            tail[0]->kind() ==
                market::MarketEventKindV1::kShenzhenOrder &&
            tail[1]->kind() ==
                market::MarketEventKindV1::kShenzhenTransaction,
        "Shenzhen order and transaction append to one ordered tick lane");
    test->Expect(
        runtime->Latest(
            33U, 0U, market::InstrumentHistoryLaneV1::kTick,
            &latest) == market::InstrumentHistoryQueryErrorV1::kNotFound,
        "query does not merge another source lane");
}

void TestQueryHardLimit(TestContext* test) {
    auto config = Config(1U);
    config.maximum_records_per_query = 2U;
    auto runtime = Runtime(test, config);
    for (std::uint64_t sequence = 1U; sequence <= 3U; ++sequence) {
        auto envelope = MakeEnvelope(
            0U, 101U, sequence, 32U,
            market::MarketEventKindV1::kShanghaiTick);
        SubmitRetry(test, runtime.get(), &envelope);
    }
    const auto barrier = runtime->CaptureBarrier(0U);
    test->Expect(
        runtime->WaitForBarrier(barrier, 2s) ==
            market::InstrumentHistoryBarrierWaitErrorV1::kNone,
        "query-limit fixture reaches its history barrier");

    std::vector<market::InstrumentHistoryRecordHandleV1> output;
    test->Expect(
        runtime->Tail(
            32U, 0U, market::InstrumentHistoryLaneV1::kTick,
            3U, &output) ==
                market::InstrumentHistoryQueryErrorV1::
                    kQueryLimitExceeded &&
            output.empty(),
        "Tail rejects work above the configured per-query hard cap");
    const auto range = runtime->RangeBySourceSequence(
        32U, 0U, market::InstrumentHistoryLaneV1::kTick,
        1U, 4U, 3U, &output);
    test->Expect(
        range.error ==
                market::InstrumentHistoryQueryErrorV1::
                    kQueryLimitExceeded &&
            range.matched_records == 0U && !range.truncated &&
            output.empty(),
        "Range rejects work above the configured per-query hard cap");
}

struct BarrierGate final {
    std::atomic<bool> first_entered{false};
    std::atomic<bool> release_first{false};
};

void GateFirstAppend(
    void* context,
    std::uint32_t,
    const market::OwnedInstrumentEventEnvelopeV1& envelope) noexcept {
    auto* gate = static_cast<BarrierGate*>(context);
    if (envelope.source_slot() == 0U &&
        envelope.dispatch_ticket() == 1U) {
        gate->first_entered.store(true, std::memory_order_release);
        while (!gate->release_first.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }
}

struct QuerySnapshotGate final {
    std::atomic<bool> armed{false};
    std::atomic<bool> entered{false};
    std::atomic<bool> release{false};
};

void GateAfterQuerySnapshot(void* context) noexcept {
    auto* gate = static_cast<QuerySnapshotGate*>(context);
    if (gate == nullptr ||
        !gate->armed.exchange(false, std::memory_order_acq_rel)) {
        return;
    }
    gate->entered.store(true, std::memory_order_release);
    while (!gate->release.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
}

void ArmQueryGate(QuerySnapshotGate* gate) {
    gate->entered.store(false, std::memory_order_relaxed);
    gate->release.store(false, std::memory_order_relaxed);
    gate->armed.store(true, std::memory_order_release);
}

bool WaitForQueryGate(
    const QuerySnapshotGate& gate,
    std::chrono::milliseconds timeout = 2s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!gate.entered.load(std::memory_order_acquire)) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::yield();
    }
    return true;
}

void TestExactBarrierAndVisibility(TestContext* test) {
    BarrierGate gate;
    auto config = Config(2U);
    config.before_append_hook = &GateFirstAppend;
    config.before_append_hook_context = &gate;
    auto runtime = Runtime(test, config);

    // Instrument 16 belongs to logical shard/worker 0; 17 belongs to worker 1.
    auto first = MakeEnvelope(
        0U, 101U, 100U, 16U,
        market::MarketEventKindV1::kShanghaiTick);
    SubmitRetry(test, runtime.get(), &first);
    while (!gate.first_entered.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    auto second = MakeEnvelope(
        0U, 101U, 101U, 17U,
        market::MarketEventKindV1::kShanghaiTick);
    SubmitRetry(test, runtime.get(), &second);
    const auto barrier = runtime->CaptureBarrier(0U);

    market::InstrumentHistorySourceFrontierV1 frontier{};
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    do {
        frontier = runtime->Frontier(0U);
        if (frontier.completed_out_of_order != 0U) {
            break;
        }
        std::this_thread::yield();
    } while (std::chrono::steady_clock::now() < deadline);
    test->Expect(
        frontier.submitted_ticket == 2U &&
            frontier.acknowledged_ticket == 0U &&
            frontier.completed_out_of_order == 1U,
        "later worker completion cannot cross the earlier ticket gap");
    market::InstrumentHistoryRecordHandleV1 hidden;
    test->Expect(
        runtime->Latest(
            17U, 0U, market::InstrumentHistoryLaneV1::kTick,
            &hidden) == market::InstrumentHistoryQueryErrorV1::kNotFound,
        "physically appended later record stays hidden before exact ACK prefix");

    gate.release_first.store(true, std::memory_order_release);
    test->Expect(
        runtime->WaitForBarrier(barrier, 2s) ==
                market::InstrumentHistoryBarrierWaitErrorV1::kNone,
        "barrier reaches only after the missing earlier completion");
    frontier = runtime->Frontier(0U);
    test->Expect(
        frontier.acknowledged_ticket == 2U &&
            frontier.acknowledged_source_sequence == 101U &&
            frontier.completed_out_of_order == 0U,
        "frontier advances through the exact contiguous completion prefix");
    test->Expect(
        runtime->Latest(
            17U, 0U, market::InstrumentHistoryLaneV1::kTick,
            &hidden) == market::InstrumentHistoryQueryErrorV1::kNone &&
            hidden->source_sequence() == 101U,
        "record becomes visible after its exact source barrier");
}

void TestBackpressurePreservesOwnership(TestContext* test) {
    BarrierGate gate;
    auto config = Config(1U);
    config.queue_capacity = 1U;
    config.maximum_inflight_per_source = 2U;
    config.before_append_hook = &GateFirstAppend;
    config.before_append_hook_context = &gate;
    auto runtime = Runtime(test, config);

    auto first = MakeEnvelope(
        0U, 101U, 1U, 16U,
        market::MarketEventKindV1::kShanghaiTick);
    SubmitRetry(test, runtime.get(), &first);
    while (!gate.first_entered.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    auto second = MakeEnvelope(
        0U, 101U, 2U, 16U,
        market::MarketEventKindV1::kShanghaiTick);
    SubmitRetry(test, runtime.get(), &second);

    auto queue_full = MakeEnvelope(
        0U, 101U, 3U, 16U,
        market::MarketEventKindV1::kShanghaiTick);
    const auto* queue_full_event = &queue_full->event();
    test->Expect(
        runtime->TrySubmit(std::move(*queue_full)) ==
                market::InstrumentHistorySubmitErrorV1::kQueueFull &&
            queue_full.has_value() &&
            queue_full->dispatch_ticket() == 0U &&
            queue_full->source_sequence() == 3U &&
            &queue_full->event() == queue_full_event,
        "queue-full result preserves the exact move-only envelope");

    auto inflight = MakeEnvelope(
        0U, 101U, 3U, 17U,
        market::MarketEventKindV1::kShanghaiTick);
    const auto* inflight_event = &inflight->event();
    test->Expect(
        runtime->TrySubmit(std::move(*inflight)) ==
                market::InstrumentHistorySubmitErrorV1::kInflightLimit &&
            inflight.has_value() && inflight->dispatch_ticket() == 0U &&
            inflight->source_sequence() == 3U &&
            &inflight->event() == inflight_event,
        "inflight-limit result preserves the exact move-only envelope");

    const auto first_two = runtime->CaptureBarrier(0U);
    gate.release_first.store(true, std::memory_order_release);
    test->Expect(
        runtime->WaitForBarrier(first_two, 2s) ==
            market::InstrumentHistoryBarrierWaitErrorV1::kNone,
        "blocked worker drains after backpressure test releases it");
    SubmitRetry(test, runtime.get(), &inflight);
    const auto third = runtime->CaptureBarrier(0U);
    test->Expect(
        runtime->WaitForBarrier(third, 2s) ==
                market::InstrumentHistoryBarrierWaitErrorV1::kNone &&
            runtime->Frontier(0U).acknowledged_ticket == 3U,
        "unchanged inflight envelope is retryable after ACK progress");
}

void TestConcurrentQueriesDoNotHoldWriterLock(TestContext* test) {
    QuerySnapshotGate gate;
    auto config = Config(2U);
    config.queue_capacity = 16U;
    config.maximum_inflight_per_source = 512U;
    config.chunk_record_capacity = 8U;
    config.maximum_records_per_query = 512U;
    config.after_query_snapshot_hook = &GateAfterQuerySnapshot;
    config.after_query_snapshot_hook_context = &gate;
    auto runtime = Runtime(test, config);

    constexpr std::uint32_t kInstrument = 48U;
    constexpr std::uint64_t kInitialRecords = 256U;
    for (std::uint64_t sequence = 1U;
         sequence <= kInitialRecords; ++sequence) {
        auto envelope = MakeEnvelope(
            0U, 101U, sequence, kInstrument,
            market::MarketEventKindV1::kShanghaiTick);
        SubmitRetry(test, runtime.get(), &envelope);
    }
    auto barrier = runtime->CaptureBarrier(0U);
    test->Expect(
        runtime->WaitForBarrier(barrier, 5s) ==
            market::InstrumentHistoryBarrierWaitErrorV1::kNone,
        "concurrent-query fixture seals multiple immutable chunks");

    market::InstrumentHistoryRangeResultV1 range_result{};
    std::vector<market::InstrumentHistoryRecordHandleV1> range;
    ArmQueryGate(&gate);
    std::thread range_query([&]() {
        range_result = runtime->RangeBySourceSequence(
            kInstrument, 0U,
            market::InstrumentHistoryLaneV1::kTick,
            1U, 1'000U,
            static_cast<std::size_t>(kInitialRecords), &range);
    });
    const bool range_snapshot_reached = WaitForQueryGate(gate);
    test->Expect(
        range_snapshot_reached,
        "Range reaches its post-lock immutable snapshot seam");
    auto after_range_snapshot = MakeEnvelope(
        0U, 101U, kInitialRecords + 1U, kInstrument,
        market::MarketEventKindV1::kShanghaiTick);
    SubmitRetry(test, runtime.get(), &after_range_snapshot);
    barrier = runtime->CaptureBarrier(0U);
    const bool range_writer_progressed =
        runtime->WaitForBarrier(barrier, 2s) ==
        market::InstrumentHistoryBarrierWaitErrorV1::kNone;
    gate.release.store(true, std::memory_order_release);
    range_query.join();
    test->Expect(
        range_writer_progressed,
        "writer appends and ACKs while a large Range traverses its snapshot");
    test->Expect(
        range_result.error ==
                market::InstrumentHistoryQueryErrorV1::kNone &&
            !range_result.truncated && range.size() == kInitialRecords &&
            range.front()->source_sequence() == 1U &&
            range.back()->source_sequence() == kInitialRecords,
        "Range remains bounded by its pre-append ACK/source snapshot");

    market::InstrumentHistoryQueryErrorV1 tail_error =
        market::InstrumentHistoryQueryErrorV1::kNotFound;
    std::vector<market::InstrumentHistoryRecordHandleV1> tail;
    constexpr std::size_t kTailRecords = 128U;
    ArmQueryGate(&gate);
    std::thread tail_query([&]() {
        tail_error = runtime->Tail(
            kInstrument, 0U,
            market::InstrumentHistoryLaneV1::kTick,
            kTailRecords, &tail);
    });
    const bool tail_snapshot_reached = WaitForQueryGate(gate);
    test->Expect(
        tail_snapshot_reached,
        "Tail reaches its post-lock immutable snapshot seam");
    auto after_tail_snapshot = MakeEnvelope(
        0U, 101U, kInitialRecords + 2U, kInstrument,
        market::MarketEventKindV1::kShanghaiTick);
    SubmitRetry(test, runtime.get(), &after_tail_snapshot);
    barrier = runtime->CaptureBarrier(0U);
    const bool tail_writer_progressed =
        runtime->WaitForBarrier(barrier, 2s) ==
        market::InstrumentHistoryBarrierWaitErrorV1::kNone;
    gate.release.store(true, std::memory_order_release);
    tail_query.join();
    test->Expect(
        tail_writer_progressed,
        "writer appends into the active chunk while Tail holds pinned handles");
    test->Expect(
        tail_error == market::InstrumentHistoryQueryErrorV1::kNone &&
            tail.size() == kTailRecords &&
            tail.front()->source_sequence() == 130U &&
            tail.back()->source_sequence() == kInitialRecords + 1U,
        "Tail is ascending and excludes an append beyond its ACK snapshot");
    for (std::size_t index = 1U; index < tail.size(); ++index) {
        test->Expect(
            tail[index - 1U]->source_sequence() <
                tail[index]->source_sequence(),
            "concurrent Tail remains strictly source-sequence ordered");
    }
}

void TestFourConcurrentSources(TestContext* test) {
    auto config = Config(4U);
    config.queue_capacity = 2U;
    config.maximum_inflight_per_source = 16U;
    auto runtime = Runtime(test, config);
    constexpr std::uint64_t kEventsPerSource = 300U;
    std::array<std::thread, 4U> producers;
    std::atomic<int> producer_failures{0};
    for (std::uint8_t source = 0U; source < 4U; ++source) {
        producers[source] = std::thread([&, source]() {
            for (std::uint64_t index = 0U;
                 index < kEventsPerSource; ++index) {
                const std::uint64_t sequence = 1U + index * 2U;
                const std::uint32_t instrument =
                    1'000U + static_cast<std::uint32_t>(index % 17U);
                market::MarketEventKindV1 kind =
                    market::MarketEventKindV1::kShanghaiSnapshot;
                if (source == 1U) {
                    kind = market::MarketEventKindV1::kShanghaiTick;
                } else if (source == 2U) {
                    kind =
                        market::MarketEventKindV1::kShenzhenSnapshot;
                } else if (source == 3U) {
                    kind = index % 2U == 0U
                        ? market::MarketEventKindV1::kShenzhenOrder
                        : market::MarketEventKindV1::
                              kShenzhenTransaction;
                }
                auto envelope = MakeEnvelope(
                    source,
                    static_cast<std::uint32_t>(101U + source),
                    sequence, instrument, kind);
                for (;;) {
                    const auto* event_before = &envelope->event();
                    const auto error =
                        runtime->TrySubmit(std::move(*envelope));
                    if (error ==
                        market::InstrumentHistorySubmitErrorV1::kNone) {
                        envelope.reset();
                        break;
                    }
                    if (error !=
                            market::InstrumentHistorySubmitErrorV1::
                                kQueueFull &&
                        error !=
                            market::InstrumentHistorySubmitErrorV1::
                                kInflightLimit) {
                        producer_failures.fetch_add(
                            1, std::memory_order_relaxed);
                        return;
                    }
                    if (!envelope.has_value() ||
                        envelope->dispatch_ticket() != 0U ||
                        &envelope->event() != event_before) {
                        producer_failures.fetch_add(
                            1, std::memory_order_relaxed);
                        return;
                    }
                    std::this_thread::yield();
                }
            }
        });
    }
    for (auto& producer : producers) {
        producer.join();
    }
    test->Expect(
        producer_failures.load(std::memory_order_relaxed) == 0,
        "four source producers submit concurrently without ownership loss");
    for (std::uint8_t source = 0U; source < 4U; ++source) {
        const auto barrier = runtime->CaptureBarrier(source);
        test->Expect(
            runtime->WaitForBarrier(barrier, 5s) ==
                    market::InstrumentHistoryBarrierWaitErrorV1::kNone,
            "each concurrent source reaches its independent barrier");
        const auto frontier = runtime->Frontier(source);
        test->Expect(
            frontier.submitted_ticket == kEventsPerSource &&
                frontier.acknowledged_ticket == kEventsPerSource &&
                frontier.acknowledged_source_sequence ==
                    1U + (kEventsPerSource - 1U) * 2U,
            "concurrent source frontier is exact and allows sequence gaps");
    }

    std::vector<market::InstrumentHistoryRecordHandleV1> records;
    test->Expect(
        runtime->Tail(
            1'000U, 3U, market::InstrumentHistoryLaneV1::kTick,
            1'000U, &records) ==
                market::InstrumentHistoryQueryErrorV1::kNone,
        "concurrent Shenzhen source has one instrument-local tick history");
    for (std::size_t index = 1U; index < records.size(); ++index) {
        test->Expect(
            records[index - 1U]->source_sequence() <
                records[index]->source_sequence(),
            "instrument subsequence remains strictly ordered");
    }
}

void TestFatalRevocationAndStop(TestContext* test) {
    auto runtime = Runtime(test);
    auto source0 = MakeEnvelope(
        0U, 101U, 10U, 65U,
        market::MarketEventKindV1::kShanghaiTick);
    SubmitRetry(test, runtime.get(), &source0);
    auto barrier0 = runtime->CaptureBarrier(0U);
    test->Expect(
        runtime->WaitForBarrier(barrier0, 2s) ==
            market::InstrumentHistoryBarrierWaitErrorV1::kNone,
        "pre-fatal history reaches its barrier");
    market::InstrumentHistoryRecordHandleV1 record;
    test->Expect(
        runtime->Latest(
            65U, 0U, market::InstrumentHistoryLaneV1::kTick,
            &record) == market::InstrumentHistoryQueryErrorV1::kNone,
        "pre-fatal source history is initially readable");

    runtime->MarkSourceFatal(0U);
    runtime->MarkSourceFatal(0U);
    test->Expect(
        runtime->Frontier(0U).fatal &&
            runtime->Latest(
                65U, 0U, market::InstrumentHistoryLaneV1::kTick,
                &record) ==
                market::InstrumentHistoryQueryErrorV1::kSourceFatal &&
            runtime->WaitForBarrier(barrier0, 1ms) ==
                market::InstrumentHistoryBarrierWaitErrorV1::kSourceFatal,
        "source fatal revokes barriers and every old history query");
    auto rejected = MakeEnvelope(
        0U, 101U, 12U, 65U,
        market::MarketEventKindV1::kShanghaiTick);
    const auto* rejected_event = &rejected->event();
    test->Expect(
        runtime->TrySubmit(std::move(*rejected)) ==
                market::InstrumentHistorySubmitErrorV1::kSourceFatal &&
            rejected.has_value() && rejected->dispatch_ticket() == 0U &&
            &rejected->event() == rejected_event,
        "fatal-source submission preserves the retry envelope");

    auto source1 = MakeEnvelope(
        1U, 102U, 7U, 65U,
        market::MarketEventKindV1::kShanghaiTick);
    SubmitRetry(test, runtime.get(), &source1);
    const auto barrier1 = runtime->CaptureBarrier(1U);
    test->Expect(
        runtime->WaitForBarrier(barrier1, 2s) ==
                market::InstrumentHistoryBarrierWaitErrorV1::kNone &&
            runtime->Latest(
                65U, 1U, market::InstrumentHistoryLaneV1::kTick,
                &record) ==
                market::InstrumentHistoryQueryErrorV1::kNone,
        "one source fatal does not stop another source");

    runtime->StopAndDrain();
    runtime->StopAndDrain();
    market::InstrumentHistoryRecordHandleV1 retained_after_runtime;
    test->Expect(
        runtime->Latest(
            65U, 1U, market::InstrumentHistoryLaneV1::kTick,
            &retained_after_runtime) ==
                market::InstrumentHistoryQueryErrorV1::kNone &&
            retained_after_runtime->source_sequence() == 7U,
        "clean stop preserves committed history queries");
    auto stopped = MakeEnvelope(
        1U, 102U, 8U, 65U,
        market::MarketEventKindV1::kShanghaiTick);
    const auto* stopped_event = &stopped->event();
    test->Expect(
        runtime->TrySubmit(std::move(*stopped)) ==
                market::InstrumentHistorySubmitErrorV1::kStopped &&
            stopped.has_value() && stopped->dispatch_ticket() == 0U &&
            &stopped->event() == stopped_event,
        "stopped runtime rejects without consuming the envelope");
    runtime.reset();
    test->Expect(
        retained_after_runtime &&
            retained_after_runtime->source_sequence() == 7U &&
            retained_after_runtime->instrument_id() == 65U,
        "record handle pins its immutable chunk beyond runtime lifetime");
}

}  // namespace

int main() {
    TestContext test;
    TestOwnedEnvelopeAndTopology(&test);
    TestQueryHardLimit(&test);
    TestRetainedPayloadLimit(&test);
    TestQueriesAndSourceLanes(&test);
    TestExactBarrierAndVisibility(&test);
    TestBackpressurePreservesOwnership(&test);
    TestFourConcurrentSources(&test);
    TestConcurrentQueriesDoNotHoldWriterLock(&test);
    TestFatalRevocationAndStop(&test);
    if (test.failures() != 0) {
        std::cerr << test.failures() << " instrument history checks failed\n";
        return 1;
    }
    std::cout << "instrument history checks passed\n";
    return 0;
}
