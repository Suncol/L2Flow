#include "l2flow/state/state_checkpoint_v1.h"

#include "l2flow/control/quality_flags_v1.h"

#include <array>
#include <atomic>
#include <barrier>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <span>
#include <string_view>
#include <thread>
#include <vector>

namespace canonical = l2flow::canonical;
namespace common = l2flow::common;
namespace control = l2flow::control;
namespace state = l2flow::state;

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
    for (std::size_t index = 0U; index < value.size(); ++index) {
        value[index] = static_cast<std::byte>(
            static_cast<std::uint8_t>(seed + index));
    }
    return value;
}

state::LatestStateConfigV1 Config() {
    state::LatestStateConfigV1 value;
    value.state_generation = 11U;
    value.state_writer_instance = Pattern<16U>(45U);
    value.shard_count = 16U;
    value.shard_id = 101U % value.shard_count;
    value.canonical_schema_sha256 =
        canonical::CanonicalSchemaDescriptorSha256V1();
    value.canonical_dtype_sha256 =
        canonical::CanonicalDtypeDescriptorSha256V1();
    value.registry_version = 8U;
    value.registry_sha256 = Pattern<32U>(70U);
    return value;
}

state::LatestStateCanonicalOriginV1 Origin(
    std::uint32_t source_stream_id,
    std::uint8_t seed,
    std::uint64_t clock_label = 10U) {
    state::LatestStateCanonicalOriginV1 value;
    value.capture_date = 20260722U;
    value.source_stream_id = source_stream_id;
    value.stream_day_id = Pattern<16U>(seed);
    value.source_writer_instance = Pattern<16U>(
        static_cast<std::uint8_t>(seed + 30U));
    value.source_generation = 2U;
    value.canonical_generation = 6U;
    value.clock_epoch.algorithm = 1U;
    value.clock_epoch.digest = Pattern<32U>(99U);
    value.clock_epoch.label = clock_label;
    const state::LatestStateConfigV1 config = Config();
    value.canonical_schema_sha256 = config.canonical_schema_sha256;
    value.canonical_dtype_sha256 = config.canonical_dtype_sha256;
    value.registry_version = config.registry_version;
    value.registry_sha256 = config.registry_sha256;
    return value;
}

canonical::CanonicalSnapshotRecordV1 Snapshot(
    std::uint64_t cursor,
    std::int64_t marker) {
    canonical::CanonicalSnapshotRecordV1 record;
    record.header.event_type = canonical::CanonicalEventTypeV1::kSnapshot;
    record.header.record_size =
        static_cast<std::uint32_t>(sizeof(record));
    record.header.source_stream_id = 1001U;
    record.header.connection_epoch = 1U;
    record.header.trade_date = 20260722U;
    record.header.shard_event_id = cursor;
    record.header.origin_ingress_sequence = cursor + 100U;
    record.header.origin_wal_end_pos = cursor * 4096U;
    record.header.vendor_sequence_id = cursor + 200U;
    record.header.recv_realtime_ns = 1'000'000 +
                                     static_cast<std::int64_t>(cursor);
    record.header.recv_monotonic_ns = 2'000'000 +
                                      static_cast<std::int64_t>(cursor);
    record.header.instrument_id = 101U;
    record.header.market = canonical::CanonicalMarketV1::kShanghai;
    record.header.origin_service_version = 101U;
    record.header.origin_message_id = 4U;
    record.header.origin_service_id = 4U;
    record.payload.last_price_p6 = marker;
    record.payload.actual_bid_depth = 1U;
    record.payload.actual_ask_depth = 1U;
    record.payload.bid_price_p6[0] = marker;
    record.payload.ask_price_p6[0] = marker + 1;
    record.payload.bid_quantity_native[0] = marker;
    record.payload.ask_quantity_native[0] = marker;
    record.payload.bid_order_count[0] = 1U;
    record.payload.ask_order_count[0] = 1U;
    record.payload.bid1_total_order_count = 1U;
    record.payload.ask1_total_order_count = 1U;
    record.payload.bid1_revealed_count = 1U;
    record.payload.ask1_revealed_count = 1U;
    record.payload.bid1_queue_quantity_native[0] = marker;
    record.payload.ask1_queue_quantity_native[0] = marker;
    record.payload.bid_price_validity = 1U;
    record.payload.ask_price_validity = 1U;
    record.payload.bid_quantity_validity = 1U;
    record.payload.ask_quantity_validity = 1U;
    record.payload.bid_order_count_validity = 1U;
    record.payload.ask_order_count_validity = 1U;
    record.payload.bid_queue_validity = 1U;
    record.payload.ask_queue_validity = 1U;
    record.payload.scalar_validity =
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::kLastPrice) |
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::kActualBidDepth) |
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::kActualAskDepth) |
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::
                kBid1TotalOrderCount) |
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::
                kAsk1TotalOrderCount) |
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::
                kBid1RevealedCount) |
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::
                kAsk1RevealedCount);
    return record;
}

state::LatestStateTickQualityUpdateV1 TickUpdate() {
    state::LatestStateTickQualityUpdateV1 tick;
    tick.instrument_id = 101U;
    tick.origin = Origin(1002U, 2U);
    tick.shard_event_id = 31U;
    tick.origin_ingress_sequence = 131U;
    tick.origin_wal_end_pos = 126'976U;
    tick.recv_monotonic_ns = 2'000'031;
    tick.quality_flags = control::QualityBit(
        control::QualityFlagV1::kExchangeSequenceGap);
    return tick;
}

common::Sha256Digest LogicalHash(
    TestContext* test,
    const state::LatestStateConfigV1& config,
    std::span<const state::LatestStateCheckpointSlotRefV1> slots) {
    common::Sha256Digest digest{};
    test->Expect(
        state::ComputeLatestStateLogicalHashV1(
            config, slots, &digest) ==
            state::LatestStateCheckpointErrorV1::kNone,
        "logical state hash computes over stable slots");
    return digest;
}

void TestCheckpointRoundTrip(TestContext* test) {
    const state::LatestStateConfigV1 config = Config();
    const state::LatestStateCanonicalOriginV1 snapshot_origin =
        Origin(1001U, 1U);
    const canonical::CanonicalSnapshotRecordV1 first = Snapshot(10U, 1000);
    const state::LatestStateTickQualityUpdateV1 tick = TickUpdate();
    state::LatestStateSlotV1 source;
    state::LatestStateSlotV1 empty;
    test->Expect(
        state::PublishLatestSnapshotV1(
            config, snapshot_origin, first, &source) ==
                state::LatestStateErrorV1::kNone &&
            state::PublishLatestTickQualityV1(config, tick, &source) ==
                state::LatestStateErrorV1::kNone,
        "checkpoint source contains snapshot and independent tick lineage");

    const std::array<state::LatestStateCheckpointSlotRefV1, 2U> refs{{
        {101U, &source},
        {117U, &empty},
    }};
    const common::Sha256Digest source_hash = LogicalHash(test, config, refs);
    std::vector<std::byte> wire;
    const state::LatestStateCheckpointOptionsV1 default_options;
    test->Expect(
        state::EncodeLatestStateCheckpointV1(
            config, refs, default_options, &wire) ==
                state::LatestStateCheckpointErrorV1::kNone &&
            !wire.empty(),
        "default checkpoint encodes a non-durable recovery image");
    state::LatestStateCheckpointV1 checkpoint;
    test->Expect(
        state::DecodeLatestStateCheckpointV1(wire, &checkpoint) ==
                state::LatestStateCheckpointErrorV1::kNone &&
            !checkpoint.writer_quiesced &&
            !checkpoint.durability_barrier_satisfied &&
            checkpoint.durability_barriers.empty() &&
            checkpoint.logical_state_sha256 == source_hash &&
            checkpoint.entries.size() == 2U &&
            !checkpoint.entries[1].initialized,
        "decode retains identities, full slots and explicit non-durable status");

    state::LatestStateSlotV1 restored;
    state::LatestStateSlotV1 restored_empty;
    const std::array<state::LatestStateCheckpointTargetV1, 2U> targets{{
        {101U, &restored},
        {117U, &restored_empty},
    }};
    test->Expect(
        state::RestoreLatestStateCheckpointV1(
            checkpoint, config, targets) ==
            state::LatestStateCheckpointErrorV1::kNone,
        "checkpoint restores only into an empty exact target set");
    const std::array<state::LatestStateCheckpointSlotRefV1, 2U>
        restored_refs{{
            {101U, &restored},
            {117U, &restored_empty},
        }};
    const common::Sha256Digest restored_hash =
        LogicalHash(test, config, restored_refs);
    state::LatestStateValueV1 source_value;
    state::LatestStateValueV1 restored_value;
    test->Expect(
        source_hash == restored_hash &&
            state::ReadLatestStateSlotV1(source, &source_value) ==
                state::LatestStateErrorV1::kNone &&
            state::ReadLatestStateSlotV1(restored, &restored_value) ==
                state::LatestStateErrorV1::kNone &&
            source_value.slot_sequence != restored_value.slot_sequence,
        "restore hash excludes the fresh seqlock sequence");
    test->Expect(
        state::RestoreLatestStateCheckpointV1(
            checkpoint, config, targets) ==
            state::LatestStateCheckpointErrorV1::kTargetNotEmpty,
        "restore refuses to overwrite a live target generation");

    std::vector<std::byte> corrupt = wire;
    corrupt.back() ^= std::byte{1U};
    test->Expect(
        state::DecodeLatestStateCheckpointV1(corrupt, &checkpoint) ==
            state::LatestStateCheckpointErrorV1::kCrcMismatch,
        "checkpoint CRC rejects changed bytes before restore");
}

void TestDurabilityBarrier(TestContext* test) {
    const state::LatestStateConfigV1 config = Config();
    const state::LatestStateCanonicalOriginV1 snapshot_origin =
        Origin(1001U, 1U);
    const canonical::CanonicalSnapshotRecordV1 first = Snapshot(10U, 1000);
    const state::LatestStateTickQualityUpdateV1 tick = TickUpdate();
    state::LatestStateSlotV1 slot;
    test->Expect(
        state::PublishLatestSnapshotV1(
            config, snapshot_origin, first, &slot) ==
                state::LatestStateErrorV1::kNone &&
            state::PublishLatestTickQualityV1(config, tick, &slot) ==
                state::LatestStateErrorV1::kNone,
        "durability fixture initializes");
    const std::array<state::LatestStateCheckpointSlotRefV1, 1U> refs{{
        {101U, &slot},
    }};

    state::LatestStateDurabilityBarrierV1 snapshot_barrier;
    snapshot_barrier.family = state::LatestStateInputFamilyV1::kSnapshot;
    snapshot_barrier.shard_id = config.shard_id;
    snapshot_barrier.origin = snapshot_origin;
    snapshot_barrier.max_consumed_shard_event_id =
        first.header.shard_event_id;
    snapshot_barrier.max_consumed_origin_ingress_sequence =
        first.header.origin_ingress_sequence;
    snapshot_barrier.max_consumed_origin_wal_end_pos =
        first.header.origin_wal_end_pos;
    snapshot_barrier.current_raw_durable_global_wal_pos =
        first.header.origin_wal_end_pos + 4096U;
    state::LatestStateDurabilityBarrierV1 tick_barrier;
    tick_barrier.family = state::LatestStateInputFamilyV1::kTickQuality;
    tick_barrier.shard_id = config.shard_id;
    tick_barrier.origin = tick.origin;
    tick_barrier.max_consumed_shard_event_id = tick.shard_event_id;
    tick_barrier.max_consumed_origin_ingress_sequence =
        tick.origin_ingress_sequence;
    tick_barrier.max_consumed_origin_wal_end_pos = tick.origin_wal_end_pos;
    tick_barrier.current_raw_durable_global_wal_pos =
        tick.origin_wal_end_pos + 4096U;

    std::vector<std::byte> wire;
    const std::array<state::LatestStateDurabilityBarrierV1, 1U>
        incomplete{{snapshot_barrier}};
    state::LatestStateCheckpointOptionsV1 options;
    options.writer_quiesced = true;
    options.durability_barrier_satisfied = true;
    options.durability_barriers = incomplete;
    test->Expect(
        state::EncodeLatestStateCheckpointV1(
            config, refs, options, &wire) ==
            state::LatestStateCheckpointErrorV1::kInvalidDurabilityProof,
        "one source cannot prove a two-input durability barrier");

    tick_barrier.current_raw_durable_global_wal_pos =
        tick.origin_wal_end_pos - 1U;
    const std::array<state::LatestStateDurabilityBarrierV1, 2U> lagging{{
        snapshot_barrier,
        tick_barrier,
    }};
    options.durability_barriers = lagging;
    test->Expect(
        state::EncodeLatestStateCheckpointV1(
            config, refs, options, &wire) ==
            state::LatestStateCheckpointErrorV1::kInvalidDurabilityProof,
        "consumed cursor beyond Raw durable cursor blocks durable status");

    tick_barrier.current_raw_durable_global_wal_pos =
        tick.origin_wal_end_pos + 4096U;
    const std::array<state::LatestStateDurabilityBarrierV1, 2U> complete{{
        tick_barrier,
        snapshot_barrier,
    }};
    options.durability_barriers = complete;
    options.writer_quiesced = false;
    test->Expect(
        state::EncodeLatestStateCheckpointV1(
            config, refs, options, &wire) ==
            state::LatestStateCheckpointErrorV1::kInvalidDurabilityProof,
        "durable status requires a caller-asserted writer-quiesced common cut");
    options.writer_quiesced = true;
    test->Expect(
        state::EncodeLatestStateCheckpointV1(
            config, refs, options, &wire) ==
            state::LatestStateCheckpointErrorV1::kNone,
        "all exact source/family Raw barriers allow caller assertion");
    state::LatestStateCheckpointV1 checkpoint;
    test->Expect(
        state::DecodeLatestStateCheckpointV1(wire, &checkpoint) ==
                state::LatestStateCheckpointErrorV1::kNone &&
            checkpoint.writer_quiesced &&
            checkpoint.durability_barrier_satisfied &&
            checkpoint.durability_barriers.size() == 2U,
        "durability assertion and exact barrier map survive decode");

    state::LatestStateCheckpointV1 contradictory = checkpoint;
    contradictory.writer_quiesced = false;
    state::LatestStateSlotV1 empty_target;
    const std::array<state::LatestStateCheckpointTargetV1, 1U> targets{{
        {101U, &empty_target},
    }};
    test->Expect(
        state::RestoreLatestStateCheckpointV1(
            contradictory, config, targets) ==
            state::LatestStateCheckpointErrorV1::kInvalidDurabilityProof,
        "restore rejects a durable object without its common-cut assertion");
}

void TestConcurrentDurableEncodeAlwaysDecodes(TestContext* test) {
    // This deliberately lies about writer quiescence to stress the codec's
    // image/requirements self-consistency under misuse.  It does not establish
    // a durable common cut.  Even so, every wire that Encode accepts must be
    // internally self-consistent and therefore accepted by Decode.
    constexpr std::size_t kIterations = 256U;
    const state::LatestStateConfigV1 config = Config();
    const state::LatestStateCanonicalOriginV1 origin = Origin(1001U, 1U);
    const canonical::CanonicalSnapshotRecordV1 before = Snapshot(10U, 1000);
    const canonical::CanonicalSnapshotRecordV1 after = Snapshot(11U, 2000);
    std::vector<state::LatestStateSlotV1> slots(kIterations);
    std::uint32_t fixture_failures = 0U;
    for (state::LatestStateSlotV1& slot : slots) {
        if (state::PublishLatestSnapshotV1(
                config, origin, before, &slot) !=
            state::LatestStateErrorV1::kNone) {
            ++fixture_failures;
        }
    }
    test->Expect(
        fixture_failures == 0U,
        "concurrent checkpoint fixtures initialize");
    if (fixture_failures != 0U) {
        return;
    }

    state::LatestStateDurabilityBarrierV1 barrier;
    barrier.family = state::LatestStateInputFamilyV1::kSnapshot;
    barrier.shard_id = config.shard_id;
    barrier.origin = origin;
    barrier.max_consumed_shard_event_id = after.header.shard_event_id;
    barrier.max_consumed_origin_ingress_sequence =
        after.header.origin_ingress_sequence;
    barrier.max_consumed_origin_wal_end_pos =
        after.header.origin_wal_end_pos;
    barrier.current_raw_durable_global_wal_pos =
        after.header.origin_wal_end_pos + 4096U;
    const std::array<state::LatestStateDurabilityBarrierV1, 1U> barriers{{
        barrier,
    }};
    state::LatestStateCheckpointOptionsV1 options;
    options.writer_quiesced = true;
    options.durability_barrier_satisfied = true;
    options.durability_barriers = barriers;

    std::barrier rendezvous(2);
    std::atomic<std::size_t> encode_started{0U};
    std::atomic<std::size_t> publication_done{0U};
    std::atomic<std::uint32_t> writer_failures{0U};
    std::atomic<std::uint32_t> successful_encodes{0U};
    std::atomic<std::uint32_t> decode_failures{0U};

    std::thread writer([&]() {
        for (std::size_t index = 0U; index < kIterations; ++index) {
            rendezvous.arrive_and_wait();
            if ((index & 1U) != 0U) {
                while (encode_started.load(std::memory_order_acquire) <
                       index + 1U) {
                    std::this_thread::yield();
                }
                const std::size_t delays = index % 8U;
                for (std::size_t delay = 0U; delay < delays; ++delay) {
                    std::this_thread::yield();
                }
            }
            if (state::PublishLatestSnapshotV1(
                    config, origin, after, &slots[index]) !=
                state::LatestStateErrorV1::kNone) {
                writer_failures.fetch_add(1U, std::memory_order_relaxed);
            }
            publication_done.store(index + 1U, std::memory_order_release);
            rendezvous.arrive_and_wait();
        }
    });

    std::thread encoder([&]() {
        for (std::size_t index = 0U; index < kIterations; ++index) {
            rendezvous.arrive_and_wait();
            if ((index & 1U) == 0U) {
                while (publication_done.load(std::memory_order_acquire) <
                       index + 1U) {
                    std::this_thread::yield();
                }
            } else {
                encode_started.store(index + 1U, std::memory_order_release);
            }
            const std::array<state::LatestStateCheckpointSlotRefV1, 1U>
                refs{{{101U, &slots[index]}}};
            std::vector<std::byte> wire;
            if (state::EncodeLatestStateCheckpointV1(
                    config, refs, options, &wire) ==
                state::LatestStateCheckpointErrorV1::kNone) {
                successful_encodes.fetch_add(
                    1U, std::memory_order_relaxed);
                state::LatestStateCheckpointV1 checkpoint;
                if (state::DecodeLatestStateCheckpointV1(
                        wire, &checkpoint) !=
                    state::LatestStateCheckpointErrorV1::kNone) {
                    decode_failures.fetch_add(
                        1U, std::memory_order_relaxed);
                }
            }
            rendezvous.arrive_and_wait();
        }
    });

    writer.join();
    encoder.join();
    test->Expect(
        writer_failures.load(std::memory_order_relaxed) == 0U,
        "concurrent checkpoint writer publishes every transition");
    test->Expect(
        successful_encodes.load(std::memory_order_relaxed) != 0U,
        "concurrent checkpoint stress produces accepted durable wires");
    test->Expect(
        decode_failures.load(std::memory_order_relaxed) == 0U,
        "every accepted durable wire decodes against its captured image");
}

void TestCheckpointTailEqualsFullReplay(TestContext* test) {
    const state::LatestStateConfigV1 config = Config();
    const state::LatestStateCanonicalOriginV1 origin = Origin(1001U, 1U);
    const canonical::CanonicalSnapshotRecordV1 first = Snapshot(10U, 1000);
    const canonical::CanonicalSnapshotRecordV1 second = Snapshot(11U, 2000);
    const state::LatestStateTickQualityUpdateV1 tick = TickUpdate();
    state::LatestStateSlotV1 checkpoint_source;
    test->Expect(
        state::PublishLatestSnapshotV1(
            config, origin, first, &checkpoint_source) ==
                state::LatestStateErrorV1::kNone &&
            state::PublishLatestTickQualityV1(
                config, tick, &checkpoint_source) ==
                state::LatestStateErrorV1::kNone,
        "checkpoint-tail source initializes");
    const std::array<state::LatestStateCheckpointSlotRefV1, 1U> refs{{
        {101U, &checkpoint_source},
    }};
    std::vector<std::byte> wire;
    const state::LatestStateCheckpointOptionsV1 options;
    state::LatestStateCheckpointV1 checkpoint;
    test->Expect(
        state::EncodeLatestStateCheckpointV1(
            config, refs, options, &wire) ==
                state::LatestStateCheckpointErrorV1::kNone &&
            state::DecodeLatestStateCheckpointV1(wire, &checkpoint) ==
                state::LatestStateCheckpointErrorV1::kNone,
        "checkpoint-tail image round trips");

    state::LatestStateSlotV1 restored_then_tail;
    const std::array<state::LatestStateCheckpointTargetV1, 1U> target{{
        {101U, &restored_then_tail},
    }};
    state::LatestStateSlotV1 full_replay;
    test->Expect(
        state::RestoreLatestStateCheckpointV1(
            checkpoint, config, target) ==
                state::LatestStateCheckpointErrorV1::kNone &&
            state::PublishLatestSnapshotV1(
                config, origin, second, &restored_then_tail) ==
                state::LatestStateErrorV1::kNone &&
            state::PublishLatestSnapshotV1(
                config, origin, first, &full_replay) ==
                state::LatestStateErrorV1::kNone &&
            state::PublishLatestTickQualityV1(
                config, tick, &full_replay) ==
                state::LatestStateErrorV1::kNone &&
            state::PublishLatestSnapshotV1(
                config, origin, second, &full_replay) ==
                state::LatestStateErrorV1::kNone,
        "checkpoint+tail and full replay reach final state");
    const std::array<state::LatestStateCheckpointSlotRefV1, 1U> tail_ref{{
        {101U, &restored_then_tail},
    }};
    const std::array<state::LatestStateCheckpointSlotRefV1, 1U> full_ref{{
        {101U, &full_replay},
    }};
    test->Expect(
        LogicalHash(test, config, tail_ref) ==
            LogicalHash(test, config, full_ref),
        "checkpoint+tail logical hash equals full replay hash");
}

void TestClockLabelExcludedFromLogicalHash(TestContext* test) {
    const state::LatestStateConfigV1 config = Config();
    state::LatestStateConfigV1 replacement_config = config;
    replacement_config.state_generation += 1U;
    replacement_config.state_writer_instance = Pattern<16U>(11U);
    state::LatestStateSlotV1 first;
    state::LatestStateSlotV1 second;
    state::LatestStateSlotV1 replacement;
    const canonical::CanonicalSnapshotRecordV1 snapshot = Snapshot(10U, 1000);
    test->Expect(
        state::PublishLatestSnapshotV1(
            config, Origin(1001U, 1U, 10U), snapshot, &first) ==
                state::LatestStateErrorV1::kNone &&
            state::PublishLatestSnapshotV1(
                config, Origin(1001U, 1U, 999U), snapshot, &second) ==
                state::LatestStateErrorV1::kNone &&
            state::PublishLatestSnapshotV1(
                replacement_config, Origin(1001U, 1U, 10U), snapshot,
                &replacement) ==
                state::LatestStateErrorV1::kNone,
        "clock-label fixtures share algorithm and full digest");
    const std::array<state::LatestStateCheckpointSlotRefV1, 1U> first_ref{{
        {101U, &first},
    }};
    const std::array<state::LatestStateCheckpointSlotRefV1, 1U> second_ref{{
        {101U, &second},
    }};
    const std::array<state::LatestStateCheckpointSlotRefV1, 1U>
        replacement_ref{{
            {101U, &replacement},
        }};
    test->Expect(
        LogicalHash(test, config, first_ref) ==
                LogicalHash(test, config, second_ref) &&
            LogicalHash(test, config, first_ref) ==
                LogicalHash(test, replacement_config, replacement_ref),
        "display labels and state-writer generation do not change logical content hash");
}

}  // namespace

int main() {
    TestContext test;
    TestCheckpointRoundTrip(&test);
    TestDurabilityBarrier(&test);
    TestConcurrentDurableEncodeAlwaysDecodes(&test);
    TestCheckpointTailEqualsFullReplay(&test);
    TestClockLabelExcludedFromLogicalHash(&test);
    if (test.failures != 0) {
        std::cerr << test.failures
                  << " Phase 6 checkpoint test(s) failed\n";
        return 1;
    }
    std::cout << "Phase 6 checkpoint tests passed\n";
    return 0;
}
