#include "l2flow/state/latest_state_v1.h"

#include "l2flow/control/quality_flags_v1.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <span>
#include <string_view>
#include <thread>
#include <vector>

namespace canonical = l2flow::canonical;
namespace common = l2flow::common;
namespace control = l2flow::control;
namespace state = l2flow::state;

namespace {

static_assert(sizeof(state::LatestStateSlotV1) == 4096U);
static_assert(alignof(state::LatestStateSlotV1) == 64U);

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

canonical::ClockEpochIdentityV1 Clock(
    std::uint8_t seed,
    std::uint64_t label = 77U) {
    canonical::ClockEpochIdentityV1 value;
    value.algorithm = 1U;
    value.digest = Pattern<32U>(seed);
    value.label = label;
    return value;
}

state::LatestStateConfigV1 Config() {
    state::LatestStateConfigV1 value;
    value.state_generation = 9U;
    value.state_writer_instance = Pattern<16U>(55U);
    value.shard_count = 16U;
    value.shard_id = 101U % value.shard_count;
    value.canonical_schema_sha256 =
        canonical::CanonicalSchemaDescriptorSha256V1();
    value.canonical_dtype_sha256 =
        canonical::CanonicalDtypeDescriptorSha256V1();
    value.registry_version = 3U;
    value.registry_sha256 = Pattern<32U>(90U);
    return value;
}

state::LatestStateCanonicalOriginV1 Origin(
    std::uint32_t source_stream_id,
    std::uint8_t seed,
    std::uint64_t label = 77U) {
    state::LatestStateCanonicalOriginV1 value;
    value.capture_date = 20260722U;
    value.source_stream_id = source_stream_id;
    value.stream_day_id = Pattern<16U>(seed);
    value.source_writer_instance = Pattern<16U>(
        static_cast<std::uint8_t>(seed + 20U));
    value.source_generation = 4U;
    value.canonical_generation = 5U;
    value.clock_epoch = Clock(33U, label);
    const state::LatestStateConfigV1 config = Config();
    value.canonical_schema_sha256 = config.canonical_schema_sha256;
    value.canonical_dtype_sha256 = config.canonical_dtype_sha256;
    value.registry_version = config.registry_version;
    value.registry_sha256 = config.registry_sha256;
    return value;
}

canonical::CanonicalSnapshotRecordV1 Snapshot(
    std::uint64_t cursor,
    std::uint32_t depth,
    std::uint32_t queue,
    std::int64_t marker,
    bool last_valid = true) {
    canonical::CanonicalSnapshotRecordV1 record;
    record.header.event_type = canonical::CanonicalEventTypeV1::kSnapshot;
    record.header.record_size =
        static_cast<std::uint32_t>(sizeof(record));
    record.header.source_stream_id = 1001U;
    record.header.connection_epoch = 2U;
    record.header.trade_date = 20260722U;
    record.header.shard_event_id = cursor;
    record.header.origin_ingress_sequence = cursor + 100U;
    record.header.origin_wal_end_pos = cursor * 4096U;
    record.header.vendor_sequence_id = cursor + 200U;
    record.header.exchange_sequence = 0U;
    record.header.exchange_time_ns = 1'000'000 +
                                     static_cast<std::int64_t>(cursor);
    record.header.recv_realtime_ns = 2'000'000 +
                                     static_cast<std::int64_t>(cursor);
    record.header.recv_monotonic_ns = 3'000'000 +
                                      static_cast<std::int64_t>(cursor);
    record.header.instrument_id = 101U;
    record.header.channel = 0U;
    record.header.market = canonical::CanonicalMarketV1::kShanghai;
    record.header.origin_service_version = 101U;
    record.header.origin_message_id = 4U;
    record.header.origin_service_id = 4U;

    record.payload.actual_bid_depth = depth;
    record.payload.actual_ask_depth = depth;
    record.payload.bid1_revealed_count = queue;
    record.payload.ask1_revealed_count = queue;
    record.payload.bid1_total_order_count = queue;
    record.payload.ask1_total_order_count = queue;
    record.payload.phase = canonical::CanonicalTradingPhaseV1::kContinuous;
    record.payload.scalar_validity =
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::kActualBidDepth) |
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::kActualAskDepth) |
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::
                kBid1RevealedCount) |
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::
                kAsk1RevealedCount) |
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::
                kBid1TotalOrderCount) |
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::
                kAsk1TotalOrderCount) |
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::kExchangeTime) |
        canonical::CanonicalSnapshotValidityBitV1(
            canonical::CanonicalSnapshotScalarValidityV1::
                kNormalizedPhase);
    if (last_valid) {
        record.payload.last_price_p6 = marker;
        record.payload.scalar_validity |=
            canonical::CanonicalSnapshotValidityBitV1(
                canonical::CanonicalSnapshotScalarValidityV1::kLastPrice);
    }
    const std::uint16_t level_mask = depth >= 10U
                                         ? std::uint16_t{0x03ffU}
                                         : static_cast<std::uint16_t>(
                                               (1U << depth) - 1U);
    record.payload.bid_price_validity = level_mask;
    record.payload.bid_quantity_validity = level_mask;
    record.payload.bid_order_count_validity = level_mask;
    record.payload.ask_price_validity = level_mask;
    record.payload.ask_quantity_validity = level_mask;
    record.payload.ask_order_count_validity = level_mask;
    for (std::uint32_t index = 0U; index < depth && index < 10U; ++index) {
        const std::size_t position = static_cast<std::size_t>(index);
        record.payload.bid_price_p6[position] = marker -
                                                static_cast<std::int64_t>(index);
        record.payload.ask_price_p6[position] = marker + 100 +
                                                static_cast<std::int64_t>(index);
        record.payload.bid_quantity_native[position] = marker;
        record.payload.ask_quantity_native[position] = marker;
        record.payload.bid_order_count[position] = index + 1U;
        record.payload.ask_order_count[position] = index + 1U;
    }
    if (depth != 0U) {
        record.payload.bid_order_count[0] = queue;
        record.payload.ask_order_count[0] = queue;
    }
    record.payload.bid_queue_validity =
        queue == 0U ? 0U : (std::uint64_t{1U} << queue) - 1U;
    record.payload.ask_queue_validity = record.payload.bid_queue_validity;
    for (std::uint32_t index = 0U; index < queue; ++index) {
        const std::size_t position = static_cast<std::size_t>(index);
        record.payload.bid1_queue_quantity_native[position] = marker;
        record.payload.ask1_queue_quantity_native[position] = marker;
    }
    return record;
}

state::LatestStateReadOptionsV1 ReadOptions(
    std::int64_t now,
    std::int64_t threshold) {
    state::LatestStateReadOptionsV1 options;
    options.now_monotonic_ns = now;
    options.now_clock_epoch = Clock(33U);
    options.stale_policy.max_age_ns_by_phase.fill(threshold);
    return options;
}

void TestUpdatesAndQueries(TestContext* test) {
    test->Expect(
        state::LatestStateHostSupportedV1() &&
            l2flow_latest_state_atomic_u64_lock_free_v1(),
        "Phase 6 requires lock-free shared u64 atomics");
    test->Expect(
        !state::LatestStateSchemaDescriptorV1().empty() &&
            state::LatestStateSchemaDescriptorSha256V1() !=
                common::Sha256Digest{},
        "latest-state opaque layout has a stable descriptor identity");

    const state::LatestStateConfigV1 config = Config();
    const state::LatestStateCanonicalOriginV1 snapshot_origin =
        Origin(1001U, 1U);
    state::LatestStateSlotV1 slot;
    const canonical::CanonicalSnapshotRecordV1 first =
        Snapshot(10U, 10U, 50U, 1'000U);
    state::LatestStateUpdateDispositionV1 disposition =
        state::LatestStateUpdateDispositionV1::kIdempotent;
    const state::LatestStateErrorV1 first_error =
        state::PublishLatestSnapshotV1(
            config, snapshot_origin, first, &slot, &disposition);
    if (first_error != state::LatestStateErrorV1::kNone) {
        std::cerr << "first publish error: "
                  << state::LatestStateErrorNameV1(first_error) << '\n';
        std::cerr << "snapshot validation: "
                  << static_cast<unsigned>(
                         canonical::ValidateCanonicalSnapshotRecordV1(first))
                  << '\n';
        std::cerr << "depth " << first.payload.actual_bid_depth << ' '
                  << first.payload.actual_ask_depth << " masks "
                  << first.payload.bid_order_count_validity << ' '
                  << first.payload.ask_order_count_validity << ' '
                  << first.payload.bid_price_validity << ' '
                  << first.payload.ask_price_validity << ' '
                  << first.payload.bid_quantity_validity << ' '
                  << first.payload.ask_quantity_validity << " scalar "
                  << first.payload.scalar_validity << " flags "
                  << first.payload.snapshot_flags << '\n';
    }
    test->Expect(
        first_error ==
                state::LatestStateErrorV1::kNone &&
            disposition == state::LatestStateUpdateDispositionV1::kPublished,
        "first complete snapshot initializes a slot");

    state::LatestStateValueV1 value;
    test->Expect(
        state::ReadLatestStateSlotV1(slot, &value) ==
                state::LatestStateErrorV1::kNone &&
            value.origin.stream_day_id == snapshot_origin.stream_day_id &&
            value.origin.source_writer_instance ==
                snapshot_origin.source_writer_instance &&
            value.origin.source_generation ==
                snapshot_origin.source_generation &&
            value.origin.canonical_generation ==
                snapshot_origin.canonical_generation &&
            value.config.registry_sha256 == config.registry_sha256,
        "slot retains complete snapshot lineage and registry identity");
    const std::uint64_t first_sequence = value.slot_sequence;
    test->Expect(
        state::PublishLatestSnapshotV1(
            config, snapshot_origin, first, &slot, &disposition) ==
                state::LatestStateErrorV1::kNone &&
            disposition == state::LatestStateUpdateDispositionV1::kIdempotent &&
            state::ReadLatestStateSlotV1(slot, &value) ==
                state::LatestStateErrorV1::kNone &&
            value.slot_sequence == first_sequence,
        "exact duplicate is idempotent and does not advance seqlock");

    canonical::CanonicalSnapshotRecordV1 conflict = first;
    conflict.payload.last_price_p6 += 1;
    test->Expect(
        state::PublishLatestSnapshotV1(
            config, snapshot_origin, conflict, &slot) ==
            state::LatestStateErrorV1::kConflictingDuplicate,
        "same cursor with different complete content is rejected");

    const canonical::CanonicalSnapshotRecordV1 shorter =
        Snapshot(11U, 3U, 2U, 2'000U, false);
    test->Expect(
        state::PublishLatestSnapshotV1(
            config, snapshot_origin, shorter, &slot) ==
            state::LatestStateErrorV1::kNone,
        "newer snapshot replaces the complete payload");
    test->Expect(
        state::ReadLatestStateSlotV1(slot, &value) ==
                state::LatestStateErrorV1::kNone &&
            value.snapshot.payload.bid_price_p6[2] != 0 &&
            value.snapshot.payload.bid_price_p6[3] == 0 &&
            value.snapshot.payload.bid1_queue_quantity_native[1] != 0 &&
            value.snapshot.payload.bid1_queue_quantity_native[2] == 0 &&
            value.snapshot.payload.last_price_p6 == 0 &&
            (value.snapshot.payload.scalar_validity &
             canonical::CanonicalSnapshotValidityBitV1(
                 canonical::CanonicalSnapshotScalarValidityV1::kLastPrice)) ==
                0U,
        "depth, queue and null tails cannot retain old values");
    test->Expect(
        state::PublishLatestSnapshotV1(
            config, snapshot_origin, first, &slot) ==
            state::LatestStateErrorV1::kStaleCursor,
        "an old cursor cannot overwrite current state");
    state::LatestStateConfigV1 stale_writer = config;
    stale_writer.state_writer_instance = Pattern<16U>(7U);
    test->Expect(
        state::PublishLatestSnapshotV1(
            stale_writer, snapshot_origin,
            Snapshot(12U, 3U, 2U, 3'000U), &slot) ==
            state::LatestStateErrorV1::kIdentityMismatch,
        "a stale state-writer owner cannot mutate another state generation");

    state::LatestStateTickQualityUpdateV1 tick;
    tick.instrument_id = 101U;
    tick.origin = Origin(1002U, 2U);
    tick.shard_event_id = 20U;
    tick.origin_ingress_sequence = 120U;
    tick.origin_wal_end_pos = 80'000U;
    tick.recv_monotonic_ns = 3'000'100;
    tick.quality_flags = control::QualityBit(
        control::QualityFlagV1::kExchangeSequenceGap);
    const canonical::CanonicalSnapshotRecordV1 snapshot_before_tick =
        value.snapshot;
    test->Expect(
        state::PublishLatestTickQualityV1(config, tick, &slot) ==
            state::LatestStateErrorV1::kNone,
        "tick quality publishes with its own lineage and cursor");
    test->Expect(
        state::ReadLatestStateSlotV1(slot, &value) ==
                state::LatestStateErrorV1::kNone &&
            std::memcmp(
                &snapshot_before_tick, &value.snapshot,
                sizeof(value.snapshot)) == 0 &&
            value.snapshot.header.quality_flags == 0U &&
            value.tick_quality_initialized &&
            value.tick_origin.source_stream_id == 1002U &&
            value.tick_origin_wal_end_pos == tick.origin_wal_end_pos &&
            value.tick_quality_flags == tick.quality_flags,
        "tick gap never changes snapshot payload or snapshot quality");
    state::LatestStateTickQualityUpdateV1 old_tick = tick;
    old_tick.shard_event_id -= 1U;
    old_tick.origin_ingress_sequence -= 1U;
    old_tick.origin_wal_end_pos -= 1U;
    test->Expect(
        state::PublishLatestTickQualityV1(config, old_tick, &slot) ==
            state::LatestStateErrorV1::kStaleCursor,
        "old tick-quality cursor cannot overwrite new tick quality");
    state::LatestStateTickQualityUpdateV1 new_generation = tick;
    new_generation.origin.canonical_generation += 1U;
    new_generation.shard_event_id += 1U;
    new_generation.origin_ingress_sequence += 1U;
    new_generation.origin_wal_end_pos += 1U;
    test->Expect(
        state::PublishLatestTickQualityV1(
            config, new_generation, &slot) ==
            state::LatestStateErrorV1::kIdentityMismatch,
        "local V1 rejects an in-place tick generation change");

    const state::LatestStateReadOptionsV1 fresh_options = ReadOptions(
        shorter.header.recv_monotonic_ns + 99, 100);
    state::LatestStateReadResultV1 latest;
    test->Expect(
        state::ReadLatestStateV1(slot, fresh_options, &latest) ==
                state::LatestStateErrorV1::kNone &&
            !latest.snapshot_stale && latest.age_ns == 99,
        "caller-supplied phase threshold keeps a fresh snapshot fresh");
    const state::LatestStateReadOptionsV1 stale_options = ReadOptions(
        shorter.header.recv_monotonic_ns + 101, 100);
    test->Expect(
        state::ReadLatestStateV1(slot, stale_options, &latest) ==
                state::LatestStateErrorV1::kNone &&
            latest.snapshot_stale &&
            (latest.effective_snapshot_quality_flags &
             control::QualityBit(
                 control::QualityFlagV1::kSnapshotStale)) != 0U &&
            latest.value.snapshot.header.quality_flags == 0U,
        "stale is derived without mutating persisted snapshot quality");
    state::LatestStateReadOptionsV1 wrong_clock = stale_options;
    wrong_clock.now_clock_epoch.digest[0] ^= std::byte{1U};
    test->Expect(
        state::ReadLatestStateV1(slot, wrong_clock, &latest) ==
            state::LatestStateErrorV1::kClockEpochMismatch,
        "monotonic age is never computed across clock epochs");

    const std::array<state::LatestStateBatchRequestV1, 2U> requests{{
        {101U, &slot},
        {202U, nullptr},
    }};
    std::array<state::LatestStateBatchResultV1, 2U> batch{};
    test->Expect(
        state::BatchReadLatestStateV1(requests, stale_options, batch) ==
                state::LatestStateErrorV1::kNone &&
            batch[0].error == state::LatestStateErrorV1::kNone &&
            batch[1].error == state::LatestStateErrorV1::kNotFound,
        "batch API returns per-instrument status");

    const std::array<state::LatestStateQueryEntryV1, 1U> entries{{
        {101U, &slot},
    }};
    std::unique_ptr<state::LatestStateLocalQueryV1> query;
    test->Expect(
        state::LatestStateLocalQueryV1::Create(entries, &query) ==
                state::LatestStateErrorV1::kNone &&
            query->GetLatest(101U, stale_options, &latest) ==
                state::LatestStateErrorV1::kNone &&
            latest.value.snapshot.header.instrument_id == 101U,
        "local query directory resolves an instrument without owning SHM");
}

void TestConcurrentNoTornState(TestContext* test) {
    const state::LatestStateConfigV1 config = Config();
    const state::LatestStateCanonicalOriginV1 origin = Origin(1001U, 1U);
    state::LatestStateSlotV1 slot;
    test->Expect(
        state::PublishLatestSnapshotV1(
            config, origin, Snapshot(100U, 3U, 2U, 100U), &slot) ==
            state::LatestStateErrorV1::kNone,
        "concurrency fixture initializes");

    std::atomic<bool> start{false};
    std::atomic<bool> done{false};
    std::atomic<int> failures{0};
    std::thread writer([&]() {
        while (!start.load(std::memory_order_acquire)) {
        }
        for (std::uint64_t cursor = 101U; cursor < 2101U; ++cursor) {
            const std::int64_t marker = static_cast<std::int64_t>(cursor);
            if (state::PublishLatestSnapshotV1(
                    config, origin,
                    Snapshot(cursor, 3U, 2U, marker), &slot) !=
                state::LatestStateErrorV1::kNone) {
                failures.fetch_add(1, std::memory_order_relaxed);
                break;
            }
        }
        done.store(true, std::memory_order_release);
    });
    std::array<std::thread, 4U> readers;
    for (std::thread& reader : readers) {
        reader = std::thread([&]() {
            while (!start.load(std::memory_order_acquire)) {
            }
            do {
                state::LatestStateValueV1 value;
                const state::LatestStateErrorV1 error =
                    state::ReadLatestStateSlotV1(slot, &value);
                if (error == state::LatestStateErrorV1::kReadBusy) {
                    continue;
                }
                if (error != state::LatestStateErrorV1::kNone ||
                    value.snapshot.payload.bid_quantity_native[0] !=
                        value.snapshot.payload.ask_quantity_native[0] ||
                    value.snapshot.payload.bid_quantity_native[0] !=
                        value.snapshot.payload.bid1_queue_quantity_native[0] ||
                    (value.slot_sequence & 1U) != 0U) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                    return;
                }
            } while (!done.load(std::memory_order_acquire));
        });
    }
    start.store(true, std::memory_order_release);
    writer.join();
    for (std::thread& reader : readers) {
        reader.join();
    }
    test->Expect(
        failures.load(std::memory_order_relaxed) == 0,
        "atomic-word seqlock readers never observe torn state");
}

}  // namespace

int main() {
    TestContext test;
    TestUpdatesAndQueries(&test);
    TestConcurrentNoTornState(&test);
    if (test.failures != 0) {
        std::cerr << test.failures << " Phase 6 latest-state test(s) failed\n";
        return 1;
    }
    std::cout << "Phase 6 latest-state tests passed\n";
    return 0;
}
