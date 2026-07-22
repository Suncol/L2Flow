#include "l2flow/market/market_session.h"
#include "l2flow/market/market_types_v1.h"
#include "l2flow/market/session_store.h"

#include "mdl_shl2_msg.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace market = l2flow::market;
namespace sh = datayes::mdl::mdl_shl2_msg;

namespace {

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

l2flow::common::Identity128 Identity(std::uint8_t seed) {
    l2flow::common::Identity128 result{};
    result[0] = static_cast<std::byte>(seed);
    return result;
}

market::SessionStoreConfigV1 StoreConfig(
    market::SessionRetentionModeV1 retention,
    std::uint64_t window_ns,
    std::uint64_t max_records,
    std::uint64_t max_payload_bytes,
    std::size_t chunk_capacity = 16U) {
    market::SessionStoreConfigV1 config;
    for (std::size_t index = 0U;
         index < config.streams.size();
         ++index) {
        config.streams[index] = market::SessionStreamKeyV1{
            20260722U,
            static_cast<std::uint32_t>(1001U + index),
            Identity(static_cast<std::uint8_t>(index + 1U))};
    }
    config.retention_mode = retention;
    config.recv_monotonic_window_ns = window_ns;
    config.max_records = max_records;
    config.max_payload_bytes = max_payload_bytes;
    config.chunk_record_capacity = chunk_capacity;
    return config;
}

struct SmallOwnedEvent final {
    std::uint64_t value = 0U;
};

using SmallStore = market::SessionStoreV1<SmallOwnedEvent>;
using SmallSnapshot =
    market::SessionStoreSnapshotV1<SmallOwnedEvent>;

market::SessionOwnedRecordV1<SmallOwnedEvent> SmallRecord(
    const market::SessionStreamKeyV1& stream,
    std::uint64_t sequence,
    std::uint64_t recv_monotonic_ns,
    std::uint64_t value,
    std::uint64_t payload_bytes = sizeof(SmallOwnedEvent)) {
    return market::SessionOwnedRecordV1<SmallOwnedEvent>{
        stream,
        sequence,
        recv_monotonic_ns,
        payload_bytes,
        SmallOwnedEvent{value}};
}

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
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        bytes[offset + index] = static_cast<std::byte>(
            (value >> shift) & 0xffU);
    }
}

void StoreU64(
    std::span<std::byte> bytes,
    std::size_t offset,
    std::uint64_t value) {
    for (std::size_t index = 0U; index < 8U; ++index) {
        const unsigned int shift =
            static_cast<unsigned int>(index * 8U);
        bytes[offset + index] = static_cast<std::byte>(
            (value >> shift) & 0xffU);
    }
}

void AppendText(
    std::vector<std::byte>* bytes,
    std::size_t descriptor,
    std::string_view value) {
    const std::size_t start = bytes->size();
    bytes->reserve(start + value.size());
    for (const char character : value) {
        bytes->push_back(static_cast<std::byte>(
            static_cast<unsigned char>(character)));
    }
    StoreU16(*bytes, descriptor,
             static_cast<std::uint16_t>(value.size()));
    StoreU32(
        *bytes,
        descriptor + 2U,
        static_cast<std::uint32_t>(start - descriptor));
}

std::vector<std::byte> ShanghaiAddWire() {
    // Independent frozen SH 4.24 offsets.  The test intentionally does not
    // construct or dereference the vendor packed type.
    std::vector<std::byte> bytes(70U, std::byte{0U});
    StoreU64(bytes, 0U, 101U);       // BizIndex
    StoreU32(bytes, 8U, 7U);         // Channel
    StoreU32(bytes, 18U, 93'000'123U);
    StoreU64(bytes, 28U, 1'101U);    // BuyOrderNO
    StoreU64(bytes, 36U, 0U);        // SellOrderNO
    StoreU32(bytes, 44U, 12'345U);   // Price p3
    StoreU64(bytes, 48U, 99U);       // Qty
    StoreU64(bytes, 56U, 7'000U);    // matched quantity p3
    AppendText(&bytes, 12U, "600000");
    AppendText(&bytes, 22U, "A");
    AppendText(&bytes, 64U, "B");
    return bytes;
}

market::DecodedMarketEventV1 OwnedShanghaiTick(
    std::uint32_t source_stream_id,
    std::uint64_t sequence,
    std::uint64_t recv_monotonic_ns,
    std::string security_id,
    std::string raw_type) {
    market::ShanghaiTickV1 tick;
    tick.common.kind = market::MarketEventKindV1::kShanghaiTick;
    tick.common.market = market::MarketV1::kShanghai;
    tick.common.origin.source_stream_id = source_stream_id;
    tick.common.origin.trade_date = 20260722U;
    tick.common.origin.source_sequence = sequence;
    tick.common.origin.recv_monotonic_ns =
        static_cast<std::int64_t>(recv_monotonic_ns);
    // This is the decoder publication invariant: no callback/Raw span is
    // allowed to survive in an owned decoded event.
    tick.common.origin.body = {};
    tick.common.security_id = std::move(security_id);
    tick.common.security_id_source = "SecurityID";
    tick.raw_type = std::move(raw_type);
    tick.raw_tick_flag = "B";
    tick.business_index = static_cast<std::int64_t>(sequence);
    return market::DecodedMarketEventV1{std::move(tick)};
}

using DecodedStore =
    market::SessionStoreV1<market::DecodedMarketEventV1>;
using DecodedSnapshot =
    market::SessionStoreSnapshotV1<market::DecodedMarketEventV1>;

market::SessionOwnedRecordV1<market::DecodedMarketEventV1>
DecodedRecord(
    const market::SessionStreamKeyV1& stream,
    std::uint64_t sequence,
    std::uint64_t recv_monotonic_ns,
    market::DecodedMarketEventV1 event) {
    const std::size_t estimated =
        market::EstimateOwnedMarketEventBytesV1(event);
    return {
        stream,
        sequence,
        recv_monotonic_ns,
        static_cast<std::uint64_t>(estimated),
        std::move(event)};
}

void TestFullSessionExceedsOldProbeLimit(TestContext* test) {
    constexpr std::uint64_t kRecords = 100'001U;
    market::SessionStoreConfigV1 config = StoreConfig(
        market::SessionRetentionModeV1::kFullSession,
        0U,
        kRecords + 1U,
        (kRecords + 1U) * sizeof(SmallOwnedEvent),
        512U);
    std::unique_ptr<SmallStore> store;
    test->Expect(
        SmallStore::Create(config, &store) ==
            market::SessionStoreCreateErrorV1::kNone,
        "full-session small store is created");
    if (store == nullptr) {
        return;
    }

    bool accepted = true;
    for (std::uint64_t index = 0U; index < kRecords; ++index) {
        // Deliberate gaps prove that market source order is increasing, not
        // falsely required to be contiguous across intervening controls.
        const market::SessionAppendResultV1 result = store->Append(
            SmallRecord(
                config.streams[0],
                index * 2U + 1U,
                index,
                index));
        accepted = accepted && result.accepted();
    }
    test->Expect(
        accepted,
        "full-session store accepts more than feeder probe's old 100000 cap");

    SmallSnapshot snapshot;
    test->Expect(
        store->Snapshot(&snapshot) ==
            market::SessionSnapshotErrorV1::kNone,
        "large full-session immutable snapshot succeeds");
    const auto* stream = snapshot.StreamAt(0U);
    test->Expect(
        stream != nullptr && stream->record_count() == kRecords,
        "large full-session snapshot retains every accepted record");
    if (stream != nullptr) {
        const auto first = stream->RecordAt(0U);
        const auto last = stream->RecordAt(kRecords - 1U);
        test->Expect(
            first && last && first->source_sequence == 1U &&
                last->source_sequence == (kRecords - 1U) * 2U + 1U,
            "large full-session order and endpoints are exact");
    }
}

void TestDecodedOwnershipAndPerStreamOrder(TestContext* test) {
    market::SessionStoreConfigV1 config = StoreConfig(
        market::SessionRetentionModeV1::kFullSession,
        0U,
        16U,
        4U * 1024U * 1024U,
        2U);
    std::unique_ptr<DecodedStore> store;
    test->Expect(
        DecodedStore::Create(config, &store) ==
            market::SessionStoreCreateErrorV1::kNone,
        "decoded market store is created");
    if (store == nullptr) {
        return;
    }

    {
        std::string source_security = "600000";
        std::string source_type = "A";
        test->Expect(
            store->Append(DecodedRecord(
                config.streams[0],
                1U,
                100U,
                OwnedShanghaiTick(
                    config.streams[0].source_stream_id,
                    1U,
                    100U,
                    source_security,
                    source_type)))
                .accepted(),
            "decoded owned event is injected");
        source_security.assign("destroyed-source-buffer");
        source_type.clear();
    }
    test->Expect(
        store->Append(DecodedRecord(
            config.streams[0],
            4U,
            101U,
            OwnedShanghaiTick(
                config.streams[0].source_stream_id,
                4U,
                101U,
                "600001",
                "D")))
            .accepted(),
        "noncontiguous strictly increasing sequence is accepted");

    // Identical bare sequences in different source namespaces are unrelated.
    for (std::size_t index = 1U; index < config.streams.size(); ++index) {
        test->Expect(
            store->Append(DecodedRecord(
                config.streams[index],
                1U,
                10U + index,
                OwnedShanghaiTick(
                    config.streams[index].source_stream_id,
                    1U,
                    10U + index,
                    "independent",
                    "A")))
                .accepted(),
            "same bare sequence is independently accepted in another stream");
    }

    const market::SessionAppendResultV1 backward = store->Append(
        DecodedRecord(
            config.streams[0],
            3U,
            102U,
            OwnedShanghaiTick(
                config.streams[0].source_stream_id,
                3U,
                102U,
                "backward",
                "A")));
    test->Expect(
        backward.error ==
            market::SessionAppendErrorV1::
                kSourceSequenceNotIncreasing,
        "backward per-stream source sequence fails explicitly");

    market::SessionStreamKeyV1 wrong_day = config.streams[0];
    wrong_day.capture_date = 20260723U;
    const market::SessionAppendResultV1 context_mismatch =
        store->Append(DecodedRecord(
            wrong_day,
            5U,
            102U,
            OwnedShanghaiTick(
                wrong_day.source_stream_id,
                5U,
                102U,
                "wrong-day",
                "A")));
    test->Expect(
        context_mismatch.error ==
            market::SessionAppendErrorV1::kStreamContextMismatch,
        "same source id with a different session context is rejected");

    DecodedSnapshot snapshot;
    test->Expect(
        store->Snapshot(&snapshot) ==
            market::SessionSnapshotErrorV1::kNone,
        "decoded immutable snapshot succeeds");
    const auto* first_stream = snapshot.StreamAt(0U);
    const auto owned = first_stream == nullptr
                           ? market::SessionRecordHandleV1<
                                 market::DecodedMarketEventV1>{}
                           : first_stream->RecordAt(0U);
    const market::ShanghaiTickV1* tick =
        owned ? std::get_if<market::ShanghaiTickV1>(&owned->event)
              : nullptr;
    test->Expect(
        tick != nullptr && tick->common.security_id == "600000" &&
            tick->raw_type == "A" && tick->common.origin.body.empty(),
        "stored decoded content owns strings and retains no borrowed body");
    test->Expect(
        snapshot.StreamAt(0U) != nullptr &&
            snapshot.StreamAt(0U)->record_count() == 2U,
        "failed order/context attempts never overwrite resident content");
}

void TestCapacityIsExplicitAndNonOverwriting(TestContext* test) {
    market::SessionStoreConfigV1 config = StoreConfig(
        market::SessionRetentionModeV1::kFullSession,
        0U,
        2U,
        100U,
        2U);
    std::unique_ptr<SmallStore> store;
    test->Expect(
        SmallStore::Create(config, &store) ==
            market::SessionStoreCreateErrorV1::kNone,
        "record-limited store is created");
    if (store == nullptr) {
        return;
    }
    test->Expect(
        store->Append(SmallRecord(config.streams[0], 1U, 1U, 11U))
            .accepted() &&
            store->Append(SmallRecord(config.streams[0], 2U, 2U, 22U))
                .accepted(),
        "records below hard count limit are accepted");

    SmallSnapshot before_failure;
    test->Expect(
        store->Snapshot(&before_failure) ==
            market::SessionSnapshotErrorV1::kNone,
        "pre-capacity snapshot succeeds");
    const auto stable_first =
        before_failure.StreamAt(0U)->RecordAt(0U);
    const market::SessionAppendResultV1 rejected = store->Append(
        SmallRecord(config.streams[0], 3U, 3U, 33U));
    test->Expect(
        rejected.error ==
            market::SessionAppendErrorV1::kMaxRecordsExceeded &&
            rejected.store_resident_records == 2U,
        "hard record capacity rejects the new event explicitly");

    SmallSnapshot after_failure;
    test->Expect(
        store->Snapshot(&after_failure) ==
            market::SessionSnapshotErrorV1::kNone &&
            after_failure.StreamAt(0U)->record_count() == 2U &&
            after_failure.StreamAt(0U)->RecordAt(1U)->event.value == 22U &&
            stable_first && stable_first->event.value == 11U,
        "capacity failure preserves both current and old snapshot content");
    const market::SessionStoreWatermarkV1 watermark =
        store->Watermark();
    test->Expect(
        watermark.counters.rejected_records == 1U &&
            watermark.counters.rejected_by_error[
                static_cast<std::size_t>(
                    market::SessionAppendErrorV1::
                        kMaxRecordsExceeded)] == 1U,
        "capacity rejection is visible in reason counters");

    market::SessionStoreConfigV1 byte_config = StoreConfig(
        market::SessionRetentionModeV1::kFullSession,
        0U,
        4U,
        3U,
        2U);
    std::unique_ptr<SmallStore> byte_store;
    test->Expect(
        SmallStore::Create(byte_config, &byte_store) ==
            market::SessionStoreCreateErrorV1::kNone,
        "byte-limited store is created");
    if (byte_store != nullptr) {
        test->Expect(
            byte_store
                    ->Append(SmallRecord(
                        byte_config.streams[0], 1U, 1U, 1U, 2U))
                    .accepted(),
            "first payload fits byte capacity");
        test->Expect(
            byte_store
                    ->Append(SmallRecord(
                        byte_config.streams[0], 2U, 2U, 2U, 2U))
                    .error == market::SessionAppendErrorV1::
                                  kMaxPayloadBytesExceeded,
            "payload byte capacity rejects without silent replacement");
    }
}

void TestWindowIsInclusiveAndPerStream(TestContext* test) {
    market::SessionStoreConfigV1 config = StoreConfig(
        market::SessionRetentionModeV1::kRecvMonotonicWindow,
        10U,
        16U,
        1024U,
        2U);
    std::unique_ptr<SmallStore> store;
    test->Expect(
        SmallStore::Create(config, &store) ==
            market::SessionStoreCreateErrorV1::kNone,
        "windowed store is created");
    if (store == nullptr) {
        return;
    }

    test->Expect(
        store->Append(SmallRecord(config.streams[0], 1U, 100U, 100U))
            .accepted(),
        "window first record is accepted");
    SmallSnapshot old_snapshot;
    test->Expect(
        store->Snapshot(&old_snapshot) ==
            market::SessionSnapshotErrorV1::kNone,
        "old window snapshot is captured");
    const auto old_record = old_snapshot.StreamAt(0U)->RecordAt(0U);

    const market::SessionAppendResultV1 boundary = store->Append(
        SmallRecord(config.streams[0], 3U, 110U, 110U));
    test->Expect(
        boundary.accepted() && boundary.evicted_records == 0U,
        "record exactly on inclusive monotonic window boundary is retained");
    test->Expect(
        store->Append(SmallRecord(config.streams[1], 1U, 1000U, 1000U))
            .accepted() &&
            store->Watermark().streams[0].resident_records == 2U,
        "another stream's receive time does not advance this stream window");

    const market::SessionAppendResultV1 advanced = store->Append(
        SmallRecord(config.streams[0], 5U, 111U, 111U));
    test->Expect(
        advanced.accepted() && advanced.evicted_records == 1U &&
            advanced.stream_resident_records == 2U,
        "strictly older prefix is explicitly evicted after window advances");
    test->Expect(
        old_record && old_record->event.value == 100U,
        "old immutable snapshot remains valid after store eviction");

    const market::SessionAppendResultV1 regression = store->Append(
        SmallRecord(config.streams[0], 7U, 109U, 109U));
    test->Expect(
        regression.error ==
            market::SessionAppendErrorV1::kRecvMonotonicRegression &&
            regression.stream_resident_records == 2U,
        "same-stream receive monotonic regression fails explicitly");
    const market::SessionStreamWatermarkV1 stream_watermark =
        store->Watermark().streams[0];
    test->Expect(
        stream_watermark.retention_floor_recv_monotonic_ns == 101U &&
            stream_watermark.oldest_resident_recv_monotonic_ns == 110U &&
            stream_watermark.counters.evicted_records == 1U,
        "window floor, oldest resident, and eviction counter agree");
}

void TestSnapshotReaderDoesNotBlockOnLaterAppends(TestContext* test) {
    market::SessionStoreConfigV1 config = StoreConfig(
        market::SessionRetentionModeV1::kFullSession,
        0U,
        2000U,
        2000U * sizeof(SmallOwnedEvent),
        32U);
    std::unique_ptr<SmallStore> store;
    test->Expect(
        SmallStore::Create(config, &store) ==
            market::SessionStoreCreateErrorV1::kNone,
        "concurrent snapshot store is created");
    if (store == nullptr) {
        return;
    }
    for (std::uint64_t index = 0U; index < 100U; ++index) {
        const bool accepted = store
                                  ->Append(SmallRecord(
                                      config.streams[0],
                                      index + 1U,
                                      index + 1U,
                                      index + 1U))
                                  .accepted();
        test->Expect(accepted, "concurrent test prefix append succeeds");
    }

    SmallSnapshot stable;
    test->Expect(
        store->Snapshot(&stable) ==
            market::SessionSnapshotErrorV1::kNone,
        "concurrent test stable snapshot succeeds");
    std::atomic<bool> writer_ok{true};
    std::atomic<bool> reader_ok{true};
    std::thread writer([&]() {
        for (std::uint64_t index = 100U; index < 1100U; ++index) {
            if (!store
                     ->Append(SmallRecord(
                         config.streams[0],
                         index + 1U,
                         index + 1U,
                         index + 1U))
                     .accepted()) {
                writer_ok.store(false, std::memory_order_relaxed);
                return;
            }
        }
    });
    std::thread reader([stable, &reader_ok]() {
        for (std::size_t pass = 0U; pass < 100U; ++pass) {
            const auto* stream = stable.StreamAt(0U);
            if (stream == nullptr || stream->record_count() != 100U) {
                reader_ok.store(false, std::memory_order_relaxed);
                return;
            }
            std::uint64_t expected = 1U;
            stream->VisitRecords([&](const auto& record) {
                if (record.source_sequence != expected ||
                    record.event.value != expected) {
                    reader_ok.store(false, std::memory_order_relaxed);
                }
                ++expected;
            });
            if (expected != 101U) {
                reader_ok.store(false, std::memory_order_relaxed);
                return;
            }
        }
    });
    writer.join();
    reader.join();
    test->Expect(
        writer_ok.load(std::memory_order_relaxed) &&
            reader_ok.load(std::memory_order_relaxed),
        "immutable reader traversal and later writer appends both succeed");

    SmallSnapshot latest;
    test->Expect(
        store->Snapshot(&latest) ==
                market::SessionSnapshotErrorV1::kNone &&
            latest.StreamAt(0U)->record_count() == 1100U &&
            stable.StreamAt(0U)->record_count() == 100U,
        "new high-water snapshot grows while old snapshot stays frozen");
}

#ifndef L2FLOW_SESSION_STORE_CORE_ONLY
void TestMarketSessionOwnedInjection(TestContext* test) {
    market::MarketSessionConfigV1 config;
    config.store = StoreConfig(
        market::SessionRetentionModeV1::kFullSession,
        0U,
        16U,
        4U * 1024U * 1024U,
        4U);
    std::unique_ptr<market::MarketSessionV1> session;
    test->Expect(
        market::MarketSessionV1::Create(config, &session) ==
            market::MarketSessionCreateErrorV1::kNone,
        "direct market injection session is created");
    if (session == nullptr) {
        return;
    }

    std::vector<std::byte> callback_body = ShanghaiAddWire();
    market::MarketMessageViewV1 input;
    input.source_stream_id =
        config.store.streams[1].source_stream_id;
    input.trade_date = config.store.streams[1].capture_date;
    input.source_sequence = 11U;
    input.service_id = 4U;
    input.service_version = 101U;
    input.message_id = 24U;
    input.vendor_local_time_raw = 93'000'124U;
    input.vendor_sequence_id = 88U;
    input.recv_realtime_ns = 1'000;
    input.recv_monotonic_ns = 100;
    input.body = callback_body;

    const market::MarketSessionInjectResultV1 result =
        session->Inject(input);
    test->Expect(
        result.accepted() && result.append.accepted() &&
            result.append.source_stream_known &&
            result.append.stream_resident_records == 1U,
        "synchronous feeder view decodes and enters session memory");

    // The feeder callback buffer may be reused immediately after Inject.
    // Stored content must therefore remain independent of these bytes.
    std::fill(
        callback_body.begin(), callback_body.end(), std::byte{0xffU});
    input.body = {};

    market::MarketSessionSnapshotV1 snapshot;
    test->Expect(
        session->Snapshot(&snapshot) ==
            market::SessionSnapshotErrorV1::kNone,
        "direct injection snapshot succeeds");
    const auto* stream = snapshot.StreamAt(1U);
    const auto record =
        stream == nullptr
            ? market::SessionRecordHandleV1<
                  market::RetainedMarketEventV1>{}
            : stream->RecordAt(0U);
    const market::ShanghaiTickV1* tick =
        record
            ? market::RetainedMarketEventGetV1<
                  market::ShanghaiTickV1>(record->event)
            : nullptr;
    test->Expect(
        tick != nullptr && tick->common.security_id == "600000" &&
            tick->raw_type == "A" && tick->raw_tick_flag == "B" &&
            tick->fields.price.normalized_p6 == 12'345'000 &&
            tick->fields.matched_quantity.raw == 7 &&
            tick->common.origin.body.empty(),
        "session retains owned decoded fields after callback buffer reuse");
    test->Expect(
        session->Watermark().resident_records == 1U,
        "direct injection is visible in the aggregate watermark");
    test->Expect(
        static_cast<bool>(record) &&
            record->owned_payload_bytes ==
                market::EstimateRetainedMarketEventBytesV1(
                    record->event) &&
            record->owned_payload_bytes <
                sizeof(market::DecodedMarketEventV1),
        "retained tick is charged from its materialized exact-type owner");
}

void TestMarketSessionFamilyPoisoning(TestContext* test) {
    market::MarketSessionConfigV1 config;
    config.store = StoreConfig(
        market::SessionRetentionModeV1::kFullSession,
        0U,
        16U,
        4U * 1024U * 1024U,
        4U);
    std::unique_ptr<market::MarketSessionV1> session;
    test->Expect(
        market::MarketSessionV1::Create(config, &session) ==
            market::MarketSessionCreateErrorV1::kNone,
        "market injection session is created");
    if (session == nullptr) {
        return;
    }

    market::MarketMessageViewV1 unknown;
    unknown.source_stream_id = 999999U;
    const market::MarketSessionInjectResultV1 unknown_result =
        session->Inject(unknown);
    test->Expect(
        unknown_result.error ==
            market::MarketSessionInjectErrorV1::kUnknownSourceStream,
        "unknown injection source fails without inventing a namespace");

    market::MarketMessageViewV1 wrong_family;
    wrong_family.source_stream_id =
        config.store.streams[0].source_stream_id;
    wrong_family.trade_date = config.store.streams[0].capture_date;
    wrong_family.source_sequence = 1U;
    wrong_family.service_id = sh::NGTSTick::ServiceID;
    wrong_family.service_version = 101U;
    wrong_family.message_id = sh::NGTSTick::MessageID;
    wrong_family.recv_monotonic_ns = 1;
    const market::MarketSessionInjectResultV1 wrong_result =
        session->Inject(wrong_family);
    test->Expect(
        wrong_result.error ==
                market::MarketSessionInjectErrorV1::kWrongStreamFamily &&
            wrong_result.source_poisoned &&
            session->source_poisoned(0U),
        "wrong message family poisons only its configured source");

    wrong_family.service_id = sh::SHL2MarketData::ServiceID;
    wrong_family.message_id = sh::SHL2MarketData::MessageID;
    const market::MarketSessionInjectResultV1 poisoned_result =
        session->Inject(wrong_family);
    test->Expect(
        poisoned_result.error ==
            market::MarketSessionInjectErrorV1::kSourcePoisoned,
        "poisoned source rejects subsequent injections deterministically");

    market::MarketMessageViewV1 negative_time;
    negative_time.source_stream_id =
        config.store.streams[1].source_stream_id;
    negative_time.trade_date = config.store.streams[1].capture_date;
    negative_time.source_sequence = 1U;
    negative_time.service_id = sh::NGTSTick::ServiceID;
    negative_time.service_version = 101U;
    negative_time.message_id = sh::NGTSTick::MessageID;
    negative_time.recv_monotonic_ns = -1;
    const market::MarketSessionInjectResultV1 negative_result =
        session->Inject(negative_time);
    test->Expect(
        negative_result.error ==
                market::MarketSessionInjectErrorV1::kInvalidReceiveTime &&
            session->source_poisoned(1U) &&
            !session->source_poisoned(2U),
        "invalid receive time is explicit and poison scope remains per source");
    test->Expect(
        session->Watermark().resident_records == 0U,
        "rejected injections never enter in-memory session content");
}
#endif

}  // namespace

int main() {
    TestContext test;
    TestFullSessionExceedsOldProbeLimit(&test);
    TestDecodedOwnershipAndPerStreamOrder(&test);
    TestCapacityIsExplicitAndNonOverwriting(&test);
    TestWindowIsInclusiveAndPerStream(&test);
    TestSnapshotReaderDoesNotBlockOnLaterAppends(&test);
#ifndef L2FLOW_SESSION_STORE_CORE_ONLY
    TestMarketSessionOwnedInjection(&test);
    TestMarketSessionFamilyPoisoning(&test);
#endif

    if (test.failures() == 0) {
        std::cout << "phase4 session store tests passed\n";
    }
    return test.failures() == 0 ? 0 : 1;
}
