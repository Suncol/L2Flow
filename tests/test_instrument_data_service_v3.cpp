#include "l2flow/ipc/instrument_data_service_v3.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

namespace {

namespace ipc = l2flow::ipc;
namespace market = l2flow::market;
namespace runtime = l2flow::runtime;

constexpr std::uint32_t kTradeDate = 20260805U;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

l2flow::common::Identity128 SessionId() {
    l2flow::common::Identity128 result{};
    result[0U] = std::byte{0x31U};
    result[15U] = std::byte{0x13U};
    return result;
}

runtime::RealtimePlanesConfigV1 PlaneConfig() {
    runtime::RealtimePlanesConfigV1 result{};
    result.fast.session_id = SessionId();
    result.fast.trade_date = kTradeDate;
    result.fast.instrument_count = 1U;
    result.fast.worker_count = 1U;
    result.fast.maximum_session_records = 64U;
    result.fast.records_per_chunk = 4U;
    result.fast.maximum_records_per_read = 16U;
    result.fast.coverage_from_open = true;
    result.fast.tick_routes = {0U};

    result.event.session_id = SessionId();
    result.event.trade_date = kTradeDate;
    result.event.instrument_count = 1U;
    result.event.worker_count = 1U;
    result.event.maximum_order_states_per_instrument = 64U;
    result.event.maximum_inputs_per_instrument = 64U;
    result.event.maximum_events_per_instrument = 256U;
    result.event.input_block_records = 4U;
    result.event.event_block_records = 4U;
    result.event.cdc_range_chunk_records = 4U;
    result.event.maximum_change_records_per_instrument = 1024U;
    result.event.maximum_changes_per_read = 16U;
    result.event.event_routes = {0U};

    result.kline.session_id = SessionId();
    result.kline.trade_date = kTradeDate;
    result.kline.instrument_count = 1U;
    result.kline.worker_count = 1U;
    result.kline.windows = {{1U, 1'000'000'000U}};
    result.kline.maximum_trades_per_instrument = 64U;
    result.kline.maximum_bars_per_instrument = 64U;
    result.kline.maximum_change_records_per_instrument = 512U;
    result.kline.maximum_changes_per_read = 16U;
    result.kline.kline_routes = {0U};

    result.event_queue_capacity_per_tick_worker = 8U;
    result.kline_queue_capacity_per_tick_worker = 8U;
    result.live_batch_budget = 4U;
    return result;
}

bool ProjectTrade(
    std::uint64_t arrival_id,
    market::CompactFastTickV1* compact,
    market::DecodedFastTickV1* owned) {
    market::ShanghaiTickV1 tick{};
    tick.common.kind = market::MarketEventKindV1::kShanghaiTick;
    tick.common.market = market::MarketV1::kShanghai;
    tick.common.origin.trade_date = kTradeDate;
    tick.common.origin.source_stream_id = 1U;
    tick.common.origin.source_sequence = arrival_id;
    tick.common.origin.vendor_sequence_id = arrival_id;
    tick.common.origin.recv_realtime_ns =
        static_cast<std::int64_t>(arrival_id);
    tick.common.origin.recv_monotonic_ns =
        static_cast<std::int64_t>(arrival_id);
    tick.common.instrument_id = 1U;
    tick.common.ordinal = 0U;
    tick.common.exchange_time.valid = true;
    tick.common.exchange_time.unix_nanoseconds_valid = true;
    tick.common.exchange_time.nanoseconds_since_midnight =
        9U * 3'600U * 1'000'000'000U + arrival_id;
    tick.common.exchange_time.unix_nanoseconds =
        1'786'000'000'000'000'000LL +
        static_cast<std::int64_t>(arrival_id);
    tick.channel = 3;
    tick.business_index = static_cast<std::int64_t>(arrival_id);
    tick.fields.action = market::TickActionV1::kTrade;
    tick.fields.side = market::SideV1::kBuy;
    tick.fields.aggressor = market::AggressorV1::kBuy;
    tick.fields.price.valid = true;
    tick.fields.price.normalized_p6 = 10'000'000;
    tick.fields.quantity.valid = true;
    tick.fields.quantity.raw = 100;
    tick.fields.buy_order_id = 10;
    tick.fields.sell_order_id = 20;
    tick.fields.validity_bitmap =
        market::kTickPriceValidV1 |
        market::kTickQuantityValidV1 |
        market::kTickBuyOrderIdValidV1 |
        market::kTickSellOrderIdValidV1 |
        market::kTickAggressorValidV1 |
        market::kTickExchangeTimeValidV1;
    market::DecodedMarketEventV1 decoded(std::move(tick));
    return market::ProjectFastTickV1(
               std::move(decoded),
               market::FastTickSourceV1::kShanghaiTick,
               arrival_id,
               owned,
               compact) == market::FastTickProjectionErrorV1::kNone;
}

bool TestWireCodecs() {
    bool ok = true;
    ipc::FastTickCursorWireV3 cursor{};
    cursor.session_id = SessionId();
    cursor.instrument_id = 0x01020304U;
    cursor.next_arrival_row = 0x0102030405060708ULL;
    ipc::RealtimeCursorBytesV3 cursor_bytes{};
    const ipc::RealtimeCursorBytesV3 cursor_golden{
        std::byte{0x31U}, std::byte{0x00U}, std::byte{0x00U},
        std::byte{0x00U}, std::byte{0x00U}, std::byte{0x00U},
        std::byte{0x00U}, std::byte{0x00U}, std::byte{0x00U},
        std::byte{0x00U}, std::byte{0x00U}, std::byte{0x00U},
        std::byte{0x00U}, std::byte{0x00U}, std::byte{0x00U},
        std::byte{0x13U}, std::byte{0x04U}, std::byte{0x03U},
        std::byte{0x02U}, std::byte{0x01U}, std::byte{0x00U},
        std::byte{0x00U}, std::byte{0x00U}, std::byte{0x00U},
        std::byte{0x08U}, std::byte{0x07U}, std::byte{0x06U},
        std::byte{0x05U}, std::byte{0x04U}, std::byte{0x03U},
        std::byte{0x02U}, std::byte{0x01U}};
    ok &= Expect(
        ipc::EncodeFastTickCursorWireV3(cursor, &cursor_bytes) ==
                ipc::RealtimeWireCodecErrorV3::kNone &&
            cursor_bytes == cursor_golden,
        "C++ cursor codec matches the language-neutral little-endian golden");

    ipc::FastTickCursorWireV3 decoded_cursor{};
    ok &= Expect(
        ipc::DecodeFastTickCursorWireV3(
            cursor_bytes, &decoded_cursor) ==
                ipc::RealtimeWireCodecErrorV3::kNone &&
            decoded_cursor.session_id == cursor.session_id &&
            decoded_cursor.instrument_id == cursor.instrument_id &&
            decoded_cursor.next_arrival_row == cursor.next_arrival_row,
        "C++ cursor decoder round-trips explicit bytes");
    auto invalid_cursor = cursor_bytes;
    invalid_cursor[20U] = std::byte{0x01U};
    ok &= Expect(
        ipc::DecodeFastTickCursorWireV3(
            invalid_cursor, &decoded_cursor) ==
            ipc::RealtimeWireCodecErrorV3::kReservedNonzero,
        "cursor decoder rejects nonzero reserved bytes");

    ipc::InstrumentStableStatusWireV3 status{};
    status.dataset = static_cast<std::uint8_t>(
        ipc::RealtimeDatasetV3::kDerivedEvent);
    status.repair_state = static_cast<std::uint8_t>(
        ipc::RealtimeRepairStateWireV3::kRebuilding);
    status.instrument_id = 0x01020304U;
    status.stable_tail = 0x0102030405060708ULL;
    status.repair_through_arrival_id = 0x1112131415161718ULL;
    ipc::RealtimeStableStatusBytesV3 status_bytes{};
    const ipc::RealtimeStableStatusBytesV3 status_golden{
        std::byte{'L'}, std::byte{'2'}, std::byte{'F'}, std::byte{'3'},
        std::byte{0x03U}, std::byte{0x00U}, std::byte{0x00U},
        std::byte{0x00U}, std::byte{0x02U}, std::byte{0x02U},
        std::byte{0x00U}, std::byte{0x00U}, std::byte{0x04U},
        std::byte{0x03U}, std::byte{0x02U}, std::byte{0x01U},
        std::byte{0x08U}, std::byte{0x07U}, std::byte{0x06U},
        std::byte{0x05U}, std::byte{0x04U}, std::byte{0x03U},
        std::byte{0x02U}, std::byte{0x01U}, std::byte{0x18U},
        std::byte{0x17U}, std::byte{0x16U}, std::byte{0x15U},
        std::byte{0x14U}, std::byte{0x13U}, std::byte{0x12U},
        std::byte{0x11U}};
    ok &= Expect(
        ipc::EncodeStableStatusWireV3(status, &status_bytes) ==
                ipc::RealtimeWireCodecErrorV3::kNone &&
            status_bytes == status_golden,
        "C++ status codec matches the language-neutral little-endian golden");
    ipc::InstrumentStableStatusWireV3 decoded_status{};
    ok &= Expect(
        ipc::DecodeStableStatusWireV3(
            status_bytes, &decoded_status) ==
                ipc::RealtimeWireCodecErrorV3::kNone &&
            decoded_status.dataset == status.dataset &&
            decoded_status.repair_state == status.repair_state &&
            decoded_status.instrument_id == status.instrument_id &&
            decoded_status.stable_tail == status.stable_tail &&
            decoded_status.repair_through_arrival_id ==
                status.repair_through_arrival_id,
        "C++ status decoder validates and round-trips explicit bytes");
    auto invalid_status = status_bytes;
    invalid_status[8U] = std::byte{0xffU};
    ok &= Expect(
        ipc::DecodeStableStatusWireV3(
            invalid_status, &decoded_status) ==
            ipc::RealtimeWireCodecErrorV3::kInvalidDataset,
        "status decoder rejects an unknown dataset code");
    return ok;
}

template <typename Predicate>
bool WaitUntil(Predicate predicate) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::yield();
    }
    return predicate();
}

}  // namespace

int main() {
    bool ok = TestWireCodecs();
    static_assert(ipc::kRealtimeWireMajorV3 == 3U);
    static_assert(sizeof(ipc::FastTickCursorWireV3) == 32U);

    std::unique_ptr<runtime::RealtimePlanesV1> planes;
    std::string detail;
    ok &= Expect(
        runtime::RealtimePlanesV1::Create(
            PlaneConfig(), &planes, &detail) ==
                runtime::RealtimePlanesCreateErrorV1::kNone &&
            planes != nullptr,
        "create three independent planes");
    if (planes == nullptr) {
        return 1;
    }

    std::unique_ptr<ipc::InstrumentDataServiceV3> service;
    ok &= Expect(
        ipc::InstrumentDataServiceV3::Create(
            {}, planes.get(), &service) ==
                ipc::InstrumentDataServiceErrorV3::kNone &&
            service != nullptr,
        "create instrument-local V3 service");
    if (service == nullptr) {
        return 1;
    }

    ipc::EventStableViewV3 initial_event{};
    ipc::KLineStableViewV3 initial_kline{};
    ok &= Expect(
        service->AcquireEventStable(1U, &initial_event) ==
                ipc::InstrumentDataServiceErrorV3::kNone &&
            initial_event.root != nullptr &&
            initial_event.root->row_count() == 0U,
        "acquire initial Event root and CDC cursor atomically");
    ok &= Expect(
        service->AcquireKLineStable(1U, &initial_kline) ==
                ipc::InstrumentDataServiceErrorV3::kNone &&
            initial_kline.root != nullptr &&
            initial_kline.root->bar_count() == 0U,
        "acquire initial KLine root and CDC cursor atomically");

    market::CompactFastTickV1 compact{};
    market::DecodedFastTickV1 owned{};
    ok &= Expect(ProjectTrade(1U, &compact, &owned), "project FAST trade");
    const auto routed = planes->PublishDecoded(
        0U, compact, std::move(owned));
    ok &= Expect(
        routed.error == runtime::RealtimePublishErrorV1::kNone &&
            routed.fast_published && routed.event_enqueued &&
            routed.kline_enqueued,
        "FAST publish precedes both compact derived fan-outs");
    ok &= Expect(
        planes->WaitFastPublished(
            1U, 1U, std::chrono::seconds(3)),
        "FAST row becomes visible");
    ok &= Expect(
        WaitUntil([&]() {
            return planes->Snapshot().event_applied == 1U &&
                   planes->Snapshot().kline_applied == 1U;
        }),
        "derived planes catch up independently");

    ipc::FastTickCursorWireV3 fast_cursor{};
    fast_cursor.session_id = SessionId();
    fast_cursor.instrument_id = 1U;
    ipc::FastTickDeltaV3 fast_delta{};
    ok &= Expect(
        service->ReadFastDelta(fast_cursor, 8U, &fast_delta) ==
                ipc::InstrumentDataServiceErrorV3::kNone &&
            fast_delta.rows.size() == 1U &&
            fast_delta.rows[0U].instrument_tick_sequence == 1U &&
            fast_delta.next.next_arrival_row == 2U &&
            fast_delta.coverage_from_open &&
            fast_delta.coverage_complete,
        "FAST V3 cursor reads only the instrument append ledger");

    std::array<market::EventMutationV1, 8U> event_changes{};
    std::size_t event_written = 0U;
    auto event_cursor = initial_event.next_changes;
    ok &= Expect(
        service->ReadEventChanges(
            &event_cursor, event_changes, &event_written) ==
                ipc::InstrumentDataServiceErrorV3::kNone &&
            event_written > 0U,
        "Event V3 cursor reads instrument-local CDC");

    std::array<market::KLineMutationV1, 8U> kline_changes{};
    std::size_t kline_written = 0U;
    auto kline_cursor = initial_kline.next_changes;
    ok &= Expect(
        service->ReadKLineChanges(
            &kline_cursor, kline_changes, &kline_written) ==
                ipc::InstrumentDataServiceErrorV3::kNone &&
            kline_written == 1U &&
            kline_changes[0U].kind ==
                market::KLineMutationKindV1::kUpsert,
        "KLine V3 cursor reads revisioned UPSERT CDC");

    ipc::EventStableViewV3 event_after{};
    ipc::KLineStableViewV3 kline_after{};
    ok &= Expect(
        service->AcquireEventStable(1U, &event_after) ==
                ipc::InstrumentDataServiceErrorV3::kNone &&
            event_after.root->strictly_ordered() &&
            event_after.status.dataset == static_cast<std::uint8_t>(
                ipc::RealtimeDatasetV3::kDerivedEvent),
        "Event stable view is ordered and tagged as V3 Event");
    ok &= Expect(
        service->AcquireKLineStable(1U, &kline_after) ==
                ipc::InstrumentDataServiceErrorV3::kNone &&
            kline_after.root->bar_count() == 1U &&
            kline_after.status.dataset == static_cast<std::uint8_t>(
                ipc::RealtimeDatasetV3::kKLine),
        "KLine stable view exposes one atomic current bar");

    auto wrong = fast_cursor;
    wrong.session_id[0U] = std::byte{0xffU};
    ok &= Expect(
        service->ReadFastDelta(wrong, 8U, &fast_delta) ==
            ipc::InstrumentDataServiceErrorV3::kCursorMismatch,
        "V3 rejects a cursor from another session");

    planes->StopAndDrain();
    return ok ? 0 : 1;
}
