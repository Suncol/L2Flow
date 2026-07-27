#include "l2flow/ipc/realtime_shared_service_v1.h"
#include "l2flow/ipc/realtime_shm_reader_c_v1.h"
#include "l2flow/ipc/realtime_wire_projection_v1.h"
#include "l2flow/ipc/realtime_wire_v1.h"
#include "l2flow/market/instrument_registry.h"
#include "l2flow/market/intraday_instrument_store_v1.h"
#include "l2flow/market/realtime_history_v1.h"

#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

namespace common = l2flow::common;
namespace ipc = l2flow::ipc;
namespace market = l2flow::market;

constexpr std::uint32_t kShanghaiInstrumentId = 1001U;
constexpr std::uint32_t kShenzhenInstrumentId = 2002U;
constexpr std::uint32_t kUnobservedInstrumentId = 3003U;
constexpr std::uint32_t kUnknownInstrumentId = 9009U;
constexpr std::uint32_t kTradeDate = 20260727U;
constexpr std::array<std::uint32_t, 4U> kSourceStreamIds{
    11U, 12U, 13U, 14U};

enum RecordIndex : std::size_t {
    kShanghaiSnapshot = 0U,
    kShanghaiTick,
    kShenzhenSnapshot,
    kShenzhenOrder,
    kShenzhenTransaction,
    kRecordCount,
};

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

std::vector<std::byte> Bytes(std::string_view text) {
    const std::span<const char> characters(text.data(), text.size());
    const std::span<const std::byte> bytes =
        std::as_bytes(characters);
    return {bytes.begin(), bytes.end()};
}

market::DecimalValueV1 Decimal(
    std::int64_t raw,
    std::int64_t normalized_p6,
    std::uint8_t scale) {
    market::DecimalValueV1 value{};
    value.raw = raw;
    value.normalized_p6 = normalized_p6;
    value.scale = scale;
    value.valid = true;
    return value;
}

market::QuantityValueV1 Quantity(
    std::int64_t raw,
    std::uint8_t scale) {
    market::QuantityValueV1 value{};
    value.raw = raw;
    value.scale = scale;
    value.valid = true;
    return value;
}

class UniqueFd final {
public:
    explicit UniqueFd(int descriptor = -1) noexcept
        : descriptor_(descriptor) {}
    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;
    ~UniqueFd() { Reset(); }

    [[nodiscard]] int get() const noexcept { return descriptor_; }

    void Reset(int descriptor = -1) noexcept {
        if (descriptor_ >= 0) {
            static_cast<void>(::close(descriptor_));
        }
        descriptor_ = descriptor;
    }

private:
    int descriptor_ = -1;
};

class ReaderHandle final {
public:
    ReaderHandle() = default;
    ReaderHandle(const ReaderHandle&) = delete;
    ReaderHandle& operator=(const ReaderHandle&) = delete;
    ~ReaderHandle() { Reset(); }

    [[nodiscard]] l2flow_shm_reader_v1* get() const noexcept {
        return reader_;
    }
    [[nodiscard]] l2flow_shm_reader_v1** output() noexcept {
        Reset();
        return &reader_;
    }

    void Reset() noexcept {
        l2flow_shm_reader_close_v1(reader_);
        reader_ = nullptr;
    }

private:
    l2flow_shm_reader_v1* reader_ = nullptr;
};

class ScopedTempDirectory final {
public:
    ScopedTempDirectory() {
        char path_template[] =
            "/tmp/l2flow-realtime-ipc-v1-XXXXXX";
        char* const created = ::mkdtemp(path_template);
        if (created != nullptr && ::chmod(created, 0700) == 0) {
            path_ = created;
            return;
        }
        if (created != nullptr) {
            static_cast<void>(::rmdir(created));
        }
    }

    ScopedTempDirectory(const ScopedTempDirectory&) = delete;
    ScopedTempDirectory& operator=(const ScopedTempDirectory&) = delete;

    ~ScopedTempDirectory() {
        if (path_.empty()) {
            return;
        }
        std::error_code error;
        static_cast<void>(std::filesystem::remove_all(path_, error));
    }

    [[nodiscard]] bool valid() const noexcept {
        return !path_.empty();
    }
    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

std::unique_ptr<market::InstrumentRegistryV1> MakeRegistry() {
    std::vector<market::InstrumentRegistryEntryV1> entries;

    market::InstrumentRegistryEntryV1 shanghai{};
    shanghai.instrument_id = kShanghaiInstrumentId;
    shanghai.key.market = market::MarketV1::kShanghai;
    shanghai.key.security_id_source = Bytes("101");
    shanghai.key.security_id = Bytes("600001");
    shanghai.quantity_unit = market::QuantityUnitV1::kShare;
    shanghai.security_type = market::SecurityTypeV1::kEquity;
    shanghai.asset_scope = market::AssetScopeV1::kDocumentedCore;
    entries.push_back(std::move(shanghai));

    market::InstrumentRegistryEntryV1 shenzhen{};
    shenzhen.instrument_id = kShenzhenInstrumentId;
    shenzhen.key.market = market::MarketV1::kShenzhen;
    shenzhen.key.security_id_source = Bytes("102");
    shenzhen.key.security_id = Bytes("000002");
    shenzhen.quantity_unit = market::QuantityUnitV1::kShare;
    shenzhen.security_type = market::SecurityTypeV1::kEquity;
    shenzhen.asset_scope = market::AssetScopeV1::kDocumentedCore;
    entries.push_back(std::move(shenzhen));

    market::InstrumentRegistryEntryV1 unobserved{};
    unobserved.instrument_id = kUnobservedInstrumentId;
    unobserved.key.market = market::MarketV1::kShanghai;
    unobserved.key.security_id_source = Bytes("101");
    unobserved.key.security_id = Bytes("600003");
    unobserved.quantity_unit = market::QuantityUnitV1::kShare;
    unobserved.security_type = market::SecurityTypeV1::kEquity;
    unobserved.asset_scope = market::AssetScopeV1::kDocumentedCore;
    entries.push_back(std::move(unobserved));

    std::unique_ptr<market::InstrumentRegistryV1> registry;
    const market::InstrumentRegistryCreateErrorV1 error =
        market::InstrumentRegistryV1::Create(
            71U, entries, &registry);
    return error == market::InstrumentRegistryCreateErrorV1::kNone
               ? std::move(registry)
               : nullptr;
}

class MarketFixture final {
public:
    [[nodiscard]] bool Initialize() {
        registry = MakeRegistry();
        if (!Expect(registry != nullptr, "create IPC fixture registry")) {
            return false;
        }

        market::IntradayInstrumentStoreConfigV1 config{};
        config.segment_target_bytes =
            market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
        config.maximum_session_records = 64U;
        config.maximum_session_accounted_bytes = 4U * 1024U * 1024U;
        config.maximum_records_per_batch = 64U;
        config.coverage_from_open = true;
        const market::IntradayInstrumentStoreCreateErrorV1 error =
            market::IntradayInstrumentStoreV1::Create(
                config,
                1U,
                kSourceStreamIds,
                registry.get(),
                &store);
        if (!Expect(
                error ==
                    market::IntradayInstrumentStoreCreateErrorV1::kNone &&
                    store != nullptr,
                "create IPC fixture store")) {
            return false;
        }
        return BuildRecords();
    }

    [[nodiscard]] std::size_t Ordinal(
        std::uint32_t instrument_id) const {
        return registry->LookupById(instrument_id).registry_ordinal;
    }

    [[nodiscard]] bool BuildKLineGenerations(
        common::Identity128 run_id,
        std::shared_ptr<const market::RealtimeKLineGenerationV1>*
            first_output,
        std::shared_ptr<const market::RealtimeKLineGenerationV1>*
            second_output) const {
        if (first_output == nullptr || second_output == nullptr) {
            return false;
        }
        first_output->reset();
        second_output->reset();

        market::RealtimeHistoryRuntimeConfigV1 config{};
        config.source_stream_ids = kSourceStreamIds;
        config.worker_count = 1U;
        config.queue_capacity_per_source_worker = 16U;
        config.registry = registry.get();
        config.intraday_store.segment_target_bytes =
            market::kIntradayInstrumentStoreMinimumSegmentBytesV1;
        config.intraday_store.maximum_session_records = 8U;
        config.intraday_store.maximum_session_accounted_bytes =
            1U << 20U;
        config.intraday_store.maximum_records_per_batch = 8U;
        config.intraday_store.coverage_from_open = true;
        config.kline.trade_date = kTradeDate;
        config.kline.windows = {{
            60'000U,
            60U * market::kKLineNanosecondsPerSecondV1,
        }};

        std::unique_ptr<market::RealtimeHistoryRuntimeV1> runtime;
        if (!Expect(
                market::RealtimeHistoryRuntimeV1::Create(
                    config, &runtime) ==
                        market::RealtimeHistoryCreateErrorV1::kNone &&
                    runtime != nullptr,
                "create IPC KLine fixture runtime")) {
            return false;
        }

        std::array<market::RealtimeSourceWatermarkV1, 4U> sources{};
        for (std::size_t index = 0U; index < sources.size(); ++index) {
            sources[index].source_stream_id =
                kSourceStreamIds[index];
            sources[index].sequence_exclusive =
                index == 3U ? 2U : 1U;
        }
        market::RealtimeHistoryWatermarkV1 watermark{};
        bool ok = Expect(
            market::BuildRealtimeHistoryWatermarkV1(
                run_id,
                1U,
                kTradeDate,
                2U,
                40'000U,
                *registry,
                sources,
                &watermark) ==
                market::RealtimeHistoryWatermarkErrorV1::kNone,
            "build IPC KLine fixture watermark");
        ok &= Expect(
            runtime->BeginGeneration(watermark) ==
                market::RealtimeHistoryGenerationErrorV1::kNone,
            "begin IPC KLine fixture generation");

        market::ShenzhenTransactionV1 transaction{};
        ok &= Expect(
            FillCommon(
                &transaction.common,
                market::MarketEventKindV1::kShenzhenTransaction,
                market::MarketV1::kShenzhen,
                3U,
                1U,
                1U,
                kShenzhenInstrumentId),
            "build IPC KLine fixture transaction common");
        transaction.application_sequence = 901;
        transaction.channel = 44U;
        transaction.fields.action = market::TickActionV1::kTrade;
        transaction.fields.price =
            Decimal(2236, 2'236'000, 3U);
        transaction.fields.quantity = Quantity(103U, 0U);
        transaction.fields.validity_bitmap =
            market::kTickPriceValidV1 |
            market::kTickQuantityValidV1 |
            market::kTickExchangeTimeValidV1;
        market::DecodedMarketEventV1 decoded(std::move(transaction));
        market::KLineTradeV1 projected_trade{};
        ok &= Expect(
            market::ProjectKLineTradeV1(
                decoded, 1U, &projected_trade) ==
                    market::KLineTradeProjectionV1::kTrade,
            "project IPC KLine fixture transaction");
        std::optional<market::RealtimeHistoryEventInputV1> input =
            market::RealtimeHistoryEventInputV1::Create(
                3U, 1U, std::move(decoded), 1U);
        ok &= Expect(
            input.has_value(),
            "create IPC KLine fixture input");
        if (input.has_value()) {
            ok &= Expect(
                runtime->TrySubmit(std::move(*input)) ==
                    market::RealtimeHistorySubmitErrorV1::kNone,
                "submit IPC KLine fixture transaction");
        }
        for (std::uint8_t source = 0U; source < 4U; ++source) {
            ok &= Expect(
                runtime->SealSource(source, 1U) ==
                    market::RealtimeHistoryGenerationErrorV1::kNone,
                "seal IPC KLine fixture source");
        }
        std::shared_ptr<
            const market::IntradayInstrumentStoreGenerationV1>
            store_generation;
        if (ok) {
            const market::RealtimeHistoryGenerationErrorV1 wait_error =
                runtime->WaitForGeneration(
                    1U,
                    std::chrono::seconds(2),
                    &store_generation,
                    first_output);
            ok &= Expect(
                wait_error ==
                        market::RealtimeHistoryGenerationErrorV1::kNone &&
                    store_generation != nullptr &&
                    *first_output != nullptr,
                "build first IPC KLine fixture generation");
            if (!ok) {
                std::cerr
                    << "KLine fixture generation error: "
                    << market::RealtimeHistoryGenerationErrorNameV1(
                           wait_error)
                    << '\n';
            }
        }
        market::RealtimeHistoryWatermarkV1 second_watermark{};
        if (ok) {
            ok &= Expect(
                market::BuildRealtimeHistoryWatermarkV1(
                    run_id,
                    2U,
                    kTradeDate,
                    2U,
                    50'000U,
                    *registry,
                    sources,
                    &second_watermark) ==
                    market::RealtimeHistoryWatermarkErrorV1::kNone,
                "build second IPC KLine fixture watermark");
            ok &= Expect(
                runtime->BeginGeneration(second_watermark) ==
                    market::RealtimeHistoryGenerationErrorV1::kNone,
                "begin second IPC KLine fixture generation");
            for (std::uint8_t source = 0U; source < 4U; ++source) {
                ok &= Expect(
                    runtime->SealSource(source, 2U) ==
                        market::RealtimeHistoryGenerationErrorV1::kNone,
                    "seal second IPC KLine fixture source");
            }
        }
        std::shared_ptr<
            const market::IntradayInstrumentStoreGenerationV1>
            second_store_generation;
        if (ok) {
            const market::RealtimeHistoryGenerationErrorV1 wait_error =
                runtime->WaitForGeneration(
                    2U,
                    std::chrono::seconds(2),
                    &second_store_generation,
                    second_output);
            ok &= Expect(
                wait_error ==
                        market::RealtimeHistoryGenerationErrorV1::kNone &&
                    second_store_generation != nullptr &&
                    *second_output != nullptr,
                "build second IPC KLine fixture generation");
        }
        runtime->StopAndDrain();
        return ok;
    }

    std::unique_ptr<market::InstrumentRegistryV1> registry;
    std::unique_ptr<market::IntradayInstrumentStoreV1> store;
    std::array<const market::RealtimeHistoryRecordV1*, kRecordCount>
        records{};

private:
    [[nodiscard]] bool FillCommon(
        market::DecodedMarketCommonV1* common_record,
        market::MarketEventKindV1 kind,
        market::MarketV1 venue,
        std::uint8_t source_slot,
        std::uint64_t source_sequence,
        std::uint64_t ingress_sequence,
        std::uint32_t instrument_id) const {
        if (common_record == nullptr || source_slot >=
                                            kSourceStreamIds.size()) {
            return false;
        }
        const market::InstrumentRegistryLookupResultV1 lookup =
            registry->LookupById(instrument_id);
        if (!lookup.known()) {
            return false;
        }
        common_record->kind = kind;
        common_record->market = venue;
        common_record->origin.source_stream_id =
            kSourceStreamIds[source_slot];
        common_record->origin.trade_date = kTradeDate;
        common_record->origin.source_sequence = source_sequence;
        common_record->origin.vendor_local_time_raw = 93000000U;
        common_record->origin.vendor_sequence_id =
            10'000U + ingress_sequence;
        common_record->origin.recv_realtime_ns =
            static_cast<std::int64_t>(20'000U + ingress_sequence);
        common_record->origin.recv_monotonic_ns =
            static_cast<std::int64_t>(30'000U + ingress_sequence);
        common_record->exchange_time.raw_hhmmssmmm = 93000000U;
        common_record->exchange_time.nanoseconds_since_midnight =
            34'200'000'000'000U + ingress_sequence;
        common_record->exchange_time.unix_nanoseconds =
            1'785'115'800'000'000'000LL +
            static_cast<std::int64_t>(ingress_sequence);
        common_record->exchange_time.valid = true;
        common_record->exchange_time.unix_nanoseconds_valid = true;
        common_record->instrument_id = instrument_id;
        common_record->registry_ordinal = lookup.registry_ordinal;
        common_record->quantity_unit = lookup.quantity_unit;
        common_record->security_type = lookup.security_type;
        common_record->asset_scope = lookup.asset_scope;
        common_record->quality_flags = 0x12U;
        common_record->market_notices = 0x34U;
        return true;
    }

    template <typename Event>
    [[nodiscard]] bool Append(
        Event event,
        std::uint8_t source_slot,
        std::uint64_t ingress_sequence,
        std::uint64_t tick_stream_sequence,
        RecordIndex index) {
        market::DecodedMarketEventV1 decoded(std::move(event));
        std::optional<market::RealtimeHistoryEventInputV1> input =
            market::RealtimeHistoryEventInputV1::Create(
                source_slot,
                ingress_sequence,
                std::move(decoded),
                tick_stream_sequence);
        if (!Expect(input.has_value(), "create IPC fixture input")) {
            return false;
        }
        market::InstrumentRouteTokenV1 route{};
        if (!Expect(
                store->ResolveRouteToken(
                    input->registry_ordinal(),
                    input->instrument_id(),
                    &route) ==
                    market::IntradayInstrumentStoreQueryErrorV1::kNone,
                "resolve IPC fixture route")) {
            return false;
        }
        const market::RealtimeHistoryRecordV1* appended = nullptr;
        const market::IntradayInstrumentStoreAppendErrorV1 error =
            store->Append(
                route.worker, route, std::move(*input), &appended);
        if (!Expect(
                error ==
                    market::IntradayInstrumentStoreAppendErrorV1::kNone &&
                    appended != nullptr,
                "append IPC fixture record")) {
            return false;
        }
        records[index] = appended;
        return true;
    }

    [[nodiscard]] bool BuildRecords() {
        market::ShanghaiSnapshotV1 shanghai_snapshot{};
        if (!FillCommon(
                &shanghai_snapshot.common,
                market::MarketEventKindV1::kShanghaiSnapshot,
                market::MarketV1::kShanghai,
                0U,
                1U,
                1U,
                kShanghaiInstrumentId)) {
            return false;
        }
        shanghai_snapshot.image_status = 7;
        shanghai_snapshot.trade_count = 41U;
        shanghai_snapshot.last_price = Decimal(1234, 1'234'000, 3U);
        shanghai_snapshot.trade_volume = Quantity(900U, 0U);
        shanghai_snapshot.book.actual_bid_depth = 1U;
        shanghai_snapshot.book.retained_bid_depth = 1U;
        shanghai_snapshot.book.bids[0U].price =
            Decimal(1233, 1'233'000, 3U);
        shanghai_snapshot.book.bids[0U].quantity =
            Quantity(100U, 0U);
        if (!Append(
                std::move(shanghai_snapshot),
                0U,
                1U,
                0U,
                kShanghaiSnapshot)) {
            return false;
        }

        market::ShanghaiTickV1 shanghai_tick{};
        if (!FillCommon(
                &shanghai_tick.common,
                market::MarketEventKindV1::kShanghaiTick,
                market::MarketV1::kShanghai,
                1U,
                1U,
                2U,
                kShanghaiInstrumentId)) {
            return false;
        }
        shanghai_tick.business_index = 501;
        shanghai_tick.channel = 9;
        shanghai_tick.raw_type = "A";
        shanghai_tick.raw_tick_flag = "B";
        shanghai_tick.fields.action = market::TickActionV1::kAdd;
        shanghai_tick.fields.side = market::SideV1::kBuy;
        shanghai_tick.fields.order_type =
            market::OrderTypeV1::kLimit;
        shanghai_tick.fields.price = Decimal(1235, 1'235'000, 3U);
        shanghai_tick.fields.quantity = Quantity(101U, 0U);
        shanghai_tick.fields.primary_order_id = 5001;
        shanghai_tick.fields.validity_bitmap =
            market::kTickPriceValidV1 |
            market::kTickQuantityValidV1 |
            market::kTickPrimaryOrderIdValidV1;
        if (!Append(
                std::move(shanghai_tick),
                1U,
                2U,
                1U,
                kShanghaiTick)) {
            return false;
        }

        market::ShenzhenSnapshotV1 shenzhen_snapshot{};
        if (!FillCommon(
                &shenzhen_snapshot.common,
                market::MarketEventKindV1::kShenzhenSnapshot,
                market::MarketV1::kShenzhen,
                2U,
                1U,
                3U,
                kShenzhenInstrumentId)) {
            return false;
        }
        shenzhen_snapshot.channel = 22U;
        shenzhen_snapshot.trade_count = 52;
        shenzhen_snapshot.last_price =
            Decimal(2234, 2'234'000, 3U);
        shenzhen_snapshot.volume = Quantity(901U, 0U);
        shenzhen_snapshot.open_interest = Quantity(77U, 0U);
        if (!Append(
                std::move(shenzhen_snapshot),
                2U,
                3U,
                0U,
                kShenzhenSnapshot)) {
            return false;
        }

        market::ShenzhenOrderV1 shenzhen_order{};
        if (!FillCommon(
                &shenzhen_order.common,
                market::MarketEventKindV1::kShenzhenOrder,
                market::MarketV1::kShenzhen,
                3U,
                1U,
                4U,
                kShenzhenInstrumentId)) {
            return false;
        }
        shenzhen_order.channel = 33U;
        shenzhen_order.application_sequence = 701;
        shenzhen_order.raw_side = 66;
        shenzhen_order.raw_order_type = 77;
        shenzhen_order.fields.action = market::TickActionV1::kAdd;
        shenzhen_order.fields.side = market::SideV1::kSell;
        shenzhen_order.fields.price = Decimal(2235, 2'235'000, 3U);
        shenzhen_order.fields.quantity = Quantity(102U, 0U);
        shenzhen_order.fields.primary_order_id = 7001;
        shenzhen_order.fields.validity_bitmap =
            market::kTickPriceValidV1 |
            market::kTickQuantityValidV1 |
            market::kTickPrimaryOrderIdValidV1;
        if (!Append(
                std::move(shenzhen_order),
                3U,
                4U,
                2U,
                kShenzhenOrder)) {
            return false;
        }

        market::ShenzhenTransactionV1 shenzhen_transaction{};
        if (!FillCommon(
                &shenzhen_transaction.common,
                market::MarketEventKindV1::kShenzhenTransaction,
                market::MarketV1::kShenzhen,
                3U,
                2U,
                5U,
                kShenzhenInstrumentId)) {
            return false;
        }
        shenzhen_transaction.channel = 44U;
        shenzhen_transaction.application_sequence = 801;
        shenzhen_transaction.raw_execution_type = 88;
        shenzhen_transaction.fields.action =
            market::TickActionV1::kTrade;
        shenzhen_transaction.fields.aggressor =
            market::AggressorV1::kBuy;
        shenzhen_transaction.fields.price =
            Decimal(2236, 2'236'000, 3U);
        shenzhen_transaction.fields.quantity = Quantity(103U, 0U);
        shenzhen_transaction.fields.buy_order_id = 8001;
        shenzhen_transaction.fields.sell_order_id = 8002;
        shenzhen_transaction.fields.validity_bitmap =
            market::kTickPriceValidV1 |
            market::kTickQuantityValidV1 |
            market::kTickBuyOrderIdValidV1 |
            market::kTickSellOrderIdValidV1;
        return Append(
            std::move(shenzhen_transaction),
            3U,
            5U,
            3U,
            kShenzhenTransaction);
    }
};

bool TestWireProjection(const MarketFixture& fixture) {
    bool ok = true;

    ipc::RealtimeWireSnapshotPayloadV1 snapshot{};
    ok &= Expect(
        ipc::ProjectSnapshotWireV1(
            *fixture.records[kShanghaiSnapshot],
            fixture.Ordinal(kShanghaiInstrumentId),
            &snapshot),
        "project Shanghai snapshot");
    ok &= Expect(
        snapshot.common.instrument_id == kShanghaiInstrumentId &&
            snapshot.common.ingress_sequence == 1U &&
            snapshot.common.tick_stream_sequence == 0U &&
            snapshot.image_status == 7 &&
            snapshot.trade_count == 41 &&
            snapshot.last_price.normalized_p6 == 1'234'000 &&
            snapshot.bids[0U].quantity.raw == 100,
        "Shanghai snapshot wire fields");

    snapshot = {};
    ok &= Expect(
        ipc::ProjectSnapshotWireV1(
            *fixture.records[kShenzhenSnapshot],
            fixture.Ordinal(kShenzhenInstrumentId),
            &snapshot),
        "project Shenzhen snapshot");
    ok &= Expect(
        snapshot.common.instrument_id == kShenzhenInstrumentId &&
            snapshot.channel == 22U && snapshot.trade_count == 52 &&
            snapshot.last_price.normalized_p6 == 2'234'000 &&
            snapshot.open_interest.raw == 77,
        "Shenzhen snapshot wire fields");

    ipc::RealtimeWireTickPayloadV1 tick{};
    ok &= Expect(
        ipc::ProjectTickWireV1(
            *fixture.records[kShanghaiTick],
            fixture.Ordinal(kShanghaiInstrumentId),
            &tick),
        "project Shanghai tick");
    ok &= Expect(
        tick.common.tick_stream_sequence == 1U &&
            tick.native_event_sequence == 501 && tick.channel == 9 &&
            tick.raw_type_length == 1U && tick.raw_type[0U] == 'A' &&
            tick.raw_tick_flag_length == 1U &&
            tick.raw_tick_flag[0U] == 'B' &&
            tick.primary_order_id == 5001,
        "Shanghai tick wire fields");

    tick = {};
    ok &= Expect(
        ipc::ProjectTickWireV1(
            *fixture.records[kShenzhenOrder],
            fixture.Ordinal(kShenzhenInstrumentId),
            &tick),
        "project Shenzhen order");
    ok &= Expect(
        tick.common.tick_stream_sequence == 2U &&
            tick.native_event_sequence == 701 && tick.channel == 33 &&
            tick.source_raw_code_1 == 66 &&
            tick.source_raw_code_2 == 77 &&
            tick.action ==
                static_cast<std::uint8_t>(
                    market::TickActionV1::kAdd),
        "Shenzhen order wire fields");

    tick = {};
    ok &= Expect(
        ipc::ProjectTickWireV1(
            *fixture.records[kShenzhenTransaction],
            fixture.Ordinal(kShenzhenInstrumentId),
            &tick),
        "project Shenzhen transaction");
    ok &= Expect(
        tick.common.tick_stream_sequence == 3U &&
            tick.native_event_sequence == 801 && tick.channel == 44 &&
            tick.source_raw_code_1 == 88 &&
            tick.action ==
                static_cast<std::uint8_t>(
                    market::TickActionV1::kTrade) &&
            tick.buy_order_id == 8001 && tick.sell_order_id == 8002,
        "Shenzhen transaction wire fields");

    ipc::RealtimeWireTickPayloadV1 unchanged{};
    unchanged.common.instrument_id = 0xfeedU;
    ok &= Expect(
        !ipc::ProjectTickWireV1(
            *fixture.records[kShanghaiSnapshot],
            fixture.Ordinal(kShanghaiInstrumentId),
            &unchanged) &&
            unchanged.common.instrument_id == 0xfeedU,
        "failed projection leaves destination unchanged");
    return ok;
}

int ReceiveSessionFd(
    const std::filesystem::path& socket_path,
    ipc::RealtimeControlResponseV1* response_output) {
    if (response_output == nullptr) {
        return -1;
    }
    UniqueFd client(
        ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0));
    if (!Expect(client.get() >= 0, "create IPC control client")) {
        return -1;
    }
    timeval timeout{};
    timeout.tv_sec = 2;
    if (!Expect(
            ::setsockopt(
                client.get(),
                SOL_SOCKET,
                SO_RCVTIMEO,
                &timeout,
                static_cast<socklen_t>(sizeof(timeout))) == 0 &&
                ::setsockopt(
                    client.get(),
                    SOL_SOCKET,
                    SO_SNDTIMEO,
                    &timeout,
                    static_cast<socklen_t>(sizeof(timeout))) == 0,
            "configure IPC control client timeout")) {
        return -1;
    }

    const std::string native_path = socket_path.string();
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    if (!Expect(
            native_path.size() < sizeof(address.sun_path),
            "IPC socket path fits sockaddr_un")) {
        return -1;
    }
    std::memcpy(
        address.sun_path, native_path.c_str(), native_path.size() + 1U);
    const socklen_t address_bytes = static_cast<socklen_t>(
        offsetof(sockaddr_un, sun_path) + native_path.size() + 1U);
    if (!Expect(
            ::connect(
                client.get(),
                reinterpret_cast<const sockaddr*>(&address),
                address_bytes) == 0,
            "connect IPC control client")) {
        return -1;
    }

    ipc::RealtimeControlRequestV1 request{};
    request.magic = ipc::kRealtimeControlMagicV1;
    request.protocol_major = ipc::kRealtimeWireMajorV1;
    request.protocol_minor = ipc::kRealtimeWireMinorV1;
    request.opcode = static_cast<std::uint16_t>(
        ipc::RealtimeControlOpcodeV1::kGetSession);
    request.message_bytes =
        static_cast<std::uint32_t>(sizeof(request));
    request.request_id = 0x1020304050607080ULL;
    const ssize_t sent =
        ::send(client.get(), &request, sizeof(request), MSG_NOSIGNAL);
    if (!Expect(
            sent == static_cast<ssize_t>(sizeof(request)),
            "send IPC control request")) {
        return -1;
    }

    ipc::RealtimeControlResponseV1 response{};
    iovec vector{};
    vector.iov_base = &response;
    vector.iov_len = sizeof(response);
    std::array<std::byte, CMSG_SPACE(sizeof(int))> control{};
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1U;
    message.msg_control = control.data();
    message.msg_controllen = control.size();
    const ssize_t received =
        ::recvmsg(client.get(), &message, MSG_CMSG_CLOEXEC);
    if (!Expect(
            received == static_cast<ssize_t>(sizeof(response)) &&
                (message.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) == 0,
            "receive IPC control response")) {
        return -1;
    }

    int received_fd = -1;
    for (cmsghdr* header = CMSG_FIRSTHDR(&message);
         header != nullptr;
         header = CMSG_NXTHDR(&message, header)) {
        if (header->cmsg_level == SOL_SOCKET &&
            header->cmsg_type == SCM_RIGHTS &&
            header->cmsg_len == CMSG_LEN(sizeof(int))) {
            std::memcpy(
                &received_fd, CMSG_DATA(header), sizeof(received_fd));
            break;
        }
    }
    if (!Expect(received_fd >= 0, "receive SCM_RIGHTS data fd")) {
        return -1;
    }
    *response_output = response;
    return received_fd;
}

bool StatusEquals(std::uint8_t actual, int expected) {
    return static_cast<int>(actual) == expected;
}

#if defined(L2FLOW_IPC_E2E_PYTHON_EXECUTABLE)
bool RunPythonE2EProbe(
    const std::filesystem::path& socket_path) {
    const std::string native_socket_path = socket_path.string();
    const pid_t child = ::fork();
    if (!Expect(child >= 0, "fork Python IPC E2E probe")) {
        return false;
    }
    if (child == 0) {
        ::execl(
            L2FLOW_IPC_E2E_PYTHON_EXECUTABLE,
            L2FLOW_IPC_E2E_PYTHON_EXECUTABLE,
            "-B",
            L2FLOW_IPC_E2E_PYTHON_PROBE,
            native_socket_path.c_str(),
            L2FLOW_IPC_E2E_NATIVE_READER,
            L2FLOW_IPC_E2E_PYTHON_SOURCE,
            static_cast<char*>(nullptr));
        constexpr char message[] =
            "FAIL: exec Python IPC E2E probe\n";
        const ssize_t reported =
            ::write(STDERR_FILENO, message, sizeof(message) - 1U);
        if (reported < 0) {
            ::_exit(127);
        }
        ::_exit(127);
    }

    int status = 0;
    pid_t waited = -1;
    do {
        waited = ::waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    return Expect(
        waited == child && WIFEXITED(status) &&
            WEXITSTATUS(status) == 0,
        "independent Python/native IPC E2E probe");
}
#endif

bool TestLatestBatches(
    l2flow_shm_reader_v1* reader,
    const MarketFixture& fixture) {
    bool ok = true;
    constexpr std::array<std::uint32_t, 6U> instrument_ids{
        kShanghaiInstrumentId,
        kShenzhenInstrumentId,
        kUnobservedInstrumentId,
        kUnknownInstrumentId,
        0U,
        kShanghaiInstrumentId,
    };

    std::array<ipc::RealtimeWireSnapshotPayloadV1, 6U> snapshots{};
    std::array<std::uint8_t, 6U> snapshot_statuses{};
    const int snapshot_error =
        l2flow_shm_reader_latest_snapshots_v1(
            reader,
            instrument_ids.data(),
            instrument_ids.size(),
            snapshots.data(),
            sizeof(snapshots[0U]),
            snapshot_statuses.data());
    ok &= Expect(
        snapshot_error == L2FLOW_SHM_READER_OK_V1,
        "read latest snapshot batch");
    ok &= Expect(
        StatusEquals(
            snapshot_statuses[0U], L2FLOW_LATEST_AVAILABLE_V1) &&
            StatusEquals(
                snapshot_statuses[1U],
                L2FLOW_LATEST_AVAILABLE_V1) &&
            StatusEquals(
                snapshot_statuses[2U],
                L2FLOW_LATEST_NOT_YET_OBSERVED_V1) &&
            StatusEquals(
                snapshot_statuses[3U],
                L2FLOW_LATEST_UNKNOWN_INSTRUMENT_V1) &&
            StatusEquals(
                snapshot_statuses[4U],
                L2FLOW_LATEST_INVALID_INSTRUMENT_ID_V1) &&
            StatusEquals(
                snapshot_statuses[5U], L2FLOW_LATEST_AVAILABLE_V1),
        "latest snapshot per-item statuses");
    ok &= Expect(
        snapshots[0U].common.instrument_id ==
                kShanghaiInstrumentId &&
            snapshots[1U].common.instrument_id ==
                kShenzhenInstrumentId &&
            snapshots[5U].common.ingress_sequence ==
                snapshots[0U].common.ingress_sequence,
        "latest snapshot batch preserves order and duplicates");

    std::array<ipc::RealtimeWireTickPayloadV1, 6U> ticks{};
    std::array<std::uint8_t, 6U> tick_statuses{};
    const int tick_error = l2flow_shm_reader_latest_ticks_v1(
        reader,
        instrument_ids.data(),
        instrument_ids.size(),
        ticks.data(),
        sizeof(ticks[0U]),
        tick_statuses.data());
    ok &= Expect(
        tick_error == L2FLOW_SHM_READER_OK_V1,
        "read latest mixed-tick batch");
    ok &= Expect(
        StatusEquals(
            tick_statuses[0U], L2FLOW_LATEST_AVAILABLE_V1) &&
            StatusEquals(
                tick_statuses[1U], L2FLOW_LATEST_AVAILABLE_V1) &&
            StatusEquals(
                tick_statuses[2U],
                L2FLOW_LATEST_NOT_YET_OBSERVED_V1) &&
            StatusEquals(
                tick_statuses[3U],
                L2FLOW_LATEST_UNKNOWN_INSTRUMENT_V1) &&
            StatusEquals(
                tick_statuses[4U],
                L2FLOW_LATEST_INVALID_INSTRUMENT_ID_V1) &&
            StatusEquals(
                tick_statuses[5U], L2FLOW_LATEST_AVAILABLE_V1),
        "latest mixed-tick per-item statuses");
    ok &= Expect(
        ticks[0U].common.event_kind ==
                static_cast<std::uint8_t>(
                    market::MarketEventKindV1::kShanghaiTick) &&
            ticks[0U].common.tick_stream_sequence == 1U &&
            ticks[1U].common.event_kind ==
                static_cast<std::uint8_t>(
                    market::MarketEventKindV1::
                        kShenzhenTransaction) &&
            ticks[1U].common.tick_stream_sequence == 3U &&
            ticks[5U].common.ingress_sequence ==
                ticks[0U].common.ingress_sequence,
        "latest tick retains mixed event semantics");

    constexpr std::array<std::uint32_t, 7U> kline_instruments{
        kShanghaiInstrumentId,
        kShenzhenInstrumentId,
        kUnobservedInstrumentId,
        kUnknownInstrumentId,
        0U,
        kShenzhenInstrumentId,
        kShenzhenInstrumentId,
    };
    constexpr std::array<std::uint32_t, 7U> kline_windows{
        60'000U,
        60'000U,
        60'000U,
        60'000U,
        60'000U,
        0U,
        1U,
    };
    std::array<ipc::RealtimeWireKLinePayloadV1, 7U> klines{};
    std::array<std::uint8_t, 7U> kline_statuses{};
    const int kline_error =
        l2flow_shm_reader_latest_klines_v1(
            reader,
            kline_instruments.data(),
            kline_windows.data(),
            kline_instruments.size(),
            klines.data(),
            sizeof(klines[0U]),
            kline_statuses.data());
    ok &= Expect(
        kline_error == L2FLOW_SHM_READER_OK_V1 &&
            StatusEquals(
                kline_statuses[0U],
                L2FLOW_LATEST_NOT_YET_OBSERVED_V1) &&
            StatusEquals(
                kline_statuses[1U],
                L2FLOW_LATEST_AVAILABLE_V1) &&
            StatusEquals(
                kline_statuses[2U],
                L2FLOW_LATEST_NOT_YET_OBSERVED_V1) &&
            StatusEquals(
                kline_statuses[3U],
                L2FLOW_LATEST_UNKNOWN_INSTRUMENT_V1) &&
            StatusEquals(
                kline_statuses[4U],
                L2FLOW_LATEST_INVALID_INSTRUMENT_ID_V1) &&
            StatusEquals(
                kline_statuses[5U],
                L2FLOW_LATEST_INVALID_WINDOW_ID_V1) &&
            StatusEquals(
                kline_statuses[6U],
                L2FLOW_LATEST_UNKNOWN_WINDOW_V1) &&
            klines[1U].generation == 1U &&
            klines[1U].instrument_id == kShenzhenInstrumentId &&
            klines[1U].window_id == 60'000U &&
            klines[1U].close_price_p6 == 2'236'000,
        "latest KLine batch preserves pair order and all statuses");

    ipc::RealtimeWireInstrumentV1 instrument{};
    std::array<std::uint8_t, 3U> source{};
    std::array<std::uint8_t, 6U> security_id{};
    std::size_t source_written = 0U;
    std::size_t id_written = 0U;
    const int instrument_error = l2flow_shm_reader_instrument_v1(
        reader,
        kShanghaiInstrumentId,
        &instrument,
        sizeof(instrument),
        source.data(),
        source.size(),
        &source_written,
        security_id.data(),
        security_id.size(),
        &id_written);
    ok &= Expect(
        instrument_error == L2FLOW_SHM_READER_OK_V1 &&
            instrument.instrument_id == kShanghaiInstrumentId &&
            source_written == source.size() &&
            id_written == security_id.size() &&
            std::memcmp(source.data(), "101", source.size()) == 0 &&
            std::memcmp(
                security_id.data(),
                "600001",
                security_id.size()) == 0 &&
            instrument.market ==
                static_cast<std::uint8_t>(
                    market::MarketV1::kShanghai) &&
            instrument.quantity_unit ==
                static_cast<std::uint8_t>(
                    fixture.registry
                        ->LookupById(kShanghaiInstrumentId)
                        .quantity_unit),
        "read registry row and opaque keys");
    return ok;
}

bool TestServiceAndReader(const MarketFixture& fixture) {
    bool ok = true;
    ScopedTempDirectory directory;
    if (!Expect(directory.valid(), "create private IPC temp directory")) {
        return false;
    }
    struct stat directory_stat {};
    ok &= Expect(
        ::stat(directory.path().c_str(), &directory_stat) == 0 &&
            S_ISDIR(directory_stat.st_mode) &&
            (directory_stat.st_mode & 0777) == 0700,
        "IPC temp directory is mode 0700");
    const std::filesystem::path socket_path =
        directory.path() / "control.sock";

    common::Identity128 service_run_id{};
    service_run_id[0U] = std::byte{0x5aU};
    ipc::RealtimeSharedServiceConfigV1 config{};
    config.run_id = service_run_id;
    config.session_epoch = 19U;
    config.trade_date = kTradeDate;
    config.registry = fixture.registry.get();
    config.kline_windows = {{
        60'000U,
        60U * market::kKLineNanosecondsPerSecondV1,
    }};
    config.tick_ring_capacity = 2U;
    config.maximum_mapping_bytes = 16U * 1024U * 1024U;
    config.control_socket_path = socket_path;

    std::shared_ptr<ipc::RealtimeSharedMarketServiceV1> service;
    int system_error = 0;
    const ipc::RealtimeSharedServiceCreateErrorV1 create_error =
        ipc::RealtimeSharedMarketServiceV1::Create(
            std::move(config), &service, &system_error);
    if (!Expect(
            create_error ==
                    ipc::RealtimeSharedServiceCreateErrorV1::kNone &&
                service != nullptr && system_error == 0,
            "create shared market service")) {
        std::cerr << "create error: "
                  << ipc::RealtimeSharedServiceCreateErrorNameV1(
                         create_error)
                  << ", errno=" << system_error << '\n';
        return false;
    }

    struct stat socket_stat {};
    ok &= Expect(
        ::lstat(socket_path.c_str(), &socket_stat) == 0 &&
            S_ISSOCK(socket_stat.st_mode) &&
            (socket_stat.st_mode & 0777) == 0600,
        "Create binds a private UDS");
    ok &= Expect(
        service->Start(&system_error) && system_error == 0,
        "start shared market service");

    ipc::RealtimeControlResponseV1 response{};
    UniqueFd data_fd(ReceiveSessionFd(socket_path, &response));
    if (!Expect(data_fd.get() >= 0, "obtain shared-memory fd")) {
        service->StopControl();
        service.reset();
        return false;
    }
    ok &= Expect(
        response.magic == ipc::kRealtimeControlMagicV1 &&
            response.protocol_major == ipc::kRealtimeWireMajorV1 &&
            response.status == static_cast<std::uint16_t>(
                                   ipc::RealtimeControlStatusV1::kOk) &&
            response.request_id == 0x1020304050607080ULL &&
            response.session_epoch == 19U &&
            response.total_mapping_bytes == service->mapping_bytes(),
        "validate IPC control response");

    const int descriptor_flags = ::fcntl(data_fd.get(), F_GETFL);
    const int descriptor_fd_flags = ::fcntl(data_fd.get(), F_GETFD);
    std::byte attempted_write{0x7fU};
    errno = 0;
    const ssize_t write_result =
        ::pwrite(data_fd.get(), &attempted_write, 1U, 0);
    const int write_error = errno;
    ok &= Expect(
        descriptor_flags >= 0 &&
            (descriptor_flags & O_ACCMODE) == O_RDONLY &&
            descriptor_fd_flags >= 0 &&
            (descriptor_fd_flags & FD_CLOEXEC) != 0 &&
            write_result == -1 && write_error == EBADF,
        "SCM_RIGHTS fd is read-only and close-on-exec");

    ReaderHandle reader;
    const int open_error =
        l2flow_shm_reader_open_fd_v1(data_fd.get(), reader.output());
    if (!Expect(
            open_error == L2FLOW_SHM_READER_OK_V1 &&
                reader.get() != nullptr,
            "open native shared-memory reader")) {
        service->StopControl();
        service.reset();
        return false;
    }
    data_fd.Reset();

    l2flow_shm_session_info_v1 session{};
    ok &= Expect(
        l2flow_shm_reader_session_v1(reader.get(), &session) ==
                L2FLOW_SHM_READER_OK_V1 &&
            session.session_epoch == 19U &&
            session.trade_date == kTradeDate &&
            session.instrument_count == 3U &&
            session.window_count == 1U &&
            session.tick_ring_capacity == 2U &&
            (session.flags &
             ipc::kRealtimeHeaderKLineEnabledV1) != 0U &&
            session.heartbeat_monotonic_ns != 0U &&
            session.server_state ==
                static_cast<std::uint32_t>(
                    ipc::RealtimeServerStateV1::kActive),
        "read active IPC session metadata");
    const std::uint64_t initial_heartbeat =
        session.heartbeat_monotonic_ns;

    const auto publish = [&](RecordIndex index,
                             std::uint32_t instrument_id) {
        return service->PublishApplied(
            fixture.Ordinal(instrument_id), *fixture.records[index]);
    };
    ok &= Expect(
        publish(kShanghaiSnapshot, kShanghaiInstrumentId) &&
            publish(kShenzhenSnapshot, kShenzhenInstrumentId) &&
            publish(kShenzhenOrder, kShenzhenInstrumentId) &&
            publish(kShanghaiTick, kShanghaiInstrumentId),
        "publish snapshots and first two mixed ticks out of worker order");

    std::array<ipc::RealtimeWireTickPayloadV1, 4U> ring_records{};
    std::size_t written = 0U;
    std::uint64_t next_sequence = 0U;
    std::uint64_t observed_sequence = 0U;
    int read_error = l2flow_shm_reader_ticks_v1(
        reader.get(),
        1U,
        ring_records.data(),
        sizeof(ring_records[0U]),
        ring_records.size(),
        &written,
        &next_sequence,
        &observed_sequence);
    ok &= Expect(
        read_error == L2FLOW_SHM_READER_OK_V1 && written == 2U &&
            next_sequence == 3U && observed_sequence == 0U &&
            ring_records[0U].common.tick_stream_sequence == 1U &&
            ring_records[0U].common.event_kind ==
                static_cast<std::uint8_t>(
                    market::MarketEventKindV1::kShanghaiTick) &&
            ring_records[1U].common.tick_stream_sequence == 2U &&
            ring_records[1U].common.event_kind ==
                static_cast<std::uint8_t>(
                    market::MarketEventKindV1::kShenzhenOrder),
        "tick ring returns contiguous mixed ticks in order");

    ok &= Expect(
        publish(kShenzhenTransaction, kShenzhenInstrumentId),
        "publish third mixed tick");
    std::shared_ptr<const market::RealtimeKLineGenerationV1>
        kline_generation;
    std::shared_ptr<const market::RealtimeKLineGenerationV1>
        second_kline_generation;
    ok &= fixture.BuildKLineGenerations(
        service_run_id,
        &kline_generation,
        &second_kline_generation);
    ok &= Expect(
        kline_generation != nullptr &&
            service->PublishKLineGeneration(*kline_generation),
        "publish exact immutable KLine generation");

#if defined(L2FLOW_IPC_E2E_PYTHON_EXECUTABLE)
    ok &= RunPythonE2EProbe(socket_path);
#endif

    written = 99U;
    next_sequence = 99U;
    observed_sequence = 0U;
    read_error = l2flow_shm_reader_ticks_v1(
        reader.get(),
        1U,
        ring_records.data(),
        sizeof(ring_records[0U]),
        ring_records.size(),
        &written,
        &next_sequence,
        &observed_sequence);
    ok &= Expect(
        read_error == L2FLOW_SHM_READER_OVERRUN_V1 &&
            written == 0U && next_sequence == 1U &&
            observed_sequence == 2U,
        "tick ring reports overrun without advancing cursor");

    written = 0U;
    next_sequence = 0U;
    observed_sequence = 0U;
    read_error = l2flow_shm_reader_ticks_v1(
        reader.get(),
        2U,
        ring_records.data(),
        sizeof(ring_records[0U]),
        ring_records.size(),
        &written,
        &next_sequence,
        &observed_sequence);
    ok &= Expect(
        read_error == L2FLOW_SHM_READER_OK_V1 && written == 2U &&
            next_sequence == 4U && observed_sequence == 0U &&
            ring_records[0U].common.tick_stream_sequence == 2U &&
            ring_records[1U].common.tick_stream_sequence == 3U &&
            ring_records[1U].common.event_kind ==
                static_cast<std::uint8_t>(
                    market::MarketEventKindV1::
                        kShenzhenTransaction),
        "tick ring resumes from reported oldest sequence");

    ok &= TestLatestBatches(reader.get(), fixture);
    ok &= Expect(
        second_kline_generation != nullptr &&
            service->PublishKLineGeneration(
                *second_kline_generation),
        "publish the next immutable KLine generation into the "
        "inactive table");
    std::array<std::uint32_t, 2U> kline_id{
        kShenzhenInstrumentId,
        kUnobservedInstrumentId};
    std::array<std::uint32_t, 2U> kline_window{
        60'000U, 60'000U};
    std::array<ipc::RealtimeWireKLinePayloadV1, 2U> latest_kline{};
    std::array<std::uint8_t, 2U> latest_kline_status{};
    ok &= Expect(
        l2flow_shm_reader_latest_klines_v1(
            reader.get(),
            kline_id.data(),
            kline_window.data(),
            kline_id.size(),
            latest_kline.data(),
            sizeof(latest_kline[0U]),
            latest_kline_status.data()) ==
                L2FLOW_SHM_READER_OK_V1 &&
            StatusEquals(
                latest_kline_status[0U],
                L2FLOW_LATEST_AVAILABLE_V1) &&
            StatusEquals(
                latest_kline_status[1U],
                L2FLOW_LATEST_NOT_YET_OBSERVED_V1) &&
            latest_kline[0U].generation == 2U &&
            latest_kline[0U].close_price_p6 == 2'236'000,
        "KLine reader flips atomically to the second completed "
        "generation");
    ok &= Expect(
        l2flow_shm_reader_session_v1(reader.get(), &session) ==
                L2FLOW_SHM_READER_OK_V1 &&
            session.tick_highest_published_sequence == 3U &&
            session.tick_contiguous_published_sequence == 3U &&
            session.kline_generation == 2U &&
            session.heartbeat_monotonic_ns >= initial_heartbeat,
        "session exposes complete tick prefix and monotonic heartbeat");

    ok &= Expect(
        second_kline_generation != nullptr &&
            !service->PublishKLineGeneration(
                *second_kline_generation) &&
            service->failed(),
        "duplicate KLine generation fails closed");
    ok &= Expect(
        l2flow_shm_reader_session_v1(reader.get(), &session) ==
                L2FLOW_SHM_READER_OK_V1 &&
            (session.flags & ipc::kRealtimeHeaderCoverageLostV1) != 0U &&
            session.server_state ==
                static_cast<std::uint32_t>(
                    ipc::RealtimeServerStateV1::kFailed),
        "coverage loss is visible in session metadata");

    std::array<std::uint32_t, 1U> one_id{kShanghaiInstrumentId};
    std::array<ipc::RealtimeWireSnapshotPayloadV1, 1U> one_snapshot{};
    std::array<std::uint8_t, 1U> one_status{};
    ok &= Expect(
        l2flow_shm_reader_latest_snapshots_v1(
            reader.get(),
            one_id.data(),
            one_id.size(),
            one_snapshot.data(),
            sizeof(one_snapshot[0U]),
            one_status.data()) ==
            L2FLOW_SHM_READER_UNAVAILABLE_V1,
        "latest reads fail closed after coverage loss");
    written = 99U;
    next_sequence = 99U;
    observed_sequence = 99U;
    ok &= Expect(
        l2flow_shm_reader_ticks_v1(
            reader.get(),
            2U,
            ring_records.data(),
            sizeof(ring_records[0U]),
            ring_records.size(),
            &written,
            &next_sequence,
            &observed_sequence) ==
                L2FLOW_SHM_READER_UNAVAILABLE_V1 &&
            written == 0U && next_sequence == 2U &&
            observed_sequence == 0U,
        "tick stream fails closed after coverage loss");
    ok &= Expect(
        !publish(kShanghaiSnapshot, kShanghaiInstrumentId),
        "failed service rejects further publication");

    reader.Reset();
    service->StopControl();
    service.reset();
    errno = 0;
    ok &= Expect(
        ::lstat(socket_path.c_str(), &socket_stat) == -1 &&
            errno == ENOENT,
        "service destruction removes its control socket");
    return ok;
}

bool TestServiceLifecycle(const MarketFixture& fixture) {
    ScopedTempDirectory directory;
    if (!Expect(
            directory.valid(),
            "create private IPC lifecycle directory")) {
        return false;
    }
    const auto make_config =
        [&](std::string_view socket_name) {
            common::Identity128 run_id{};
            run_id[0U] = std::byte{0x6bU};
            ipc::RealtimeSharedServiceConfigV1 config{};
            config.run_id = run_id;
            config.trade_date = kTradeDate;
            config.registry = fixture.registry.get();
            config.tick_ring_capacity = 2U;
            config.maximum_mapping_bytes = 16U * 1024U * 1024U;
            config.control_socket_path =
                directory.path() / socket_name;
            return config;
        };
    const auto create =
        [&](std::string_view socket_name,
            std::shared_ptr<ipc::RealtimeSharedMarketServiceV1>*
                output) {
            int system_error = 0;
            return ipc::RealtimeSharedMarketServiceV1::Create(
                       make_config(socket_name),
                       output,
                       &system_error) ==
                       ipc::RealtimeSharedServiceCreateErrorV1::kNone &&
                   *output != nullptr && system_error == 0;
        };

    bool ok = true;
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV1> stopped_before_start;
    ok &= Expect(
        create("prestart.sock", &stopped_before_start),
        "create pre-start lifecycle service");
    if (stopped_before_start != nullptr) {
        stopped_before_start->StopControl();
        int system_error = 0;
        ok &= Expect(
            !stopped_before_start->Start(&system_error) &&
                system_error == EINVAL &&
                stopped_before_start->failed(),
            "StopControl before Start is sticky and fail-closed");
    }
    stopped_before_start.reset();

    std::shared_ptr<ipc::RealtimeSharedMarketServiceV1> clean;
    ok &= Expect(
        create("clean.sock", &clean),
        "create clean lifecycle service");
    if (clean != nullptr) {
        int system_error = 0;
        ok &= Expect(
            clean->Start(&system_error) && system_error == 0,
            "start clean lifecycle service");
        clean->MarkDraining();
        ok &= Expect(
            clean->MarkStoppedClean(0U) && !clean->failed(),
            "empty mixed-tick session reaches STOPPED_CLEAN");
        clean->StopControl();
    }
    clean.reset();

    std::shared_ptr<ipc::RealtimeSharedMarketServiceV1> mismatch;
    ok &= Expect(
        create("mismatch.sock", &mismatch),
        "create terminal mismatch lifecycle service");
    if (mismatch != nullptr) {
        int system_error = 0;
        ok &= Expect(
            mismatch->Start(&system_error) && system_error == 0,
            "start terminal mismatch lifecycle service");
        mismatch->MarkDraining();
        ok &= Expect(
            !mismatch->MarkStoppedClean(1U) && mismatch->failed(),
            "terminal tick sequence mismatch fails closed");
    }
    return ok;
}

}  // namespace

int main() {
    MarketFixture fixture;
    if (!fixture.Initialize()) {
        return 1;
    }
    bool ok = true;
    ok &= TestWireProjection(fixture);
    ok &= TestServiceAndReader(fixture);
    ok &= TestServiceLifecycle(fixture);
    if (!ok) {
        return 1;
    }
    std::cout << "realtime IPC V1 tests passed\n";
    return 0;
}
