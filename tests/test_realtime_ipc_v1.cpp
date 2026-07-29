#include "l2flow/ipc/realtime_shared_service_v1.h"
#include "l2flow/ipc/realtime_shm_reader_c_v1.h"
#include "l2flow/ipc/realtime_history_wire_v1.h"
#include "l2flow/ipc/realtime_instrument_tick_delta_wire_v2.h"
#include "l2flow/ipc/realtime_wire_projection_v1.h"
#include "l2flow/ipc/realtime_wire_v1.h"
#include "l2flow/market/instrument_registry.h"
#include "l2flow/market/intraday_instrument_store_v1.h"
#include "l2flow/market/realtime_history_v1.h"

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
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
    kHistoryZeroSequenceTick,
    kHistoryLongRawTick,
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

class HistoryPageStageObserver final
    : public ipc::RealtimeHistoryPageStageObserverV1 {
public:
    void ObserveHistoryPageStageTiming(
        const ipc::RealtimeHistoryPageStageTimingV1& timing)
        noexcept override {
        const std::size_t index =
            next_.fetch_add(1U, std::memory_order_relaxed);
        if (index >= timings_.size()) {
            overflow_.store(true, std::memory_order_release);
            return;
        }
        timings_[index] = timing;
        completed_.fetch_add(1U, std::memory_order_release);
    }

    [[nodiscard]] std::size_t completed() const noexcept {
        return completed_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool overflow() const noexcept {
        return overflow_.load(std::memory_order_acquire);
    }

    [[nodiscard]] const ipc::RealtimeHistoryPageStageTimingV1&
    timing(std::size_t index) const noexcept {
        return timings_[index];
    }

private:
    std::array<ipc::RealtimeHistoryPageStageTimingV1, 8U> timings_{};
    std::atomic<std::size_t> next_{0U};
    std::atomic<std::size_t> completed_{0U};
    std::atomic<bool> overflow_{false};
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

    [[nodiscard]] bool BuildStoreGeneration(
        common::Identity128 run_id,
        std::uint64_t generation,
        std::uint64_t recv_monotonic_cut_ns,
        std::shared_ptr<const market::
                            IntradayInstrumentStoreGenerationV1>*
            output) {
        if (output == nullptr || generation == 0U ||
            recv_monotonic_cut_ns == 0U) {
            return false;
        }
        output->reset();
        std::array<market::RealtimeSourceWatermarkV1, 4U> sources{};
        for (std::size_t source = 0U; source < sources.size(); ++source) {
            sources[source].source_stream_id =
                kSourceStreamIds[source];
            sources[source].sequence_exclusive =
                source == 1U ? 4U : source == 3U ? 3U : 2U;
        }
        market::RealtimeHistoryWatermarkV1 watermark{};
        if (!Expect(
                market::BuildRealtimeHistoryWatermarkV1(
                    run_id,
                    generation,
                    kTradeDate,
                    8U,
                    recv_monotonic_cut_ns,
                    *registry,
                    sources,
                    &watermark) ==
                    market::RealtimeHistoryWatermarkErrorV1::kNone,
                "build IPC Store fixture watermark")) {
            return false;
        }
        std::unique_ptr<
            market::IntradayInstrumentStoreWorkerSliceV1>
            slice;
        if (!Expect(
                store->CaptureWorker(0U, generation, &slice) ==
                        market::
                            IntradayInstrumentStoreGenerationErrorV1::
                                kNone &&
                    slice != nullptr,
                "capture IPC Store fixture worker")) {
            return false;
        }
        std::vector<std::unique_ptr<
            market::IntradayInstrumentStoreWorkerSliceV1>>
            slices;
        slices.push_back(std::move(slice));
        return Expect(
            store->BuildGeneration(
                watermark, std::move(slices), output) ==
                    market::
                        IntradayInstrumentStoreGenerationErrorV1::
                            kNone &&
                *output != nullptr,
            "build exact immutable IPC Store generation");
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
        if (!Append(
                std::move(shenzhen_transaction),
                3U,
                5U,
                3U,
                kShenzhenTransaction)) {
            return false;
        }

        market::ShanghaiTickV1 zero_sequence_tick{};
        if (!FillCommon(
                &zero_sequence_tick.common,
                market::MarketEventKindV1::kShanghaiTick,
                market::MarketV1::kShanghai,
                1U,
                2U,
                6U,
                kShanghaiInstrumentId)) {
            return false;
        }
        zero_sequence_tick.business_index = 502;
        zero_sequence_tick.channel = 10;
        zero_sequence_tick.raw_type = "D";
        zero_sequence_tick.raw_tick_flag = "N";
        zero_sequence_tick.fields.action =
            market::TickActionV1::kCancel;
        if (!Append(
                std::move(zero_sequence_tick),
                1U,
                6U,
                0U,
                kHistoryZeroSequenceTick)) {
            return false;
        }

        market::ShanghaiTickV1 long_raw_tick{};
        if (!FillCommon(
                &long_raw_tick.common,
                market::MarketEventKindV1::kShanghaiTick,
                market::MarketV1::kShanghai,
                1U,
                3U,
                7U,
                kShanghaiInstrumentId)) {
            return false;
        }
        long_raw_tick.business_index = 503;
        long_raw_tick.channel = 11;
        long_raw_tick.raw_type = std::string(33U, 'T');
        long_raw_tick.raw_tick_flag = std::string(34U, 'F');
        long_raw_tick.fields.action = market::TickActionV1::kTrade;
        return Append(
            std::move(long_raw_tick),
            1U,
            7U,
            4U,
            kHistoryLongRawTick);
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

    std::uint32_t history_projection_flags =
        std::numeric_limits<std::uint32_t>::max();
    tick = {};
    ok &= Expect(
        ipc::ProjectHistoryTickWireV1(
            *fixture.records[kHistoryZeroSequenceTick],
            fixture.Ordinal(kShanghaiInstrumentId),
            &tick,
            &history_projection_flags),
        "history projection accepts standalone zero mixed-tick sequence");
    ok &= Expect(
        history_projection_flags == 0U &&
            tick.common.ingress_sequence == 6U &&
            tick.common.tick_stream_sequence == 0U &&
            tick.native_event_sequence == 502 &&
            tick.raw_type_length == 1U && tick.raw_type[0U] == 'D' &&
            tick.raw_tick_flag_length == 1U &&
            tick.raw_tick_flag[0U] == 'N',
        "zero-sequence history tick retains its CoreV1 fields");

    ipc::RealtimeWireTickPayloadV1 strict_unchanged{};
    strict_unchanged.common.instrument_id = 0xfeedU;
    ok &= Expect(
        !ipc::ProjectTickWireV1(
            *fixture.records[kHistoryZeroSequenceTick],
            fixture.Ordinal(kShanghaiInstrumentId),
            &strict_unchanged) &&
            strict_unchanged.common.instrument_id == 0xfeedU,
        "latest/ring projection still rejects zero mixed-tick sequence");

    history_projection_flags = 0U;
    tick = {};
    ok &= Expect(
        ipc::ProjectHistoryTickWireV1(
            *fixture.records[kHistoryLongRawTick],
            fixture.Ordinal(kShanghaiInstrumentId),
            &tick,
            &history_projection_flags),
        "history projection retains tick with oversized raw strings");
    ok &= Expect(
        history_projection_flags ==
                (ipc::kRealtimeHistoryRawTypeOmittedV1 |
                 ipc::kRealtimeHistoryRawTickFlagOmittedV1) &&
            tick.projection_flags == history_projection_flags &&
            tick.common.ingress_sequence == 7U &&
            tick.common.tick_stream_sequence == 4U &&
            tick.native_event_sequence == 503 &&
            tick.raw_type_length == 0U &&
            tick.raw_tick_flag_length == 0U &&
            tick.raw_type ==
                std::array<std::uint8_t, 32U>{} &&
            tick.raw_tick_flag ==
                std::array<std::uint8_t, 32U>{},
        "oversized raw strings are explicitly omitted without dropping row");

    strict_unchanged = {};
    ok &= Expect(
        ipc::ProjectTickWireV1(
            *fixture.records[kHistoryLongRawTick],
            fixture.Ordinal(kShanghaiInstrumentId),
            &strict_unchanged) &&
            strict_unchanged.projection_flags ==
                (ipc::kRealtimeWireTickRawTypeOmittedV1 |
                 ipc::kRealtimeWireTickRawTickFlagOmittedV1) &&
            strict_unchanged.raw_type_length == 0U &&
            strict_unchanged.raw_tick_flag_length == 0U &&
            strict_unchanged.raw_type ==
                std::array<std::uint8_t, 32U>{} &&
            strict_unchanged.raw_tick_flag ==
                std::array<std::uint8_t, 32U>{},
        "latest/ring projection explicitly marks oversized raw strings");

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

int ConnectControlClient(
    const std::filesystem::path& socket_path) {
    const int client =
        ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (client < 0) {
        return -1;
    }
    timeval timeout{};
    timeout.tv_sec = 2;
    if (::setsockopt(
            client,
            SOL_SOCKET,
            SO_RCVTIMEO,
            &timeout,
            static_cast<socklen_t>(sizeof(timeout))) != 0 ||
        ::setsockopt(
            client,
            SOL_SOCKET,
            SO_SNDTIMEO,
            &timeout,
            static_cast<socklen_t>(sizeof(timeout))) != 0) {
        static_cast<void>(::close(client));
        return -1;
    }
    const std::string native_path = socket_path.string();
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    if (native_path.size() >= sizeof(address.sun_path)) {
        static_cast<void>(::close(client));
        return -1;
    }
    std::memcpy(
        address.sun_path,
        native_path.c_str(),
        native_path.size() + 1U);
    const socklen_t address_bytes = static_cast<socklen_t>(
        offsetof(sockaddr_un, sun_path) +
        native_path.size() + 1U);
    if (::connect(
            client,
            reinterpret_cast<const sockaddr*>(&address),
            address_bytes) != 0) {
        static_cast<void>(::close(client));
        return -1;
    }
    return client;
}

template <typename Packet>
bool SendControlPacket(int client, const Packet& packet) {
    static_assert(std::is_standard_layout_v<Packet>);
    return client >= 0 &&
           ::send(
               client,
               &packet,
               sizeof(packet),
               MSG_NOSIGNAL) ==
               static_cast<ssize_t>(sizeof(packet));
}

template <typename Packet>
bool ReceiveControlPacket(
    int client,
    Packet* output,
    int* attached_fd) {
    static_assert(std::is_standard_layout_v<Packet>);
    if (client < 0 || output == nullptr ||
        attached_fd == nullptr) {
        return false;
    }
    *output = Packet{};
    *attached_fd = -1;
    iovec vector{};
    vector.iov_base = output;
    vector.iov_len = sizeof(*output);
    std::array<std::byte, CMSG_SPACE(sizeof(int))> control{};
    msghdr message{};
    message.msg_iov = &vector;
    message.msg_iovlen = 1U;
    message.msg_control = control.data();
    message.msg_controllen = control.size();
    const ssize_t received =
        ::recvmsg(client, &message, MSG_CMSG_CLOEXEC);
    if (received != static_cast<ssize_t>(sizeof(*output)) ||
        (message.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) != 0) {
        return false;
    }
    for (cmsghdr* header = CMSG_FIRSTHDR(&message);
         header != nullptr;
         header = CMSG_NXTHDR(&message, header)) {
        if (header->cmsg_level == SOL_SOCKET &&
            header->cmsg_type == SCM_RIGHTS &&
            header->cmsg_len == CMSG_LEN(sizeof(int))) {
            if (*attached_fd >= 0) {
                return false;
            }
            std::memcpy(
                attached_fd,
                CMSG_DATA(header),
                sizeof(*attached_fd));
        } else {
            return false;
        }
    }
    return true;
}

struct TickDeltaPageCopy final {
    ipc::RealtimeInstrumentTickDeltaPageHeaderV2 header{};
    std::vector<ipc::RealtimeWireTickPayloadV1> ticks;
    bool descriptor_is_read_only = false;
    bool descriptor_is_sealed = false;
};

bool OpenSealedReadOnlyPage(
    std::span<const std::byte> image,
    int* output) {
    if (image.empty() || output == nullptr) {
        return false;
    }
    *output = -1;
    UniqueFd writable(::memfd_create(
        "l2flow-test-tick-delta-page",
        MFD_CLOEXEC | MFD_ALLOW_SEALING));
    if (writable.get() < 0 ||
        ::ftruncate(
            writable.get(),
            static_cast<off_t>(image.size())) != 0) {
        return false;
    }
    std::size_t written = 0U;
    while (written < image.size()) {
        const ssize_t result = ::write(
            writable.get(),
            image.data() + written,
            image.size() - written);
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result <= 0) {
            return false;
        }
        written += static_cast<std::size_t>(result);
    }
    constexpr int seals =
        F_SEAL_WRITE | F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL;
    if (::fcntl(writable.get(), F_ADD_SEALS, seals) != 0 ||
        ::fcntl(writable.get(), F_GET_SEALS) != seals) {
        return false;
    }
    std::array<char, 64U> path{};
    const int path_bytes = std::snprintf(
        path.data(),
        path.size(),
        "/proc/self/fd/%d",
        writable.get());
    if (path_bytes <= 0 ||
        static_cast<std::size_t>(path_bytes) >= path.size()) {
        return false;
    }
    *output = ::open(path.data(), O_RDONLY | O_CLOEXEC);
    return *output >= 0;
}

class TickDeltaWireClient final {
public:
    TickDeltaWireClient() = default;
    TickDeltaWireClient(const TickDeltaWireClient&) = delete;
    TickDeltaWireClient& operator=(const TickDeltaWireClient&) =
        delete;

    [[nodiscard]] bool OpenSession(
        const std::filesystem::path& socket_path,
        ipc::RealtimeInstrumentTickDeltaOpenSessionResponseV2*
            output) {
        if (output == nullptr) {
            return false;
        }
        client_.Reset(ConnectControlClient(socket_path));
        if (client_.get() < 0) {
            return false;
        }
        ipc::RealtimeInstrumentTickDeltaOpenSessionRequestV2
            request{};
        request.magic = ipc::kRealtimeControlMagicV1;
        request.protocol_major = ipc::kRealtimeWireMajorV1;
        request.protocol_minor = ipc::kRealtimeWireMinorV1;
        request.opcode = static_cast<std::uint16_t>(
            ipc::RealtimeInstrumentTickDeltaControlOpcodeV2::
                kOpenDeltaSession);
        request.message_bytes =
            static_cast<std::uint32_t>(sizeof(request));
        request.request_id = NextRequestId();
        int received_fd = -1;
        if (!SendControlPacket(client_.get(), request) ||
            !ReceiveControlPacket(
                client_.get(), output, &received_fd)) {
            if (received_fd >= 0) {
                static_cast<void>(::close(received_fd));
            }
            return false;
        }
        const bool valid =
            received_fd < 0 &&
            output->magic == ipc::kRealtimeControlMagicV1 &&
            output->protocol_major ==
                ipc::kRealtimeWireMajorV1 &&
            output->protocol_minor ==
                ipc::kRealtimeWireMinorV1 &&
            output->message_bytes == sizeof(*output) &&
            output->request_id == request.request_id;
        if (valid &&
            output->status ==
                static_cast<std::uint16_t>(
                    ipc::
                        RealtimeInstrumentTickDeltaControlStatusV2::
                            kOk)) {
            session_token_ = output->delta_session_token;
        }
        return valid;
    }

    [[nodiscard]] bool OpenInstrument(
        std::uint32_t instrument_id,
        std::uint32_t requested_page_records,
        ipc::RealtimeInstrumentTickDeltaBaseKindV2 base_kind,
        const ipc::RealtimeInstrumentTickDeltaCheckpointV2&
            base_checkpoint,
        ipc::RealtimeInstrumentTickDeltaOpenInstrumentResponseV2*
            output) {
        if (output == nullptr || client_.get() < 0 ||
            session_token_ == 0U) {
            return false;
        }
        ipc::RealtimeInstrumentTickDeltaOpenInstrumentRequestV2
            request{};
        request.magic = ipc::kRealtimeControlMagicV1;
        request.protocol_major = ipc::kRealtimeWireMajorV1;
        request.protocol_minor = ipc::kRealtimeWireMinorV1;
        request.opcode = static_cast<std::uint16_t>(
            ipc::RealtimeInstrumentTickDeltaControlOpcodeV2::
                kOpenInstrumentDelta);
        request.message_bytes =
            static_cast<std::uint32_t>(sizeof(request));
        request.request_id = NextRequestId();
        request.instrument_id = instrument_id;
        request.requested_page_records =
            requested_page_records;
        request.base_kind =
            static_cast<std::uint32_t>(base_kind);
        request.delta_session_token = session_token_;
        request.base_checkpoint = base_checkpoint;
        int received_fd = -1;
        if (!SendControlPacket(client_.get(), request) ||
            !ReceiveControlPacket(
                client_.get(), output, &received_fd)) {
            if (received_fd >= 0) {
                static_cast<void>(::close(received_fd));
            }
            return false;
        }
        return received_fd < 0 &&
               output->magic == ipc::kRealtimeControlMagicV1 &&
               output->protocol_major ==
                   ipc::kRealtimeWireMajorV1 &&
               output->protocol_minor ==
                   ipc::kRealtimeWireMinorV1 &&
               output->message_bytes == sizeof(*output) &&
               output->request_id == request.request_id;
    }

    [[nodiscard]] bool Read(
        std::uint64_t expected_page_index,
        std::uint64_t read_token,
        ipc::RealtimeInstrumentTickDeltaReadResponseV2* output,
        TickDeltaPageCopy* page_output,
        std::vector<std::byte>* page_image = nullptr) {
        if (output == nullptr || page_output == nullptr ||
            client_.get() < 0) {
            return false;
        }
        *page_output = TickDeltaPageCopy{};
        if (page_image != nullptr) {
            page_image->clear();
        }
        ipc::RealtimeInstrumentTickDeltaReadRequestV2 request{};
        request.magic = ipc::kRealtimeControlMagicV1;
        request.protocol_major = ipc::kRealtimeWireMajorV1;
        request.protocol_minor = ipc::kRealtimeWireMinorV1;
        request.opcode = static_cast<std::uint16_t>(
            ipc::RealtimeInstrumentTickDeltaControlOpcodeV2::
                kReadInstrumentDelta);
        request.message_bytes =
            static_cast<std::uint32_t>(sizeof(request));
        request.request_id = NextRequestId();
        request.expected_page_index = expected_page_index;
        request.read_token = read_token;
        int received_fd = -1;
        if (!SendControlPacket(client_.get(), request) ||
            !ReceiveControlPacket(
                client_.get(), output, &received_fd)) {
            if (received_fd >= 0) {
                static_cast<void>(::close(received_fd));
            }
            return false;
        }
        UniqueFd page_fd(received_fd);
        if (output->magic != ipc::kRealtimeControlMagicV1 ||
            output->protocol_major !=
                ipc::kRealtimeWireMajorV1 ||
            output->protocol_minor !=
                ipc::kRealtimeWireMinorV1 ||
            output->message_bytes != sizeof(*output) ||
            output->request_id != request.request_id) {
            return false;
        }
        const bool success =
            output->status ==
            static_cast<std::uint16_t>(
                ipc::RealtimeInstrumentTickDeltaControlStatusV2::
                    kOk);
        const bool terminal =
            (output->flags &
             ipc::
                 kRealtimeInstrumentTickDeltaResponseTerminalV2) !=
            0U;
        if (!success || terminal) {
            return page_fd.get() < 0;
        }
        if (page_fd.get() < 0 ||
            output->record_count == 0U ||
            output->page_mapping_bytes <
                sizeof(
                    ipc::
                        RealtimeInstrumentTickDeltaPageHeaderV2)) {
            return false;
        }
        struct stat page_stat {};
        const int descriptor_flags =
            ::fcntl(page_fd.get(), F_GETFL);
        const int descriptor_fd_flags =
            ::fcntl(page_fd.get(), F_GETFD);
        const int seals =
            ::fcntl(page_fd.get(), F_GET_SEALS);
        const int expected_seals =
            F_SEAL_WRITE | F_SEAL_GROW |
            F_SEAL_SHRINK | F_SEAL_SEAL;
        page_output->descriptor_is_read_only =
            descriptor_flags >= 0 &&
            (descriptor_flags & O_ACCMODE) == O_RDONLY &&
            descriptor_fd_flags >= 0 &&
            (descriptor_fd_flags & FD_CLOEXEC) != 0;
        page_output->descriptor_is_sealed =
            seals == expected_seals;
        if (::fstat(page_fd.get(), &page_stat) != 0 ||
            !S_ISREG(page_stat.st_mode) ||
            page_stat.st_size !=
                static_cast<off_t>(
                    output->page_mapping_bytes)) {
            return false;
        }
        void* const mapping = ::mmap(
            nullptr,
            static_cast<std::size_t>(
                output->page_mapping_bytes),
            PROT_READ,
            MAP_SHARED,
            page_fd.get(),
            0);
        if (mapping == MAP_FAILED) {
            return false;
        }
        std::memcpy(
            &page_output->header,
            mapping,
            sizeof(page_output->header));
        const std::uint64_t tick_bytes =
            static_cast<std::uint64_t>(output->record_count) *
            sizeof(ipc::RealtimeWireTickPayloadV1);
        if (page_output->header.record_count !=
                output->record_count ||
            page_output->header.tick_payloads_offset >
                output->page_mapping_bytes ||
            tick_bytes >
                output->page_mapping_bytes -
                    page_output->header.tick_payloads_offset) {
            static_cast<void>(::munmap(
                mapping,
                static_cast<std::size_t>(
                    output->page_mapping_bytes)));
            return false;
        }
        const auto* const ticks =
            reinterpret_cast<
                const ipc::RealtimeWireTickPayloadV1*>(
                static_cast<const std::byte*>(mapping) +
                page_output->header.tick_payloads_offset);
        try {
            page_output->ticks.assign(
                ticks,
                ticks + output->record_count);
            if (page_image != nullptr) {
                const auto* const image_begin =
                    static_cast<const std::byte*>(mapping);
                page_image->assign(
                    image_begin,
                    image_begin +
                        static_cast<std::size_t>(
                            output->page_mapping_bytes));
            }
        } catch (...) {
            static_cast<void>(::munmap(
                mapping,
                static_cast<std::size_t>(
                    output->page_mapping_bytes)));
            return false;
        }
        return ::munmap(
                   mapping,
                   static_cast<std::size_t>(
                       output->page_mapping_bytes)) == 0;
    }

private:
    [[nodiscard]] std::uint64_t NextRequestId() noexcept {
        return next_request_id_++;
    }

    UniqueFd client_;
    std::uint64_t session_token_ = 0U;
    std::uint64_t next_request_id_ =
        0x8100000000000001ULL;
};

bool StatusEquals(std::uint8_t actual, int expected) {
    return static_cast<int>(actual) == expected;
}

#if defined(L2FLOW_IPC_E2E_PYTHON_EXECUTABLE)
bool ReadOneByte(int descriptor, char* output) {
    if (descriptor < 0 || output == nullptr) {
        return false;
    }
    ssize_t result = -1;
    do {
        result = ::read(descriptor, output, 1U);
    } while (result < 0 && errno == EINTR);
    return result == 1;
}

bool WriteOneByte(int descriptor, char value) {
    if (descriptor < 0) {
        return false;
    }
    ssize_t result = -1;
    do {
        result = ::write(descriptor, &value, 1U);
    } while (result < 0 && errno == EINTR);
    return result == 1;
}

bool RunPythonE2EProbe(
    const std::filesystem::path& socket_path,
    const std::shared_ptr<ipc::RealtimeSharedMarketServiceV1>& service,
    const std::shared_ptr<const market::
                              IntradayInstrumentStoreGenerationV1>&
        second_store_generation) {
    if (!Expect(
            service != nullptr && second_store_generation != nullptr,
            "prepare Python IPC generation handoff")) {
        return false;
    }

    int ready_descriptors[2]{-1, -1};
    int release_descriptors[2]{-1, -1};
    if (!Expect(
            ::pipe(ready_descriptors) == 0,
            "create Python IPC ready pipe")) {
        return false;
    }
    UniqueFd ready_reader(ready_descriptors[0U]);
    UniqueFd ready_writer(ready_descriptors[1U]);
    if (!Expect(
            ::pipe(release_descriptors) == 0,
            "create Python IPC release pipe")) {
        return false;
    }
    UniqueFd release_reader(release_descriptors[0U]);
    UniqueFd release_writer(release_descriptors[1U]);

    const std::string native_socket_path = socket_path.string();
    const pid_t child = ::fork();
    if (!Expect(child >= 0, "fork Python IPC E2E probe")) {
        return false;
    }
    if (child == 0) {
        ready_reader.Reset();
        release_writer.Reset();
        const std::string ready_descriptor =
            std::to_string(ready_writer.get());
        const std::string release_descriptor =
            std::to_string(release_reader.get());
        ::execl(
            L2FLOW_IPC_E2E_PYTHON_EXECUTABLE,
            L2FLOW_IPC_E2E_PYTHON_EXECUTABLE,
            "-B",
            L2FLOW_IPC_E2E_PYTHON_PROBE,
            native_socket_path.c_str(),
            L2FLOW_IPC_E2E_NATIVE_READER,
            L2FLOW_IPC_E2E_PYTHON_SOURCE,
            ready_descriptor.c_str(),
            release_descriptor.c_str(),
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

    ready_writer.Reset();
    release_reader.Reset();
    char child_ready = '\0';
    const bool ready =
        ReadOneByte(ready_reader.get(), &child_ready) &&
        child_ready == 'R';
    const bool published =
        ready &&
        service->PublishStoreGeneration(second_store_generation);
    const bool released =
        ready &&
        WriteOneByte(
            release_writer.get(), published ? 'P' : 'F');
    ready_reader.Reset();
    release_writer.Reset();

    int status = 0;
    pid_t waited = -1;
    do {
        waited = ::waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    return Expect(
        ready && published && released &&
            waited == child && WIFEXITED(status) &&
            WEXITSTATUS(status) == 0,
        "Python cursor pins generation 1 and a new cursor reads "
        "empty-increment generation 2");
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

    constexpr std::size_t key_count = 9U;
    const std::array<std::uint8_t, key_count> markets{
        1U, 1U, 2U, 1U, 2U, 1U, 1U, 0U, 1U};
    const std::array<std::string_view, key_count> sources{
        "101", "101", "102", "101", "102 ", "101", "101", "", "101"};
    const std::array<std::string_view, key_count> security_ids{
        "600001",
        "600003",
        "000002",
        "600001",
        "000002",
        std::string_view("600001\0", 7U),
        "999999",
        "600001",
        "",
    };
    std::array<const std::uint8_t*, key_count> source_pointers{};
    std::array<std::size_t, key_count> source_lengths{};
    std::array<const std::uint8_t*, key_count> security_pointers{};
    std::array<std::size_t, key_count> security_lengths{};
    for (std::size_t index = 0U; index < key_count; ++index) {
        source_pointers[index] =
            sources[index].empty()
                ? nullptr
                : reinterpret_cast<const std::uint8_t*>(
                      sources[index].data());
        source_lengths[index] = sources[index].size();
        security_pointers[index] =
            security_ids[index].empty()
                ? nullptr
                : reinterpret_cast<const std::uint8_t*>(
                      security_ids[index].data());
        security_lengths[index] = security_ids[index].size();
    }
    std::array<std::uint32_t, key_count> resolved_ids{};
    resolved_ids.fill(std::numeric_limits<std::uint32_t>::max());
    std::array<std::uint8_t, key_count> lookup_statuses{};
    const int lookup_error =
        l2flow_shm_reader_resolve_instruments_v1(
            reader,
            markets.data(),
            source_pointers.data(),
            source_lengths.data(),
            security_pointers.data(),
            security_lengths.data(),
            key_count,
            resolved_ids.data(),
            lookup_statuses.data());
    ok &= Expect(
        lookup_error == L2FLOW_SHM_READER_OK_V1 &&
            resolved_ids[0U] == kShanghaiInstrumentId &&
            resolved_ids[1U] == kUnobservedInstrumentId &&
            resolved_ids[2U] == kShenzhenInstrumentId &&
            resolved_ids[3U] == kShanghaiInstrumentId &&
            resolved_ids[4U] == 0U && resolved_ids[5U] == 0U &&
            resolved_ids[6U] == 0U && resolved_ids[7U] == 0U &&
            resolved_ids[8U] == 0U &&
            lookup_statuses[0U] ==
                L2FLOW_INSTRUMENT_LOOKUP_FOUND_V1 &&
            lookup_statuses[1U] ==
                L2FLOW_INSTRUMENT_LOOKUP_FOUND_V1 &&
            lookup_statuses[2U] ==
                L2FLOW_INSTRUMENT_LOOKUP_FOUND_V1 &&
            lookup_statuses[3U] ==
                L2FLOW_INSTRUMENT_LOOKUP_FOUND_V1 &&
            lookup_statuses[4U] ==
                L2FLOW_INSTRUMENT_LOOKUP_UNKNOWN_V1 &&
            lookup_statuses[5U] ==
                L2FLOW_INSTRUMENT_LOOKUP_UNKNOWN_V1 &&
            lookup_statuses[6U] ==
                L2FLOW_INSTRUMENT_LOOKUP_UNKNOWN_V1 &&
            lookup_statuses[7U] ==
                L2FLOW_INSTRUMENT_LOOKUP_INVALID_MARKET_V1 &&
            lookup_statuses[8U] ==
                L2FLOW_INSTRUMENT_LOOKUP_EMPTY_SECURITY_ID_V1,
        "exact-byte instrument resolver preserves order, duplicates, "
        "and per-item status");
    ok &= Expect(
        l2flow_shm_reader_resolve_instruments_v1(
            reader,
            nullptr,
            nullptr,
            nullptr,
            nullptr,
            nullptr,
            0U,
            nullptr,
            nullptr) == L2FLOW_SHM_READER_OK_V1,
        "empty instrument resolver batch permits null arrays");

    auto malformed_source_pointers = source_pointers;
    malformed_source_pointers[0U] = nullptr;
    resolved_ids.fill(std::numeric_limits<std::uint32_t>::max());
    ok &= Expect(
        l2flow_shm_reader_resolve_instruments_v1(
            reader,
            markets.data(),
            malformed_source_pointers.data(),
            source_lengths.data(),
            security_pointers.data(),
            security_lengths.data(),
            key_count,
            resolved_ids.data(),
            lookup_statuses.data()) ==
                L2FLOW_SHM_READER_INVALID_ARGUMENT_V1 &&
            resolved_ids[0U] ==
                std::numeric_limits<std::uint32_t>::max(),
        "instrument resolver validates all byte spans before output");
    auto malformed_security_pointers = security_pointers;
    malformed_security_pointers[0U] = nullptr;
    resolved_ids.fill(std::numeric_limits<std::uint32_t>::max());
    ok &= Expect(
        l2flow_shm_reader_resolve_instruments_v1(
            reader,
            markets.data(),
            source_pointers.data(),
            source_lengths.data(),
            malformed_security_pointers.data(),
            security_lengths.data(),
            key_count,
            resolved_ids.data(),
            lookup_statuses.data()) ==
                L2FLOW_SHM_READER_INVALID_ARGUMENT_V1 &&
            resolved_ids[0U] ==
                std::numeric_limits<std::uint32_t>::max(),
        "instrument resolver validates every security-ID span before output");
    return ok;
}

bool TestInstrumentTickDeltaV2() {
    bool ok = true;
    MarketFixture fixture;
    if (!fixture.Initialize()) {
        return false;
    }
    ScopedTempDirectory directory;
    if (!Expect(
            directory.valid(),
            "create tick-delta IPC temp directory")) {
        return false;
    }
    const std::filesystem::path socket_path =
        directory.path() / "tick-delta.sock";

    common::Identity128 run_id{};
    run_id[0U] = std::byte{0x7dU};
    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        first_generation;
    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        second_generation;
    if (!fixture.BuildStoreGeneration(
            run_id, 1U, 80'000U, &first_generation) ||
        !fixture.BuildStoreGeneration(
            run_id, 2U, 80'001U, &second_generation)) {
        return false;
    }

    ipc::RealtimeSharedServiceConfigV1 config{};
    config.run_id = run_id;
    config.session_epoch = 29U;
    config.trade_date = kTradeDate;
    config.registry = fixture.registry.get();
    config.tick_ring_capacity = 2U;
    config.maximum_mapping_bytes = 16U * 1024U * 1024U;
    config.maximum_history_page_records = 2U;
    config.control_socket_path = socket_path;
    std::shared_ptr<ipc::RealtimeSharedMarketServiceV1> service;
    int system_error = 0;
    const auto create_error =
        ipc::RealtimeSharedMarketServiceV1::Create(
            std::move(config), &service, &system_error);
    if (!Expect(
            create_error ==
                    ipc::RealtimeSharedServiceCreateErrorV1::kNone &&
                service != nullptr && system_error == 0,
            "create tick-delta IPC service")) {
        return false;
    }
    if (!Expect(
            service->Start(&system_error) && system_error == 0 &&
                service->PublishStoreGeneration(first_generation),
            "start tick-delta service and publish generation 1")) {
        service->StopControl();
        return false;
    }

    const auto status_is =
        [](std::uint16_t actual,
           ipc::RealtimeInstrumentTickDeltaControlStatusV2
               expected) noexcept {
            return actual ==
                   static_cast<std::uint16_t>(expected);
        };
    const auto terminal =
        [](const ipc::RealtimeInstrumentTickDeltaReadResponseV2&
               response) noexcept {
            return (response.flags &
                    ipc::
                        kRealtimeInstrumentTickDeltaResponseTerminalV2) !=
                   0U;
        };

    TickDeltaWireClient client;
    ipc::RealtimeInstrumentTickDeltaOpenSessionResponseV2
        session{};
    if (!Expect(
            client.OpenSession(socket_path, &session) &&
                status_is(
                    session.status,
                    ipc::
                        RealtimeInstrumentTickDeltaControlStatusV2::
                            kOk),
            "open one pinned tick-delta session")) {
        service->StopControl();
        return false;
    }
    const auto& target = session.target_generation;
    std::uint64_t source_record_total = 0U;
    for (const std::uint64_t exclusive :
         target.source_sequence_exclusive) {
        if (exclusive == 0U ||
            source_record_total >
                std::numeric_limits<std::uint64_t>::max() -
                    (exclusive - 1U)) {
            source_record_total =
                std::numeric_limits<std::uint64_t>::max();
            break;
        }
        source_record_total += exclusive - 1U;
    }
    const std::uint64_t checked_tick_exclusive =
        1U +
        (target.source_sequence_exclusive[1U] - 1U) +
        (target.source_sequence_exclusive[3U] - 1U);
    ok &= Expect(
        target.generation == 1U &&
            target.session_epoch == 29U &&
            target.trade_date == kTradeDate &&
            target.instrument_count == 3U &&
            target.ingress_sequence_exclusive == 8U &&
            target.source_stream_ids == kSourceStreamIds &&
            target.source_sequence_exclusive ==
                std::array<std::uint64_t, 4U>{2U, 4U, 2U, 3U} &&
            source_record_total + 1U ==
                target.ingress_sequence_exclusive &&
            checked_tick_exclusive == 6U &&
            target.tick_stream_sequence_exclusive ==
                checked_tick_exclusive &&
            target.flags ==
                (ipc::
                     kRealtimeInstrumentTickDeltaCoverageFromOpenV2 |
                 ipc::
                     kRealtimeInstrumentTickDeltaTickRecordCoverageCompleteV2) &&
            target.payload_projection ==
                static_cast<std::uint32_t>(
                    ipc::
                        RealtimeInstrumentTickDeltaPayloadProjectionV2::
                            kCoreV1) &&
            session.delta_session_token != 0U,
        "target endpoint carries checked I/S/Q identity and coverage");
    ok &= Expect(
        service->PublishStoreGeneration(second_generation),
        "publish generation 2 after V2 pins generation 1");

    const ipc::RealtimeInstrumentTickDeltaCheckpointV2
        origin_checkpoint{};
    ipc::RealtimeInstrumentTickDeltaOpenInstrumentResponseV2
        shenzhen_open{};
    if (!Expect(
            client.OpenInstrument(
                kShenzhenInstrumentId,
                1U,
                ipc::RealtimeInstrumentTickDeltaBaseKindV2::kOrigin,
                origin_checkpoint,
                &shenzhen_open) &&
                status_is(
                    shenzhen_open.status,
                    ipc::
                        RealtimeInstrumentTickDeltaControlStatusV2::
                            kOk),
            "open paginated Shenzhen origin delta")) {
        service->StopControl();
        return false;
    }
    const auto& metadata = shenzhen_open.metadata;
    ok &= Expect(
        metadata.base_kind ==
                static_cast<std::uint32_t>(
                    ipc::RealtimeInstrumentTickDeltaBaseKindV2::
                        kOrigin) &&
            metadata.selected_source_mask ==
                ipc::kRealtimeInstrumentTickDeltaSourceMaskV2 &&
            std::memcmp(
                &metadata.base_checkpoint,
                &origin_checkpoint,
                sizeof(origin_checkpoint)) == 0 &&
            metadata.target_checkpoint.generation.generation == 1U &&
            metadata.target_checkpoint.instrument_id ==
                kShenzhenInstrumentId &&
            metadata.target_checkpoint.registry_ordinal ==
                fixture.Ordinal(kShenzhenInstrumentId) &&
            metadata.target_checkpoint
                    .instrument_tick_source_record_counts ==
                std::array<std::uint64_t, 4U>{0U, 0U, 0U, 2U} &&
            metadata.target_checkpoint
                    .instrument_tick_record_count == 2U &&
            metadata.delta_tick_source_record_counts ==
                std::array<std::uint64_t, 4U>{0U, 0U, 0U, 2U} &&
            metadata.delta_tick_record_count == 2U &&
            metadata.ingress_sequence_begin_inclusive == 1U &&
            metadata.ingress_sequence_end_exclusive == 8U &&
            metadata.tick_stream_sequence_begin_inclusive == 1U &&
            metadata.tick_stream_sequence_end_exclusive == 6U &&
            metadata.flags == target.flags &&
            metadata.payload_projection ==
                target.payload_projection &&
            shenzhen_open.initial_read_token != 0U &&
            shenzhen_open.initial_read_token !=
                session.delta_session_token,
        "origin metadata has exact counts, bounds, and distinct token");

    std::uint64_t read_token = shenzhen_open.initial_read_token;
    constexpr std::array<std::uint64_t, 2U>
        expected_ingress{4U, 5U};
    constexpr std::array<std::uint64_t, 2U>
        expected_tick{2U, 3U};
    constexpr std::array<std::uint64_t, 2U>
        expected_source{1U, 2U};
    std::uint64_t native_prior_ingress = 0U;
    std::uint64_t native_prior_tick = 0U;
    std::array<std::uint64_t, 4U> native_prior_sources{};
    for (std::uint64_t page_index = 0U;
         page_index < 2U;
         ++page_index) {
        ipc::RealtimeInstrumentTickDeltaReadResponseV2 response{};
        TickDeltaPageCopy page{};
        std::vector<std::byte> page_image;
        if (!Expect(
                client.Read(
                    page_index,
                    read_token,
                    &response,
                    &page,
                    &page_image) &&
                    status_is(
                        response.status,
                        ipc::
                            RealtimeInstrumentTickDeltaControlStatusV2::
                                kOk) &&
                    !terminal(response) &&
                    response.record_count == 1U &&
                    response.page_index == page_index &&
                    response.target_generation == 1U &&
                    response.next_read_token != 0U &&
                    response.next_read_token != read_token &&
                    response.next_read_token !=
                        session.delta_session_token &&
                    page.ticks.size() == 1U,
                "read one dense paginated tick-delta page")) {
            service->StopControl();
            return false;
        }
        const auto& header = page.header;
        const auto& tick = page.ticks[0U].common;
        ok &= Expect(
            page.descriptor_is_read_only &&
                page.descriptor_is_sealed &&
                header.magic ==
                    ipc::kRealtimeInstrumentTickDeltaPageMagicV2 &&
                header.abi_major == ipc::kRealtimeWireMajorV1 &&
                header.abi_minor == ipc::kRealtimeWireMinorV1 &&
                header.header_bytes ==
                    sizeof(
                        ipc::
                            RealtimeInstrumentTickDeltaPageHeaderV2) &&
                header.endian_marker ==
                    ipc::kRealtimeLittleEndianMarkerV1 &&
                header.flags == 0U &&
                header.total_mapping_bytes ==
                    sizeof(
                        ipc::
                            RealtimeInstrumentTickDeltaPageHeaderV2) +
                        sizeof(ipc::RealtimeWireTickPayloadV1) &&
                header.page_index == page_index &&
                header.record_count == 1U &&
                header.tick_payload_bytes ==
                    sizeof(ipc::RealtimeWireTickPayloadV1) &&
                header.tick_payloads_offset ==
                    sizeof(
                        ipc::
                            RealtimeInstrumentTickDeltaPageHeaderV2) &&
                header.first_ingress_sequence ==
                    expected_ingress[page_index] &&
                header.last_ingress_sequence ==
                    expected_ingress[page_index] &&
                header.first_tick_stream_sequence ==
                    expected_tick[page_index] &&
                header.last_tick_stream_sequence ==
                    expected_tick[page_index] &&
                header.metadata.target_checkpoint.generation.generation ==
                    1U &&
                tick.instrument_id == kShenzhenInstrumentId &&
                tick.registry_ordinal ==
                    fixture.Ordinal(kShenzhenInstrumentId) &&
                tick.source_slot == 3U &&
                tick.source_stream_id == kSourceStreamIds[3U] &&
                tick.source_sequence ==
                    expected_source[page_index] &&
                tick.ingress_sequence ==
                    expected_ingress[page_index] &&
                tick.tick_stream_sequence ==
                    expected_tick[page_index] &&
                tick.event_kind ==
                    static_cast<std::uint8_t>(
                        page_index == 0U
                            ? market::MarketEventKindV1::
                                  kShenzhenOrder
                            : market::MarketEventKindV1::
                                  kShenzhenTransaction),
            "page is sealed, dense, oldest-first, and bounded");

        int validation_fd_value = -1;
        if (!Expect(
                OpenSealedReadOnlyPage(
                    page_image, &validation_fd_value),
                "recreate sealed read-only tick-delta page")) {
            service->StopControl();
            return false;
        }
        UniqueFd validation_fd(validation_fd_value);
        std::vector<std::byte> validated_ticks(
            response.record_count *
            sizeof(ipc::RealtimeWireTickPayloadV1));
        l2flow_instrument_tick_delta_page_result_v2
            validation_result{};
        const int validation_error =
            l2flow_shm_reader_instrument_tick_delta_page_v2(
                validation_fd.get(),
                response.page_mapping_bytes,
                response.record_count,
                response.page_index,
                &metadata,
                sizeof(metadata),
                native_prior_ingress,
                native_prior_tick,
                native_prior_sources.data(),
                validated_ticks.data(),
                validated_ticks.size(),
                &validation_result);
        ok &= Expect(
            validation_error == L2FLOW_SHM_READER_OK_V1 &&
                validated_ticks.size() ==
                    page.ticks.size() *
                        sizeof(
                            ipc::RealtimeWireTickPayloadV1) &&
                std::memcmp(
                    validated_ticks.data(),
                    page.ticks.data(),
                    validated_ticks.size()) == 0 &&
                validation_result.source_counts[0U] == 0U &&
                validation_result.source_counts[1U] == 0U &&
                validation_result.source_counts[2U] == 0U &&
                validation_result.source_counts[3U] == 1U &&
                validation_result.last_ingress_sequence ==
                    expected_ingress[page_index] &&
                validation_result.last_tick_stream_sequence ==
                    expected_tick[page_index] &&
                validation_result.last_source_sequences[3U] ==
                    expected_source[page_index],
            "native column page validator copies and advances "
            "cross-page state");

        if (page_index == 0U) {
            std::vector<std::byte> corrupt_image = page_image;
            constexpr std::size_t instrument_id_offset =
                sizeof(
                    ipc::
                        RealtimeInstrumentTickDeltaPageHeaderV2) +
                offsetof(
                    ipc::RealtimeWireTickPayloadV1,
                    common) +
                offsetof(
                    ipc::RealtimeWireCommonRecordV1,
                    instrument_id);
            corrupt_image[instrument_id_offset] ^= std::byte{1U};
            int corrupt_fd_value = -1;
            if (!Expect(
                    OpenSealedReadOnlyPage(
                        corrupt_image, &corrupt_fd_value),
                    "seal mutated tick-delta page")) {
                service->StopControl();
                return false;
            }
            UniqueFd corrupt_fd(corrupt_fd_value);
            std::vector<std::byte> failure_output(
                validated_ticks.size(), std::byte{0xa5U});
            const std::vector<std::byte>
                expected_failure_output = failure_output;
            l2flow_instrument_tick_delta_page_result_v2
                failure_result{};
            std::memset(
                &failure_result, 0x5a, sizeof(failure_result));
            const auto expected_failure_result = failure_result;
            const int failure_error =
                l2flow_shm_reader_instrument_tick_delta_page_v2(
                    corrupt_fd.get(),
                    response.page_mapping_bytes,
                    response.record_count,
                    response.page_index,
                    &metadata,
                    sizeof(metadata),
                    native_prior_ingress,
                    native_prior_tick,
                    native_prior_sources.data(),
                    failure_output.data(),
                    failure_output.size(),
                    &failure_result);
            ok &= Expect(
                failure_error ==
                        L2FLOW_SHM_READER_LAYOUT_INVALID_V1 &&
                    failure_output == expected_failure_output &&
                    std::memcmp(
                        &failure_result,
                        &expected_failure_result,
                        sizeof(failure_result)) == 0,
                "mutated payload fails without changing outputs");
        }
        native_prior_ingress =
            validation_result.last_ingress_sequence;
        native_prior_tick =
            validation_result.last_tick_stream_sequence;
        std::copy(
            std::begin(
                validation_result.last_source_sequences),
            std::end(
                validation_result.last_source_sequences),
            native_prior_sources.begin());
        read_token = response.next_read_token;
    }
    ipc::RealtimeInstrumentTickDeltaReadResponseV2 shenzhen_eof{};
    TickDeltaPageCopy empty_page{};
    ok &= Expect(
        client.Read(2U, read_token, &shenzhen_eof, &empty_page) &&
            status_is(
                shenzhen_eof.status,
                ipc::
                    RealtimeInstrumentTickDeltaControlStatusV2::kOk) &&
            terminal(shenzhen_eof) &&
            shenzhen_eof.record_count == 0U &&
            shenzhen_eof.page_mapping_bytes == 0U &&
            shenzhen_eof.page_index == 2U &&
            shenzhen_eof.target_generation == 1U &&
            shenzhen_eof.next_read_token == 0U &&
            empty_page.ticks.empty(),
        "Shenzhen delta ends with explicit zero-row EOF");

    ipc::RealtimeInstrumentTickDeltaOpenInstrumentResponseV2
        unobserved_open{};
    if (!Expect(
            client.OpenInstrument(
                kUnobservedInstrumentId,
                1U,
                ipc::RealtimeInstrumentTickDeltaBaseKindV2::kOrigin,
                origin_checkpoint,
                &unobserved_open) &&
                status_is(
                    unobserved_open.status,
                    ipc::
                        RealtimeInstrumentTickDeltaControlStatusV2::
                            kOk),
            "open a second, empty instrument on the same target")) {
        service->StopControl();
        return false;
    }
    ok &= Expect(
        unobserved_open.metadata
                    .target_checkpoint.generation.generation == 1U &&
            unobserved_open.metadata
                    .target_checkpoint.instrument_id ==
                kUnobservedInstrumentId &&
            unobserved_open.metadata
                    .target_checkpoint.instrument_tick_record_count ==
                0U &&
            unobserved_open.metadata.delta_tick_record_count == 0U &&
            unobserved_open.initial_read_token !=
                session.delta_session_token,
        "empty delta retains the session's pinned generation 1 target");
    ipc::RealtimeInstrumentTickDeltaReadResponseV2
        unobserved_eof{};
    ok &= Expect(
        client.Read(
            0U,
            unobserved_open.initial_read_token,
            &unobserved_eof,
            &empty_page) &&
            status_is(
                unobserved_eof.status,
                ipc::
                    RealtimeInstrumentTickDeltaControlStatusV2::kOk) &&
            terminal(unobserved_eof) &&
            unobserved_eof.record_count == 0U &&
            unobserved_eof.page_index == 0U &&
            unobserved_eof.target_generation == 1U,
        "empty delta emits explicit EOF and permits another instrument");

    std::unique_ptr<market::IntradayInstrumentTickDeltaCursorV1>
        shanghai_summary_cursor;
    if (!Expect(
            first_generation->OpenInstrumentTickDeltaCursor(
                kShanghaiInstrumentId,
                1U,
                &shanghai_summary_cursor) ==
                    market::IntradayInstrumentStoreQueryErrorV1::
                        kNone &&
                shanghai_summary_cursor != nullptr,
            "obtain Store-derived Shanghai checkpoint oracle")) {
        service->StopControl();
        return false;
    }
    ipc::RealtimeInstrumentTickDeltaCheckpointV2
        shanghai_checkpoint{};
    shanghai_checkpoint.generation = target;
    shanghai_checkpoint.instrument_id = kShanghaiInstrumentId;
    shanghai_checkpoint.registry_ordinal =
        static_cast<std::uint32_t>(
            fixture.Ordinal(kShanghaiInstrumentId));
    shanghai_checkpoint
        .instrument_tick_source_record_counts =
        shanghai_summary_cursor->summary()
            .target_tick_source_record_counts;
    shanghai_checkpoint.instrument_tick_record_count =
        shanghai_summary_cursor->summary().delta_tick_record_count;
    ok &= Expect(
        shanghai_checkpoint
                .instrument_tick_source_record_counts ==
                std::array<std::uint64_t, 4U>{0U, 3U, 0U, 0U} &&
            shanghai_checkpoint.instrument_tick_record_count == 3U,
        "Store oracle counts all three Shanghai tick-lane rows");

    auto mismatched_checkpoint = shanghai_checkpoint;
    --mismatched_checkpoint
          .instrument_tick_source_record_counts[1U];
    --mismatched_checkpoint.instrument_tick_record_count;
    ipc::RealtimeInstrumentTickDeltaOpenInstrumentResponseV2
        mismatch_response{};
    ok &= Expect(
        client.OpenInstrument(
            kShanghaiInstrumentId,
            1U,
            ipc::RealtimeInstrumentTickDeltaBaseKindV2::kCheckpoint,
            mismatched_checkpoint,
            &mismatch_response) &&
            status_is(
                mismatch_response.status,
                ipc::
                    RealtimeInstrumentTickDeltaControlStatusV2::
                        kCheckpointMismatch) &&
            mismatch_response.initial_read_token == 0U,
        "checkpoint counts must match Store-derived base counts");

    ipc::RealtimeInstrumentTickDeltaOpenInstrumentResponseV2
        exact_checkpoint_open{};
    if (!Expect(
            client.OpenInstrument(
                kShanghaiInstrumentId,
                1U,
                ipc::
                    RealtimeInstrumentTickDeltaBaseKindV2::kCheckpoint,
                shanghai_checkpoint,
                &exact_checkpoint_open) &&
                status_is(
                    exact_checkpoint_open.status,
                    ipc::
                        RealtimeInstrumentTickDeltaControlStatusV2::
                            kOk),
            "checkpoint mismatch is recoverable within the same session")) {
        service->StopControl();
        return false;
    }
    ok &= Expect(
        std::memcmp(
            &exact_checkpoint_open.metadata.base_checkpoint,
            &shanghai_checkpoint,
            sizeof(shanghai_checkpoint)) == 0 &&
            exact_checkpoint_open.metadata
                    .target_checkpoint.generation.generation == 1U &&
            exact_checkpoint_open.metadata.delta_tick_record_count ==
                0U &&
            exact_checkpoint_open.metadata
                    .ingress_sequence_begin_inclusive == 8U &&
            exact_checkpoint_open.metadata
                    .ingress_sequence_end_exclusive == 8U &&
            exact_checkpoint_open.metadata
                    .tick_stream_sequence_begin_inclusive == 6U &&
            exact_checkpoint_open.metadata
                    .tick_stream_sequence_end_exclusive == 6U &&
            exact_checkpoint_open.initial_read_token !=
                session.delta_session_token,
        "same-target checkpoint produces exact empty half-open delta");
    ipc::RealtimeInstrumentTickDeltaReadResponseV2
        exact_checkpoint_eof{};
    ok &= Expect(
        client.Read(
            0U,
            exact_checkpoint_open.initial_read_token,
            &exact_checkpoint_eof,
            &empty_page) &&
            status_is(
                exact_checkpoint_eof.status,
                ipc::
                    RealtimeInstrumentTickDeltaControlStatusV2::kOk) &&
            terminal(exact_checkpoint_eof) &&
            exact_checkpoint_eof.record_count == 0U &&
            exact_checkpoint_eof.target_generation == 1U,
        "same-target checkpoint has explicit zero-row EOF");

    ipc::RealtimeInstrumentTickDeltaOpenInstrumentResponseV2
        shanghai_origin_open{};
    if (!Expect(
            client.OpenInstrument(
                kShanghaiInstrumentId,
                1U,
                ipc::RealtimeInstrumentTickDeltaBaseKindV2::kOrigin,
                origin_checkpoint,
                &shanghai_origin_open) &&
                status_is(
                    shanghai_origin_open.status,
                    ipc::
                        RealtimeInstrumentTickDeltaControlStatusV2::
                            kOk) &&
                shanghai_origin_open.metadata
                        .delta_tick_record_count == 3U &&
                shanghai_origin_open.initial_read_token !=
                    session.delta_session_token,
            "open Shanghai origin delta containing a zero tick seam")) {
        service->StopControl();
        return false;
    }
    ipc::RealtimeInstrumentTickDeltaReadResponseV2
        shanghai_first{};
    TickDeltaPageCopy shanghai_page{};
    if (!Expect(
            client.Read(
                0U,
                shanghai_origin_open.initial_read_token,
                &shanghai_first,
                &shanghai_page) &&
                status_is(
                    shanghai_first.status,
                    ipc::
                        RealtimeInstrumentTickDeltaControlStatusV2::
                            kOk) &&
                !terminal(shanghai_first) &&
                shanghai_page.ticks.size() == 1U &&
                shanghai_page.ticks[0U]
                        .common.tick_stream_sequence == 1U,
            "read valid Shanghai row before zero tick seam")) {
        service->StopControl();
        return false;
    }
    ipc::RealtimeInstrumentTickDeltaReadResponseV2
        zero_tick_failure{};
    ok &= Expect(
        client.Read(
            1U,
            shanghai_first.next_read_token,
            &zero_tick_failure,
            &empty_page) &&
            status_is(
                zero_tick_failure.status,
                ipc::
                    RealtimeInstrumentTickDeltaControlStatusV2::
                        kInternalFailure) &&
            !terminal(zero_tick_failure) &&
            zero_tick_failure.record_count == 0U &&
            zero_tick_failure.page_mapping_bytes == 0U &&
            !service->failed(),
        "zero tick fails only the V2 cursor without coverage loss");

    shanghai_summary_cursor.reset();
    first_generation.reset();
    TickDeltaWireClient advanced_client;
    ipc::RealtimeInstrumentTickDeltaOpenSessionResponseV2
        advanced_session{};
    if (!Expect(
            advanced_client.OpenSession(
                socket_path, &advanced_session) &&
                status_is(
                    advanced_session.status,
                    ipc::
                        RealtimeInstrumentTickDeltaControlStatusV2::
                            kOk) &&
                advanced_session.target_generation.generation == 2U &&
                advanced_session.target_generation
                        .recv_monotonic_cut_ns == 80'001U,
            "new V2 session pins the subsequently published generation 2")) {
        service->StopControl();
        return false;
    }
    ipc::RealtimeInstrumentTickDeltaOpenInstrumentResponseV2
        advanced_open{};
    if (!Expect(
            advanced_client.OpenInstrument(
                kShanghaiInstrumentId,
                1U,
                ipc::
                    RealtimeInstrumentTickDeltaBaseKindV2::kCheckpoint,
                shanghai_checkpoint,
                &advanced_open) &&
                status_is(
                    advanced_open.status,
                    ipc::
                        RealtimeInstrumentTickDeltaControlStatusV2::
                            kOk),
            "open generation 1 checkpoint against generation 2 target")) {
        service->StopControl();
        return false;
    }
    ok &= Expect(
        advanced_open.metadata.base_checkpoint.generation.generation ==
                1U &&
            advanced_open.metadata
                    .target_checkpoint.generation.generation == 2U &&
            advanced_open.metadata
                    .target_checkpoint.instrument_tick_source_record_counts ==
                shanghai_checkpoint
                    .instrument_tick_source_record_counts &&
            advanced_open.metadata.delta_tick_record_count == 0U &&
            advanced_open.metadata
                    .ingress_sequence_begin_inclusive == 8U &&
            advanced_open.metadata
                    .ingress_sequence_end_exclusive == 8U &&
            advanced_open.metadata
                    .tick_stream_sequence_begin_inclusive == 6U &&
            advanced_open.metadata
                    .tick_stream_sequence_end_exclusive == 6U &&
            advanced_open.initial_read_token !=
                advanced_session.delta_session_token,
        "durable prior-generation checkpoint advances to exact gen2 cut");
    ipc::RealtimeInstrumentTickDeltaReadResponseV2 advanced_eof{};
    ok &= Expect(
        advanced_client.Read(
            0U,
            advanced_open.initial_read_token,
            &advanced_eof,
            &empty_page) &&
            status_is(
                advanced_eof.status,
                ipc::
                    RealtimeInstrumentTickDeltaControlStatusV2::kOk) &&
            terminal(advanced_eof) &&
            advanced_eof.record_count == 0U &&
            advanced_eof.page_mapping_bytes == 0U &&
            advanced_eof.page_index == 0U &&
            advanced_eof.target_generation == 2U,
        "cross-generation empty delta ends with explicit gen2 EOF");

    ipc::RealtimeControlResponseV1 v1_response{};
    UniqueFd v1_data_fd(
        ReceiveSessionFd(socket_path, &v1_response));
    ok &= Expect(
        v1_data_fd.get() >= 0 &&
            v1_response.status ==
                static_cast<std::uint16_t>(
                    ipc::RealtimeControlStatusV1::kOk) &&
            v1_response.message_bytes == sizeof(v1_response) &&
            v1_response.session_epoch == 29U &&
            !service->failed(),
        "V1 GET_SESSION remains compatible after V2 cursor failure");

    service->StopControl();
    return ok;
}

bool TestServiceAndReader(MarketFixture& fixture) {
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
    HistoryPageStageObserver history_stage_observer;
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
    config.maximum_history_page_records = 2U;
    config.history_stage_observer = &history_stage_observer;
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
    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        store_generation;
    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        second_store_generation;
    ok &= fixture.BuildStoreGeneration(
        service_run_id, 1U, 60'000U, &store_generation);
    ok &= fixture.BuildStoreGeneration(
        service_run_id, 2U, 60'001U, &second_store_generation);
    ok &= Expect(
        store_generation != nullptr &&
            second_store_generation != nullptr &&
            store_generation->store_session_epoch() != 0U &&
            second_store_generation->store_session_epoch() ==
                store_generation->store_session_epoch() &&
            second_store_generation->record_count() ==
                store_generation->record_count(),
        "same Store builds a valid empty-increment generation");
    ok &= Expect(
        store_generation != nullptr &&
            service->PublishStoreGeneration(store_generation),
        "publish exact immutable Store generation");
    ok &= Expect(
        kline_generation != nullptr &&
            service->PublishKLineGeneration(*kline_generation),
        "publish exact immutable KLine generation");

#if defined(L2FLOW_IPC_E2E_PYTHON_EXECUTABLE)
    ok &= RunPythonE2EProbe(
        socket_path, service, second_store_generation);
    ok &= Expect(
        history_stage_observer.completed() == 4U &&
            !history_stage_observer.overflow(),
        "history stage observer receives both pinned generations");
    if (history_stage_observer.completed() == 4U) {
        for (std::size_t index = 0U; index < 4U; ++index) {
            const ipc::RealtimeHistoryPageStageTimingV1& timing =
                history_stage_observer.timing(index);
            const std::size_t generation_page = index % 2U;
            const std::uint64_t named_build_stages =
                timing.cursor_read_ns +
                timing.classify_layout_ns +
                timing.memfd_prepare_ns +
                timing.projection_ns +
                timing.memfd_finalize_ns;
            const std::uint32_t expected_snapshot_count =
                generation_page == 0U ? 1U : 0U;
            const std::uint32_t expected_tick_count =
                generation_page == 0U ? 1U : 2U;
            const std::uint64_t expected_mapping_bytes =
                sizeof(ipc::RealtimeHistoryPageHeaderV1) +
                2U * sizeof(
                         ipc::RealtimeHistoryRecordDescriptorV1) +
                static_cast<std::uint64_t>(
                    expected_snapshot_count) *
                    sizeof(ipc::RealtimeWireSnapshotPayloadV1) +
                static_cast<std::uint64_t>(expected_tick_count) *
                    sizeof(ipc::RealtimeWireTickPayloadV1);
            ok &= Expect(
                timing.open_request_id != 0U &&
                    timing.read_request_id != 0U &&
                    timing.generation ==
                        (index < 2U ? 1U : 2U) &&
                    timing.instrument_id ==
                        kShanghaiInstrumentId &&
                    timing.page_index == generation_page &&
                    timing.record_count == 2U &&
                    timing.snapshot_count ==
                        expected_snapshot_count &&
                    timing.tick_count == expected_tick_count &&
                    timing.page_mapping_bytes ==
                        expected_mapping_bytes &&
                    timing.clock_read_failures == 0U &&
                    timing.cursor_read_ns != 0U &&
                    timing.classify_layout_ns != 0U &&
                    timing.memfd_prepare_ns != 0U &&
                    timing.projection_ns != 0U &&
                    timing.memfd_finalize_ns != 0U &&
                    timing.token_ns != 0U &&
                    timing.send_ns != 0U &&
                    timing.build_total_ns >= named_build_stages,
                "history stage timing identity, mix, and durations are valid");
        }
        ok &= Expect(
            history_stage_observer.timing(0U).open_request_id ==
                    history_stage_observer.timing(1U).open_request_id &&
                history_stage_observer.timing(0U).read_request_id !=
                    history_stage_observer.timing(1U).read_request_id &&
                history_stage_observer.timing(2U).open_request_id ==
                    history_stage_observer.timing(3U).open_request_id &&
                history_stage_observer.timing(2U).read_request_id !=
                    history_stage_observer.timing(3U).read_request_id &&
                history_stage_observer.timing(0U).open_request_id !=
                    history_stage_observer.timing(2U).open_request_id,
            "history stage timing distinguishes pinned cursor identities");
    }
#else
    ok &= Expect(
        second_store_generation != nullptr &&
            service->PublishStoreGeneration(second_store_generation) &&
            !service->failed(),
        "publish valid empty-increment Store generation without Python");
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
        publish(kHistoryLongRawTick, kShanghaiInstrumentId) &&
            !service->failed(),
        "production applied sink retains tick with oversized raw strings");
    std::array<std::uint32_t, 1U> long_raw_id{
        kShanghaiInstrumentId};
    std::array<ipc::RealtimeWireTickPayloadV1, 1U>
        long_raw_latest{};
    std::array<std::uint8_t, 1U> long_raw_status{};
    ok &= Expect(
        l2flow_shm_reader_latest_ticks_v1(
            reader.get(),
            long_raw_id.data(),
            long_raw_id.size(),
            long_raw_latest.data(),
            sizeof(long_raw_latest[0U]),
            long_raw_status.data()) ==
                L2FLOW_SHM_READER_OK_V1 &&
            StatusEquals(
                long_raw_status[0U],
                L2FLOW_LATEST_AVAILABLE_V1) &&
            long_raw_latest[0U].common.tick_stream_sequence == 4U &&
            long_raw_latest[0U].projection_flags ==
                (ipc::kRealtimeWireTickRawTypeOmittedV1 |
                 ipc::kRealtimeWireTickRawTickFlagOmittedV1) &&
            long_raw_latest[0U].raw_type_length == 0U &&
            long_raw_latest[0U].raw_tick_flag_length == 0U,
        "latest tick exposes explicit oversized-raw omission flags");
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
            session.tick_highest_published_sequence == 4U &&
            session.tick_contiguous_published_sequence == 4U &&
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

bool TestForeignStoreGenerationFailsClosed() {
    MarketFixture owner_fixture;
    MarketFixture foreign_fixture;
    if (!owner_fixture.Initialize() || !foreign_fixture.Initialize()) {
        return false;
    }

    bool ok = true;
    ok &= Expect(
        owner_fixture.registry.get() != foreign_fixture.registry.get() &&
            owner_fixture.registry->registry_version() ==
                foreign_fixture.registry->registry_version() &&
            owner_fixture.registry->registry_sha256() ==
                foreign_fixture.registry->registry_sha256(),
        "foreign Store fixture has an independent registry object "
        "with the same registry identity");

    common::Identity128 run_id{};
    run_id[0U] = std::byte{0x6cU};
    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        owner_generation;
    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        foreign_generation;
    ok &= owner_fixture.BuildStoreGeneration(
        run_id, 1U, 70'000U, &owner_generation);
    ok &= foreign_fixture.BuildStoreGeneration(
        run_id, 2U, 70'001U, &foreign_generation);
    if (owner_generation == nullptr || foreign_generation == nullptr) {
        return false;
    }

    bool same_source_cuts = true;
    for (std::size_t source = 0U;
         source <
         owner_generation->watermark().sources.size();
         ++source) {
        same_source_cuts =
            same_source_cuts &&
            owner_generation->watermark()
                    .sources[source]
                    .source_stream_id ==
                foreign_generation->watermark()
                    .sources[source]
                    .source_stream_id &&
            owner_generation->watermark()
                    .sources[source]
                    .sequence_exclusive ==
            foreign_generation->watermark()
                    .sources[source]
                    .sequence_exclusive;
    }
    bool same_instrument_counts = true;
    for (std::size_t ordinal = 0U;
         ordinal < owner_generation->instrument_count();
         ++ordinal) {
        market::IntradayInstrumentSummaryV1 owner_summary{};
        market::IntradayInstrumentSummaryV1 foreign_summary{};
        same_instrument_counts =
            same_instrument_counts &&
            owner_generation->SummaryAt(ordinal, &owner_summary) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            foreign_generation->SummaryAt(ordinal, &foreign_summary) ==
                market::IntradayInstrumentStoreQueryErrorV1::kNone &&
            owner_summary.instrument_id ==
                foreign_summary.instrument_id &&
            owner_summary.record_count ==
                foreign_summary.record_count &&
            owner_summary.source_record_counts ==
                foreign_summary.source_record_counts;
    }
    ok &= Expect(
        owner_generation->store_session_epoch() != 0U &&
            foreign_generation->store_session_epoch() != 0U &&
            owner_generation->store_session_epoch() !=
                foreign_generation->store_session_epoch() &&
            owner_generation->record_count() ==
                foreign_generation->record_count() &&
            owner_generation->watermark()
                    .ingress_sequence_exclusive ==
                foreign_generation->watermark()
                    .ingress_sequence_exclusive &&
            owner_generation->watermark().generation == 1U &&
            foreign_generation->watermark().generation == 2U &&
            owner_generation->watermark().recv_monotonic_cut_ns ==
                70'000U &&
            foreign_generation->watermark().recv_monotonic_cut_ns ==
                70'001U &&
            owner_generation->watermark().run_id ==
                foreign_generation->watermark().run_id &&
            owner_generation->watermark().trade_date ==
                foreign_generation->watermark().trade_date &&
            owner_generation->watermark().registry_version ==
                foreign_generation->watermark().registry_version &&
            owner_generation->watermark().registry_sha256 ==
                foreign_generation->watermark().registry_sha256 &&
            owner_generation->coverage_from_open() ==
                foreign_generation->coverage_from_open() &&
            same_source_cuts && same_instrument_counts,
        "foreign generation matches service continuity metadata "
        "except Store provenance and expected monotonic cuts");

    ScopedTempDirectory directory;
    if (!Expect(
            directory.valid(),
            "create private foreign-Store IPC directory")) {
        return false;
    }
    const std::filesystem::path socket_path =
        directory.path() / "foreign-store.sock";
    ipc::RealtimeSharedServiceConfigV1 config{};
    config.run_id = run_id;
    config.session_epoch = 23U;
    config.trade_date = kTradeDate;
    config.registry = owner_fixture.registry.get();
    config.tick_ring_capacity = 2U;
    config.maximum_mapping_bytes = 16U * 1024U * 1024U;
    config.control_socket_path = socket_path;

    std::shared_ptr<ipc::RealtimeSharedMarketServiceV1> service;
    int system_error = 0;
    const auto create_error =
        ipc::RealtimeSharedMarketServiceV1::Create(
            std::move(config), &service, &system_error);
    if (!Expect(
            create_error ==
                    ipc::RealtimeSharedServiceCreateErrorV1::kNone &&
                service != nullptr && system_error == 0,
            "create independent foreign-Store test service")) {
        return false;
    }
    if (!Expect(
            service->Start(&system_error) && system_error == 0,
            "start independent foreign-Store test service")) {
        service->StopControl();
        return false;
    }

    ok &= Expect(
        service->PublishStoreGeneration(owner_generation) &&
            !service->failed(),
        "independent service accepts its first Store provenance");
    ok &= Expect(
        !service->PublishStoreGeneration(foreign_generation) &&
            service->failed(),
        "same-registry generation from a foreign Store fails closed");
    ok &= Expect(
        !service->PublishApplied(
            owner_fixture.Ordinal(kShanghaiInstrumentId),
            *owner_fixture.records[kShanghaiSnapshot]),
        "failed service rejects publication after foreign Store provenance");
    service->StopControl();
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
        std::thread first_stop(
            [&clean]() { clean->StopControl(); });
        std::thread second_stop(
            [&clean]() { clean->StopControl(); });
        first_stop.join();
        second_stop.join();
        ok &= Expect(
            !clean->failed(),
            "concurrent control stop calls are serialized safely");
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
    ok &= TestForeignStoreGenerationFailsClosed();
    ok &= TestInstrumentTickDeltaV2();
    ok &= TestServiceAndReader(fixture);
    ok &= TestServiceLifecycle(fixture);
    if (!ok) {
        return 1;
    }
    std::cout << "realtime IPC V1 tests passed\n";
    return 0;
}
