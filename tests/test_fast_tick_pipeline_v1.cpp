#include "l2flow/runtime/fast_tick_pipeline_v1.h"
#include "l2flow/sdk/market_message_catalog_v1.h"
#include "l2flow/sdk/production_subscription_v1.h"
#include "l2flow/sdk/vendor_head_view.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <span>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

namespace market = l2flow::market;
namespace mdl = datayes::mdl;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;

constexpr std::uint32_t kTradeDate = 20260805U;

bool Expect(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
    }
    return condition;
}

class WireWriter final {
public:
    explicit WireWriter(std::size_t fixed_bytes)
        : bytes_(fixed_bytes, std::byte{0U}) {}

    void StoreU16(std::size_t offset, std::uint16_t value) {
        StoreUnsigned(offset, value);
    }
    void StoreU32(std::size_t offset, std::uint32_t value) {
        StoreUnsigned(offset, value);
    }
    void StoreU64(std::size_t offset, std::uint64_t value) {
        StoreUnsigned(offset, value);
    }
    void StoreString(std::size_t descriptor, std::string_view value) {
        const std::size_t start = bytes_.size();
        StoreU16(descriptor, static_cast<std::uint16_t>(value.size()));
        StoreU32(
            descriptor + sizeof(std::uint16_t),
            value.empty()
                ? 0U
                : static_cast<std::uint32_t>(start - descriptor));
        const auto characters =
            std::span<const char>(value.data(), value.size());
        const auto bytes = std::as_bytes(characters);
        bytes_.insert(bytes_.end(), bytes.begin(), bytes.end());
    }
    [[nodiscard]] std::vector<std::byte> Take() && {
        return std::move(bytes_);
    }

private:
    template <typename Unsigned>
    void StoreUnsigned(std::size_t offset, Unsigned value) {
        static_assert(std::is_unsigned_v<Unsigned>);
        for (std::size_t index = 0U; index < sizeof(Unsigned); ++index) {
            bytes_.at(offset + index) = static_cast<std::byte>(
                (value >> static_cast<unsigned int>(index * 8U)) &
                static_cast<Unsigned>(0xffU));
        }
    }

    std::vector<std::byte> bytes_;
};

std::vector<std::byte> ShanghaiTradeBody(std::uint64_t sequence) {
    WireWriter writer(70U);
    writer.StoreU64(0U, sequence);
    writer.StoreU32(8U, 7U);
    writer.StoreU32(18U, 93'000'125U);
    writer.StoreU64(28U, 11'001U);
    writer.StoreU64(36U, 22'002U);
    writer.StoreU32(44U, 12'345U);
    writer.StoreU64(48U, 41U);
    writer.StoreU64(56U, 506'145U);
    writer.StoreString(12U, "600007");
    writer.StoreString(22U, "T");
    writer.StoreString(64U, "B");
    return std::move(writer).Take();
}

std::vector<std::byte> ShenzhenOrderBody(std::uint64_t sequence) {
    WireWriter writer(58U);
    writer.StoreU32(0U, 12U);
    writer.StoreU64(4U, sequence);
    writer.StoreU64(30U, 123'456U);
    writer.StoreU64(38U, 201U);
    writer.StoreU32(46U, 49U);
    writer.StoreU32(50U, 93'000'124U);
    writer.StoreU32(54U, 50U);
    writer.StoreString(12U, "010");
    writer.StoreString(18U, "000001");
    writer.StoreString(24U, "102 ");
    return std::move(writer).Take();
}

std::vector<std::byte> ShenzhenTransactionBody(std::uint64_t sequence) {
    WireWriter writer(70U);
    writer.StoreU32(0U, 12U);
    writer.StoreU64(4U, sequence);
    writer.StoreU64(18U, 201U);
    writer.StoreU64(26U, 301U);
    writer.StoreU64(46U, 123'456U);
    writer.StoreU64(54U, 33U);
    writer.StoreU32(62U, 70U);
    writer.StoreU32(66U, 93'000'124U);
    writer.StoreString(12U, "010");
    writer.StoreString(34U, "000001");
    writer.StoreString(40U, "102 ");
    return std::move(writer).Take();
}

class FakeMessage final : public mdl::MDLMessage {
public:
    FakeMessage(sdk::MessageKey key, std::vector<std::byte> body)
        : body_(std::move(body)) {
        std::memset(&head_, 0, sizeof(head_));
        head_.HeadSize =
            static_cast<std::uint8_t>(sdk::kVendorHeadBytes);
        head_.MessageSize = static_cast<std::uint32_t>(
            sdk::kVendorHeadBytes + body_.size());
        head_.MessageEncoding =
            static_cast<std::uint8_t>(mdl::MDLEID_BINARY);
        head_.ServiceID = key.service_id;
        head_.ServiceVersion = key.service_version;
        head_.MessageID = key.message_id;
        head_.LocalTime.m_Value = 93'000'000U;
        head_.SequenceID = 77U;
    }

    void AddRef() override {}
    int ReleaseRef() override { return 1; }
    mdl::MDLMessageHead* GetHead() const override {
        return const_cast<mdl::MDLMessageHead*>(&head_);
    }
    char* GetBody() const override {
        return body_.empty()
                   ? nullptr
                   : reinterpret_cast<char*>(
                         const_cast<std::byte*>(body_.data()));
    }
    mdl::MDLMessage* _Copy() const override { return nullptr; }

    void DestroyCallbackBytes() noexcept {
        std::fill(body_.begin(), body_.end(), std::byte{0xffU});
    }

private:
    mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
};

std::vector<std::byte> Bytes(std::string_view value) {
    const auto chars = std::span<const char>(value.data(), value.size());
    const auto bytes = std::as_bytes(chars);
    return std::vector<std::byte>(bytes.begin(), bytes.end());
}

std::shared_ptr<const market::DailyInstrumentCatalogV2> Catalog(
    bool* ok) {
    const market::InstrumentMetadataV2 metadata{
        market::QuantityUnitV1::kShare,
        market::SecurityTypeV1::kEquity,
        market::AssetScopeV1::kDocumentedCore};
    std::array<market::DailyInstrumentSourceEntryV2, 2U> entries{};
    entries[0U].key.market = market::MarketV1::kShanghai;
    entries[0U].key.security_id = Bytes("600007");
    entries[0U].metadata = metadata;
    entries[1U].key.market = market::MarketV1::kShenzhen;
    entries[1U].key.security_id_source = Bytes("102 ");
    entries[1U].key.security_id = Bytes("000001");
    entries[1U].metadata = metadata;
    market::DailyInstrumentCatalogConfigV2 config{};
    config.trade_date = kTradeDate;
    config.catalog_version = 1U;
    config.session_epoch = 1U;
    config.coverage_complete = true;
    std::unique_ptr<market::DailyInstrumentCatalogV2> catalog;
    *ok &= Expect(
        market::DailyInstrumentCatalogV2::Create(
            config, entries, &catalog) ==
                market::DailyInstrumentCatalogCreateErrorV2::kNone &&
            catalog != nullptr,
        "create complete daily catalog");
    return std::shared_ptr<const market::DailyInstrumentCatalogV2>(
        std::move(catalog));
}

l2flow::common::Identity128 SessionId() {
    l2flow::common::Identity128 result{};
    result[0U] = std::byte{0x42U};
    result[15U] = std::byte{0x24U};
    return result;
}

runtime::FastTickPipelineConfigV1 Config(
    std::shared_ptr<const market::DailyInstrumentCatalogV2> catalog) {
    runtime::FastTickPipelineConfigV1 result{};
    result.run_id = SessionId();
    result.trade_date = kTradeDate;
    result.daily_catalog = std::move(catalog);
    result.source_stream_ids = {101U, 202U};
    result.maximum_sdk_message_bytes = 4096U;
    result.decoder_queue_capacity_per_source = 16U;
    result.decoder_batch_budget = 4U;
    result.maximum_inflight_messages = 32U;
    result.prewarm_message_bytes = 512U;
    result.prewarm_message_count = 32U;

    result.planes.fast.session_id = SessionId();
    result.planes.fast.trade_date = kTradeDate;
    result.planes.fast.instrument_count = 2U;
    result.planes.fast.worker_count = 1U;
    result.planes.fast.maximum_session_records = 128U;
    result.planes.fast.records_per_chunk = 4U;
    result.planes.fast.maximum_records_per_read = 16U;
    result.planes.fast.coverage_from_open = true;
    result.planes.fast.tick_routes = {0U, 0U};

    result.planes.event.session_id = SessionId();
    result.planes.event.trade_date = kTradeDate;
    result.planes.event.instrument_count = 2U;
    result.planes.event.worker_count = 1U;
    result.planes.event.maximum_order_states_per_instrument = 128U;
    result.planes.event.maximum_inputs_per_instrument = 128U;
    result.planes.event.maximum_events_per_instrument = 512U;
    result.planes.event.input_block_records = 4U;
    result.planes.event.event_block_records = 4U;
    result.planes.event.cdc_range_chunk_records = 4U;
    result.planes.event.maximum_change_records_per_instrument = 2048U;
    result.planes.event.maximum_changes_per_read = 32U;
    result.planes.event.event_routes = {0U, 0U};

    result.planes.kline.session_id = SessionId();
    result.planes.kline.trade_date = kTradeDate;
    result.planes.kline.instrument_count = 2U;
    result.planes.kline.worker_count = 1U;
    result.planes.kline.windows = {{1U, 1'000'000'000U}};
    result.planes.kline.maximum_trades_per_instrument = 128U;
    result.planes.kline.maximum_bars_per_instrument = 128U;
    result.planes.kline.maximum_change_records_per_instrument = 1024U;
    result.planes.kline.maximum_changes_per_read = 32U;
    result.planes.kline.kline_routes = {0U, 0U};
    result.planes.tick_queue_capacity_per_source_worker = 16U;
    result.planes.event_queue_capacity_per_source_worker = 16U;
    result.planes.kline_queue_capacity_per_source_worker = 16U;
    result.planes.live_batch_budget = 4U;
    return result;
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

std::uint32_t InstrumentId(
    const market::DailyInstrumentCatalogV2& catalog,
    market::MarketV1 market_value) {
    for (const auto& entry : catalog.entries()) {
        if (entry.key.market == market_value) {
            return entry.instrument_id;
        }
    }
    return 0U;
}

}  // namespace

int main() {
    bool ok = true;
    static_assert(sdk::kProductionMessageCountV1 == 3U);
    ok &= Expect(
        sdk::kProductionMessageKeysV1[0U] ==
                sdk::MessageKey{4U, 101U, 24U} &&
            sdk::kProductionMessageKeysV1[1U] ==
                sdk::MessageKey{6U, 101U, 33U} &&
            sdk::kProductionMessageKeysV1[2U] ==
                sdk::MessageKey{6U, 101U, 36U} &&
            sdk::IsForbiddenProductionSubscriptionV1(
                sdk::MessageKey{6U, 101U, 53U}),
        "production admits exactly three Tick tuples and forbids 6.101.53");

    auto catalog = Catalog(&ok);
    if (catalog == nullptr) {
        return 1;
    }
    std::unique_ptr<runtime::FastTickPipelineV1> pipeline;
    std::string detail;
    ok &= Expect(
        runtime::FastTickPipelineV1::Create(
            Config(catalog), &pipeline, &detail) ==
                runtime::FastTickPipelineCreateErrorV1::kNone &&
            pipeline != nullptr,
        "create two-source FAST Tick pipeline");
    if (pipeline == nullptr) {
        std::cerr << detail << '\n';
        return 1;
    }

    FakeMessage sh(
        sdk::kProductionMessageKeysV1[0U], ShanghaiTradeBody(100U));
    const auto sh_result = pipeline->IngestForTest(&sh, 1U, 1U);
    sh.DestroyCallbackBytes();
    FakeMessage sz_order(
        sdk::kProductionMessageKeysV1[1U], ShenzhenOrderBody(200U));
    const auto order_result = pipeline->IngestForTest(
        &sz_order, 2U, 2U);
    sz_order.DestroyCallbackBytes();
    FakeMessage sz_trade(
        sdk::kProductionMessageKeysV1[2U],
        ShenzhenTransactionBody(201U));
    const auto trade_result = pipeline->IngestForTest(
        &sz_trade, 3U, 3U);
    sz_trade.DestroyCallbackBytes();
    ok &= Expect(
        sh_result.accepted() && order_result.accepted() &&
            trade_result.accepted() &&
            sh_result.source_slot == 0U &&
            order_result.source_slot == 1U &&
            trade_result.source_slot == 1U &&
            order_result.source_sequence == 1U &&
            trade_result.source_sequence == 2U,
        "Shenzhen Order/Transaction retain one shared source order");

    const std::uint32_t sh_id = InstrumentId(
        *catalog, market::MarketV1::kShanghai);
    const std::uint32_t sz_id = InstrumentId(
        *catalog, market::MarketV1::kShenzhen);
    ok &= Expect(
        WaitUntil([&] {
            const auto snapshot = pipeline->Snapshot();
            return snapshot.decoded_messages == 3U &&
                   snapshot.planes.fast_applied == 3U &&
                   snapshot.planes.event_applied >= 2U &&
                   snapshot.planes.kline_applied >= 2U;
        }),
        "two decoders feed independent FAST/Event/KLine workers");

    market::FastTickInstrumentStatusV1 sh_status{};
    market::FastTickInstrumentStatusV1 sz_status{};
    ok &= Expect(
        pipeline->planes().fast_store().Status(sh_id, &sh_status) ==
                market::FastTickStoreQueryErrorV1::kNone &&
            pipeline->planes().fast_store().Status(sz_id, &sz_status) ==
                market::FastTickStoreQueryErrorV1::kNone &&
            sh_status.published_tail == 1U &&
            sz_status.published_tail == 2U &&
            sh_status.coverage_complete && sz_status.coverage_complete,
        "FAST publishes independent instrument-local tails");

    market::EventStableSnapshotV1 sh_events{};
    market::EventStableSnapshotV1 sz_events{};
    ok &= Expect(
        pipeline->planes().event_history().AcquireStable(
            sh_id, &sh_events) ==
                market::OrderedEventHistoryErrorV1::kNone &&
            pipeline->planes().event_history().AcquireStable(
                sz_id, &sz_events) ==
                market::OrderedEventHistoryErrorV1::kNone &&
            sh_events.root != nullptr && sz_events.root != nullptr &&
            sh_events.root->strictly_ordered() &&
            sz_events.root->strictly_ordered(),
        "derived Event readers acquire ordered immutable roots");

    const auto live = pipeline->Snapshot();
    ok &= Expect(
        live.accepted_messages == 3U &&
            live.accepted_by_source[0U] == 1U &&
            live.accepted_by_source[1U] == 2U &&
            !live.fatal,
        "pipeline counters expose no global market-order cursor");
    pipeline->StopAndDrain();
    const auto stopped = pipeline->Snapshot();
    ok &= Expect(stopped.stopped && !stopped.fatal, "cleanly drain pipeline");

    auto isolated_config = Config(catalog);
    isolated_config.planes.fast.maximum_session_records = 2U;
    isolated_config.planes.fast.records_per_chunk = 2U;
    std::unique_ptr<runtime::FastTickPipelineV1> isolated;
    ok &= Expect(
        runtime::FastTickPipelineV1::Create(
            std::move(isolated_config), &isolated, &detail) ==
                runtime::FastTickPipelineCreateErrorV1::kNone &&
            isolated != nullptr,
        "create pipeline with one FAST row per instrument");
    if (isolated != nullptr) {
        FakeMessage first_sh(
            sdk::kProductionMessageKeysV1[0U],
            ShanghaiTradeBody(300U));
        ok &= Expect(
            isolated->IngestForTest(&first_sh, 10U, 10U).accepted(),
            "accept first local-capacity Shanghai row");
        ok &= Expect(
            WaitUntil([&] {
                market::FastTickInstrumentStatusV1 status{};
                return isolated->planes().fast_store().Status(
                           sh_id, &status) ==
                           market::FastTickStoreQueryErrorV1::kNone &&
                       status.published_tail == 1U;
            }),
            "publish the Shanghai instrument capacity");

        FakeMessage overflowing_sh(
            sdk::kProductionMessageKeysV1[0U],
            ShanghaiTradeBody(301U));
        ok &= Expect(
            isolated->IngestForTest(
                        &overflowing_sh, 11U, 11U)
                .accepted(),
            "callback admission remains nonblocking before FAST exhaustion");
        ok &= Expect(
            WaitUntil([&] {
                market::FastTickInstrumentStatusV1 status{};
                return isolated->planes().fast_store().Status(
                           sh_id, &status) ==
                           market::FastTickStoreQueryErrorV1::kNone &&
                       !status.coverage_complete;
            }),
            "capacity exhaustion fails closed only for Shanghai");

        FakeMessage dropped_sh(
            sdk::kProductionMessageKeysV1[0U],
            ShanghaiTradeBody(302U));
        ok &= Expect(
            isolated->IngestForTest(&dropped_sh, 12U, 12U).accepted(),
            "source callback still admits work without blocking");
        FakeMessage healthy_sz(
            sdk::kProductionMessageKeysV1[1U],
            ShenzhenOrderBody(400U));
        ok &= Expect(
            isolated->IngestForTest(&healthy_sz, 13U, 13U).accepted(),
            "unrelated Shenzhen instrument remains admissible");
        ok &= Expect(
            WaitUntil([&] {
                market::FastTickInstrumentStatusV1 status{};
                const auto snapshot = isolated->Snapshot();
                return isolated->planes().fast_store().Status(
                           sz_id, &status) ==
                           market::FastTickStoreQueryErrorV1::kNone &&
                       status.published_tail == 1U &&
                       status.coverage_complete &&
                       snapshot.planes.fast_unrecoverable_drops >= 1U &&
                       !snapshot.fatal;
            }),
            "lost FAST instrument stops queue use while healthy FAST advances");
        isolated->StopAndDrain();
        ok &= Expect(
            isolated->Snapshot().stopped &&
                !isolated->Snapshot().fatal,
            "instrument-local FAST loss drains without a global fatal state");
    }

    std::unique_ptr<runtime::FastTickPipelineV1> fatal_pipeline;
    ok &= Expect(
        runtime::FastTickPipelineV1::Create(
            Config(catalog), &fatal_pipeline, &detail) ==
                runtime::FastTickPipelineCreateErrorV1::kNone &&
            fatal_pipeline != nullptr,
        "create fatal-admission coverage fixture");
    if (fatal_pipeline != nullptr) {
        FakeMessage combined(
            sdk::MessageKey{6U, 101U, 53U}, std::vector<std::byte>{});
        const auto rejected = fatal_pipeline->IngestForTest(
            &combined, 20U, 20U);
        market::FastTickInstrumentStatusV1 sh_fatal{};
        market::FastTickInstrumentStatusV1 sz_fatal{};
        ok &= Expect(
            rejected.error == runtime::FastTickPipelineIngressErrorV1::
                                  kForbiddenCombinedTick &&
                fatal_pipeline->Snapshot().fatal &&
                fatal_pipeline->planes().fast_store().Status(
                    sh_id, &sh_fatal) ==
                    market::FastTickStoreQueryErrorV1::kNone &&
                fatal_pipeline->planes().fast_store().Status(
                    sz_id, &sz_fatal) ==
                    market::FastTickStoreQueryErrorV1::kNone &&
                !sh_fatal.coverage_complete &&
                !sz_fatal.coverage_complete &&
                fatal_pipeline->planes().event_history().RepairState(
                    sh_id) == market::EventRepairStateV1::kUnrecoverable &&
                fatal_pipeline->planes().kline_history().RepairState(
                    sz_id) == market::EventRepairStateV1::kUnrecoverable,
            "an unattributable fatal callback clears every instrument coverage claim");
        fatal_pipeline->StopAndDrain();
    }
    return ok ? 0 : 1;
}
