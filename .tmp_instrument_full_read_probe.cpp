#include "l2flow/market/instrument_registry.h"
#include "l2flow/runtime/realtime_pipeline_v1.h"
#include "l2flow/sdk/market_message_catalog_v1.h"
#include "l2flow/sdk/sdk_runtime.h"

#include "mdl_api.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iomanip>
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

#include <sys/resource.h>
#include <sched.h>

namespace {

namespace market = l2flow::market;
namespace mdl = datayes::mdl;
namespace runtime = l2flow::runtime;
namespace sdk = l2flow::sdk;

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

constexpr std::uint32_t kUniverse = 50'000U;
constexpr std::uint32_t kMarketUniverse = 25'000U;
constexpr std::uint32_t kActivePerMarket = 3'000U;
constexpr std::uint32_t kActive = 6'000U;
constexpr std::uint64_t kRate = 100'000U;
constexpr std::uint64_t kBatch = 1'000U;
constexpr std::uint64_t kWarmup =
    static_cast<std::uint64_t>(kActivePerMarket) * 5U;
constexpr std::uint64_t kIntervalNs = 10'000U;
constexpr std::uint32_t kTradeDate = 20'260'725U;
constexpr std::array<std::uint32_t, 4U> kSourceStreamIds{
    1001U, 1002U, 2001U, 2002U};

std::uint64_t Ns(Clock::duration value) {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(value)
            .count());
}

double Us(std::uint64_t value) {
    return static_cast<double>(value) / 1'000.0;
}

double GiB(std::uint64_t value) {
    return static_cast<double>(value) /
           static_cast<double>(1ULL << 30U);
}

double PeakRssMiB() {
    struct rusage usage {};
    return ::getrusage(RUSAGE_SELF, &usage) == 0
               ? static_cast<double>(usage.ru_maxrss) / 1024.0
               : 0.0;
}

struct Stats final {
    std::uint64_t count = 0U;
    double p50 = 0.0;
    double p90 = 0.0;
    double p95 = 0.0;
    double p99 = 0.0;
    double p999 = 0.0;
    double maximum = 0.0;
};

Stats TimeStats(const std::vector<std::uint64_t>& input) {
    Stats result{};
    result.count = input.size();
    if (input.empty()) {
        return result;
    }
    std::vector<std::uint64_t> values = input;
    std::sort(values.begin(), values.end());
    const auto at = [&](double q) {
        const std::size_t index = static_cast<std::size_t>(
            std::ceil(q * static_cast<double>(values.size() - 1U)));
        return Us(values[std::min(index, values.size() - 1U)]);
    };
    result.p50 = at(0.50);
    result.p90 = at(0.90);
    result.p95 = at(0.95);
    result.p99 = at(0.99);
    result.p999 = at(0.999);
    result.maximum = Us(values.back());
    return result;
}

std::uint64_t CountQuantile(
    const std::vector<std::uint64_t>& input,
    double q) {
    if (input.empty()) {
        return 0U;
    }
    std::vector<std::uint64_t> values = input;
    std::sort(values.begin(), values.end());
    const std::size_t index = static_cast<std::size_t>(
        std::ceil(q * static_cast<double>(values.size() - 1U)));
    return values[std::min(index, values.size() - 1U)];
}

void PrintStats(
    std::string_view name,
    const std::vector<std::uint64_t>& values) {
    const Stats stats = TimeStats(values);
    std::cout << name << "_samples=" << stats.count
              << ' ' << name << "_p50_us=" << stats.p50
              << ' ' << name << "_p90_us=" << stats.p90
              << ' ' << name << "_p95_us=" << stats.p95
              << ' ' << name << "_p99_us=" << stats.p99
              << ' ' << name << "_p99_9_us=" << stats.p999
              << ' ' << name << "_max_us=" << stats.maximum
              << '\n';
}

class WireWriter final {
public:
    explicit WireWriter(std::size_t fixed)
        : bytes_(fixed, std::byte{0U}) {}

    void U16(std::size_t offset, std::uint16_t value) {
        Store(offset, value);
    }
    void U32(std::size_t offset, std::uint32_t value) {
        Store(offset, value);
    }
    void U64(std::size_t offset, std::uint64_t value) {
        Store(offset, value);
    }
    void Text(std::size_t descriptor, std::string_view value) {
        const std::size_t start = bytes_.size();
        U16(descriptor, static_cast<std::uint16_t>(value.size()));
        U32(
            descriptor + 2U,
            value.empty()
                ? 0U
                : static_cast<std::uint32_t>(start - descriptor));
        const auto encoded =
            std::as_bytes(std::span(value.data(), value.size()));
        bytes_.insert(bytes_.end(), encoded.begin(), encoded.end());
    }
    std::vector<std::byte> Take() && {
        return std::move(bytes_);
    }

private:
    template <typename T>
    void Store(std::size_t offset, T value) {
        static_assert(std::is_unsigned_v<T>);
        for (std::size_t index = 0U; index < sizeof(T); ++index) {
            bytes_[offset + index] = static_cast<std::byte>(
                (value >> (index * 8U)) & static_cast<T>(0xffU));
        }
    }
    std::vector<std::byte> bytes_;
};

std::vector<std::byte> Bytes(std::string_view value) {
    const auto bytes = std::as_bytes(std::span(value));
    return {bytes.begin(), bytes.end()};
}

std::string SixDigits(std::uint32_t value) {
    std::array<char, 7U> text{};
    std::snprintf(text.data(), text.size(), "%06u", value);
    return std::string(text.data(), 6U);
}

std::string ShId(std::uint32_t ordinal) {
    return SixDigits(600'000U + ordinal);
}

std::string SzId(std::uint32_t ordinal) {
    return SixDigits(ordinal + 1U);
}

std::unique_ptr<market::InstrumentRegistryV1> MakeRegistry() {
    std::vector<market::InstrumentRegistryEntryV1> entries;
    entries.reserve(kUniverse);
    for (std::uint32_t ordinal = 0U;
         ordinal < kMarketUniverse;
         ++ordinal) {
        market::InstrumentRegistryEntryV1 entry{};
        entry.instrument_id = ordinal + 1U;
        entry.key.market = market::MarketV1::kShanghai;
        entry.key.security_id = Bytes(ShId(ordinal));
        entry.quantity_unit = market::QuantityUnitV1::kShare;
        entry.security_type = market::SecurityTypeV1::kEquity;
        entry.asset_scope = market::AssetScopeV1::kDocumentedCore;
        entries.push_back(std::move(entry));
    }
    for (std::uint32_t ordinal = 0U;
         ordinal < kMarketUniverse;
         ++ordinal) {
        market::InstrumentRegistryEntryV1 entry{};
        entry.instrument_id = kMarketUniverse + ordinal + 1U;
        entry.key.market = market::MarketV1::kShenzhen;
        entry.key.security_id_source = Bytes("102 ");
        entry.key.security_id = Bytes(SzId(ordinal));
        entry.quantity_unit = market::QuantityUnitV1::kShare;
        entry.security_type = market::SecurityTypeV1::kEquity;
        entry.asset_scope = market::AssetScopeV1::kDocumentedCore;
        entries.push_back(std::move(entry));
    }
    std::unique_ptr<market::InstrumentRegistryV1> registry;
    const auto error = market::InstrumentRegistryV1::Create(
        2026072501ULL, entries, &registry);
    if (error != market::InstrumentRegistryCreateErrorV1::kNone) {
        std::cerr << "registry_error="
                  << static_cast<unsigned int>(error) << '\n';
        return nullptr;
    }
    return registry;
}

std::vector<std::byte> ShSnapshot(std::string_view id) {
    WireWriter writer(248U);
    writer.U32(0U, 93'000'123U);
    for (const std::size_t offset :
         {14U, 18U, 22U, 26U, 30U, 34U}) {
        writer.U32(offset, 12'360U);
    }
    writer.Text(4U, id);
    writer.Text(38U, "TRADE");
    return std::move(writer).Take();
}

std::vector<std::byte> ShTick(
    std::string_view id,
    std::uint64_t sequence) {
    WireWriter writer(70U);
    writer.U64(0U, sequence);
    writer.U32(8U, 1U);
    writer.U32(18U, 93'000'123U);
    writer.U64(28U, sequence);
    writer.U32(44U, 12'360U);
    writer.U64(48U, 100U);
    writer.U64(56U, 1'236'000U);
    writer.Text(12U, id);
    writer.Text(22U, "A");
    writer.Text(64U, "B");
    return std::move(writer).Take();
}

std::vector<std::byte> SzSnapshot(std::string_view id) {
    WireWriter writer(224U);
    writer.U32(0U, 93'000'123U);
    writer.U32(4U, 12U);
    writer.U64(32U, 12'000'000U);
    writer.U64(40U, 1U);
    writer.U64(48U, 100U);
    writer.U64(56U, 1'234'560U);
    writer.U64(64U, 12'345'600U);
    writer.Text(8U, "010");
    writer.Text(14U, id);
    writer.Text(20U, "102 ");
    writer.Text(26U, "T");
    return std::move(writer).Take();
}

std::vector<std::byte> SzOrder(
    std::string_view id,
    std::uint64_t sequence) {
    WireWriter writer(58U);
    writer.U32(0U, 12U);
    writer.U64(4U, sequence);
    writer.U64(30U, 123'456U);
    writer.U64(38U, 201U);
    writer.U32(46U, 49U);
    writer.U32(50U, 93'000'123U);
    writer.U32(54U, 50U);
    writer.Text(12U, "010");
    writer.Text(18U, id);
    writer.Text(24U, "102 ");
    return std::move(writer).Take();
}

std::vector<std::byte> SzTransaction(
    std::string_view id,
    std::uint64_t sequence) {
    WireWriter writer(70U);
    writer.U32(0U, 12U);
    writer.U64(4U, sequence);
    writer.U64(18U, sequence);
    writer.U64(26U, sequence + 1U);
    writer.U64(46U, 123'456U);
    writer.U64(54U, 201U);
    writer.U32(62U, 70U);
    writer.U32(66U, 93'000'123U);
    writer.Text(12U, "010");
    writer.Text(34U, id);
    writer.Text(40U, "102 ");
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
        head_.SequenceID = 9988U;
    }
    void AddRef() override {}
    int ReleaseRef() override { return 1; }
    mdl::MDLMessageHead* GetHead() const override {
        return const_cast<mdl::MDLMessageHead*>(&head_);
    }
    char* GetBody() const override {
        return reinterpret_cast<char*>(
            const_cast<std::byte*>(body_.data()));
    }
    mdl::MDLMessage* _Copy() const override { return nullptr; }

private:
    mdl::MDLMessageHead head_{};
    std::vector<std::byte> body_;
};

struct Pools final {
    std::vector<std::unique_ptr<FakeMessage>> sh_snapshot;
    std::vector<std::unique_ptr<FakeMessage>> sh_tick;
    std::vector<std::unique_ptr<FakeMessage>> sz_snapshot;
    std::vector<std::unique_ptr<FakeMessage>> sz_order;
    std::vector<std::unique_ptr<FakeMessage>> sz_transaction;
};

Pools MakePools() {
    Pools pools;
    pools.sh_snapshot.reserve(kActivePerMarket);
    pools.sh_tick.reserve(kActivePerMarket);
    pools.sz_snapshot.reserve(kActivePerMarket);
    pools.sz_order.reserve(kActivePerMarket);
    pools.sz_transaction.reserve(kActivePerMarket);
    for (std::uint32_t ordinal = 0U;
         ordinal < kActivePerMarket;
         ++ordinal) {
        const std::string id = ShId(ordinal);
        pools.sh_snapshot.push_back(std::make_unique<FakeMessage>(
            sdk::kProductionMessageKeysV1[0U], ShSnapshot(id)));
        pools.sh_tick.push_back(std::make_unique<FakeMessage>(
            sdk::kProductionMessageKeysV1[1U],
            ShTick(id, ordinal + 1U)));
    }
    for (std::uint32_t ordinal = 0U;
         ordinal < kActivePerMarket;
         ++ordinal) {
        const std::string id = SzId(ordinal);
        pools.sz_snapshot.push_back(std::make_unique<FakeMessage>(
            sdk::kProductionMessageKeysV1[2U], SzSnapshot(id)));
        pools.sz_order.push_back(std::make_unique<FakeMessage>(
            sdk::kProductionMessageKeysV1[3U],
            SzOrder(id, ordinal + 1U)));
        pools.sz_transaction.push_back(
            std::make_unique<FakeMessage>(
                sdk::kProductionMessageKeysV1[4U],
                SzTransaction(id, ordinal + 1U)));
    }
    return pools;
}

runtime::RealtimePipelineConfigV1 Config(
    const market::InstrumentRegistryV1* registry,
    std::uint64_t stress_records) {
    runtime::RealtimePipelineConfigV1 config{};
    config.run_id[0U] = std::byte{0x75U};
    config.run_id[15U] = std::byte{0xc2U};
    config.trade_date = kTradeDate;
    config.registry = registry;
    config.source_stream_ids = kSourceStreamIds;
    config.maximum_sdk_message_bytes = 4096U;
    config.decoder_queue_capacity_per_source = 4096U;
    config.store_worker_count = 4U;
    config.store_queue_capacity_per_source_worker = 4096U;
    config.intraday_store.segment_target_bytes = 64U * 1024U;
    config.intraday_store.maximum_session_records =
        kWarmup + stress_records + 1'000'000U;
    config.intraday_store.maximum_session_accounted_bytes =
        256ULL << 30U;
    config.intraday_store.maximum_records_per_batch = 65'536U;
    config.intraday_store.coverage_from_open = true;
    config.enforce_receive_trade_date = false;
    config.sdk.enabled = false;
    config.wal.enabled = false;
    return config;
}

bool WaitAppended(
    runtime::RealtimePipelineV1* pipeline,
    std::uint64_t expected) {
    const Clock::time_point deadline = Clock::now() + 30s;
    for (;;) {
        const auto snapshot = pipeline->Snapshot();
        if (snapshot.store.appended_records >= expected) {
            return snapshot.store.appended_records == expected;
        }
        if (snapshot.fatal || Clock::now() >= deadline) {
            std::cerr << "wait_append_failed expected=" << expected
                      << " accepted=" << snapshot.accepted_messages
                      << " decoded=" << snapshot.decoded_messages
                      << " appended=" << snapshot.store.appended_records
                      << " fatal=" << snapshot.fatal << '\n';
            return false;
        }
        std::this_thread::yield();
    }
}

bool Submit(
    runtime::RealtimePipelineV1* pipeline,
    const FakeMessage* message,
    std::uint32_t instrument_id,
    std::size_t kind,
    std::vector<std::uint64_t>* latest,
    std::array<std::uint64_t, 5U>* kinds) {
    const auto result = pipeline->InjectSdkMessageForTest(message);
    if (!result.accepted()) {
        std::cerr << "submit_failed error="
                  << runtime::RealtimePipelineIngressErrorNameV1(
                         result.error)
                  << " instrument=" << instrument_id << '\n';
        return false;
    }
    (*latest)[instrument_id] = result.global_ingress_sequence;
    ++(*kinds)[kind];
    return true;
}

bool ReadLatest(
    const market::IntradayInstrumentStoreGenerationV1& generation,
    std::uint32_t instrument_id,
    std::uint64_t expected,
    std::uint64_t* latency) {
    const Clock::time_point begin = Clock::now();
    std::unique_ptr<market::IntradayInstrumentCursorV1> cursor;
    const auto open_error =
        generation.OpenTailCursor(instrument_id, 1U, &cursor);
    std::array<const market::RealtimeHistoryRecordV1*, 1U> batch{};
    std::size_t written = 0U;
    const auto read_error =
        cursor == nullptr
            ? market::IntradayInstrumentStoreQueryErrorV1::kNotFound
            : cursor->ReadBatch(batch, &written);
    *latency = Ns(Clock::now() - begin);
    if (open_error != market::IntradayInstrumentStoreQueryErrorV1::kNone ||
        read_error != market::IntradayInstrumentStoreQueryErrorV1::kNone ||
        written != 1U || batch[0U] == nullptr ||
        batch[0U]->instrument_id() != instrument_id ||
        batch[0U]->ingress_sequence() != expected) {
        std::cerr << "latest_read_failed instrument=" << instrument_id
                  << " expected=" << expected
                  << " written=" << written << '\n';
        return false;
    }
    return true;
}

bool SweepLatest(
    const market::IntradayInstrumentStoreGenerationV1& generation,
    const std::vector<std::uint32_t>& active,
    const std::vector<std::uint64_t>& latest,
    std::vector<std::uint64_t>* point_latencies) {
    for (const std::uint32_t instrument_id : active) {
        std::uint64_t latency = 0U;
        if (!ReadLatest(
                generation,
                instrument_id,
                latest[instrument_id],
                &latency)) {
            return false;
        }
        point_latencies->push_back(latency);
    }
    return true;
}

void Pace(Clock::time_point target) {
    for (;;) {
        const Clock::time_point now = Clock::now();
        if (now >= target) {
            return;
        }
        const auto remaining = target - now;
        if (remaining > 250us) {
            std::this_thread::sleep_for(remaining - 100us);
        } else if (remaining > 30us) {
            std::this_thread::yield();
        }
    }
}

std::uint64_t XorTo(std::uint64_t value) {
    switch (value & 3U) {
        case 0U:
            return value;
        case 1U:
            return 1U;
        case 2U:
            return value + 1U;
        default:
            return 0U;
    }
}

std::array<std::uint64_t, 5U> ExpectedKinds(
    std::uint32_t seconds) {
    const std::uint64_t duration = seconds;
    return {
        kActivePerMarket + 3'000U * duration,
        kActivePerMarket + 41'500U * duration,
        kActivePerMarket + 3'400U * duration,
        kActivePerMarket + 26'050U * duration,
        kActivePerMarket + 26'050U * duration,
    };
}

std::array<std::uint64_t, 4U> ExpectedSources(
    std::uint32_t seconds) {
    const std::uint64_t duration = seconds;
    return {
        kActivePerMarket + 3'000U * duration,
        kActivePerMarket + 41'500U * duration,
        kActivePerMarket + 3'400U * duration,
        2U * kActivePerMarket + 52'100U * duration,
    };
}

std::uint64_t ExpectedInstrumentRecords(
    std::uint32_t instrument_id,
    std::uint32_t seconds) {
    std::uint64_t market_records = 0U;
    std::uint64_t warmup_records = 0U;
    std::uint32_t ordinal = 0U;
    if (instrument_id >= 1U &&
        instrument_id <= kActivePerMarket) {
        market_records =
            static_cast<std::uint64_t>(seconds) * 44'500U;
        warmup_records = 2U;
        ordinal = instrument_id - 1U;
    } else if (
        instrument_id > kMarketUniverse &&
        instrument_id <= kMarketUniverse + kActivePerMarket) {
        market_records =
            static_cast<std::uint64_t>(seconds) * 55'500U;
        warmup_records = 3U;
        ordinal = instrument_id - kMarketUniverse - 1U;
    } else {
        return 0U;
    }
    const std::uint64_t base =
        market_records / kActivePerMarket;
    const std::uint64_t remainder =
        market_records % kActivePerMarket;
    return warmup_records + base +
           (ordinal < remainder ? 1U : 0U);
}

bool MarkSequence(
    std::vector<std::uint64_t>* bitmap,
    std::uint64_t sequence,
    std::uint64_t maximum) {
    if (bitmap == nullptr || sequence == 0U || sequence > maximum) {
        return false;
    }
    const std::uint64_t zero_based = sequence - 1U;
    const std::size_t word =
        static_cast<std::size_t>(zero_based / 64U);
    const std::uint64_t mask =
        std::uint64_t{1U} << (zero_based % 64U);
    if (((*bitmap)[word] & mask) != 0U) {
        return false;
    }
    (*bitmap)[word] |= mask;
    return true;
}

struct ScanVerifier final {
    ScanVerifier(
        std::uint64_t expected_records,
        std::array<std::uint64_t, 4U> expected_sources)
        : ingress(
              static_cast<std::size_t>(
                  (expected_records + 63U) / 64U),
              0U) {
        for (std::size_t source = 0U;
             source < source_sequences.size();
             ++source) {
            source_sequences[source].assign(
                static_cast<std::size_t>(
                    (expected_sources[source] + 63U) / 64U),
                0U);
        }
    }

    std::vector<std::uint64_t> ingress;
    std::array<std::vector<std::uint64_t>, 4U> source_sequences;
};

struct ScanResult final {
    bool ok = false;
    std::uint64_t records = 0U;
    std::uint64_t sum = 0U;
    std::uint64_t xor_value = 0U;
    std::array<std::uint64_t, 5U> kinds{};
    std::array<std::uint64_t, 4U> sources{};
    std::uint64_t elapsed = 0U;
    std::uint64_t validation_elapsed = 0U;
    std::vector<std::uint64_t> batch_latencies;
};

const market::DecodedMarketCommonV1* StoredCommon(
    const market::RealtimeHistoryRecordV1& record) {
    const auto event = record.event();
    switch (record.kind()) {
        case market::MarketEventKindV1::kShanghaiSnapshot: {
            const auto* value =
                market::StoredMarketEventGetV1<
                    market::ShanghaiSnapshotV1>(event);
            return value == nullptr ? nullptr : &value->common;
        }
        case market::MarketEventKindV1::kShanghaiTick: {
            const auto* value =
                market::StoredMarketEventGetV1<
                    market::ShanghaiTickV1>(event);
            return value == nullptr ? nullptr : &value->common;
        }
        case market::MarketEventKindV1::kShenzhenSnapshot: {
            const auto* value =
                market::StoredMarketEventGetV1<
                    market::ShenzhenSnapshotV1>(event);
            return value == nullptr ? nullptr : &value->common;
        }
        case market::MarketEventKindV1::kShenzhenOrder: {
            const auto* value =
                market::StoredMarketEventGetV1<
                    market::ShenzhenOrderV1>(event);
            return value == nullptr ? nullptr : &value->common;
        }
        case market::MarketEventKindV1::kShenzhenTransaction: {
            const auto* value =
                market::StoredMarketEventGetV1<
                    market::ShenzhenTransactionV1>(event);
            return value == nullptr ? nullptr : &value->common;
        }
    }
    return nullptr;
}

bool AcceptScannedRecord(
    const market::RealtimeHistoryRecordV1* record,
    const std::vector<bool>& active,
    std::uint64_t expected_records,
    const std::array<std::uint64_t, 4U>& expected_sources,
    ScanVerifier* verifier,
    ScanResult* result) {
    if (record == nullptr ||
        record->instrument_id() >= active.size() ||
        !active[record->instrument_id()] ||
        verifier == nullptr || result == nullptr) {
        return false;
    }
    const std::size_t raw_kind =
        static_cast<std::size_t>(record->kind());
    if (raw_kind == 0U || raw_kind > result->kinds.size()) {
        return false;
    }
    constexpr std::array<std::uint8_t, 5U> kind_sources{
        0U, 1U, 2U, 3U, 3U};
    const std::size_t source = record->source_slot();
    const market::DecodedMarketCommonV1* common =
        StoredCommon(*record);
    if (source >= result->sources.size() ||
        source != kind_sources[raw_kind - 1U] ||
        record->source_stream_id() != kSourceStreamIds[source] ||
        common == nullptr ||
        common->kind != record->kind() ||
        common->instrument_id != record->instrument_id() ||
        common->origin.source_stream_id !=
            record->source_stream_id() ||
        common->origin.source_sequence != record->source_sequence() ||
        common->origin.trade_date != kTradeDate ||
        !MarkSequence(
            &verifier->ingress,
            record->ingress_sequence(),
            expected_records) ||
        !MarkSequence(
            &verifier->source_sequences[source],
            record->source_sequence(),
            expected_sources[source])) {
        return false;
    }
    ++result->records;
    result->sum += record->ingress_sequence();
    result->xor_value ^= record->ingress_sequence();
    ++result->kinds[raw_kind - 1U];
    ++result->sources[source];
    return true;
}

ScanResult ScanUniverse(
    const market::IntradayInstrumentStoreGenerationV1& generation,
    const std::vector<bool>& active,
    std::uint64_t expected_records,
    const std::array<std::uint64_t, 4U>& expected_sources,
    std::size_t scan_batch_records) {
    ScanResult result{};
    ScanVerifier verifier(expected_records, expected_sources);
    std::unique_ptr<market::IntradayUniverseCursorV1> cursor;
    if (generation.OpenUniverseCursor({}, &cursor) !=
            market::IntradayInstrumentStoreQueryErrorV1::kNone ||
        cursor == nullptr) {
        return result;
    }
    std::vector<const market::RealtimeHistoryRecordV1*> batch(
        scan_batch_records);
    const Clock::time_point begin = Clock::now();
    for (;;) {
        std::size_t written = 0U;
        const Clock::time_point read_begin = Clock::now();
        const auto error = cursor->ReadBatch(batch, &written);
        result.batch_latencies.push_back(
            Ns(Clock::now() - read_begin));
        if (error != market::IntradayInstrumentStoreQueryErrorV1::kNone) {
            return result;
        }
        if (written == 0U) {
            if (!cursor->done()) {
                return result;
            }
            break;
        }
        const Clock::time_point validation_begin = Clock::now();
        for (std::size_t index = 0U; index < written; ++index) {
            if (!AcceptScannedRecord(
                    batch[index],
                    active,
                    expected_records,
                    expected_sources,
                    &verifier,
                    &result)) {
                return result;
            }
        }
        result.validation_elapsed +=
            Ns(Clock::now() - validation_begin);
    }
    const std::uint64_t total_elapsed = Ns(Clock::now() - begin);
    result.elapsed =
        total_elapsed >= result.validation_elapsed
            ? total_elapsed - result.validation_elapsed
            : 0U;
    result.ok = true;
    return result;
}

struct InstrumentScanResult final {
    ScanResult aggregate;
    std::uint64_t active_instruments = 0U;
    std::vector<std::uint64_t> instrument_latencies;
    std::vector<std::uint64_t> instrument_record_counts;
};

InstrumentScanResult ScanEveryInstrument(
    const market::IntradayInstrumentStoreGenerationV1& generation,
    const std::vector<bool>& active,
    const std::vector<std::uint64_t>& latest,
    std::uint32_t seconds,
    std::uint64_t expected_records,
    const std::array<std::uint64_t, 4U>& expected_sources,
    std::size_t scan_batch_records) {
    InstrumentScanResult output{};
    ScanVerifier verifier(expected_records, expected_sources);
    std::vector<const market::RealtimeHistoryRecordV1*> batch(
        scan_batch_records);
    const Clock::time_point all_begin = Clock::now();
    for (std::size_t ordinal = 0U;
         ordinal < generation.instrument_count();
         ++ordinal) {
        market::IntradayInstrumentSummaryV1 summary{};
        if (generation.SummaryAt(ordinal, &summary) !=
            market::IntradayInstrumentStoreQueryErrorV1::kNone) {
            return output;
        }
        const bool should_be_active =
            summary.instrument_id < active.size() &&
            active[summary.instrument_id];
        if (summary.record_count == 0U) {
            if (should_be_active) {
                return output;
            }
            continue;
        }
        if (!should_be_active) {
            return output;
        }
        ++output.active_instruments;
        const Clock::time_point instrument_begin = Clock::now();
        std::unique_ptr<market::IntradayInstrumentCursorV1> cursor;
        if (generation.OpenInstrumentCursor(
                summary.instrument_id, {}, &cursor) !=
                market::IntradayInstrumentStoreQueryErrorV1::kNone ||
            cursor == nullptr) {
            return output;
        }
        std::uint64_t instrument_records = 0U;
        std::uint64_t previous_sequence = 0U;
        std::uint64_t instrument_validation_elapsed = 0U;
        for (;;) {
            std::size_t written = 0U;
            const Clock::time_point read_begin = Clock::now();
            const auto error = cursor->ReadBatch(batch, &written);
            output.aggregate.batch_latencies.push_back(
                Ns(Clock::now() - read_begin));
            if (error !=
                market::IntradayInstrumentStoreQueryErrorV1::kNone) {
                return output;
            }
            if (written == 0U) {
                if (!cursor->done()) {
                    return output;
                }
                break;
            }
            const Clock::time_point validation_begin = Clock::now();
            for (std::size_t index = 0U; index < written; ++index) {
                const auto* record = batch[index];
                if (record == nullptr ||
                    record->instrument_id() != summary.instrument_id ||
                    record->ingress_sequence() <= previous_sequence ||
                    !AcceptScannedRecord(
                        record,
                        active,
                        expected_records,
                        expected_sources,
                        &verifier,
                        &output.aggregate)) {
                    return output;
                }
                previous_sequence = record->ingress_sequence();
                ++instrument_records;
            }
            const std::uint64_t validation_elapsed =
                Ns(Clock::now() - validation_begin);
            instrument_validation_elapsed += validation_elapsed;
            output.aggregate.validation_elapsed += validation_elapsed;
        }
        const std::uint64_t instrument_elapsed =
            Ns(Clock::now() - instrument_begin);
        output.instrument_latencies.push_back(
            instrument_elapsed >= instrument_validation_elapsed
                ? instrument_elapsed -
                      instrument_validation_elapsed
                : 0U);
        output.instrument_record_counts.push_back(instrument_records);
        if (instrument_records != summary.record_count ||
            instrument_records != ExpectedInstrumentRecords(
                                      summary.instrument_id, seconds) ||
            previous_sequence != latest[summary.instrument_id]) {
            return output;
        }
    }
    const std::uint64_t total_elapsed =
        Ns(Clock::now() - all_begin);
    output.aggregate.elapsed =
        total_elapsed >= output.aggregate.validation_elapsed
            ? total_elapsed -
                  output.aggregate.validation_elapsed
            : 0U;
    output.aggregate.ok =
        output.active_instruments == kActive;
    return output;
}

bool PinCurrentThread(std::uint32_t cpu) {
    cpu_set_t allowed{};
    if (::sched_getaffinity(0, sizeof(allowed), &allowed) != 0 ||
        cpu >= CPU_SETSIZE || !CPU_ISSET(cpu, &allowed)) {
        return false;
    }
    cpu_set_t target{};
    CPU_ZERO(&target);
    CPU_SET(cpu, &target);
    return ::sched_setaffinity(0, sizeof(target), &target) == 0;
}

int Run(
    std::uint32_t seconds,
    std::size_t scan_batch_records,
    std::optional<std::uint32_t> reader_cpu) {
    const std::uint64_t stress_records =
        static_cast<std::uint64_t>(seconds) * kRate;
    const std::uint64_t expected_total = kWarmup + stress_records;
    const auto expected_kinds = ExpectedKinds(seconds);
    const auto expected_sources = ExpectedSources(seconds);
    auto registry = MakeRegistry();
    if (registry == nullptr) {
        return 1;
    }
    Pools pools = MakePools();

    std::vector<std::uint32_t> active_ids;
    std::vector<bool> active_flags(kUniverse + 1U, false);
    active_ids.reserve(kActive);
    for (std::uint32_t ordinal = 0U;
         ordinal < kActivePerMarket;
         ++ordinal) {
        const std::uint32_t id = ordinal + 1U;
        active_ids.push_back(id);
        active_flags[id] = true;
    }
    for (std::uint32_t ordinal = 0U;
         ordinal < kActivePerMarket;
         ++ordinal) {
        const std::uint32_t id =
            kMarketUniverse + ordinal + 1U;
        active_ids.push_back(id);
        active_flags[id] = true;
    }

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    const auto create_error = runtime::RealtimePipelineV1::Create(
        Config(registry.get(), stress_records), &pipeline, &detail);
    if (create_error != runtime::RealtimePipelineCreateErrorV1::kNone ||
        pipeline == nullptr) {
        std::cerr << "create_failed="
                  << runtime::RealtimePipelineCreateErrorNameV1(
                         create_error)
                  << " detail=" << detail << '\n';
        return 1;
    }

    std::vector<std::uint64_t> latest(kUniverse + 1U, 0U);
    std::array<std::uint64_t, 5U> kinds{};
    std::cout << std::fixed << std::setprecision(3)
              << "CONFIG seconds=" << seconds
              << " rate=" << kRate
              << " universe=" << kUniverse
              << " active=" << kActive
              << " warmup=" << kWarmup
              << " stress_records=" << stress_records
              << " scan_batch_records=" << scan_batch_records
              << " reader_cpu="
              << (reader_cpu.has_value()
                      ? std::to_string(*reader_cpu)
                      : std::string("unbound"))
              << " per_instrument_full_scan=1\n";

    std::uint64_t warm = 0U;
    for (std::uint32_t ordinal = 0U;
         ordinal < kActivePerMarket;
         ++ordinal) {
        const std::uint32_t id = ordinal + 1U;
        if (!Submit(
                pipeline.get(), pools.sh_snapshot[ordinal].get(),
                id, 0U, &latest, &kinds) ||
            !Submit(
                pipeline.get(), pools.sh_tick[ordinal].get(),
                id, 1U, &latest, &kinds)) {
            pipeline->StopAndDrain();
            return 1;
        }
        warm += 2U;
        if (warm % kBatch == 0U &&
            !WaitAppended(pipeline.get(), warm)) {
            pipeline->StopAndDrain();
            return 1;
        }
    }
    for (std::uint32_t ordinal = 0U;
         ordinal < kActivePerMarket;
         ++ordinal) {
        const std::uint32_t id =
            kMarketUniverse + ordinal + 1U;
        if (!Submit(
                pipeline.get(), pools.sz_snapshot[ordinal].get(),
                id, 2U, &latest, &kinds) ||
            !Submit(
                pipeline.get(), pools.sz_order[ordinal].get(),
                id, 3U, &latest, &kinds) ||
            !Submit(
                pipeline.get(), pools.sz_transaction[ordinal].get(),
                id, 4U, &latest, &kinds)) {
            pipeline->StopAndDrain();
            return 1;
        }
        warm += 3U;
        if (warm % kBatch == 0U &&
            !WaitAppended(pipeline.get(), warm)) {
            pipeline->StopAndDrain();
            return 1;
        }
    }
    if (warm != kWarmup || !WaitAppended(pipeline.get(), kWarmup)) {
        pipeline->StopAndDrain();
        return 1;
    }
    const auto warm_cut = pipeline->CutAndPublishGeneration(30s);
    std::vector<std::uint64_t> warm_reads;
    if (!warm_cut.published() ||
        warm_cut.store_generation->record_count() != kWarmup ||
        !SweepLatest(
            *warm_cut.store_generation,
            active_ids,
            latest,
            &warm_reads)) {
        std::cerr << "warmup_validation_failed\n";
        pipeline->StopAndDrain();
        return 1;
    }
    std::cout << "WARMUP records=" << kWarmup
              << " active_verified=" << kActive << '\n';

    std::vector<std::uint64_t> callback;
    std::vector<std::uint64_t> pacing_lateness;
    std::vector<std::uint64_t> append_tail;
    std::vector<std::uint64_t> backlog;
    std::vector<std::uint64_t> cut_latency;
    std::vector<std::uint64_t> direct_query;
    std::vector<std::uint64_t> callback_to_direct;
    std::vector<std::uint64_t> append_to_direct;
    std::vector<std::uint64_t> sweep_latency;
    std::vector<std::uint64_t> callback_to_sweep;
    std::vector<std::uint64_t> point_reads;
    callback.reserve(stress_records / 32U);
    pacing_lateness.reserve(stress_records / kBatch);
    append_tail.reserve(stress_records / kBatch);
    backlog.reserve(stress_records / kBatch);
    cut_latency.reserve(seconds);
    direct_query.reserve(seconds);
    callback_to_direct.reserve(seconds);
    append_to_direct.reserve(seconds);
    sweep_latency.reserve(seconds);
    callback_to_sweep.reserve(seconds);
    point_reads.reserve(static_cast<std::size_t>(seconds) * kActive);

    std::uint64_t sh_counter = 0U;
    std::uint64_t sz_counter = 0U;
    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        final_generation = warm_cut.store_generation;
    Clock::time_point final_append{};
    Clock::time_point final_direct{};
    Clock::time_point final_sweep{};
    Clock::time_point last_callback{};
    std::uint32_t last_instrument = 0U;
    const Clock::time_point start = Clock::now();

    for (std::uint64_t record_index = 0U;
         record_index < stress_records;
         ++record_index) {
        const Clock::time_point target =
            start +
            std::chrono::nanoseconds(record_index * kIntervalNs);
        Pace(target);
        const std::uint64_t slot =
            ((record_index % kRate) * 48'271U) % kRate;
        const FakeMessage* message = nullptr;
        std::uint32_t instrument_id = 0U;
        std::size_t kind = 0U;
        if (slot < 3'000U) {
            const std::size_t ordinal =
                sh_counter++ % kActivePerMarket;
            message = pools.sh_snapshot[ordinal].get();
            instrument_id = static_cast<std::uint32_t>(ordinal) + 1U;
            kind = 0U;
        } else if (slot < 44'500U) {
            const std::size_t ordinal =
                sh_counter++ % kActivePerMarket;
            message = pools.sh_tick[ordinal].get();
            instrument_id = static_cast<std::uint32_t>(ordinal) + 1U;
            kind = 1U;
        } else if (slot < 47'900U) {
            const std::size_t ordinal =
                sz_counter++ % kActivePerMarket;
            message = pools.sz_snapshot[ordinal].get();
            instrument_id = kMarketUniverse +
                            static_cast<std::uint32_t>(ordinal) + 1U;
            kind = 2U;
        } else if (slot < 73'950U) {
            const std::size_t ordinal =
                sz_counter++ % kActivePerMarket;
            message = pools.sz_order[ordinal].get();
            instrument_id = kMarketUniverse +
                            static_cast<std::uint32_t>(ordinal) + 1U;
            kind = 3U;
        } else {
            const std::size_t ordinal =
                sz_counter++ % kActivePerMarket;
            message = pools.sz_transaction[ordinal].get();
            instrument_id = kMarketUniverse +
                            static_cast<std::uint32_t>(ordinal) + 1U;
            kind = 4U;
        }
        const Clock::time_point callback_begin = Clock::now();
        const auto ingress =
            pipeline->InjectSdkMessageForTest(message);
        const Clock::time_point callback_end = Clock::now();
        if (!ingress.accepted()) {
            std::cerr << "submit_failed error="
                      << runtime::RealtimePipelineIngressErrorNameV1(
                             ingress.error)
                      << " instrument=" << instrument_id << '\n';
            pipeline->StopAndDrain();
            return 1;
        }
        latest[instrument_id] = ingress.global_ingress_sequence;
        ++kinds[kind];
        if (record_index % 32U == 0U) {
            callback.push_back(Ns(callback_end - callback_begin));
        }
        last_callback = callback_begin;
        last_instrument = instrument_id;
        const std::uint64_t complete = record_index + 1U;
        if (complete % kBatch != 0U) {
            continue;
        }
        pacing_lateness.push_back(
            callback_begin > target
                ? Ns(callback_begin - target)
                : 0U);
        const std::uint64_t expected = kWarmup + complete;
        const auto before = pipeline->Snapshot();
        backlog.push_back(
            before.accepted_messages >= before.store.appended_records
                ? before.accepted_messages -
                      before.store.appended_records
                : 0U);
        if (!WaitAppended(pipeline.get(), expected)) {
            pipeline->StopAndDrain();
            return 1;
        }
        const Clock::time_point append_done = Clock::now();
        append_tail.push_back(Ns(append_done - last_callback));
        final_append = append_done;
        if (complete % kRate != 0U) {
            continue;
        }
        const Clock::time_point cut_begin = Clock::now();
        const auto cut = pipeline->CutAndPublishGeneration(30s);
        const Clock::time_point cut_done = Clock::now();
        if (!cut.published() ||
            cut.store_generation->record_count() != expected) {
            std::cerr << "cut_failed checkpoint=" << complete / kRate
                      << '\n';
            pipeline->StopAndDrain();
            return 1;
        }
        cut_latency.push_back(Ns(cut_done - cut_begin));
        std::uint64_t query = 0U;
        if (!ReadLatest(
                *cut.store_generation,
                last_instrument,
                latest[last_instrument],
                &query)) {
            pipeline->StopAndDrain();
            return 1;
        }
        const Clock::time_point direct_done = Clock::now();
        direct_query.push_back(query);
        callback_to_direct.push_back(Ns(direct_done - last_callback));
        append_to_direct.push_back(Ns(direct_done - append_done));
        const Clock::time_point sweep_begin = Clock::now();
        if (!SweepLatest(
                *cut.store_generation,
                active_ids,
                latest,
                &point_reads)) {
            pipeline->StopAndDrain();
            return 1;
        }
        const Clock::time_point sweep_done = Clock::now();
        sweep_latency.push_back(Ns(sweep_done - sweep_begin));
        callback_to_sweep.push_back(Ns(sweep_done - last_callback));
        final_direct = direct_done;
        final_sweep = sweep_done;
        final_generation = cut.store_generation;
        const std::uint64_t checkpoint = complete / kRate;
        if (checkpoint % 10U == 0U || checkpoint == seconds) {
            const auto snapshot = pipeline->Snapshot();
            const std::size_t window_begin =
                append_tail.size() > 1'000U
                    ? append_tail.size() - 1'000U
                    : 0U;
            const std::vector<std::uint64_t> recent_tail(
                append_tail.begin() +
                    static_cast<std::ptrdiff_t>(window_begin),
                append_tail.end());
            const std::size_t cut_begin_index =
                cut_latency.size() > 10U
                    ? cut_latency.size() - 10U
                    : 0U;
            const std::vector<std::uint64_t> recent_cut(
                cut_latency.begin() +
                    static_cast<std::ptrdiff_t>(cut_begin_index),
                cut_latency.end());
            const std::vector<std::uint64_t> recent_direct(
                callback_to_direct.begin() +
                    static_cast<std::ptrdiff_t>(cut_begin_index),
                callback_to_direct.end());
            const std::vector<std::uint64_t> recent_sweep(
                sweep_latency.begin() +
                    static_cast<std::ptrdiff_t>(cut_begin_index),
                sweep_latency.end());
            const Stats tail_stats = TimeStats(recent_tail);
            const Stats cut_stats = TimeStats(recent_cut);
            const Stats direct_stats = TimeStats(recent_direct);
            const Stats sweep_stats = TimeStats(recent_sweep);
            const double elapsed =
                static_cast<double>(Ns(sweep_done - start)) / 1.0e9;
            const std::uint64_t logical =
                snapshot.store.accounted_record_bytes +
                snapshot.store.allocated_index_bytes;
            std::cout
                << "PROGRESS elapsed_s=" << elapsed
                << " checkpoint=" << checkpoint
                << " test_records=" << complete
                << " accepted=" << snapshot.accepted_messages
                << " decoded=" << snapshot.decoded_messages
                << " appended=" << snapshot.store.appended_records
                << " append_tail_p99_us=" << tail_stats.p99
                << " cut_p50_us=" << cut_stats.p50
                << " callback_to_read_p99_us="
                << direct_stats.p99
                << " active6000_sweep_p99_us="
                << sweep_stats.p99
                << " logical_gib=" << GiB(logical)
                << " rss_mib=" << PeakRssMiB()
                << " rejected=" << snapshot.rejected_messages
                << " fatal=" << snapshot.fatal
                << '\n';
            std::cout.flush();
        }
    }

    if (final_generation == nullptr) {
        pipeline->StopAndDrain();
        return 1;
    }
    if (reader_cpu.has_value() && !PinCurrentThread(*reader_cpu)) {
        std::cerr << "reader_affinity_failed cpu=" << *reader_cpu
                  << '\n';
        pipeline->StopAndDrain();
        return 1;
    }
    std::cout << "UNIVERSE_SCAN_BEGIN records="
              << final_generation->record_count() << '\n';
    std::cout.flush();
    const ScanResult universe_scan =
        ScanUniverse(
            *final_generation,
            active_flags,
            expected_total,
            expected_sources,
            scan_batch_records);
    std::cout << "INSTRUMENT_SCAN_BEGIN active=" << kActive
              << " records=" << final_generation->record_count()
              << '\n';
    std::cout.flush();
    const InstrumentScanResult instrument_scan =
        ScanEveryInstrument(
            *final_generation,
            active_flags,
            latest,
            seconds,
            expected_total,
            expected_sources,
            scan_batch_records);

    const auto snapshot = pipeline->Snapshot();
    const std::uint64_t expected_sum =
        expected_total * (expected_total + 1U) / 2U;
    const std::uint64_t expected_xor = XorTo(expected_total);
    const auto scan_exact = [&](const ScanResult& scan) {
        return scan.ok &&
               scan.records == expected_total &&
               scan.sum == expected_sum &&
               scan.xor_value == expected_xor &&
               scan.kinds == expected_kinds &&
               scan.sources == expected_sources;
    };
    const auto& watermark = final_generation->watermark();
    bool watermark_exact =
        watermark.trade_date == kTradeDate &&
        watermark.ingress_sequence_exclusive ==
            expected_total + 1U;
    for (std::size_t source = 0U;
         source < expected_sources.size();
         ++source) {
        watermark_exact =
            watermark_exact &&
            watermark.sources[source].source_stream_id ==
                kSourceStreamIds[source] &&
            watermark.sources[source].sequence_exclusive ==
                expected_sources[source] + 1U;
    }
    const bool exact =
        final_generation->instrument_count() == kUniverse &&
        final_generation->record_count() == expected_total &&
        snapshot.accepted_messages == expected_total &&
        snapshot.decoded_messages == expected_total &&
        snapshot.store.appended_records == expected_total &&
        snapshot.rejected_messages == 0U &&
        !snapshot.fatal &&
        kinds == expected_kinds &&
        watermark_exact &&
        scan_exact(universe_scan) &&
        scan_exact(instrument_scan.aggregate) &&
        instrument_scan.active_instruments == kActive;

    const double append_seconds =
        static_cast<double>(Ns(final_append - start)) / 1.0e9;
    const double readable_seconds =
        static_cast<double>(Ns(final_direct - start)) / 1.0e9;
    const double sweep_seconds =
        static_cast<double>(Ns(final_sweep - start)) / 1.0e9;
    const std::uint64_t logical =
        snapshot.store.accounted_record_bytes +
        snapshot.store.allocated_index_bytes;
    const Stats universe_batches =
        TimeStats(universe_scan.batch_latencies);
    const Stats instrument_batches =
        TimeStats(instrument_scan.aggregate.batch_latencies);
    const Stats instrument_times =
        TimeStats(instrument_scan.instrument_latencies);

    std::cout
        << "FINAL append_elapsed_s=" << append_seconds
        << " readable_elapsed_s=" << readable_seconds
        << " sweep_elapsed_s=" << sweep_seconds
        << " append_rate_per_s="
        << static_cast<double>(stress_records) / append_seconds
        << " readable_rate_per_s="
        << static_cast<double>(stress_records) / readable_seconds
        << " accepted=" << snapshot.accepted_messages
        << " decoded=" << snapshot.decoded_messages
        << " appended=" << snapshot.store.appended_records
        << " rejected=" << snapshot.rejected_messages
        << " fatal=" << snapshot.fatal
        << " exact=" << exact
        << " watermark_ok=" << watermark_exact
        << " kinds_ok="
        << (instrument_scan.aggregate.kinds == expected_kinds)
        << " sources_ok="
        << (instrument_scan.aggregate.sources == expected_sources)
        << " sequence_unique_ok="
        << (universe_scan.ok && instrument_scan.aggregate.ok)
        << " scan_batch_records=" << scan_batch_records
        << " reader_cpu="
        << (reader_cpu.has_value()
                ? std::to_string(*reader_cpu)
                : std::string("unbound"))
        << " active=" << instrument_scan.active_instruments
        << " backlog_p50=" << CountQuantile(backlog, 0.50)
        << " backlog_p95=" << CountQuantile(backlog, 0.95)
        << " backlog_p99=" << CountQuantile(backlog, 0.99)
        << " backlog_max="
        << (backlog.empty()
                ? 0U
                : *std::max_element(backlog.begin(), backlog.end()))
        << " logical_gib=" << GiB(logical)
        << " record_gib="
        << GiB(snapshot.store.accounted_record_bytes)
        << " index_gib="
        << GiB(snapshot.store.allocated_index_bytes)
        << " segments=" << snapshot.store.allocated_segments
        << " rss_mib=" << PeakRssMiB()
        << " universe_scan_records=" << universe_scan.records
        << " universe_scan_s="
        << static_cast<double>(universe_scan.elapsed) / 1.0e9
        << " universe_validation_s="
        << static_cast<double>(
               universe_scan.validation_elapsed) /
               1.0e9
        << " universe_scan_rate_per_s="
        << static_cast<double>(universe_scan.records) * 1.0e9 /
               static_cast<double>(universe_scan.elapsed)
        << " universe_batch_p50_us=" << universe_batches.p50
        << " universe_batch_p99_us=" << universe_batches.p99
        << " universe_batch_max_us=" << universe_batches.maximum
        << " instrument_scan_records="
        << instrument_scan.aggregate.records
        << " instrument_scan_s="
        << static_cast<double>(
               instrument_scan.aggregate.elapsed) /
               1.0e9
        << " instrument_validation_s="
        << static_cast<double>(
               instrument_scan.aggregate.validation_elapsed) /
               1.0e9
        << " instrument_scan_rate_per_s="
        << static_cast<double>(
               instrument_scan.aggregate.records) *
               1.0e9 /
               static_cast<double>(
                   instrument_scan.aggregate.elapsed)
        << " instrument_record_count_min="
        << (instrument_scan.instrument_record_counts.empty()
                ? 0U
                : *std::min_element(
                      instrument_scan.instrument_record_counts.begin(),
                      instrument_scan.instrument_record_counts.end()))
        << " instrument_record_count_p50="
        << CountQuantile(
               instrument_scan.instrument_record_counts, 0.50)
        << " instrument_record_count_p99="
        << CountQuantile(
               instrument_scan.instrument_record_counts, 0.99)
        << " instrument_record_count_max="
        << (instrument_scan.instrument_record_counts.empty()
                ? 0U
                : *std::max_element(
                      instrument_scan.instrument_record_counts.begin(),
                      instrument_scan.instrument_record_counts.end()))
        << " instrument_batch_p50_us="
        << instrument_batches.p50
        << " instrument_batch_p99_us="
        << instrument_batches.p99
        << " instrument_batch_max_us="
        << instrument_batches.maximum
        << " instrument_full_read_p50_us="
        << instrument_times.p50
        << " instrument_full_read_p95_us="
        << instrument_times.p95
        << " instrument_full_read_p99_us="
        << instrument_times.p99
        << " instrument_full_read_p99_9_us="
        << instrument_times.p999
        << " instrument_full_read_max_us="
        << instrument_times.maximum
        << " sum_ok="
        << (instrument_scan.aggregate.sum == expected_sum)
        << " xor_ok="
        << (instrument_scan.aggregate.xor_value == expected_xor)
        << " kinds=" << kinds[0U] << ',' << kinds[1U] << ','
        << kinds[2U] << ',' << kinds[3U] << ',' << kinds[4U]
        << " sources=" << instrument_scan.aggregate.sources[0U]
        << ',' << instrument_scan.aggregate.sources[1U]
        << ',' << instrument_scan.aggregate.sources[2U]
        << ',' << instrument_scan.aggregate.sources[3U]
        << '\n';
    PrintStats("callback", callback);
    PrintStats("pacing_lateness", pacing_lateness);
    PrintStats("append_tail", append_tail);
    PrintStats("generation_cut", cut_latency);
    PrintStats("direct_query", direct_query);
    PrintStats("callback_to_direct_read", callback_to_direct);
    PrintStats("append_to_direct_read", append_to_direct);
    PrintStats("active6000_sweep", sweep_latency);
    PrintStats("callback_to_active6000_sweep", callback_to_sweep);
    PrintStats("per_instrument_latest_read", point_reads);
    PrintStats(
        "per_instrument_full_history_read",
        instrument_scan.instrument_latencies);
    std::cout.flush();
    pipeline->StopAndDrain();
    return exact ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    std::uint32_t seconds = 600U;
    std::size_t scan_batch_records = 65'536U;
    std::optional<std::uint32_t> reader_cpu;
    if (argc >= 2) {
        const unsigned long parsed =
            std::strtoul(argv[1], nullptr, 10);
        if (parsed == 0UL ||
            parsed > std::numeric_limits<std::uint32_t>::max()) {
            return 2;
        }
        seconds = static_cast<std::uint32_t>(parsed);
    }
    if (argc >= 3) {
        const unsigned long parsed =
            std::strtoul(argv[2], nullptr, 10);
        if (parsed == 0UL || parsed > 65'536UL) {
            return 2;
        }
        scan_batch_records = static_cast<std::size_t>(parsed);
    }
    if (argc >= 4) {
        const unsigned long parsed =
            std::strtoul(argv[3], nullptr, 10);
        if (parsed >= CPU_SETSIZE) {
            return 2;
        }
        reader_cpu = static_cast<std::uint32_t>(parsed);
    }
    if (argc > 4) {
        return 2;
    }
    try {
        return Run(seconds, scan_batch_records, reader_cpu);
    } catch (const std::exception& error) {
        std::cerr << "exception=" << error.what() << '\n';
        return 1;
    } catch (...) {
        std::cerr << "unknown_exception\n";
        return 1;
    }
}
