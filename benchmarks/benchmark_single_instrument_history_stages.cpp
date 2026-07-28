#include "l2flow/common/identity128.h"
#include "l2flow/ipc/realtime_history_wire_v1.h"
#include "l2flow/ipc/realtime_shared_service_v1.h"
#include "l2flow/ipc/realtime_wire_v1.h"
#include "l2flow/market/instrument_registry.h"
#include "l2flow/market/intraday_instrument_store_v1.h"
#include "l2flow/market/realtime_history_v1.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cerrno>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#ifndef L2FLOW_HISTORY_BENCHMARK_PYTHON_EXECUTABLE
#error "benchmark target must define the Python executable"
#endif

#ifndef L2FLOW_HISTORY_BENCHMARK_PYTHON_PROBE
#error "benchmark target must define the Python probe path"
#endif

#ifndef L2FLOW_HISTORY_BENCHMARK_PYTHON_SOURCE
#error "benchmark target must define the Python source path"
#endif

namespace {

namespace common = l2flow::common;
namespace ipc = l2flow::ipc;
namespace market = l2flow::market;

constexpr std::uint32_t kInstrumentId = 1001U;
constexpr std::uint32_t kTradeDate = 20'260'728U;
constexpr std::array<std::uint32_t, 4U> kSourceStreamIds{
    11U, 12U, 13U, 14U};
constexpr std::uint64_t kPageHeaderBytes =
    sizeof(ipc::RealtimeHistoryPageHeaderV1);
constexpr std::uint64_t kDescriptorBytes =
    sizeof(ipc::RealtimeHistoryRecordDescriptorV1);
constexpr std::uint64_t kSnapshotBytes =
    sizeof(ipc::RealtimeWireSnapshotPayloadV1);
constexpr std::uint64_t kMaximumRecords = 5'000'000U;
constexpr std::uint64_t kMaximumRounds = 10'000U;
constexpr std::uint32_t kMaximumPageRecords = 1024U * 1024U;

struct Options final {
    std::filesystem::path output_directory;
    std::uint64_t records = 100'000U;
    std::uint32_t page_records = 4096U;
    std::uint32_t rounds = 5U;
    std::uint32_t warmups = 3U;
    std::uint32_t snapshot_every = 0U;
    std::uint64_t seed = 20'260'728'01ULL;
    std::uint64_t benchmark_run_id = 0U;
};

void PrintUsage(std::ostream& output, const char* program) {
    output
        << "usage: " << program
        << " --output-dir PATH [--records N] [--page-records N]"
           " [--rounds N] [--warmups N] [--snapshot-every N]"
           " [--seed N] [--benchmark-run-id N]\n"
           "\n"
           "snapshot-every=0 selects tick-only, 1 selects snapshot-only,"
           " and N>1 emits a snapshot at each zero-based Nth record.\n";
}

template <typename Integer>
bool ParseUnsigned(
    std::string_view text,
    Integer minimum,
    Integer maximum,
    Integer* output) noexcept {
    static_assert(std::is_unsigned_v<Integer>);
    if (output == nullptr || text.empty()) {
        return false;
    }
    Integer parsed = 0U;
    const char* const begin = text.data();
    const char* const end = begin + text.size();
    const auto result = std::from_chars(begin, end, parsed, 10);
    if (result.ec != std::errc{} || result.ptr != end ||
        parsed < minimum || parsed > maximum) {
        return false;
    }
    *output = parsed;
    return true;
}

bool ParseOptions(int argc, char** argv, Options* output) {
    if (output == nullptr || argc <= 0 || argv == nullptr) {
        return false;
    }
    Options result{};
    bool has_output_directory = false;
    for (int index = 1; index < argc; ++index) {
        const std::string_view name(argv[index]);
        if (name == "--help" || name == "-h") {
            PrintUsage(std::cout, argv[0]);
            return false;
        }
        if (index + 1 >= argc) {
            std::cerr << "missing value for " << name << '\n';
            return false;
        }
        const std::string_view value(argv[++index]);
        if (name == "--output-dir") {
            if (value.empty()) {
                std::cerr << "--output-dir must not be empty\n";
                return false;
            }
            result.output_directory = std::filesystem::path(value);
            has_output_directory = true;
        } else if (name == "--records") {
            if (!ParseUnsigned(
                    value,
                    UINT64_C(1),
                    kMaximumRecords,
                    &result.records)) {
                std::cerr << "invalid --records\n";
                return false;
            }
        } else if (name == "--page-records") {
            if (!ParseUnsigned(
                    value,
                    UINT32_C(1),
                    kMaximumPageRecords,
                    &result.page_records)) {
                std::cerr << "invalid --page-records\n";
                return false;
            }
        } else if (name == "--rounds") {
            if (!ParseUnsigned(
                    value,
                    UINT32_C(1),
                    static_cast<std::uint32_t>(kMaximumRounds),
                    &result.rounds)) {
                std::cerr << "invalid --rounds\n";
                return false;
            }
        } else if (name == "--warmups") {
            if (!ParseUnsigned(
                    value,
                    UINT32_C(0),
                    static_cast<std::uint32_t>(kMaximumRounds),
                    &result.warmups)) {
                std::cerr << "invalid --warmups\n";
                return false;
            }
        } else if (name == "--snapshot-every") {
            if (!ParseUnsigned(
                    value,
                    UINT32_C(0),
                    std::numeric_limits<std::uint32_t>::max(),
                    &result.snapshot_every)) {
                std::cerr << "invalid --snapshot-every\n";
                return false;
            }
        } else if (name == "--seed") {
            if (!ParseUnsigned(
                    value,
                    UINT64_C(0),
                    std::numeric_limits<std::uint64_t>::max(),
                    &result.seed)) {
                std::cerr << "invalid --seed\n";
                return false;
            }
        } else if (name == "--benchmark-run-id") {
            if (!ParseUnsigned(
                    value,
                    UINT64_C(1),
                    std::numeric_limits<std::uint64_t>::max(),
                    &result.benchmark_run_id)) {
                std::cerr << "invalid --benchmark-run-id\n";
                return false;
            }
        } else {
            std::cerr << "unknown option: " << name << '\n';
            return false;
        }
    }
    if (!has_output_directory) {
        std::cerr << "--output-dir is required\n";
        return false;
    }
    const std::uint64_t scan_count =
        static_cast<std::uint64_t>(result.rounds) +
        static_cast<std::uint64_t>(result.warmups);
    const std::uint64_t pages_per_scan =
        result.records / result.page_records +
        (result.records % result.page_records != 0U ? 1U : 0U);
    if (scan_count != 0U &&
        pages_per_scan >
            std::numeric_limits<std::uint64_t>::max() / scan_count) {
        std::cerr << "benchmark page count overflows uint64\n";
        return false;
    }
    if (result.benchmark_run_id == 0U) {
        const auto now = std::chrono::system_clock::now()
                             .time_since_epoch()
                             .count();
        result.benchmark_run_id =
            static_cast<std::uint64_t>(now) ^
            (static_cast<std::uint64_t>(::getpid()) << 32U) ^
            result.seed;
        if (result.benchmark_run_id == 0U) {
            result.benchmark_run_id = 1U;
        }
    }
    *output = std::move(result);
    return true;
}

std::vector<std::byte> Bytes(std::string_view value) {
    const std::span<const char> characters(value.data(), value.size());
    const std::span<const std::byte> bytes =
        std::as_bytes(characters);
    return {bytes.begin(), bytes.end()};
}

market::DecimalValueV1 Decimal(
    std::int64_t raw,
    std::uint8_t scale) noexcept {
    market::DecimalValueV1 value{};
    value.raw = raw;
    value.normalized_p6 = raw * 1000;
    value.scale = scale;
    value.valid = true;
    return value;
}

market::QuantityValueV1 Quantity(
    std::int64_t raw,
    std::uint8_t scale) noexcept {
    market::QuantityValueV1 value{};
    value.raw = raw;
    value.scale = scale;
    value.valid = true;
    return value;
}

common::Identity128 MakeRunId(std::uint64_t seed) noexcept {
    common::Identity128 result{};
    constexpr std::array<std::uint8_t, 8U> tag{
        0x4cU, 0x32U, 0x46U, 0x48U, 0x42U, 0x45U, 0x4eU, 0x43U};
    for (std::size_t index = 0U; index < 8U; ++index) {
        result[index] = static_cast<std::byte>(tag[index]);
        result[index + 8U] = static_cast<std::byte>(
            (seed >> (index * 8U)) & UINT64_C(0xff));
    }
    return result;
}

bool IsSnapshotRecord(
    std::uint64_t zero_based_index,
    std::uint32_t snapshot_every) noexcept {
    return snapshot_every != 0U &&
           zero_based_index %
                   static_cast<std::uint64_t>(snapshot_every) ==
               0U;
}

std::string_view WorkloadName(std::uint32_t snapshot_every) noexcept {
    if (snapshot_every == 0U) {
        return "tick_only";
    }
    if (snapshot_every == 1U) {
        return "snapshot_only";
    }
    return "fixed_mixed";
}

class Fixture final {
public:
    [[nodiscard]] bool Initialize(const Options& options) {
        run_id_ = MakeRunId(options.seed);
        if (!BuildRegistry(options.seed) ||
            !BuildStore(options) ||
            !AppendRecords(options) ||
            !BuildGeneration(options)) {
            return false;
        }
        const market::IntradayInstrumentStoreSnapshotV1 snapshot =
            store_->Snapshot();
        if (snapshot.coverage_lost || snapshot.failed_appends != 0U ||
            snapshot.appended_records != options.records ||
            generation_ == nullptr ||
            generation_->record_count() != options.records) {
            std::cerr << "fixture integrity validation failed\n";
            return false;
        }
        return true;
    }

    [[nodiscard]] const common::Identity128& run_id() const noexcept {
        return run_id_;
    }

    [[nodiscard]] const market::InstrumentRegistryV1& registry()
        const noexcept {
        return *registry_;
    }

    [[nodiscard]] const std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>&
    generation() const noexcept {
        return generation_;
    }

    [[nodiscard]] std::uint64_t snapshot_count() const noexcept {
        return source_sequences_[0U];
    }

    [[nodiscard]] std::uint64_t tick_count() const noexcept {
        return source_sequences_[1U];
    }

private:
    [[nodiscard]] bool BuildRegistry(std::uint64_t seed) {
        market::InstrumentRegistryEntryV1 entry{};
        entry.instrument_id = kInstrumentId;
        entry.key.market = market::MarketV1::kShanghai;
        entry.key.security_id_source = Bytes("101");
        entry.key.security_id = Bytes("600001");
        entry.quantity_unit = market::QuantityUnitV1::kShare;
        entry.security_type = market::SecurityTypeV1::kEquity;
        entry.asset_scope = market::AssetScopeV1::kDocumentedCore;
        std::vector<market::InstrumentRegistryEntryV1> entries;
        entries.push_back(std::move(entry));
        const std::uint64_t registry_version =
            seed == 0U ? UINT64_C(1) : seed;
        const market::InstrumentRegistryCreateErrorV1 error =
            market::InstrumentRegistryV1::Create(
                registry_version, entries, &registry_);
        if (error != market::InstrumentRegistryCreateErrorV1::kNone ||
            registry_ == nullptr) {
            std::cerr << "registry create failed: "
                      << static_cast<unsigned int>(error) << '\n';
            return false;
        }
        return true;
    }

    [[nodiscard]] bool BuildStore(const Options& options) {
        constexpr std::uint64_t base_budget =
            UINT64_C(64) * 1024U * 1024U;
        constexpr std::uint64_t per_record_budget = 16U * 1024U;
        if (options.records >
            (std::numeric_limits<std::uint64_t>::max() - base_budget) /
                per_record_budget) {
            std::cerr << "Store byte budget overflows\n";
            return false;
        }
        market::IntradayInstrumentStoreConfigV1 config{};
        config.segment_target_bytes = 64U * 1024U;
        config.maximum_session_records = options.records;
        config.maximum_session_accounted_bytes =
            base_budget + options.records * per_record_budget;
        config.maximum_records_per_batch = options.page_records;
        config.coverage_from_open = true;
        const market::IntradayInstrumentStoreCreateErrorV1 error =
            market::IntradayInstrumentStoreV1::Create(
                config,
                1U,
                kSourceStreamIds,
                registry_.get(),
                &store_);
        if (error != market::IntradayInstrumentStoreCreateErrorV1::kNone ||
            store_ == nullptr) {
            std::cerr << "Store create failed: "
                      << market::IntradayInstrumentStoreCreateErrorNameV1(
                             error)
                      << '\n';
            return false;
        }
        const market::InstrumentRegistryLookupResultV1 lookup =
            registry_->LookupById(kInstrumentId);
        if (!lookup.known() ||
            store_->ResolveRouteToken(
                lookup.registry_ordinal, kInstrumentId, &route_) !=
                market::IntradayInstrumentStoreQueryErrorV1::kNone) {
            std::cerr << "Store route resolution failed\n";
            return false;
        }
        return true;
    }

    void FillCommon(
        market::DecodedMarketCommonV1* common_record,
        market::MarketEventKindV1 kind,
        std::uint8_t source_slot,
        std::uint64_t source_sequence,
        std::uint64_t ingress_sequence) const noexcept {
        const market::InstrumentRegistryLookupResultV1 lookup =
            registry_->LookupById(kInstrumentId);
        common_record->kind = kind;
        common_record->market = market::MarketV1::kShanghai;
        common_record->origin.source_stream_id =
            kSourceStreamIds[source_slot];
        common_record->origin.trade_date = kTradeDate;
        common_record->origin.source_sequence = source_sequence;
        common_record->origin.vendor_local_time_raw = 93'000'000U;
        common_record->origin.vendor_sequence_id = ingress_sequence;
        common_record->origin.recv_realtime_ns =
            static_cast<std::int64_t>(ingress_sequence * 100U);
        common_record->origin.recv_monotonic_ns =
            static_cast<std::int64_t>(ingress_sequence * 10U);
        common_record->exchange_time.raw_hhmmssmmm = 93'000'000U;
        common_record->exchange_time.nanoseconds_since_midnight =
            34'200'000'000'000U + ingress_sequence;
        common_record->exchange_time.unix_nanoseconds =
            1'785'115'800'000'000'000LL +
            static_cast<std::int64_t>(ingress_sequence);
        common_record->exchange_time.valid = true;
        common_record->exchange_time.unix_nanoseconds_valid = true;
        common_record->instrument_id = kInstrumentId;
        common_record->registry_ordinal = lookup.registry_ordinal;
        common_record->quantity_unit = lookup.quantity_unit;
        common_record->security_type = lookup.security_type;
        common_record->asset_scope = lookup.asset_scope;
    }

    [[nodiscard]] market::ShanghaiSnapshotV1 MakeSnapshot(
        std::uint64_t source_sequence,
        std::uint64_t ingress_sequence) const noexcept {
        market::ShanghaiSnapshotV1 event{};
        FillCommon(
            &event.common,
            market::MarketEventKindV1::kShanghaiSnapshot,
            0U,
            source_sequence,
            ingress_sequence);
        event.image_status = 7;
        event.trade_count = static_cast<std::uint32_t>(
            ingress_sequence %
            static_cast<std::uint64_t>(
                std::numeric_limits<std::uint32_t>::max()));
        event.last_price = Decimal(
            static_cast<std::int64_t>(12'000U + ingress_sequence % 1000U),
            3U);
        event.trade_volume = Quantity(
            static_cast<std::int64_t>(100U + ingress_sequence % 1000U),
            0U);
        event.book.actual_bid_depth =
            static_cast<std::uint32_t>(market::kMaximumPublicDepthV1);
        event.book.actual_ask_depth =
            static_cast<std::uint32_t>(market::kMaximumPublicDepthV1);
        event.book.retained_bid_depth =
            static_cast<std::uint32_t>(market::kMaximumPublicDepthV1);
        event.book.retained_ask_depth =
            static_cast<std::uint32_t>(market::kMaximumPublicDepthV1);
        for (std::size_t level = 0U;
             level < market::kMaximumPublicDepthV1;
             ++level) {
            const std::int64_t level_value =
                static_cast<std::int64_t>(level + 1U);
            event.book.bids[level].price =
                Decimal(12'000 - level_value, 3U);
            event.book.bids[level].quantity =
                Quantity(100 + level_value, 0U);
            event.book.bids[level].order_count =
                static_cast<std::uint32_t>(level + 1U);
            event.book.bids[level].order_count_valid = true;
            event.book.asks[level].price =
                Decimal(12'001 + level_value, 3U);
            event.book.asks[level].quantity =
                Quantity(200 + level_value, 0U);
            event.book.asks[level].order_count =
                static_cast<std::uint32_t>(level + 2U);
            event.book.asks[level].order_count_valid = true;
        }
        return event;
    }

    [[nodiscard]] market::ShanghaiTickV1 MakeTick(
        std::uint64_t source_sequence,
        std::uint64_t ingress_sequence) const {
        market::ShanghaiTickV1 event{};
        FillCommon(
            &event.common,
            market::MarketEventKindV1::kShanghaiTick,
            1U,
            source_sequence,
            ingress_sequence);
        event.business_index =
            static_cast<std::int64_t>(source_sequence);
        event.channel = 9;
        event.raw_type = "A";
        event.raw_tick_flag = "B";
        event.raw_type_valid = true;
        event.raw_tick_flag_valid = true;
        event.fields.action = market::TickActionV1::kAdd;
        event.fields.side = market::SideV1::kBuy;
        event.fields.order_type = market::OrderTypeV1::kLimit;
        event.fields.price = Decimal(
            static_cast<std::int64_t>(
                12'000U + ingress_sequence % 1000U),
            3U);
        event.fields.quantity = Quantity(
            static_cast<std::int64_t>(
                100U + ingress_sequence % 1000U),
            0U);
        event.fields.primary_order_id =
            static_cast<std::int64_t>(ingress_sequence);
        event.fields.validity_bitmap =
            market::kTickPriceValidV1 |
            market::kTickQuantityValidV1 |
            market::kTickPrimaryOrderIdValidV1 |
            market::kTickSideValidV1 |
            market::kTickOrderTypeValidV1;
        return event;
    }

    template <typename Event>
    [[nodiscard]] bool Append(
        Event event,
        std::uint8_t source_slot,
        std::uint64_t ingress_sequence,
        std::uint64_t tick_stream_sequence) {
        market::DecodedMarketEventV1 decoded(std::move(event));
        std::optional<market::RealtimeHistoryEventInputV1> input =
            market::RealtimeHistoryEventInputV1::Create(
                source_slot,
                ingress_sequence,
                std::move(decoded),
                tick_stream_sequence);
        if (!input.has_value()) {
            return false;
        }
        return store_->Append(
                   route_.worker, route_, std::move(*input)) ==
               market::IntradayInstrumentStoreAppendErrorV1::kNone;
    }

    [[nodiscard]] bool AppendRecords(const Options& options) {
        std::uint64_t tick_stream_sequence = 0U;
        for (std::uint64_t index = 0U; index < options.records; ++index) {
            const std::uint64_t ingress_sequence = index + 1U;
            const bool snapshot =
                IsSnapshotRecord(index, options.snapshot_every);
            const std::uint8_t source_slot =
                static_cast<std::uint8_t>(snapshot ? 0U : 1U);
            ++source_sequences_[source_slot];
            bool appended = false;
            if (snapshot) {
                appended = Append(
                    MakeSnapshot(
                        source_sequences_[source_slot],
                        ingress_sequence),
                    source_slot,
                    ingress_sequence,
                    0U);
            } else {
                ++tick_stream_sequence;
                appended = Append(
                    MakeTick(
                        source_sequences_[source_slot],
                        ingress_sequence),
                    source_slot,
                    ingress_sequence,
                    tick_stream_sequence);
            }
            if (!appended) {
                std::cerr << "Store append failed at record " << index
                          << '\n';
                return false;
            }
        }
        return source_sequences_[0U] + source_sequences_[1U] ==
               options.records;
    }

    [[nodiscard]] bool BuildGeneration(const Options& options) {
        std::array<market::RealtimeSourceWatermarkV1, 4U> sources{};
        for (std::size_t source = 0U; source < sources.size(); ++source) {
            sources[source].source_stream_id = kSourceStreamIds[source];
            sources[source].sequence_exclusive =
                source_sequences_[source] + 1U;
        }
        market::RealtimeHistoryWatermarkV1 watermark{};
        const market::RealtimeHistoryWatermarkErrorV1 watermark_error =
            market::BuildRealtimeHistoryWatermarkV1(
                run_id_,
                1U,
                kTradeDate,
                options.records + 1U,
                options.records * 10U + 1U,
                *registry_,
                sources,
                &watermark);
        if (watermark_error !=
            market::RealtimeHistoryWatermarkErrorV1::kNone) {
            std::cerr << "watermark build failed: "
                      << static_cast<unsigned int>(watermark_error)
                      << '\n';
            return false;
        }
        std::unique_ptr<
            market::IntradayInstrumentStoreWorkerSliceV1>
            slice;
        const market::IntradayInstrumentStoreGenerationErrorV1
            capture_error =
                store_->CaptureWorker(0U, 1U, &slice);
        if (capture_error !=
                market::IntradayInstrumentStoreGenerationErrorV1::kNone ||
            slice == nullptr) {
            std::cerr << "worker capture failed: "
                      << market::
                             IntradayInstrumentStoreGenerationErrorNameV1(
                                 capture_error)
                      << '\n';
            return false;
        }
        std::vector<std::unique_ptr<
            market::IntradayInstrumentStoreWorkerSliceV1>>
            slices;
        slices.push_back(std::move(slice));
        const market::IntradayInstrumentStoreGenerationErrorV1
            generation_error = store_->BuildGeneration(
                watermark, std::move(slices), &generation_);
        if (generation_error !=
                market::IntradayInstrumentStoreGenerationErrorV1::kNone ||
            generation_ == nullptr) {
            std::cerr << "generation build failed: "
                      << market::
                             IntradayInstrumentStoreGenerationErrorNameV1(
                                 generation_error)
                      << '\n';
            return false;
        }
        const market::IntradayInstrumentStoreGenerationErrorV1
            publish_error = store_->PublishGeneration(generation_);
        if (publish_error !=
            market::IntradayInstrumentStoreGenerationErrorV1::kNone) {
            std::cerr << "generation publish failed: "
                      << market::
                             IntradayInstrumentStoreGenerationErrorNameV1(
                                 publish_error)
                      << '\n';
            return false;
        }
        return true;
    }

    common::Identity128 run_id_{};
    std::unique_ptr<market::InstrumentRegistryV1> registry_;
    std::unique_ptr<market::IntradayInstrumentStoreV1> store_;
    market::InstrumentRouteTokenV1 route_{};
    std::array<std::uint64_t, 4U> source_sequences_{};
    std::shared_ptr<
        const market::IntradayInstrumentStoreGenerationV1>
        generation_;
};

class TimingCollector final
    : public ipc::RealtimeHistoryPageStageObserverV1 {
public:
    explicit TimingCollector(std::size_t expected_events) {
        events_.reserve(expected_events);
    }

    void ObserveHistoryPageStageTiming(
        const ipc::RealtimeHistoryPageStageTimingV1& timing)
        noexcept override {
        try {
            const std::lock_guard<std::mutex> lock(mutex_);
            events_.push_back(timing);
        } catch (...) {
            failed_.store(true, std::memory_order_release);
        }
    }

    [[nodiscard]] bool failed() const noexcept {
        return failed_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::vector<ipc::RealtimeHistoryPageStageTimingV1>
    Snapshot() const {
        const std::lock_guard<std::mutex> lock(mutex_);
        return events_;
    }

private:
    mutable std::mutex mutex_;
    std::vector<ipc::RealtimeHistoryPageStageTimingV1> events_;
    std::atomic<bool> failed_{false};
};

class ScopedTempDirectory final {
public:
    ScopedTempDirectory() {
        char path_template[] =
            "/tmp/l2flow-history-benchmark-XXXXXX";
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

std::uint64_t ExpectedPageEvents(const Options& options) noexcept {
    const std::uint64_t pages_per_scan =
        options.records / options.page_records +
        (options.records % options.page_records != 0U ? 1U : 0U);
    return pages_per_scan *
           (static_cast<std::uint64_t>(options.rounds) +
            static_cast<std::uint64_t>(options.warmups));
}

bool CheckedHistoryPageBytes(
    std::uint32_t page_records,
    std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    constexpr std::uint64_t per_record =
        kDescriptorBytes + kSnapshotBytes;
    if (page_records >
        (std::numeric_limits<std::uint64_t>::max() -
         kPageHeaderBytes) /
            per_record) {
        return false;
    }
    *output =
        kPageHeaderBytes +
        static_cast<std::uint64_t>(page_records) * per_record;
    return true;
}

int RunPythonProbe(
    const std::filesystem::path& socket_path,
    const Options& options) {
    const std::string socket = socket_path.string();
    const std::string output_directory =
        options.output_directory.string();
    const std::string instrument = std::to_string(kInstrumentId);
    const std::string page_records =
        std::to_string(options.page_records);
    const std::string rounds = std::to_string(options.rounds);
    const std::string warmups = std::to_string(options.warmups);
    const std::string expected_records =
        std::to_string(options.records);
    const std::string benchmark_run_id =
        std::to_string(options.benchmark_run_id);
    std::vector<std::string> arguments{
        L2FLOW_HISTORY_BENCHMARK_PYTHON_EXECUTABLE,
        "-B",
        L2FLOW_HISTORY_BENCHMARK_PYTHON_PROBE,
        socket,
        L2FLOW_HISTORY_BENCHMARK_PYTHON_SOURCE,
        output_directory,
        "--instrument-id",
        instrument,
        "--page-records",
        page_records,
        "--rounds",
        rounds,
        "--warmup-rounds",
        warmups,
        "--expected-records",
        expected_records,
        "--benchmark-run-id",
        benchmark_run_id,
    };
    std::vector<char*> native_arguments;
    native_arguments.reserve(arguments.size() + 1U);
    for (std::string& argument : arguments) {
        native_arguments.push_back(argument.data());
    }
    native_arguments.push_back(nullptr);

    const pid_t child = ::fork();
    if (child < 0) {
        std::perror("fork Python history benchmark probe");
        return 1;
    }
    if (child == 0) {
        ::execv(
            L2FLOW_HISTORY_BENCHMARK_PYTHON_EXECUTABLE,
            native_arguments.data());
        constexpr char message[] =
            "failed to exec Python history benchmark probe\n";
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
    if (waited != child) {
        std::perror("waitpid Python history benchmark probe");
        return 1;
    }
    if (!WIFEXITED(status)) {
        std::cerr << "Python history benchmark probe terminated by signal\n";
        return 1;
    }
    return WEXITSTATUS(status);
}

bool ValidateServerEvents(
    const std::vector<ipc::RealtimeHistoryPageStageTimingV1>& events,
    const Options& options,
    const Fixture& fixture) {
    const std::uint64_t expected_events = ExpectedPageEvents(options);
    if (events.size() !=
        static_cast<std::size_t>(expected_events)) {
        std::cerr << "server page event count mismatch: observed="
                  << events.size() << " expected=" << expected_events
                  << '\n';
        return false;
    }
    std::uint64_t total_records = 0U;
    std::uint64_t total_snapshots = 0U;
    std::uint64_t total_ticks = 0U;
    std::set<std::uint64_t> open_request_ids;
    std::set<std::tuple<std::uint64_t, std::uint64_t, std::uint64_t>>
        page_keys;
    for (const auto& event : events) {
        const auto key = std::make_tuple(
            event.open_request_id,
            event.generation,
            event.page_index);
        if (event.open_request_id == 0U ||
            event.read_request_id == 0U ||
            event.generation != 1U ||
            event.instrument_id != kInstrumentId ||
            event.reserved0 != 0U ||
            event.record_count == 0U ||
            event.snapshot_count + event.tick_count !=
                event.record_count ||
            event.page_mapping_bytes < kPageHeaderBytes ||
            event.clock_read_failures != 0U ||
            !page_keys.insert(key).second) {
            std::cerr << "invalid server page timing event\n";
            return false;
        }
        open_request_ids.insert(event.open_request_id);
        total_records += event.record_count;
        total_snapshots += event.snapshot_count;
        total_ticks += event.tick_count;
    }
    const std::uint64_t scan_count =
        static_cast<std::uint64_t>(options.rounds) +
        static_cast<std::uint64_t>(options.warmups);
    if (open_request_ids.size() !=
            static_cast<std::size_t>(scan_count) ||
        total_records != options.records * scan_count ||
        total_snapshots != fixture.snapshot_count() * scan_count ||
        total_ticks != fixture.tick_count() * scan_count) {
        std::cerr << "server page timing totals do not reconcile\n";
        return false;
    }
    return true;
}

bool WriteServerCsv(
    const std::filesystem::path& path,
    const Options& options,
    const std::vector<ipc::RealtimeHistoryPageStageTimingV1>&
        events) {
    std::ofstream output(path, std::ios::trunc);
    if (!output) {
        std::cerr << "cannot open server CSV: " << path << '\n';
        return false;
    }
    output
        << "schema_version,benchmark_run_id,open_request_id,"
           "read_request_id,generation,instrument_id,page_index,"
           "record_count,snapshot_count,tick_count,page_mapping_bytes,"
           "clock_read_failures,cursor_read_ns,classify_layout_ns,"
           "memfd_prepare_ns,projection_ns,memfd_finalize_ns,"
           "build_total_ns,token_ns,send_ns\n";
    for (const auto& event : events) {
        output << 1U << ',' << options.benchmark_run_id << ','
               << event.open_request_id << ','
               << event.read_request_id << ',' << event.generation << ','
               << event.instrument_id << ',' << event.page_index << ','
               << event.record_count << ',' << event.snapshot_count << ','
               << event.tick_count << ',' << event.page_mapping_bytes
               << ',' << event.clock_read_failures << ','
               << event.cursor_read_ns << ','
               << event.classify_layout_ns << ','
               << event.memfd_prepare_ns << ','
               << event.projection_ns << ','
               << event.memfd_finalize_ns << ','
               << event.build_total_ns << ',' << event.token_ns << ','
               << event.send_ns << '\n';
    }
    output.close();
    if (!output) {
        std::cerr << "failed to write server CSV: " << path << '\n';
        return false;
    }
    return true;
}

bool WriteConfigCsv(
    const std::filesystem::path& path,
    const Options& options) {
    std::ofstream output(path, std::ios::trunc);
    if (!output) {
        std::cerr << "cannot open benchmark config CSV: " << path
                  << '\n';
        return false;
    }
    output
        << "schema_version,benchmark_run_id,records,page_records,rounds,"
           "warmup_rounds,snapshot_every,seed,workload,trade_date,"
           "instrument_id\n"
        << 1U << ',' << options.benchmark_run_id << ','
        << options.records << ',' << options.page_records << ','
        << options.rounds << ',' << options.warmups << ','
        << options.snapshot_every << ',' << options.seed << ','
        << WorkloadName(options.snapshot_every) << ',' << kTradeDate
        << ',' << kInstrumentId << '\n';
    output.close();
    if (!output) {
        std::cerr << "failed to write benchmark config CSV: " << path
                  << '\n';
        return false;
    }
    return true;
}

int Run(const Options& options) {
    std::error_code filesystem_error;
    static_cast<void>(std::filesystem::create_directories(
        options.output_directory, filesystem_error));
    if (filesystem_error ||
        !std::filesystem::is_directory(
            options.output_directory, filesystem_error) ||
        filesystem_error) {
        std::cerr << "cannot create output directory: "
                  << options.output_directory << '\n';
        return 1;
    }

    const auto fixture_begin = std::chrono::steady_clock::now();
    Fixture fixture;
    if (!fixture.Initialize(options)) {
        return 1;
    }
    const auto fixture_elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - fixture_begin);

    const std::uint64_t expected_events = ExpectedPageEvents(options);
    if (expected_events >
        static_cast<std::uint64_t>(
            std::numeric_limits<std::size_t>::max())) {
        std::cerr << "observer event count exceeds size_t\n";
        return 1;
    }
    TimingCollector collector(
        static_cast<std::size_t>(expected_events));
    ScopedTempDirectory socket_directory;
    if (!socket_directory.valid()) {
        std::cerr << "cannot create private socket directory\n";
        return 1;
    }
    const std::filesystem::path socket_path =
        socket_directory.path() / "control.sock";

    std::uint64_t maximum_history_page_bytes = 0U;
    if (!CheckedHistoryPageBytes(
            options.page_records,
            &maximum_history_page_bytes)) {
        std::cerr << "history page byte limit overflows\n";
        return 1;
    }
    ipc::RealtimeSharedServiceConfigV1 config{};
    config.run_id = fixture.run_id();
    config.session_epoch = 1U;
    config.trade_date = kTradeDate;
    config.registry = &fixture.registry();
    config.tick_ring_capacity = 1U;
    config.maximum_mapping_bytes = 16U * 1024U * 1024U;
    // One logical reader is active at a time. Keep a second worker slot so a
    // just-closed EOF connection can finish its asynchronous server-side
    // teardown while the next measured scan is admitted.
    config.maximum_history_readers = 2U;
    config.maximum_history_page_records = options.page_records;
    config.maximum_history_page_bytes =
        maximum_history_page_bytes;
    config.history_reader_idle_timeout = std::chrono::minutes(5);
    config.history_stage_observer = &collector;
    config.control_socket_path = socket_path;

    std::shared_ptr<ipc::RealtimeSharedMarketServiceV1> service;
    int system_error = 0;
    const ipc::RealtimeSharedServiceCreateErrorV1 create_error =
        ipc::RealtimeSharedMarketServiceV1::Create(
            std::move(config), &service, &system_error);
    if (create_error !=
            ipc::RealtimeSharedServiceCreateErrorV1::kNone ||
        service == nullptr || system_error != 0) {
        std::cerr << "service create failed: "
                  << ipc::RealtimeSharedServiceCreateErrorNameV1(
                         create_error)
                  << " errno=" << system_error << '\n';
        return 1;
    }
    if (!service->Start(&system_error) || system_error != 0) {
        std::cerr << "service start failed: errno=" << system_error
                  << '\n';
        service->StopControl();
        return 1;
    }
    if (!service->PublishStoreGeneration(fixture.generation())) {
        std::cerr << "Store generation publication failed\n";
        service->StopControl();
        return 1;
    }

    const int probe_status = RunPythonProbe(socket_path, options);
    service->StopControl();
    service.reset();

    const auto events = collector.Snapshot();
    const bool observer_ok = !collector.failed();
    const bool events_ok =
        observer_ok &&
        ValidateServerEvents(events, options, fixture);
    const bool csv_ok = WriteServerCsv(
        options.output_directory / "server_pages.csv",
        options,
        events);
    const bool config_csv_ok = WriteConfigCsv(
        options.output_directory / "benchmark_config.csv",
        options);

    std::cerr << "workload=" << WorkloadName(options.snapshot_every)
              << " records=" << options.records
              << " snapshots=" << fixture.snapshot_count()
              << " ticks=" << fixture.tick_count()
              << " page_records=" << options.page_records
              << " rounds=" << options.rounds
              << " warmups=" << options.warmups
              << " benchmark_run_id=" << options.benchmark_run_id
              << " fixture_ms=" << fixture_elapsed.count()
              << " server_pages=" << events.size()
              << " output=" << options.output_directory << '\n';
    if (probe_status != 0) {
        std::cerr << "Python benchmark probe failed with status "
                  << probe_status << '\n';
    }
    return probe_status == 0 && events_ok && csv_ok && config_csv_ok
               ? 0
               : 1;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 2 && argv != nullptr &&
        (std::string_view(argv[1]) == "--help" ||
         std::string_view(argv[1]) == "-h")) {
        PrintUsage(std::cout, argv[0]);
        return 0;
    }
    Options options{};
    if (!ParseOptions(argc, argv, &options)) {
        if (argc <= 1) {
            PrintUsage(std::cerr, argc == 1 ? argv[0] : "benchmark");
        }
        return 2;
    }
    try {
        options.output_directory =
            std::filesystem::absolute(options.output_directory);
        return Run(options);
    } catch (const std::exception& error) {
        std::cerr << "exception: " << error.what() << '\n';
        return 1;
    } catch (...) {
        std::cerr << "unknown exception\n";
        return 1;
    }
}
