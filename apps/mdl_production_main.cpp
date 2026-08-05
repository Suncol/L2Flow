#include "l2flow/common/identity128.h"
#include "l2flow/common/linux_thread_affinity_v1.h"
#include "l2flow/market/daily_instrument_catalog_loader_v2.h"
#include "l2flow/runtime/fast_tick_pipeline_v1.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

namespace common = l2flow::common;
namespace market = l2flow::market;
namespace runtime = l2flow::runtime;

volatile std::sig_atomic_t g_stop_requested = 0;

extern "C" void HandleStop(int) noexcept {
    g_stop_requested = 1;
}

bool InstallSignals(int* error_number) noexcept {
    struct sigaction action {};
    action.sa_handler = &HandleStop;
    action.sa_flags = 0;
    if (::sigemptyset(&action.sa_mask) != 0 ||
        ::sigaction(SIGINT, &action, nullptr) != 0 ||
        ::sigaction(SIGTERM, &action, nullptr) != 0) {
        if (error_number != nullptr) {
            *error_number = errno;
        }
        return false;
    }
    return true;
}

struct Options final {
    std::filesystem::path sdk_library;
    std::filesystem::path daily_catalog;
    std::uint32_t trade_date = 0U;
    std::uint64_t catalog_version = 0U;
    std::uint64_t session_epoch = 0U;
    std::string server_address;
    std::string user_name;
    std::string sdk_log_prefix = "l2flow-fast";
    std::uint32_t sdk_work_threads = 1U;

    std::uint32_t tick_workers = 1U;
    std::uint32_t event_workers = 1U;
    std::uint32_t kline_workers = 1U;
    std::vector<common::LinuxCpuSetV1> tick_cpu_sets;
    std::vector<common::LinuxCpuSetV1> event_cpu_sets;
    std::vector<common::LinuxCpuSetV1> kline_cpu_sets;

    std::uint64_t fast_maximum_records = 0U;
    std::size_t maximum_order_states_per_instrument = 65'536U;
    std::size_t maximum_event_inputs_per_instrument = 1'000'000U;
    std::size_t maximum_events_per_instrument = 4'000'000U;
    std::size_t maximum_event_changes_per_instrument = 8'000'000U;
    std::size_t maximum_kline_trades_per_instrument = 1'000'000U;
    std::size_t maximum_kline_bars_per_instrument = 100'000U;
    std::size_t maximum_kline_changes_per_instrument = 8'000'000U;
    std::size_t raw_tick_queue_capacity = 65'536U;
    std::size_t raw_tick_prewarm_per_shard = 4096U;
    std::size_t event_queue_capacity = 32'768U;
    std::size_t kline_queue_capacity = 32'768U;
    std::vector<std::uint32_t> kline_windows_ms{1000U};
    bool help = false;
};

void PrintUsage(std::ostream& output) {
    output
        << "Usage: mdl-production-router OPTIONS\n"
        << "Required:\n"
        << "  --sdk-library PATH\n"
        << "  --daily-catalog PATH\n"
        << "  --trade-date YYYYMMDD\n"
        << "  --catalog-version N\n"
        << "  --session-epoch N\n"
        << "  --server ADDRESS\n"
        << "  --user TOKEN\n"
        << "  --fast-max-records N\n"
        << "  --tick-cpu-set LIST       repeat once per Tick worker\n"
        << "  --event-cpu-set LIST      repeat once per Event worker\n"
        << "  --kline-cpu-set LIST      repeat once per KLine worker\n"
        << "Worker and capacity options:\n"
        << "  --tick-workers N --event-workers N --kline-workers N\n"
        << "  --raw-tick-queue N --raw-tick-prewarm-per-shard N\n"
        << "  --event-queue N --kline-queue N\n"
        << "  --event-order-states-per-instrument N\n"
        << "  --event-inputs-per-instrument N\n"
        << "  --event-rows-per-instrument N\n"
        << "  --event-changes-per-instrument N\n"
        << "  --kline-trades-per-instrument N\n"
        << "  --kline-bars-per-instrument N\n"
        << "  --kline-changes-per-instrument N\n"
        << "  --kline-window-ms N       repeat for each window\n"
        << "  --sdk-work-threads 1 --sdk-log-prefix TEXT\n";
}

template <typename Unsigned>
bool ParseUnsigned(std::string_view text, Unsigned* output) noexcept {
    static_assert(std::is_unsigned_v<Unsigned>);
    if (output == nullptr || text.empty()) {
        return false;
    }
    Unsigned value = 0U;
    const char* const begin = text.data();
    const char* const end = begin + text.size();
    const auto result = std::from_chars(begin, end, value, 10);
    if (result.ec != std::errc{} || result.ptr != end || value == 0U) {
        return false;
    }
    *output = value;
    return true;
}

bool TakeValue(
    int argc,
    char** argv,
    int* index,
    std::string_view* output) noexcept {
    if (index == nullptr || output == nullptr || *index + 1 >= argc) {
        return false;
    }
    ++(*index);
    *output = argv[*index];
    return !output->empty();
}

bool ParseCpuSet(
    std::string_view text,
    std::vector<common::LinuxCpuSetV1>* output,
    std::string* error) {
    common::LinuxCpuSetV1 cpus;
    const auto parsed = common::ParseLinuxCpuSetV1(text, &cpus);
    if (parsed != common::LinuxCpuSetParseErrorV1::kNone) {
        *error = "invalid CPU set: " +
            std::string(common::LinuxCpuSetParseErrorNameV1(parsed));
        return false;
    }
    try {
        output->push_back(cpus);
        return true;
    } catch (...) {
        *error = "cannot retain CPU set";
        return false;
    }
}

bool ParseOptions(
    int argc,
    char** argv,
    Options* options,
    std::string* error) {
    if (options == nullptr || error == nullptr) {
        return false;
    }
    bool custom_windows = false;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument == "--help" || argument == "-h") {
            options->help = true;
            continue;
        }
        std::string_view value;
        if (!TakeValue(argc, argv, &index, &value)) {
            *error = "missing value for " + std::string(argument);
            return false;
        }
        if (argument == "--sdk-library") {
            options->sdk_library = value;
        } else if (argument == "--daily-catalog") {
            options->daily_catalog = value;
        } else if (argument == "--server") {
            options->server_address = value;
        } else if (argument == "--user") {
            options->user_name = value;
        } else if (argument == "--sdk-log-prefix") {
            options->sdk_log_prefix = value;
        } else if (argument == "--trade-date") {
            if (!ParseUnsigned(value, &options->trade_date)) {
                *error = "invalid --trade-date";
                return false;
            }
        } else if (argument == "--catalog-version") {
            if (!ParseUnsigned(value, &options->catalog_version)) {
                *error = "invalid --catalog-version";
                return false;
            }
        } else if (argument == "--session-epoch") {
            if (!ParseUnsigned(value, &options->session_epoch)) {
                *error = "invalid --session-epoch";
                return false;
            }
        } else if (argument == "--sdk-work-threads") {
            if (!ParseUnsigned(value, &options->sdk_work_threads) ||
                options->sdk_work_threads != 1U) {
                *error =
                    "--sdk-work-threads must be 1 for serialized ingress";
                return false;
            }
        } else if (argument == "--tick-workers") {
            if (!ParseUnsigned(value, &options->tick_workers)) {
                *error = "invalid --tick-workers";
                return false;
            }
        } else if (argument == "--event-workers") {
            if (!ParseUnsigned(value, &options->event_workers)) {
                *error = "invalid --event-workers";
                return false;
            }
        } else if (argument == "--kline-workers") {
            if (!ParseUnsigned(value, &options->kline_workers)) {
                *error = "invalid --kline-workers";
                return false;
            }
        } else if (argument == "--fast-max-records") {
            if (!ParseUnsigned(value, &options->fast_maximum_records)) {
                *error = "invalid --fast-max-records";
                return false;
            }
        } else if (argument == "--raw-tick-queue") {
            if (!ParseUnsigned(value, &options->raw_tick_queue_capacity)) {
                *error = "invalid --raw-tick-queue";
                return false;
            }
        } else if (argument == "--raw-tick-prewarm-per-shard") {
            if (!ParseUnsigned(
                    value, &options->raw_tick_prewarm_per_shard)) {
                *error = "invalid --raw-tick-prewarm-per-shard";
                return false;
            }
        } else if (argument == "--event-queue") {
            if (!ParseUnsigned(value, &options->event_queue_capacity)) {
                *error = "invalid --event-queue";
                return false;
            }
        } else if (argument == "--kline-queue") {
            if (!ParseUnsigned(value, &options->kline_queue_capacity)) {
                *error = "invalid --kline-queue";
                return false;
            }
        } else if (argument ==
                   "--event-order-states-per-instrument") {
            if (!ParseUnsigned(
                    value,
                    &options->maximum_order_states_per_instrument)) {
                *error = "invalid Event order-state capacity";
                return false;
            }
        } else if (argument == "--event-inputs-per-instrument") {
            if (!ParseUnsigned(
                    value,
                    &options->maximum_event_inputs_per_instrument)) {
                *error = "invalid Event input capacity";
                return false;
            }
        } else if (argument == "--event-rows-per-instrument") {
            if (!ParseUnsigned(
                    value, &options->maximum_events_per_instrument)) {
                *error = "invalid Event row capacity";
                return false;
            }
        } else if (argument == "--event-changes-per-instrument") {
            if (!ParseUnsigned(
                    value,
                    &options->maximum_event_changes_per_instrument)) {
                *error = "invalid Event change-record capacity";
                return false;
            }
        } else if (argument == "--kline-trades-per-instrument") {
            if (!ParseUnsigned(
                    value,
                    &options->maximum_kline_trades_per_instrument)) {
                *error = "invalid KLine trade capacity";
                return false;
            }
        } else if (argument == "--kline-bars-per-instrument") {
            if (!ParseUnsigned(
                    value,
                    &options->maximum_kline_bars_per_instrument)) {
                *error = "invalid KLine bar capacity";
                return false;
            }
        } else if (argument == "--kline-changes-per-instrument") {
            if (!ParseUnsigned(
                    value,
                    &options->maximum_kline_changes_per_instrument)) {
                *error = "invalid KLine change-record capacity";
                return false;
            }
        } else if (argument == "--kline-window-ms") {
            std::uint32_t window = 0U;
            if (!ParseUnsigned(value, &window)) {
                *error = "invalid --kline-window-ms";
                return false;
            }
            if (!custom_windows) {
                options->kline_windows_ms.clear();
                custom_windows = true;
            }
            options->kline_windows_ms.push_back(window);
        } else if (argument == "--tick-cpu-set") {
            if (!ParseCpuSet(value, &options->tick_cpu_sets, error)) {
                return false;
            }
        } else if (argument == "--event-cpu-set") {
            if (!ParseCpuSet(value, &options->event_cpu_sets, error)) {
                return false;
            }
        } else if (argument == "--kline-cpu-set") {
            if (!ParseCpuSet(value, &options->kline_cpu_sets, error)) {
                return false;
            }
        } else {
            *error = "unknown option: " + std::string(argument);
            return false;
        }
    }
    if (options->help) {
        return true;
    }
    if (options->sdk_library.empty() || options->daily_catalog.empty() ||
        options->trade_date == 0U || options->catalog_version == 0U ||
        options->session_epoch == 0U ||
        options->server_address.empty() || options->user_name.empty() ||
        options->fast_maximum_records == 0U) {
        *error = "one or more required options are missing";
        return false;
    }
    if (options->tick_cpu_sets.size() != options->tick_workers ||
        options->event_cpu_sets.size() != options->event_workers ||
        options->kline_cpu_sets.size() != options->kline_workers) {
        *error = "supply exactly one CPU set per configured worker";
        return false;
    }
    if (options->raw_tick_prewarm_per_shard >
            options->raw_tick_queue_capacity &&
        options->raw_tick_prewarm_per_shard -
                options->raw_tick_queue_capacity >
            2U) {
        *error =
            "raw Tick prewarm exceeds queue plus active shard slots";
        return false;
    }
    return true;
}

std::vector<std::uint32_t> RoundRobinRoutes(
    std::size_t instruments,
    std::uint32_t workers) {
    std::vector<std::uint32_t> result(instruments);
    for (std::size_t ordinal = 0U; ordinal < instruments; ++ordinal) {
        result[ordinal] = static_cast<std::uint32_t>(
            ordinal % static_cast<std::size_t>(workers));
    }
    return result;
}

bool KLineWindows(
    const std::vector<std::uint32_t>& milliseconds,
    std::vector<market::KLineWindowSpecV1>* output) {
    output->clear();
    try {
        output->reserve(milliseconds.size());
        for (std::uint32_t value : milliseconds) {
            constexpr std::uint64_t nanoseconds_per_millisecond =
                1'000'000U;
            const std::uint64_t duration =
                static_cast<std::uint64_t>(value) *
                nanoseconds_per_millisecond;
            output->push_back(market::KLineWindowSpecV1{
                value, duration});
        }
        return !output->empty();
    } catch (...) {
        output->clear();
        return false;
    }
}

runtime::FastTickPipelineConfigV1 MakePipelineConfig(
    const Options& options,
    const common::Identity128& session_id,
    std::shared_ptr<const market::DailyInstrumentCatalogV2> catalog) {
    runtime::FastTickPipelineConfigV1 config{};
    config.run_id = session_id;
    config.trade_date = options.trade_date;
    config.daily_catalog = std::move(catalog);
    config.source_stream_ids = {1U, 2U};
    config.maximum_sdk_message_bytes = 4096U;
    config.raw_tick_queue_capacity_per_source_worker =
        options.raw_tick_queue_capacity;
    config.prewarm_message_bytes = 4096U;
    config.prewarm_message_count_per_source_worker =
        options.raw_tick_prewarm_per_shard;
    config.enforce_receive_trade_date = true;
    config.sdk.enabled = true;
    config.sdk.library_path = options.sdk_library;
    config.sdk.work_threads = static_cast<int>(options.sdk_work_threads);
    config.sdk.io_threads = 1;
    config.sdk.log_prefix = options.sdk_log_prefix;
    config.sdk.server_address = options.server_address;
    config.sdk.user_name = options.user_name;

    const std::size_t instruments =
        config.daily_catalog->instrument_count();
    config.planes.fast.session_id = session_id;
    config.planes.fast.trade_date = options.trade_date;
    config.planes.fast.instrument_count = instruments;
    config.planes.fast.worker_count = options.tick_workers;
    config.planes.fast.maximum_session_records =
        options.fast_maximum_records;
    config.planes.fast.records_per_chunk = 256U;
    config.planes.fast.maximum_records_per_read = 64U * 1024U;
    config.planes.fast.coverage_from_open = true;
    config.planes.fast.tick_routes = RoundRobinRoutes(
        instruments, options.tick_workers);

    config.planes.event.session_id = session_id;
    config.planes.event.trade_date = options.trade_date;
    config.planes.event.instrument_count = instruments;
    config.planes.event.worker_count = options.event_workers;
    config.planes.event.maximum_order_states_per_instrument =
        options.maximum_order_states_per_instrument;
    config.planes.event.maximum_inputs_per_instrument =
        options.maximum_event_inputs_per_instrument;
    config.planes.event.maximum_events_per_instrument =
        options.maximum_events_per_instrument;
    config.planes.event.maximum_change_records_per_instrument =
        options.maximum_event_changes_per_instrument;
    config.planes.event.event_routes = RoundRobinRoutes(
        instruments, options.event_workers);

    config.planes.kline.session_id = session_id;
    config.planes.kline.trade_date = options.trade_date;
    config.planes.kline.instrument_count = instruments;
    config.planes.kline.worker_count = options.kline_workers;
    static_cast<void>(KLineWindows(
        options.kline_windows_ms, &config.planes.kline.windows));
    config.planes.kline.maximum_trades_per_instrument =
        options.maximum_kline_trades_per_instrument;
    config.planes.kline.maximum_bars_per_instrument =
        options.maximum_kline_bars_per_instrument;
    config.planes.kline.maximum_change_records_per_instrument =
        options.maximum_kline_changes_per_instrument;
    config.planes.kline.kline_routes = RoundRobinRoutes(
        instruments, options.kline_workers);

    config.planes.event_queue_capacity_per_tick_worker =
        options.event_queue_capacity;
    config.planes.kline_queue_capacity_per_tick_worker =
        options.kline_queue_capacity;
    config.planes.affinity.enforce = true;
    config.planes.affinity.tick_workers = options.tick_cpu_sets;
    config.planes.affinity.event_workers = options.event_cpu_sets;
    config.planes.affinity.kline_workers = options.kline_cpu_sets;
    return config;
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    std::string error;
    if (!ParseOptions(argc, argv, &options, &error)) {
        std::cerr << error << '\n';
        PrintUsage(std::cerr);
        return 2;
    }
    if (options.help) {
        PrintUsage(std::cout);
        return 0;
    }
    int signal_error = 0;
    if (!InstallSignals(&signal_error)) {
        std::cerr << "cannot install signal handlers: "
                  << signal_error << '\n';
        return 1;
    }

    market::DailyInstrumentCatalogFileOptionsV2 catalog_options{};
    catalog_options.path = options.daily_catalog;
    catalog_options.expected_trade_date = options.trade_date;
    catalog_options.expected_catalog_version = options.catalog_version;
    catalog_options.session_epoch = options.session_epoch;
    auto loaded = market::LoadDailyInstrumentCatalogFileV2(
        catalog_options);
    if (!loaded.ok()) {
        std::cerr << "daily catalog load failed: "
                  << market::DailyInstrumentCatalogFileErrorNameV2(
                         loaded.error)
                  << " at line " << loaded.line << '\n';
        return 1;
    }
    if (options.tick_workers > loaded.catalog->instrument_count() ||
        options.event_workers > loaded.catalog->instrument_count() ||
        options.kline_workers > loaded.catalog->instrument_count()) {
        std::cerr << "worker count exceeds instrument count\n";
        return 2;
    }
    common::Identity128 session_id{};
    int identity_error = 0;
    if (!common::GenerateIdentity128(&session_id, &identity_error)) {
        std::cerr << "cannot generate session identity: "
                  << identity_error << '\n';
        return 1;
    }
    auto catalog =
        std::shared_ptr<const market::DailyInstrumentCatalogV2>(
            std::move(loaded.catalog));
    runtime::FastTickPipelineConfigV1 config = MakePipelineConfig(
        options, session_id, std::move(catalog));
    std::unique_ptr<runtime::FastTickPipelineV1> pipeline;
    std::string detail;
    const auto create = runtime::FastTickPipelineV1::Create(
        std::move(config), &pipeline, &detail);
    if (create != runtime::FastTickPipelineCreateErrorV1::kNone ||
        pipeline == nullptr) {
        std::cerr << "FAST Tick pipeline creation failed: "
                  << runtime::FastTickPipelineCreateErrorNameV1(create);
        if (!detail.empty()) {
            std::cerr << ": " << detail;
        }
        std::cerr << '\n';
        return 1;
    }

    while (g_stop_requested == 0) {
        const runtime::FastTickPipelineSnapshotV1 snapshot =
            pipeline->Snapshot();
        if (snapshot.fatal || !snapshot.accepting) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    pipeline->StopAndDrain();
    const runtime::FastTickPipelineSnapshotV1 final =
        pipeline->Snapshot();
    std::cout << "session=" << common::Identity128Hex(session_id)
              << " accepted=" << final.accepted_messages
              << " fast=" << final.planes.fast_applied
              << " event=" << final.planes.event_applied
              << " kline=" << final.planes.kline_applied
              << " event_repairs="
              << final.planes.event_rebuild_attempts
              << " kline_repairs="
              << final.planes.kline_rebuild_attempts << '\n';
    return final.fatal ? 1 : 0;
}
