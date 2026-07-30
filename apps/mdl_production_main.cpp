#include "l2flow/common/identity128.h"
#if !defined(L2FLOW_HAS_LINUX_REALTIME_IPC_V2)
#error "mdl-production-router requires Linux realtime IPC Wire V2"
#endif
#include "l2flow/ipc/order_event_delta_control_v1.h"
#include "l2flow/ipc/realtime_shared_service_v2.h"
#include "l2flow/market/observed_instrument_directory_v2.h"
#include "l2flow/runtime/realtime_pipeline_v1.h"

#include <algorithm>
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
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

namespace common = l2flow::common;
namespace factor = l2flow::factor;
namespace ipc = l2flow::ipc;
namespace market = l2flow::market;
namespace runtime = l2flow::runtime;

static_assert(
    market::kObservedInstrumentDirectoryDefaultCapacityV2 <=
    std::numeric_limits<std::uint32_t>::max());
static_assert(market::kRealtimeHistorySourceCountV1 == 4U);

volatile std::sig_atomic_t g_stop_requested = 0;

extern "C" void HandleStopSignal(int) noexcept {
    g_stop_requested = 1;
}

bool InstallStopSignalHandlers(int* error_number) noexcept {
    struct sigaction action {};
    action.sa_handler = &HandleStopSignal;
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

bool CurrentFixedUtc8TradeDate(std::uint32_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    const auto shifted =
        std::chrono::system_clock::now() + std::chrono::hours(8);
    const std::chrono::year_month_day calendar(
        std::chrono::floor<std::chrono::days>(shifted));
    if (!calendar.ok()) {
        return false;
    }
    const int year = static_cast<int>(calendar.year());
    const unsigned int month =
        static_cast<unsigned int>(calendar.month());
    const unsigned int day = static_cast<unsigned int>(calendar.day());
    if (year <= 0 || year > 9999 || month == 0U || day == 0U) {
        return false;
    }
    *output = static_cast<std::uint32_t>(year) * 10'000U +
              static_cast<std::uint32_t>(month) * 100U +
              static_cast<std::uint32_t>(day);
    return true;
}

bool IsValidTradeDate(std::uint32_t value) noexcept {
    const std::uint32_t year_value = value / 10'000U;
    const std::uint32_t month_value = (value / 100U) % 100U;
    const std::uint32_t day_value = value % 100U;
    if (year_value == 0U || year_value > 9999U) {
        return false;
    }
    const std::chrono::year_month_day date{
        std::chrono::year(static_cast<int>(year_value)),
        std::chrono::month(static_cast<unsigned int>(month_value)),
        std::chrono::day(static_cast<unsigned int>(day_value))};
    return date.ok();
}

enum class IntervalWaitResult : std::uint8_t {
    kElapsed = 0U,
    kSignal,
    kTradeDateBoundary,
    kClockFailure,
};

IntervalWaitResult WaitForInterval(
    std::chrono::milliseconds interval,
    std::uint32_t expected_trade_date) {
    const auto deadline = std::chrono::steady_clock::now() + interval;
    constexpr auto maximum_sleep = std::chrono::milliseconds(100);
    for (;;) {
        if (g_stop_requested != 0) {
            return IntervalWaitResult::kSignal;
        }
        std::uint32_t current_trade_date = 0U;
        if (!CurrentFixedUtc8TradeDate(&current_trade_date)) {
            return IntervalWaitResult::kClockFailure;
        }
        if (current_trade_date != expected_trade_date) {
            return IntervalWaitResult::kTradeDateBoundary;
        }
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            return IntervalWaitResult::kElapsed;
        }
        const auto remaining = deadline - now;
        const auto maximum = std::chrono::duration_cast<
            std::chrono::steady_clock::duration>(maximum_sleep);
        std::this_thread::sleep_for(std::min(remaining, maximum));
    }
}

struct Options final {
    std::filesystem::path sdk_library;
    std::uint64_t session_epoch = 0U;
    std::uint32_t instrument_capacity = static_cast<std::uint32_t>(
        market::kObservedInstrumentDirectoryDefaultCapacityV2);
    std::uint32_t trade_date = 0U;
    std::string server_address;
    std::string user_name;
    std::string sdk_log_prefix = "l2flow-realtime";
    bool enable_mainland_a_share_filter = true;

    std::uint32_t instrument_store_workers = 4U;
    std::uint64_t intraday_store_maximum_records = 0U;
    std::uint64_t intraday_store_memory_bytes = 0U;
    std::uint32_t intraday_store_segment_kib = 64U;
    std::uint32_t intraday_store_batch_records = 64U * 1024U;
    bool intraday_store_from_open = false;
    bool intraday_store_maximum_records_set = false;
    bool intraday_store_memory_set = false;

    // Each duration in milliseconds is also its stable public window_id.
    std::vector<std::uint32_t> kline_windows_ms;
    std::uint32_t generation_interval_ms = 1000U;
    std::uint32_t generation_timeout_ms = 10'000U;

    std::filesystem::path ipc_socket;
    std::filesystem::path event_aggregator_socket;
    std::uint32_t event_aggregator_ready_timeout_ms = 30'000U;
    bool event_aggregator_ready_timeout_set = false;
    std::uint64_t ipc_tick_ring_records = 262'144U;
    std::uint64_t ipc_key_arena_bytes =
        16ULL * 1024ULL * 1024ULL;
    std::uint64_t ipc_maximum_mapping_bytes =
        2ULL * 1024ULL * 1024ULL * 1024ULL;
};

void PrintUsage(std::ostream& output) {
    output
        << "Usage: mdl-production-router [required options] [optional]\n"
        << "Required:\n"
        << "  --sdk-library PATH            vendor .so selected by operator\n"
        << "  --session-epoch N             positive u64 session identity\n"
        << "  --trade-date YYYYMMDD         fixed UTC+08:00 trading date\n"
        << "  --server-address HOST:PORT    vendor endpoint\n"
        << "  --user-name VALUE             nonempty vendor user/token field\n"
        << "  --ipc-socket PATH             absolute GET_SESSION UDS path\n"
        << "  --intraday-store-max-records N\n"
        << "                                positive u64 session record cap\n"
        << "  --intraday-store-memory-gib N positive u64 logical total GiB cap\n"
        << "  --intraday-store-from-open    required assertion that capture "
           "starts at market open\n"
        << "Optional:\n"
        << "  --instrument-capacity N       1..4294967294, default 65536\n"
        << "  --sdk-log-prefix PATH         default l2flow-realtime\n"
        << "  --enable-mainland-a-share-filter BOOL\n"
        << "                                true|false, default true\n"
        << "  --instrument-store-workers N  1..256, default 4\n"
        << "  --intraday-store-segment-kib N\n"
        << "                                4..16384, default 64\n"
        << "  --intraday-store-batch-records N\n"
        << "                                1..1048576, default 65536\n"
        << "  --kline-windows-ms LIST       comma-separated unique durations "
           "in 1..86400000;\n"
        << "                                duration-ms is the window id\n"
        << "  --generation-interval-ms N    1..60000, default 1000\n"
        << "  --generation-timeout-ms N     1..600000, default 10000\n"
        << "  --ipc-tick-ring-records N     positive u64, default 262144\n"
        << "  --ipc-key-arena-mib N         positive u64, default 16\n"
        << "  --ipc-max-mapping-mib N       positive u64, default 2048\n"
        << "  --event-aggregator-socket PATH\n"
        << "                                optional absolute event-delta "
           "GET_SESSION UDS;\n"
        << "                                when set, SDK connect waits for "
           "an exact-session READY\n"
        << "  --event-aggregator-ready-timeout-ms N\n"
        << "                                1..600000, default 30000; "
           "requires event socket\n"
        << "  --help\n\n"
        << "The session exposes only the observed universe. Capacity and key "
           "arena exhaustion are fatal; this process does not roll over or "
           "resume a session. The current production source catalog contains "
           "Shanghai and Shenzhen tuples only; enabling the filter does not "
           "add Beijing ingress.\n";
}

bool ParseU32(std::string_view text, std::uint32_t* output) noexcept {
    if (output == nullptr || text.empty()) {
        return false;
    }
    std::uint32_t value = 0U;
    const std::from_chars_result parsed = std::from_chars(
        text.data(), text.data() + text.size(), value, 10);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != text.data() + text.size()) {
        return false;
    }
    *output = value;
    return true;
}

bool ParseU64(std::string_view text, std::uint64_t* output) noexcept {
    if (output == nullptr || text.empty()) {
        return false;
    }
    std::uint64_t value = 0U;
    const std::from_chars_result parsed = std::from_chars(
        text.data(), text.data() + text.size(), value, 10);
    if (parsed.ec != std::errc{} ||
        parsed.ptr != text.data() + text.size()) {
        return false;
    }
    *output = value;
    return true;
}

bool ParseBool(std::string_view text, bool* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    if (text == "true") {
        *output = true;
        return true;
    }
    if (text == "false") {
        *output = false;
        return true;
    }
    return false;
}

bool ParsePositiveScaledBytes(
    std::string_view text,
    std::uint64_t scale,
    std::uint64_t* output) noexcept {
    std::uint64_t units = 0U;
    if (output == nullptr || scale == 0U ||
        !ParseU64(text, &units) || units == 0U ||
        units > std::numeric_limits<std::uint64_t>::max() / scale) {
        return false;
    }
    *output = units * scale;
    return true;
}

bool ParseKLineWindows(
    std::string_view text,
    std::vector<std::uint32_t>* output,
    std::string* error) {
    if (output == nullptr || error == nullptr || text.empty()) {
        return false;
    }
    std::vector<std::uint32_t> parsed;
    std::size_t begin = 0U;
    while (begin < text.size()) {
        const std::size_t comma = text.find(',', begin);
        const std::size_t end =
            comma == std::string_view::npos ? text.size() : comma;
        std::uint32_t duration_ms = 0U;
        if (end == begin ||
            !ParseU32(
                text.substr(begin, end - begin), &duration_ms) ||
            duration_ms == 0U || duration_ms > 86'400'000U ||
            std::find(
                parsed.begin(), parsed.end(), duration_ms) !=
                parsed.end()) {
            *error =
                "--kline-windows-ms requires unique comma-separated "
                "durations in 1..86400000";
            return false;
        }
        parsed.push_back(duration_ms);
        if (parsed.size() > market::kKLineMaximumWindowsV1) {
            *error = "--kline-windows-ms accepts at most 32 windows";
            return false;
        }
        if (comma == std::string_view::npos) {
            break;
        }
        if (comma + 1U == text.size()) {
            *error =
                "--kline-windows-ms must not end with a comma";
            return false;
        }
        begin = comma + 1U;
    }
    *output = std::move(parsed);
    return true;
}

bool TakeValue(
    int argc,
    char* argv[],
    int* index,
    std::string_view option,
    std::string_view* output,
    std::string* error) {
    if (index == nullptr || output == nullptr || error == nullptr ||
        *index + 1 >= argc || argv[*index + 1] == nullptr) {
        if (error != nullptr) {
            *error = std::string(option) + " requires a value";
        }
        return false;
    }
    *output = argv[++*index];
    if (output->empty()) {
        *error = std::string(option) + " rejects an empty value";
        return false;
    }
    return true;
}

bool ParseOptions(
    int argc,
    char* argv[],
    Options* output,
    bool* help,
    std::string* error) {
    if (output == nullptr || help == nullptr || error == nullptr) {
        return false;
    }
    *help = false;
    Options parsed{};
    std::set<std::string_view> seen;
    for (int index = 1; index < argc; ++index) {
        if (argv[index] == nullptr) {
            *error = "argv contains null";
            return false;
        }
        const std::string_view option(argv[index]);
        if (option == "--help") {
            if (argc != 2) {
                *error = "--help must be used alone";
                return false;
            }
            *help = true;
            return true;
        }
        if (option == "--intraday-store-from-open") {
            if (!seen.insert(option).second) {
                *error = "duplicate option: " + std::string(option);
                return false;
            }
            parsed.intraday_store_from_open = true;
            continue;
        }

        if (option != "--sdk-library" &&
            option != "--session-epoch" &&
            option != "--instrument-capacity" &&
            option != "--trade-date" &&
            option != "--server-address" &&
            option != "--user-name" &&
            option != "--sdk-log-prefix" &&
            option != "--enable-mainland-a-share-filter" &&
            option != "--instrument-store-workers" &&
            option != "--intraday-store-max-records" &&
            option != "--intraday-store-memory-gib" &&
            option != "--intraday-store-segment-kib" &&
            option != "--intraday-store-batch-records" &&
            option != "--kline-windows-ms" &&
            option != "--generation-interval-ms" &&
            option != "--generation-timeout-ms" &&
            option != "--ipc-socket" &&
            option != "--event-aggregator-socket" &&
            option != "--event-aggregator-ready-timeout-ms" &&
            option != "--ipc-tick-ring-records" &&
            option != "--ipc-key-arena-mib" &&
            option != "--ipc-max-mapping-mib") {
            *error = "unknown option: " + std::string(option);
            return false;
        }
        if (!seen.insert(option).second) {
            *error = "duplicate option: " + std::string(option);
            return false;
        }

        std::string_view value;
        if (!TakeValue(
                argc, argv, &index, option, &value, error)) {
            return false;
        }
        if (option == "--sdk-library") {
            parsed.sdk_library = std::string(value);
        } else if (option == "--session-epoch") {
            if (!ParseU64(value, &parsed.session_epoch) ||
                parsed.session_epoch == 0U) {
                *error = "--session-epoch must be positive u64";
                return false;
            }
        } else if (option == "--instrument-capacity") {
            std::uint64_t capacity = 0U;
            if (!ParseU64(value, &capacity) || capacity == 0U ||
                capacity >=
                    std::numeric_limits<std::uint32_t>::max()) {
                *error =
                    "--instrument-capacity must be 1..4294967294";
                return false;
            }
            parsed.instrument_capacity =
                static_cast<std::uint32_t>(capacity);
        } else if (option == "--trade-date") {
            if (value.size() != 8U ||
                !ParseU32(value, &parsed.trade_date) ||
                !IsValidTradeDate(parsed.trade_date)) {
                *error = "--trade-date must be a valid YYYYMMDD date";
                return false;
            }
        } else if (option == "--server-address") {
            parsed.server_address = value;
        } else if (option == "--user-name") {
            parsed.user_name = value;
        } else if (option == "--sdk-log-prefix") {
            parsed.sdk_log_prefix = value;
        } else if (option ==
                   "--enable-mainland-a-share-filter") {
            if (!ParseBool(
                    value,
                    &parsed.enable_mainland_a_share_filter)) {
                *error =
                    "--enable-mainland-a-share-filter must be "
                    "true or false";
                return false;
            }
        } else if (option == "--instrument-store-workers") {
            if (!ParseU32(
                    value, &parsed.instrument_store_workers) ||
                parsed.instrument_store_workers == 0U ||
                parsed.instrument_store_workers > 256U) {
                *error =
                    "--instrument-store-workers must be 1..256";
                return false;
            }
        } else if (option == "--intraday-store-max-records") {
            if (!ParseU64(
                    value,
                    &parsed.intraday_store_maximum_records) ||
                parsed.intraday_store_maximum_records == 0U) {
                *error =
                    "--intraday-store-max-records must be positive u64";
                return false;
            }
            parsed.intraday_store_maximum_records_set = true;
        } else if (option == "--intraday-store-memory-gib") {
            constexpr std::uint64_t bytes_per_gib =
                std::uint64_t{1024U} * 1024U * 1024U;
            if (!ParsePositiveScaledBytes(
                    value,
                    bytes_per_gib,
                    &parsed.intraday_store_memory_bytes)) {
                *error =
                    "--intraday-store-memory-gib must be a positive u64 "
                    "whose byte conversion does not overflow";
                return false;
            }
            parsed.intraday_store_memory_set = true;
        } else if (option == "--intraday-store-segment-kib") {
            if (!ParseU32(
                    value, &parsed.intraday_store_segment_kib) ||
                parsed.intraday_store_segment_kib <
                    market::
                        kIntradayInstrumentStoreMinimumSegmentBytesV1 /
                        1024U ||
                parsed.intraday_store_segment_kib >
                    market::
                        kIntradayInstrumentStoreMaximumSegmentBytesV1 /
                        1024U) {
                *error =
                    "--intraday-store-segment-kib must be 4..16384";
                return false;
            }
        } else if (option == "--intraday-store-batch-records") {
            if (!ParseU32(
                    value,
                    &parsed.intraday_store_batch_records) ||
                parsed.intraday_store_batch_records == 0U ||
                parsed.intraday_store_batch_records >
                    market::
                        kIntradayInstrumentStoreMaximumBatchRecordsV1) {
                *error =
                    "--intraday-store-batch-records must be 1..1048576";
                return false;
            }
        } else if (option == "--kline-windows-ms") {
            if (!ParseKLineWindows(
                    value, &parsed.kline_windows_ms, error)) {
                return false;
            }
        } else if (option == "--generation-interval-ms") {
            if (!ParseU32(
                    value, &parsed.generation_interval_ms) ||
                parsed.generation_interval_ms == 0U ||
                parsed.generation_interval_ms > 60'000U) {
                *error =
                    "--generation-interval-ms must be 1..60000";
                return false;
            }
        } else if (option == "--generation-timeout-ms") {
            if (!ParseU32(
                    value, &parsed.generation_timeout_ms) ||
                parsed.generation_timeout_ms == 0U ||
                parsed.generation_timeout_ms > 600'000U) {
                *error =
                    "--generation-timeout-ms must be 1..600000";
                return false;
            }
        } else if (option == "--ipc-socket") {
            parsed.ipc_socket = std::string(value);
        } else if (option == "--event-aggregator-socket") {
            parsed.event_aggregator_socket = std::string(value);
        } else if (
            option == "--event-aggregator-ready-timeout-ms") {
            if (!ParseU32(
                    value,
                    &parsed.event_aggregator_ready_timeout_ms) ||
                parsed.event_aggregator_ready_timeout_ms == 0U ||
                parsed.event_aggregator_ready_timeout_ms > 600'000U) {
                *error =
                    "--event-aggregator-ready-timeout-ms must be "
                    "1..600000";
                return false;
            }
            parsed.event_aggregator_ready_timeout_set = true;
        } else if (option == "--ipc-tick-ring-records") {
            if (!ParseU64(
                    value, &parsed.ipc_tick_ring_records) ||
                parsed.ipc_tick_ring_records == 0U) {
                *error =
                    "--ipc-tick-ring-records must be positive u64";
                return false;
            }
        } else if (option == "--ipc-key-arena-mib") {
            constexpr std::uint64_t bytes_per_mib =
                std::uint64_t{1024U} * 1024U;
            if (!ParsePositiveScaledBytes(
                    value,
                    bytes_per_mib,
                    &parsed.ipc_key_arena_bytes)) {
                *error =
                    "--ipc-key-arena-mib must be a positive u64 whose "
                    "byte conversion does not overflow";
                return false;
            }
        } else if (option == "--ipc-max-mapping-mib") {
            constexpr std::uint64_t bytes_per_mib =
                std::uint64_t{1024U} * 1024U;
            if (!ParsePositiveScaledBytes(
                    value,
                    bytes_per_mib,
                    &parsed.ipc_maximum_mapping_bytes)) {
                *error =
                    "--ipc-max-mapping-mib must be a positive u64 whose "
                    "byte conversion does not overflow";
                return false;
            }
        }
    }

    if (parsed.sdk_library.empty() ||
        parsed.session_epoch == 0U ||
        parsed.trade_date == 0U ||
        parsed.server_address.empty() ||
        parsed.user_name.empty() ||
        parsed.ipc_socket.empty()) {
        *error = "all required options must be supplied";
        return false;
    }
    if (!parsed.ipc_socket.is_absolute()) {
        *error = "--ipc-socket must be an absolute path";
        return false;
    }
    if (!parsed.event_aggregator_socket.empty() &&
        !parsed.event_aggregator_socket.is_absolute()) {
        *error =
            "--event-aggregator-socket must be an absolute path";
        return false;
    }
    if (parsed.event_aggregator_socket.empty() &&
        parsed.event_aggregator_ready_timeout_set) {
        *error =
            "--event-aggregator-ready-timeout-ms requires "
            "--event-aggregator-socket";
        return false;
    }
    if (!parsed.intraday_store_maximum_records_set ||
        !parsed.intraday_store_memory_set) {
        *error =
            "production requires explicit positive "
            "--intraday-store-max-records and --intraday-store-memory-gib";
        return false;
    }
    if (!parsed.intraday_store_from_open) {
        *error =
            "production requires --intraday-store-from-open; "
            "partial-session startup is outside this runtime";
        return false;
    }
    if (parsed.ipc_key_arena_bytes >
        parsed.ipc_maximum_mapping_bytes) {
        *error =
            "--ipc-key-arena-mib must not exceed "
            "--ipc-max-mapping-mib";
        return false;
    }
    *output = std::move(parsed);
    return true;
}

bool MinimumTickRingCapacity(
    const runtime::RealtimePipelineConfigV1& config,
    std::uint64_t* output) noexcept {
    if (output == nullptr) {
        return false;
    }
    std::size_t applied_window = 0U;
    if (!runtime::RealtimePipelineAppliedWindowCapacityV1(
            config, &applied_window)) {
        return false;
    }
    *output = static_cast<std::uint64_t>(applied_window);
    return true;
}

std::vector<market::KLineWindowSpecV1> BuildKLineWindows(
    const Options& options) {
    std::vector<market::KLineWindowSpecV1> windows;
    windows.reserve(options.kline_windows_ms.size());
    for (const std::uint32_t duration_ms :
         options.kline_windows_ms) {
        market::KLineWindowSpecV1 window{};
        window.window_id = duration_ms;
        window.duration_ns =
            static_cast<std::uint64_t>(duration_ms) *
            market::kKLineNanosecondsPerMillisecondV1;
        windows.push_back(window);
    }
    return windows;
}

bool PublishKLineGeneration(
    const runtime::RealtimePipelineCutResultV1& cut,
    const std::shared_ptr<ipc::RealtimeSharedMarketServiceV2>&
        ipc_service,
    std::string_view context) {
    if (!cut.kline_enabled) {
        return true;
    }
    if (cut.kline_generation == nullptr ||
        !ipc_service->PublishKLineGeneration(
            *cut.kline_generation)) {
        std::cerr
            << "mdl-production-router: " << context
            << " IPC KLine generation publication failed\n";
        return false;
    }
    return true;
}

void ReportFatalSnapshot(
    const runtime::RealtimePipelineSnapshotV1& snapshot) {
    std::cerr
        << "mdl-production-router: pipeline failed: accepted_sequence="
        << snapshot.processing_progress.accepted_sequence
        << " applied_sequence="
        << snapshot.processing_progress.applied_sequence
        << " processing_lag_records="
        << snapshot.processing_progress.processing_lag_records()
        << " filtered_messages=" << snapshot.filtered_messages
        << " last_decode_error="
        << static_cast<unsigned int>(snapshot.last_decode_error)
        << '\n';
}

bool EventAggregatorProbeErrorIsRetryable(
    ipc::OrderEventDeltaControlClientErrorV1 error) noexcept {
    using Error = ipc::OrderEventDeltaControlClientErrorV1;
    return error == Error::kConnectFailed ||
           error == Error::kTimeout ||
           error == Error::kTransportFailed ||
           error == Error::kUnavailable;
}

bool WaitForEventAggregatorReady(
    const Options& options,
    const common::Identity128& source_run_id,
    ipc::OrderEventDeltaControlSnapshotV1* output_snapshot,
    std::string* output_detail) {
    if (output_snapshot == nullptr || output_detail == nullptr ||
        options.event_aggregator_socket.empty()) {
        return false;
    }
    *output_snapshot = {};
    output_detail->clear();

    ipc::OrderEventDeltaControlClientConfigV1 config{};
    config.control_socket_path = options.event_aggregator_socket;
    config.expected_source_session.run_id = source_run_id;
    config.expected_source_session.session_epoch =
        options.session_epoch;
    config.expected_source_session.trade_date = options.trade_date;

    const auto deadline =
        std::chrono::steady_clock::now() +
        std::chrono::milliseconds(
            options.event_aggregator_ready_timeout_ms);
    ipc::OrderEventDeltaControlClientErrorV1 last_error =
        ipc::OrderEventDeltaControlClientErrorV1::kConnectFailed;
    int last_system_error = 0;

    for (;;) {
        if (g_stop_requested != 0) {
            *output_detail = "stop signal while waiting for READY";
            return false;
        }
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            *output_detail =
                "READY timeout: last_error=" +
                std::string(
                    ipc::OrderEventDeltaControlClientErrorNameV1(
                        last_error)) +
                " errno=" + std::to_string(last_system_error);
            return false;
        }

        auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - now);
        if (remaining.count() == 0) {
            remaining = std::chrono::milliseconds(1);
        }
        config.timeout = std::min(
            remaining, std::chrono::milliseconds(250));

        ipc::OrderEventDeltaControlSnapshotV1 snapshot{};
        int system_error = 0;
        const ipc::OrderEventDeltaControlClientErrorV1 probe_error =
            ipc::OrderEventDeltaControlProbeV1(
                config, &snapshot, &system_error);
        if (probe_error ==
            ipc::OrderEventDeltaControlClientErrorV1::kNone) {
            // The source pipeline has not been created, so an exact-session
            // event service can only be a pre-ingress READY if both public
            // prefixes are still empty. This is the startup guarantee that
            // prevents silently beginning after the first callback.
            if (snapshot.source_tick_consumed_sequence != 0U ||
                snapshot.event_published_sequence != 0U) {
                *output_detail =
                    "READY service is not at the pre-ingress origin";
                return false;
            }
            *output_snapshot = snapshot;
            return true;
        }
        last_error = probe_error;
        last_system_error = system_error;
        if (!EventAggregatorProbeErrorIsRetryable(probe_error)) {
            *output_detail =
                "READY probe failed: error=" +
                std::string(
                    ipc::OrderEventDeltaControlClientErrorNameV1(
                        probe_error)) +
                " errno=" + std::to_string(system_error);
            return false;
        }

        const auto after_probe = std::chrono::steady_clock::now();
        if (after_probe >= deadline) {
            continue;
        }
        const auto retry_delay = std::min(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - after_probe),
            std::chrono::milliseconds(25));
        const IntervalWaitResult wait =
            WaitForInterval(retry_delay, options.trade_date);
        if (wait == IntervalWaitResult::kSignal) {
            *output_detail = "stop signal while waiting for READY";
            return false;
        }
        if (wait == IntervalWaitResult::kTradeDateBoundary) {
            *output_detail =
                "trade-date boundary while waiting for READY";
            return false;
        }
        if (wait == IntervalWaitResult::kClockFailure) {
            *output_detail =
                "clock failure while waiting for READY";
            return false;
        }
    }
}

int Run(const Options& options) {
    std::uint32_t current_trade_date = 0U;
    if (!CurrentFixedUtc8TradeDate(&current_trade_date)) {
        std::cerr
            << "mdl-production-router: cannot read fixed UTC+08:00 date\n";
        return 1;
    }
    if (current_trade_date != options.trade_date) {
        std::cerr
            << "mdl-production-router: --trade-date must equal the current "
               "fixed UTC+08:00 date (current="
            << current_trade_date << ")\n";
        return 1;
    }

    common::Identity128 run_id{};
    int entropy_error = 0;
    if (!common::GenerateIdentity128(&run_id, &entropy_error)) {
        std::cerr
            << "mdl-production-router: run-id entropy failed: errno="
            << entropy_error << '\n';
        return 1;
    }

    market::ObservedInstrumentDirectoryConfigV2 directory_config{};
    directory_config.capacity =
        static_cast<std::size_t>(options.instrument_capacity);
    directory_config.session_epoch = options.session_epoch;
    std::unique_ptr<market::ObservedInstrumentDirectoryV2> directory;
    const market::ObservedInstrumentDirectoryErrorV2 directory_error =
        market::ObservedInstrumentDirectoryV2::Create(
            directory_config, &directory);
    if (directory_error !=
            market::ObservedInstrumentDirectoryErrorV2::kNone ||
        directory == nullptr) {
        std::cerr
            << "mdl-production-router: observed directory create failed: "
            << market::ObservedInstrumentDirectoryErrorNameV2(
                   directory_error)
            << '\n';
        return 1;
    }

    const std::vector<market::KLineWindowSpecV1> kline_windows =
        BuildKLineWindows(options);

    runtime::RealtimePipelineConfigV1 pipeline_config{};
    pipeline_config.run_id = run_id;
    pipeline_config.trade_date = options.trade_date;
    pipeline_config.directory = directory.get();
    pipeline_config.source_stream_ids =
        {1001U, 1002U, 2001U, 2002U};
    pipeline_config.store_worker_count =
        options.instrument_store_workers;
    pipeline_config.intraday_store.segment_target_bytes =
        static_cast<std::size_t>(
            options.intraday_store_segment_kib) *
        1024U;
    pipeline_config.intraday_store.maximum_session_records =
        options.intraday_store_maximum_records;
    pipeline_config.intraday_store.maximum_session_accounted_bytes =
        options.intraday_store_memory_bytes;
    pipeline_config.intraday_store.maximum_records_per_batch =
        static_cast<std::size_t>(
            options.intraday_store_batch_records);
    pipeline_config.intraday_store.coverage_from_open =
        options.intraday_store_from_open;
    pipeline_config.kline.windows = kline_windows;
    pipeline_config.enforce_receive_trade_date = true;
    pipeline_config.enable_mainland_a_share_filter =
        options.enable_mainland_a_share_filter;
    pipeline_config.sdk.enabled = true;
    pipeline_config.sdk.library_path = options.sdk_library;
    pipeline_config.sdk.server_address = options.server_address;
    pipeline_config.sdk.user_name = options.user_name;
    pipeline_config.sdk.log_prefix = options.sdk_log_prefix;
    pipeline_config.sdk.message_encoding =
        datayes::mdl::MDLEID_BINARY;
    pipeline_config.sdk.merge_message = false;

    std::uint64_t minimum_tick_ring_capacity = 0U;
    if (!MinimumTickRingCapacity(
            pipeline_config, &minimum_tick_ring_capacity)) {
        std::cerr
            << "mdl-production-router: applied dispatch window "
               "overflowed u64\n";
        return 1;
    }
    if (options.ipc_tick_ring_records <
        minimum_tick_ring_capacity) {
        std::cerr
            << "mdl-production-router: --ipc-tick-ring-records must "
               "cover the explicitly bounded applied dispatch window "
               "(minimum="
            << minimum_tick_ring_capacity << ")\n";
        return 1;
    }

    ipc::RealtimeSharedServiceConfigV2 ipc_config{};
    ipc_config.run_id = run_id;
    ipc_config.session_epoch = options.session_epoch;
    ipc_config.trade_date = options.trade_date;
    ipc_config.directory = directory.get();
    ipc_config.kline_windows = kline_windows;
    ipc_config.tick_ring_capacity =
        options.ipc_tick_ring_records;
    ipc_config.key_arena_bytes = options.ipc_key_arena_bytes;
    ipc_config.maximum_mapping_bytes =
        options.ipc_maximum_mapping_bytes;
    ipc_config.control_socket_path = options.ipc_socket;

    std::shared_ptr<ipc::RealtimeSharedMarketServiceV2> ipc_service;
    int ipc_system_error = 0;
    const ipc::RealtimeSharedServiceCreateErrorV2 ipc_error =
        ipc::RealtimeSharedMarketServiceV2::Create(
            std::move(ipc_config),
            &ipc_service,
            &ipc_system_error);
    if (ipc_error !=
            ipc::RealtimeSharedServiceCreateErrorV2::kNone ||
        ipc_service == nullptr) {
        std::cerr
            << "mdl-production-router: IPC V2 service create failed: "
            << ipc::RealtimeSharedServiceCreateErrorNameV2(
                   ipc_error)
            << " errno=" << ipc_system_error << '\n';
        return 1;
    }

    ipc_system_error = 0;
    if (!ipc_service->Start(&ipc_system_error)) {
        std::cerr
            << "mdl-production-router: IPC V2 control start failed: "
               "errno="
            << ipc_system_error << '\n';
        ipc_service->MarkFailed();
        ipc_service->StopControl();
        return 1;
    }
    std::cerr
        << "mdl-production-router: IPC V2 ACTIVE: socket="
        << ipc_service->control_socket_path()
        << " capacity=" << options.instrument_capacity
        << " bound_count=0 catalog_scope=OBSERVED_ONLY"
        << " coverage_complete=false"
        << " mapping_bytes=" << ipc_service->mapping_bytes()
        << " key_arena_bytes=" << options.ipc_key_arena_bytes
        << " mainland_a_share_filter="
        << (options.enable_mainland_a_share_filter
                ? "true"
                : "false")
        << '\n';

    if (!options.event_aggregator_socket.empty()) {
        ipc::OrderEventDeltaControlSnapshotV1 event_snapshot{};
        std::string ready_detail;
        if (!WaitForEventAggregatorReady(
                options,
                run_id,
                &event_snapshot,
                &ready_detail)) {
            std::cerr
                << "mdl-production-router: event aggregator READY "
                   "gate failed: "
                << ready_detail << '\n';
            ipc_service->MarkFailed();
            ipc_service->StopControl();
            return 1;
        }
        std::cerr
            << "mdl-production-router: event aggregator READY: socket="
            << options.event_aggregator_socket
            << " event_ring_capacity="
            << event_snapshot.event_session.ring_capacity
            << " source_tick_consumed_sequence=0"
            << " event_published_sequence=0\n";
    }

    pipeline_config.applied_record_sink = ipc_service;
    pipeline_config.instrument_binding_sink = ipc_service;
    pipeline_config.processing_progress_sink = ipc_service;
    pipeline_config.store_generation_sink = ipc_service;

    std::unique_ptr<runtime::RealtimePipelineV1> pipeline;
    std::string detail;
    const runtime::RealtimePipelineCreateErrorV1 create_error =
        runtime::RealtimePipelineV1::Create(
            std::move(pipeline_config), &pipeline, &detail);
    if (create_error !=
            runtime::RealtimePipelineCreateErrorV1::kNone ||
        pipeline == nullptr) {
        std::cerr
            << "mdl-production-router: pipeline create failed: "
            << runtime::RealtimePipelineCreateErrorNameV1(
                   create_error)
            << (detail.empty() ? "" : ": ") << detail << '\n';
        ipc_service->MarkFailed();
        ipc_service->StopControl();
        return 1;
    }

    const auto interval =
        std::chrono::milliseconds(
            options.generation_interval_ms);
    const auto timeout =
        std::chrono::milliseconds(
            options.generation_timeout_ms);
    int exit_code = 0;
    while (g_stop_requested == 0) {
        const IntervalWaitResult wait =
            WaitForInterval(interval, options.trade_date);
        if (wait == IntervalWaitResult::kSignal) {
            break;
        }
        if (wait == IntervalWaitResult::kClockFailure) {
            std::cerr
                << "mdl-production-router: civil-date clock read "
                   "failed\n";
            exit_code = 1;
            break;
        }
        if (wait == IntervalWaitResult::kTradeDateBoundary) {
            std::cerr
                << "mdl-production-router: trade-date boundary "
                   "reached; publishing final prior-day generation\n";
            break;
        }
        if (ipc_service->failed()) {
            std::cerr
                << "mdl-production-router: IPC V2 projection failed\n";
            exit_code = 1;
            break;
        }

        const runtime::RealtimePipelineSnapshotV1 before_cut =
            pipeline->Snapshot();
        if (before_cut.fatal) {
            ReportFatalSnapshot(before_cut);
            exit_code = 1;
            break;
        }
        if (before_cut.trade_date_boundary_reached) {
            std::cerr
                << "mdl-production-router: callback observed "
                   "trade-date boundary; publishing final prior-day "
                   "generation\n";
            break;
        }

        const runtime::RealtimePipelineCutResultV1 cut =
            pipeline->CutAndPublishGeneration(timeout);
        if (!cut.published()) {
            const runtime::RealtimePipelineSnapshotV1 failed_snapshot =
                pipeline->Snapshot();
            if (cut.error ==
                    runtime::RealtimePipelineCutErrorV1::kStopped &&
                failed_snapshot.trade_date_boundary_reached) {
                std::cerr
                    << "mdl-production-router: callback closed "
                       "admission at trade-date boundary; publishing "
                       "final generation\n";
                break;
            }
            std::cerr
                << "mdl-production-router: generation failed: "
                << runtime::RealtimePipelineCutErrorNameV1(
                       cut.error)
                << " generation="
                << market::RealtimeHistoryGenerationErrorNameV1(
                       cut.generation_error)
                << " factor="
                << factor::RealtimeFactorPublishErrorNameV1(
                       cut.factor_result.error)
                << '\n';
            exit_code = 1;
            break;
        }
        if (!PublishKLineGeneration(
                cut, ipc_service, "periodic")) {
            exit_code = 1;
            break;
        }
    }

    if (!ipc_service->failed()) {
        ipc_service->MarkDraining();
    }

    if (exit_code == 0 && !pipeline->fatal()) {
        const runtime::RealtimePipelineCutResultV1 final_cut =
            pipeline->StopAndPublishFinalGeneration(timeout);
        if (!final_cut.published()) {
            std::cerr
                << "mdl-production-router: final generation failed: "
                << runtime::RealtimePipelineCutErrorNameV1(
                       final_cut.error)
                << " generation="
                << market::RealtimeHistoryGenerationErrorNameV1(
                       final_cut.generation_error)
                << " factor="
                << factor::RealtimeFactorPublishErrorNameV1(
                       final_cut.factor_result.error)
                << '\n';
            exit_code = 1;
        } else if (!PublishKLineGeneration(
                       final_cut, ipc_service, "final")) {
            exit_code = 1;
        }
    } else {
        pipeline->StopAndDrain();
    }

    const runtime::RealtimePipelineSnapshotV1 final_snapshot =
        pipeline->Snapshot();
    if (final_snapshot.fatal) {
        ReportFatalSnapshot(final_snapshot);
        exit_code = 1;
    }
    if (ipc_service->failed()) {
        std::cerr
            << "mdl-production-router: IPC V2 projection ended in "
               "FAILED state\n";
        exit_code = 1;
    }

    if (exit_code == 0) {
        if (!ipc_service->MarkStoppedClean(
                final_snapshot.tick_stream_sequence)) {
            std::cerr
                << "mdl-production-router: IPC V2 final mixed-tick "
                   "prefix is incomplete\n";
            exit_code = 1;
            ipc_service->MarkFailed();
        }
    } else {
        ipc_service->MarkFailed();
    }

    const std::shared_ptr<const market::RealtimeKLineGenerationV1>
        final_kline = pipeline->AcquireLatestKLineGeneration();
    std::shared_ptr<
        const market::ObservedInstrumentCatalogSnapshotV2>
        final_catalog;
    const market::ObservedInstrumentDirectoryErrorV2
        catalog_snapshot_error =
            directory->AcquireSnapshot(&final_catalog);
    const market::IntradayInstrumentStoreSnapshotV1& final_store =
        final_snapshot.store;
    std::cerr
        << "mdl-production-router: final: records="
        << final_store.appended_records
        << " record_bytes=" << final_store.accounted_record_bytes
        << " index_bytes=" << final_store.allocated_index_bytes
        << " byte_limit="
        << final_store.maximum_session_accounted_bytes
        << " coverage_from_open="
        << (final_store.coverage_from_open ? "true" : "false")
        << " coverage_lost="
        << (final_store.coverage_lost ? "true" : "false")
        << " kline_bars="
        << (final_kline == nullptr ? 0U : final_kline->bar_count())
        << " accepted_sequence="
        << final_snapshot.processing_progress.accepted_sequence
        << " applied_sequence="
        << final_snapshot.processing_progress.applied_sequence
        << " processing_lag_records="
        << final_snapshot.processing_progress.processing_lag_records()
        << " mainland_a_share_filter="
        << (final_snapshot.mainland_a_share_filter_enabled
                ? "true"
                : "false")
        << " filtered_messages="
        << final_snapshot.filtered_messages
        << " filtered_by_source_sh_snapshot_sh_tick_sz_snapshot_sz_tick="
        << final_snapshot.filtered_messages_by_source[0] << ','
        << final_snapshot.filtered_messages_by_source[1] << ','
        << final_snapshot.filtered_messages_by_source[2] << ','
        << final_snapshot.filtered_messages_by_source[3];
    if (catalog_snapshot_error ==
            market::ObservedInstrumentDirectoryErrorV2::kNone &&
        final_catalog != nullptr) {
        std::cerr
            << " catalog_scope=OBSERVED_ONLY"
            << " coverage_complete="
            << (final_catalog->coverage_complete()
                    ? "true"
                    : "false")
            << " catalog_generation="
            << final_catalog->catalog_generation()
            << " bound_count=" << final_catalog->bound_count()
            << " available_count="
            << final_catalog->available_count()
            << " snapshot_available_count="
            << final_catalog->snapshot_available_count()
            << " tick_available_count="
            << final_catalog->tick_available_count()
            << " factor_eligible_count="
            << final_catalog->factor_eligible_count();
    } else {
        std::cerr
            << " catalog_snapshot_error="
            << market::ObservedInstrumentDirectoryErrorNameV2(
                   catalog_snapshot_error);
    }
    std::cerr << '\n';

    ipc_service->StopControl();
    return exit_code;
}

}  // namespace

int main(int argc, char* argv[]) {
    Options options{};
    bool help = false;
    std::string error;
    if (!ParseOptions(
            argc, argv, &options, &help, &error)) {
        std::cerr
            << "mdl-production-router: " << error << '\n';
        PrintUsage(std::cerr);
        return 2;
    }
    if (help) {
        PrintUsage(std::cout);
        return 0;
    }

    int signal_error = 0;
    if (!InstallStopSignalHandlers(&signal_error)) {
        std::cerr
            << "mdl-production-router: cannot install stop handlers: "
               "errno="
            << signal_error << '\n';
        return 1;
    }
    return Run(options);
}
